from .types import *
from .utils import *
from .rerun import *

from collections.abc import Iterable

import numpy as np

import akayu


def _create_source_node(source_fn):
    def source_node(events, *args):
        exe_start = events.filter(isinstance, Start)
        exe_stop = events.filter(isinstance, Stop)
        exe_chunk = akayu.combine_latest(exe_start, exe_stop).filter(
            lambda x: (
                x[0].name == x[1].name
                and x[0].set == x[1].set
                and x[0].timestamp < x[1].timestamp
            )
        )
        batched_imu = exe_chunk.starmap(source_fn, *args)
        events = (
            akayu.zip(exe_chunk, batched_imu)
            .starmap(
                lambda exe_chunk, batched_imu: (
                    [exe_chunk[0]] + batched_imu + [exe_chunk[1]]
                )
            )
            .flatten()
        )

        return events

    return source_node


try:
    from .utils import source_v2_imu, source_opensim_imu, source_v2_uls, source_v1_imu, source_v1_uls, source_v1_uls_mask
    device_imu_v2_node = _create_source_node(source_v2_imu)
    device_imu_opensim_node = _create_source_node(source_opensim_imu)
    device_uls_v2_node = _create_source_node(source_v2_uls)
    v1_imu_node = _create_source_node(source_v1_imu)
    v1_uls_node = _create_source_node(source_v1_uls)
    v1_uls_mask_node = _create_source_node(source_v1_uls_mask)
except (ImportError, NameError):
    pass


def reinterpolate_imu_node(events, target_hz=100):
    exe_stop = events.filter(isinstance, Stop)
    exe_info = events.filter(isinstance, ExerciseInfo)
    ts = events.filter(isinstance, Timestamp)
    rotmats = events.filter(isinstance, Rotmat)
    gyros = events.filter(isinstance, Gyro)
    accels = events.filter(isinstance, Accel)

    imu_data = akayu.combine_latest(ts, rotmats, gyros, accels, emit_on=rotmats)

    imu_chunk = imu_data.collect()
    exe_stop.sink(imu_chunk.flush)

    def reinterpolate(samples):
        if len(samples) < 2:
            result = []
            for t, rm, gy, ac in samples:
                result.extend([t, Gyro(gy), Accel(ac), Rotmat(rm)])
            return result

        timestamps, rotmats, gyros, accels = zip(*samples)
        timestamps = np.array(timestamps)
        rotmats = np.array(rotmats)
        gyros = np.array(gyros)
        accels = np.array(accels)

        quats = R.from_matrix(rotmats).as_quat(scalar_first=True)
        timestamps_interp, quats_interp, gyros_interp, accels_interp = interpolate_imu(
            timestamps, quats, gyros, accels, scalar_first=True, target_hz=target_hz
        )
        rotmats_interp = R.from_quat(quats_interp, scalar_first=True).as_matrix()

        result = []
        for t, rm, gy, ac in zip(
            timestamps_interp, rotmats_interp, gyros_interp, accels_interp
        ):
            result.extend([Timestamp(int(t)), Gyro(gy), Accel(ac), Rotmat(rm)])
        return result

    reinterpolated = imu_chunk.map(reinterpolate).flatten()
    return akayu.union(exe_info, reinterpolated)


def sts_pca_calibration_node(events):
    pass


def _create_calibration_node(calib_func, apply_func):
    def calibration_node(events, *exe_names_list):
        current_exe = events.filter(isinstance, ExerciseInfo)
        events_tagged = akayu.combine_latest(events, current_exe, emit_on=events)

        start_exe = current_exe.filter(isinstance, Start)

        exe_events_list = []
        exe_chunk_list = []
        rotmats = []
        remainder = events_tagged
        for exe_names in exe_names_list:
            standing_events, remainder = split_filter(
                remainder,
                lambda x, exe_names: (
                    x[1].name in exe_names
                    and (isinstance(x[1], Start) or isinstance(x[0], Stop))
                ),
                exe_names,
            )
            standing_events = standing_events.map(lambda x: x[0])

            calib_starts = standing_events.filter(isinstance, Start)
            calib_stops = standing_events.filter(isinstance, Stop)
            calib_chunk = akayu.zip(calib_starts, calib_stops)

            standing_data = standing_events.filter(
                lambda x: not isinstance(x, ExerciseInfo)
            )
            standing_rotmats = standing_data.filter(isinstance, Rotmat)

            standing_data = standing_data.collect()
            standing_rotmats = standing_rotmats.collect()

            calib_stops.sink(standing_rotmats.flush)
            standing_rotmats = standing_rotmats.filter(len)

            exe_events_list.append(standing_data)
            exe_chunk_list.append(calib_chunk)
            rotmats.append(standing_rotmats)

        events = remainder.map(lambda x: x[0]).filter(
            lambda x: (
                not (
                    isinstance(x, ExerciseInfo)
                    and any(x.name in exe_names for exe_names in exe_names_list)
                )
            )
        )
        calibs = akayu.zip(*rotmats).starmap(calib_func)

        flushed_events = []
        for exe_events in exe_events_list:
            calibs.sink(exe_events.flush)
            flushed_events.append(exe_events)

        tmp = akayu.zip(calibs, *flushed_events)
        calibs = tmp.map(lambda x: x[0])
        exe_events_list = [
            tmp.map(lambda x, i: x[i + 1], i) for i in range(len(flushed_events))
        ]

        init_calib = start_exe.accumulate(
            lambda hist, x: (x, x) if hist is None else (x, None),
            start=None,
            returns_state=True,
        ).filter(lambda x: x is not None)
        init_calib = init_calib.map(lambda x: Rotmat(np.eye(3)))

        calibs = calibs.union(init_calib)

        exe_events_flat = []
        for exe_events, calib_chunk in zip(exe_events_list, exe_chunk_list):
            exe_events = akayu.combine_latest(
                exe_events, calib_chunk, emit_on=exe_events
            ).starmap(lambda data, chunk: [chunk[0]] + list(data) + [chunk[1]])
            exe_events = exe_events.flatten()
            exe_events_flat.append(exe_events)

        events = akayu.union(events, *exe_events_flat)

        data, events = split_filter(events, lambda x: isinstance(x, Rotmat | Accel | Gyro))
        data = akayu.combine_latest(calibs, data, emit_on=data).starmap(apply_func)
        events = akayu.union(events, data)

        return events

    return calibration_node


heading_calibration_node = _create_calibration_node(
    standing_heading_calibration, apply_standing_heading_calibration
)
pose_calibration_node = _create_calibration_node(
    standing_pose_calibration, apply_standing_pose_calibration
)
relrot_calibration_node = _create_calibration_node(
    standing_rel_rotmat_calibration, apply_rel_rotmat_calibration
)
sitstand_calibration_node = _create_calibration_node(
    sitstand_calibration, apply_sitstand_calibration
)


def rr_log_rotmat_node(events, rec, name):
    exe_stop = events.filter(isinstance, Stop)
    ts = events.filter(isinstance, Timestamp)
    rotmats = events.filter(isinstance, Rotmat)
    logs = akayu.combine_latest(ts, rotmats, emit_on=rotmats)

    logs_chunk = logs.collect()
    logs_chunk.filter(len).sink(
        lambda x, rec, name: rr_log_rotmat(rec, name, *zip(*x)), rec, name
    )
    exe_stop.sink(logs_chunk.flush)


def rr_log_rotmat_euler_node(events, rec, name):
    ts = events.filter(isinstance, Timestamp)
    rotmats = events.filter(isinstance, Rotmat)
    logs = akayu.combine_latest(ts, rotmats, emit_on=rotmats)

    logs_chunk = logs.collect()
    logs_chunk.filter(len).sink(
        lambda x, rec, name: rr_log_rotmat_as_euler(rec, name, *zip(*x)), rec, name
    )
    exe_stop = events.filter(isinstance, Stop)
    exe_stop.sink(logs_chunk.flush)


def rr_log_image_node(events, rec, name, color_model="L", type=Image):
    ts = events.filter(isinstance, Timestamp)
    data = events.filter(isinstance, type)
    logs = akayu.combine_latest(ts, data, emit_on=data)

    logs_chunk = logs.collect()
    logs_chunk.filter(len).sink(
        lambda x, rec, name, color_model: rr_log_image(
            rec, name, *zip(*x), color_model=color_model
        ),
        rec,
        name,
        color_model,
    )
    exe_stop = events.filter(isinstance, Stop)
    exe_stop.sink(logs_chunk.flush)


def rr_log_scalar_node(events, rec, name, type=int):
    ts = events.filter(isinstance, Timestamp)
    data = events.filter(isinstance, type)
    logs = akayu.combine_latest(ts, data, emit_on=data)

    logs_chunk = logs.collect()
    logs_chunk.filter(len).sink(
        lambda x, rec, name: rr_log_scalar(rec, name, *zip(*x)), rec, name
    )
    exe_stop = events.filter(isinstance, Stop)
    exe_stop.sink(logs_chunk.flush)


def rr_log_text_node(events, rec, name, type=ExerciseInfo):
    logs = events.filter(isinstance, type)
    logs.sink(lambda x, rec, name: rr_log_text(rec, name, x), rec, name)


def merge_node(*events_arr):
    events_tagged_arr = [
        events.map(lambda x, i: (i, x), i) for i, events in enumerate(events_arr)
    ]
    events_tagged = akayu.union(*events_tagged_arr)
    merged_events_tagged = (
        events_tagged.accumulate(
            merge_tagged_events,
            start={k: [] for k in range(len(events_arr))},
            returns_state=True,
        )
        .filter(len)
        .flatten()
    )
    exe_info, events_tagged = split_filter(
        merged_events_tagged, isinstance, ExerciseInfo
    )
    events = events_tagged.map(lambda x: x[1]).flatten()
    events = akayu.union(exe_info, events)
    return events


def resync_node(*events_arr):
    if isinstance(events_arr[0], dict):
        events_dict = events_arr[0]
        keys = list(events_dict.keys())
        new_vals = resync_node(*events_dict.values())

        return dict(zip(keys, new_vals))

    events_tagged_arr = [
        events.map(lambda x, i: (i, x), i) for i, events in enumerate(events_arr)
    ]
    events_tagged = akayu.union(*events_tagged_arr)
    merged_events_tagged = (
        events_tagged.accumulate(
            merge_tagged_events,
            start={k: [] for k in range(len(events_arr))},
            returns_state=True,
        )
        .filter(len)
        .flatten()
    )
    exe_info, events_tagged = split_filter(
        merged_events_tagged, isinstance, ExerciseInfo
    )
    events_arr = [
        events_tagged.filter(lambda x, i: x[0] == i, i).map(lambda x: x[1]).flatten()
        for i in range(len(events_arr))
    ]
    events_arr = [akayu.union(exe_info, events) for events in events_arr]
    return events_arr

def rel_rotmat_node(events1, events2, threshold=10_000):
    events_arr = [events1, events2]
    exe_arr = [events.filter(isinstance, ExerciseInfo) for events in events_arr]
    ts_arr = [events.filter(isinstance, Timestamp) for events in events_arr]
    rotmats_arr = [events.filter(isinstance, Rotmat) for events in events_arr]

    angles = events1.filter(isinstance, Angle)
    ts_angles = akayu.combine_latest(ts_arr[0], angles, emit_on=angles).flatten()

    ts_rotmats_exe_arr = [akayu.combine_latest(exe, ts, rotmat, emit_on=rotmat) for exe, ts, rotmat in zip(exe_arr, ts_arr, rotmats_arr)]
    paired_data = akayu.combine_latest(*ts_rotmats_exe_arr, emit_on=ts_rotmats_exe_arr[0])
    paired_data = paired_data.filter(lambda x: (type(x[0][0]) == type(x[1][0])) and (x[0][0].name == x[1][0].name))
    paired_data = paired_data.filter(lambda x: (x[0][1] - x[1][1]) < threshold)

    ts = paired_data.map(lambda x: x[0][1])
    rotmats = paired_data.map(lambda x: (x[0][2], x[1][2]))

    rotmats = rotmats.starmap(rel_rotmat)

    events = ts.zip(rotmats).flatten()

    current_exe = akayu.combine_latest(*exe_arr).filter(lambda x: type(x[0]) is type(x[1]) and x[0].name == x[1].name).map(lambda x: x[0])
    events = akayu.union(events, current_exe, ts_angles)

    return events

def xy_ignored_calibration_node(events1, events2, threshold=10_000):
    events_arr = [events1, events2]
    exe_arr = [events.filter(isinstance, ExerciseInfo) for events in events_arr]
    ts_arr = [events.filter(isinstance, Timestamp) for events in events_arr]
    rotmats_arr = [events.filter(isinstance, Rotmat) for events in events_arr]

    ts_rotmats_exe_arr = [akayu.combine_latest(exe, ts, rotmat, emit_on=rotmat) for exe, ts, rotmat in zip(exe_arr, ts_arr, rotmats_arr)]
    paired_data = akayu.combine_latest(*ts_rotmats_exe_arr, emit_on=ts_rotmats_exe_arr[0])
    paired_data = paired_data.filter(lambda x: (type(x[0][0]) == type(x[1][0])) and (x[0][0].name == x[1][0].name))
    paired_data = paired_data.filter(lambda x: (x[0][1] - x[1][1]) < threshold)

    ts = paired_data.map(lambda x: x[0][1])
    rotmats = paired_data.map(lambda x: (x[0][2], x[1][2]))

    rotmats = rotmats.collect()
    exe_arr[0].filter(isinstance, Stop).sink(lambda x: print(x.name))
    exe_arr[0].filter(isinstance, Stop).sink(rotmats.flush)
    rotmats = rotmats.map(xy_ignored_calibration2)

    rotmats_arr = [rotmats.map(lambda x: x[0]).flatten(), rotmats.map(lambda x: x[1]).flatten()]
    events_arr = [ts.zip(rotmats).flatten() for rotmats in rotmats_arr]

    current_exe = akayu.combine_latest(*exe_arr).filter(lambda x: type(x[0]) is type(x[1]) and x[0].name == x[1].name).map(lambda x: x[0])
    events_arr = [akayu.union(events, current_exe) for events in events_arr]

    return events_arr

def table_calibration_node(events_arr, exe_names):
    exe_arr = [events.filter(isinstance, ExerciseInfo) for events in events_arr]

    events_tagged_arr = [akayu.combine_latest(events, current_exe, emit_on=events) for events, current_exe in zip(events_arr, exe_arr)]
    start_exe_arr = [current_exe.filter(isinstance, Start) for current_exe in exe_arr]

    table_events_arr, remainder_arr = zip(*[split_filter(events_tagged, lambda x: (x[1].name in exe_names and isinstance(x[1], Start | Stop))) for events_tagged in events_tagged_arr])
    table_events_arr = [table_events.map(lambda x: x[0]) for table_events in table_events_arr]

    calib_starts_arr = [table_events.filter(isinstance, Start) for table_events in table_events_arr]
    calib_stops_arr = [table_events.filter(isinstance, Stop) for table_events in table_events_arr]
    calib_chunk_arr = [akayu.zip(calib_starts, calib_stops) for calib_starts, calib_stops in zip(calib_starts_arr, calib_stops_arr)]

    table_data_arr = [table_events.filter(lambda x: not isinstance(x, ExerciseInfo)) for table_events in table_events_arr]
    table_rotmats_arr = [table_data.filter(isinstance, Rotmat) for table_data in table_data_arr]

    table_data_arr = [table_data.collect() for table_data in table_data_arr]
    table_rotmats_arr = [table_rotmats.collect() for table_rotmats in table_rotmats_arr]

    [calib_stops.sink(table_rotmats.flush) for calib_stops, table_rotmats in zip(calib_stops_arr, table_rotmats_arr)]
    table_rotmats_arr = [table_rotmats.filter(len) for table_rotmats in table_rotmats_arr]

    events_arr = [remainder.map(lambda x: x[0]).filter(lambda x: not (isinstance(x, ExerciseInfo) and (x.name in exe_names))) for remainder in remainder_arr]
    calibs_arr = [akayu.zip(table_rotmats_arr[0], table_rotmats).starmap(table_calibration) for table_rotmats in table_rotmats_arr]

    [calibs.sink(table_data.flush) for calibs, table_data in zip(calibs_arr, table_data_arr)]

    tmp_arr = [akayu.zip(calibs, table_data) for calibs, table_data in zip(calibs_arr, table_data_arr)]
    calibs_arr = [tmp.map(lambda x: x[0]) for tmp in tmp_arr]
    table_data_arr = [tmp.map(lambda x: x[1]) for tmp in tmp_arr]

    init_calib_arr = [start_exe.accumulate(
        lambda hist, x: (x, x) if hist is None else (x, None),
        start=None,
        returns_state=True,
    ).filter(lambda x: x is not None) for start_exe in start_exe_arr]
    init_calib_arr = [init_calib.map(lambda x: Rotmat(np.eye(3))) for init_calib in init_calib_arr]

    calibs_arr = [calibs.union(init_calib) for calibs, init_calib in zip(calibs_arr, init_calib_arr)]

    exe_events_arr = [akayu.combine_latest(
        exe_events, calib_chunk, emit_on=exe_events
    ).starmap(lambda data, chunk: [chunk[0]] + list(data) + [chunk[1]]) for exe_events, calib_chunk in zip(table_data_arr, calib_chunk_arr)]
    exe_events_arr = [exe_events.flatten() for exe_events in exe_events_arr]

    events_arr = [akayu.union(events, exe_events) for events, exe_events in zip(events_arr, exe_events_arr)]

    data_arr, events_arr = zip(*[split_filter(events, isinstance, Rotmat | Accel | Gyro) for events in events_arr])
    data_arr = [akayu.combine_latest(calibs, data, emit_on=data).starmap(apply_table_calibration) for calibs, data in zip(calibs_arr, data_arr)]
    events_arr = [akayu.union(events, data) for events, data in zip(events_arr, data_arr)]

    return events_arr

def add_idle_exercise_node(events):
    exe_start = events.filter(isinstance, Start)
    exe_stop = events.filter(isinstance, Stop)
    exe_stop = exe_stop.map(
        lambda exe_stop: (exe_stop, Start("Idle", exe_stop.timestamp, 0))
    )
    exe_start = exe_start.map(
        lambda exe_start: (Stop("Idle", exe_start.timestamp, 0), exe_start)
    )

    events_remainder = events.filter(lambda x: not isinstance(x, ExerciseInfo))
    events = akayu.union(exe_start, exe_stop, events_remainder).flatten()

    return events


def segment_bone_node(events):
    images, _ = split_filter(events, isinstance, Image)
    masks = images.map(segment_bone)
    events = events.union(masks)

    return events


def get_uls_angles_node(events):
    masks = events.filter(isinstance, Mask)
    masks_list = masks.collect()
    events_list = events.collect()
    exe = events.filter(isinstance, ExerciseInfo)
    exe.sink(masks_list.flush)
    exe.sink(events_list.flush)

    angles_list = masks_list.map(get_uls_angles, [get_angle_pystack])

    def insert_angles_at_masks(events, angles):
        new_events = []
        for event in events:
            new_events.append(event)
            if isinstance(event, Mask):
                new_events.append(Angle(angles.pop(0)))
        return new_events

    events_list = events_list.zip(angles_list).starmap(insert_angles_at_masks)
    return events_list.flatten()


def get_uls_angles_rot_node(events):
    from scipy.spatial.transform import Rotation

    def window_2(hist, x):
        if hist is None:
            return x, None
        return x, (hist, x)

    def imu_delta(hist, x):
        if hist is None:
            return x, None
        delta = Rotmat(np.array(hist).T @ np.array(x))
        return x, delta

    def sum_ex_rotmat(hist, x):
        R_cum = hist[0]
        if isinstance(x, Stop):
            return (Rotation.identity(),), x
        if not isinstance(x, (float, Rotmat)):
            return (R_cum,), x

        if isinstance(x, Rotmat):
            R_delta = Rotation.from_matrix(np.array(x))
        else:
            R_delta = Rotation.from_euler("y", x)

        R_cum = R_cum * R_delta

        rotvec = R_cum.as_rotvec()
        angle = float(np.degrees(np.linalg.norm(rotvec)))
        if angle > 0:
            angle *= float(np.sign(rotvec[1]))

        return (R_cum,), angle

    images, events = split_filter(events, isinstance, Image)
    uls_deltas = (
        images.accumulate(window_2, returns_state=True, start=None)
        .filter(lambda x: x is not None)
        .starmap(optimize_angle_between)
    )

    rotmats, events = split_filter(events, isinstance, Rotmat)
    imu_deltas = rotmats.accumulate(imu_delta, returns_state=True, start=None).filter(
        lambda x: x is not None
    )

    events = akayu.union(events, uls_deltas, imu_deltas)
    events = events.accumulate(
        sum_ex_rotmat, returns_state=True, start=(Rotation.identity(),)
    )

    return events


def export_masks_node(events, session_id):
    ts = events.filter(isinstance, Timestamp)
    masks = events.filter(isinstance, Mask)
    images = events.filter(isinstance, Image)
    logs = akayu.combine_latest(ts, images, masks, emit_on=masks)

    def save_images(
        timestamps: list[Timestamp],
        images: list[Image],
        masks: list[Mask],
        exe_info,
        path,
    ):
        path = Path(path) / f"{format_exeinfo(exe_info)}"
        path.mkdir(exist_ok=True, parents=True)
        for timestamp, image, image in zip(timestamps, images, images):
            image = np.array(image)
            image = ((1 - image) * 255).astype(np.uint8)

            image = np.array(image)
            image = (image * 255).astype(np.uint8)
            image = cv2.resize(image, image.shape[::-1])

            color_image = np.zeros(list(image.shape) + [3])
            color_image[:] = image[:, :, None]
            color_image[:, :, 2] = np.clip(color_image[:, :, 2] + image, max=255)

            cv2.imwrite(f"{path}/uls_{timestamp}.png", color_image)

    exe_stop = events.filter(isinstance, Stop)
    logs_chunk = logs.collect()
    logs_chunk_exe = logs_chunk.zip(exe_stop)
    logs_chunk_exe.filter(lambda x: len(x[0])).sink(
        lambda x, *args: save_images(*zip(*x[:-1]), x[-1], *args),
        f"masks/{session_id}/",
    )
    exe_stop.sink(logs_chunk.flush)


def export_plot_node(events, session_id, segment):
    ts = events.filter(isinstance, Timestamp)
    data = events.filter(isinstance, Angle)
    logs = akayu.combine_latest(ts, data, emit_on=data)

    def save_plots(
        timestamps: list[Timestamp], angles: list[Angle], exe_info: ExerciseInfo, path
    ):
        path = Path(path)
        path.mkdir(exist_ok=True, parents=True)

        timestamps = np.array(timestamps)
        angles = np.array(angles)

        plt.scatter(timestamps, angles)
        plt.savefig(path / f"{format_exeinfo(exe_info)}.png")
        plt.cla()

    logs_chunk = logs.collect()
    exe_stop = events.filter(isinstance, Stop)
    logs_chunk_exe = logs_chunk.zip(exe_stop)
    logs_chunk_exe.filter(lambda x: len(x[0])).sink(
        lambda x, *args: save_plots(*zip(*x[0]), x[1], *args),
        f"plots/{session_id}/{segment}",
    )
    exe_stop.sink(logs_chunk.flush)

    return events


def fuse_uls_imu_angles_node(events):
    from scipy.spatial.transform import Rotation

    angles = events.filter(isinstance, Angle)
    exe_info = events.filter(isinstance, ExerciseInfo)

    default_angle = exe_info.map(lambda _: Angle(0.0))
    latest_angle = akayu.union(angles, default_angle)

    rotmats, events = split_filter(events, isinstance, Rotmat)
    paired = akayu.combine_latest(latest_angle, rotmats, emit_on=rotmats)

    def fuse(angle, rotmat):
        R_uls = Rotation.from_euler("z", float(angle), degrees=True).as_matrix()
        return Rotmat(R_uls @ np.array(rotmat))

    fused = paired.starmap(fuse)
    return akayu.union(events, fused)

def pivot_shift_prediction_node(events_left, events_right):
    events_arr = [events_left, events_right]
    ts_arr = [events.filter(isinstance, Timestamp) for events in events_arr]
    uls_angle_arr = [events.filter(isinstance, Angle) for events in events_arr]
    rotmat_arr = [events.filter(isinstance, Rotmat) for events in events_arr]
    ts_rotmat_arr = [akayu.combine_latest(ts, rotmat, emit_on=rotmat).map(lambda x: x[0]) for ts, rotmat in zip(ts_arr, rotmat_arr)]
    ts_uls_angle_arr = [akayu.combine_latest(ts, uls_angle, emit_on=uls_angle).map(lambda x: x[0]) for ts, uls_angle in zip(ts_arr, uls_angle_arr)]
    segment_start_arr = [events.filter(isinstance, SegmentStart).map(lambda x: x.timestamp) for events in events_arr]
    segment_stop_arr = [events.filter(isinstance, SegmentStop).map(lambda x: x.timestamp) for events in events_arr]
    segments_arr = [akayu.combine_latest(segment_start, segment_stop, emit_on=segment_stop) for segment_start, segment_stop in zip(segment_start_arr, segment_stop_arr)]
    exe_arr = [events.filter(isinstance, Stop) for events in events_arr]

    rotmat_list_arr = [rotmat.collect() for rotmat in rotmat_arr]
    uls_angle_list_arr = [uls_angle.collect() for uls_angle in uls_angle_arr]
    ts_rotmat_list_arr = [ts_rotmat.collect() for ts_rotmat in ts_rotmat_arr]
    ts_uls_angle_list_arr = [ts_uls_angle.collect() for ts_uls_angle in ts_uls_angle_arr]
    segments_list_arr = [seg.collect() for seg in segments_arr]
    [exe.sink(rotmat_list.flush) for exe, rotmat_list in zip(exe_arr, rotmat_list_arr)]
    [exe.sink(uls_angle_list.flush) for exe, uls_angle_list in zip(exe_arr, uls_angle_list_arr)]
    [exe.sink(ts_rotmat_list.flush) for exe, ts_rotmat_list in zip(exe_arr, ts_rotmat_list_arr)]
    [exe.sink(ts_uls_angle_list.flush) for exe, ts_uls_angle_list in zip(exe_arr, ts_uls_angle_list_arr)]
    [exe.sink(segments_list.flush) for exe, segments_list in zip(exe_arr, segments_list_arr)]

    rotmat_list_arr = [rotmat.map(np.array) for rotmat in rotmat_list_arr]
    uls_angle_list_arr = [uls_angle.map(np.array) for uls_angle in uls_angle_list_arr]
    ts_rotmat_list_arr = [ts_rotmat.map(np.array) for ts_rotmat in ts_rotmat_list_arr]
    ts_uls_angle_list_arr = [ts_uls_angle.map(np.array) for ts_uls_angle in ts_uls_angle_list_arr]
    segments_list_arr = [seg.map(np.array) for seg in segments_list_arr]

    z_list_arr = [rotmat_list.map(lambda x: rotmat_to_euler(x)[0][:, -1]) for rotmat_list in rotmat_list_arr]

    from .model.pivot_shift.predict import PivotShiftPredictor
    predictor = PivotShiftPredictor("./src/pipeline/model/pivot_shift/exported", device="cpu")

    pivot_shift = akayu.zip(
        *uls_angle_list_arr,
        *z_list_arr,
        *ts_uls_angle_list_arr,
        *ts_rotmat_list_arr,
        segments_list_arr[0],
        exe_arr[0],
        ).filter(lambda x: x[-1].name == "5 Repetition Slow Sit to Stand Test").map(lambda x: x[:-1]).map(lambda x, predictor: PivotShift(predictor.predict(*x[:4], x[4:6], x[6:8], x[8])), predictor)

    events_arr = [events.union(pivot_shift) for events in events_arr]
    return events_arr