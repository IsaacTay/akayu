from .types import *
from .nodes import *
from .utils import *
from .rerun import *

try:
    from .write import *
    from .write.table import *
    from .write.typst import *
    from .write.graph import *
    from .ikdc import summarize_ikdc
except ImportError:
    pass

from pathlib import Path
import io
import os
import shutil
import json
import traceback
import zipfile

import akayu

try:
    from src.ultrasound import get_first_segment_ultrasound_frames
except ImportError:
    def get_first_segment_ultrasound_frames(*args, **kwargs):
        raise ImportError("src.ultrasound not available")


def update_task(_, progress, task):
    import rich.progress
    rich.progress.Progress.update(progress, task, advance=1)


def static_standing_opensim_flow(data_path: Path):
    import rerun as rr

    data_path = data_path

    source = akayu.Stream()
    bones = ["femur_r", "femur_l", "tibia_r", "tibia_l"]

    rec = rr.RecordingStream("HelloWorld")
    with rec:
        rr.save("test.rrd")

    with rich.progress.Progress(
        rich.progress.TextColumn("[progress.description]{task.description}"),
        rich.progress.TimeElapsedColumn(),
        rich.progress.MofNCompleteColumn(),
    ) as progress:
        segment_events = {}
        for bone in bones:
            segment, side = bone.split("_")
            rr_init_pose(rec, f"{side}/{segment}")

            events = source
            events = device_imu_opensim_node(events, data_path, bone)

            events = heading_calibration_node(events, "standing")
            rr_log_rotmat_node(events, rec, f"{side}/{segment}")

            segment_events[bone] = events

        segment_events = resync_node(segment_events)
        for side in ("l", "r"):
            events_arr = [
                segment_events.get(f"{segment}_{side}", akayu.Stream())
                for segment in ("femur", "tibia")
            ]
            events_thigh, events_shank = events_arr
            events = rel_rotmat_node(events_thigh, events_shank)
            events = relrot_calibration_node(events, "standing")
            events.sink(
                update_task, progress, progress.add_task(f"{side}_log", total=None)
            )

            rr_log_rotmat_euler_node(events, rec, side)

        for exe in ["standing", "sts"]:
            source.emit(Start(exe, 0, 0))
            source.emit(Stop(exe, 10_000, 0))

    with rec:
        rr.disconnect()


def source_v1_flow(session_path, imu=True, ultrasound=True):
    source = akayu.Stream()
    events = source.map(source_v1_exercise).flatten()

    with rich.progress.Progress(
        rich.progress.TextColumn("[progress.description]{task.description}"),
        rich.progress.TimeElapsedColumn(),
        rich.progress.MofNCompleteColumn(),
    ) as progress:
        segment_events = {
            segment: events
            for segment in ("left/thigh", "left/shank", "right/thigh", "right/shank")
        }
        for segment, events in segment_events.items():
            events_uls = events
            if ultrasound:
                events_uls = v1_uls_node(events, session_path, segment)
                events_uls.sink(
                    update_task,
                    progress,
                    progress.add_task(f"{segment} uls", total=None),
                )
            events_imu = events
            if imu:
                events_imu = v1_imu_node(events, session_path, segment)
                events_imu = reinterpolate_imu_node(
                    events_imu, target_hz=100
                )
                events_imu.sink(
                    update_task,
                    progress,
                    progress.add_task(f"{segment} imu", total=None),
                )

            events = merge_node(events_uls, events_imu)

            segment_events[segment] = events

        segment_events = flush_segments(segment_events)
        source.emit(session_path)

    segment_events_list = segment_events.flush()
    return segment_events_list


def source_v2_metadata_flow(
    metadata: Metadata,
    session_path: Path | str | None = None,
    imu=True,
    ultrasound=True,
):
    if session_path is None:
        data_path = Path("sample_session_data") / metadata["sessionId"]
    else:
        data_path = Path(session_path)

    leg_config = metadata["legConfiguration"]
    leg_config = {k.replace("_", "/"): v for k, v in leg_config.items() if v != ""}

    source = akayu.Stream()
    events = source.map(source_v2_exercise).flatten()

    with rich.progress.Progress(
        rich.progress.TextColumn("[progress.description]{task.description}"),
        rich.progress.TimeElapsedColumn(),
        rich.progress.MofNCompleteColumn(),
    ) as progress:
        segment_events = {segment: events for segment in leg_config.keys()}
        for segment, device in leg_config.items():
            events = segment_events[segment]

            events_uls = events
            if ultrasound:
                events_uls = device_uls_v2_node(events, data_path / device)
                events_uls.sink(
                    update_task,
                    progress,
                    progress.add_task(f"{segment} uls", total=None),
                )

            events_imu = events
            if imu:
                events_imu = device_imu_v2_node(events, data_path / device)
                events_imu.sink(
                    update_task,
                    progress,
                    progress.add_task(f"{segment} imu", total=None),
                )

            events = merge_node(events_uls, events_imu)

            segment_events[segment] = events

        segment_events = flush_segments(segment_events)
        source.emit(metadata)

    segment_events_list = segment_events.flush()
    return segment_events_list


def source_v2_flow(session_path, **kwargs):
    with open(f"{session_path}/{session_path.name}.json", "r") as f:
        metadata = json.load(f)
    return source_v2_metadata_flow(metadata, session_path=session_path, **kwargs)


def source_v2_online_flow(session_id, extract_root=Path("./artifacts/downloads")):
    import requests

    api_url = os.getenv("API_URL", None)
    url = f"{api_url}/session/{session_id}/download"

    print("Download...")
    response = requests.get(url, stream=True)

    data = io.BytesIO()
    chunk_size = 1024 * 1024
    downloaded = 0

    for chunk in response.iter_content(chunk_size=chunk_size):
        if chunk:
            data.write(chunk)
            downloaded += len(chunk)

    extract_root.mkdir(exist_ok=True, parents=True)
    extract_path = extract_root / f"{session_id}"

    print("Extraction...")
    with zipfile.ZipFile(data) as zip_ref:
        zip_ref.extractall(extract_path)

    segment_event_list = source_v2_flow(extract_path)

    shutil.rmtree(extract_path)
    print("Cleanup")

    return segment_event_list


def source_any_flow(session_path: Path, **kwargs):
    for source_fn in (source_v1_flow, source_v2_flow):
        print(f"Trying using: {source_fn}")
        try:
            return source_fn(session_path, **kwargs)
        except Exception as e:
            print(f"Failed using: {source_fn}")
            print(f"Error: {e!r}")
            traceback.print_exc()
            continue
    raise Exception("source any failed")


def default_flow(
    segment_events_list,
    session_id,
):
    segment_source = {k: akayu.Stream() for k in segment_events_list.keys()}
    segment_events = {k: events.flatten() for k, events in segment_source.items()}

    sit_exe_names = ["Sitting Calibration", "Calibration - Calibration 2 - Sitting"]
    stand_exe_names = ["Standing Calibration", "Calibration - Calibration 1 - Standing"]

    sts_exe_names = [
        "5 Repetition Slow Sit to Stand Test",
        "5 Repetition Slow Sit to Stand",
    ]
    active_flexion_exe_names = [
        "Standing Active Flexion Test",
        "Standing Active Flexion",
    ]
    fast_walk_exe_names = ["10m Fast Walk Test", "10m Fast Walk"]

    table_exe_names = [
        "Table Calibration",
    ]
    sts_exe_names = [
        "5 Repetition Slow Sit to Stand Test",
    ]
    active_flexion_exe_names = [
        "Standing Active Flexion Test",
    ]

    with rich.progress.Progress(
        rich.progress.TextColumn("[progress.description]{task.description}"),
        rich.progress.TimeElapsedColumn(),
        rich.progress.MofNCompleteColumn(),
    ) as progress:
        for segment, events in segment_events.items():
            events = segment_bone_node(events)

            events = get_uls_angles_node(events)

            events = events.filter(lambda x: not isinstance(x, Image))
            events = events.filter(lambda x: not isinstance(x, Mask))

            events = sitstand_calibration_node(events, sit_exe_names, stand_exe_names)
            events = pose_calibration_node(events, stand_exe_names, sit_exe_names)
            events.sink(
                update_task, progress, progress.add_task(f"{segment}", total=None)
            )

            segment_events[segment] = events

        sides = set(k.split("/")[0] for k in segment_events.keys())

        for side in sides:
            events_arr = [
                segment_events.get(f"{side}/{segment}")
                for segment in ("thigh", "shank")
            ]
            events_arr = resync_node(*events_arr)
            events_thigh, events_shank = events_arr
            events = rel_rotmat_node(events_thigh, events_shank)
            events.sink(update_task, progress, progress.add_task(f"{side}", total=None))

            timestamp = events.filter(isinstance, Timestamp)
            relrot = events.filter(isinstance, Rotmat)
            thigh_gyro = events_thigh.filter(isinstance, Gyro)
            shank_gyro = events_shank.filter(isinstance, Gyro)
            shank_accel = events_shank.filter(isinstance, Accel)
            relrot_thigh_gyro_ts = relrot.combine_latest(
                thigh_gyro, timestamp, emit_on=relrot
            )
            relrot_shank_gyro_ts = relrot.combine_latest(
                shank_gyro, shank_accel, timestamp, emit_on=relrot
            )
            relrot_thigh_gyro_ts = relrot_thigh_gyro_ts.collect()
            relrot_shank_gyro_ts = relrot_shank_gyro_ts.collect()

            exe_info = events.filter(isinstance, ExerciseInfo)
            exe_info.sink(relrot_thigh_gyro_ts.flush)
            exe_info.sink(relrot_shank_gyro_ts.flush)

            is_end = exe_info.map(isinstance, Stop)
            relrot_thigh_gyro_ts = (
                is_end.zip(relrot_thigh_gyro_ts)
                .filter(lambda x: x[0])
                .map(lambda x: x[1])
            )
            relrot_shank_gyro_ts = (
                is_end.zip(relrot_shank_gyro_ts)
                .filter(lambda x: x[0])
                .map(lambda x: x[1])
            )
            exe_stop = exe_info.filter(isinstance, Stop)

            sts_names_lc = {n.strip().lower() for n in sts_exe_names}
            active_flexion_names_lc = {n.strip().lower() for n in active_flexion_exe_names}
            fast_walk_names_lc = {n.strip().lower() for n in fast_walk_exe_names}

            def pick_segmenter(exe_name: str):
                name = exe_name.strip().lower()
                if name in sts_names_lc or "sit to stand" in name:
                    return segment_sit_to_stand
                if name in active_flexion_names_lc or "active flexion" in name:
                    return segment_active_flexion
                if name in fast_walk_names_lc:
                    return segment_walking
                return lambda _: []

            def pick_segment_samples(stop_name: str, thigh_samples, shank_samples):
                segmenter = pick_segmenter(stop_name)
                if segmenter is segment_walking:
                    return segmenter(shank_samples)
                rotmat_ts = [(s[0], s[-1]) for s in thigh_samples]
                return segmenter(rotmat_ts)

            segments = exe_stop.zip(relrot_thigh_gyro_ts, relrot_shank_gyro_ts).starmap(
                lambda stop, thigh_samples, shank_samples: pick_segment_samples(
                    stop.name,
                    thigh_samples,
                    shank_samples,
                )
            )

            exe_start = exe_info.filter(isinstance, Start)
            exe_chunk = akayu.combine_latest(exe_start, exe_stop, emit_on=exe_stop)
            segments = (
                exe_chunk.zip(segments)
                .starmap(lambda exe_chunk, segs: [exe_chunk[0]] + segs + [exe_chunk[1]])
                .flatten()
            )

            events = merge_node(segments, events)

            segment_events[side] = events

        segment_events["left"], segment_events["right"] = pivot_shift_prediction_node(segment_events["left"], segment_events["right"])

        segment_events = flush_segments(segment_events)
        for k, events in segment_source.items():
            events.emit(segment_events_list[k])

    segment_events_list = segment_events.flush()
    return segment_events_list


def rerun_flow(segment_events_list, session_id, out_path=Path("artifacts/rrd")):
    import rerun as rr

    segment_source = {k: akayu.Stream() for k in segment_events_list.keys()}
    segment_events = {k: events.flatten() for k, events in segment_source.items()}
    sides = set(k for k in segment_events_list.keys() if "/" not in k)

    rec = rr.RecordingStream(session_id)
    out_path.mkdir(exist_ok=True, parents=True)
    rrd_path = out_path / f"{session_id}.rrd"
    with rec:
        rr.save(rrd_path)

    for segment, events in segment_events.items():
        if segment in sides:
            rr_log_rotmat_euler_node(events, rec, segment)
            rr_log_text_node(events, rec, f"{segment}/logs", type=SegmentInfo)
            continue

        rr_init_pose(rec, segment)
        rr_log_rotmat_node(events, rec, segment)
        rr_log_image_node(events, rec, segment)
        rr_log_image_node(events, rec, f"{segment}/mask", type=Mask)
        rr_log_scalar_node(events, rec, f"{segment}/uls_angles", type=Angle)

    for k, events in segment_source.items():
        events.emit(segment_events_list[k])

    with rec:
        rr.disconnect()

    return {"files": [rrd_path]}


def typst_flow(
    segment_events_list,
    session_id=None,
    out_path=Path("artifacts/pdf"),
    report_json_path: Path | str | None = None,
    report_pdf_path: Path | str | None = None,
    template_path: str = "templates/templatev3.typ",
    session_path: Path | str | None = None,
):
    import yaml

    typ_ctx = TypstCtx()

    sides = set(k for k in segment_events_list.keys() if "/" not in k)
    segment_source = {k: akayu.Stream() for k in sides}
    segment_events = {k: events.flatten() for k, events in segment_source.items()}
    for k, events in segment_events.items():
        timestamps = events.filter(isinstance, Timestamp)
        rotmats = events.filter(isinstance, Rotmat)
        rotmats_ts = rotmats.combine_latest(timestamps, emit_on=rotmats)
        rotmats_ts = rotmats_ts.collect()

        segment = events.filter(isinstance, SegmentInfo)
        segment.sink(rotmats_ts.flush)

        is_end = segment.map(isinstance, SegmentStop)
        rotmats_ts = (
            is_end.zip(rotmats_ts)
            .filter(lambda x: x[0] and len(x[1]) > 0)
            .map(lambda x: x[1])
        )
        ts_rotmats = rotmats_ts.map(lambda x: list(map(np.array, zip(*x)))[::-1])

        ts_rotmats.sink(lambda x: typ_log_rotmat_as_euler(typ_ctx, f"/{k}/sts", *x))

    for k, events in segment_source.items():
        events.emit(segment_events_list[k])

    typ_ctx.calculate()

    template_dir = Path(template_path).resolve().parent
    rotational_kinematics_plot = None
    graph_asset_dir = (
        template_dir
        / "_generated_graphs"
        / (session_id if session_id is not None else "session")
    )
    graph_path = render_rotational_kinematics_plot(
        graph_asset_dir / "rotational_kinematics.png",
        typ_ctx.data.get("/left/sts/y/_calc/mean", [None, None])[1],
        typ_ctx.data.get("/left/sts/z/_calc/mean", [None, None])[1],
        typ_ctx.data.get("/right/sts/y/_calc/mean", [None, None])[1],
        typ_ctx.data.get("/right/sts/z/_calc/mean", [None, None])[1],
    )
    if graph_path is not None:
        rotational_kinematics_plot = os.path.relpath(
            graph_path.resolve(), start=template_dir
        )

    typ_ctx.set_field("rotational_kinematics_plot", rotational_kinematics_plot)

    selected_ultrasound_frames = []
    if session_path is not None:
        source_ultrasound_frames = get_first_segment_ultrasound_frames(
            session_path, n_frames=9
        )
        if len(source_ultrasound_frames) > 0:
            frame_asset_dir = (
                template_dir
                / "_generated_ultrasound_frames"
                / (session_id if session_id is not None else "session")
            )
            frame_asset_dir.mkdir(parents=True, exist_ok=True)

            copied_frames = []
            for i, frame in enumerate(source_ultrasound_frames):
                src = Path(frame)
                suffix = src.suffix if src.suffix else ".png"
                dst = frame_asset_dir / f"frame_{i + 1:02d}{suffix}"
                shutil.copy2(src, dst)
                copied_frames.append(dst.resolve())

            selected_ultrasound_frames = [
                os.path.relpath(path, start=template_dir) for path in copied_frames
            ]
    typ_ctx.set_field("selected_ultrasound_frames", selected_ultrasound_frames)

    patient_metadata = {}
    if session_path is not None:
        session_path = Path(session_path)
        session_json_path = session_path / "session_data.json"
        metafile_path = session_path / "metafile.yml"

        if session_json_path.exists():
            with open(session_json_path, "r") as f:
                session_data = json.load(f)
            patient_metadata = session_data.get("patient_metadata", {})
        elif metafile_path.exists():
            with open(metafile_path, "r") as f:
                loaded = yaml.safe_load(f)
            patient_metadata = loaded if isinstance(loaded, dict) else {}

    try:
        from .ikdc import summarize_ikdc
        ikdc_summary = summarize_ikdc(patient_metadata.get("ikdc"))
        if ikdc_summary is not None:
            patient_metadata["ikdc"] = ikdc_summary["ikdc"]
            patient_metadata["ikdc_summary"] = ikdc_summary
    except ImportError:
        pass

    typ_ctx.set_field("patient_metadata", patient_metadata)

    if report_pdf_path is None:
        if session_id is None:
            raise ValueError(
                "Either session_id or report_pdf_path must be provided to typst_flow"
            )
        report_pdf_path = Path(out_path) / f"{session_id}.pdf"
    else:
        report_pdf_path = Path(report_pdf_path)

    if report_json_path is not None:
        report_json_path = Path(report_json_path)
        report_json_path.parent.mkdir(parents=True, exist_ok=True)
        with open(report_json_path, "w") as f:
            f.write(typ_ctx.to_json())

    typ_ctx.to_report(report_pdf_path, template_path=template_path)

    return segment_events


def csv_flow(segment_events_list, session_id, out_path=Path("artifacts/csv")):
    tbl_ctx = TableCtx()

    sides = set(k for k in segment_events_list.keys() if "/" not in k)
    segment_source = {k: akayu.Stream() for k in segment_events_list.keys()}
    segment_events = {k: events.flatten() for k, events in segment_source.items()}
    for k in sides:
        events = segment_events[k]

        timestamps = events.filter(isinstance, Timestamp)
        rotmats = events.filter(isinstance, Rotmat)
        rotmats_ts = rotmats.combine_latest(timestamps, emit_on=rotmats)
        rotmats_ts = rotmats_ts.collect()

        current_exe = events.filter(isinstance, ExerciseInfo)
        current_exe.sink(rotmats_ts.flush)

        rotmats_ts = current_exe.zip(rotmats_ts).filter(
            lambda x: isinstance(x[0], Stop) and len(x[1]) > 0
        )
        ts_rotmats = rotmats_ts.map(
            lambda x: [format_exeinfo(x[0])] + list(map(np.array, zip(*x[1])))[::-1]
        )

        ts_rotmats.sink(
            lambda x, k: tbl_log_rotmat_as_euler(
                tbl_ctx, f"{k}/{x[0]}_kinematics", *x[1:]
            ),
            k,
        )

        events = segment_events[f"{k}/thigh"]

        timestamps = events.filter(isinstance, Timestamp)
        angles = events.filter(isinstance, Angle)
        angles_ts = angles.combine_latest(timestamps, emit_on=angles)
        angles_ts = angles_ts.collect()

        current_exe = events.filter(isinstance, ExerciseInfo)
        current_exe.sink(angles_ts.flush)

        angles_ts = current_exe.zip(angles_ts).filter(
            lambda x: isinstance(x[0], Stop) and len(x[1]) > 0
        )
        ts_angles = angles_ts.map(
            lambda x: [format_exeinfo(x[0])] + list(map(np.array, zip(*x[1])))[::-1]
        )

        ts_angles.sink(
            lambda x, k: tbl_log_scalar(tbl_ctx, f"{k}/{x[0]}_uls", "timestamp", x[1]),
            k,
        )
        ts_angles.sink(
            lambda x, k: tbl_log_scalar(tbl_ctx, f"{k}/{x[0]}_uls", "angles", x[2]), k
        )

    for k, events in segment_source.items():
        events.emit(segment_events_list[k])

    tbl_ctx.export_csvs(out_path / f"{session_id}")


def segment_csv_flow(
    segment_events_list, session_id, out_path=Path("artifacts/csv_segments")
):
    tbl_ctx = TableCtx()

    sides = set(k for k in segment_events_list.keys() if "/" not in k)
    segment_source = {k: akayu.Stream() for k in sides}
    segment_events = {k: events.flatten() for k, events in segment_source.items()}

    for k in sides:
        events = segment_events[k]

        seg_starts = events.filter(isinstance, SegmentStart).collect()
        seg_stops = events.filter(isinstance, SegmentStop).collect()

        exe_info = events.filter(isinstance, ExerciseInfo)
        exe_info.sink(seg_starts.flush)
        exe_info.sink(seg_stops.flush)

        is_stop = exe_info.map(isinstance, Stop)
        seg_starts = is_stop.zip(seg_starts).filter(lambda x: x[0]).map(lambda x: x[1])
        seg_stops = is_stop.zip(seg_stops).filter(lambda x: x[0]).map(lambda x: x[1])

        exe_stop = exe_info.filter(isinstance, Stop)

        exe_stop.zip(seg_starts, seg_stops).filter(lambda x: len(x[1]) > 0).sink(
            lambda x, k=k: tbl_log_segments(
                tbl_ctx, f"{k}/{format_exeinfo(x[0])}_segmentation", x[1], x[2]
            )
        )

    for k, events in segment_source.items():
        events.emit(segment_events_list[k])

    tbl_ctx.export_csvs(out_path / f"{session_id}")


def graph_flow(segment_events_list, session_id, out_path=Path("artifacts/graphs")):
    graph_ctx = GraphCtx()

    sides = set(k for k in segment_events_list.keys() if "/" not in k)
    segment_source = {k: akayu.Stream() for k in sides}
    segment_events = {k: events.flatten() for k, events in segment_source.items()}

    for k, events in segment_events.items():
        timestamps = events.filter(isinstance, Timestamp)
        rotmats = events.filter(isinstance, Rotmat)
        rotmats_ts = rotmats.combine_latest(timestamps, emit_on=rotmats)
        rotmats_ts = rotmats_ts.collect()

        seg_starts = events.filter(isinstance, SegmentStart).collect()
        seg_stops = events.filter(isinstance, SegmentStop).collect()

        exe_info = events.filter(isinstance, ExerciseInfo)
        exe_info.sink(rotmats_ts.flush)
        exe_info.sink(seg_starts.flush)
        exe_info.sink(seg_stops.flush)

        is_end = exe_info.map(isinstance, Stop)
        rotmats_ts = (
            is_end.zip(rotmats_ts)
            .filter(lambda x: x[0] and len(x[1]) > 0)
            .map(lambda x: x[1])
        )
        seg_starts = is_end.zip(seg_starts).filter(lambda x: x[0]).map(lambda x: x[1])
        seg_stops = is_end.zip(seg_stops).filter(lambda x: x[0]).map(lambda x: x[1])

        exe_stop = exe_info.filter(isinstance, Stop)

        exe_stop.zip(rotmats_ts, seg_starts, seg_stops).sink(
            lambda x, k=k: graph_log_exercise(
                graph_ctx,
                f"/{k}/{format_exeinfo(x[0])}",
                *list(map(np.array, zip(*x[1])))[::-1],
                x[2],
                x[3],
            )
        )

    for k, events in segment_source.items():
        events.emit(segment_events_list[k])

    graph_ctx.export(out_path / session_id)