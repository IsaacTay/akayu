from typing import TypeVar
from types import SimpleNamespace
import re

import numpy as np
from scipy.spatial.transform import Rotation as R
import scipy.signal

import akayu

from .types import *

EULER_ORDER = "xyz"

T = TypeVar("T", Rotmat, Accel, Gyro)

def global_rot(calib: Rotmat, x: T) -> T:
    return calib.T @ x

def local_rot(calib: Rotmat, x: T) -> T:
    if isinstance(x, Rotmat):
        return x @ calib.T
    return calib @ x

def get_steady_rotmat(rotmats: list[Rotmat]) -> Rotmat:
    rotations = R.from_matrix(np.array(rotmats))
    median_vec = np.median(rotations.as_rotvec(), axis=0)
    return Rotmat(R.from_rotvec(median_vec).as_matrix())

def standing_heading_calibration(rotmat_data: list[Rotmat], *args) -> Rotmat:
    rotmat_data = np.array(rotmat_data)
    EULER_ORDER = "yxz"
    heading_calib = rotmat_data[0]
    heading_calib = R.from_matrix(heading_calib).as_euler(EULER_ORDER)
    for i, xyz in enumerate(EULER_ORDER):
        if xyz.lower() == "z":
            continue
        heading_calib[i] = 0
    heading_calib = R.from_euler(EULER_ORDER, heading_calib).as_matrix()
    return Rotmat(heading_calib)

apply_standing_heading_calibration = global_rot

def standing_pose_calibration(rotmat_data: list[Rotmat], *args) -> Rotmat:
    return rotmat_data[len(rotmat_data) // 2]

apply_standing_pose_calibration = local_rot

def rel_rotmat(thigh: Rotmat, shank: Rotmat) -> Rotmat:
    return thigh.T @ shank

def standing_rel_rotmat_calibration(rel_rotmats: list[Rotmat], *args):
    return rel_rotmats[0]

apply_rel_rotmat_calibration = local_rot

def sitstand_calibration(sit_rotmat_data: list[Rotmat], stand_rotmat_data: list[Rotmat], *args) -> Rotmat:
    sit_rotmat = sit_rotmat_data[len(sit_rotmat_data) // 2]
    stand_rotmat = stand_rotmat_data[len(stand_rotmat_data) // 2]
    delta = sit_rotmat @ stand_rotmat.T
    delta_mag = np.degrees(R.from_matrix(delta).magnitude())
    if delta_mag < 20:
        print(f"Not enough rotation between sit and stand: {delta_mag}")
    n = np.arctan2(delta[2, 0], -delta[2, 1])
    calib = R.from_euler('z', n).as_matrix()
    return Rotmat(calib)

apply_sitstand_calibration = global_rot

def get_tilt(rotmats):
    vec = rotmats[:, :, 0]
    return np.arctan2(np.hypot(vec[:, 0], vec[:, 1]), np.abs(vec[:, 2]))

def xy_ignored_calibration2(data):
    data = np.array(data)
    thigh = data[:, 0]
    shank = data[:, 1]

    thigh_angles = get_tilt(thigh)
    shank_angles = get_tilt(shank)

    tilt_angles = thigh_angles + shank_angles
    min_idx = tilt_angles.argmin()
    max_idx = thigh_angles.argmax()

    delta = thigh[max_idx] @ thigh[min_idx].T
    delta_mag = np.degrees(R.from_matrix(delta).magnitude())
    if delta_mag < 20:
        print(f"Not enough rotation between poses of femur in xy_ignored: {delta_mag}")
        return thigh, shank

    rotmats = thigh.mT @ shank
    relrots = R.from_matrix(rotmats)

    order = "xzy"
    angles = relrots.as_euler(order)
    x = angles[:, order.index("x")]
    y = angles[:, order.index("y")]
    z = angles[:, order.index("z")]

    points = np.c_[x, y]
    points = points - points.mean(axis=0)
    _, _, Vt = np.linalg.svd(points, full_matrices=False)
    axis = Vt[0]

    north_angle = np.arctan2(axis[1], axis[0])
    north_calib = R.from_euler("z", north_angle).as_matrix()

    thigh = thigh @ north_calib
    shank = shank @ north_calib

    thigh = list(map(Rotmat, thigh.tolist()))
    shank = list(map(Rotmat, shank.tolist()))

    return thigh, shank

def table_calibration(reference_rotmats: list[Rotmat], sensor_rotmats: list[Rotmat]) -> Rotmat:
    reference_rotmat = reference_rotmats[len(reference_rotmats) // 2]
    sensor_rotmat = sensor_rotmats[len(sensor_rotmats) // 2]
    delta = sensor_rotmat @ reference_rotmat.T
    n = np.arctan2(delta[1, 0], delta[0, 0])
    calib = R.from_euler('z', n).as_matrix()
    return Rotmat(calib)

apply_table_calibration = global_rot

def split_filter(stream, predicate, *args):
    return stream.filter(predicate, *args), stream.filter(lambda *args: not predicate(*args), *args)

def merge_tagged_events(hist, event):
    match event:
        case (k, Timestamp(_)):
            hist[k].append([event[1]])
        case (k, ExerciseInfo(_, _, _)):
            hist[k].append(event[1])
        case (k, _):
            hist[k][-1].append(event[1])

    outs = []
    while True:
        if all(len(h) > 0 and isinstance(h[0], ExerciseInfo) for h in hist.values()):
            for h in hist.values():
                out = h.pop(0)
            outs.append(out)
            continue

        if all(len(h) > 1 or (len(h) > 0 and isinstance(h[0], ExerciseInfo)) for h in hist.values()):
            earliest_ts = None
            earliest_key = None
            for k, h in hist.items():
                if isinstance(h[0], ExerciseInfo):
                    continue
                ts = h[0][0]
                if earliest_ts is None:
                    earliest_ts = ts
                    earliest_key = k
                if ts <= earliest_ts:
                    earliest_ts = ts
                    earliest_key = k
            out = hist[earliest_key].pop(0)
            out = (earliest_key, out)
            outs.append(out)
            continue
        break

    return hist, outs

def batch_events(hist, event, device=""):
    out = []
    match event:
        case Timestamp(ts):
            out = hist
            hist = [Device(device), event]
        case ExerciseInfo(_, _, _):
            hist = []
        case _:
            hist.append(event)
    return hist, out

def interpolate_imu(timestamps, quaternions, gyro, accel, scalar_first=False, target_hz=100):
    if len(timestamps) < 2:
        return timestamps, quaternions, gyro, accel

    start_time = timestamps[0]
    end_time = timestamps[-1]
    timestamps_interp = np.arange(start_time, end_time, 1_000_000 / target_hz)

    try:
        from scipy.spatial.transform import Slerp
        rotations = R.from_quat(quaternions, scalar_first=scalar_first)
        slerp = Slerp(timestamps, rotations)
        rotations_interp = slerp(timestamps_interp)
        quaternions_interp = rotations_interp.as_quat(scalar_first=scalar_first)

        gyro_interp = np.zeros((len(timestamps_interp), 3))
        accel_interp = np.zeros((len(timestamps_interp), 3))

        for i in range(3):
            from scipy import interpolate
            gyro_func = interpolate.interp1d(timestamps, gyro[:, i], kind='cubic', fill_value='extrapolate')
            gyro_interp[:, i] = gyro_func(timestamps_interp)

            accel_func = interpolate.interp1d(timestamps, accel[:, i], kind='cubic', fill_value='extrapolate')
            accel_interp[:, i] = accel_func(timestamps_interp)

        return timestamps_interp, quaternions_interp, gyro_interp, accel_interp

    except Exception as e:
        print(f"Warning: IMU interpolation failed: {e}. Using original data.")
        return timestamps, quaternions, gyro, accel

START_TIME_RE = re.compile(r".*ts(\d+).*")
END_TIME_RE = re.compile(r".*te(\d+).*")
TIMESTAMP_COL = "timestamp_us"

FREQ = 7.5 * 1_000_000
WIDTH = 0.04
DEPTH = 0.07
SOFT_TISSUE_SPEED = 1540
RECV_TIME = DEPTH / SOFT_TISSUE_SPEED

v1_devices = {
    "thigh": "Main",
    "shank": "Base"
}

def flush_segments(segment_events):
    segment_collect = {k: events.collect() for k, events in segment_events.items()}
    segment_events = {}
    def _assign(k, x):
        segment_events[k] = x
    for k, events in segment_collect.items():
        events.sink(lambda x, k: _assign(k, x), k)

    def flush():
        for events in segment_collect.values():
            events.flush()
        return segment_events

    return SimpleNamespace(flush=flush)

def rotmat_to_euler(rotmat: list[Rotmat], order=EULER_ORDER):
    if len(rotmat) == 0:
        return np.array([[0., 0., 0.]]), order
    rotmat = np.array(rotmat)

    try:
        euler = R.from_matrix(rotmat).as_euler(order, degrees=True)
    except Exception as e:
        print(e)
    return euler, order

def format_exeinfo(exe_info: ExerciseInfo):
    if exe_info.addition_data is not None:
        return f"EXE{exe_info.addition_data['Exercise index']}-{exe_info.addition_data['SET index']}-{exe_info.addition_data['RETRY index']}"
    return f"EXE {exe_info.name} - SET {exe_info.set}"

def _rough_segment_points_from_alpha(alpha: np.ndarray, min_distance: int = 15) -> np.ndarray:
    if alpha.shape[0] < 7:
        return np.empty((0, 2), dtype=int)

    if alpha.shape[0] >= 11:
        alpha_smooth = scipy.signal.savgol_filter(alpha, 11, 2)
    else:
        alpha_smooth = alpha

    amp = float(alpha_smooth.max() - alpha_smooth.min())
    if amp <= 1e-6:
        return np.empty((0, 2), dtype=int)

    prominence = max(3.0, 0.18 * amp)
    distance = max(min_distance, 20)
    minima, _ = scipy.signal.find_peaks(-alpha_smooth, prominence=prominence, distance=distance)

    anchors = minima

    if anchors.shape[0] < 2:
        return np.empty((0, 2), dtype=int)

    segment_pts = np.stack([anchors[:-1], anchors[1:]], axis=-1)
    return segment_pts.astype(int)


def _refine_segment_points_with_alpha_velocity(
    segment_pts: np.ndarray,
    alpha: np.ndarray,
    timestamps: np.ndarray,
    broad_duration_s: float = 2.5,
) -> np.ndarray:
    if segment_pts.shape[0] == 0 or alpha.shape[0] < 5:
        return segment_pts

    timestamps_s = (timestamps - timestamps[0]) / 1e6

    if alpha.shape[0] >= 11:
        alpha_smooth = scipy.signal.savgol_filter(alpha, 11, 2)
    else:
        alpha_smooth = alpha

    dalpha_dt = np.gradient(alpha_smooth, timestamps_s)
    if dalpha_dt.shape[0] >= 11:
        dalpha_dt = scipy.signal.savgol_filter(dalpha_dt, 11, 2)

    vel_abs = np.abs(dalpha_dt)
    high_thres = max(2.0, float(np.percentile(vel_abs, 70) * 0.45))
    low_thres = max(0.8, high_thres * 0.45)

    indices = np.arange(alpha.shape[0])

    refined = segment_pts.copy()
    for i, (start, stop) in enumerate(segment_pts):
        if stop <= start:
            continue

        duration_s = timestamps_s[stop] - timestamps_s[start]
        if duration_s < 0.8 or duration_s > 8.0:
            continue

        if duration_s <= broad_duration_s:
            continue

        in_chunk = (indices >= start) & (indices <= stop)
        moving = np.where(in_chunk & (vel_abs >= high_thres))[0]
        if moving.shape[0] < 2:
            continue

        left = int(moving[0])
        right = int(moving[-1])

        while left > start and vel_abs[left - 1] > low_thres:
            left -= 1
        while right < stop and vel_abs[right + 1] > low_thres:
            right += 1

        if right - left >= 2:
            refined[i, 0] = left
            refined[i, 1] = right

    return refined


def _segment_points_from_peaks_and_valleys(alpha: np.ndarray, min_distance: int = 12) -> np.ndarray:
    if alpha.shape[0] < 7:
        return np.empty((0, 2), dtype=int)

    if alpha.shape[0] >= 11:
        alpha_smooth = scipy.signal.savgol_filter(alpha, 11, 2)
    else:
        alpha_smooth = alpha

    amp = float(alpha_smooth.max() - alpha_smooth.min())
    if amp <= 1e-6:
        return np.empty((0, 2), dtype=int)

    peaks, _ = scipy.signal.find_peaks(
        alpha_smooth,
        prominence=max(6.0, 0.20 * amp),
        distance=max(min_distance, 12),
    )
    valleys, _ = scipy.signal.find_peaks(
        -alpha_smooth,
        prominence=max(2.0, 0.08 * amp),
        distance=max(min_distance // 2, 8),
    )

    if peaks.shape[0] == 0 or valleys.shape[0] == 0:
        return np.empty((0, 2), dtype=int)

    segments: list[list[int]] = []
    for peak in peaks:
        left = valleys[valleys < peak]
        right = valleys[valleys > peak]
        if left.shape[0] == 0 or right.shape[0] == 0:
            continue

        start = int(left[-1])
        stop = int(right[0])
        if stop <= start:
            continue
        segments.append([start, stop])

    if len(segments) == 0:
        return np.empty((0, 2), dtype=int)

    segment_pts = np.array(segments, dtype=int)

    unique = [segment_pts[0].tolist()]
    for start, stop in segment_pts[1:]:
        if start != unique[-1][0] or stop != unique[-1][1]:
            unique.append([int(start), int(stop)])
    return np.array(unique, dtype=int)


def _segment_points_from_activity_envelope(alpha: np.ndarray, min_samples: int = 20) -> np.ndarray:
    if alpha.shape[0] < 7:
        return np.empty((0, 2), dtype=int)

    if alpha.shape[0] >= 11:
        alpha_smooth = scipy.signal.savgol_filter(alpha, 11, 2)
    else:
        alpha_smooth = alpha

    amp = float(alpha_smooth.max() - alpha_smooth.min())
    if amp <= 1e-6:
        return np.empty((0, 2), dtype=int)

    center = float(np.median(alpha_smooth))
    envelope = np.abs(alpha_smooth - center)
    high_thres = max(4.0, 0.18 * amp)
    low_thres = max(2.0, 0.08 * amp)

    active = envelope >= high_thres
    if not np.any(active):
        return np.empty((0, 2), dtype=int)

    idx = np.where(active)[0]
    chunks = []
    start = int(idx[0])
    prev = int(idx[0])
    for i in idx[1:]:
        i = int(i)
        if i != prev + 1:
            chunks.append((start, prev))
            start = i
        prev = i
    chunks.append((start, prev))

    segments = []
    for left, right in chunks:
        while left > 0 and envelope[left - 1] >= low_thres:
            left -= 1
        while right < envelope.shape[0] - 1 and envelope[right + 1] >= low_thres:
            right += 1
        if right - left + 1 >= min_samples:
            segments.append([left, right])

    if len(segments) == 0:
        return np.empty((0, 2), dtype=int)
    return np.array(segments, dtype=int)


def _legacy_segment_points_with_gyro(alpha: np.ndarray, feGyro: np.ndarray) -> np.ndarray:
    feGyrX = feGyro[:, 0]
    if feGyrX.shape[0] >= 11:
        feGyrX = scipy.signal.savgol_filter(feGyrX, 11, 2)

    mid_point = (alpha.max() + alpha.min()) / 2
    features = alpha - mid_point

    mask = np.diff(np.sign(features)) != 0
    zero_intersect_peaks = np.abs(np.diff(features))
    zero_intersect_peaks[~mask] = 0
    zero_intersect_pts, _ = scipy.signal.find_peaks(
        zero_intersect_peaks, distance=15
    )
    zero_intersect_pts = np.concatenate(
        [[0], zero_intersect_pts, [features.shape[0] - 1]], axis=-1
    )

    thres = 0.03
    gyr_intersect_pts = np.concatenate(
        (
            np.argwhere(np.diff(np.sign(feGyrX + thres))).flatten(),
            np.argwhere(np.diff(np.sign(feGyrX - thres))).flatten(),
        )
    )
    gyr_intersect_pts.sort()

    if zero_intersect_pts.shape[0] % 2:
        zero_intersect_pts = zero_intersect_pts[:-1]

    segment_pts = zero_intersect_pts.reshape(-1, 2)
    if segment_pts.shape[0] == 0 or gyr_intersect_pts.shape[0] == 0:
        return np.empty((0, 2), dtype=int)

    valid_pts = (segment_pts[:, :1] < gyr_intersect_pts) & (
        gyr_intersect_pts < segment_pts[:, -1:]
    )
    valid_mask = valid_pts.any(axis=-1)

    segment_pts[valid_mask, 0] = np.argmax(valid_pts, axis=-1)[valid_mask]
    segment_pts[valid_mask, 1] = (
        valid_pts.shape[-1]
        - 1
        - np.argmax(valid_pts[..., ::-1], axis=-1)[valid_mask]
    )
    segment_pts[valid_mask, :] = gyr_intersect_pts[segment_pts[valid_mask, :]]

    segment_pts = segment_pts.reshape(-1)[1:-1]
    if segment_pts.shape[0] == 0:
        return np.empty((0, 2), dtype=int)

    return segment_pts.reshape(-1, 2).astype(int)


def _segment_points_from_sts_valleys(
    alpha: np.ndarray,
    timestamps: np.ndarray,
    min_distance: int = 20,
) -> np.ndarray:
    if alpha.shape[0] < 7:
        return np.empty((0, 2), dtype=int)

    if alpha.shape[0] >= 11:
        alpha_smooth = scipy.signal.savgol_filter(alpha, 11, 2)
    else:
        alpha_smooth = alpha

    amp = float(alpha_smooth.max() - alpha_smooth.min())
    if amp <= 1e-6:
        return np.empty((0, 2), dtype=int)

    timestamps_s = (timestamps - timestamps[0]) / 1e6
    dalpha_dt = np.gradient(alpha_smooth, timestamps_s)
    if dalpha_dt.shape[0] >= 11:
        dalpha_dt = scipy.signal.savgol_filter(dalpha_dt, 11, 2)

    standing_level = float(np.percentile(alpha_smooth, 80))
    standing_band = max(4.0, 0.12 * amp)
    plateau_vel_thres = max(1.2, float(np.percentile(np.abs(dalpha_dt), 55) * 0.6))
    peak_prominence = max(3.0, 0.10 * amp)
    valley_prominence = max(4.0, 0.18 * amp)

    peaks, _ = scipy.signal.find_peaks(
        alpha_smooth,
        prominence=peak_prominence,
        distance=max(min_distance // 2, 8),
    )
    valleys, _ = scipy.signal.find_peaks(
        -alpha_smooth,
        prominence=valley_prominence,
        distance=max(min_distance, 20),
    )
    if valleys.shape[0] == 0:
        return np.empty((0, 2), dtype=int)

    at_upright = alpha_smooth >= (standing_level - standing_band)

    segments: list[list[int]] = []
    for valley in valleys:
        left_peaks = peaks[peaks < valley]
        right_peaks = peaks[peaks > valley]

        if left_peaks.shape[0] > 0:
            start = int(left_peaks[-1])
        else:
            pre = np.flatnonzero(at_upright[:valley])
            if pre.size == 0:
                continue
            start = int(pre[-1])

        if right_peaks.shape[0] > 0:
            stop = int(right_peaks[0])
        else:
            post = np.flatnonzero(at_upright[valley + 1:])
            if post.size == 0:
                continue
            stop = int(valley + 1 + post[0])

        if stop <= start:
            continue

        while (
            start > 0
            and alpha_smooth[start - 1] >= standing_level - standing_band
            and abs(dalpha_dt[start - 1]) <= plateau_vel_thres
        ):
            start -= 1

        while (
            stop < alpha_smooth.shape[0] - 1
            and alpha_smooth[stop + 1] >= standing_level - standing_band
            and abs(dalpha_dt[stop + 1]) <= plateau_vel_thres
        ):
            stop += 1

        if alpha_smooth[start] < standing_level - standing_band:
            continue
        if alpha_smooth[stop] < standing_level - standing_band:
            continue

        if segments:
            prev_start, prev_stop = segments[-1]
            if start == prev_start and stop == prev_stop:
                continue
            if start < prev_stop:
                start = prev_stop
                if stop <= start:
                    continue

        segments.append([start, stop])

    if len(segments) == 0:
        return np.empty((0, 2), dtype=int)

    return np.array(segments, dtype=int)


def _segment_points_to_events(segment_pts: np.ndarray, timestamps: np.ndarray):
    segments = []
    for i, (start, stop) in enumerate(timestamps[segment_pts]):
        segments.append(Timestamp(start))
        segments.append(SegmentStart(int(start), i))
        segments.append(Timestamp(stop))
        segments.append(SegmentStop(int(stop), i))
    return segments


def _phase_points_to_events(phase_pts: list[tuple[int, int, str, int]], timestamps: np.ndarray):
    segments = []
    for i, (start_idx, stop_idx, phase, cycle_idx) in enumerate(phase_pts):
        start = int(timestamps[start_idx])
        stop = int(timestamps[stop_idx])
        meta = {"kind": "phase", "phase": phase, "cycle": int(cycle_idx)}
        segments.append(Timestamp(start))
        segments.append(SegmentStart(start, i, meta))
        segments.append(Timestamp(stop))
        segments.append(SegmentStop(stop, i, meta))
    return segments


def _cycle_points_to_events(segment_pts: np.ndarray, timestamps: np.ndarray):
    segments = []
    for i, (start, stop) in enumerate(timestamps[segment_pts]):
        meta = {"kind": "cycle", "cycle": int(i)}
        segments.append(Timestamp(int(start)))
        segments.append(SegmentStart(int(start), i, meta))
        segments.append(Timestamp(int(stop)))
        segments.append(SegmentStop(int(stop), i, meta))
    return segments


def _pick_primary_motion_axis(eulers: np.ndarray, order: str) -> int:
    robust_amp = np.percentile(eulers, 95, axis=0) - np.percentile(eulers, 5, axis=0)
    if np.allclose(robust_amp, 0.0):
        return order.index("x") if "x" in order else 0
    return int(np.argmax(robust_amp))


def _print_segmentation_comparison(
    timestamps: np.ndarray,
    segment_pts_new: np.ndarray,
    segment_pts_legacy: np.ndarray,
):
    new_count = int(segment_pts_new.shape[0])
    legacy_count = int(segment_pts_legacy.shape[0])

    print(f"[segmentation] reps new={new_count} legacy={legacy_count}")
    if new_count == 0 or legacy_count == 0:
        return

    n = min(new_count, legacy_count)
    new_ts = timestamps[segment_pts_new[:n]]
    legacy_ts = timestamps[segment_pts_legacy[:n]]

    start_err_s = np.abs(new_ts[:, 0] - legacy_ts[:, 0]) / 1e6
    stop_err_s = np.abs(new_ts[:, 1] - legacy_ts[:, 1]) / 1e6
    print(
        "[segmentation] median |start_err|={:.3f}s median |stop_err|={:.3f}s".format(
            float(np.median(start_err_s)), float(np.median(stop_err_s))
        )
    )


def _peak_points_from_segments(alpha: np.ndarray, segment_pts: np.ndarray) -> np.ndarray:
    if segment_pts.shape[0] == 0:
        return np.empty((0,), dtype=int)
    peaks = np.empty(segment_pts.shape[0], dtype=int)
    for i, (start, stop) in enumerate(segment_pts):
        start = int(start)
        stop = int(stop)
        peaks[i] = start + int(np.argmin(alpha[start:stop + 1]))
    return peaks


def _peak_points_to_events(peak_pts: np.ndarray, timestamps: np.ndarray):
    events = []
    for i, idx in enumerate(peak_pts):
        t = int(timestamps[int(idx)])
        meta = {"kind": "peak", "cycle": int(i)}
        events.append(Timestamp(t))
        events.append(SegmentStart(t, i, meta))
        events.append(Timestamp(t))
        events.append(SegmentStop(t, i, meta))
    return events


def segment_sit_to_stand(rotmat_ts: list[tuple[Rotmat, Timestamp]]):
    if len(rotmat_ts) == 0:
        return []

    relrots, timestamps = zip(*rotmat_ts)
    relrots = np.array(relrots)
    timestamps = np.array(timestamps)
    eulers, order = rotmat_to_euler(relrots)
    alpha = eulers[:, order.index("x")]

    segment_pts = _segment_points_from_sts_valleys(alpha, timestamps)
    if segment_pts.shape[0] == 0:
        segment_pts = _rough_segment_points_from_alpha(alpha)
        segment_pts = _refine_segment_points_with_alpha_velocity(segment_pts, alpha, timestamps)

    import os
    use_legacy = os.getenv("STS_SEGMENT_USE_LEGACY", "0") == "1"
    compare_legacy = os.getenv("STS_SEGMENT_COMPARE", "0") == "1"

    if use_legacy or compare_legacy:
        feGyro = np.zeros(len(relrots))
        legacy_pts = _legacy_segment_points_with_gyro(alpha, feGyro)
        if compare_legacy:
            _print_segmentation_comparison(timestamps, segment_pts, legacy_pts)
        if use_legacy:
            segment_pts = legacy_pts

    if segment_pts.shape[0] == 0:
        print("No segments found for sit to stand")
        return []

    peak_pts = _peak_points_from_segments(alpha, segment_pts)
    return _segment_points_to_events(segment_pts, timestamps) + _peak_points_to_events(peak_pts, timestamps)


def segment_active_flexion(rotmat_ts: list[tuple[Rotmat, Timestamp]]):
    if len(rotmat_ts) == 0:
        return []

    relrots, timestamps = zip(*rotmat_ts)
    relrots = np.array(relrots)
    timestamps = np.array(timestamps)

    eulers, order = rotmat_to_euler(relrots)
    axis_idx = _pick_primary_motion_axis(eulers, order)
    alpha = eulers[:, axis_idx]

    segment_pts = _segment_points_from_activity_envelope(alpha, min_samples=18)
    if segment_pts.shape[0] == 0:
        segment_pts = _segment_points_from_peaks_and_valleys(alpha, min_distance=12)
    if segment_pts.shape[0] == 0:
        segment_pts = _rough_segment_points_from_alpha(alpha, min_distance=12)

    segment_pts = _refine_segment_points_with_alpha_velocity(
        segment_pts,
        alpha,
        timestamps,
        broad_duration_s=1.2,
    )

    if segment_pts.shape[0] == 0:
        print("No segments found for active flexion")
        return []

    return _segment_points_to_events(segment_pts, timestamps)


def _lowpass_signal(signal: np.ndarray, fs: float, cutoff: float, order: int = 2) -> np.ndarray:
    nyquist = fs / 2.0
    normalized_cutoff = cutoff / nyquist if nyquist > 0 else 0.0
    if signal.shape[0] < 8 or normalized_cutoff <= 0.0 or normalized_cutoff >= 1.0:
        return signal

    b, a = scipy.signal.butter(order, normalized_cutoff, btype="low")
    return scipy.signal.filtfilt(b, a, signal)


def _find_prominent_peaks(signal: np.ndarray, fs: float, min_spacing_s: float = 0.35) -> np.ndarray:
    if signal.shape[0] < 5:
        return np.empty(0, dtype=int)

    amp = float(signal.max() - signal.min())
    if amp <= 1e-6:
        return np.empty(0, dtype=int)

    min_distance = max(18, int(fs * min_spacing_s))
    prominence = max(0.15 * amp, np.percentile(np.abs(signal), 60) * 0.35)
    peaks, _ = scipy.signal.find_peaks(signal, prominence=prominence, distance=min_distance)
    return peaks.astype(int)


def _detect_heel_strike_candidates(
    accel_mag_dyn: np.ndarray,
    swing_peaks: np.ndarray,
    fs: float,
) -> np.ndarray:
    if accel_mag_dyn.shape[0] < 5 or swing_peaks.shape[0] == 0:
        return np.empty(0, dtype=int)

    amp = float(accel_mag_dyn.max() - accel_mag_dyn.min())
    prominence = max(0.10 * amp, np.percentile(np.abs(accel_mag_dyn), 65) * 0.25)
    min_distance = max(18, int(fs * 0.35))
    accel_peaks, _ = scipy.signal.find_peaks(
        accel_mag_dyn,
        prominence=prominence,
        distance=min_distance,
    )

    if accel_peaks.shape[0] == 0:
        return np.empty(0, dtype=int)

    valid_hs = []
    min_lag = int(fs * 0.05)
    max_lag = int(fs * 0.45)
    for peak in accel_peaks:
        prev_swings = swing_peaks[swing_peaks < peak]
        if prev_swings.shape[0] == 0:
            continue
        lag = int(peak - prev_swings[-1])
        if min_lag <= lag <= max_lag:
            valid_hs.append(int(peak))

    return np.array(valid_hs, dtype=int)


def _find_mid_swing_peaks(signal: np.ndarray, fs: float) -> np.ndarray:
    if signal.shape[0] < 10:
        return np.empty(0, dtype=int)

    q10, q50, q90 = np.percentile(signal, [10, 50, 90])
    robust_amp = float(q90 - q10)
    if robust_amp <= 1e-6:
        return np.empty(0, dtype=int)

    height = q50 + 0.25 * robust_amp
    prominence = 0.3 * robust_amp
    distance = max(8, int(fs * 0.3))

    peaks, _ = scipy.signal.find_peaks(
        signal,
        height=height,
        prominence=prominence,
        distance=distance,
    )
    return peaks.astype(int)


def _estimate_gravity_dir(accel: np.ndarray, walking_slice: slice) -> np.ndarray | None:
    if accel is None:
        return None
    window = accel[walking_slice]
    if window.shape[0] < 10:
        return None
    mean_accel = np.mean(window, axis=0)
    norm = float(np.linalg.norm(mean_accel))
    if norm < 1e-3:
        return None
    return mean_accel / norm


def _horizontal_gyro_mag(gyro: np.ndarray, gravity_dir: np.ndarray) -> np.ndarray:
    longitudinal = gyro @ gravity_dir
    horizontal = gyro - np.outer(longitudinal, gravity_dir)
    return np.linalg.norm(horizontal, axis=1)


def _find_heel_strike_candidates(accel: np.ndarray, fs: float, walking_slice: slice) -> np.ndarray:
    if accel is None or accel.shape[0] < 10:
        return np.empty(0, dtype=int)

    accel_mag = np.linalg.norm(accel, axis=1)
    nyquist = fs / 2.0
    low, high = 3.0, min(15.0, 0.45 * fs)
    if high <= low or nyquist <= low:
        return np.empty(0, dtype=int)

    b, a = scipy.signal.butter(2, [low / nyquist, high / nyquist], btype="band")
    accel_bp = scipy.signal.filtfilt(b, a, accel_mag)
    jerk = np.abs(np.gradient(accel_bp)) * fs

    jerk_win = jerk[walking_slice]
    if jerk_win.shape[0] < 5:
        return np.empty(0, dtype=int)

    q50, q95 = np.percentile(jerk_win, [50, 95])
    robust_amp = float(q95 - q50)
    if robust_amp <= 1e-6:
        return np.empty(0, dtype=int)

    peaks, _ = scipy.signal.find_peaks(
        jerk,
        height=q50 + 0.4 * robust_amp,
        prominence=0.3 * robust_amp,
        distance=max(8, int(fs * 0.5)),
    )
    mask = (peaks >= walking_slice.start) & (peaks < (walking_slice.stop or len(jerk)))
    return peaks[mask].astype(int)


def _merge_anchor_families(
    primary: np.ndarray,
    secondary: np.ndarray,
    fs: float,
    min_abs_lag_s: float = 0.1,
    max_abs_lag_s: float = 0.5,
) -> np.ndarray:
    if primary.shape[0] < 2 or secondary.shape[0] == 0:
        return primary

    min_lag = int(fs * min_abs_lag_s)
    max_lag = int(fs * max_abs_lag_s)

    offsets: list[int] = []
    for p in primary:
        diffs = secondary.astype(int) - int(p)
        mask = (np.abs(diffs) >= min_lag) & (np.abs(diffs) <= max_lag)
        if np.any(mask):
            candidates = diffs[mask]
            nearest = candidates[int(np.argmin(np.abs(candidates)))]
            offsets.append(int(nearest))

    if len(offsets) < 2:
        return primary

    median_offset = int(np.median(offsets))
    median_interval = int(np.median(np.diff(primary)))
    gap_threshold = int(1.5 * median_interval)
    separation = max(1, int(0.5 * median_interval))

    merged = list(map(int, primary))
    intervals = np.diff(primary)
    for i, interval in enumerate(intervals):
        if interval <= gap_threshold:
            continue

        gap_start = int(primary[i])
        gap_stop = int(primary[i + 1])
        candidates = secondary[
            (secondary > gap_start + median_offset)
            & (secondary < gap_stop + median_offset)
        ]
        for s in candidates:
            back_shifted = int(s) - median_offset
            if all(abs(back_shifted - m) > separation for m in merged):
                merged.append(back_shifted)

    return np.array(sorted(merged), dtype=int)


def _detect_toe_off(
    gyro_mag: np.ndarray,
    gyro_axis: np.ndarray,
    cycle_start: int,
    cycle_stop: int,
) -> int | None:
    if cycle_stop - cycle_start < 8:
        return None

    local_abs = np.abs(gyro_axis[cycle_start:cycle_stop + 1])
    if local_abs.shape[0] < 5:
        return None

    swing_rel = int(np.argmax(local_abs))
    swing_idx = cycle_start + swing_rel
    if swing_idx <= cycle_start + 2:
        return None

    search_start = cycle_start + max(2, int(0.05 * (cycle_stop - cycle_start)))
    search_stop = max(search_start + 1, swing_idx - 1)
    pre_swing = gyro_mag[search_start:search_stop + 1]
    if pre_swing.shape[0] < 3:
        return None

    minima, _ = scipy.signal.find_peaks(-pre_swing, distance=max(3, pre_swing.shape[0] // 5))
    if minima.shape[0] > 0:
        return int(search_start + minima[-1])

    return int(search_start + np.argmin(pre_swing))


def _detect_walking_phase(gyro_signal, window_size=50):
    if len(gyro_signal) < window_size:
        return 0, len(gyro_signal) - 1

    half_window = window_size // 2
    moving_std = np.array([
        np.std(gyro_signal[max(0, i-half_window):min(len(gyro_signal), i+half_window)])
        for i in range(len(gyro_signal))
    ])

    smooth_window = min(11, len(moving_std)//2*2+1)
    if smooth_window > 2:
        try:
            smoothed_std = scipy.signal.savgol_filter(moving_std, smooth_window, 2)
        except:
            smoothed_std = moving_std
    else:
        smoothed_std = moving_std

    max_activity_idx = np.argmax(smoothed_std)
    max_activity = smoothed_std[max_activity_idx]

    if max_activity == 0:
        return 0, len(gyro_signal) - 1

    def _fill_small_gaps(mask: np.ndarray, max_gap: int) -> np.ndarray:
        if mask.size == 0 or max_gap <= 0:
            return mask

        filled = mask.copy()
        idx = 0
        n = mask.size
        while idx < n:
            if filled[idx]:
                idx += 1
                continue

            gap_start = idx
            while idx < n and not filled[idx]:
                idx += 1
            gap_stop = idx

            left_active = gap_start > 0 and filled[gap_start - 1]
            right_active = gap_stop < n and filled[gap_stop]
            if left_active and right_active and (gap_stop - gap_start) <= max_gap:
                filled[gap_start:gap_stop] = True
        return filled

    def _active_runs(mask: np.ndarray) -> list[tuple[int, int]]:
        if mask.size == 0:
            return []
        padded = np.pad(mask.astype(np.int8), (1, 1))
        transitions = np.diff(padded)
        starts = np.flatnonzero(transitions == 1)
        stops = np.flatnonzero(transitions == -1) - 1
        return [(int(start), int(stop)) for start, stop in zip(starts, stops) if stop >= start]

    high_threshold = max_activity * 0.45
    low_threshold = max_activity * 0.20
    high_mask = _fill_small_gaps(smoothed_std >= high_threshold, max(3, window_size // 3))
    low_mask = _fill_small_gaps(smoothed_std >= low_threshold, max(5, window_size))

    runs = _active_runs(low_mask)
    if len(runs) == 0:
        return 0, len(gyro_signal) - 1

    scored_runs = []
    for start, stop in runs:
        run_high = high_mask[start:stop + 1]
        if not np.any(run_high):
            continue

        duration = stop - start + 1
        mean_activity = float(np.mean(smoothed_std[start:stop + 1]))
        contains_peak = start <= max_activity_idx <= stop
        score = duration * (mean_activity + 1e-6)
        if contains_peak:
            score *= 1.25
        scored_runs.append((score, start, stop))

    if len(scored_runs) == 0:
        scored_runs = [
            ((stop - start + 1) * (float(np.mean(smoothed_std[start:stop + 1])) + 1e-6), start, stop)
            for start, stop in runs
        ]

    _, walking_start, walking_end = max(scored_runs, key=lambda x: x[0])

    if walking_end <= walking_start:
        walking_start = max(0, max_activity_idx - window_size)
        walking_end = min(len(gyro_signal) - 1, max_activity_idx + window_size)

    return int(walking_start), int(walking_end)


def segment_walking(rotmat_gyro_ts: list[tuple], remove_initial_jerk=True):
    if len(rotmat_gyro_ts) == 0:
        return []

    sample_width = len(rotmat_gyro_ts[0])
    if sample_width == 4:
        _, tibia_gyro, tibia_accel, timestamps = zip(*rotmat_gyro_ts)
        tibia_accel = np.array(tibia_accel)
    elif sample_width == 3:
        _, tibia_gyro, timestamps = zip(*rotmat_gyro_ts)
        tibia_accel = None
    else:
        raise ValueError(f"Unexpected walking sample width: {sample_width}")

    tibia_gyro = np.array(tibia_gyro)
    timestamps = np.array(timestamps)

    fs = 100.0
    if timestamps.shape[0] >= 2:
        dt_s = float(np.median(np.diff(timestamps))) / 1e6
        if dt_s > 1e-6:
            fs = float(np.clip(1.0 / dt_s, 20.0, 200.0))

    gyro_mag = np.linalg.norm(tibia_gyro, axis=1)
    gyro_mag_filtered = _lowpass_signal(gyro_mag, fs, cutoff=4.0)

    walking_start, walking_end = _detect_walking_phase(gyro_mag_filtered, window_size=max(25, int(fs * 0.5)))
    walking_slice = slice(walking_start, walking_end + 1)
    walking_gyro = tibia_gyro[walking_slice]
    walking_gyro_mag = gyro_mag_filtered[walking_slice]

    if walking_gyro.shape[0] < max(20, int(fs * 0.4)):
        print("Walking phase too short in walking test")
        return []

    gravity_dir = _estimate_gravity_dir(tibia_accel, walking_slice)
    if gravity_dir is not None:
        horizontal_mag = _horizontal_gyro_mag(tibia_gyro, gravity_dir)
        walking_signal_full = _lowpass_signal(horizontal_mag, fs, cutoff=4.0)
    else:
        robust_amp = np.percentile(walking_gyro, 95, axis=0) - np.percentile(walking_gyro, 5, axis=0)
        axis_idx = int(np.argmax(robust_amp))
        walking_signal_full = _lowpass_signal(np.abs(tibia_gyro[:, axis_idx]), fs, cutoff=4.0)

    walking_signal = walking_signal_full[walking_slice]
    amp = float(walking_signal.max() - walking_signal.min())
    if amp <= 1e-6 or np.allclose(walking_gyro_mag, 0.0):
        print("No gait cycles detected in walking test")
        return []

    gyro_peaks = _find_mid_swing_peaks(walking_signal, fs)
    if gyro_peaks.shape[0] < 2:
        print("Not enough gait cycles detected in walking test")
        return []

    if remove_initial_jerk and gyro_peaks.shape[0] > 2:
        early_limit = int(walking_signal.shape[0] * 0.15)
        if gyro_peaks[0] < early_limit:
            gyro_peaks = gyro_peaks[1:]

    if gyro_peaks.shape[0] < 2:
        print("Not enough gait cycles detected in walking test")
        return []

    gyro_peaks = gyro_peaks + walking_start

    heel_strikes = np.empty(0, dtype=int)
    if tibia_accel is not None:
        heel_strikes = _find_heel_strike_candidates(tibia_accel, fs, walking_slice)

    if heel_strikes.shape[0] >= max(2, int(0.6 * gyro_peaks.shape[0])):
        anchors = _merge_anchor_families(heel_strikes, gyro_peaks, fs)
    else:
        anchors = _merge_anchor_families(gyro_peaks, heel_strikes, fs)

    cycle_pts: list[tuple[int, int]] = []
    phase_pts: list[tuple[int, int, str, int]] = []
    for cycle_idx, (cycle_start, cycle_stop) in enumerate(zip(anchors[:-1], anchors[1:])):
        cycle_start = int(cycle_start)
        cycle_stop = int(cycle_stop)
        if cycle_stop - cycle_start < max(12, int(fs * 0.2)):
            continue

        toe_off = _detect_toe_off(gyro_mag_filtered, walking_signal_full, cycle_start, cycle_stop)
        if toe_off is None:
            continue

        toe_off = int(np.clip(toe_off, cycle_start + 1, cycle_stop - 1))
        if toe_off <= cycle_start or toe_off >= cycle_stop:
            continue

        cycle_pts.append((cycle_start, cycle_stop))
        phase_pts.append((cycle_start, toe_off, "stance", cycle_idx))
        phase_pts.append((toe_off, cycle_stop, "swing", cycle_idx))

    if len(cycle_pts) == 0 or len(phase_pts) == 0:
        print("No stance/swing phases detected in walking test")
        return []

    cycle_events = _cycle_points_to_events(np.array(cycle_pts, dtype=int), timestamps)
    phase_events = _phase_points_to_events(phase_pts, timestamps)
    return cycle_events + phase_events