from ..types import *
from ..utils import *

import numpy as np

import matplotlib

matplotlib.use("agg")
import matplotlib.pyplot as plt

from pathlib import Path


class GraphCtx:
    def __init__(self):
        self.data = {}

    def log(self, key: str, timestamps, euler, order, segment_data):
        self.data.setdefault(key, []).append((timestamps, euler, order, segment_data))

    def export(self, out_path: Path):
        out_path.mkdir(exist_ok=True, parents=True)

        for key, entries in self.data.items():
            for i, (timestamps, euler, order, segment_data) in enumerate(entries):
                ts = (timestamps - timestamps[0]) / 1e6

                fig, axes = plt.subplots(
                    len(order), 1, sharex=True, figsize=(12, 3 * len(order))
                )
                if len(order) == 1:
                    axes = [axes]

                for ax, axis_label, col in zip(axes, order, range(euler.shape[1])):
                    ax.plot(ts, euler[:, col], linewidth=0.8)
                    ax.set_ylabel(f"{axis_label} (deg)")

                    for start, stop in segment_data["phase_stance"]:
                        start_s = (start - timestamps[0]) / 1e6
                        stop_s = (stop - timestamps[0]) / 1e6
                        ax.axvspan(start_s, stop_s, color="tab:purple", alpha=0.22, label="stance")
                    for t in seg_starts:
                        t_sec = (t - timestamps[0]) / 1e6
                        ax.axvline(
                            t_sec,
                            color="green",
                            linestyle="--",
                            alpha=0.7,
                            label="seg start",
                        )

                    for start, stop in segment_data["phase_swing"]:
                        start_s = (start - timestamps[0]) / 1e6
                        stop_s = (stop - timestamps[0]) / 1e6
                        ax.axvspan(start_s, stop_s, color="tab:orange", alpha=0.18, label="swing")

                    for t in segment_data["cycle_starts"]:
                        t_sec = (t - timestamps[0]) / 1e6
                        ax.axvline(t_sec, color="green", linestyle="--", alpha=0.85, label="cycle start")

                    for t in segment_data["cycle_stops"]:
                        t_sec = (t - timestamps[0]) / 1e6
                        ax.axvline(t_sec, color="red", linestyle="--", alpha=0.85, label="cycle stop")

                    for t in segment_data["peaks"]:
                        t_sec = (t - timestamps[0]) / 1e6
                        ax.axvline(t_sec, color="blue", linestyle="-.", alpha=0.85, label="peak")

                    for t in segment_data["phase_boundaries"]:
                        t_sec = (t - timestamps[0]) / 1e6
                        ax.axvline(t_sec, color="tab:orange", linestyle=":", alpha=0.65, label="phase boundary")
                        ax.axvline(
                            t_sec,
                            color="red",
                            linestyle="--",
                            alpha=0.7,
                            label="seg stop",
                        )

                axes[-1].set_xlabel("Time (s)")

                handles, labels = axes[0].get_legend_handles_labels()
                by_label = dict(zip(labels, handles))
                axes[0].legend(by_label.values(), by_label.keys(), loc="upper right")

                safe_key = key.strip("/").replace("/", "_")
                fig.suptitle(f"{key} rep {i}")
                fig.tight_layout()
                fig.savefig(out_path / f"{safe_key}_{i}.png", dpi=150)
                plt.close(fig)


def _segment_kind(segment) -> str:
    meta = getattr(segment, "addition_data", None) or {}
    return meta.get("kind", "generic")


def _segment_phase(segment) -> str | None:
    meta = getattr(segment, "addition_data", None) or {}
    return meta.get("phase")


def _build_segment_data(segment_starts, segment_stops):
    segment_data = {
        "cycles": [],
        "cycle_starts": [],
        "cycle_stops": [],
        "peaks": [],
        "phase_stance": [],
        "phase_swing": [],
        "phase_boundaries": [],
    }

    for start, stop in zip(segment_starts, segment_stops):
        kind = _segment_kind(start)
        if kind != _segment_kind(stop):
            continue

        if kind == "cycle":
            segment_data["cycles"].append((start.timestamp, stop.timestamp))
            segment_data["cycle_starts"].append(start.timestamp)
            segment_data["cycle_stops"].append(stop.timestamp)
            continue

        if kind == "peak":
            segment_data["peaks"].append(start.timestamp)
            continue

        if kind == "phase":
            phase = _segment_phase(start)
            if phase == "stance":
                segment_data["phase_stance"].append((start.timestamp, stop.timestamp))
            elif phase == "swing":
                segment_data["phase_swing"].append((start.timestamp, stop.timestamp))
            segment_data["phase_boundaries"].append(start.timestamp)
            segment_data["phase_boundaries"].append(stop.timestamp)
            continue

        segment_data["cycle_starts"].append(start.timestamp)
        segment_data["cycle_stops"].append(stop.timestamp)

    segment_data["phase_boundaries"] = sorted(set(segment_data["phase_boundaries"]))
    return segment_data


def graph_log_exercise(graph_ctx: GraphCtx, key: str, timestamps, rotmats, segment_starts, segment_stops):
    timestamps = np.array(timestamps)
    euler, order = rotmat_to_euler(rotmats)
    segment_data = _build_segment_data(segment_starts, segment_stops)
    graph_ctx.log(key, timestamps, euler, order, segment_data)
def graph_log_exercise(
    graph_ctx: GraphCtx, key: str, timestamps, rotmats, segment_starts, segment_stops
):
    timestamps = np.array(timestamps)
    euler, order = rotmat_to_euler(rotmats)
    segment_starts = np.array(segment_starts)
    segment_stops = np.array(segment_stops)
    graph_ctx.log(key, timestamps, euler, order, segment_starts, segment_stops)


def _split_sts_curve(
    flexion: np.ndarray, rotation: np.ndarray
) -> tuple[tuple[np.ndarray, np.ndarray], tuple[np.ndarray, np.ndarray]]:
    n = min(flexion.shape[0], rotation.shape[0])
    if n < 3:
        return (np.array([]), np.array([])), (np.array([]), np.array([]))

    flexion = flexion[:n]
    rotation = rotation[:n]

    split_idx = int(np.argmin(np.abs(flexion)))
    if split_idx <= 0 or split_idx >= n - 1:
        split_idx = n // 2

    sit_to_stand = (flexion[: split_idx + 1], rotation[: split_idx + 1])
    stand_to_sit = (flexion[split_idx:], rotation[split_idx:])
    return sit_to_stand, stand_to_sit


def _flexion_axis_transform(angle_deg: np.ndarray) -> np.ndarray:
    a = np.asarray(angle_deg, dtype=float)
    a = np.clip(a, 0.0, 70.0)

    out = np.zeros_like(a)
    ranges = [
        (70.0, 60.0, 0.5),
        (60.0, 40.0, 1.0),
        (40.0, 20.0, 2.0),
        (20.0, 0.0, 1.0),
    ]

    offset = 0.0
    for hi, lo, scale in ranges:
        width = (hi - lo) * scale
        mask = (a <= hi) & (a >= lo)
        out[mask] = offset + (hi - a[mask]) * scale
        offset += width
    return out


def _map_curve_to_display(
    values: np.ndarray,
    src_start: float,
    src_end: float,
    dst_start: float,
    dst_end: float,
) -> np.ndarray:
    values = np.asarray(values, dtype=float)
    denom = src_end - src_start
    if np.isclose(denom, 0.0):
        return np.full(values.shape, (dst_start + dst_end) / 2.0, dtype=float)
    out = dst_start + (values - src_start) * (dst_end - dst_start) / denom
    return np.clip(out, 0.0, 70.0)


def render_rotational_kinematics_plot(
    out_path: Path,
    left_flexion,
    left_rotation,
    right_flexion,
    right_rotation,
) -> Path | None:
    if any(
        x is None for x in (left_flexion, left_rotation, right_flexion, right_rotation)
    ):
        return None

    left_flexion = np.asarray(left_flexion, dtype=float)
    left_rotation = np.asarray(left_rotation, dtype=float)
    right_flexion = np.asarray(right_flexion, dtype=float)
    right_rotation = np.asarray(right_rotation, dtype=float)

    (l_sts_x, l_sts_y), (l_stsi_x, l_stsi_y) = _split_sts_curve(
        left_flexion, left_rotation
    )
    (r_sts_x, r_sts_y), (r_stsi_x, r_stsi_y) = _split_sts_curve(
        right_flexion, right_rotation
    )

    if (
        l_sts_x.size == 0
        or l_stsi_x.size == 0
        or r_sts_x.size == 0
        or r_stsi_x.size == 0
    ):
        return None

    out_path.parent.mkdir(exist_ok=True, parents=True)

    fig, axes = plt.subplots(1, 2, figsize=(11.0, 4.2), sharey=True)
    phase_data = [
        (axes[0], "Sit to Stand", (l_sts_x, l_sts_y), (r_sts_x, r_sts_y), (70.0, 0.0)),
        (
            axes[1],
            "Stand to Sit",
            (l_stsi_x, l_stsi_y),
            (r_stsi_x, r_stsi_y),
            (70.0, 0.0),
        ),
    ]
    tick_angles = np.array([70, 60, 50, 40, 30, 20, 10, 0], dtype=float)
    tick_positions = _flexion_axis_transform(tick_angles)
    x_left = float(_flexion_axis_transform(np.array([70.0]))[0])
    x_right = float(_flexion_axis_transform(np.array([0.0]))[0])
    te_start = float(_flexion_axis_transform(np.array([40.0]))[0])
    te_stop = float(_flexion_axis_transform(np.array([20.0]))[0])
    te_center = float(_flexion_axis_transform(np.array([30.0]))[0])

    for ax, title, left_curve, right_curve, dst_range in phase_data:
        src_start = float(np.median([left_curve[0][0], right_curve[0][0]]))
        src_end = float(np.median([left_curve[0][-1], right_curve[0][-1]]))

        left_display = _map_curve_to_display(
            left_curve[0], src_start, src_end, dst_range[0], dst_range[1]
        )
        right_display = _map_curve_to_display(
            right_curve[0], src_start, src_end, dst_range[0], dst_range[1]
        )

        left_x = _flexion_axis_transform(left_display)
        right_x = _flexion_axis_transform(right_display)

        ax.axvspan(te_start, te_stop, color="#d9ecff", alpha=0.75, zorder=0)
        ax.plot(left_x, left_curve[1], color="#1f77b4", linewidth=2.0, label="Left")
        ax.plot(
            right_x,
            right_curve[1],
            color="#d62728",
            linewidth=2.0,
            label="Right",
        )
        ax.set_title(title)
        ax.set_xlabel("Flexion angle (deg)")
        ax.grid(True, alpha=0.25)
        ax.set_xlim(x_left, x_right)
        ax.set_xticks(tick_positions)
        ax.set_xticklabels([str(int(x)) for x in tick_angles])
        ax.set_ylim(-1, 3)
        ax.text(
            te_center,
            2.86,
            "Terminal Extension",
            ha="center",
            va="bottom",
            fontsize=8,
            color="#2f5f89",
        )

    axes[0].set_ylabel("Total rotation ROM (deg)")
    axes[1].legend(loc="best", frameon=True)

    fig.tight_layout()
    fig.savefig(out_path, dpi=180)
    plt.close(fig)
    return out_path