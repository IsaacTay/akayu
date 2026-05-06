from ..types import *
from ..utils import *

from pathlib import Path

import numpy as np

import json
import typst

import scipy.interpolate


class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super().default(obj)


CALC_KEY = "_calc"


class TypstCtx:
    def __init__(self):
        self.data = {}

    def log(self, namespace: str, ts, val):
        self.data.setdefault(namespace.lower() + "/raw", []).append((ts, val))

    def set_field(self, key: str, val):
        self.data[key] = val

    def _calculate(self):
        keys = set(
            "/".join(k.split("/")[:-1]) for k in self.data if f"/{CALC_KEY}/" not in k
        )

        for key in keys:
            calc_ts = np.linspace(0, 1, 1000)

            interp_vals = []
            for ts, val in self.data.get(f"{key}/raw", []):
                ts = np.asarray(ts)
                val = np.asarray(val)

                if ts.size == 0 or val.size == 0:
                    continue

                if ts.shape[0] != val.shape[0]:
                    n = min(ts.shape[0], val.shape[0])
                    if n == 0:
                        continue
                    ts = ts[:n]
                    val = val[:n]

                if ts.shape[0] == 1:
                    calc_ts = [0]
                    interp_vals.append(val)
                    continue

                ts = ts - ts[0]
                if ts[-1] == 0:
                    interp_vals.append(val)
                    continue
                ts = ts / ts[-1]

                if ts.shape[0] <= 3:
                    interp = np.interp(calc_ts, ts, val)
                else:
                    interp = scipy.interpolate.make_interp_spline(ts, val)(calc_ts)
                interp_vals.append(interp)

            if len(interp_vals) == 0:
                continue

            interp_vals = np.array(interp_vals)

            self.data[f"{key}/{CALC_KEY}/ts"] = calc_ts
            self.data[f"{key}/{CALC_KEY}/mean"] = (calc_ts, interp_vals.mean(axis=0))
            self.data[f"{key}/{CALC_KEY}/min"] = (calc_ts, interp_vals.min(axis=0))
            self.data[f"{key}/{CALC_KEY}/max"] = (calc_ts, interp_vals.max(axis=0))

    def to_json(self):
        self._calculate()
        return json.dumps(self.data, cls=NumpyEncoder, indent=2)

    def calculate(self):
        self._calculate()
        return self.data

    def to_report(self, out="report.pdf", template_path="templates/templatev3.typ"):
        out = Path(out)
        out.parent.mkdir(parents=True, exist_ok=True)
        report_data = self.to_json()
        typst.compile(
            template_path,
            output=str(out),
            font_paths=["templates/fonts"],
            sys_inputs=dict(report_data=report_data),
        )


def typ_log_rotmat_as_euler(
    typ_ctx: TypstCtx, key: str, ts: list[Timestamp], rotmats: list[Rotmat]
):
    ts = np.array(ts)
    euler, order = rotmat_to_euler(rotmats)

    for i, xyz in enumerate(order):
        angles = euler[:, i]
        typ_ctx.log(f"{key}/{xyz}", ts, angles)
        typ_ctx.log(
            f"{key}/{xyz}/range", np.array([0]), np.array([angles.max() - angles.min()])
        )