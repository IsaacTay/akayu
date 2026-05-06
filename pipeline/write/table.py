from ..types import *
from ..utils import *

from pathlib import Path
import polars as pl
import numpy as np

class TableCtx:
    def __init__(self):
        self.data = {}
    def log(self, exe: str, key: str, val):
        exe = self.data.setdefault(exe, {})
        exe[key] = val

    def export_csvs(self, out_path):
        out_path = Path(out_path)
        out_path.mkdir(exist_ok=True, parents=True)
        for exe_path, exe in self.data.items():
            df = pl.from_dict(exe)
            exe_path = out_path / f"{exe_path}.csv"
            exe_path.parent.mkdir(parents=True, exist_ok=True)
            df.write_csv(exe_path)

def tbl_log_scalar(tbl_ctx: TableCtx, exe: str, key: str, data):
    tbl_ctx.log(exe, key, data)

def tbl_log_segments(tbl_ctx: TableCtx, exe: str, starts: list, stops: list):
    def _kind(s):
        meta = getattr(s, "addition_data", None) or {}
        return meta.get("kind", "generic")

    tbl_ctx.log(exe, "rep", np.array([s.set for s in starts]))
    tbl_ctx.log(exe, "kind", np.array([_kind(s) for s in starts]))
    tbl_ctx.log(exe, "start_timestamp", np.array([s.timestamp for s in starts]))
    tbl_ctx.log(exe, "stop_timestamp", np.array([s.timestamp for s in stops]))

def tbl_log_rotmat_as_euler(tbl_ctx: TableCtx, exe: str, ts: list[Timestamp], rotmats: list[Rotmat]):
    ts = np.array(ts)
    euler, order = rotmat_to_euler(rotmats)

    tbl_ctx.log(exe, f"timestamp", ts)
    for i, xyz in enumerate(order):
        angles = euler[:, i]
        tbl_ctx.log(exe, f"{xyz}", angles)