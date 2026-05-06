from .types import *
from .utils import rotmat_to_euler

import numpy as np

try:
    import rerun as rr
except ImportError:
    rr = None

def rr_init_pose(rec, name):
    rec.log(
        f"{name}/device",
        rr.Boxes3D(
            half_sizes=[[0.1, 0.05, 0.02]],
        ),
        static=True,
    )

    rec.log(
        f"{name}/axes",
        rr.Arrows3D(
            vectors=[[0.1, 0, 0], [0, 0.1, 0], [0, 0, 0.1]],
            origins=[[0, 0, 0], [0, 0, 0], [0, 0, 0]],
            colors=[[255, 0, 0], [0, 255, 0], [0, 0, 255]],
        ),
        static=True,
    )
    return rec

def rr_log_rotmat(rec, name, timestamps, rotmat):
    timestamps = np.array(timestamps)
    rotmat = np.array(rotmat)
    rec.send_columns(
        name,
        indexes = [rr.TimeColumn("timestamp", timestamp=timestamps / 1e6)],
        columns = rr.Transform3D.columns(mat3x3=rotmat)
    )
    return rec

def rr_log_rotmat_as_euler(rec, name, timestamps, rotmat):
    timestamps = np.array(timestamps)
    euler, order = rotmat_to_euler(rotmat)

    for i, axis in enumerate(order):
        rec.send_columns(
            f"{name}/{axis}",
            indexes = [rr.TimeColumn("timestamp", timestamp=timestamps / 1e6)],
            columns = rr.Scalars.columns(scalars=euler[:, i])
        )
    return rec

def rr_log_scalar(rec, name, timestamps, data):
    timestamps = np.array(timestamps)
    data = np.array(data)
    rec.send_columns(
        name,
        indexes = [rr.TimeColumn("timestamp", timestamp=timestamps / 1e6)],
        columns = rr.Scalars.columns(scalars=data)
    )
    return rec

def rr_log_image(rec, name, timestamps, data, color_model="L"):
    timestamps = np.array(timestamps)
    data = np.array(data)
    dtype = data.dtype
    channel_datatype = None
    if dtype == np.uint8:
        channel_datatype = "U8"
    elif dtype == np.float32:
        channel_datatype = "F32"
    elif dtype == np.float64:
        channel_datatype = "F64"
    fmt = rr.components.ImageFormat(width=data.shape[2], height=data.shape[1], color_model=color_model, channel_datatype=channel_datatype)
    rec.send_columns(
        f"{name}/image",
        indexes = [rr.TimeColumn("timestamp", timestamp=timestamps / 1e6)],
        columns = rr.Image.columns(buffer=data.view(np.uint8).reshape(data.shape[0], -1), format=[fmt for _ in range(data.shape[0])])
    )
    rec.log(f"{name}/image", rr.Pinhole(focal_length=10, width=data.shape[2], height=data.shape[1]), static=True)
    return rec

def rr_log_text(rec, name, data):
    rec.set_time("timestamp", timestamp=data.timestamp / 1e6)
    rec.log(
        name,
        rr.TextLog(repr(data), level=rr.TextLogLevel.INFO)
    )
    return rec
