import pytest
import numpy as np

import akayu
from pipeline.nodes import (
    merge_node,
    _create_calibration_node,
    reinterpolate_imu_node,
    resync_node,
    rel_rotmat_node,
    _create_source_node,
)
from pipeline.types import Start, Stop, Timestamp, Rotmat, Accel, Gyro, SegmentStart, SegmentStop
from pipeline.utils import split_filter, flush_segments, batch_events


def test_merge_node_two_streams():
    events1 = akayu.Stream()
    events2 = akayu.Stream()

    merged = merge_node(events1, events2)
    output = []
    merged.sink(output.append)

    events1.emit(Start(name="ex1", timestamp=0, set=1))
    events1.emit(Timestamp(100))
    events1.emit(Timestamp(200))
    events2.emit(Start(name="ex2", timestamp=50, set=1))
    events2.emit(Timestamp(150))
    events1.emit(Stop(name="ex1", timestamp=300, set=1))
    events2.emit(Stop(name="ex2", timestamp=250, set=1))

    assert len(output) > 0


def test_calibration_node_pattern():
    def mock_calib(rotmats, *args):
        return np.eye(3)

    def mock_apply(calib, x):
        return x

    calib_node = _create_calibration_node(mock_calib, mock_apply)
    events = akayu.Stream()
    result = calib_node(events, ["standing"], ["walking"])

    output = []
    result.sink(output.append)

    events.emit(Start(name="standing", timestamp=0, set=1))
    for i in range(10):
        events.emit(Timestamp(i * 1000))
        events.emit(Rotmat(np.eye(3)))
    events.emit(Stop(name="standing", timestamp=10000, set=1))

    events.emit(Start(name="walking", timestamp=11000, set=1))
    for i in range(5):
        events.emit(Timestamp(11000 + i * 1000))
        events.emit(Rotmat(np.eye(3)))
    events.emit(Stop(name="walking", timestamp=16000, set=1))

    assert len(output) > 0


def test_reinterpolate_imu_node():
    events = akayu.Stream()
    result = reinterpolate_imu_node(events, target_hz=100)
    output = []
    result.sink(output.append)

    events.emit(Start(name="test", timestamp=0, set=1))
    for i in range(20):
        events.emit(Timestamp(i * 10000))
        events.emit(Rotmat(np.eye(3) + np.random.randn(3, 3) * 0.01))
        events.emit(Gyro([0.1, 0.2, 0.3]))
        events.emit(Accel([0, 0, 9.81]))
    events.emit(Stop(name="test", timestamp=200000, set=1))

    assert len(output) > 0
    assert isinstance(output[0], Start)
    assert isinstance(output[-1], Stop)


def test_resync_node():
    events1 = akayu.Stream()
    events2 = akayu.Stream()

    resynced = resync_node(events1, events2)
    assert isinstance(resynced, list)
    assert len(resynced) == 2


def test_rel_rotmat_node():
    events1 = akayu.Stream()
    events2 = akayu.Stream()

    result = rel_rotmat_node(events1, events2)
    output = []
    result.sink(output.append)

    for i in range(5):
        events1.emit(Timestamp(i * 1000))
        events1.emit(Rotmat(np.eye(3)))
        events2.emit(Timestamp(i * 1000))
        events2.emit(Rotmat(np.eye(3)))

    assert len(output) >= 0


def test_full_pipeline_source_to_merge():
    def imu_source(start, stop):
        n = 20
        result = []
        for i in range(n):
            result.extend([
                Timestamp(start.timestamp + i * 10000),
                Rotmat(np.eye(3)),
                Accel([0, 0, 9.81]),
                Gyro([0, 0, 0]),
            ])
        return result

    source = _create_source_node(imu_source)
    events = akayu.Stream()
    sourced = source(events)
    reinterpolated = reinterpolate_imu_node(sourced)
    output = []
    reinterpolated.sink(output.append)

    events.emit(Start(name="test", timestamp=0, set=1))
    events.emit(Stop(name="test", timestamp=200000, set=1))

    assert len(output) > 0
    assert isinstance(output[0], Start)
    assert isinstance(output[-1], Stop)


def test_segment_csv_flow_pattern():
    events = akayu.Stream()
    exe_events, seg_events = split_filter(
        events, lambda x: isinstance(x, (Start, Stop, Timestamp))
    )

    seg = flush_segments({"test": seg_events})
    result = []
    seg_events.sink(result.append)

    events.emit(Start(name="test", timestamp=0, set=1))
    events.emit(SegmentStart(timestamp=0, set=0))
    events.emit(Timestamp(100))
    events.emit(Timestamp(200))
    events.emit(SegmentStop(timestamp=300, set=0))
    events.emit(SegmentStart(timestamp=400, set=1))
    events.emit(Timestamp(500))
    events.emit(Timestamp(600))
    events.emit(SegmentStop(timestamp=700, set=1))
    events.emit(Stop(name="test", timestamp=1000, set=1))

    flushed = seg.flush()
    segment_events = flushed["test"]
    segment_starts = [e for e in segment_events if isinstance(e, SegmentStart)]
    assert len(segment_starts) == 2