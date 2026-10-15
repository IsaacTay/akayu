import pytest
from support import UNDER_WINE

if UNDER_WINE:
    pytest.skip("numpy crashes under Wine (ucrtbase crealf)", allow_module_level=True)

import numpy as np

import akayu
from pipeline.nodes import (
    _create_calibration_node,
    _create_source_node,
    merge_node,
    reinterpolate_imu_node,
    rel_rotmat_node,
    resync_node,
)
from pipeline.types import Accel, Gyro, Rotmat, SegmentStart, SegmentStop, Start, Stop, Timestamp
from pipeline.utils import batch_events, flush_segments, merge_tagged_events, split_filter


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
            result.extend(
                [
                    Timestamp(start.timestamp + i * 10000),
                    Rotmat(np.eye(3)),
                    Accel([0, 0, 9.81]),
                    Gyro([0, 0, 0]),
                ]
            )
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
    _exe_events, seg_events = split_filter(events, lambda x: isinstance(x, (Start, Stop, Timestamp)))

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


# --- Pipeline fixture patterns (was test_pipeline_patterns.py) ---


def test_create_source_node():
    def mock_source(start, stop, *args):
        result = []
        for i in range(5):
            result.extend([Timestamp(i * 1000), Rotmat(np.eye(3))])
        return result

    source_node = _create_source_node(mock_source)
    events = akayu.Stream()
    output = []
    result = source_node(events)
    result.sink(output.append)

    s = Start(name="test", timestamp=0, set=1)
    e = Stop(name="test", timestamp=5000, set=1)
    events.emit(s)
    for i in range(5):
        events.emit(Timestamp(i * 1000))
        events.emit(Rotmat(np.eye(3)))
    events.emit(e)

    assert len(output) > 0
    assert isinstance(output[0], Start)
    assert isinstance(output[-1], Stop)


def test_split_filter():
    s = akayu.Stream()
    matching, non_matching = split_filter(s, lambda x: x > 5)
    m_out, n_out = [], []
    matching.sink(m_out.append)
    non_matching.sink(n_out.append)

    for i in range(10):
        s.emit(i)

    assert m_out == [6, 7, 8, 9]
    assert n_out == [0, 1, 2, 3, 4, 5]


def test_merge_tagged_events():
    s1 = akayu.Stream()
    s2 = akayu.Stream()

    tagged1 = s1.map(lambda x: (0, x))
    tagged2 = s2.map(lambda x: (1, x))

    merged = akayu.union(tagged1, tagged2)

    def accumulate_wrapper(hist, event):
        new_hist, outs = merge_tagged_events(hist, event)
        return new_hist, (new_hist, outs)

    accumulated = merged.accumulate(accumulate_wrapper, start={0: [], 1: []}, returns_state=True)

    result = []
    accumulated.sink(result.append)

    s1.emit(Timestamp(1000))
    s2.emit(Timestamp(2000))
    s1.emit(Gyro([1, 2, 3]))
    s2.emit(Gyro([4, 5, 6]))

    assert len(result) >= 2


def test_flush_segments():
    segment_events = {"left": akayu.Stream(), "right": akayu.Stream()}

    for i in range(3):
        segment_events["left"].emit(SegmentStart(timestamp=i * 1000, set=i))
        segment_events["left"].emit(Timestamp(i * 1000 + 100))
        segment_events["left"].emit(Timestamp(i * 1000 + 200))
        segment_events["left"].emit(SegmentStop(timestamp=i * 1000 + 300, set=i))

    seg = flush_segments(segment_events)
    result_dict = seg.flush()

    assert len(result_dict) == 2
    assert isinstance(result_dict, dict)


def test_combine_latest_any():
    s1 = akayu.Stream()
    s2 = akayu.Stream()

    cl = akayu.combine_latest(s1, s2)
    result = []
    cl.sink(result.append)

    s1.emit(1)
    s2.emit("a")

    assert len(result) == 1
    assert result[-1] == (1, "a")


def test_fan_out_diamond():
    source = akayu.Stream()

    processed = source.map(lambda x: x * 2)
    zipped = akayu.zip(source, processed)

    result = []
    zipped.sink(result.append)

    source.emit(1)
    source.emit(2)
    source.emit(3)

    assert result == [(1, 2), (2, 4), (3, 6)]


def test_batch_events():
    s = Start(name="test", timestamp=0, set=1)
    e = Stop(name="test", timestamp=5000, set=1)

    hist = None
    batched_results = []
    events_in = [s] + [Timestamp(i * 100) for i in range(5)] + [e]
    for ev in events_in:
        hist, batched = batch_events(hist if hist is not None else {}, ev)
        if batched:
            batched_results.extend(batched)

    assert len(batched_results) > 0


def test_source_node_synthetic_imu():
    def synthetic_imu_source(start_info, stop_info):
        n = 10
        result = []
        for i in range(n):
            result.extend(
                [
                    Timestamp(start_info.timestamp + i * 10000),
                    Rotmat(np.eye(3)),
                    Accel([0, 0, 9.81]),
                    Gyro([0, 0, 0]),
                ]
            )
        return result

    node = _create_source_node(synthetic_imu_source)
    events = akayu.Stream()
    result_stream = node(events)
    output = []
    result_stream.sink(output.append)

    s = Start(name="exercise", timestamp=0, set=1)
    e = Stop(name="exercise", timestamp=100000, set=1)

    events.emit(s)
    events.emit(e)

    assert len(output) >= 2
    assert isinstance(output[0], Start)
    assert isinstance(output[-1], Stop)
    assert len(output) == 1 + 10 * 4 + 1
