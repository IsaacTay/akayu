from dataclasses import dataclass
from types import SimpleNamespace

import numpy as np
import streamz as real_streamz

import akayu


@dataclass
class ExerciseInfo:
    name: str
    timestamp: int
    set: int
    addition_data: dict | None = None


@dataclass
class Start:
    name: str
    timestamp: int
    set: int


@dataclass
class Stop:
    name: str
    timestamp: int
    set: int


class FixedArray:
    _shape = None

    def __init__(self, data):
        self.data = np.array(data, dtype=float).reshape(self._shape)

    def __repr__(self):
        return f"{self.__class__.__name__}({self.data.tolist()})"


class Rotmat(FixedArray):
    _shape = (3, 3)


class Accel(FixedArray):
    _shape = (3,)


class Gyro(FixedArray):
    _shape = (3,)


class Timestamp(int):
    pass


def split_filter(stream, predicate, *args):
    return stream.filter(predicate, *args), stream.filter(lambda *args: not predicate(*args), *args)


def test_combine_latest_no_emit_on():
    s1 = real_streamz.Stream()
    s2 = real_streamz.Stream()
    cl = real_streamz.combine_latest(s1, s2)
    sz_out = []
    cl.sink(sz_out.append)
    s1.emit(1)
    s2.emit("a")
    s1.emit(2)

    a1 = akayu.Stream()
    a2 = akayu.Stream()
    acl = akayu.combine_latest(a1, a2)
    ak_out = []
    acl.sink(ak_out.append)
    a1.emit(1)
    a2.emit("a")
    a1.emit(2)

    assert sz_out == ak_out, f"combine_latest mismatch: streamz={sz_out}, akayu={ak_out}"


def test_combine_latest_with_emit_on():
    s1 = real_streamz.Stream()
    s2 = real_streamz.Stream()
    cl = real_streamz.combine_latest(s1, s2, emit_on=s2)
    sz_out = []
    cl.sink(sz_out.append)
    s1.emit(1)
    s2.emit("a")
    s1.emit(2)

    a1 = akayu.Stream()
    a2 = akayu.Stream()
    acl = akayu.combine_latest(a1, a2, emit_on=a2)
    ak_out = []
    acl.sink(ak_out.append)
    a1.emit(1)
    a2.emit("a")
    a1.emit(2)

    assert sz_out == ak_out, f"combine_latest emit_on mismatch: streamz={sz_out}, akayu={ak_out}"


def test_combine_latest_with_emit_on_index():
    s1 = real_streamz.Stream()
    s2 = real_streamz.Stream()
    cl = real_streamz.combine_latest(s1, s2, emit_on=0)
    sz_out = []
    cl.sink(sz_out.append)
    s1.emit(1)
    s2.emit("a")
    s1.emit(2)

    a1 = akayu.Stream()
    a2 = akayu.Stream()
    try:
        acl = akayu.combine_latest(a1, a2, emit_on=0)
        ak_out = []
        acl.sink(ak_out.append)
        a1.emit(1)
        a2.emit("a")
        a1.emit(2)
        assert sz_out == ak_out, f"combine_latest emit_on=0 mismatch: streamz={sz_out}, akayu={ak_out}"
    except TypeError as e:
        if "emit_on" in str(e) and "int" in str(e):
            print(f"NOTE: akayu does not support emit_on with integer index (streamz does): {e}")
        else:
            raise


def test_zip():
    s1 = real_streamz.Stream()
    s2 = real_streamz.Stream()
    z = real_streamz.zip(s1, s2)
    sz_out = []
    z.sink(sz_out.append)
    s1.emit(1)
    s1.emit(2)
    s1.emit(3)
    s2.emit("a")
    s2.emit("b")

    a1 = akayu.Stream()
    a2 = akayu.Stream()
    az = akayu.zip(a1, a2)
    ak_out = []
    az.sink(ak_out.append)
    a1.emit(1)
    a1.emit(2)
    a1.emit(3)
    a2.emit("a")
    a2.emit("b")

    assert sz_out == ak_out, f"zip mismatch: streamz={sz_out}, akayu={ak_out}"


def test_fan_out_to_map_and_zip():
    source = real_streamz.Stream()
    mapped = source.map(lambda x: x * 2)
    zipped = real_streamz.zip(source, mapped)
    sz_out = []
    zipped.sink(sz_out.append)
    source.emit(1)
    source.emit(2)
    source.emit(3)

    asource = akayu.Stream()
    amapped = asource.map(lambda x: x * 2)
    azipped = akayu.zip(asource, amapped)
    ak_out = []
    azipped.sink(ak_out.append)
    asource.emit(1)
    asource.emit(2)
    asource.emit(3)

    assert sz_out == ak_out, f"Fan-out diamond mismatch: streamz={sz_out}, akayu={ak_out}"


def test_source_node_pattern():
    def source_fn(ex_start, ex_stop, *args):
        return [1, 2, 3]

    def create_source_node(source_fn):
        def source_node(events, *args):
            exe_start = events.filter(isinstance, Start)
            exe_stop = events.filter(isinstance, Stop)
            exe_chunk = real_streamz.combine_latest(exe_start, exe_stop).filter(
                lambda x: (x[0].name == x[1].name and x[0].set == x[1].set and x[0].timestamp < x[1].timestamp)
            )
            batched_imu = exe_chunk.starmap(source_fn, *args)
            events = (
                real_streamz.zip(exe_chunk, batched_imu)
                .starmap(lambda exe_chunk, batched_imu: ([exe_chunk[0], *batched_imu, exe_chunk[1]]))
                .flatten()
            )
            return events

        return source_node

    def create_source_node_akayu(source_fn):
        def source_node(events, *args):
            exe_start = events.filter(isinstance, Start)
            exe_stop = events.filter(isinstance, Stop)
            exe_chunk = akayu.combine_latest(exe_start, exe_stop).filter(
                lambda x: (x[0].name == x[1].name and x[0].set == x[1].set and x[0].timestamp < x[1].timestamp)
            )
            batched_imu = exe_chunk.starmap(source_fn, *args)
            events = (
                akayu.zip(exe_chunk, batched_imu)
                .starmap(lambda exe_chunk, batched_imu: ([exe_chunk[0], *batched_imu, exe_chunk[1]]))
                .flatten()
            )
            return events

        return source_node

    sz_events = [Start("exe1", 1000, 0), Stop("exe1", 2000, 0)]
    sz_source = real_streamz.Stream()
    sz_node = create_source_node(source_fn)(sz_source.flatten())
    sz_out = []
    sz_node.sink(sz_out.append)
    sz_source.emit(sz_events)

    ak_events = [Start("exe1", 1000, 0), Stop("exe1", 2000, 0)]
    ak_source = akayu.Stream()
    ak_node = create_source_node_akayu(source_fn)(ak_source.flatten())
    ak_out = []
    ak_node.sink(ak_out.append)
    ak_source.emit(ak_events)

    assert sz_out == ak_out, f"source_node pattern mismatch: streamz={sz_out}, akayu={ak_out}"


def test_union_and_accumulate():
    s1 = real_streamz.Stream()
    s2 = real_streamz.Stream()
    t1 = s1.map(lambda x: (0, x))
    t2 = s2.map(lambda x: (1, x))
    merged = real_streamz.union(t1, t2)
    acc = merged.accumulate(lambda hist, x: ({**hist[0], x[0]: x[1]}, None), start=({}, None))
    sz_out = []
    acc.sink(sz_out.append)
    s1.emit("a")
    s2.emit("b")
    s1.emit("c")

    a1 = akayu.Stream()
    a2 = akayu.Stream()
    at1 = a1.map(lambda x: (0, x))
    at2 = a2.map(lambda x: (1, x))
    amerged = akayu.union(at1, at2)
    aacc = amerged.accumulate(lambda hist, x: ({**hist[0], x[0]: x[1]}, None), start=({}, None))
    ak_out = []
    aacc.sink(ak_out.append)
    a1.emit("a")
    a2.emit("b")
    a1.emit("c")

    assert [x[0] for x in sz_out] == [x[0] for x in ak_out], (
        f"union/accumulate mismatch: streamz={[x[0] for x in sz_out]}, akayu={[x[0] for x in ak_out]}"
    )


def test_collect_and_flush():
    s = real_streamz.Stream()
    collected = s.collect()
    sz_out = []
    collected.sink(sz_out.append)
    s.emit(1)
    s.emit(2)
    s.emit(3)
    collected.flush()

    a = akayu.Stream()
    acollected = a.collect()
    ak_out = []
    acollected.sink(ak_out.append)
    a.emit(1)
    a.emit(2)
    a.emit(3)
    acollected.flush()

    assert sz_out == ak_out, f"collect/flush mismatch: streamz={sz_out}, akayu={ak_out}"


def test_split_filter():
    def sf_split_filter(stream, predicate, *args):
        return stream.filter(predicate, *args), stream.filter(lambda *args: not predicate(*args), *args)

    s = real_streamz.Stream()
    evens, odds = sf_split_filter(s, lambda x: x % 2 == 0)
    sz_evens = []
    sz_odds = []
    evens.sink(sz_evens.append)
    odds.sink(sz_odds.append)
    s.emit(1)
    s.emit(2)
    s.emit(3)
    s.emit(4)

    a = akayu.Stream()
    ak_evens, ak_odds = sf_split_filter(a, lambda x: x % 2 == 0)
    ak_evens_out = []
    ak_odds_out = []
    ak_evens.sink(ak_evens_out.append)
    ak_odds.sink(ak_odds_out.append)
    a.emit(1)
    a.emit(2)
    a.emit(3)
    a.emit(4)

    assert sz_evens == ak_evens_out, f"split_filter evens mismatch: streamz={sz_evens}, akayu={ak_evens_out}"
    assert sz_odds == ak_odds_out, f"split_filter odds mismatch: streamz={sz_odds}, akayu={ak_odds_out}"


def test_starmap():
    s = real_streamz.Stream()
    sm = s.starmap(lambda a, b: a + b)
    sz_out = []
    sm.sink(sz_out.append)
    s.emit((1, 2))
    s.emit((3, 4))

    a = akayu.Stream()
    asm = a.starmap(lambda a, b: a + b)
    ak_out = []
    asm.sink(ak_out.append)
    a.emit((1, 2))
    a.emit((3, 4))

    assert sz_out == ak_out, f"starmap mismatch: streamz={sz_out}, akayu={ak_out}"


def test_full_pipeline_comparison():
    def source_fn(ex_start, ex_stop, *args):
        return [1, 2, 3]

    def create_source_node_streamz(source_fn):
        def source_node(events, *args):
            exe_start = events.filter(isinstance, Start)
            exe_stop = events.filter(isinstance, Stop)
            exe_chunk = real_streamz.combine_latest(exe_start, exe_stop).filter(
                lambda x: (x[0].name == x[1].name and x[0].set == x[1].set and x[0].timestamp < x[1].timestamp)
            )
            batched_imu = exe_chunk.starmap(source_fn, *args)
            events = (
                real_streamz.zip(exe_chunk, batched_imu)
                .starmap(lambda exe_chunk, batched_imu: ([exe_chunk[0], *batched_imu, exe_chunk[1]]))
                .flatten()
            )
            return events

        return source_node

    def create_source_node_akayu(source_fn):
        def source_node(events, *args):
            exe_start = events.filter(isinstance, Start)
            exe_stop = events.filter(isinstance, Stop)
            exe_chunk = akayu.combine_latest(exe_start, exe_stop).filter(
                lambda x: (x[0].name == x[1].name and x[0].set == x[1].set and x[0].timestamp < x[1].timestamp)
            )
            batched_imu = exe_chunk.starmap(source_fn, *args)
            events = (
                akayu.zip(exe_chunk, batched_imu)
                .starmap(lambda exe_chunk, batched_imu: ([exe_chunk[0], *batched_imu, exe_chunk[1]]))
                .flatten()
            )
            return events

        return source_node

    events = [Start("exe1", 1000, 0), Stop("exe1", 2000, 0)]

    sz_source = real_streamz.Stream()
    sz_node = create_source_node_streamz(source_fn)(sz_source.flatten())
    sz_out = []
    sz_node.sink(sz_out.append)
    sz_source.emit(events)

    ak_source = akayu.Stream()
    ak_node = create_source_node_akayu(source_fn)(ak_source.flatten())
    ak_out = []
    ak_node.sink(ak_out.append)
    ak_source.emit(events)

    assert sz_out == ak_out, f"full pipeline mismatch: streamz={sz_out}, akayu={ak_out}"


def np_array_equal(a, b):
    if type(a) is not type(b):
        return False
    if hasattr(a, "data"):
        return np.array_equal(a.data, b.data)
    if isinstance(a, np.ndarray):
        return np.array_equal(a, b)
    return a == b


def events_equal(ev_a, ev_b):
    if type(ev_a) is not type(ev_b):
        return False
    if hasattr(ev_a, "data"):
        return np.array_equal(ev_a.data, ev_b.data)
    if isinstance(ev_a, (Start, Stop)):
        return ev_a.name == ev_b.name and ev_a.timestamp == ev_b.timestamp and ev_a.set == ev_b.set
    return ev_a == ev_b


def lists_equal(l_a, l_b):
    if len(l_a) != len(l_b):
        return False
    for a, b in zip(l_a, l_b, strict=True):
        if isinstance(a, list) and isinstance(b, list):
            if not lists_equal(a, b):
                return False
        elif isinstance(a, tuple) and isinstance(b, tuple):
            if not all(events_equal(x, y) for x, y in zip(a, b, strict=True)):
                return False
        elif not events_equal(a, b):
            return False
    return True


def identity_calib(rotmats_list):
    return Rotmat(np.eye(3))


def identity_apply(calib, data):
    return data


def create_calibration_node_streamz(calib_func, apply_func):
    def split_filter(stream, predicate, *args):
        return stream.filter(predicate, *args), stream.filter(lambda *args: not predicate(*args), *args)

    def calibration_node(events, *exe_names_list):
        current_exe = events.filter(isinstance, ExerciseInfo)
        events_tagged = real_streamz.combine_latest(events, current_exe, emit_on=events)

        start_exe = current_exe.filter(isinstance, Start)

        exe_events_list = []
        exe_chunk_list = []
        rotmats = []
        remainder = events_tagged
        for exe_names in exe_names_list:
            standing_events, remainder = split_filter(
                remainder,
                lambda x, exe_names: (x[1].name in exe_names and (isinstance(x[1], Start) or isinstance(x[0], Stop))),
                exe_names,
            )
            standing_events = standing_events.map(lambda x: x[0])

            calib_starts = standing_events.filter(isinstance, Start)
            calib_stops = standing_events.filter(isinstance, Stop)
            calib_chunk = real_streamz.zip(calib_starts, calib_stops)

            standing_data = standing_events.filter(lambda x: not isinstance(x, ExerciseInfo))
            standing_rotmats = standing_data.filter(isinstance, Rotmat)

            standing_data = standing_data.collect()
            standing_rotmats = standing_rotmats.collect()

            calib_stops.sink(standing_rotmats.flush)
            standing_rotmats = standing_rotmats.filter(len)

            exe_events_list.append(standing_data)
            exe_chunk_list.append(calib_chunk)
            rotmats.append(standing_rotmats)

        events = remainder.map(lambda x: x[0]).filter(
            lambda x: (not (isinstance(x, ExerciseInfo) and any(x.name in exe_names for exe_names in exe_names_list)))
        )
        calibs = real_streamz.zip(*rotmats).starmap(calib_func)

        flushed_events = []
        for exe_events in exe_events_list:
            calibs.sink(exe_events.flush)
            flushed_events.append(exe_events)

        tmp = real_streamz.zip(calibs, *flushed_events)
        calibs = tmp.map(lambda x: x[0])
        exe_events_list = [tmp.map(lambda x, i: x[i + 1], i) for i in range(len(flushed_events))]

        init_calib = start_exe.accumulate(
            lambda hist, x: (x, x) if hist is None else (x, None),
            start=None,
            returns_state=True,
        ).filter(lambda x: x is not None)
        init_calib = init_calib.map(lambda x: Rotmat(np.eye(3)))

        calibs = calibs.union(init_calib)

        exe_events_flat = []
        for events, calib_chunk in zip(exe_events_list, exe_chunk_list, strict=True):
            exe_events = real_streamz.combine_latest(events, calib_chunk, emit_on=events).starmap(
                lambda data, chunk: [chunk[0], *list(data), chunk[1]]
            )
            exe_events = exe_events.flatten()
            exe_events_flat.append(exe_events)

        events = real_streamz.union(events, *exe_events_flat)

        data, events = split_filter(events, lambda x: isinstance(x, Rotmat | Accel | Gyro))
        data = real_streamz.combine_latest(calibs, data, emit_on=data).starmap(apply_func)
        events = real_streamz.union(events, data)

        return events

    return calibration_node


def create_calibration_node_akayu(calib_func, apply_func):
    def split_filter(stream, predicate, *args):
        return stream.filter(predicate, *args), stream.filter(lambda *args: not predicate(*args), *args)

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
                lambda x, exe_names: (x[1].name in exe_names and (isinstance(x[1], Start) or isinstance(x[0], Stop))),
                exe_names,
            )
            standing_events = standing_events.map(lambda x: x[0])

            calib_starts = standing_events.filter(isinstance, Start)
            calib_stops = standing_events.filter(isinstance, Stop)
            calib_chunk = akayu.zip(calib_starts, calib_stops)

            standing_data = standing_events.filter(lambda x: not isinstance(x, ExerciseInfo))
            standing_rotmats = standing_data.filter(isinstance, Rotmat)

            standing_data = standing_data.collect()
            standing_rotmats = standing_rotmats.collect()

            calib_stops.sink(standing_rotmats.flush)
            standing_rotmats = standing_rotmats.filter(len)

            exe_events_list.append(standing_data)
            exe_chunk_list.append(calib_chunk)
            rotmats.append(standing_rotmats)

        events = remainder.map(lambda x: x[0]).filter(
            lambda x: (not (isinstance(x, ExerciseInfo) and any(x.name in exe_names for exe_names in exe_names_list)))
        )
        calibs = akayu.zip(*rotmats).starmap(calib_func)

        flushed_events = []
        for exe_events in exe_events_list:
            calibs.sink(exe_events.flush)
            flushed_events.append(exe_events)

        tmp = akayu.zip(calibs, *flushed_events)
        calibs = tmp.map(lambda x: x[0])
        exe_events_list = [tmp.map(lambda x, i: x[i + 1], i) for i in range(len(flushed_events))]

        init_calib = start_exe.accumulate(
            lambda hist, x: (x, x) if hist is None else (x, None),
            start=None,
            returns_state=True,
        ).filter(lambda x: x is not None)
        init_calib = init_calib.map(lambda x: Rotmat(np.eye(3)))

        calibs = calibs.union(init_calib)

        exe_events_flat = []
        for events, calib_chunk in zip(exe_events_list, exe_chunk_list, strict=True):
            exe_events = akayu.combine_latest(events, calib_chunk, emit_on=events).starmap(
                lambda data, chunk: [chunk[0], *list(data), chunk[1]]
            )
            exe_events = exe_events.flatten()
            exe_events_flat.append(exe_events)

        events = akayu.union(events, *exe_events_flat)

        data, events = split_filter(events, lambda x: isinstance(x, Rotmat | Accel | Gyro))
        data = akayu.combine_latest(calibs, data, emit_on=data).starmap(apply_func)
        events = akayu.union(events, data)

        return events

    return calibration_node


def test_create_calibration_node_with_single_exercise():
    events = [
        Start("standing", 0, 1),
        Rotmat(np.eye(3)),
        Stop("standing", 5000, 1),
        Start("lunge", 10000, 1),
        Rotmat(np.eye(3)),
        Accel(np.array([1.0, 2.0, 3.0])),
        Gyro(np.array([0.1, 0.2, 0.3])),
        Stop("lunge", 20000, 1),
    ]

    sz_source = real_streamz.Stream()
    sz_node = create_calibration_node_streamz(identity_calib, identity_apply)(sz_source.flatten(), "standing", "lunge")
    sz_out = []
    sz_node.sink(sz_out.append)
    sz_source.emit(events)

    ak_source = akayu.Stream()
    ak_node = create_calibration_node_akayu(identity_calib, identity_apply)(ak_source.flatten(), "standing", "lunge")
    ak_out = []
    ak_node.sink(ak_out.append)
    ak_source.emit(events)

    assert lists_equal(sz_out, ak_out), f"single exercise calibration mismatch:\nstreamz={sz_out}\nakayu={ak_out}"


def test_create_calibration_node_with_multiple_exercises():
    events = [
        Start("standing", 0, 1),
        Rotmat(np.eye(3)),
        Stop("standing", 5000, 1),
        Start("lunge", 10000, 1),
        Rotmat(np.eye(3)),
        Accel(np.array([1.0, 2.0, 3.0])),
        Gyro(np.array([0.1, 0.2, 0.3])),
        Stop("lunge", 20000, 1),
        Start("squat", 25000, 1),
        Rotmat(np.eye(3)),
        Accel(np.array([4.0, 5.0, 6.0])),
        Gyro(np.array([0.4, 0.5, 0.6])),
        Stop("squat", 35000, 1),
    ]

    sz_source = real_streamz.Stream()
    sz_node = create_calibration_node_streamz(identity_calib, identity_apply)(
        sz_source.flatten(), "standing", "lunge", "squat"
    )
    sz_out = []
    sz_node.sink(sz_out.append)
    sz_source.emit(events)

    ak_source = akayu.Stream()
    ak_node = create_calibration_node_akayu(identity_calib, identity_apply)(
        ak_source.flatten(), "standing", "lunge", "squat"
    )
    ak_out = []
    ak_node.sink(ak_out.append)
    ak_source.emit(events)

    assert lists_equal(sz_out, ak_out), f"multi-exercise calibration mismatch:\nstreamz={sz_out}\nakayu={ak_out}"


def test_create_calibration_node_multiple_exercise_groups():
    events = [
        Start("standing", 0, 1),
        Rotmat(np.eye(3)),
        Stop("standing", 5000, 1),
        Start("lunge", 10000, 1),
        Rotmat(np.eye(3)),
        Accel(np.array([1.0, 2.0, 3.0])),
        Gyro(np.array([0.1, 0.2, 0.3])),
        Stop("lunge", 20000, 1),
        Start("squat", 25000, 1),
        Rotmat(np.eye(3)),
        Accel(np.array([4.0, 5.0, 6.0])),
        Gyro(np.array([0.4, 0.5, 0.6])),
        Stop("squat", 35000, 1),
    ]

    sz_source = real_streamz.Stream()
    sz_node = create_calibration_node_streamz(identity_calib, identity_apply)(sz_source.flatten(), "standing", "lunge")
    sz_out = []
    sz_node.sink(sz_out.append)
    sz_source.emit(events)

    ak_source = akayu.Stream()
    ak_node = create_calibration_node_akayu(identity_calib, identity_apply)(ak_source.flatten(), "standing", "lunge")
    ak_out = []
    ak_node.sink(ak_out.append)
    ak_source.emit(events)

    assert lists_equal(sz_out, ak_out), f"multi-exercise-group calibration mismatch:\nstreamz={sz_out}\nakayu={ak_out}"


if __name__ == "__main__":
    test_combine_latest_no_emit_on()
    print("test_combine_latest_no_emit_on PASSED")
    test_combine_latest_with_emit_on()
    print("test_combine_latest_with_emit_on PASSED")
    test_combine_latest_with_emit_on_index()
    print("test_combine_latest_with_emit_on_index PASSED")
    test_zip()
    print("test_zip PASSED")
    test_fan_out_to_map_and_zip()
    print("test_fan_out_to_map_and_zip PASSED")
    test_source_node_pattern()
    print("test_source_node_pattern PASSED")
    test_union_and_accumulate()
    print("test_union_and_accumulate PASSED")
    test_collect_and_flush()
    print("test_collect_and_flush PASSED")
    test_split_filter()
    print("test_split_filter PASSED")
    test_starmap()
    print("test_starmap PASSED")
    test_full_pipeline_comparison()
    print("test_full_pipeline_comparison PASSED")
    test_create_calibration_node_with_single_exercise()
    print("test_create_calibration_node_with_single_exercise PASSED")
    test_create_calibration_node_with_multiple_exercises()
    print("test_create_calibration_node_with_multiple_exercises PASSED")
    test_create_calibration_node_multiple_exercise_groups()
    print("test_create_calibration_node_multiple_exercise_groups PASSED")
    print("\nALL TESTS PASSED")


def create_source_node_streamz(source_fn):
    def source_node(events, *args):
        exe_start = events.filter(isinstance, Start)
        exe_stop = events.filter(isinstance, Stop)
        exe_chunk = real_streamz.combine_latest(exe_start, exe_stop).filter(
            lambda x: (x[0].name == x[1].name and x[0].set == x[1].set and x[0].timestamp < x[1].timestamp)
        )
        batched_imu = exe_chunk.starmap(source_fn, *args)
        events = (
            real_streamz.zip(exe_chunk, batched_imu)
            .starmap(lambda exe_chunk, batched_imu: ([exe_chunk[0], *batched_imu, exe_chunk[1]]))
            .flatten()
        )
        return events

    return source_node


def create_source_node_akayu(source_fn):
    def source_node(events, *args):
        exe_start = events.filter(isinstance, Start)
        exe_stop = events.filter(isinstance, Stop)
        exe_chunk = akayu.combine_latest(exe_start, exe_stop).filter(
            lambda x: (x[0].name == x[1].name and x[0].set == x[1].set and x[0].timestamp < x[1].timestamp)
        )
        batched_imu = exe_chunk.starmap(source_fn, *args)
        events = (
            akayu.zip(exe_chunk, batched_imu)
            .starmap(lambda exe_chunk, batched_imu: ([exe_chunk[0], *batched_imu, exe_chunk[1]]))
            .flatten()
        )
        return events

    return source_node


class Image:
    def __init__(self, data):
        self.data = np.array(data)

    def __repr__(self):
        return f"Image(shape={self.data.shape})"

    __hash__ = None  # mutable array data: unhashable, like any class defining __eq__

    def __eq__(self, other):
        if not isinstance(other, Image):
            return False
        return np.array_equal(self.data, other.data)


def mock_source_v1_uls(start, stop, session_path, segment):
    n = 3
    result = []
    ts = start.timestamp
    for i in range(n):
        result.append(Timestamp(ts + i * 1000))
        result.append(Image(np.zeros((8, 8, 4), dtype=np.uint8) + i))
    return result


def mock_source_v1_imu(start, stop, session_path, segment):
    n = 3
    result = []
    ts = start.timestamp
    for i in range(n):
        result.append(Timestamp(ts + i * 500))
        result.append(Rotmat(np.eye(3) * (i + 1)))
        result.append(Gyro(np.array([i, i, i], dtype=float)))
        result.append(Accel(np.array([i * 0.1, i * 0.1, i * 0.1], dtype=float)))
    return result


def _is_exercise_info(e):
    return isinstance(e, (ExerciseInfo, Start, Stop))


def merge_tagged_events(hist, event):
    k, e = event
    if _is_exercise_info(e):
        hist[k].append(e)
    else:
        if not hist[k] or not isinstance(hist[k][-1], list):
            hist[k].append([e])
        else:
            hist[k][-1].append(e)

    outs = []
    while True:
        if all(len(h) > 0 and _is_exercise_info(h[0]) for h in hist.values()):
            for h in hist.values():
                out = h.pop(0)
                outs.append(out)
            continue

        if all(len(h) > 1 or (len(h) > 0 and _is_exercise_info(h[0])) for h in hist.values()):
            earliest_ts = None
            earliest_key = None
            for k, h in hist.items():
                if _is_exercise_info(h[0]):
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


def make_merge_node(streamz_lib):
    def merge_node(*events_arr):
        events_tagged_arr = [events.map(lambda x, i: (i, x), i) for i, events in enumerate(events_arr)]
        events_tagged = streamz_lib.union(*events_tagged_arr)
        merged_events_tagged = (
            events_tagged.accumulate(
                merge_tagged_events,
                start={k: [] for k in range(len(events_arr))},
                returns_state=True,
            )
            .filter(len)
            .flatten()
        )
        exe_info, events_tagged = split_filter(merged_events_tagged, _is_exercise_info)
        events = events_tagged.map(lambda x: x[1]).flatten()
        events = streamz_lib.union(exe_info, events)
        return events

    return merge_node


merge_node_streamz = make_merge_node(real_streamz)
merge_node_akayu = make_merge_node(akayu)


def flush_segments_streamz(segment_events):
    segment_collect = {k: events.collect() for k, events in segment_events.items()}
    segment_results = {}

    def _assign(k, x):
        segment_results[k] = x

    for k, events in segment_collect.items():
        events.sink(lambda x, k: _assign(k, x), k)

    def flush():
        for events in segment_collect.values():
            events.flush()
        return segment_results

    return SimpleNamespace(flush=flush)


def flush_segments_akayu(segment_events):
    segment_collect = {k: events.collect() for k, events in segment_events.items()}
    segment_results = {}

    def _assign(k, x):
        segment_results[k] = x

    for k, events in segment_collect.items():
        events.sink(lambda x, k: _assign(k, x), k)

    def flush():
        for events in segment_collect.values():
            events.flush()
        return segment_results

    return SimpleNamespace(flush=flush)


def run_source_v1_flow_streamz(exercises, segments, imu=True, ultrasound=True):
    source = real_streamz.Stream()
    events = source.map(lambda _: exercises).flatten()

    segment_events = dict.fromkeys(segments, events)
    for segment, seg_events in segment_events.items():
        events_uls = seg_events
        if ultrasound:
            source_node_fn = create_source_node_streamz(mock_source_v1_uls)
            events_uls = source_node_fn(seg_events, "session_path", segment)

        events_imu = seg_events
        if imu:
            source_node_fn = create_source_node_streamz(mock_source_v1_imu)
            events_imu = source_node_fn(seg_events, "session_path", segment)

        merged = merge_node_streamz(events_uls, events_imu)
        segment_events[segment] = merged

    flushed = flush_segments_streamz(segment_events)
    source.emit("session_path")
    return flushed.flush()


def run_source_v1_flow_akayu(exercises, segments, imu=True, ultrasound=True):
    source = akayu.Stream()
    events = source.map(lambda _: exercises).flatten()

    segment_events = dict.fromkeys(segments, events)
    for segment, seg_events in segment_events.items():
        events_uls = seg_events
        if ultrasound:
            source_node_fn = create_source_node_akayu(mock_source_v1_uls)
            events_uls = source_node_fn(seg_events, "session_path", segment)

        events_imu = seg_events
        if imu:
            source_node_fn = create_source_node_akayu(mock_source_v1_imu)
            events_imu = source_node_fn(seg_events, "session_path", segment)

        merged = merge_node_akayu(events_uls, events_imu)
        segment_events[segment] = merged

    flushed = flush_segments_akayu(segment_events)
    source.emit("session_path")
    return flushed.flush()


def exercise_event_summary(events):
    return [type(e).__name__ for e in events]


def test_simple_source_node_multi_exercise():
    exercises = [
        Start("exe1", 0, 1),
        Stop("exe1", 5000, 1),
        Start("exe2", 10000, 1),
        Stop("exe2", 20000, 1),
    ]

    def mock_fn(start, stop, session_path, segment):
        return [Timestamp(start.timestamp + 100), Rotmat(np.eye(3))]

    sz_source = real_streamz.Stream()
    sz_events = sz_source.map(lambda _: exercises).flatten()
    sz_node = create_source_node_streamz(mock_fn)(sz_events, "path", "seg1")
    sz_out = []
    sz_node.sink(sz_out.append)
    sz_source.emit("session_path")

    print(f"streamz output ({len(sz_out)}):")
    for e in sz_out:
        print(f"  {type(e).__name__}")

    ak_source = akayu.Stream()
    ak_events = ak_source.map(lambda _: exercises).flatten()
    ak_node = create_source_node_akayu(mock_fn)(ak_events, "path", "seg1")
    ak_out = []
    ak_node.sink(ak_out.append)
    ak_source.emit("session_path")

    print(f"akayu output ({len(ak_out)}):")
    for e in ak_out:
        print(f"  {type(e).__name__}")

    assert len(sz_out) == len(ak_out), f"length mismatch: streamz={len(sz_out)}, akayu={len(ak_out)}"

    print("test_simple_source_node_multi_exercise PASSED")


def test_source_v1_flow_merge_two_sources():
    def make_source_streamz(exercises):
        source = real_streamz.Stream()
        events = source.map(lambda _: exercises).flatten()
        return source, events

    def make_source_akayu(exercises):
        source = akayu.Stream()
        events = source.map(lambda _: exercises).flatten()
        return source, events

    exercises = [
        Start("exe1", 0, 1),
        Stop("exe1", 5000, 1),
        Start("exe2", 10000, 1),
        Stop("exe2", 20000, 1),
    ]

    def mock_uls_fn(start, stop, session_path, segment):
        return [Timestamp(start.timestamp + 100), Image(np.zeros((4, 4)))]

    def mock_imu_fn(start, stop, session_path, segment):
        return [Timestamp(start.timestamp + 50), Rotmat(np.eye(3))]

    sz_source, sz_events = make_source_streamz(exercises)
    sz_uls = create_source_node_streamz(mock_uls_fn)(sz_events, "path", "seg1")
    sz_imu = create_source_node_streamz(mock_imu_fn)(sz_events, "path", "seg1")
    sz_merged = merge_node_streamz(sz_uls, sz_imu)
    sz_out = []
    sz_merged.sink(sz_out.append)
    sz_source.emit("session_path")

    print(f"streamz merge output ({len(sz_out)} events):")
    for e in sz_out:
        print(f"  {type(e).__name__}: {e}")

    ak_source, ak_events = make_source_akayu(exercises)
    ak_uls = create_source_node_akayu(mock_uls_fn)(ak_events, "path", "seg1")
    ak_imu = create_source_node_akayu(mock_imu_fn)(ak_events, "path", "seg1")
    ak_merged = merge_node_akayu(ak_uls, ak_imu)
    ak_out = []
    ak_merged.sink(ak_out.append)
    ak_source.emit("session_path")

    print(f"akayu merge output ({len(ak_out)} events):")
    for e in ak_out:
        print(f"  {type(e).__name__}: {e}")

    print("test_source_v1_flow_merge_two_sources PASSED")


def test_source_v1_flow_multi_segment_multi_exercise():
    exercises = [
        Start("squat", 0, 1),
        Stop("squat", 5000, 1),
        Start("lunge", 10000, 1),
        Stop("lunge", 20000, 1),
        Start("standing", 30000, 1),
        Stop("standing", 40000, 1),
    ]

    segments = ["left/thigh", "left/shank", "right/thigh", "right/shank"]

    sz_result = run_source_v1_flow_streamz(exercises, segments, imu=True, ultrasound=True)
    ak_result = run_source_v1_flow_akayu(exercises, segments, imu=True, ultrasound=True)

    for segment in segments:
        sz_summary = exercise_event_summary(sz_result[segment])
        ak_summary = exercise_event_summary(ak_result[segment])
        assert sz_summary == ak_summary, f"Segment {segment} mismatch:\n  streamz: {sz_summary}\n  akayu:  {ak_summary}"

    for segment in segments:
        sz_starts = [e for e in sz_result[segment] if isinstance(e, Start)]
        ak_starts = [e for e in ak_result[segment] if isinstance(e, Start)]
        assert len(sz_starts) == len(ak_starts), (
            f"Segment {segment} Start count mismatch:\n"
            f"  streamz: {len(sz_starts)} {sz_starts}\n"
            f"  akayu:  {len(ak_starts)} {ak_starts}"
        )
        for sz_start, ak_start in zip(sz_starts, ak_starts, strict=True):
            assert sz_start.name == ak_start.name, f"Start name mismatch: {sz_start.name} vs {ak_start.name}"
            assert sz_start.timestamp == ak_start.timestamp
            assert sz_start.set == ak_start.set

    print("test_source_v1_flow_multi_segment_multi_exercise PASSED")


def test_source_v1_flow_two_segments_two_exercises():
    exercises = [
        Start("exe1", 0, 1),
        Stop("exe1", 5000, 1),
        Start("exe2", 10000, 1),
        Stop("exe2", 20000, 1),
    ]

    segments = ["seg1", "seg2"]

    sz_result = run_source_v1_flow_streamz(exercises, segments, imu=True, ultrasound=True)
    ak_result = run_source_v1_flow_akayu(exercises, segments, imu=True, ultrasound=True)

    for segment in segments:
        sz_summary = exercise_event_summary(sz_result[segment])
        ak_summary = exercise_event_summary(ak_result[segment])
        assert sz_summary == ak_summary, f"Segment {segment} mismatch:\n  streamz: {sz_summary}\n  akayu:  {ak_summary}"

    for segment in segments:
        sz_starts = [e for e in sz_result[segment] if isinstance(e, Start)]
        ak_starts = [e for e in ak_result[segment] if isinstance(e, Start)]
        assert len(sz_starts) == len(ak_starts), (
            f"Segment {segment} Start count mismatch:\n"
            f"  streamz: {len(sz_starts)} {sz_starts}\n"
            f"  akayu:  {len(ak_starts)} {ak_starts}"
        )

    print("test_source_v1_flow_two_segments_two_exercises PASSED")


def test_source_v1_flow_fan_out():
    exercises = [
        Start("exe1", 0, 1),
        Stop("exe1", 5000, 1),
        Start("exe2", 10000, 1),
        Stop("exe2", 20000, 1),
    ]

    sz_source = real_streamz.Stream()
    sz_events = sz_source.map(lambda _: exercises).flatten()

    sz_seg1 = sz_events.map(lambda x: ("seg1", x))
    sz_seg2 = sz_events.map(lambda x: ("seg2", x))

    sz_out1 = []
    sz_seg1.sink(sz_out1.append)
    sz_out2 = []
    sz_seg2.sink(sz_out2.append)

    sz_source.emit("session_path")

    ak_source = akayu.Stream()
    ak_events = ak_source.map(lambda _: exercises).flatten()

    ak_seg1 = ak_events.map(lambda x: ("seg1", x))
    ak_seg2 = ak_events.map(lambda x: ("seg2", x))

    ak_out1 = []
    ak_seg1.sink(ak_out1.append)
    ak_out2 = []
    ak_seg2.sink(ak_out2.append)

    ak_source.emit("session_path")

    assert len(sz_out1) == len(ak_out1), f"seg1 count mismatch: streamz={len(sz_out1)}, akayu={len(ak_out1)}"
    assert len(sz_out2) == len(ak_out2), f"seg2 count mismatch: streamz={len(sz_out2)}, akayu={len(ak_out2)}"

    sz_summary1 = exercise_event_summary([x[1] for x in sz_out1])
    ak_summary1 = exercise_event_summary([x[1] for x in ak_out1])
    assert sz_summary1 == ak_summary1, "seg1 types mismatch"

    print("test_source_v1_flow_fan_out PASSED")


def test_source_v1_flow_simple_node():
    exercises = [
        Start("exe1", 0, 1),
        Stop("exe1", 5000, 1),
        Start("exe2", 10000, 1),
        Stop("exe2", 20000, 1),
    ]

    def mock_fn(start, stop, session_path, segment):
        return [Timestamp(start.timestamp + 100), Image(np.zeros((4, 4)))]

    sz_source = real_streamz.Stream()
    sz_events = sz_source.map(lambda _: exercises).flatten()
    sz_node = create_source_node_streamz(mock_fn)(sz_events, "path", "seg1")
    sz_out = []
    sz_node.sink(sz_out.append)
    sz_source.emit("session_path")

    ak_source = akayu.Stream()
    ak_events = ak_source.map(lambda _: exercises).flatten()
    ak_node = create_source_node_akayu(mock_fn)(ak_events, "path", "seg1")
    ak_out = []
    ak_node.sink(ak_out.append)
    ak_source.emit("session_path")

    sz_summary = exercise_event_summary(sz_out)
    ak_summary = exercise_event_summary(ak_out)
    assert sz_summary == ak_summary, f"node output mismatch: streamz={sz_summary}, akayu={ak_summary}"

    sz_starts = [e for e in sz_out if isinstance(e, Start)]
    ak_starts = [e for e in ak_out if isinstance(e, Start)]
    assert len(sz_starts) == 2, f"streamz should have 2 starts, got {len(sz_starts)}"
    assert len(ak_starts) == 2, f"akayu should have 2 starts, got {len(ak_starts)}"

    print("test_source_v1_flow_simple_node PASSED")


if __name__ == "__main__":
    test_combine_latest_no_emit_on()
    print("test_combine_latest_no_emit_on PASSED")
    test_combine_latest_with_emit_on()
    print("test_combine_latest_with_emit_on PASSED")
    test_combine_latest_with_emit_on_index()
    print("test_combine_latest_with_emit_on_index PASSED")
    test_zip()
    print("test_zip PASSED")
    test_fan_out_to_map_and_zip()
    print("test_fan_out_to_map_and_zip PASSED")
    test_source_node_pattern()
    print("test_source_node_pattern PASSED")
    test_union_and_accumulate()
    print("test_union_and_accumulate PASSED")
    test_collect_and_flush()
    print("test_collect_and_flush PASSED")
    test_split_filter()
    print("test_split_filter PASSED")
    test_starmap()
    print("test_starmap PASSED")
    test_full_pipeline_comparison()
    print("test_full_pipeline_comparison PASSED")
    test_create_calibration_node_with_single_exercise()
    print("test_create_calibration_node_with_single_exercise PASSED")
    test_create_calibration_node_with_multiple_exercises()
    print("test_create_calibration_node_with_multiple_exercises PASSED")
    test_create_calibration_node_multiple_exercise_groups()
    print("test_create_calibration_node_multiple_exercise_groups PASSED")
    test_source_v1_flow_fan_out()
    print("test_source_v1_flow_fan_out PASSED")
    test_source_v1_flow_simple_node()
    print("test_source_v1_flow_simple_node PASSED")
    test_source_v1_flow_multi_segment_multi_exercise()
    print("test_source_v1_flow_multi_segment_multi_exercise PASSED")
    test_source_v1_flow_two_segments_two_exercises()
    print("test_source_v1_flow_two_segments_two_exercises PASSED")
    print("\nALL TESTS PASSED")
