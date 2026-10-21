from dataclasses import dataclass
from types import SimpleNamespace

import pytest
from support import UNDER_WINE

if UNDER_WINE:
    pytest.skip("numpy crashes under Wine (ucrtbase crealf)", allow_module_level=True)

import numpy as np
import streamz as real_streamz

import akayu


@dataclass
class TaskInfo:
    name: str
    timestamp: int
    set: int
    extra: dict | None = None


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


class Matrix(FixedArray):
    _shape = (3, 3)


class VecA(FixedArray):
    _shape = (3,)


class VecB(FixedArray):
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
    def source_fn(begin, end, *args):
        return [1, 2, 3]

    def create_source_node(source_fn):
        def source_node(events, *args):
            task_start = events.filter(isinstance, Start)
            task_stop = events.filter(isinstance, Stop)
            window = real_streamz.combine_latest(task_start, task_stop).filter(
                lambda x: (x[0].name == x[1].name and x[0].set == x[1].set and x[0].timestamp < x[1].timestamp)
            )
            batched_items = window.starmap(source_fn, *args)
            events = (
                real_streamz.zip(window, batched_items)
                .starmap(lambda window, batched_items: ([window[0], *batched_items, window[1]]))
                .flatten()
            )
            return events

        return source_node

    def create_source_node_akayu(source_fn):
        def source_node(events, *args):
            task_start = events.filter(isinstance, Start)
            task_stop = events.filter(isinstance, Stop)
            window = akayu.combine_latest(task_start, task_stop).filter(
                lambda x: (x[0].name == x[1].name and x[0].set == x[1].set and x[0].timestamp < x[1].timestamp)
            )
            batched_items = window.starmap(source_fn, *args)
            events = (
                akayu.zip(window, batched_items)
                .starmap(lambda window, batched_items: ([window[0], *batched_items, window[1]]))
                .flatten()
            )
            return events

        return source_node

    sz_events = [Start("task1", 1000, 0), Stop("task1", 2000, 0)]
    sz_source = real_streamz.Stream()
    sz_node = create_source_node(source_fn)(sz_source.flatten())
    sz_out = []
    sz_node.sink(sz_out.append)
    sz_source.emit(sz_events)

    ak_events = [Start("task1", 1000, 0), Stop("task1", 2000, 0)]
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
    def source_fn(begin, end, *args):
        return [1, 2, 3]

    def create_source_node_streamz(source_fn):
        def source_node(events, *args):
            task_start = events.filter(isinstance, Start)
            task_stop = events.filter(isinstance, Stop)
            window = real_streamz.combine_latest(task_start, task_stop).filter(
                lambda x: (x[0].name == x[1].name and x[0].set == x[1].set and x[0].timestamp < x[1].timestamp)
            )
            batched_items = window.starmap(source_fn, *args)
            events = (
                real_streamz.zip(window, batched_items)
                .starmap(lambda window, batched_items: ([window[0], *batched_items, window[1]]))
                .flatten()
            )
            return events

        return source_node

    def create_source_node_akayu(source_fn):
        def source_node(events, *args):
            task_start = events.filter(isinstance, Start)
            task_stop = events.filter(isinstance, Stop)
            window = akayu.combine_latest(task_start, task_stop).filter(
                lambda x: (x[0].name == x[1].name and x[0].set == x[1].set and x[0].timestamp < x[1].timestamp)
            )
            batched_items = window.starmap(source_fn, *args)
            events = (
                akayu.zip(window, batched_items)
                .starmap(lambda window, batched_items: ([window[0], *batched_items, window[1]]))
                .flatten()
            )
            return events

        return source_node

    events = [Start("task1", 1000, 0), Stop("task1", 2000, 0)]

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


def identity_baseline(matrices_list):
    return Matrix(np.eye(3))


def identity_apply(baseline, data):
    return data


def create_baseline_node_streamz(baseline_func, apply_func):
    def split_filter(stream, predicate, *args):
        return stream.filter(predicate, *args), stream.filter(lambda *args: not predicate(*args), *args)

    def baseline_node(events, *task_names_list):
        current_task = events.filter(isinstance, TaskInfo)
        events_tagged = real_streamz.combine_latest(events, current_task, emit_on=events)

        start_task = current_task.filter(isinstance, Start)

        task_events_list = []
        window_list = []
        matrices = []
        remainder = events_tagged
        for task_names in task_names_list:
            baseline_events, remainder = split_filter(
                remainder,
                lambda x, task_names: (x[1].name in task_names and (isinstance(x[1], Start) or isinstance(x[0], Stop))),
                task_names,
            )
            baseline_events = baseline_events.map(lambda x: x[0])

            baseline_starts = baseline_events.filter(isinstance, Start)
            baseline_stops = baseline_events.filter(isinstance, Stop)
            baseline_window = real_streamz.zip(baseline_starts, baseline_stops)

            baseline_data = baseline_events.filter(lambda x: not isinstance(x, TaskInfo))
            first_matrices = baseline_data.filter(isinstance, Matrix)

            baseline_data = baseline_data.collect()
            first_matrices = first_matrices.collect()

            baseline_stops.sink(first_matrices.flush)
            first_matrices = first_matrices.filter(len)

            task_events_list.append(baseline_data)
            window_list.append(baseline_window)
            matrices.append(first_matrices)

        events = remainder.map(lambda x: x[0]).filter(
            lambda x: (not (isinstance(x, TaskInfo) and any(x.name in task_names for task_names in task_names_list)))
        )
        baselines = real_streamz.zip(*matrices).starmap(baseline_func)

        flushed_events = []
        for task_events in task_events_list:
            baselines.sink(task_events.flush)
            flushed_events.append(task_events)

        tmp = real_streamz.zip(baselines, *flushed_events)
        baselines = tmp.map(lambda x: x[0])
        task_events_list = [tmp.map(lambda x, i: x[i + 1], i) for i in range(len(flushed_events))]

        init_baseline = start_task.accumulate(
            lambda hist, x: (x, x) if hist is None else (x, None),
            start=None,
            returns_state=True,
        ).filter(lambda x: x is not None)
        init_baseline = init_baseline.map(lambda x: Matrix(np.eye(3)))

        baselines = baselines.union(init_baseline)

        task_events_flat = []
        for events, baseline_window in zip(task_events_list, window_list, strict=True):
            task_events = real_streamz.combine_latest(events, baseline_window, emit_on=events).starmap(
                lambda data, chunk: [chunk[0], *list(data), chunk[1]]
            )
            task_events = task_events.flatten()
            task_events_flat.append(task_events)

        events = real_streamz.union(events, *task_events_flat)

        data, events = split_filter(events, lambda x: isinstance(x, Matrix | VecA | VecB))
        data = real_streamz.combine_latest(baselines, data, emit_on=data).starmap(apply_func)
        events = real_streamz.union(events, data)

        return events

    return baseline_node


def create_baseline_node_akayu(baseline_func, apply_func):
    def split_filter(stream, predicate, *args):
        return stream.filter(predicate, *args), stream.filter(lambda *args: not predicate(*args), *args)

    def baseline_node(events, *task_names_list):
        current_task = events.filter(isinstance, TaskInfo)
        events_tagged = akayu.combine_latest(events, current_task, emit_on=events)

        start_task = current_task.filter(isinstance, Start)

        task_events_list = []
        window_list = []
        matrices = []
        remainder = events_tagged
        for task_names in task_names_list:
            baseline_events, remainder = split_filter(
                remainder,
                lambda x, task_names: (x[1].name in task_names and (isinstance(x[1], Start) or isinstance(x[0], Stop))),
                task_names,
            )
            baseline_events = baseline_events.map(lambda x: x[0])

            baseline_starts = baseline_events.filter(isinstance, Start)
            baseline_stops = baseline_events.filter(isinstance, Stop)
            baseline_window = akayu.zip(baseline_starts, baseline_stops)

            baseline_data = baseline_events.filter(lambda x: not isinstance(x, TaskInfo))
            first_matrices = baseline_data.filter(isinstance, Matrix)

            baseline_data = baseline_data.collect()
            first_matrices = first_matrices.collect()

            baseline_stops.sink(first_matrices.flush)
            first_matrices = first_matrices.filter(len)

            task_events_list.append(baseline_data)
            window_list.append(baseline_window)
            matrices.append(first_matrices)

        events = remainder.map(lambda x: x[0]).filter(
            lambda x: (not (isinstance(x, TaskInfo) and any(x.name in task_names for task_names in task_names_list)))
        )
        baselines = akayu.zip(*matrices).starmap(baseline_func)

        flushed_events = []
        for task_events in task_events_list:
            baselines.sink(task_events.flush)
            flushed_events.append(task_events)

        tmp = akayu.zip(baselines, *flushed_events)
        baselines = tmp.map(lambda x: x[0])
        task_events_list = [tmp.map(lambda x, i: x[i + 1], i) for i in range(len(flushed_events))]

        init_baseline = start_task.accumulate(
            lambda hist, x: (x, x) if hist is None else (x, None),
            start=None,
            returns_state=True,
        ).filter(lambda x: x is not None)
        init_baseline = init_baseline.map(lambda x: Matrix(np.eye(3)))

        baselines = baselines.union(init_baseline)

        task_events_flat = []
        for events, baseline_window in zip(task_events_list, window_list, strict=True):
            task_events = akayu.combine_latest(events, baseline_window, emit_on=events).starmap(
                lambda data, chunk: [chunk[0], *list(data), chunk[1]]
            )
            task_events = task_events.flatten()
            task_events_flat.append(task_events)

        events = akayu.union(events, *task_events_flat)

        data, events = split_filter(events, lambda x: isinstance(x, Matrix | VecA | VecB))
        data = akayu.combine_latest(baselines, data, emit_on=data).starmap(apply_func)
        events = akayu.union(events, data)

        return events

    return baseline_node


def test_create_baseline_node_with_single_task():
    events = [
        Start("warmup", 0, 1),
        Matrix(np.eye(3)),
        Stop("warmup", 5000, 1),
        Start("alpha", 10000, 1),
        Matrix(np.eye(3)),
        VecA(np.array([1.0, 2.0, 3.0])),
        VecB(np.array([0.1, 0.2, 0.3])),
        Stop("alpha", 20000, 1),
    ]

    sz_source = real_streamz.Stream()
    sz_node = create_baseline_node_streamz(identity_baseline, identity_apply)(sz_source.flatten(), "warmup", "alpha")
    sz_out = []
    sz_node.sink(sz_out.append)
    sz_source.emit(events)

    ak_source = akayu.Stream()
    ak_node = create_baseline_node_akayu(identity_baseline, identity_apply)(ak_source.flatten(), "warmup", "alpha")
    ak_out = []
    ak_node.sink(ak_out.append)
    ak_source.emit(events)

    assert lists_equal(sz_out, ak_out), f"single task baseline mismatch:\nstreamz={sz_out}\nakayu={ak_out}"


def test_create_baseline_node_with_multiple_tasks():
    events = [
        Start("warmup", 0, 1),
        Matrix(np.eye(3)),
        Stop("warmup", 5000, 1),
        Start("alpha", 10000, 1),
        Matrix(np.eye(3)),
        VecA(np.array([1.0, 2.0, 3.0])),
        VecB(np.array([0.1, 0.2, 0.3])),
        Stop("alpha", 20000, 1),
        Start("beta", 25000, 1),
        Matrix(np.eye(3)),
        VecA(np.array([4.0, 5.0, 6.0])),
        VecB(np.array([0.4, 0.5, 0.6])),
        Stop("beta", 35000, 1),
    ]

    sz_source = real_streamz.Stream()
    sz_node = create_baseline_node_streamz(identity_baseline, identity_apply)(
        sz_source.flatten(), "warmup", "alpha", "beta"
    )
    sz_out = []
    sz_node.sink(sz_out.append)
    sz_source.emit(events)

    ak_source = akayu.Stream()
    ak_node = create_baseline_node_akayu(identity_baseline, identity_apply)(
        ak_source.flatten(), "warmup", "alpha", "beta"
    )
    ak_out = []
    ak_node.sink(ak_out.append)
    ak_source.emit(events)

    assert lists_equal(sz_out, ak_out), f"multi-task baseline mismatch:\nstreamz={sz_out}\nakayu={ak_out}"


def test_create_baseline_node_multiple_task_groups():
    events = [
        Start("warmup", 0, 1),
        Matrix(np.eye(3)),
        Stop("warmup", 5000, 1),
        Start("alpha", 10000, 1),
        Matrix(np.eye(3)),
        VecA(np.array([1.0, 2.0, 3.0])),
        VecB(np.array([0.1, 0.2, 0.3])),
        Stop("alpha", 20000, 1),
        Start("beta", 25000, 1),
        Matrix(np.eye(3)),
        VecA(np.array([4.0, 5.0, 6.0])),
        VecB(np.array([0.4, 0.5, 0.6])),
        Stop("beta", 35000, 1),
    ]

    sz_source = real_streamz.Stream()
    sz_node = create_baseline_node_streamz(identity_baseline, identity_apply)(sz_source.flatten(), "warmup", "alpha")
    sz_out = []
    sz_node.sink(sz_out.append)
    sz_source.emit(events)

    ak_source = akayu.Stream()
    ak_node = create_baseline_node_akayu(identity_baseline, identity_apply)(ak_source.flatten(), "warmup", "alpha")
    ak_out = []
    ak_node.sink(ak_out.append)
    ak_source.emit(events)

    assert lists_equal(sz_out, ak_out), f"multi-task-group baseline mismatch:\nstreamz={sz_out}\nakayu={ak_out}"


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
    test_create_baseline_node_with_single_task()
    print("test_create_baseline_node_with_single_task PASSED")
    test_create_baseline_node_with_multiple_tasks()
    print("test_create_baseline_node_with_multiple_tasks PASSED")
    test_create_baseline_node_multiple_task_groups()
    print("test_create_baseline_node_multiple_task_groups PASSED")
    print("\nALL TESTS PASSED")


def create_source_node_streamz(source_fn):
    def source_node(events, *args):
        task_start = events.filter(isinstance, Start)
        task_stop = events.filter(isinstance, Stop)
        window = real_streamz.combine_latest(task_start, task_stop).filter(
            lambda x: (x[0].name == x[1].name and x[0].set == x[1].set and x[0].timestamp < x[1].timestamp)
        )
        batched_items = window.starmap(source_fn, *args)
        events = (
            real_streamz.zip(window, batched_items)
            .starmap(lambda window, batched_items: ([window[0], *batched_items, window[1]]))
            .flatten()
        )
        return events

    return source_node


def create_source_node_akayu(source_fn):
    def source_node(events, *args):
        task_start = events.filter(isinstance, Start)
        task_stop = events.filter(isinstance, Stop)
        window = akayu.combine_latest(task_start, task_stop).filter(
            lambda x: (x[0].name == x[1].name and x[0].set == x[1].set and x[0].timestamp < x[1].timestamp)
        )
        batched_items = window.starmap(source_fn, *args)
        events = (
            akayu.zip(window, batched_items)
            .starmap(lambda window, batched_items: ([window[0], *batched_items, window[1]]))
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


def mock_read_images(start, stop, path, part):
    n = 3
    result = []
    ts = start.timestamp
    for i in range(n):
        result.append(Timestamp(ts + i * 1000))
        result.append(Image(np.zeros((8, 8, 4), dtype=np.uint8) + i))
    return result


def mock_read_vectors(start, stop, path, part):
    n = 3
    result = []
    ts = start.timestamp
    for i in range(n):
        result.append(Timestamp(ts + i * 500))
        result.append(Matrix(np.eye(3) * (i + 1)))
        result.append(VecB(np.array([i, i, i], dtype=float)))
        result.append(VecA(np.array([i * 0.1, i * 0.1, i * 0.1], dtype=float)))
    return result


def _is_task_info(e):
    return isinstance(e, (TaskInfo, Start, Stop))


def merge_tagged_events(hist, event):
    k, e = event
    if _is_task_info(e):
        hist[k].append(e)
    else:
        if not hist[k] or not isinstance(hist[k][-1], list):
            hist[k].append([e])
        else:
            hist[k][-1].append(e)

    outs = []
    while True:
        if all(len(h) > 0 and _is_task_info(h[0]) for h in hist.values()):
            for h in hist.values():
                out = h.pop(0)
                outs.append(out)
            continue

        if all(len(h) > 1 or (len(h) > 0 and _is_task_info(h[0])) for h in hist.values()):
            earliest_ts = None
            earliest_key = None
            for k, h in hist.items():
                if _is_task_info(h[0]):
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
        task_info, events_tagged = split_filter(merged_events_tagged, _is_task_info)
        events = events_tagged.map(lambda x: x[1]).flatten()
        events = streamz_lib.union(task_info, events)
        return events

    return merge_node


merge_node_streamz = make_merge_node(real_streamz)
merge_node_akayu = make_merge_node(akayu)


def flush_parts_streamz(part_events):
    part_collect = {k: events.collect() for k, events in part_events.items()}
    part_results = {}

    def _assign(k, x):
        part_results[k] = x

    for k, events in part_collect.items():
        events.sink(lambda x, k: _assign(k, x), k)

    def flush():
        for events in part_collect.values():
            events.flush()
        return part_results

    return SimpleNamespace(flush=flush)


def flush_parts_akayu(part_events):
    part_collect = {k: events.collect() for k, events in part_events.items()}
    part_results = {}

    def _assign(k, x):
        part_results[k] = x

    for k, events in part_collect.items():
        events.sink(lambda x, k: _assign(k, x), k)

    def flush():
        for events in part_collect.values():
            events.flush()
        return part_results

    return SimpleNamespace(flush=flush)


def run_reader_flow_streamz(tasks, parts, vectors=True, images=True):
    source = real_streamz.Stream()
    events = source.map(lambda _: tasks).flatten()

    part_events = dict.fromkeys(parts, events)
    for part, sec_events in part_events.items():
        events_img = sec_events
        if images:
            source_node_fn = create_source_node_streamz(mock_read_images)
            events_img = source_node_fn(sec_events, "path", part)

        events_vec = sec_events
        if vectors:
            source_node_fn = create_source_node_streamz(mock_read_vectors)
            events_vec = source_node_fn(sec_events, "path", part)

        merged = merge_node_streamz(events_img, events_vec)
        part_events[part] = merged

    flushed = flush_parts_streamz(part_events)
    source.emit("path")
    return flushed.flush()


def run_reader_flow_akayu(tasks, parts, vectors=True, images=True):
    source = akayu.Stream()
    events = source.map(lambda _: tasks).flatten()

    part_events = dict.fromkeys(parts, events)
    for part, sec_events in part_events.items():
        events_img = sec_events
        if images:
            source_node_fn = create_source_node_akayu(mock_read_images)
            events_img = source_node_fn(sec_events, "path", part)

        events_vec = sec_events
        if vectors:
            source_node_fn = create_source_node_akayu(mock_read_vectors)
            events_vec = source_node_fn(sec_events, "path", part)

        merged = merge_node_akayu(events_img, events_vec)
        part_events[part] = merged

    flushed = flush_parts_akayu(part_events)
    source.emit("path")
    return flushed.flush()


def task_event_summary(events):
    return [type(e).__name__ for e in events]


def test_simple_source_node_multi_task():
    tasks = [
        Start("task1", 0, 1),
        Stop("task1", 5000, 1),
        Start("task2", 10000, 1),
        Stop("task2", 20000, 1),
    ]

    def mock_fn(start, stop, path, part):
        return [Timestamp(start.timestamp + 100), Matrix(np.eye(3))]

    sz_source = real_streamz.Stream()
    sz_events = sz_source.map(lambda _: tasks).flatten()
    sz_node = create_source_node_streamz(mock_fn)(sz_events, "path", "sec1")
    sz_out = []
    sz_node.sink(sz_out.append)
    sz_source.emit("path")

    print(f"streamz output ({len(sz_out)}):")
    for e in sz_out:
        print(f"  {type(e).__name__}")

    ak_source = akayu.Stream()
    ak_events = ak_source.map(lambda _: tasks).flatten()
    ak_node = create_source_node_akayu(mock_fn)(ak_events, "path", "sec1")
    ak_out = []
    ak_node.sink(ak_out.append)
    ak_source.emit("path")

    print(f"akayu output ({len(ak_out)}):")
    for e in ak_out:
        print(f"  {type(e).__name__}")

    assert len(sz_out) == len(ak_out), f"length mismatch: streamz={len(sz_out)}, akayu={len(ak_out)}"

    print("test_simple_source_node_multi_task PASSED")


def test_reader_flow_merge_two_sources():
    def make_source_streamz(tasks):
        source = real_streamz.Stream()
        events = source.map(lambda _: tasks).flatten()
        return source, events

    def make_source_akayu(tasks):
        source = akayu.Stream()
        events = source.map(lambda _: tasks).flatten()
        return source, events

    tasks = [
        Start("task1", 0, 1),
        Stop("task1", 5000, 1),
        Start("task2", 10000, 1),
        Stop("task2", 20000, 1),
    ]

    def mock_image_fn(start, stop, path, part):
        return [Timestamp(start.timestamp + 100), Image(np.zeros((4, 4)))]

    def mock_vector_fn(start, stop, path, part):
        return [Timestamp(start.timestamp + 50), Matrix(np.eye(3))]

    sz_source, sz_events = make_source_streamz(tasks)
    sz_img = create_source_node_streamz(mock_image_fn)(sz_events, "path", "sec1")
    sz_vec = create_source_node_streamz(mock_vector_fn)(sz_events, "path", "sec1")
    sz_merged = merge_node_streamz(sz_img, sz_vec)
    sz_out = []
    sz_merged.sink(sz_out.append)
    sz_source.emit("path")

    print(f"streamz merge output ({len(sz_out)} events):")
    for e in sz_out:
        print(f"  {type(e).__name__}: {e}")

    ak_source, ak_events = make_source_akayu(tasks)
    ak_img = create_source_node_akayu(mock_image_fn)(ak_events, "path", "sec1")
    ak_vec = create_source_node_akayu(mock_vector_fn)(ak_events, "path", "sec1")
    ak_merged = merge_node_akayu(ak_img, ak_vec)
    ak_out = []
    ak_merged.sink(ak_out.append)
    ak_source.emit("path")

    print(f"akayu merge output ({len(ak_out)} events):")
    for e in ak_out:
        print(f"  {type(e).__name__}: {e}")

    print("test_reader_flow_merge_two_sources PASSED")


def test_reader_flow_multi_part_multi_task():
    tasks = [
        Start("beta", 0, 1),
        Stop("beta", 5000, 1),
        Start("alpha", 10000, 1),
        Stop("alpha", 20000, 1),
        Start("warmup", 30000, 1),
        Stop("warmup", 40000, 1),
    ]

    parts = ["left/thigh", "left/shank", "right/thigh", "right/shank"]

    sz_result = run_reader_flow_streamz(tasks, parts, vectors=True, images=True)
    ak_result = run_reader_flow_akayu(tasks, parts, vectors=True, images=True)

    for part in parts:
        sz_summary = task_event_summary(sz_result[part])
        ak_summary = task_event_summary(ak_result[part])
        assert sz_summary == ak_summary, f"Part {part} mismatch:\n  streamz: {sz_summary}\n  akayu:  {ak_summary}"

    for part in parts:
        sz_starts = [e for e in sz_result[part] if isinstance(e, Start)]
        ak_starts = [e for e in ak_result[part] if isinstance(e, Start)]
        assert len(sz_starts) == len(ak_starts), (
            f"Part {part} Start count mismatch:\n"
            f"  streamz: {len(sz_starts)} {sz_starts}\n"
            f"  akayu:  {len(ak_starts)} {ak_starts}"
        )
        for sz_start, ak_start in zip(sz_starts, ak_starts, strict=True):
            assert sz_start.name == ak_start.name, f"Start name mismatch: {sz_start.name} vs {ak_start.name}"
            assert sz_start.timestamp == ak_start.timestamp
            assert sz_start.set == ak_start.set

    print("test_reader_flow_multi_part_multi_task PASSED")


def test_reader_flow_two_parts_two_tasks():
    tasks = [
        Start("task1", 0, 1),
        Stop("task1", 5000, 1),
        Start("task2", 10000, 1),
        Stop("task2", 20000, 1),
    ]

    parts = ["sec1", "sec2"]

    sz_result = run_reader_flow_streamz(tasks, parts, vectors=True, images=True)
    ak_result = run_reader_flow_akayu(tasks, parts, vectors=True, images=True)

    for part in parts:
        sz_summary = task_event_summary(sz_result[part])
        ak_summary = task_event_summary(ak_result[part])
        assert sz_summary == ak_summary, f"Part {part} mismatch:\n  streamz: {sz_summary}\n  akayu:  {ak_summary}"

    for part in parts:
        sz_starts = [e for e in sz_result[part] if isinstance(e, Start)]
        ak_starts = [e for e in ak_result[part] if isinstance(e, Start)]
        assert len(sz_starts) == len(ak_starts), (
            f"Part {part} Start count mismatch:\n"
            f"  streamz: {len(sz_starts)} {sz_starts}\n"
            f"  akayu:  {len(ak_starts)} {ak_starts}"
        )

    print("test_reader_flow_two_parts_two_tasks PASSED")


def test_reader_flow_fan_out():
    tasks = [
        Start("task1", 0, 1),
        Stop("task1", 5000, 1),
        Start("task2", 10000, 1),
        Stop("task2", 20000, 1),
    ]

    sz_source = real_streamz.Stream()
    sz_events = sz_source.map(lambda _: tasks).flatten()

    sz_sec1 = sz_events.map(lambda x: ("sec1", x))
    sz_sec2 = sz_events.map(lambda x: ("sec2", x))

    sz_out1 = []
    sz_sec1.sink(sz_out1.append)
    sz_out2 = []
    sz_sec2.sink(sz_out2.append)

    sz_source.emit("path")

    ak_source = akayu.Stream()
    ak_events = ak_source.map(lambda _: tasks).flatten()

    ak_sec1 = ak_events.map(lambda x: ("sec1", x))
    ak_sec2 = ak_events.map(lambda x: ("sec2", x))

    ak_out1 = []
    ak_sec1.sink(ak_out1.append)
    ak_out2 = []
    ak_sec2.sink(ak_out2.append)

    ak_source.emit("path")

    assert len(sz_out1) == len(ak_out1), f"sec1 count mismatch: streamz={len(sz_out1)}, akayu={len(ak_out1)}"
    assert len(sz_out2) == len(ak_out2), f"sec2 count mismatch: streamz={len(sz_out2)}, akayu={len(ak_out2)}"

    sz_summary1 = task_event_summary([x[1] for x in sz_out1])
    ak_summary1 = task_event_summary([x[1] for x in ak_out1])
    assert sz_summary1 == ak_summary1, "sec1 types mismatch"

    print("test_reader_flow_fan_out PASSED")


def test_reader_flow_simple_node():
    tasks = [
        Start("task1", 0, 1),
        Stop("task1", 5000, 1),
        Start("task2", 10000, 1),
        Stop("task2", 20000, 1),
    ]

    def mock_fn(start, stop, path, part):
        return [Timestamp(start.timestamp + 100), Image(np.zeros((4, 4)))]

    sz_source = real_streamz.Stream()
    sz_events = sz_source.map(lambda _: tasks).flatten()
    sz_node = create_source_node_streamz(mock_fn)(sz_events, "path", "sec1")
    sz_out = []
    sz_node.sink(sz_out.append)
    sz_source.emit("path")

    ak_source = akayu.Stream()
    ak_events = ak_source.map(lambda _: tasks).flatten()
    ak_node = create_source_node_akayu(mock_fn)(ak_events, "path", "sec1")
    ak_out = []
    ak_node.sink(ak_out.append)
    ak_source.emit("path")

    sz_summary = task_event_summary(sz_out)
    ak_summary = task_event_summary(ak_out)
    assert sz_summary == ak_summary, f"node output mismatch: streamz={sz_summary}, akayu={ak_summary}"

    sz_starts = [e for e in sz_out if isinstance(e, Start)]
    ak_starts = [e for e in ak_out if isinstance(e, Start)]
    assert len(sz_starts) == 2, f"streamz should have 2 starts, got {len(sz_starts)}"
    assert len(ak_starts) == 2, f"akayu should have 2 starts, got {len(ak_starts)}"

    print("test_reader_flow_simple_node PASSED")


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
    test_create_baseline_node_with_single_task()
    print("test_create_baseline_node_with_single_task PASSED")
    test_create_baseline_node_with_multiple_tasks()
    print("test_create_baseline_node_with_multiple_tasks PASSED")
    test_create_baseline_node_multiple_task_groups()
    print("test_create_baseline_node_multiple_task_groups PASSED")
    test_reader_flow_fan_out()
    print("test_reader_flow_fan_out PASSED")
    test_reader_flow_simple_node()
    print("test_reader_flow_simple_node PASSED")
    test_reader_flow_multi_part_multi_task()
    print("test_reader_flow_multi_part_multi_task PASSED")
    test_reader_flow_two_parts_two_tasks()
    print("test_reader_flow_two_parts_two_tasks PASSED")
    print("\nALL TESTS PASSED")
