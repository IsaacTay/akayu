"""Edge case tests to harden coverage before refactoring.

Covers: prefetch, combine_latest/zip edges, error recovery,
empty/minimal streams, union, flush behavior, async dispatch.
"""

import pytest

from akayu import Stream

# ============================================================
# a) Prefetch functional tests
# ============================================================


def test_prefetch_map_basic():
    s = Stream()
    results = []

    s.prefetch(2).map(lambda x: x * 2).sink(results.append)

    for i in range(5):
        s.emit(i)

    s.flush()
    assert sorted(results) == [0, 2, 4, 6, 8]


def test_prefetch_filter_basic():
    s = Stream()
    results = []

    s.prefetch(2).filter(lambda x: x % 2 == 0).sink(results.append)

    for i in range(6):
        s.emit(i)

    s.flush()
    assert sorted(results) == [0, 2, 4]


def test_prefetch_flush():
    s = Stream()
    results = []

    s.prefetch(2).map(lambda x: x * 2).sink(results.append)

    for i in range(3):
        s.emit(i)

    s.flush()
    assert sorted(results) == [0, 2, 4]


# ============================================================
# b) CombineLatest/Zip edge cases
# ============================================================


def test_combine_latest_single_item():
    s1 = Stream()
    s2 = Stream()
    s3 = Stream()
    results = []

    s1.combine_latest(s2, s3).sink(results.append)

    s1.emit(1)
    s2.emit(2)
    s3.emit(3)

    # After all three primed, last emit triggers one output
    assert len(results) == 1
    assert results[0] == (1, 2, 3)


def test_combine_latest_emit_batch():
    s1 = Stream()
    s2 = Stream()
    results = []

    s1.combine_latest(s2).sink(results.append)

    # Prime s2
    s2.emit(10)
    # Batch emit on s1
    s1.emit_batch([1, 2, 3])

    assert len(results) == 3
    assert results == [(1, 10), (2, 10), (3, 10)]


def test_zip_single_item():
    s1 = Stream()
    s2 = Stream()
    s3 = Stream()
    results = []

    s1.zip(s2, s3).sink(results.append)

    s1.emit(1)
    assert results == []
    s2.emit(2)
    assert results == []
    s3.emit(3)
    assert results == [(1, 2, 3)]


def test_zip_emit_batch():
    s1 = Stream()
    s2 = Stream()
    results = []

    s1.zip(s2).sink(results.append)

    s1.emit_batch([1, 2, 3])
    s2.emit_batch([10, 20, 30])

    assert results == [(1, 10), (2, 20), (3, 30)]


# ============================================================
# c) Error recovery
# ============================================================


def test_emit_after_error_in_map():
    s = Stream()
    results = []
    call_count = [0]

    def maybe_fail(x):
        call_count[0] += 1
        if x == 2:
            raise ValueError("boom")
        return x * 10

    s.map(maybe_fail, stream_name="maybe_fail").sink(results.append)

    s.emit(1)
    assert results == [10]

    with pytest.raises(RuntimeError):
        s.emit(2)

    # Stream should still work after error
    s.emit(3)
    assert results == [10, 30]


def test_emit_after_error_in_filter():
    s = Stream()
    results = []

    def maybe_fail(x):
        if x == 2:
            raise ValueError("boom")
        return x > 0

    s.filter(maybe_fail, stream_name="maybe_fail").sink(results.append)

    s.emit(1)
    assert results == [1]

    with pytest.raises(RuntimeError):
        s.emit(2)

    s.emit(3)
    assert results == [1, 3]


def test_emit_after_error_in_sink():
    s = Stream()
    results = []

    def maybe_fail(x):
        if x == 2:
            raise ValueError("boom")
        results.append(x)

    s.sink(maybe_fail, stream_name="maybe_fail")

    s.emit(1)
    assert results == [1]

    with pytest.raises(RuntimeError):
        s.emit(2)

    s.emit(3)
    assert results == [1, 3]


def test_batch_partial_error():
    """When one item in a batch errors, the batch raises.

    Note: emit_batch may or may not process items before the error,
    depending on implementation. We just verify it raises.
    """
    s = Stream()
    results = []

    def maybe_fail(x):
        if x == 3:
            raise ValueError("boom at 3")
        return x

    s.map(maybe_fail, stream_name="batch_fail").sink(results.append)

    with pytest.raises(RuntimeError):
        s.emit_batch([1, 2, 3, 4, 5])


# ============================================================
# d) Empty/minimal streams
# ============================================================


def test_emit_batch_empty():
    s = Stream()
    results = []
    s.map(lambda x: x + 1).sink(results.append)
    s.emit_batch([])
    assert results == []


def test_emit_batch_single_item():
    s = Stream()
    results = []
    s.map(lambda x: x * 2).sink(results.append)
    s.emit_batch([5])
    assert results == [10]


def test_emit_no_downstreams():
    s = Stream()
    # Should not crash
    s.emit(1)
    s.emit(2)


def test_compile_empty_stream():
    s = Stream()
    # Compiling with no downstreams should not crash
    s.compile()


def test_compile_source_only():
    s = Stream()
    s.compile()
    s.emit(1)  # Should not crash


def test_compile_source_sink_only():
    s = Stream()
    results = []
    s.sink(results.append)
    s.compile()
    s.emit(1)
    s.emit(2)
    assert results == [1, 2]


# ============================================================
# e) Union standalone
# ============================================================


def test_union_two_streams():
    s1 = Stream()
    s2 = Stream()
    results = []

    s1.union(s2).sink(results.append)

    s1.emit(1)
    s2.emit(2)
    s1.emit(3)

    assert results == [1, 2, 3]


def test_union_emit_batch():
    s1 = Stream()
    s2 = Stream()
    results = []

    s1.union(s2).sink(results.append)

    s1.emit_batch([1, 2, 3])
    s2.emit_batch([4, 5, 6])

    assert results == [1, 2, 3, 4, 5, 6]


def test_union_error_propagation():
    s1 = Stream()
    s2 = Stream()

    def fail(x):
        raise ValueError("union error")

    s1.union(s2).map(fail, stream_name="fail_node").sink(lambda x: None)

    with pytest.raises(RuntimeError):
        s1.emit(1)

    with pytest.raises(RuntimeError):
        s2.emit(2)


# ============================================================
# f) Flush behavior
# ============================================================


def test_flush_collect():
    s = Stream()
    results = []
    c = s.collect()
    c.sink(results.append)

    s.emit(1)
    s.emit(2)
    s.emit(3)
    assert results == []

    c.flush()
    assert len(results) == 1
    assert results[0] == (1, 2, 3)


def test_flush_propagates_downstream():
    s = Stream()
    results = []
    c = s.collect()
    c.map(lambda t: sum(t)).sink(results.append)

    s.emit(1)
    s.emit(2)
    s.emit(3)
    assert results == []

    c.flush()
    assert results == [6]


def test_flush_empty_pipeline():
    s = Stream()
    results = []
    s.map(lambda x: x).sink(results.append)
    # Flush with nothing buffered should not crash
    s.flush()
    assert results == []


#
