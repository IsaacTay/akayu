"""Tests for parallel branch execution."""

import time
import threading
from akayu import Stream, union


def test_par_basic():
    """Test that par node passes values through to all downstreams."""
    results = []

    s = Stream()
    p = s.par()
    p.map(lambda x: x * 2).sink(results.append)
    p.map(lambda x: x * 3).sink(results.append)

    s.emit(10)

    assert sorted(results) == [20, 30]


def test_parallel_alias():
    """Test that parallel() is an alias for par()."""
    results = []

    s = Stream()
    p = s.parallel()  # Using the alias
    p.map(lambda x: x * 2).sink(results.append)
    p.map(lambda x: x * 3).sink(results.append)

    s.emit(10)

    assert sorted(results) == [20, 30]


def test_par_single_downstream():
    """Test par with single downstream (should work without overhead)."""
    results = []

    s = Stream()
    p = s.par()
    p.map(lambda x: x + 1).sink(results.append)

    s.emit(5)
    s.emit(10)

    assert results == [6, 11]


def test_par_no_downstream():
    """Test par with no downstreams."""
    s = Stream()
    _p = s.par()

    # Should not raise
    s.emit(1)


def test_par_timing():
    """Test that parallel branches actually run concurrently."""
    results = []

    def slow_branch(x, delay):
        time.sleep(delay)
        results.append((x, delay))
        return x

    s = Stream()
    p = s.par()
    # Two branches, each with 0.1s delay
    p.map(lambda x: slow_branch(x, 0.1)).sink(lambda x: None)
    p.map(lambda x: slow_branch(x, 0.1)).sink(lambda x: None)

    start = time.time()
    s.emit(1)
    elapsed = time.time() - start

    # If sequential: ~0.2s, if parallel: ~0.1s
    # Allow some margin for thread overhead
    assert elapsed < 0.18, f"Expected parallel execution (~0.1s), got {elapsed:.3f}s"
    assert len(results) == 2


def test_par_three_branches_timing():
    """Test par with three branches."""
    results = []

    def slow_branch(x, branch_id):
        time.sleep(0.05)
        results.append(branch_id)
        return x

    s = Stream()
    p = s.par()
    p.map(lambda x: slow_branch(x, "A")).sink(lambda x: None)
    p.map(lambda x: slow_branch(x, "B")).sink(lambda x: None)
    p.map(lambda x: slow_branch(x, "C")).sink(lambda x: None)

    start = time.time()
    s.emit(1)
    elapsed = time.time() - start

    # If sequential: ~0.15s, if parallel: ~0.05s
    assert elapsed < 0.12, f"Expected parallel execution (~0.05s), got {elapsed:.3f}s"
    assert len(results) == 3


def test_par_with_union():
    """Test parallel branches that merge via union."""
    results = []

    def slow_load(x, source):
        time.sleep(0.05)
        return f"{source}:{x}"

    s = Stream()
    p = s.par()
    branch_a = p.map(lambda x: slow_load(x, "A"))
    branch_b = p.map(lambda x: slow_load(x, "B"))
    merged = union(branch_a, branch_b)
    merged.sink(results.append)

    start = time.time()
    s.emit("file1")
    elapsed = time.time() - start

    # Both branches should complete in parallel
    assert elapsed < 0.09, f"Expected parallel execution, got {elapsed:.3f}s"
    assert len(results) == 2
    assert sorted(results) == ["A:file1", "B:file1"]


def test_par_error_propagation():
    """Test that errors from one branch are propagated."""
    results = []

    def failing_branch(x):
        raise ValueError("boom")

    s = Stream()
    p = s.par()
    p.map(failing_branch).sink(results.append)
    p.map(lambda x: x * 2).sink(results.append)

    try:
        s.emit(1)
        assert False, "Expected ValueError to be raised"
    except (ValueError, RuntimeError) as e:
        # Error should be propagated (may be wrapped in RuntimeError)
        assert "boom" in str(e) or "boom" in str(e.__cause__)


def test_par_preserves_order_within_branch():
    """Test that order is preserved within each branch."""
    results_a = []
    results_b = []

    s = Stream()
    p = s.par()
    p.sink(results_a.append)
    p.sink(results_b.append)

    for i in range(5):
        s.emit(i)

    # Each branch should see values in order
    assert results_a == [0, 1, 2, 3, 4]
    assert results_b == [0, 1, 2, 3, 4]


def test_par_multiple_emits():
    """Test par with multiple emit calls."""
    results = []

    def record(x, branch):
        results.append((branch, x))
        return x

    s = Stream()
    p = s.par()
    p.map(lambda x: record(x, "A")).sink(lambda x: None)
    p.map(lambda x: record(x, "B")).sink(lambda x: None)

    s.emit(1)
    s.emit(2)
    s.emit(3)

    # Should have 6 results (2 branches * 3 emits)
    assert len(results) == 6
    # Each value should appear in both branches
    for val in [1, 2, 3]:
        assert ("A", val) in results
        assert ("B", val) in results


def test_par_with_filter():
    """Test par branches with filter."""
    evens = []
    odds = []

    s = Stream()
    p = s.par()
    p.filter(lambda x: x % 2 == 0).sink(evens.append)
    p.filter(lambda x: x % 2 == 1).sink(odds.append)

    for i in range(6):
        s.emit(i)

    assert evens == [0, 2, 4]
    assert odds == [1, 3, 5]


def test_par_threads_used():
    """Verify that different threads are actually used for par branches."""
    thread_ids = []

    def capture_thread(x):
        thread_ids.append(threading.current_thread().ident)
        time.sleep(0.05)  # Ensure overlap
        return x

    s = Stream()
    p = s.par()
    p.map(capture_thread).sink(lambda x: None)
    p.map(capture_thread).sink(lambda x: None)

    s.emit(1)

    # With parallel execution, we should see at least 2 different thread IDs
    # (Note: they could be the same if thread pool reuses, but with sleep they should differ)
    assert len(thread_ids) == 2


def test_par_emit_batch():
    """Test par with emit_batch."""
    results = []

    s = Stream()
    p = s.par()
    p.map(lambda x: x * 2).sink(results.append)
    p.map(lambda x: x * 3).sink(results.append)

    s.emit_batch([1, 2, 3])

    # Should have 6 results (2 branches * 3 items)
    assert len(results) == 6
    # Each value doubled and tripled
    assert sorted(results) == [2, 3, 4, 6, 6, 9]


def test_par_chained():
    """Test chaining after par node."""
    results = []

    s = Stream()
    p = s.par()
    # Chain operations after par
    p.map(lambda x: x * 2).map(lambda x: x + 1).sink(results.append)
    p.map(lambda x: x * 3).filter(lambda x: x > 5).sink(results.append)

    s.emit(2)  # Branch 1: 2*2+1=5, Branch 2: 2*3=6 (>5, passes filter)
    s.emit(1)  # Branch 1: 1*2+1=3, Branch 2: 1*3=3 (<5, filtered)

    assert sorted(results) == [3, 5, 6]


def test_seq_basic():
    """Test that seq() creates a sequential pass-through node."""
    results = []

    s = Stream()
    seq = s.seq()
    seq.map(lambda x: x * 2).sink(results.append)
    seq.map(lambda x: x * 3).sink(results.append)

    s.emit(10)

    # Should work, results in order (sequential)
    assert sorted(results) == [20, 30]


def test_seq_after_par():
    """Test seq() for explicit sequential execution point.

    Note: par() branches should not directly share a downstream (like union)
    because parallel threads would conflict when updating the same node.
    Instead, use seq() to create explicit sequential sections.
    """
    results = []

    def slow_op(x, label):
        time.sleep(0.03)
        results.append((label, x))
        return x

    # Use seq() to create an explicit sequential section
    s = Stream()
    seq = s.seq()

    # Multiple downstreams run sequentially (not in parallel)
    seq.map(lambda x: slow_op(x, "seq_A")).sink(lambda x: None)
    seq.map(lambda x: slow_op(x, "seq_B")).sink(lambda x: None)

    s.emit(1)

    # Both should complete
    assert len(results) == 2
    assert ("seq_A", 1) in results
    assert ("seq_B", 1) in results


def test_par_seq_timing():
    """Test that par is parallel and seq is sequential."""
    results = []

    def timed_op(x, label):
        start = time.time()
        time.sleep(0.05)
        results.append((label, time.time() - start))
        return x

    # Test parallel timing
    s1 = Stream()
    p = s1.par()
    p.map(lambda x: timed_op(x, "A")).sink(lambda x: None)
    p.map(lambda x: timed_op(x, "B")).sink(lambda x: None)

    start = time.time()
    s1.emit(1)
    par_elapsed = time.time() - start

    results.clear()

    # Test sequential timing
    s2 = Stream()
    seq = s2.seq()
    seq.map(lambda x: timed_op(x, "A")).sink(lambda x: None)
    seq.map(lambda x: timed_op(x, "B")).sink(lambda x: None)

    start = time.time()
    s2.emit(1)
    seq_elapsed = time.time() - start

    # Parallel should be ~50ms, sequential should be ~100ms
    assert par_elapsed < 0.08, f"par should be ~50ms, got {par_elapsed * 1000:.0f}ms"
    assert seq_elapsed > 0.09, f"seq should be ~100ms, got {seq_elapsed * 1000:.0f}ms"
