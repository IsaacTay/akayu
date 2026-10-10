"""Comprehensive tests for collect() and flush() behavior."""

from akayu import Stream

# ---------------------------------------------------------------------------
# External flush (called after emits, not from within the graph)
# ---------------------------------------------------------------------------


class TestExternalFlush:
    """flush() called manually from outside the graph."""

    def test_basic_external_flush(self):
        s = Stream()
        results = []
        c = s.collect()
        c.sink(results.append)

        s.emit(1)
        s.emit(2)
        s.emit(3)
        c.flush()

        assert results == [(1, 2, 3)]

    def test_multiple_external_flushes(self):
        s = Stream()
        results = []
        c = s.collect()
        c.sink(results.append)

        s.emit(1)
        s.emit(2)
        c.flush()

        s.emit(3)
        s.emit(4)
        c.flush()

        assert results == [(1, 2), (3, 4)]

    def test_flush_empty_collect(self):
        """Flushing an empty collect should emit an empty tuple."""
        s = Stream()
        results = []
        c = s.collect()
        c.sink(results.append)

        c.flush()
        assert results == [()]

    def test_flush_after_batch(self):
        s = Stream()
        results = []
        c = s.collect()
        c.sink(results.append)

        s.emit_batch([1, 2, 3])
        c.flush()
        assert results == [(1, 2, 3)]

    def test_flush_interleaved_emit_and_batch(self):
        s = Stream()
        results = []
        c = s.collect()
        c.sink(results.append)

        s.emit(1)
        s.emit_batch([2, 3])
        s.emit(4)
        c.flush()

        assert results == [(1, 2, 3, 4)]

    def test_external_flush_compiled(self):
        """External flush still works after compile()."""
        s = Stream()
        results = []
        c = s.collect()
        c.sink(results.append)
        s.compile()

        s.emit(1)
        s.emit(2)
        c.flush()

        assert results == [(1, 2)]

    def test_external_flush_compiled_batch(self):
        s = Stream()
        results = []
        c = s.collect()
        c.sink(results.append)
        s.compile()

        s.emit_batch([10, 20, 30])
        c.flush()

        assert results == [(10, 20, 30)]

    def test_source_level_flush_propagates(self):
        """Calling flush on the source propagates to all downstream collects."""
        s = Stream()
        results = []
        c = s.collect()
        c.sink(results.append)

        s.emit(1)
        s.emit(2)
        s.flush()  # flush on source, not on collect directly

        assert results == [(1, 2)]

    def test_source_flush_compiled(self):
        s = Stream()
        results = []
        c = s.collect()
        c.sink(results.append)
        s.compile()

        s.emit(1)
        s.emit(2)
        s.flush()

        assert results == [(1, 2)]


# ---------------------------------------------------------------------------
# Multi-stage collect (chained collects)
# ---------------------------------------------------------------------------


class TestMultiStageCollect:
    """Multiple collect nodes in sequence or branching patterns."""

    def test_two_stage_collect(self):
        """collect → map → collect: c1.flush does NOT cascade to c2.

        c1.flush() emits its buffer downstream (map→c2 buffers), but does
        NOT call flush on c2. c2 must be flushed independently.
        """
        s = Stream()
        results = []
        c1 = s.collect()
        c2 = c1.map(lambda t: sum(t)).collect()
        c2.sink(results.append)

        s.emit(1)
        s.emit(2)
        c1.flush()
        # c1 emits (1, 2) → map produces 3 → c2 buffers 3
        # c1 does NOT cascade flush to c2
        assert results == []

        s.emit(3)
        s.emit(4)
        c1.flush()
        # c1 emits (3, 4) → map produces 7 → c2 buffers [3, 7]
        assert results == []

        # Explicit c2 flush drains everything accumulated
        c2.flush()
        assert results == [(3, 7)]

    def test_two_stage_collect_independent_flush(self):
        """c1.flush sends data to c2's buffer; c2.flush drains it separately."""
        s = Stream()
        results = []
        c1 = s.collect()
        c2 = c1.map(lambda t: sum(t)).collect()
        c2.sink(results.append)

        s.emit(1)
        s.emit(2)
        c1.flush()
        # c1 emits (1,2) → map → c2 buffers 3, but c2 NOT flushed
        assert results == []

        c2.flush()
        assert results == [(3,)]

    def test_source_flush_does_not_cascade_through_collects(self):
        """s.flush() flushes c1 (Source propagates), but c1 does NOT cascade to c2."""
        s = Stream()
        results = []
        c1 = s.collect()
        c2 = c1.collect()
        c2.sink(results.append)

        s.emit(1)
        s.emit(2)
        s.flush()

        # s.flush() propagates → c1.flush emits (1, 2) → c2 buffers (1, 2)
        # c1 does NOT cascade flush to c2
        assert results == []

        c2.flush()
        assert results == [((1, 2),)]

    def test_parallel_collects(self):
        """Two independent collects from same source."""
        s = Stream()
        results_a = []
        results_b = []

        c_a = s.map(lambda x: x * 2).collect()
        c_a.sink(results_a.append)

        c_b = s.map(lambda x: x + 10).collect()
        c_b.sink(results_b.append)

        s.emit(1)
        s.emit(2)

        c_a.flush()
        assert results_a == [(2, 4)]
        assert results_b == []

        c_b.flush()
        assert results_b == [(11, 12)]

    def test_parallel_collects_source_flush(self):
        """Source flush hits both parallel collects."""
        s = Stream()
        results_a = []
        results_b = []

        c_a = s.collect()
        c_a.sink(results_a.append)

        c_b = s.collect()
        c_b.sink(results_b.append)

        s.emit(1)
        s.emit(2)
        s.flush()

        assert results_a == [(1, 2)]
        assert results_b == [(1, 2)]


# ---------------------------------------------------------------------------
# Collect with transforms
# ---------------------------------------------------------------------------


class TestCollectWithTransforms:
    """collect() combined with map/filter in the pipeline."""

    def test_map_before_collect(self):
        s = Stream()
        results = []
        c = s.map(lambda x: x**2).collect()
        c.sink(results.append)

        s.emit(1)
        s.emit(2)
        s.emit(3)
        c.flush()

        assert results == [(1, 4, 9)]

    def test_filter_before_collect(self):
        s = Stream()
        results = []
        c = s.filter(lambda x: x % 2 == 0).collect()
        c.sink(results.append)

        s.emit(1)
        s.emit(2)
        s.emit(3)
        s.emit(4)
        c.flush()

        assert results == [(2, 4)]

    def test_map_after_collect(self):
        s = Stream()
        results = []
        c = s.collect()
        c.map(lambda t: len(t)).sink(results.append)

        s.emit("a")
        s.emit("b")
        s.emit("c")
        c.flush()

        assert results == [3]

    def test_filter_after_collect(self):
        """Filter after collect: only passes tuples matching predicate."""
        s = Stream()
        results = []
        c = s.collect()
        c.filter(lambda t: len(t) >= 2).sink(results.append)

        s.emit(1)
        c.flush()  # (1,) → filtered out

        s.emit(2)
        s.emit(3)
        c.flush()  # (2, 3) → passes

        assert results == [(2, 3)]

    def test_flatten_after_collect(self):
        """collect → flatten turns the tuple back into individual items."""
        s = Stream()
        results = []
        c = s.collect()
        c.flatten().sink(results.append)

        s.emit(1)
        s.emit(2)
        c.flush()

        assert results == [1, 2]


# ---------------------------------------------------------------------------
# In-graph flush (filter→sink pattern triggering flush)
# ---------------------------------------------------------------------------


class TestInGraphFlush:
    """Flush triggered from within the graph via filter→sink."""

    def test_basic_in_graph_flush(self):
        s = Stream()
        results = []
        c = s.collect()
        c.sink(results.append)
        s.filter(lambda x: x == 0).sink(lambda _: c.flush())

        s.emit(1)
        s.emit(2)
        s.emit(0)
        s.emit(3)
        s.emit(0)

        assert results == [(1, 2, 0), (3, 0)]

    def test_in_graph_flush_compiled(self):
        s = Stream()
        results = []
        c = s.collect()
        c.sink(results.append)
        s.filter(lambda x: x == 0).sink(lambda _: c.flush())
        s.compile()

        s.emit(1)
        s.emit(2)
        s.emit(0)
        s.emit(3)
        s.emit(0)

        assert results == [(1, 2, 0), (3, 0)]

    def test_in_graph_flush_with_external_flush_for_remainder(self):
        """In-graph flush handles delimiters; external flush handles leftover."""
        s = Stream()
        results = []
        c = s.collect()
        c.sink(results.append)
        s.filter(lambda x: x == "|").sink(lambda _: c.flush())

        s.emit("a")
        s.emit("|")
        s.emit("b")
        s.emit("c")
        # No trailing delimiter — need external flush
        c.flush()

        assert results == [("a", "|"), ("b", "c")]

    def test_in_graph_flush_with_external_flush_compiled(self):
        s = Stream()
        results = []
        c = s.collect()
        c.sink(results.append)
        s.filter(lambda x: x == "|").sink(lambda _: c.flush())
        s.compile()

        s.emit("a")
        s.emit("|")
        s.emit("b")
        s.emit("c")
        c.flush()

        assert results == [("a", "|"), ("b", "c")]

    def test_in_graph_flush_bound_method(self):
        """sink(c.flush) form — bound method."""
        s = Stream()
        results = []
        c = s.collect()
        c.sink(results.append)
        s.filter(lambda x: x == 0).sink(c.flush)

        s.emit(1)
        s.emit(0)
        s.emit(2)
        s.emit(3)
        s.emit(0)

        assert results == [(1, 0), (2, 3, 0)]

    def test_in_graph_flush_bound_method_compiled(self):
        s = Stream()
        results = []
        c = s.collect()
        c.sink(results.append)
        s.filter(lambda x: x == 0).sink(c.flush)
        s.compile()

        s.emit(1)
        s.emit(0)
        s.emit(2)
        s.emit(3)
        s.emit(0)

        assert results == [(1, 0), (2, 3, 0)]

    def test_in_graph_flush_batch(self):
        s = Stream()
        results = []
        c = s.collect()
        c.sink(results.append)
        s.filter(lambda x: x == 0).sink(c.flush)

        s.emit_batch([1, 2, 0, 3, 0])
        assert results == [(1, 2, 0), (3, 0)]

    def test_in_graph_flush_batch_compiled(self):
        s = Stream()
        results = []
        c = s.collect()
        c.sink(results.append)
        s.filter(lambda x: x == 0).sink(c.flush)
        s.compile()

        s.emit_batch([1, 2, 0, 3, 0])
        assert results == [(1, 2, 0), (3, 0)]


# ---------------------------------------------------------------------------
# Compiled vs uncompiled consistency
# ---------------------------------------------------------------------------


class TestCompiledConsistency:
    """Ensure compiled and uncompiled produce identical results."""

    def _run_pipeline(self, compile):
        s = Stream()
        results = []
        c = s.collect()
        c.map(lambda t: "".join(str(x) for x in t)).sink(results.append)
        s.filter(lambda x: x % 5 == 0).sink(c.flush)

        if compile:
            s.compile()

        for i in range(1, 16):
            s.emit(i)
        c.flush()  # remainder

        return results

    def test_compiled_matches_uncompiled(self):
        uncompiled = self._run_pipeline(compile=False)
        compiled = self._run_pipeline(compile=True)
        assert uncompiled == compiled

    def _run_char_pipeline(self, compile):
        s = Stream()
        results = []
        c = s.collect()
        c.map(lambda t: "".join(t)).sink(results.append)
        s.filter(lambda x: x == "\n").sink(c.flush)

        if compile:
            s.compile()

        for ch in "hello\nworld\nfoo":
            s.emit(ch)
        s.flush()

        return results

    def test_char_pipeline_compiled_matches(self):
        uncompiled = self._run_char_pipeline(compile=False)
        compiled = self._run_char_pipeline(compile=True)
        assert uncompiled == compiled
        assert compiled == ["hello\n", "world\n", "foo"]

    def _run_batch_pipeline(self, compile):
        s = Stream()
        results = []
        c = s.collect()
        c.sink(results.append)
        s.filter(lambda x: x == "|").sink(c.flush)

        if compile:
            s.compile()

        s.emit_batch(list("ab|cd|ef"))
        s.flush()

        return results

    def test_batch_compiled_matches(self):
        uncompiled = self._run_batch_pipeline(compile=False)
        compiled = self._run_batch_pipeline(compile=True)
        assert uncompiled == compiled


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestCollectFlushEdgeCases:
    def test_collect_no_downstreams(self):
        """Collect with no sink — flush shouldn't crash."""
        s = Stream()
        c = s.collect()

        s.emit(1)
        s.emit(2)
        c.flush()  # no error

    def test_rapid_flush(self):
        """Multiple flushes with no emits in between."""
        s = Stream()
        results = []
        c = s.collect()
        c.sink(results.append)

        s.emit(1)
        c.flush()
        c.flush()
        c.flush()

        assert results == [(1,), (), ()]

    def test_flush_idempotent_on_non_collect(self):
        """Flush on a map node should be a no-op (propagates to children)."""
        s = Stream()
        results = []
        m = s.map(lambda x: x * 2)
        m.sink(results.append)

        s.emit(5)
        m.flush()  # should not crash or duplicate

        assert results == [10]

    def test_collect_large_buffer(self):
        """Collect handles large number of items."""
        s = Stream()
        results = []
        c = s.collect()
        c.map(len).sink(results.append)

        for i in range(10000):
            s.emit(i)
        c.flush()

        assert results == [10000]

    def test_collect_large_buffer_batch(self):
        s = Stream()
        results = []
        c = s.collect()
        c.map(len).sink(results.append)

        s.emit_batch(list(range(10000)))
        c.flush()

        assert results == [10000]

    def test_in_graph_flush_no_items_between_delimiters(self):
        """Back-to-back delimiters: buffer contains only the delimiter."""
        s = Stream()
        results = []
        c = s.collect()
        c.sink(results.append)
        s.filter(lambda x: x == 0).sink(c.flush)

        s.emit(0)
        s.emit(0)
        s.emit(0)

        # Each 0 triggers flush. Buffer at each flush: (0,)
        assert results == [(0,), (0,), (0,)]

    def test_collect_with_none_values(self):
        """Collect handles None values without confusion."""
        s = Stream()
        results = []
        c = s.collect()
        c.sink(results.append)

        s.emit(None)
        s.emit(1)
        s.emit(None)
        c.flush()

        assert results == [(None, 1, None)]

    def test_collect_with_mixed_types(self):
        s = Stream()
        results = []
        c = s.collect()
        c.sink(results.append)

        s.emit(1)
        s.emit("hello")
        s.emit([1, 2])
        s.emit({"key": "val"})
        c.flush()

        assert results == [(1, "hello", [1, 2], {"key": "val"})]


# ---------------------------------------------------------------------------
# Warning on no-op flush
# ---------------------------------------------------------------------------


class TestFlushWarning:
    """flush() on a leaf non-buffering node should emit a UserWarning."""

    def test_flush_on_leaf_non_buffer_warns(self):
        """Flushing a leaf non-buffering node (e.g. filter with no downstreams) should warn."""
        import warnings

        s = Stream()
        f = s.filter(lambda x: x > 0)
        # f has no downstreams — flushing it is almost certainly a mistake

        s.emit(1)
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            f.flush()
            assert len(w) == 1
            assert "no buffer and no downstreams" in str(w[0].message)
            assert issubclass(w[0].category, UserWarning)

    def test_flush_on_collect_does_not_warn(self):
        """Flushing a collect node should NOT warn."""
        import warnings

        s = Stream()
        c = s.collect()
        c.sink(lambda x: None)

        s.emit(1)
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            c.flush()
            user_warnings = [x for x in w if issubclass(x.category, UserWarning)]
            assert len(user_warnings) == 0

    def test_flush_on_map_with_downstream_no_warn(self):
        """Flushing a map node that HAS downstreams should not warn."""
        import warnings

        s = Stream()
        m = s.map(lambda x: x * 2)
        m.sink(lambda x: None)

        s.emit(1)
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            m.flush()
            user_warnings = [x for x in w if issubclass(x.category, UserWarning)]
            assert len(user_warnings) == 0


# ---------------------------------------------------------------------------
# drain() — recursive flush
# ---------------------------------------------------------------------------


class TestDrain:
    """drain() recursively flushes this node and all descendants."""

    def test_drain_single_collect(self):
        """drain() on a single collect behaves like flush()."""
        s = Stream()
        results = []
        c = s.collect()
        c.sink(results.append)

        s.emit(1)
        s.emit(2)
        c.drain()

        assert results == [(1, 2)]

    def test_drain_two_stage_collect(self):
        """drain() on c1 flushes c1 AND recursively drains c2."""
        s = Stream()
        results = []
        c1 = s.collect()
        c2 = c1.map(lambda t: sum(t)).collect()
        c2.sink(results.append)

        s.emit(1)
        s.emit(2)
        c1.flush()  # c1 emits (1,2) → map → c2 buffers 3
        assert results == []

        s.emit(3)
        s.emit(4)
        # drain c1: flushes c1 buffer (3,4) → map → c2 buffers 7
        # then recursively drains c2 → c2 emits (3, 7)
        c1.drain()
        assert results == [(3, 7)]

    def test_drain_from_source(self):
        """drain() on source cascades through all collects."""
        s = Stream()
        results = []
        c1 = s.collect()
        c2 = c1.collect()
        c2.sink(results.append)

        s.emit(1)
        s.emit(2)
        s.drain()

        # Source drains → c1 flushes (1,2) → c2 buffers (1,2) → c2 drains → emits ((1,2),)
        assert results == [((1, 2),)]

    def test_drain_parallel_collects(self):
        """drain() on source reaches both parallel collects."""
        s = Stream()
        results_a = []
        results_b = []

        c_a = s.collect()
        c_a.sink(results_a.append)

        c_b = s.collect()
        c_b.sink(results_b.append)

        s.emit(1)
        s.emit(2)
        s.drain()

        assert results_a == [(1, 2)]
        assert results_b == [(1, 2)]

    def test_drain_vs_flush_multi_stage(self):
        """flush() stops at collect boundaries; drain() goes through them."""
        s = Stream()
        flush_results = []
        drain_results = []

        # flush pipeline
        c1f = s.collect()
        c2f = c1f.collect()
        c2f.sink(flush_results.append)

        # drain pipeline
        c1d = s.collect()
        c2d = c1d.collect()
        c2d.sink(drain_results.append)

        s.emit(1)
        s.emit(2)

        c1f.flush()  # does NOT cascade to c2f
        c1d.drain()  # DOES cascade to c2d

        assert flush_results == []
        assert drain_results == [((1, 2),)]


# ---------------------------------------------------------------------------
# Re-entrant flush detection
# ---------------------------------------------------------------------------


class TestReentrantFlush:
    """Re-entrant flush/drain must raise a clear error, not hang or UB."""

    def test_self_flush_in_sink_errors(self):
        """collect → sink(c.flush) where sink flushes the SAME collect."""
        import pytest

        s = Stream()
        c = s.collect()
        c.sink(lambda t: c.flush())

        s.emit(1)
        s.emit(2)
        # c.flush() → propagate tuple → sink calls c.flush() → re-entrant
        # PyO3 catches this with "Already borrowed" or our guard catches with "Re-entrant"
        # Either way, wrap_error wraps it as "Stream operation failed"
        with pytest.raises(RuntimeError):
            c.flush()

    def test_self_drain_in_sink_errors(self):
        """collect → sink(c.drain) where sink drains the SAME collect."""
        import pytest

        s = Stream()
        c = s.collect()
        c.sink(lambda t: c.drain())

        s.emit(1)
        with pytest.raises(RuntimeError):
            c.drain()

    def test_mutual_flush_cycle_errors(self):
        """Two collects flushing each other: c1 → sink(c2.flush), c2 → sink(c1.flush)."""
        import pytest

        s = Stream()
        c1 = s.collect()
        c2 = s.collect()
        c1.sink(lambda t: c2.flush())
        c2.sink(lambda t: c1.flush())

        s.emit(1)
        # c1.flush → propagate → sink calls c2.flush → c2 propagate → sink calls c1.flush
        # c1 is still borrowed from the outer call → error
        with pytest.raises(RuntimeError):
            c1.flush()

    def test_reentrant_flush_does_not_corrupt_state(self):
        """After a re-entrant error, the collect should still be usable."""
        import pytest

        s = Stream()
        results = []
        c = s.collect()

        call_count = [0]

        def maybe_reentrant(t):
            call_count[0] += 1
            if call_count[0] == 1:
                c.flush()  # re-entrant on first call
            results.append(t)

        c.sink(maybe_reentrant)

        s.emit(1)
        s.emit(2)
        with pytest.raises(RuntimeError):
            c.flush()

        # The collect should still be usable after the error
        s.emit(3)
        s.emit(4)
        c.flush()
        assert results == [(3, 4)]
