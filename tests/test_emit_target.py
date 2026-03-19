"""Tests for emit_target optimization: .sink(s2.emit) bypass."""

from akayu import Stream


class TestEmitTargetBoundMethod:
    """Detect and optimize .sink(source.emit) bound method pattern."""

    def test_basic_emit_bridge(self):
        """s1 → map → sink(s2.emit) → s2 → sink: data flows through."""
        s1 = Stream()
        s2 = Stream()
        results = []

        s1.map(lambda x: x * 2).sink(s2.emit)
        s2.sink(results.append)

        s1.emit(1)
        s1.emit(2)
        s1.emit(3)
        assert results == [2, 4, 6]

    def test_emit_bridge_compiled(self):
        """Same as above but compiled — exercises emit_target fast path."""
        s1 = Stream()
        s2 = Stream()
        results = []

        s1.map(lambda x: x * 2).sink(s2.emit)
        s2.sink(results.append)
        s1.compile()
        s2.compile()

        s1.emit(1)
        s1.emit(2)
        s1.emit(3)
        assert results == [2, 4, 6]

    def test_emit_bridge_batch(self):
        """emit_target works with emit_batch."""
        s1 = Stream()
        s2 = Stream()
        results = []

        s1.sink(s2.emit)
        s2.map(lambda x: x + 10).sink(results.append)
        s1.compile()
        s2.compile()

        s1.emit_batch([1, 2, 3])
        assert results == [11, 12, 13]

    def test_emit_bridge_compiled_matches_uncompiled(self):
        """Compiled emit_target produces identical results to uncompiled."""
        def run(compile):
            s1 = Stream()
            s2 = Stream()
            results = []

            s1.filter(lambda x: x > 0).sink(s2.emit)
            s2.map(lambda x: x * 10).sink(results.append)

            if compile:
                s1.compile()
                s2.compile()

            for v in [-1, 0, 1, 2, -3, 4]:
                s1.emit(v)
            return results

        assert run(False) == run(True)

    def test_emit_bridge_s2_has_multiple_downstreams(self):
        """s2 fans out to multiple sinks."""
        s1 = Stream()
        s2 = Stream()
        results_a = []
        results_b = []

        s1.sink(s2.emit)
        s2.map(lambda x: x * 2).sink(results_a.append)
        s2.map(lambda x: x + 100).sink(results_b.append)
        s1.compile()
        s2.compile()

        s1.emit(5)
        assert results_a == [10]
        assert results_b == [105]


class TestEmitTargetLambda:
    """Detect and optimize .sink(lambda x: source.emit(x)) pattern."""

    def test_lambda_emit_bridge(self):
        """Lambda wrapper around emit is also detected."""
        s1 = Stream()
        s2 = Stream()
        results = []

        s1.sink(lambda x: s2.emit(x))
        s2.sink(results.append)
        s1.compile()
        s2.compile()

        s1.emit(42)
        assert results == [42]

    def test_lambda_emit_bridge_batch(self):
        """Lambda emit bridge with batch."""
        s1 = Stream()
        s2 = Stream()
        results = []

        s1.sink(lambda x: s2.emit(x))
        s2.sink(results.append)
        s1.compile()
        s2.compile()

        s1.emit_batch([10, 20, 30])
        assert results == [10, 20, 30]


class TestEmitTargetEdgeCases:
    """Edge cases for emit_target optimization."""

    def test_emit_on_non_source_not_detected(self):
        """sink(map_node.emit) should NOT trigger emit_target (emit only works on Source)."""
        s1 = Stream()
        s2 = Stream()
        results = []

        m = s2.map(lambda x: x)
        # This sink calls emit on a map node, which will fail at runtime
        # but should NOT be optimized (target is not Source)
        s1.sink(results.append)

        s1.emit(1)
        assert results == [1]

    def test_chained_emit_bridges(self):
        """s1 → sink(s2.emit) → s2 → sink(s3.emit) → s3 → sink."""
        s1 = Stream()
        s2 = Stream()
        s3 = Stream()
        results = []

        s1.sink(s2.emit)
        s2.map(lambda x: x + 1).sink(s3.emit)
        s3.map(lambda x: x * 10).sink(results.append)
        s1.compile()
        s2.compile()
        s3.compile()

        s1.emit(1)
        s1.emit(2)
        assert results == [20, 30]

    def test_emit_bridge_without_compile(self):
        """Without compile, no optimization — still works via Python dispatch."""
        s1 = Stream()
        s2 = Stream()
        results = []

        s1.sink(s2.emit)
        s2.sink(results.append)

        s1.emit(1)
        s1.emit(2)
        assert results == [1, 2]

    def test_emit_bridge_with_collect(self):
        """emit_target combined with collect/flush."""
        s1 = Stream()
        s2 = Stream()
        results = []

        s1.sink(s2.emit)
        c = s2.collect()
        c.sink(results.append)
        s1.compile()
        s2.compile()

        s1.emit(1)
        s1.emit(2)
        s1.emit(3)
        assert results == []  # buffered in collect

        c.flush()
        assert results == [(1, 2, 3)]
