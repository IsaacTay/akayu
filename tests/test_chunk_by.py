"""Tests for collect/flush optimization (Tier 1: direct Rust flush) and ChunkBy (Tier 2)."""

from akayu import Stream

# ---------------------------------------------------------------------------
# Tier 1: Direct Rust flush (flush_target bypass)
# ---------------------------------------------------------------------------


class TestFlushTarget:
    """Tier 1: Sink calling .flush() on Collect bypasses Python dispatch."""

    def test_bound_method_flush(self):
        """sink(collected.flush) is detected and optimized."""
        s = Stream()
        results = []
        collected = s.collect()
        collected.sink(results.append)
        s.filter(lambda x: x == "delim").sink(collected.flush)
        s.compile()

        s.emit("a")
        s.emit("b")
        s.emit("delim")
        assert results == [("a", "b", "delim")]

    def test_lambda_flush(self):
        """sink(lambda _: collected.flush()) is detected and optimized."""
        s = Stream()
        results = []
        collected = s.collect()
        collected.sink(results.append)
        s.filter(lambda x: x == "delim").sink(lambda _: collected.flush())
        s.compile()

        s.emit("a")
        s.emit("delim")
        s.emit("c")
        s.emit("delim")
        assert results == [("a", "delim"), ("c", "delim")]

    def test_flush_batch(self):
        """flush_target works with emit_batch."""
        s = Stream()
        results = []
        collected = s.collect()
        collected.sink(results.append)
        s.filter(lambda x: x == "\n").sink(collected.flush)
        s.compile()

        s.emit_batch(list("ab\ncd\n"))
        assert results == [("a", "b", "\n"), ("c", "d", "\n")]

    def test_flush_target_only_on_collect(self):
        """flush_target is NOT set for non-collect nodes."""
        s = Stream()
        # A sink that calls flush on a non-collect stream should not be optimized
        other = Stream()
        s.sink(lambda _: other.flush())
        s.compile()
        # Should still work (just not optimized)
        s.emit("x")

    def test_flush_without_compile(self):
        """Without compile(), flush still works via normal Python dispatch."""
        s = Stream()
        results = []
        collected = s.collect()
        collected.sink(results.append)
        s.filter(lambda x: x == "\n").sink(collected.flush)

        for c in "hello\nworld\n":
            s.emit(c)

        assert results == [
            ("h", "e", "l", "l", "o", "\n"),
            ("w", "o", "r", "l", "d", "\n"),
        ]


# ---------------------------------------------------------------------------
# Tier 2: ChunkBy — direct API
# ---------------------------------------------------------------------------


class TestChunkByDirect:
    """chunk_by() API without compile-time fusion."""

    def test_basic(self):
        s = Stream()
        results = []
        s.chunk_by(lambda x: x == "\n", include_delimiter=True).sink(results.append)

        for c in "hello\nworld\n":
            s.emit(c)

        assert results == [
            ("h", "e", "l", "l", "o", "\n"),
            ("w", "o", "r", "l", "d", "\n"),
        ]

    def test_exclude_delimiter(self):
        s = Stream()
        results = []
        s.chunk_by(lambda x: x == "\n", include_delimiter=False).sink(results.append)

        for c in "hello\nworld\n":
            s.emit(c)

        assert results == [
            ("h", "e", "l", "l", "o"),
            ("w", "o", "r", "l", "d"),
        ]

    def test_flush_remaining(self):
        """flush() emits remaining buffer."""
        s = Stream()
        results = []
        s.chunk_by(lambda x: x == "\n", include_delimiter=True).sink(results.append)

        for c in "hello\npartial":
            s.emit(c)
        s.flush()

        assert results == [
            ("h", "e", "l", "l", "o", "\n"),
            ("p", "a", "r", "t", "i", "a", "l"),
        ]

    def test_emit_batch(self):
        s = Stream()
        results = []
        s.chunk_by(lambda x: x == "\n", include_delimiter=True).sink(results.append)

        s.emit_batch(list("hello\nworld\n"))

        assert results == [
            ("h", "e", "l", "l", "o", "\n"),
            ("w", "o", "r", "l", "d", "\n"),
        ]

    def test_with_downstream_map(self):
        """chunk_by → map → sink pipeline."""
        s = Stream()
        results = []
        s.chunk_by(lambda x: x == "\n", include_delimiter=True).map(lambda t: "".join(t)).sink(results.append)

        for c in "hello\nworld\n":
            s.emit(c)

        assert results == ["hello\n", "world\n"]

    def test_compiled(self):
        """chunk_by with compile() still works."""
        s = Stream()
        results = []
        s.chunk_by(lambda x: x == "\n", include_delimiter=True).map(lambda t: "".join(t)).sink(results.append)
        s.compile()

        for c in "hello\nworld\n":
            s.emit(c)

        assert results == ["hello\n", "world\n"]

    def test_empty_chunks(self):
        """Consecutive delimiters produce no empty chunks (buffer is empty)."""
        s = Stream()
        results = []
        s.chunk_by(lambda x: x == "\n", include_delimiter=True).sink(results.append)

        for c in "\n\n":
            s.emit(c)

        # First \n: buffer has just "\n" → emit ("\n",)
        # Second \n: buffer has just "\n" → emit ("\n",)
        assert results == [("\n",), ("\n",)]

    def test_empty_chunks_exclude_delimiter(self):
        """Consecutive delimiters with include_delimiter=False produce no output."""
        s = Stream()
        results = []
        s.chunk_by(lambda x: x == "\n", include_delimiter=False).sink(results.append)

        for c in "\n\n":
            s.emit(c)

        # With include_delimiter=False: delimiter popped, buffer empty → no emit
        assert results == []

    def test_non_char_predicate(self):
        """chunk_by works with non-char predicates."""
        s = Stream()
        results = []
        s.chunk_by(lambda x: x > 10, include_delimiter=True).sink(results.append)

        for v in [1, 2, 3, 100, 4, 5, 200]:
            s.emit(v)

        assert results == [(1, 2, 3, 100), (4, 5, 200)]


# ---------------------------------------------------------------------------
# Tier 2: ChunkBy — compile-time fusion
# ---------------------------------------------------------------------------


class TestChunkByFusion:
    """collect() + filter(pred).sink(flush) fused into ChunkBy at compile time."""

    def test_basic_fusion(self):
        """Direct: source → collect + source → filter → sink(flush)."""
        s = Stream()
        results = []
        collected = s.collect()
        collected.sink(results.append)
        s.filter(lambda x: x == "\n").sink(collected.flush)
        s.compile()

        for c in "hello\nworld\n":
            s.emit(c)

        assert results == [
            ("h", "e", "l", "l", "o", "\n"),
            ("w", "o", "r", "l", "d", "\n"),
        ]

    def test_fusion_with_transforms(self):
        """Transforms between branch point and collect are preserved."""
        s = Stream()
        results = []
        collected = s.map(str.upper).collect()
        collected.map(lambda t: "".join(t)).sink(results.append)
        s.filter(lambda x: x == "\n").sink(collected.flush)
        s.compile()

        for c in "hello\nworld\n":
            s.emit(c)

        assert results == ["HELLO\n", "WORLD\n"]

    def test_fusion_lambda_sink(self):
        """Lambda sink: sink(lambda _: collected.flush())."""
        s = Stream()
        results = []
        collected = s.collect()
        collected.map(lambda t: "".join(t)).sink(results.append)
        s.filter(lambda x: x == "\n").sink(lambda _: collected.flush())
        s.compile()

        for c in "hello\nworld\n":
            s.emit(c)

        assert results == ["hello\n", "world\n"]

    def test_fusion_batch(self):
        """Fused ChunkBy works with emit_batch."""
        s = Stream()
        results = []
        collected = s.collect()
        collected.map(lambda t: "".join(t)).sink(results.append)
        s.filter(lambda x: x == "\n").sink(collected.flush)
        s.compile()

        s.emit_batch(list("hello\nworld\n"))

        assert results == ["hello\n", "world\n"]

    def test_fusion_flush_remaining(self):
        """Fused ChunkBy emits remaining buffer on flush."""
        s = Stream()
        results = []
        collected = s.collect()
        collected.map(lambda t: "".join(t)).sink(results.append)
        s.filter(lambda x: x == "\n").sink(collected.flush)
        s.compile()

        for c in "hello\npartial":
            s.emit(c)
        s.flush()

        assert results == ["hello\n", "partial"]

    def test_non_fusable_pattern_preserved(self):
        """Patterns that can't be fused still work normally."""
        s = Stream()
        results = []
        collected = s.collect()
        collected.sink(results.append)
        # Manually flush (not from a filter/sink pattern)
        s.compile()

        s.emit("a")
        s.emit("b")
        collected.flush()

        assert results == [("a", "b")]

    def test_fusion_same_as_unfused(self):
        """Fused output matches unfused output exactly."""
        chars = list("hello\nworld\nfoo\n")

        # Unfused
        s1 = Stream()
        results1 = []
        collected1 = s1.collect()
        collected1.sink(results1.append)
        s1.filter(lambda x: x == "\n").sink(collected1.flush)
        for c in chars:
            s1.emit(c)

        # Fused
        s2 = Stream()
        results2 = []
        collected2 = s2.collect()
        collected2.sink(results2.append)
        s2.filter(lambda x: x == "\n").sink(collected2.flush)
        s2.compile()
        for c in chars:
            s2.emit(c)

        assert results1 == results2


class TestFusedPreBufferSteps:
    """Steps between the branch point and a collect() that compile() fuses
    into a chunk_by node run as they would unfused."""

    def run(self, build, items, compiled):
        s = Stream()
        out = build(s)
        if compiled:
            s.compile()
        for item in items:
            s.emit(item)
        return out

    def test_flatten_before_the_collect_buffers_every_item(self):
        """Fused, a flatten's items were dropped: every chunk came out empty."""

        def build(s):
            c = s.flatten().collect()
            out = c.to_list()
            s.filter(lambda x: x == []).sink(lambda _: c.flush())
            return out

        items = [[1, 2], [3], [], [4, 5, 6], []]
        expected = [(1, 2, 3), (4, 5, 6)]
        assert self.run(build, items, compiled=False) == expected
        assert self.run(build, items, compiled=True) == expected

    def test_lambdas_before_the_collect_read_globals_once(self):
        """compile() reads a lambda's globals once, at compile(); fused
        pre-buffer steps used to skip that analysis and read them per item."""
        ns = {"k": 2}
        exec("double = lambda x: x * k", ns)
        s = Stream()
        c = s.map(ns["double"]).collect()
        out = c.to_list()
        s.filter(lambda x: x >= 100).sink(lambda _: c.flush())
        s.compile()
        ns["k"] = 3
        s.emit(100)
        assert out == [(200,)]
