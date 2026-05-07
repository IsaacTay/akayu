"""Tests for nested MultiStream (MultiStream of MultiStreams)."""

import pytest

from akayu import MultiStream, Stream


def _sources(n):
    return [Stream() for _ in range(n)]


class TestNestedConstruction:
    def test_nested_list(self):
        inner_a = MultiStream(_sources(2))
        inner_b = MultiStream(_sources(3))
        outer = MultiStream([inner_a, inner_b])
        assert len(outer) == 2

    def test_nested_dict(self):
        inner_a = MultiStream(_sources(2))
        inner_b = MultiStream(_sources(2))
        outer = MultiStream({"group_a": inner_a, "group_b": inner_b})
        assert len(outer) == 2
        assert outer.keyed is True

    def test_mixed_stream_and_multistream(self):
        inner = MultiStream(_sources(2))
        s = Stream()
        outer = MultiStream([inner, s])
        assert len(outer) == 2

    def test_rejects_non_stream_non_multistream(self):
        with pytest.raises(TypeError, match="not a Stream or MultiStream"):
            MultiStream([Stream(), 42])

    def test_repr_nested(self):
        outer = MultiStream([MultiStream(_sources(2)), MultiStream(_sources(3))])
        assert repr(outer) == "MultiStream(2)"


class TestNestedTransforms:
    def test_map_recurse(self):
        srcs_a = _sources(2)
        srcs_b = _sources(2)
        inner_a = MultiStream(srcs_a)
        inner_b = MultiStream(srcs_b)
        outer = MultiStream([inner_a, inner_b])

        result = outer.map(lambda x: x * 10)

        # result is a nested MultiStream: [inner_a.map(fn), inner_b.map(fn)]
        assert isinstance(result, MultiStream)
        assert isinstance(result[0], MultiStream)
        assert isinstance(result[1], MultiStream)

        # to_list() wires up live collectors, then we emit + flush
        live = result.to_list()
        # live[0] = [list_a0, list_a1] (live lists for inner_a's 2 streams)
        # live[1] = [list_b0, list_b1] (live lists for inner_b's 2 streams)

        for s in srcs_a:
            s.emit(1)
        for s in srcs_b:
            s.emit(2)
        for s in srcs_a + srcs_b:
            s.flush()

        assert live[0][0] == [10]
        assert live[0][1] == [10]
        assert live[1][0] == [20]
        assert live[1][1] == [20]

    def test_filter_recurse(self):
        srcs_a = _sources(1)
        srcs_b = _sources(1)
        outer = MultiStream([MultiStream(srcs_a), MultiStream(srcs_b)])

        result = outer.filter(lambda x: x > 3)

        live = result.to_list()

        srcs_a[0].emit(1)
        srcs_a[0].emit(5)
        srcs_b[0].emit(2)
        srcs_b[0].emit(8)
        for s in srcs_a + srcs_b:
            s.flush()

        assert live[0][0] == [5]
        assert live[1][0] == [8]

    def test_sink_recurse(self):
        srcs_a = _sources(1)
        srcs_b = _sources(1)
        outer = MultiStream([MultiStream(srcs_a), MultiStream(srcs_b)])

        collected_a = []
        collected_b = []
        # sink on nested MS sinks each inner MS, which sinks each stream
        outer[0].sink(lambda x: collected_a.append(x))
        outer[1].sink(lambda x: collected_b.append(x))

        srcs_a[0].emit(42)
        srcs_b[0].emit(99)

        assert collected_a == [42]
        assert collected_b == [99]

    def test_collect_recurse(self):
        srcs_a = _sources(2)
        srcs_b = _sources(1)
        outer = MultiStream([MultiStream(srcs_a), MultiStream(srcs_b)])

        collected = outer.collect()

        live = collected.to_list()

        srcs_a[0].emit(1)
        srcs_a[1].emit(2)
        srcs_b[0].emit(3)
        for s in srcs_a + srcs_b:
            s.flush()

        # collect() returns tuples
        assert live[0][0] == [(1,)]
        assert live[0][1] == [(2,)]
        assert live[1][0] == [(3,)]

    def test_flatten_recurse(self):
        srcs = _sources(1)
        outer = MultiStream([MultiStream(srcs)])

        result = outer.flatten()

        live = result.to_list()

        srcs[0].emit([1, 2, 3])
        srcs[0].flush()

        assert live[0][0] == [1, 2, 3]

    def test_accumulate_recurse(self):
        srcs_a = _sources(1)
        srcs_b = _sources(1)
        outer = MultiStream([MultiStream(srcs_a), MultiStream(srcs_b)])

        result = outer.accumulate(lambda s, x: s + x, start=0)

        live = result.to_list()

        srcs_a[0].emit(1)
        srcs_a[0].emit(2)
        srcs_b[0].emit(10)
        for s in srcs_a + srcs_b:
            s.flush()

        assert live[0][0] == [1, 3]
        assert live[1][0] == [10]

    def test_starmap_recurse(self):
        srcs = _sources(1)
        outer = MultiStream([MultiStream(srcs)])

        result = outer.starmap(lambda a, b: a + b)

        live = result.to_list()

        srcs[0].emit((3, 4))
        srcs[0].flush()

        assert live[0][0] == [7]

    def test_batch_map_recurse(self):
        srcs = _sources(1)
        outer = MultiStream([MultiStream(srcs)])

        result = outer.batch_map(lambda batch: [x * 100 for x in batch])

        live = result.to_list()

        srcs[0].emit(1)
        srcs[0].emit(2)
        srcs[0].flush()

        assert live[0][0] == [100, 200]

    def test_chunk_by_recurse(self):
        srcs = _sources(1)
        outer = MultiStream([MultiStream(srcs)])

        result = outer.chunk_by(lambda x: x == ",", include_delimiter=True)

        live = result.to_list()

        srcs[0].emit("a")
        srcs[0].emit(",")
        srcs[0].emit("b")
        srcs[0].flush()

        assert live[0][0] == [("a", ","), ("b",)]


class TestNestedActions:
    def test_flush_recurse(self):
        srcs_a = _sources(1)
        srcs_b = _sources(1)
        outer = MultiStream([MultiStream(srcs_a), MultiStream(srcs_b)])

        result = outer.map(lambda x: x * 2)
        live = result.to_list()

        srcs_a[0].emit(1)
        srcs_b[0].emit(2)
        for s in srcs_a + srcs_b:
            s.flush()

        assert live[0][0] == [2]
        assert live[1][0] == [4]

    def test_drain_recurse(self):
        srcs = _sources(1)
        outer = MultiStream([MultiStream(srcs)])
        result = outer.map(lambda x: x)
        live = result.to_list()

        srcs[0].emit(1)
        for s in srcs:
            s.flush()

        assert live[0][0] == [1]

    def test_compile_recurse(self):
        srcs = _sources(1)
        outer = MultiStream([MultiStream(srcs)])
        compiled = outer.compile()
        assert isinstance(compiled, MultiStream)
        assert isinstance(compiled[0], MultiStream)


class TestNestedCollectors:
    def test_to_list_recurse(self):
        srcs_a = _sources(2)
        srcs_b = _sources(1)
        outer = MultiStream([MultiStream(srcs_a), MultiStream(srcs_b)])

        live = outer.to_list()

        srcs_a[0].emit(1)
        srcs_a[1].emit(2)
        srcs_b[0].emit(3)
        for s in srcs_a + srcs_b:
            s.flush()

        # live[0] = inner_a's to_list = [list_a0, list_a1]
        assert list(live[0][0]) == [1]
        assert list(live[0][1]) == [2]
        # live[1] = inner_b's to_list = [list_b0]
        assert list(live[1][0]) == [3]

    def test_to_set_recurse(self):
        srcs_a = _sources(1)
        srcs_b = _sources(1)
        outer = MultiStream([MultiStream(srcs_a), MultiStream(srcs_b)])

        live = outer.to_set()

        srcs_a[0].emit(1)
        srcs_a[0].emit(1)
        srcs_b[0].emit(2)
        for s in srcs_a + srcs_b:
            s.flush()

        assert live[0][0] == {1}
        assert live[1][0] == {2}


class TestNestedIndexing:
    def test_getitem_nested(self):
        inner_a = MultiStream(_sources(2))
        inner_b = MultiStream(_sources(3))
        outer = MultiStream([inner_a, inner_b])

        assert isinstance(outer[0], MultiStream)
        assert len(outer[0]) == 2
        assert isinstance(outer[1], MultiStream)
        assert len(outer[1]) == 3

    def test_iter_nested(self):
        inner_a = MultiStream(_sources(2))
        inner_b = MultiStream(_sources(2))
        outer = MultiStream([inner_a, inner_b])

        items = list(outer)
        assert len(items) == 2
        assert all(isinstance(m, MultiStream) for m in items)

    def test_destructure_nested(self):
        inner_a = MultiStream(_sources(2))
        inner_b = MultiStream(_sources(2))
        outer = MultiStream([inner_a, inner_b])

        a, b = outer
        assert isinstance(a, MultiStream)
        assert isinstance(b, MultiStream)


class TestNestedCombinatorsBlocked:
    def test_union_blocked(self):
        outer = MultiStream([MultiStream(_sources(2)), MultiStream(_sources(2))])
        other = MultiStream([Stream(), Stream()])
        with pytest.raises(TypeError, match="union\\(\\) on nested MultiStream is not yet supported"):
            outer.union(other)

    def test_combine_latest_blocked(self):
        outer = MultiStream([MultiStream(_sources(2)), MultiStream(_sources(2))])
        other = MultiStream([Stream(), Stream()])
        with pytest.raises(TypeError, match="combine_latest\\(\\) on nested MultiStream is not yet supported"):
            outer.combine_latest(other)

    def test_zip_blocked(self):
        outer = MultiStream([MultiStream(_sources(2)), MultiStream(_sources(2))])
        other = MultiStream([Stream(), Stream()])
        with pytest.raises(TypeError, match="zip\\(\\) on nested MultiStream is not yet supported"):
            outer.zip(other)


class TestNestedDictMode:
    def test_dict_nested_to_list(self):
        srcs_a = _sources(1)
        srcs_b = _sources(1)
        outer = MultiStream({"group_a": MultiStream(srcs_a), "group_b": MultiStream(srcs_b)})

        live = outer.to_list()

        srcs_a[0].emit(10)
        srcs_b[0].emit(20)
        for s in srcs_a + srcs_b:
            s.flush()

        # live is {"group_a": inner_a.to_list(), "group_b": inner_b.to_list()}
        # inner_a.to_list() = [list_a0], inner_b.to_list() = [list_b0]
        assert list(live["group_a"][0]) == [10]
        assert list(live["group_b"][0]) == [20]

    def test_dict_nested_keys(self):
        inner_a = MultiStream(_sources(1))
        inner_b = MultiStream(_sources(1))
        outer = MultiStream({"x": inner_a, "y": inner_b})

        assert list(outer.keys()) == ["x", "y"]
        assert outer["x"] is inner_a

    def test_dict_nested_map(self):
        srcs_a = _sources(2)
        srcs_b = _sources(1)
        outer = MultiStream({"a": MultiStream(srcs_a), "b": MultiStream(srcs_b)})

        result = outer.map(lambda x: x + 1)
        live = result.to_list()

        srcs_a[0].emit(1)
        srcs_a[1].emit(2)
        srcs_b[0].emit(10)
        for s in srcs_a + srcs_b:
            s.flush()

        # live["a"] = inner_a.to_list() = [list_a0, list_a1]
        assert list(live["a"][0]) == [2]
        assert list(live["a"][1]) == [3]
        assert list(live["b"][0]) == [11]
