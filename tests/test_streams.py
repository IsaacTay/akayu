import pytest

import akayu
from akayu import MultiStream, Stream


def _sources(n):
    return [Stream() for _ in range(n)]


class TestMultiStreamConstruction:
    def test_from_list(self):
        ss = MultiStream(_sources(3))
        assert len(ss) == 3

    def test_from_tuple(self):
        ss = tuple(_sources(2))
        assert len(MultiStream(ss)) == 2

    def test_empty(self):
        assert len(MultiStream([])) == 0

    def test_rejects_non_stream_element(self):
        with pytest.raises(TypeError, match="Element 1 is not a Stream"):
            MultiStream([Stream(), 42])

    def test_rejects_non_sequence(self):
        with pytest.raises(TypeError, match="dict, list, or tuple"):
            MultiStream(42)

    def test_rejects_single_stream(self):
        with pytest.raises(TypeError, match="dict, list, or tuple"):
            MultiStream(Stream())


class TestContainerProtocol:
    def test_iter(self):
        srcs = _sources(3)
        ss = MultiStream(srcs)
        assert list(ss) == srcs

    def test_len(self):
        assert len(MultiStream(_sources(5))) == 5

    def test_getitem_int(self):
        srcs = _sources(3)
        ss = MultiStream(srcs)
        assert ss[0] is srcs[0]
        assert ss[2] is srcs[2]

    def test_getitem_slice(self):
        srcs = _sources(5)
        ss = MultiStream(srcs)
        sliced = ss[1:3]
        assert isinstance(sliced, MultiStream)
        assert len(sliced) == 2

    def test_repr(self):
        ss = MultiStream(_sources(3))
        assert repr(ss) == "MultiStream(3)"


class TestTransforms:
    def test_map(self):
        srcs = _sources(2)
        results = MultiStream(srcs).map(lambda x: x * 2).to_list()
        srcs[0].emit(1)
        srcs[1].emit(10)
        srcs[0].flush()
        srcs[1].flush()
        assert list(results[0]) == [2]
        assert list(results[1]) == [20]

    def test_filter(self):
        srcs = _sources(2)
        results = MultiStream(srcs).filter(lambda x: x > 0).to_list()
        srcs[0].emit(-1)
        srcs[0].emit(5)
        srcs[1].emit(0)
        srcs[1].emit(10)
        srcs[0].flush()
        srcs[1].flush()
        assert list(results[0]) == [5]
        assert list(results[1]) == [10]

    def test_sink(self):
        srcs = _sources(2)
        r1, r2 = [], []
        MultiStream(srcs).sink(lambda x: ([r1, r2][0].append(x)))
        MultiStream(srcs).sink(lambda x: ([r1, r2][1].append(x)))
        srcs[0].emit(1)
        srcs[1].emit(2)

    def test_starmap(self):
        srcs = _sources(1)
        results = MultiStream(srcs).starmap(lambda a, b: a + b).to_list()
        srcs[0].emit((3, 4))
        srcs[0].flush()
        assert list(results[0]) == [7]

    def test_accumulate(self):
        srcs = _sources(1)
        results = MultiStream(srcs).accumulate(lambda s, x: s + x, start=0).to_list()
        srcs[0].emit(1)
        srcs[0].emit(2)
        srcs[0].emit(3)
        srcs[0].flush()
        assert list(results[0]) == [1, 3, 6]

    def test_flatten(self):
        srcs = _sources(1)
        results = MultiStream(srcs).flatten().to_list()
        srcs[0].emit([1, 2, 3])
        srcs[0].flush()
        assert list(results[0]) == [1, 2, 3]

    def test_collect(self):
        srcs = _sources(1)
        results = MultiStream(srcs).collect().to_list()
        srcs[0].emit(1)
        srcs[0].emit(2)
        srcs[0].flush()
        assert list(results[0]) == [(1, 2)]

    def test_batch_map(self):
        srcs = _sources(1)
        results = MultiStream(srcs).batch_map(lambda batch: [x * 10 for x in batch]).to_list()
        srcs[0].emit_batch([1, 2, 3])
        srcs[0].flush()
        assert list(results[0]) == [10, 20, 30]

    def test_chunk_by(self):
        srcs = _sources(1)
        results = MultiStream(srcs).chunk_by(lambda x: x == ",", include_delimiter=True).to_list()
        srcs[0].emit("a")
        srcs[0].emit(",")
        srcs[0].flush()
        assert list(results[0]) == [("a", ",")]

    def test_to_set(self):
        srcs = _sources(1)
        results = MultiStream(srcs).to_set()
        srcs[0].emit(1)
        srcs[0].emit(2)
        srcs[0].emit(1)
        srcs[0].flush()
        assert results[0] == {1, 2}

    def test_chaining(self):
        srcs = _sources(2)
        results = MultiStream(srcs).map(lambda x: x * 2).filter(lambda x: x > 5).to_list()
        srcs[0].emit(1)
        srcs[0].emit(10)
        srcs[1].emit(2)
        srcs[1].emit(5)
        srcs[0].flush()
        srcs[1].flush()
        assert list(results[0]) == [20]
        assert list(results[1]) == [10]

    def test_returns_streams(self):
        result = MultiStream(_sources(2)).map(lambda x: x)
        assert isinstance(result, MultiStream)
        assert len(result) == 2


class TestActions:
    def test_compile(self):
        srcs = _sources(2)
        result = MultiStream(srcs).map(lambda x: x).sink(lambda _: None).compile()
        assert isinstance(result, MultiStream)
        srcs[0].emit(1)
        srcs[1].emit(2)

    def test_flush(self):
        srcs = _sources(1)
        results = MultiStream(srcs).collect().to_list()
        MultiStream(srcs).flush()
        srcs[0].emit(1)
        MultiStream(srcs).collect().flush()
        srcs[0].emit(1)
        srcs[0].flush()
        assert len(results[0]) >= 1

    def test_compile_returns_self(self):
        ss = MultiStream(_sources(2))
        ret = ss.map(lambda x: x).sink(lambda _: None).compile()
        assert ret is not ss


class TestBroadcasting:
    def test_same_length(self):
        a = MultiStream(_sources(3))
        b = MultiStream(_sources(3))
        result = a.union(b)
        assert isinstance(result, MultiStream)
        assert len(result) == 3

    def test_broadcast_stream(self):
        a = MultiStream(_sources(3))
        b = Stream()
        result = a.union(b)
        assert isinstance(result, MultiStream)
        assert len(result) == 3

    def test_broadcast_length_one(self):
        a = MultiStream(_sources(3))
        b = MultiStream([Stream()])
        result = a.union(b)
        assert isinstance(result, MultiStream)
        assert len(result) == 3

    def test_always_returns_multistream(self):
        a = MultiStream([Stream()])
        b = MultiStream([Stream()])
        result = a.union(b)
        assert isinstance(result, MultiStream)

    def test_mismatch_raises(self):
        a = MultiStream(_sources(3))
        b = MultiStream(_sources(2))
        with pytest.raises(ValueError, match="Cannot broadcast"):
            a.union(b)

    def test_zip_broadcasting(self):
        a = MultiStream(_sources(2))
        b = Stream()
        result = a.zip(b)
        assert isinstance(result, MultiStream)
        assert len(result) == 2

    def test_combine_latest_broadcasting(self):
        a = MultiStream(_sources(2))
        b = Stream()
        result = a.combine_latest(b)
        assert isinstance(result, MultiStream)
        assert len(result) == 2

    def test_combine_latest_emit_on_stream(self):
        a_s = _sources(2)
        b_s = _sources(2)
        a = MultiStream(a_s)
        b = MultiStream(b_s)
        result = a.combine_latest(b, emit_on=a)
        assert isinstance(result, MultiStream)
        assert len(result) == 2

    def test_combine_latest_emit_on_broadcast(self):
        a = MultiStream(_sources(2))
        b = MultiStream(_sources(2))
        single = Stream()
        result = a.combine_latest(b, emit_on=single)
        assert isinstance(result, MultiStream)
        assert len(result) == 2


class TestModuleLevelFunctions:
    def test_union_plain_streams(self):
        s1, s2 = Stream(), Stream()
        result = akayu.union(s1, s2)
        assert isinstance(result, Stream)

    def test_union_with_streams(self):
        a = MultiStream(_sources(3))
        b = MultiStream(_sources(3))
        result = akayu.union(a, b)
        assert isinstance(result, MultiStream)
        assert len(result) == 3

    def test_union_broadcast(self):
        a = MultiStream(_sources(3))
        b = Stream()
        result = akayu.union(a, b)
        assert isinstance(result, MultiStream)

    def test_combine_latest_with_streams(self):
        a = MultiStream(_sources(2))
        b = MultiStream(_sources(2))
        result = akayu.combine_latest(a, b)
        assert isinstance(result, MultiStream)

    def test_zip_with_streams(self):
        a = MultiStream(_sources(2))
        b = MultiStream(_sources(2))
        result = akayu.zip(a, b)
        assert isinstance(result, MultiStream)

    def test_union_single_arg_raises(self):
        ss = MultiStream(_sources(3))
        with pytest.raises(ValueError, match="at least two"):
            akayu.union(ss)

    def test_union_no_args_raises(self):
        with pytest.raises(ValueError, match="at least two"):
            akayu.union()


class TestEndToEnd:
    def test_map_union_collect(self):
        srcs_a = _sources(2)
        srcs_b = _sources(2)
        results = []
        a = MultiStream(srcs_a).map(lambda x: x * 2)
        b = MultiStream(srcs_b).map(lambda x: x * 10)
        merged = a.union(b)
        merged.sink(results.append)
        srcs_a[0].emit(1)
        srcs_a[1].emit(2)
        srcs_b[0].emit(1)
        srcs_b[1].emit(2)
        assert sorted(results) == [2, 4, 10, 20]

    def test_broadcast_then_emit(self):
        srcs = _sources(3)
        MultiStream(srcs).map(lambda x: x * 2).to_list()
        lists = MultiStream(srcs).map(lambda x: x * 2).to_list()
        for s in srcs:
            i = srcs.index(s)
            s.emit(i + 1)
            s.flush()
        for i in range(3):
            assert list(lists[i]) == [(i + 1) * 2]

    def test_pairwise_combine_latest(self):
        a_arr = _sources(2)
        b_arr = _sources(2)
        a = MultiStream(a_arr)
        b = MultiStream(b_arr)
        results = a.combine_latest(b).to_list()
        a_arr[0].emit(1)
        b_arr[0].emit("x")
        a_arr[0].flush()
        a_arr[1].emit(10)
        b_arr[1].emit("y")
        a_arr[1].flush()
        assert list(results[0]) == [(1, "x")]
        assert list(results[1]) == [(10, "y")]


class TestUnsupported:
    def test_emit_raises(self):
        ss = MultiStream(_sources(1))
        with pytest.raises(AttributeError, match="does not support"):
            ss.emit(1)

    def test_prefetch_works(self):
        ss = MultiStream(_sources(1))
        result = ss.prefetch(4)
        assert isinstance(result, MultiStream)
        assert len(result) == 1

    def test_par_works(self):
        ss = MultiStream(_sources(1))
        result = ss.par()
        assert isinstance(result, MultiStream)
        assert len(result) == 1

    def test_unknown_attr_raises(self):
        ss = MultiStream(_sources(1))
        with pytest.raises(AttributeError, match="no attribute"):
            ss.nonexistent


class TestEdgeCases:
    def test_empty_streams_transform(self):
        ss = MultiStream([])
        result = ss.map(lambda x: x)
        assert isinstance(result, MultiStream)
        assert len(result) == 0

    def test_empty_streams_union_raises(self):
        ss = MultiStream([])
        with pytest.raises(ValueError, match="Cannot broadcast"):
            ss.union(Stream())

    def test_single_element_streams(self):
        ss = MultiStream([Stream()])
        assert len(ss) == 1
        result = ss.map(lambda x: x * 2)
        assert isinstance(result, MultiStream)
        assert len(result) == 1

    def test_large_batch(self):
        n = 100
        srcs = _sources(n)
        results = MultiStream(srcs).map(lambda x: x + 1).to_list()
        for i, s in enumerate(srcs):
            s.emit(i)
            s.flush()
        for i in range(n):
            assert list(results[i]) == [i + 1]

    def test_broadcast_multiple_streams(self):
        a = MultiStream(_sources(3))
        b = MultiStream(_sources(3))
        c = Stream()
        result = a.union(b, c)
        assert isinstance(result, MultiStream)
        assert len(result) == 3

    def test_getitem_negative(self):
        srcs = _sources(3)
        ss = MultiStream(srcs)
        assert ss[-1] is srcs[2]

    def test_as_streams_rejects_non_stream(self):
        with pytest.raises(ValueError, match="at least two"):
            akayu.union(42)

    def test_streams_from_streams_elements(self):
        srcs = _sources(2)
        ss = MultiStream(srcs)
        inner = [ss[0], ss[1]]
        ss2 = MultiStream(inner)
        assert len(ss2) == 2


class TestMultiStreamStreamName:
    def _check_names_via_error(self, ms, stream_name_prefix):
        for i, s in enumerate(ms):
            out = []
            s.sink(out.append)
            with pytest.raises(RuntimeError, match=stream_name_prefix + f"_{i}"):
                s.emit("boom")

    def test_stream_name_auto_indexed(self):
        def boom(x):
            raise ValueError("fail")

        srcs = _sources(3)
        ms = MultiStream(srcs)
        result = ms.map(boom, stream_name="transform")
        self._check_names_via_error(result, "transform")

    def test_stream_name_none_passes_no_name(self):
        srcs = _sources(2)
        ms = MultiStream(srcs)
        result = ms.map(lambda x: x)
        for i, s in enumerate(result):
            out = []
            s.sink(out.append)
            srcs[i].emit(1)
            assert out == [1]

    def test_stream_name_filter(self):
        def boom(x):
            raise ValueError("fail")

        srcs = _sources(2)
        ms = MultiStream(srcs)
        result = ms.filter(boom, stream_name="f")
        self._check_names_via_error(result, "f")

    def test_stream_name_sink(self):
        def boom(x):
            raise ValueError("fail")

        srcs = _sources(2)
        ms = MultiStream(srcs)
        ms.map(lambda x: x).sink(boom, stream_name="log")
        for i, s in enumerate(ms):
            with pytest.raises(RuntimeError, match=f"log_{i}"):
                s.emit(1)

    def test_stream_name_accumulate_no_name(self):
        srcs = _sources(2)
        ms = MultiStream(srcs)
        result = ms.accumulate(lambda acc, x: acc + x, 0)
        for i, s in enumerate(result):
            out = []
            s.sink(out.append)
            srcs[i].emit(1)
            srcs[i].emit(2)
            srcs[i].flush()
            assert out == [1, 3]

    def test_stream_name_batch_map(self):
        def boom(batch):
            raise ValueError("fail")

        srcs = _sources(2)
        ms = MultiStream(srcs)
        result = ms.batch_map(boom, stream_name="bm")
        for i, s in enumerate(result):
            out = []
            s.sink(out.append)
            with pytest.raises(RuntimeError, match=f"bm_{i}"):
                s.emit(1)

    def test_stream_name_chunk_by_no_name(self):
        srcs = _sources(2)
        ms = MultiStream(srcs)
        result = ms.chunk_by(lambda x: x == ",", include_delimiter=True)
        for i, s in enumerate(result):
            out = []
            s.sink(out.append)
            srcs[i].emit("a")
            srcs[i].emit(",")
            srcs[i].flush()
            assert out == [("a", ",")]

    def test_stream_name_starmap(self):
        def boom(*args):
            raise ValueError("fail")

        srcs = _sources(2)
        ms = MultiStream(srcs)
        result = ms.starmap(boom, stream_name="sm")
        self._check_names_via_error(result, "sm")

    def test_stream_name_empty_batch(self):
        ms = MultiStream([])
        result = ms.map(lambda x: x, stream_name="noop")
        assert len(result) == 0

    def test_stream_name_single_stream(self):
        def boom(x):
            raise ValueError("fail")

        srcs = _sources(1)
        ms = MultiStream(srcs)
        result = ms.map(boom, stream_name="solo")
        self._check_names_via_error(result, "solo")


def _dict_sources(keys):
    return {k: Stream() for k in keys}


class TestDictModeConstruction:
    def test_from_dict(self):
        sources = _dict_sources(["a", "b", "c"])
        ms = MultiStream(sources)
        assert len(ms) == 3
        assert ms.keyed

    def test_empty_dict(self):
        ms = MultiStream({})
        assert len(ms) == 0
        assert ms.keyed

    def test_single_entry(self):
        ms = MultiStream({"x": Stream()})
        assert len(ms) == 1

    def test_rejects_non_stream_value(self):
        with pytest.raises(TypeError, match="Value for key 'bad'"):
            MultiStream({"good": Stream(), "bad": 42})

    def test_list_mode_not_keyed(self):
        ms = MultiStream([Stream(), Stream()])
        assert not ms.keyed


class TestDictModeAccess:
    def test_getitem_string_key(self):
        sources = _dict_sources(["temp", "humid"])
        ms = MultiStream(sources)
        assert ms["temp"] is sources["temp"]
        assert ms["humid"] is sources["humid"]

    def test_getitem_missing_key(self):
        ms = MultiStream(_dict_sources(["a"]))
        with pytest.raises(KeyError):
            ms["z"]

    def test_string_key_on_list_mode_raises(self):
        ms = MultiStream([Stream()])
        with pytest.raises(TypeError, match="string key"):
            ms["a"]

    def test_getitem_int_still_works(self):
        sources = _dict_sources(["x", "y"])
        ms = MultiStream(sources)
        assert ms[0] is sources["x"]
        assert ms[1] is sources["y"]

    def test_repr_dict_mode(self):
        ms = MultiStream({"a": Stream(), "b": Stream()})
        assert repr(ms) == "MultiStream({'a', 'b'})"

    def test_repr_list_mode(self):
        ms = MultiStream([Stream(), Stream()])
        assert repr(ms) == "MultiStream(2)"


class TestDictModeDictLike:
    def test_keys(self):
        ms = MultiStream(_dict_sources(["a", "b", "c"]))
        assert list(ms.keys()) == ["a", "b", "c"]

    def test_values(self):
        sources = _dict_sources(["x", "y"])
        ms = MultiStream(sources)
        assert list(ms.values()) == [sources["x"], sources["y"]]

    def test_items(self):
        sources = _dict_sources(["p", "q"])
        ms = MultiStream(sources)
        assert list(ms.items()) == [("p", sources["p"]), ("q", sources["q"])]

    def test_to_dict(self):
        sources = _dict_sources(["a", "b"])
        ms = MultiStream(sources)
        assert ms.to_dict() == sources

    def test_keys_on_list_mode_raises(self):
        ms = MultiStream([Stream()])
        with pytest.raises(TypeError, match="dict-backed"):
            ms.keys()

    def test_items_on_list_mode_raises(self):
        ms = MultiStream([Stream()])
        with pytest.raises(TypeError, match="dict-backed"):
            ms.items()

    def test_to_dict_on_list_mode_raises(self):
        ms = MultiStream([Stream()])
        with pytest.raises(TypeError, match="dict-backed"):
            ms.to_dict()

    def test_iter_yields_streams(self):
        sources = _dict_sources(["a", "b"])
        ms = MultiStream(sources)
        assert list(ms) == [sources["a"], sources["b"]]


class TestDictModeTransforms:
    def test_map(self):
        sources = _dict_sources(["a", "b"])
        ms = MultiStream(sources)
        result = ms.map(lambda x: x * 2)
        assert result.keyed
        lists = result.to_list()
        sources["a"].emit(1)
        sources["b"].emit(10)
        sources["a"].flush()
        sources["b"].flush()
        assert list(lists["a"]) == [2]
        assert list(lists["b"]) == [20]

    def test_filter(self):
        sources = _dict_sources(["x", "y"])
        ms = MultiStream(sources)
        result = ms.filter(lambda x: x > 0)
        assert result.keyed
        lists = result.to_list()
        sources["x"].emit(-1)
        sources["x"].emit(5)
        sources["y"].emit(0)
        sources["y"].emit(10)
        sources["x"].flush()
        sources["y"].flush()
        assert list(lists["x"]) == [5]
        assert list(lists["y"]) == [10]

    def test_chaining_preserves_keys(self):
        sources = _dict_sources(["a", "b", "c"])
        result = MultiStream(sources).map(lambda x: x * 2).filter(lambda x: x > 5)
        assert result.keyed
        assert list(result.keys()) == ["a", "b", "c"]

    def test_accumulate(self):
        sources = _dict_sources(["s"])
        ms = MultiStream(sources)
        result = ms.accumulate(lambda acc, x: acc + x, start=0)
        assert result.keyed
        lists = result.to_list()
        sources["s"].emit(1)
        sources["s"].emit(2)
        sources["s"].emit(3)
        sources["s"].flush()
        assert list(lists["s"]) == [1, 3, 6]

    def test_collect(self):
        sources = _dict_sources(["a", "b"])
        ms = MultiStream(sources)
        collected = ms.collect()
        assert collected.keyed
        lists = collected.to_list()
        sources["a"].emit(1)
        sources["a"].emit(2)
        sources["b"].emit(10)
        ms.flush()
        assert list(lists["a"]) == [(1, 2)]
        assert list(lists["b"]) == [(10,)]

    def test_flatten(self):
        sources = _dict_sources(["a"])
        ms = MultiStream(sources)
        result = ms.flatten()
        assert result.keyed
        lists = result.to_list()
        sources["a"].emit([1, 2, 3])
        sources["a"].flush()
        assert list(lists["a"]) == [1, 2, 3]


class TestDictModeCollectors:
    def test_to_list_returns_dict(self):
        sources = _dict_sources(["x", "y"])
        ms = MultiStream(sources)
        result = ms.map(lambda x: x + 1).to_list()
        assert isinstance(result, dict)
        assert "x" in result
        assert "y" in result

    def test_to_set_returns_dict(self):
        sources = _dict_sources(["a"])
        ms = MultiStream(sources)
        result = ms.to_set()
        assert isinstance(result, dict)
        sources["a"].emit(1)
        sources["a"].emit(2)
        sources["a"].emit(1)
        sources["a"].flush()
        assert result["a"] == {1, 2}

    def test_list_mode_to_list_returns_list(self):
        ms = MultiStream([Stream(), Stream()])
        result = ms.to_list()
        assert isinstance(result, list)


class TestDictModeActions:
    def test_compile_returns_self(self):
        sources = _dict_sources(["a", "b"])
        ms = MultiStream(sources).map(lambda x: x).sink(lambda _: None)
        compiled = MultiStream(sources).map(lambda x: x).sink(lambda _: None).compile()
        assert isinstance(compiled, MultiStream)
        assert compiled.keyed

    def test_flush(self):
        sources = _dict_sources(["a"])
        collected = MultiStream(sources).collect()
        lists = collected.to_list()
        sources["a"].emit(1)
        sources["a"].emit(2)
        collected.flush()
        assert list(lists["a"]) == [(1, 2)]

    def test_drain(self):
        sources = _dict_sources(["a"])
        collected = MultiStream(sources).collect()
        lists = collected.to_list()
        sources["a"].emit(1)
        collected.drain()
        assert list(lists["a"]) == [(1,)]


class TestDictModeSlicing:
    def test_slice_preserves_keys(self):
        sources = _dict_sources(["a", "b", "c", "d"])
        ms = MultiStream(sources)
        sliced = ms[1:3]
        assert isinstance(sliced, MultiStream)
        assert sliced.keyed
        assert list(sliced.keys()) == ["b", "c"]
        assert len(sliced) == 2

    def test_step_slice(self):
        sources = _dict_sources(["a", "b", "c", "d", "e"])
        ms = MultiStream(sources)
        stepped = ms[::2]
        assert isinstance(stepped, MultiStream)
        assert stepped.keyed
        assert list(stepped.keys()) == ["a", "c", "e"]


class TestDictModeCombinators:
    def test_union_preserves_keys(self):
        a = MultiStream(_dict_sources(["x", "y"]))
        b = MultiStream(_dict_sources(["x", "y"]))
        result = a.union(b)
        assert isinstance(result, MultiStream)
        assert result.keyed
        assert list(result.keys()) == ["x", "y"]

    def test_combine_latest_preserves_keys(self):
        a = MultiStream(_dict_sources(["x", "y"]))
        b = MultiStream(_dict_sources(["x", "y"]))
        result = a.combine_latest(b)
        assert result.keyed
        assert list(result.keys()) == ["x", "y"]

    def test_zip_preserves_keys(self):
        a = MultiStream(_dict_sources(["x", "y"]))
        b = MultiStream(_dict_sources(["x", "y"]))
        result = a.zip(b)
        assert result.keyed
        assert list(result.keys()) == ["x", "y"]

    def test_union_mismatched_keys_raises(self):
        a = MultiStream(_dict_sources(["x", "y"]))
        b = MultiStream(_dict_sources(["a", "b"]))
        with pytest.raises(ValueError, match="Key mismatch"):
            a.union(b)

    def test_union_list_mode_stays_list(self):
        a = MultiStream([Stream(), Stream()])
        b = MultiStream([Stream(), Stream()])
        result = a.union(b)
        assert not result.keyed

    def test_union_mixed_dict_list_raises(self):
        a = MultiStream(_dict_sources(["x", "y"]))
        b = MultiStream([Stream(), Stream()])
        with pytest.raises(TypeError, match="dict-backed.*list-backed"):
            a.union(b)

    def test_union_dict_with_plain_stream_broadcasts(self):
        a = MultiStream(_dict_sources(["x", "y"]))
        b = Stream()
        result = a.union(b)
        assert result.keyed
        assert list(result.keys()) == ["x", "y"]

    def test_combine_latest_emit_on_preserves_keys(self):
        a = MultiStream(_dict_sources(["x", "y"]))
        b = MultiStream(_dict_sources(["x", "y"]))
        result = a.combine_latest(b, emit_on=a)
        assert result.keyed
        assert list(result.keys()) == ["x", "y"]

    def test_combine_latest_emit_on_mismatched_raises(self):
        a = MultiStream(_dict_sources(["x", "y"]))
        b = MultiStream(_dict_sources(["x", "y"]))
        c = MultiStream(_dict_sources(["p", "q"]))
        with pytest.raises(ValueError, match="Key mismatch"):
            a.combine_latest(b, emit_on=c)

    def test_union_produces_correct_data(self):
        a_src = _dict_sources(["x", "y"])
        b_src = _dict_sources(["x", "y"])
        a = MultiStream(a_src).map(lambda v: f"a{v}")
        b = MultiStream(b_src).map(lambda v: f"b{v}")
        result = a.union(b)
        assert result.keyed
        lists = result.to_list()
        a_src["x"].emit(1)
        a_src["y"].emit(2)
        b_src["x"].emit(10)
        b_src["y"].emit(20)
        assert sorted(list(lists["x"])) == ["a1", "b10"]
        assert sorted(list(lists["y"])) == ["a2", "b20"]

    def test_zip_produces_correct_data(self):
        a_src = _dict_sources(["temp", "humid"])
        b_src = _dict_sources(["temp", "humid"])
        a = MultiStream(a_src).map(lambda v: ("A", v))
        b = MultiStream(b_src).map(lambda v: ("B", v))
        result = a.zip(b)
        assert result.keyed
        lists = result.to_list()
        a_src["temp"].emit(22)
        b_src["temp"].emit(65)
        a_src["humid"].emit(30)
        b_src["humid"].emit(70)
        assert list(lists["temp"]) == [(("A", 22), ("B", 65))]
        assert list(lists["humid"]) == [(("A", 30), ("B", 70))]


class TestDictModeExpand:
    def test_expand_dict_mode_loses_keys(self):
        ms = MultiStream({"a": Stream()})
        expanded = ms._expand(3)
        assert not expanded.keyed
        assert len(expanded) == 3


class TestDictModeRealWorld:
    def test_sensor_pipeline(self):
        sensors = {"temp": Stream(), "humid": Stream(), "pressure": Stream()}
        ms = MultiStream(sensors)

        alarms = ms.filter(lambda v: v > 100).to_list()

        sensors["temp"].emit(50)
        sensors["humid"].emit(150)
        sensors["pressure"].emit(200)
        sensors["temp"].emit(120)

        assert list(alarms["temp"]) == [120]
        assert list(alarms["humid"]) == [150]
        assert list(alarms["pressure"]) == [200]

    def test_collect_flush_dict(self):
        sources = {"a": Stream(), "b": Stream()}
        ms = MultiStream(sources)
        collected = ms.collect()
        results = collected.to_list()

        sources["a"].emit(1)
        sources["a"].emit(2)
        sources["b"].emit(10)
        ms.flush()

        assert isinstance(results, dict)
        assert list(results["a"]) == [(1, 2)]
        assert list(results["b"]) == [(10,)]

    def test_large_dict(self):
        keys = [f"s{i}" for i in range(50)]
        sources = {k: Stream() for k in keys}
        ms = MultiStream(sources)
        results = ms.map(lambda x: x + 1).to_list()

        for k in keys:
            sources[k].emit(int(k[1:]))

        for k in keys:
            assert list(results[k]) == [int(k[1:]) + 1]


class TestModuleLevelDictCombinators:
    def test_union_dict_keys(self):
        a = MultiStream(_dict_sources(["x", "y"]))
        b = MultiStream(_dict_sources(["x", "y"]))
        result = akayu.union(a, b)
        assert result.keyed
        assert list(result.keys()) == ["x", "y"]

    def test_combine_latest_dict_keys(self):
        a = MultiStream(_dict_sources(["x", "y"]))
        b = MultiStream(_dict_sources(["x", "y"]))
        result = akayu.combine_latest(a, b)
        assert result.keyed

    def test_zip_dict_keys(self):
        a = MultiStream(_dict_sources(["x", "y"]))
        b = MultiStream(_dict_sources(["x", "y"]))
        result = akayu.zip(a, b)
        assert result.keyed

    def test_union_dict_mismatched_raises(self):
        a = MultiStream(_dict_sources(["x", "y"]))
        b = MultiStream(_dict_sources(["a", "b"]))
        with pytest.raises(ValueError, match="Key mismatch"):
            akayu.union(a, b)


class TestMultiStreamProfile:
    def test_profile_list_mode(self):
        a = Stream()
        b = Stream()
        a.map(lambda x: x * 2).sink(lambda x: None)
        b.map(lambda x: x + 1).sink(lambda x: None)
        ms = MultiStream([a, b])
        profilers = ms.profile()
        assert isinstance(profilers, list)
        assert len(profilers) == 2

    def test_profile_dict_mode(self):
        sources = _dict_sources(["temp", "humid"])
        ms = MultiStream(sources)
        for s in ms:
            s.map(lambda x: x).sink(lambda x: None)
        profilers = ms.profile()
        assert isinstance(profilers, dict)
        assert set(profilers.keys()) == {"temp", "humid"}
        with profilers["temp"]:
            sources["temp"].emit(42)
        assert profilers["temp"].report is not None
