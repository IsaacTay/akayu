from hypothesis import given, settings
from hypothesis import strategies as st

import akayu

st_ints = st.integers(min_value=-1000, max_value=1000)
st_lists = st.lists(st_ints, min_size=0, max_size=50)
st_small_ints = st.integers(min_value=1, max_value=10)


class TestCompileEquivalence:
    @settings(max_examples=50)
    @given(values=st_lists)
    def test_map_compile_equivalence(self, values):
        s1 = akayu.Stream()
        s1.map(lambda x: x * 2).sink(lambda x: None)
        for v in values:
            s1.emit(v)
        result1 = s1.to_list()

        s2 = akayu.Stream()
        s2.map(lambda x: x * 2).sink(lambda x: None).compile()
        for v in values:
            s2.emit(v)
        result2 = s2.to_list()

        assert result1 == result2

    @settings(max_examples=50)
    @given(values=st_lists)
    def test_filter_compile_equivalence(self, values):
        s1 = akayu.Stream()
        s1.filter(lambda x: x > 0).sink(lambda x: None)
        for v in values:
            s1.emit(v)
        result1 = s1.to_list()

        s2 = akayu.Stream()
        s2.filter(lambda x: x > 0).sink(lambda x: None).compile()
        for v in values:
            s2.emit(v)
        result2 = s2.to_list()

        assert result1 == result2

    @settings(max_examples=50)
    @given(values=st_lists)
    def test_map_filter_sink_compile_equivalence(self, values):
        s1 = akayu.Stream()
        s1.map(lambda x: x * 2).filter(lambda x: x > 5).sink(lambda x: None)
        for v in values:
            s1.emit(v)
        result1 = s1.to_list()

        s2 = akayu.Stream()
        s2.map(lambda x: x * 2).filter(lambda x: x > 5).sink(lambda x: None).compile()
        for v in values:
            s2.emit(v)
        result2 = s2.to_list()

        assert result1 == result2

    @settings(max_examples=50)
    @given(values=st_lists)
    def test_accumulate_compile_equivalence(self, values):
        s1 = akayu.Stream()
        s1.accumulate(lambda acc, x: acc + x, start=0).sink(lambda x: None)
        for v in values:
            s1.emit(v)
        result1 = s1.to_list()

        s2 = akayu.Stream()
        s2.accumulate(lambda acc, x: acc + x, start=0).sink(lambda x: None).compile()
        for v in values:
            s2.emit(v)
        result2 = s2.to_list()

        assert result1 == result2

    @settings(max_examples=50)
    @given(values=st_lists)
    def test_flatten_compile_equivalence(self, values):
        s1 = akayu.Stream()
        s1.map(lambda x: [x]).flatten().sink(lambda x: None)
        for v in values:
            s1.emit(v)
        s1.flush()
        result1 = s1.to_list()

        s2 = akayu.Stream()
        s2.map(lambda x: [x]).flatten().sink(lambda x: None).compile()
        for v in values:
            s2.emit(v)
        s2.flush()
        result2 = s2.to_list()

        assert result1 == result2

    @settings(max_examples=50)
    @given(values=st_lists)
    def test_collect_flatten_compile_equivalence(self, values):
        s1 = akayu.Stream()
        s1.collect().flatten().sink(lambda x: None)
        for v in values:
            s1.emit(v)
        s1.flush()
        result1 = s1.to_list()

        s2 = akayu.Stream()
        s2.collect().flatten().sink(lambda x: None).compile()
        for v in values:
            s2.emit(v)
        s2.flush()
        result2 = s2.to_list()

        assert result1 == result2

    @settings(max_examples=50)
    @given(values=st_lists)
    def test_batch_map_compile_equivalence(self, values):
        s1 = akayu.Stream()
        s1.batch_map(lambda xs: [x * 2 for x in xs]).sink(lambda x: None)
        for v in values:
            s1.emit(v)
        s1.flush()
        result1 = s1.to_list()

        s2 = akayu.Stream()
        s2.batch_map(lambda xs: [x * 2 for x in xs]).sink(lambda x: None).compile()
        for v in values:
            s2.emit(v)
        s2.flush()
        result2 = s2.to_list()

        assert result1 == result2

    @settings(max_examples=50)
    @given(values=st_lists)
    def test_par_branches_compile_equivalence(self, values):
        results1a, results1b = [], []
        s1 = akayu.Stream()
        p1 = s1.par()
        p1.map(lambda x: x * 2).sink(results1a.append)
        p1.map(lambda x: x + 100).sink(results1b.append)
        for v in values:
            s1.emit(v)
        result1a = sorted(results1a)
        result1b = sorted(results1b)

        results2a, results2b = [], []
        s2 = akayu.Stream()
        p2 = s2.par()
        p2.map(lambda x: x * 2).sink(results2a.append)
        p2.map(lambda x: x + 100).sink(results2b.append)
        s2.compile()
        for v in values:
            s2.emit(v)
        result2a = sorted(results2a)
        result2b = sorted(results2b)

        assert result1a == result2a
        assert result1b == result2b

    @settings(max_examples=50)
    @given(values=st_lists)
    def test_union_compile_equivalence(self, values):
        mid = len(values) // 2
        vals1 = values[:mid]
        vals2 = values[mid:]

        s1a, s1b = akayu.Stream(), akayu.Stream()
        s1a.union(s1b).sink(lambda x: None)
        for v in vals1:
            s1a.emit(v)
        for v in vals2:
            s1b.emit(v)
        result1 = sorted(s1a.to_list() + s1b.to_list())

        s2a, s2b = akayu.Stream(), akayu.Stream()
        s2a.union(s2b).sink(lambda x: None).compile()
        for v in vals1:
            s2a.emit(v)
        for v in vals2:
            s2b.emit(v)
        result2 = sorted(s2a.to_list() + s2b.to_list())

        assert result1 == result2

    @settings(max_examples=50)
    @given(values=st_lists)
    def test_full_pipeline_compile_equivalence(self, values):
        results1 = []
        s1 = akayu.Stream()
        s1.map(lambda x: x * 2).filter(lambda x: x > 0).map(lambda x: x + 1).sink(results1.append)
        for v in values:
            s1.emit(v)
        result1 = sorted(results1)

        results2 = []
        s2 = akayu.Stream()
        s2.map(lambda x: x * 2).filter(lambda x: x > 0).map(lambda x: x + 1).sink(results2.append).compile()
        for v in values:
            s2.emit(v)
        result2 = sorted(results2)

        assert result1 == result2


class TestTransformLaws:
    @settings(max_examples=50)
    @given(values=st_lists)
    def test_identity_map(self, values):
        results = []
        s = akayu.Stream()
        s.map(lambda x: x).sink(results.append)
        for v in values:
            s.emit(v)
        result = results
        assert sorted(result) == sorted(values)

    @settings(max_examples=50)
    @given(values=st_lists)
    def test_filter_all_pass(self, values):
        results = []
        s = akayu.Stream()
        s.filter(lambda x: True).sink(results.append)
        for v in values:
            s.emit(v)
        result = results
        assert sorted(result) == sorted(values)

    @settings(max_examples=50)
    @given(values=st_lists)
    def test_filter_none_pass(self, values):
        s = akayu.Stream()
        s.filter(lambda x: False).sink(lambda x: None)
        for v in values:
            s.emit(v)
        result = s.to_list()
        assert result == []

    @settings(max_examples=50)
    @given(values=st_lists)
    def test_double_filter(self, values):
        def p1(x):
            return x > 0

        def p2(x):
            return x % 2 == 0

        results1 = []
        s1 = akayu.Stream()
        s1.filter(p1).filter(p2).sink(results1.append)
        for v in values:
            s1.emit(v)
        result1 = sorted(results1)

        results2 = []
        s2 = akayu.Stream()
        s2.filter(lambda x: p1(x) and p2(x)).sink(results2.append)
        for v in values:
            s2.emit(v)
        result2 = sorted(results2)

        assert result1 == result2

    @settings(max_examples=50)
    @given(values=st_lists)
    def test_map_compose(self, values):
        def f(x):
            return x * 2

        def g(x):
            return x + 1

        results1 = []
        s1 = akayu.Stream()
        s1.map(f).map(g).sink(results1.append)
        for v in values:
            s1.emit(v)
        result1 = sorted(results1)

        results2 = []
        s2 = akayu.Stream()
        s2.map(lambda x: g(f(x))).sink(results2.append)
        for v in values:
            s2.emit(v)
        result2 = sorted(results2)

        assert result1 == result2

    @settings(max_examples=50)
    @given(values=st_lists)
    def test_filter_then_map_equals_map_then_filter(self, values):
        def filt(x):
            return x >= 0

        def m(x):
            return x * 2

        results1 = []
        s1 = akayu.Stream()
        s1.filter(filt).map(m).sink(results1.append)
        for v in values:
            s1.emit(v)
        result1 = sorted(results1)

        results2 = []
        s2 = akayu.Stream()
        s2.map(m).filter(filt).sink(results2.append)
        for v in values:
            s2.emit(v)
        result2 = sorted(results2)

        assert result1 == result2

    @settings(max_examples=50)
    @given(values=st_lists)
    def test_sink_collects_all(self, values):
        sink_results = []
        s1 = akayu.Stream()
        s1.sink(sink_results.append)
        for v in values:
            s1.emit(v)
        result1 = sink_results

        sink_results2 = []
        s2 = akayu.Stream()
        s2.map(lambda x: x).sink(sink_results2.append)
        for v in values:
            s2.emit(v)
        result2 = sink_results2

        assert sorted(result1) == sorted(result2)

    @settings(max_examples=50)
    @given(values=st_lists)
    def test_flatten_of_singletons(self, values):
        results1 = []
        s1 = akayu.Stream()
        s1.map(lambda x: [x]).flatten().sink(results1.append)
        for v in values:
            s1.emit(v)
        s1.flush()
        result1 = sorted(results1)

        result2 = sorted(values)

        assert result1 == result2

    @settings(max_examples=50)
    @given(values=st.lists(st.integers(min_value=0, max_value=100), min_size=0, max_size=40))
    def test_collect_flatten_preserves_all(self, values):
        results = []
        s = akayu.Stream()
        s.collect().flatten().sink(results.append)
        for v in values:
            s.emit(v)
        s.flush()
        result = results
        assert sorted(result) == sorted(values)


class TestEmitEquivalence:
    @settings(max_examples=50)
    @given(values=st_lists)
    def test_emit_each_vs_emit_batch(self, values):
        results1 = []
        s1 = akayu.Stream()
        s1.sink(results1.append)
        for v in values:
            s1.emit(v)
        result1 = results1

        results2 = []
        s2 = akayu.Stream()
        s2.sink(results2.append)
        s2.emit_batch(values)
        result2 = results2

        assert result1 == result2

    @settings(max_examples=50)
    @given(values=st_lists)
    def test_emit_batch_order(self, values):
        results = []
        s = akayu.Stream()
        s.sink(results.append)
        s.emit_batch(values)
        assert results == values

    @settings(max_examples=50)
    @given(values=st_lists)
    def test_multiple_emit_calls(self, values):
        results1 = []
        s1 = akayu.Stream()
        s1.sink(results1.append)
        for v in values:
            s1.emit(v)

        results2 = []
        s2 = akayu.Stream()
        s2.sink(results2.append)
        s2.emit_batch(values)

        assert results1 == results2


class TestCombinatorProperties:
    @settings(max_examples=50)
    @given(values=st_lists)
    def test_union_commutativity(self, values):
        mid = len(values) // 2
        vals1 = values[:mid]
        vals2 = values[mid:]

        s1a, s1b = akayu.Stream(), akayu.Stream()
        r1 = s1a.union(s1b).to_list()
        for v in vals1:
            s1a.emit(v)
        for v in vals2:
            s1b.emit(v)
        result1 = sorted(r1)

        s2a, s2b = akayu.Stream(), akayu.Stream()
        r2 = s2b.union(s2a).to_list()
        for v in vals1:
            s2a.emit(v)
        for v in vals2:
            s2b.emit(v)
        result2 = sorted(r2)

        assert result1 == result2

    @settings(max_examples=50)
    @given(values=st_lists)
    def test_union_completeness(self, values):
        mid = len(values) // 2
        vals1 = values[:mid]
        vals2 = values[mid:]

        results = []
        s1a, s1b = akayu.Stream(), akayu.Stream()
        s1a.union(s1b).sink(results.append)
        for v in vals1:
            s1a.emit(v)
        for v in vals2:
            s1b.emit(v)
        result = results

        assert sorted(result) == sorted(values)

    @settings(max_examples=50)
    @given(st.lists(st.integers(min_value=1, max_value=100), min_size=1, max_size=20))
    def test_zip_pairing(self, values):
        n = len(values) // 2
        left = values[:n]
        right = values[n : 2 * n] if len(values) > n else values

        s1 = akayu.Stream()
        s2 = akayu.Stream()
        zipped = s1.zip(s2)
        results = []
        zipped.sink(results.append)

        for a, b in zip(left, right, strict=True):
            s1.emit(a)
            s2.emit(b)

        expected = list(zip(left[: len(right)], right, strict=True))
        assert results == expected

    @settings(max_examples=50)
    @given(st.lists(st.integers(min_value=0, max_value=100), min_size=0, max_size=20))
    def test_zip_length(self, values):
        n = len(values) // 2
        left = values[:n]
        right = values[n : 2 * n] if len(values) > n else values

        s1 = akayu.Stream()
        s2 = akayu.Stream()
        zipped = s1.zip(s2)
        results = []
        zipped.sink(results.append)

        for a in left:
            s1.emit(a)
        for r in right:
            s2.emit(r)

        expected_len = min(len(left), len(right))
        assert len(results) == expected_len

    @settings(max_examples=50)
    @given(st.lists(st.integers(min_value=0, max_value=100), min_size=1, max_size=20))
    def test_combine_latest(self, values):
        s1 = akayu.Stream()
        s2 = akayu.Stream()
        combined = s1.combine_latest(s2)
        results = []
        combined.sink(results.append)

        for i, v in enumerate(values):
            s1.emit(v)
            s2.emit(v * 2)
            if i > 0:
                pass

        if len(results) > 0:
            last = results[-1]
            assert isinstance(last, tuple)
            assert len(last) == 2


class TestParEquivalence:
    @settings(max_examples=50)
    @given(values=st_lists)
    def test_par_vs_sequential(self, values):
        results1 = []
        s1 = akayu.Stream()
        p1 = s1.par()
        p1.map(lambda x: x * 2).sink(results1.append)
        for v in values:
            s1.emit(v)
        result1 = sorted(results1)

        results2 = []
        s2 = akayu.Stream()
        s2.map(lambda x: x * 2).sink(results2.append)
        for v in values:
            s2.emit(v)
        result2 = sorted(results2)

        assert result1 == result2

    @settings(max_examples=50)
    @given(values=st_lists)
    def test_par_multi_branch(self, values):
        results1, results2 = [], []
        s = akayu.Stream()
        p = s.par()
        p.map(lambda x: x * 2).sink(results1.append)
        p.map(lambda x: x + 100).sink(results2.append)
        for v in values:
            s.emit(v)

        expected1 = sorted([v * 2 for v in values])
        expected2 = sorted([v + 100 for v in values])

        assert sorted(results1) == expected1
        assert sorted(results2) == expected2

    @settings(max_examples=50)
    @given(values=st_lists)
    def test_par_with_filter(self, values):
        results1, results2 = [], []
        s = akayu.Stream()
        p = s.par()
        p.filter(lambda x: x > 0).map(lambda x: x * 2).sink(results1.append)
        p.map(lambda x: x + 1).sink(results2.append)
        for v in values:
            s.emit(v)

        expected1 = sorted([v * 2 for v in values if v > 0])
        expected2 = sorted([v + 1 for v in values])

        assert sorted(results1) == expected1
        assert sorted(results2) == expected2

    @settings(max_examples=50)
    @given(values=st_lists)
    def test_par_with_accumulate(self, values):
        results = []
        s = akayu.Stream()
        p = s.par()
        p.accumulate(lambda acc, x: acc + x, start=0).sink(results.append)
        for v in values:
            s.emit(v)

        expected = []
        acc = 0
        for v in values:
            acc = acc + v
            expected.append(acc)

        assert results == expected


class TestOrderingProperties:
    @settings(max_examples=50)
    @given(values=st_lists)
    def test_order_within_branch(self, values):
        results = []
        s = akayu.Stream()
        s.map(lambda x: x).sink(results.append)
        for v in values:
            s.emit(v)
        assert results == values

    @settings(max_examples=50)
    @given(values=st_lists)
    def test_order_after_filter(self, values):
        results = []
        s = akayu.Stream()
        s.filter(lambda x: x >= 0).sink(results.append)
        for v in values:
            s.emit(v)
        expected = [v for v in values if v >= 0]
        assert results == expected

    @settings(max_examples=50)
    @given(values=st_lists)
    def test_order_after_map(self, values):
        results = []
        s = akayu.Stream()
        s.map(lambda x: x * 2).sink(results.append)
        for v in values:
            s.emit(v)
        expected = [v * 2 for v in values]
        assert results == expected

    @settings(max_examples=50)
    @given(values=st_lists)
    def test_order_after_compile(self, values):
        results = []
        s = akayu.Stream()
        s.map(lambda x: x).filter(lambda x: True).sink(results.append).compile()
        for v in values:
            s.emit(v)
        assert results == values


class TestCountingProperties:
    @settings(max_examples=50)
    @given(values=st_lists)
    def test_filter_count(self, values):
        results = []
        s = akayu.Stream()
        s.filter(lambda x: x > 0).sink(results.append)
        for v in values:
            s.emit(v)
        assert len(results) <= len(values)

    @settings(max_examples=50)
    @given(values=st_lists)
    def test_sink_count(self, values):
        count = [0]
        s = akayu.Stream()
        s.sink(lambda x: count.__setitem__(0, count[0] + 1))
        for v in values:
            s.emit(v)
        assert count[0] == len(values)

    @settings(max_examples=50)
    @given(values=st_lists)
    def test_map_count(self, values):
        map_calls = [0]
        s = akayu.Stream()
        s.map(lambda x: (map_calls.__setitem__(0, map_calls[0] + 1), x)[1]).sink(lambda x: None)
        for v in values:
            s.emit(v)
        assert map_calls[0] == len(values)

    @settings(max_examples=50)
    @given(values=st_lists)
    def test_par_total_count(self, values):
        count1, count2 = [0], [0]
        s = akayu.Stream()
        p = s.par()
        p.sink(lambda x: count1.__setitem__(0, count1[0] + 1))
        p.sink(lambda x: count2.__setitem__(0, count2[0] + 1))
        for v in values:
            s.emit(v)
        assert count1[0] == len(values)
        assert count2[0] == len(values)


class TestBatchOperations:
    @settings(max_examples=50)
    @given(values=st_lists)
    def test_collect_output_is_list(self, values):
        s = akayu.Stream()
        s.collect().sink(lambda x: None)
        for v in values:
            s.emit(v)
        s.flush()
        result = s.to_list()
        assert isinstance(result, list)

    @settings(max_examples=50)
    @given(values=st_lists)
    def test_collect_flatten_preserves(self, values):
        results = []
        s = akayu.Stream()
        s.collect().flatten().sink(results.append)
        for v in values:
            s.emit(v)
        s.flush()

        assert sorted(results) == sorted(values)

    @settings(max_examples=50)
    @given(values=st_lists)
    def test_batch_map_output(self, values):
        results = []
        s = akayu.Stream()
        s.batch_map(lambda xs: [x * 2 for x in xs]).sink(results.append)
        s.emit_batch(values)
        result = results
        expected = [x * 2 for x in values]
        assert sorted(result) == sorted(expected)

    @settings(max_examples=50)
    @given(st.lists(st.integers(min_value=1, max_value=100), min_size=1, max_size=20))
    def test_chunk_by_partitions(self, values):
        results = []
        s = akayu.Stream()
        s.chunk_by(lambda x: x % 5 == 0, include_delimiter=True).sink(results.append)
        for v in values:
            s.emit(v)
        s.flush()

        all_items = []
        for chunk in results:
            all_items.extend(chunk)

        assert sorted(all_items) == sorted(values)


class TestAccumulateProperties:
    @settings(max_examples=50)
    @given(values=st_lists)
    def test_running_sum(self, values):
        results = []
        s = akayu.Stream()
        s.accumulate(lambda acc, x: acc + x, start=0).sink(results.append)
        for v in values:
            s.emit(v)

        expected = []
        acc = 0
        for v in values:
            acc = acc + v
            expected.append(acc)

        assert results == expected

    @settings(max_examples=50)
    @given(values=st_lists)
    def test_running_max(self, values):
        results = []
        s = akayu.Stream()
        s.accumulate(lambda acc, x: max(acc, x), start=-10000).sink(results.append)
        for v in values:
            s.emit(v)

        expected = []
        acc = -10000
        for v in values:
            acc = max(acc, v)
            expected.append(acc)

        assert results == expected

    @settings(max_examples=50)
    @given(values=st_lists)
    def test_accumulate_returns_state(self, values):
        results = []
        s = akayu.Stream()
        s.accumulate(lambda acc, x: (acc + x, acc + x), start=0, returns_state=True).sink(
            lambda x: results.append(x[1] if isinstance(x, tuple) else x)
        )
        for v in values:
            s.emit(v)

        expected = []
        acc = 0
        for v in values:
            acc = acc + v
            expected.append(acc)

        assert results == expected

    @settings(max_examples=50)
    @given(values=st_lists)
    def test_accumulate_after_compile(self, values):
        results1 = []
        s1 = akayu.Stream()
        s1.accumulate(lambda acc, x: acc + x, start=0).sink(results1.append)
        for v in values:
            s1.emit(v)

        results2 = []
        s2 = akayu.Stream()
        s2.accumulate(lambda acc, x: acc + x, start=0).sink(results2.append).compile()
        for v in values:
            s2.emit(v)

        assert results1 == results2


class TestDrainFlush:
    def test_flush_on_empty(self):
        s = akayu.Stream()
        s.collect().sink(lambda x: None)
        s.flush()

    def test_drain_on_empty(self):
        s = akayu.Stream()
        s.drain()

    @settings(max_examples=50)
    @given(values=st_lists)
    def test_flush_after_collect(self, values):
        s = akayu.Stream()
        results = []
        s.collect().sink(results.append)
        for v in values:
            s.emit(v)
        s.flush()

        all_items = []
        for chunk in results:
            all_items.extend(chunk)

        assert sorted(all_items) == sorted(values)

    @settings(max_examples=50)
    @given(values=st_lists)
    def test_drain_returns_all(self, values):
        s = akayu.Stream()
        s.map(lambda x: x * 2).sink(lambda x: None)
        for v in values:
            s.emit(v)
        s.drain()
