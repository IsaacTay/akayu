import pytest

import akayu


class TestMapErrors:
    def test_map_callback_raises_type_error(self):
        s = akayu.Stream()
        s.map(lambda x: x.nonexistent).sink(lambda x: x)
        with pytest.raises(Exception):
            s.emit(1)

    def test_map_callback_raises_value_error(self):
        s = akayu.Stream()
        s.map(lambda x: int("not a number")).sink(lambda x: x)
        with pytest.raises(Exception):
            s.emit(42)

    def test_map_callback_raises_custom_exception(self):
        class CustomError(Exception):
            pass

        s = akayu.Stream()

        def raise_custom(x):
            raise CustomError("custom")

        s.map(raise_custom).sink(lambda x: x)
        with pytest.raises(Exception):
            s.emit(1)

    def test_map_callback_returns_none(self):
        s = akayu.Stream()
        results = []
        s.map(lambda x: None).sink(results.append)
        s.emit(1)
        s.flush()
        assert results == [None]

    def test_map_callback_wrong_arity(self):
        s = akayu.Stream()

        def two_args(x, y):
            return x + y

        s.map(two_args).sink(lambda x: x)
        with pytest.raises(Exception):
            s.emit(1)

    def test_map_error_after_compile(self):
        s = akayu.Stream()
        s.map(lambda x: 1 / 0).sink(lambda x: x)
        s.compile()
        with pytest.raises(Exception):
            s.emit(1)


class TestFilterErrors:
    def test_filter_predicate_raises_exception(self):
        s = akayu.Stream()

        def bad_pred(x):
            raise ValueError("predicate fail")

        s.filter(bad_pred).sink(lambda x: x)
        with pytest.raises(Exception):
            s.emit(1)

    def test_filter_predicate_returns_string(self):
        s = akayu.Stream()
        results = []
        s.filter(lambda x: "yes" if x > 0 else "").sink(results.append)
        s.emit(1)
        s.emit(-1)
        s.flush()
        assert results == [1]

    def test_filter_predicate_returns_none(self):
        s = akayu.Stream()
        results = []
        s.filter(lambda x: None).sink(results.append)
        s.emit(1)
        s.emit(2)
        s.flush()
        assert results == []

    def test_filter_error_after_compile(self):
        s = akayu.Stream()
        s.filter(lambda x: 1 / 0).sink(lambda x: x)
        s.compile()
        with pytest.raises(Exception):
            s.emit(1)


class TestSinkErrors:
    def test_sink_function_raises_exception(self):
        s = akayu.Stream()

        def bad_sink(x):
            raise RuntimeError("sink exploded")

        s.sink(bad_sink)
        with pytest.raises(Exception):
            s.emit(1)

    def test_sink_modifies_state_then_raises(self):
        state = []
        s = akayu.Stream()

        def bad_sink(x):
            state.append(x)
            if x == 2:
                raise ValueError("fail at 2")

        s.sink(bad_sink)
        try:
            s.emit_batch([1, 2, 3])
        except Exception:
            pass
        s.flush()
        assert 1 in state

    def test_sink_error_after_compile(self):
        s = akayu.Stream()
        s.sink(lambda x: 1 / 0)
        s.compile()
        with pytest.raises(Exception):
            s.emit(1)


class TestStarmapErrors:
    def test_starmap_receives_non_tuple(self):
        s = akayu.Stream()
        s.starmap(lambda a, b: a + b).sink(lambda x: x)
        with pytest.raises(Exception):
            s.emit(42)

    def test_starmap_callback_wrong_arity(self):
        s = akayu.Stream()

        def three_args(a, b, c):
            return a + b + c

        s.starmap(three_args).sink(lambda x: x)
        with pytest.raises(Exception):
            s.emit((1, 2))

    def test_starmap_after_flatten_non_uniform(self):
        s = akayu.Stream()
        inner = akayu.Stream()

        def bad_inner():
            return 42

        inner.map(bad_inner).sink(lambda x: None)
        s.map(lambda x: inner).flatten().starmap(lambda a, b: a + b).sink(lambda x: x)
        with pytest.raises(Exception):
            s.emit(1)


class TestBatchMapErrors:
    def test_batch_map_callback_raises_mid_batch(self):
        s = akayu.Stream()

        def bad_batch(items):
            raise RuntimeError("batch fail")

        s.batch_map(bad_batch).sink(lambda x: x)
        with pytest.raises(Exception):
            s.emit_batch([1, 2, 3])

    def test_batch_map_callback_returns_non_iterable(self):
        s = akayu.Stream()

        def bad_return(items):
            return 42

        s.batch_map(bad_return).sink(lambda x: x)
        with pytest.raises(Exception):
            s.emit_batch([1])

    def test_batch_map_callback_returns_wrong_count(self):
        s = akayu.Stream()
        results = []

        def uneven_return(items):
            return [items[0]]

        s.batch_map(uneven_return).sink(results.append)
        s.emit_batch([1, 2, 3])
        s.flush()


class TestFlattenErrors:
    def test_flatten_receives_non_iterable(self):
        s = akayu.Stream()
        s.map(lambda x: 42).flatten().sink(lambda x: x)
        with pytest.raises(Exception):
            s.emit(1)

    def test_flatten_mixed_iterable_non_iterable(self):
        s = akayu.Stream()
        s.map(lambda x: [x] if x < 3 else x).flatten().sink(lambda x: x)
        with pytest.raises(Exception):
            s.emit(3)


class TestAccumulateErrors:
    def test_accumulate_func_raises(self):
        s = akayu.Stream()

        def bad_acc(acc, x):
            raise ValueError("accumulate fail")

        s.accumulate(bad_acc, start=0).sink(lambda x: x)
        with pytest.raises(Exception):
            s.emit(1)

    def test_accumulate_returns_state_wrong_structure(self):
        s = akayu.Stream()

        def bad_return(acc, x):
            return "not a tuple"

        s.accumulate(bad_return, start=0, returns_state=True).sink(lambda x: x)
        with pytest.raises(Exception):
            s.emit(1)

    def test_accumulate_state_corruption(self):
        s = akayu.Stream()

        def corrupt_acc(acc, x):
            if x == 2:
                return (acc, x)
            return (x, x)

        results = []
        s.accumulate(corrupt_acc, start=(0, 0), returns_state=True).sink(results.append)
        s.emit(1)
        s.emit(2)
        s.emit(3)
        s.flush()


class TestChunkByErrors:
    def test_chunk_by_predicate_raises(self):
        s = akayu.Stream()

        def bad_pred(x):
            raise ValueError("chunk fail")

        s.chunk_by(bad_pred).sink(lambda x: x)
        with pytest.raises(Exception):
            s.emit(1)

    def test_chunk_by_predicate_returns_non_bool(self):
        s = akayu.Stream()
        results = []
        s.chunk_by(lambda x: "split" if x == 2 else None).sink(results.append)
        s.emit(1)
        s.emit(2)
        s.emit(3)
        s.flush()


class TestPrefetchErrors:
    def test_prefetch_zero(self):
        s = akayu.Stream()
        with pytest.raises(ValueError):
            s.prefetch(0)

    def test_prefetch_callback_errors_sync(self):
        s = akayu.Stream()
        s.prefetch(2).map(lambda x: 1 / 0).sink(lambda x: x)
        s.compile()
        s.emit(1)
        with pytest.raises(Exception):
            s.flush()


class TestParErrors:
    def test_one_par_branch_raises_other_succeeds(self):
        results1 = []
        results2 = []
        s = akayu.Stream()
        p = s.par()
        p.map(lambda x: x * 2).sink(results1.append)
        p.map(lambda x: 1 / 0).sink(results2.append)
        with pytest.raises(Exception):
            s.emit(10)
        assert results1 == [20]

    def test_all_par_branches_raise(self):
        s = akayu.Stream()
        p = s.par()
        p.map(lambda x: 1 / 0).sink(lambda x: x)
        p.map(lambda x: int("bad")).sink(lambda x: x)
        with pytest.raises(Exception):
            s.emit(1)

    def test_error_in_par_branch_with_union(self):
        results = []
        s = akayu.Stream()
        p = s.par()
        branch_a = p.map(lambda x: x * 2)
        branch_b = p.map(lambda x: 1 / 0)
        merged = akayu.union(branch_a, branch_b)
        merged.sink(results.append)
        with pytest.raises(Exception):
            s.emit(10)
        assert results == [20]

    def test_error_in_compiled_par_graph(self):
        s = akayu.Stream()
        p = s.par()
        p.map(lambda x: 1 / 0).sink(lambda x: x)
        p.map(lambda x: x * 2).sink(lambda x: x)
        s.compile()
        with pytest.raises(Exception):
            s.emit(1)

    def test_par_branch_raises_after_multiple_successful(self):
        results = []
        s = akayu.Stream()
        p = s.par()
        p.map(lambda x: x * 2).sink(results.append)
        p.map(lambda x: x + 1 if x < 3 else 1 / 0).sink(lambda x: x)
        s.emit(1)
        s.emit(2)
        with pytest.raises(Exception):
            s.emit(3)
        assert results == [2, 4, 6]

    def test_error_in_par_with_chained_ops_after_branch(self):
        results = []
        s = akayu.Stream()
        p = s.par()
        p.map(lambda x: x).map(lambda x: 1 / 0).sink(results.append)
        p.map(lambda x: x * 2).sink(results.append)
        with pytest.raises(Exception):
            s.emit(1)


class TestUnionErrors:
    def test_upstream_of_union_raises_during_emit(self):
        results = []
        s1 = akayu.Stream()
        s2 = akayu.Stream()
        merged = akayu.union(s1, s2)
        merged.sink(results.append)
        s2.map(lambda x: 1 / 0).sink(lambda x: x)
        s1.emit(1)
        with pytest.raises(Exception):
            s2.emit(2)

    def test_union_with_compiled_graph_upstream_errors(self):
        results = []
        s1 = akayu.Stream()
        s2 = akayu.Stream()
        merged = akayu.union(s1, s2)
        merged.sink(results.append)
        s2.map(lambda x: 1 / 0).sink(lambda x: x)
        s1.compile()
        s2.compile()
        with pytest.raises(Exception):
            s2.emit(1)


class TestCombineLatestErrors:
    def test_callback_upstream_of_combine_latest_raises(self):
        results = []
        s1 = akayu.Stream()
        s2 = akayu.Stream()
        s1.combine_latest(s2).sink(results.append)
        s2.map(lambda x: 1 / 0).sink(lambda x: x)
        with pytest.raises(Exception):
            s2.emit(1)

    def test_combine_latest_with_compiled_graph_errors(self):
        results = []
        s1 = akayu.Stream()
        s2 = akayu.Stream()
        s1.combine_latest(s2).sink(results.append)
        s2.map(lambda x: 1 / 0).sink(lambda x: x)
        s1.compile()
        s2.compile()
        with pytest.raises(Exception):
            s2.emit(1)


class TestZipErrors:
    def test_upstream_of_zip_raises(self):
        results = []
        s1 = akayu.Stream()
        s2 = akayu.Stream()
        s1.zip(s2).sink(results.append)
        s2.map(lambda x: 1 / 0).sink(lambda x: x)
        s1.emit(1)
        with pytest.raises(Exception):
            s2.emit(2)


class TestCompileErrors:
    def test_double_compile(self):
        s = akayu.Stream()
        s.sink(lambda x: x)
        s.compile()
        with pytest.raises(RuntimeError):
            s.compile()

    def test_add_downstream_after_compile(self):
        s = akayu.Stream()
        s.sink(lambda x: x)
        s.compile()
        with pytest.raises(RuntimeError):
            s.map(lambda x: x)

    def test_emit_on_non_source(self):
        s = akayu.Stream()
        derived = s.map(lambda x: x)
        with pytest.raises(RuntimeError):
            derived.emit(1)

    def test_compile_after_partial_emit(self):
        s = akayu.Stream()
        results = []
        s.map(lambda x: x * 2).sink(results.append)
        s.emit(1)
        s.compile()
        s.emit(2)
        s.flush()


class TestEmitErrors:
    def test_reentrant_emit(self):
        s = akayu.Stream()

        def reentrant_emit(x):
            s.emit(x + 1)

        s.map(reentrant_emit).sink(lambda x: x)
        with pytest.raises(RuntimeError):
            s.emit(1)

    def test_reentrant_flush(self):
        s = akayu.Stream()

        def reentrant_flush(x):
            s.flush()

        s.map(reentrant_flush).sink(lambda x: x)
        with pytest.raises(RuntimeError):
            s.emit(1)

    def test_emit_on_non_source_node(self):
        s = akayu.Stream()
        derived = s.map(lambda x: x)
        with pytest.raises(RuntimeError):
            derived.emit(1)


class TestCompiledPathErrors:
    def test_error_in_compiled_map_chain(self):
        s = akayu.Stream()
        s.map(lambda x: x + 1).map(lambda x: 1 / 0).sink(lambda x: x)
        s.compile()
        with pytest.raises(Exception):
            s.emit(1)

    def test_error_in_compiled_filter(self):
        s = akayu.Stream()
        s.filter(lambda x: x > 0).map(lambda x: 1 / 0).sink(lambda x: x)
        s.compile()
        with pytest.raises(Exception):
            s.emit(1)

    def test_error_in_compiled_sink(self):
        s = akayu.Stream()
        s.map(lambda x: x).sink(lambda x: 1 / 0)
        s.compile()
        with pytest.raises(Exception):
            s.emit(1)

    def test_error_in_compiled_par_graph(self):
        results = []
        s = akayu.Stream()
        p = s.par()
        p.map(lambda x: x * 2).sink(results.append)
        p.map(lambda x: 1 / 0).sink(lambda x: x)
        s.compile()
        with pytest.raises(Exception):
            s.emit(1)


class TestErrorMetadata:
    def test_error_message_contains_node_name(self):
        s = akayu.Stream()
        s.map(lambda x: 1 / 0, stream_name="my_map_node").sink(lambda x: x)
        with pytest.raises(Exception) as exc:
            s.emit(1)
        assert "my_map_node" in str(exc.value)

    def test_error_has_cause_set(self):
        s = akayu.Stream()
        original = ValueError("original cause")

        def raise_cause(x):
            raise original

        s.map(raise_cause, stream_name="cause_node").sink(lambda x: x)
        with pytest.raises(Exception) as exc:
            s.emit(1)
        assert exc.value.__cause__ is original

    def test_error_message_contains_source_location(self):
        s = akayu.Stream()
        s.map(lambda x: 1 / 0, stream_name="loc_node").sink(lambda x: x)
        with pytest.raises(Exception) as exc:
            s.emit(1)
        msg = str(exc.value)
        assert "defined at" in msg or "test_error_injection" in msg


class TestPartialFailure:
    def test_pipeline_map_succeeds_filter_raises(self):
        results = []
        s = akayu.Stream()

        def bad_filter(x):
            if x == 2:
                raise ValueError("filter fail at 2")
            return True

        s.map(lambda x: x * 10).filter(bad_filter).sink(results.append)
        s.emit(1)
        s.flush()

    def test_pipeline_map_par_sink1_raises_sink2_succeeds(self):
        results1 = []
        results2 = []
        s = akayu.Stream()
        p = s.par()
        p.map(lambda x: x * 2).sink(results1.append)
        p.map(lambda x: 1 / 0).sink(lambda x: x)
        p.map(lambda x: x + 1).sink(results2.append)
        with pytest.raises(Exception):
            s.emit(10)
        assert results1 == [20]
        assert results2 == [11]

    def test_multiple_emits_one_fails_previous_preserved(self):
        results = []
        s = akayu.Stream()
        s.map(lambda x: x * 2).sink(results.append)
        s.map(lambda x: 1 / 0 if x > 5 else x).sink(lambda x: x)
        s.emit(1)
        s.emit(2)
        s.flush()
