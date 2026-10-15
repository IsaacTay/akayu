import functools
import pathlib

import pytest

import akayu

# ---------------------------------------------------------------------------
# wrap_error: node name + cause chaining
# ---------------------------------------------------------------------------


def test_error_propagation():
    s = akayu.Stream()

    def failing_func(x):
        raise ValueError("Original Error")

    # Name the node specifically to check for it
    s.map(failing_func, stream_name="failing_node").sink(lambda x: None)

    with pytest.raises(RuntimeError) as excinfo:
        s.emit(1)

    # Check that our wrapper error is present
    assert "Stream operation failed at node 'failing_node'" in str(excinfo.value)
    # Check that the original error is chained
    assert excinfo.value.__cause__ is not None
    assert isinstance(excinfo.value.__cause__, ValueError)
    assert str(excinfo.value.__cause__) == "Original Error"


def test_error_propagation_batch():
    s = akayu.Stream()

    def failing_func(x):
        if x == 2:
            raise ValueError("Batch Error")
        return x

    s.map(failing_func, stream_name="batch_fail_node").sink(lambda x: None)

    with pytest.raises(RuntimeError) as excinfo:
        s.emit_batch([1, 2, 3])

    assert "Stream operation failed at node 'batch_fail_node'" in str(excinfo.value)
    assert "Batch Error" in str(excinfo.value.__cause__)


def test_error_in_filter():
    """Test error propagation from filter predicate."""
    s = akayu.Stream()

    def bad_pred(x):
        raise ValueError("predicate error")

    s.filter(bad_pred, stream_name="bad_filter").sink(lambda x: None)
    with pytest.raises(RuntimeError) as exc:
        s.emit(1)
    assert "bad_filter" in str(exc.value)


def test_error_in_sink():
    """Test error propagation from a sink function."""
    s = akayu.Stream()

    def bad_sink(x):
        raise TypeError("sink exploded")

    s.sink(bad_sink, stream_name="boom_sink")

    with pytest.raises(RuntimeError) as exc:
        s.emit(1)
    assert "boom_sink" in str(exc.value)
    assert isinstance(exc.value.__cause__, TypeError)
    assert "sink exploded" in str(exc.value.__cause__)


def test_error_in_accumulate():
    """Test error propagation from an accumulate function."""
    s = akayu.Stream()

    def bad_acc(state, x):
        raise ValueError("acc failed")

    s.accumulate(bad_acc, start=0).sink(lambda x: None)

    with pytest.raises(RuntimeError) as exc:
        s.emit(1)
    assert "accumulate" in str(exc.value)
    assert isinstance(exc.value.__cause__, ValueError)


def test_error_in_batch_map():
    """Test error propagation from a batch_map function."""
    s = akayu.Stream()

    def bad_batch(items):
        raise RuntimeError("batch kaboom")

    s.batch_map(bad_batch, stream_name="bad_batch").sink(lambda x: None)

    with pytest.raises(RuntimeError) as exc:
        s.emit_batch([1, 2])
    assert "bad_batch" in str(exc.value)
    assert isinstance(exc.value.__cause__, RuntimeError)
    assert "batch kaboom" in str(exc.value.__cause__)


# ---------------------------------------------------------------------------
# Source location appears in error messages
# ---------------------------------------------------------------------------


def test_error_includes_source_location():
    """wrap_error messages include 'defined at <file>:<line>'."""
    s = akayu.Stream()

    def kaboom(x):
        raise ValueError("boom")

    s.map(kaboom, stream_name="loc_node").sink(lambda x: None)

    with pytest.raises(RuntimeError) as exc:
        s.emit(1)
    msg = str(exc.value)
    assert "defined at" in msg
    # The location should reference this test file
    assert "test_errors.py" in msg


# ---------------------------------------------------------------------------
# Auto-generated node names include function name
# ---------------------------------------------------------------------------


def test_auto_node_name_from_function():
    """When no stream_name is given, the node name includes the function name."""
    s = akayu.Stream()

    def my_transform(x):
        raise ValueError("fail")

    # No explicit stream_name — node should be auto-named "map(my_transform)"
    s.map(my_transform).sink(lambda x: None)

    with pytest.raises(RuntimeError) as exc:
        s.emit(1)
    assert "my_transform" in str(exc.value)


def test_auto_node_name_lambda():
    """Lambda functions get '<lambda>' in the auto-generated node name."""
    s = akayu.Stream()
    s.map(lambda x: 1 / 0).sink(lambda x: None)

    with pytest.raises(RuntimeError) as exc:
        s.emit(1)
    assert "<lambda>" in str(exc.value)


def test_auto_node_name_partial():
    """functools.partial extracts the wrapped function's name."""
    s = akayu.Stream()

    def multiply(x, factor):
        return x * factor

    p = functools.partial(multiply, factor=0)

    # Force an error downstream so we can inspect the node name
    def explode(x):
        raise ValueError("boom")

    s.map(p).map(explode).sink(lambda x: None)

    with pytest.raises(RuntimeError) as exc:
        s.emit(1)
    # The first map node should have "multiply" in its name via partial extraction
    # but the error comes from the second map — verify partial didn't break naming
    assert "explode" in str(exc.value)


def test_auto_node_name_partial_itself_fails():
    """functools.partial that raises — node name includes the wrapped function name."""
    s = akayu.Stream()

    def bad_func(x, extra):
        raise ValueError("partial fail")

    p = functools.partial(bad_func, extra=42)
    s.map(p).sink(lambda x: None)

    with pytest.raises(RuntimeError) as exc:
        s.emit(1)
    # get_func_name extracts "bad_func" from the partial's .func.__name__
    assert "bad_func" in str(exc.value)


def test_auto_node_name_builtin():
    """Builtin functions (e.g. str) get their name in the node."""
    s = akayu.Stream()

    # str() won't fail, so chain with a failing sink
    def fail_sink(x):
        raise ValueError("sink fail")

    m = s.map(str)
    m.sink(fail_sink)

    with pytest.raises(RuntimeError) as exc:
        s.emit(1)
    # The error comes from the sink node, which should show "fail_sink"
    assert "fail_sink" in str(exc.value)


def test_auto_node_name_callable_class():
    """Callable instances without __name__ fall back to the bare operation name."""
    s = akayu.Stream()

    class MyCallable:
        def __call__(self, x):
            raise ValueError("callable fail")

    s.map(MyCallable()).sink(lambda x: None)

    with pytest.raises(RuntimeError) as exc:
        s.emit(1)
    msg = str(exc.value)
    # Instances don't have __name__/__qualname__, so node name is just "map"
    assert "node 'map'" in msg
    assert isinstance(exc.value.__cause__, ValueError)


# ---------------------------------------------------------------------------
# StreamError::EmitOnNonSource
# ---------------------------------------------------------------------------


def test_emit_on_non_source():
    """emit() on a derived (non-source) node raises RuntimeError."""
    s = akayu.Stream()
    derived = s.map(lambda x: x)

    with pytest.raises(RuntimeError, match="non-source node"):
        derived.emit(1)


def test_emit_batch_on_non_source():
    """emit_batch() on a derived (non-source) node raises RuntimeError."""
    s = akayu.Stream()
    derived = s.map(lambda x: x)

    with pytest.raises(RuntimeError, match="non-source node"):
        derived.emit_batch([1, 2])


# ---------------------------------------------------------------------------
# StreamError::FrozenStream — modify after compile()
# ---------------------------------------------------------------------------


def test_frozen_stream_map():
    """Adding a map after compile() raises RuntimeError."""
    s = akayu.Stream()
    s.sink(lambda x: None)
    s.compile()

    with pytest.raises(RuntimeError, match="frozen stream"):
        s.map(lambda x: x)


def test_frozen_stream_filter():
    """Adding a filter after compile() raises RuntimeError."""
    s = akayu.Stream()
    s.sink(lambda x: None)
    s.compile()

    with pytest.raises(RuntimeError, match="frozen stream"):
        s.filter(lambda x: True)


def test_frozen_stream_sink():
    """Adding a sink after compile() raises RuntimeError."""
    s = akayu.Stream()
    s.sink(lambda x: None)
    s.compile()

    with pytest.raises(RuntimeError, match="frozen stream"):
        s.sink(lambda x: None)


# ---------------------------------------------------------------------------
# StreamError::AlreadyCompiled — double compile()
# ---------------------------------------------------------------------------


def test_already_compiled():
    """Calling compile() twice raises RuntimeError."""
    s = akayu.Stream()
    s.sink(lambda x: None)
    s.compile()

    with pytest.raises(RuntimeError, match="already compiled"):
        s.compile()


# ---------------------------------------------------------------------------
# StreamError::InvalidPrefetchSize
# ---------------------------------------------------------------------------


def test_invalid_prefetch_size():
    """prefetch(0) raises ValueError."""
    s = akayu.Stream()

    with pytest.raises(ValueError, match="prefetch size must be at least 1"):
        s.prefetch(0)


# ---------------------------------------------------------------------------
# StreamError::UnionEmpty
# ---------------------------------------------------------------------------


def test_union_empty():
    """akayu.union() with no streams raises ValueError."""
    with pytest.raises(ValueError, match="at least two streams"):
        akayu.union()


# ---------------------------------------------------------------------------
# StreamError::CombineLatestEmpty
# ---------------------------------------------------------------------------


def test_combine_latest_empty():
    """akayu.combine_latest() with no streams raises ValueError."""
    with pytest.raises(ValueError, match="at least two streams"):
        akayu.combine_latest()


# ---------------------------------------------------------------------------
# Fused node errors (compile() fuses chains — errors show combined names)
# ---------------------------------------------------------------------------


def test_fused_map_map_error():
    """After compile(), compiled chain attributes error to failing step."""
    s = akayu.Stream()

    def step_one(x):
        return x + 1

    def step_two(x):
        raise ValueError("step two failed")

    s.map(step_one).map(step_two)
    s.compile()

    with pytest.raises(RuntimeError) as exc:
        s.emit(1)
    msg = str(exc.value)
    # Error attributed to the specific failing node
    assert "step_two" in msg


def test_fused_map_map_source_location():
    """Compiled chain error shows source location of the failing step."""
    s = akayu.Stream()

    def first(x):
        return x

    def second(x):
        raise ValueError("fail")

    s.map(first).map(second)
    s.compile()

    with pytest.raises(RuntimeError) as exc:
        s.emit(1)
    msg = str(exc.value)
    assert "test_errors.py" in msg
    assert "map(second)" in msg


def test_fused_map_sink_error():
    """Compiled chain: error in sink pinpoints the sink function."""
    s = akayu.Stream()

    def transform(x):
        return x * 2

    def bad_sink(x):
        raise ValueError("sink fail")

    s.map(transform).sink(bad_sink)
    s.compile()

    with pytest.raises(RuntimeError) as exc:
        s.emit(1)
    msg = str(exc.value)
    assert "sink(bad_sink)" in msg


def test_fused_map_sink_map_error():
    """Compiled chain: error in map pinpoints the map function."""
    s = akayu.Stream()

    def bad_transform(x):
        raise ValueError("map fail")

    def good_sink(x):
        pass

    s.map(bad_transform).sink(good_sink)
    s.compile()

    with pytest.raises(RuntimeError) as exc:
        s.emit(1)
    msg = str(exc.value)
    assert "map(bad_transform)" in msg


def test_fused_long_map_chain():
    """A chain of many maps compiles into one Chain node; error narrows to failing func."""
    s = akayu.Stream()

    def add_one(x):
        return x + 1

    def double(x):
        return x * 2

    def negate(x):
        return -x

    def to_str(x):
        return str(x)

    def explode(x):
        raise ValueError("chain end")

    s.map(add_one).map(double).map(negate).map(to_str).map(explode)
    s.compile()

    with pytest.raises(RuntimeError) as exc:
        s.emit(1)
    msg = str(exc.value)
    assert "map(explode)" in msg
    assert "explode" in msg


def test_fused_long_map_chain_with_sink():
    """Long map chain + sink: error narrows to the mid-chain failing function."""
    s = akayu.Stream()

    def step_a(x):
        return x + 1

    def step_b(x):
        return x * 2

    def step_c(x):
        raise ValueError("mid-chain fail")

    def step_d(x):
        return x

    def my_sink(x):
        pass

    s.map(step_a).map(step_b).map(step_c).map(step_d).sink(my_sink)
    s.compile()

    with pytest.raises(RuntimeError) as exc:
        s.emit(1)
    msg = str(exc.value)
    assert "map(step_c)" in msg
    assert "step_c" in msg


# ---------------------------------------------------------------------------
# Compiled chain error attribution edge cases
# ---------------------------------------------------------------------------


def test_narrowing_duplicate_names():
    """Compiled chain correctly attributes error when functions have same name."""
    s = akayu.Stream()

    def process(x):
        return x + 1

    process1 = process

    def process(x):
        raise ValueError("second")

    process2 = process

    s.map(process1).map(process2)
    s.compile()

    with pytest.raises(RuntimeError) as exc:
        s.emit(1)
    msg = str(exc.value)
    assert "map(process)" in msg


def test_narrowing_multiple_lambdas():
    """Compiled chain correctly attributes error with multiple lambdas."""
    s = akayu.Stream()
    s.map(lambda x: x + 1).map(lambda x: 1 / 0)
    s.compile()

    with pytest.raises(RuntimeError) as exc:
        s.emit(1)
    msg = str(exc.value)
    assert "map(<lambda>)" in msg


def test_narrowing_nested_call():
    """When the user func calls a helper that raises, chain attributes to the user func."""
    s = akayu.Stream()

    def helper():
        raise ValueError("inner fail")

    def outer(x):
        helper()
        return x

    s.map(lambda x: x).map(outer)
    s.compile()

    with pytest.raises(RuntimeError) as exc:
        s.emit(1)
    msg = str(exc.value)
    assert "map(outer)" in msg


def test_narrowing_with_extra_args():
    """Compiled chain pinpoints the failing map function with extra args."""
    s = akayu.Stream()

    def add_n(x, n):
        raise ValueError("wrapped fail")

    s.map(add_n, 5).sink(lambda x: None)
    s.compile()

    with pytest.raises(RuntimeError) as exc:
        s.emit(1)
    msg = str(exc.value)
    assert "map(add_n)" in msg


class TestErrorTypes:
    """Every error about a graph's state is an akayu.StreamError, itself a
    RuntimeError, so `except RuntimeError` keeps working."""

    def test_hierarchy(self):
        for cls in (
            akayu.CallbackError,
            akayu.FrozenGraphError,
            akayu.NotASourceError,
            akayu.ReentrantError,
            akayu.BusyError,
            akayu.DeadlockError,
        ):
            assert issubclass(cls, akayu.StreamError)
            assert cls.__module__ == "akayu"
        assert issubclass(akayu.StreamError, RuntimeError)

    def test_callback_error_names_the_node(self, compiled):
        s = akayu.Stream()
        s.map(lambda x: 1 / x, stream_name="inverse").sink([].append)
        if compiled:
            s.compile()
        with pytest.raises(akayu.CallbackError) as info:
            s.emit(0)
        assert info.value.node == "inverse"
        # file:line; compared by name, since under Wine the recorded path is the
        # Unix one while __file__ is the Z:\\ one
        file, line = info.value.location.rsplit(":", 1)
        assert pathlib.PurePath(file).name == pathlib.PurePath(__file__).name and line.isdigit()
        assert isinstance(info.value.__cause__, ZeroDivisionError)

    def test_a_wrapped_error_is_not_wrapped_again(self):
        s = akayu.Stream()
        inner = akayu.Stream()
        inner.map(lambda x: 1 / x).sink([].append)
        s.sink(inner.emit)
        with pytest.raises(akayu.CallbackError) as info:
            s.emit(0)
        assert isinstance(info.value.__cause__, ZeroDivisionError)

    def test_print_sink_error_is_wrapped_like_any_other(self, compiled):
        """Compiled, sink(print) let the error through unwrapped."""

        class Unprintable:
            def __str__(self):
                raise ValueError("no")

        s = akayu.Stream()
        s.sink(print)
        if compiled:
            s.compile()
        with pytest.raises(akayu.CallbackError) as info:
            s.emit(Unprintable())
        assert isinstance(info.value.__cause__, ValueError)
        with pytest.raises(akayu.CallbackError):
            s.emit_batch([Unprintable()])

    def test_state_errors(self):
        s = akayu.Stream()
        m = s.map(str)
        with pytest.raises(akayu.NotASourceError):
            m.emit(1)
        s.compile()
        with pytest.raises(akayu.FrozenGraphError):
            s.map(str)
        with pytest.raises(akayu.FrozenGraphError):
            s.compile()

    def test_reentrant_emit(self):
        s = akayu.Stream()
        s.sink(lambda x: s.emit(x) if x < 1 else None)
        with pytest.raises(akayu.CallbackError) as info:
            s.emit(0)
        assert isinstance(info.value.__cause__, akayu.ReentrantError)

    def test_invalid_arguments_stay_value_errors(self):
        with pytest.raises(ValueError):
            akayu.Stream().prefetch(0)
        with pytest.raises(ValueError):
            akayu.union(akayu.Stream())


# --- Errors raised inside each kind of step (was test_error_injection.py) ---


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
