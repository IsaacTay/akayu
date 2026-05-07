import functools

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


