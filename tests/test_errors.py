import pytest
import rstreamz


def test_error_propagation():
    s = rstreamz.Stream()

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
    s = rstreamz.Stream(asynchronous=False)

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
    s = rstreamz.Stream()

    def bad_pred(x):
        raise ValueError("predicate error")

    s.filter(bad_pred, stream_name="bad_filter").sink(lambda x: None)
    with pytest.raises(RuntimeError) as exc:
        s.emit(1)
    assert "bad_filter" in str(exc.value)
