import pytest

import akayu


def test_modify_after_compile_map():
    s = akayu.Stream()
    s.compile()
    with pytest.raises(RuntimeError, match="Cannot modify frozen stream"):
        s.map(lambda x: x + 1)


def test_modify_after_compile_filter():
    s = akayu.Stream()
    s.compile()
    with pytest.raises(RuntimeError, match="Cannot modify frozen stream"):
        s.filter(lambda x: x > 0)


def test_modify_after_compile_sink():
    s = akayu.Stream()
    s.compile()
    with pytest.raises(RuntimeError, match="Cannot modify frozen stream"):
        s.sink(print)


def test_modify_after_compile_union():
    s1 = akayu.Stream()
    s2 = akayu.Stream()
    s1.compile()
    with pytest.raises(RuntimeError, match="Cannot modify frozen stream"):
        s1.union(s2)
    with pytest.raises(RuntimeError, match="Cannot modify frozen stream"):
        akayu.union(s1, s2)


def test_modify_after_compile_zip():
    s1 = akayu.Stream()
    s2 = akayu.Stream()
    s1.compile()
    with pytest.raises(RuntimeError, match="Cannot modify frozen stream"):
        s1.zip(s2)
    with pytest.raises(RuntimeError, match="Cannot modify frozen stream"):
        akayu.zip(s1, s2)


def test_modify_after_compile_combine_latest():
    s1 = akayu.Stream()
    s2 = akayu.Stream()
    s1.compile()
    with pytest.raises(RuntimeError, match="Cannot modify frozen stream"):
        s1.combine_latest(s2)
    with pytest.raises(RuntimeError, match="Cannot modify frozen stream"):
        akayu.combine_latest(s1, s2)


def test_double_compile_raises():
    s = akayu.Stream()
    s.compile()
    with pytest.raises(RuntimeError, match="already compiled"):
        s.compile()


def test_modify_downstream_after_compile():
    s = akayu.Stream()
    n = s.map(lambda x: x)
    s.compile()
    with pytest.raises(RuntimeError, match="Cannot modify frozen stream"):
        n.map(lambda x: x)


def test_node_updates_are_not_python_methods():
    """`_update`/`_update_batch` let a callback start a second update of a
    compiled node mid-update: two `&mut` to one node through its raw pointer."""
    assert not hasattr(akayu.Stream, "_update")
    assert not hasattr(akayu.Stream, "_update_batch")


@pytest.mark.parametrize("call", ["emit", "emit_batch"])
def test_emit_into_a_source_whose_child_was_folded_into_a_chain(call):
    """compile() folds the map after a seq() into the chain above it; an
    emit into the seq() node raises, and emit_batch used to panic."""
    s = akayu.Stream()
    q = s.map(lambda x: x).seq()
    out = q.map(lambda x: x + 1).to_list()
    s.compile()
    with pytest.raises(akayu.NotASourceError, match="folded into a chain"):
        getattr(q, call)(1 if call == "emit" else [1, 2])
    s.emit(1)
    assert out == [2]
