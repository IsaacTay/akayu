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
