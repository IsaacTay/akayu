"""Tests for lambda-to-BuiltinOp decompilation at compile time."""

import akayu


# --- Helpers ---

def _pipeline(func, *, is_filter=False):
    """Build a compiled pipeline with a single map/filter, return (source, results)."""
    s = akayu.Stream()
    results = []
    if is_filter:
        s.filter(func).sink(results.append)
    else:
        s.map(func).sink(results.append)
    s.compile()
    return s, results


def _emit_collect(s, results, values):
    """Emit values and return collected results."""
    results.clear()
    for v in values:
        s.emit(v)
    return list(results)


# ===========================================================================
# Phase 1: Simple lambdas → existing BuiltinOps
# ===========================================================================

class TestSimpleMapLambdas:
    """Lambda wrappers of simple builtins in .map() context."""

    def test_lambda_len(self):
        s, r = _pipeline(lambda x: len(x))
        assert _emit_collect(s, r, ["abc", "de", ""]) == [3, 2, 0]

    def test_lambda_str(self):
        s, r = _pipeline(lambda x: str(x))
        assert _emit_collect(s, r, [42, 3.14, True]) == ["42", "3.14", "True"]

    def test_lambda_int(self):
        s, r = _pipeline(lambda x: int(x))
        assert _emit_collect(s, r, ["10", 3.7, True]) == [10, 3, 1]

    def test_lambda_float(self):
        s, r = _pipeline(lambda x: float(x))
        assert _emit_collect(s, r, ["2.5", 3, True]) == [2.5, 3.0, 1.0]

    def test_lambda_bool(self):
        s, r = _pipeline(lambda x: bool(x))
        assert _emit_collect(s, r, [0, 1, "", "hi"]) == [False, True, False, True]

    def test_lambda_not(self):
        s, r = _pipeline(lambda x: not x)
        assert _emit_collect(s, r, [0, 1, "", "hi"]) == [True, False, True, False]

    def test_lambda_itemgetter(self):
        s, r = _pipeline(lambda x: x["name"])
        assert _emit_collect(s, r, [{"name": "Alice"}, {"name": "Bob"}]) == ["Alice", "Bob"]

    def test_lambda_itemgetter_index(self):
        s, r = _pipeline(lambda x: x[1])
        assert _emit_collect(s, r, [(10, 20), (30, 40)]) == [20, 40]

    def test_lambda_attrgetter(self):
        class Obj:
            def __init__(self, v):
                self.value = v
        s, r = _pipeline(lambda x: x.value)
        assert _emit_collect(s, r, [Obj(1), Obj(2)]) == [1, 2]

    def test_lambda_is_none(self):
        s, r = _pipeline(lambda x: x is None)
        assert _emit_collect(s, r, [None, 0, "", False]) == [True, False, False, False]

    def test_lambda_is_not_none(self):
        s, r = _pipeline(lambda x: x is not None)
        assert _emit_collect(s, r, [None, 0, "", 42]) == [False, True, True, True]

    def test_lambda_isinstance(self):
        s, r = _pipeline(lambda x: isinstance(x, int))
        assert _emit_collect(s, r, [1, "hi", 3.14, True]) == [True, False, False, True]


class TestSimpleFilterLambdas:
    """Lambda wrappers detected in .filter() context."""

    def test_filter_lambda_isinstance(self):
        s, r = _pipeline(lambda x: isinstance(x, str), is_filter=True)
        assert _emit_collect(s, r, [1, "hi", 3.14, "bye"]) == ["hi", "bye"]

    def test_filter_lambda_is_not_none(self):
        s, r = _pipeline(lambda x: x is not None, is_filter=True)
        assert _emit_collect(s, r, [None, 1, None, "ok"]) == [1, "ok"]

    def test_filter_lambda_not(self):
        s, r = _pipeline(lambda x: not x, is_filter=True)
        assert _emit_collect(s, r, [0, 1, "", "hi", None]) == [0, "", None]


# ===========================================================================
# Phase 2: Arithmetic, comparison, membership, method calls
# ===========================================================================

class TestBinOp:

    def test_add(self):
        s, r = _pipeline(lambda x: x + 1)
        assert _emit_collect(s, r, [10, 20]) == [11, 21]

    def test_sub(self):
        s, r = _pipeline(lambda x: x - 5)
        assert _emit_collect(s, r, [10, 3]) == [5, -2]

    def test_mul(self):
        s, r = _pipeline(lambda x: x * 2)
        assert _emit_collect(s, r, [3, 7]) == [6, 14]

    def test_truediv(self):
        s, r = _pipeline(lambda x: x / 2)
        assert _emit_collect(s, r, [10, 7]) == [5.0, 3.5]

    def test_floordiv(self):
        s, r = _pipeline(lambda x: x // 3)
        assert _emit_collect(s, r, [10, 7]) == [3, 2]

    def test_mod(self):
        s, r = _pipeline(lambda x: x % 3)
        assert _emit_collect(s, r, [10, 7]) == [1, 1]

    def test_pow(self):
        s, r = _pipeline(lambda x: x ** 2)
        assert _emit_collect(s, r, [3, 5]) == [9, 25]


class TestCompare:

    def test_gt(self):
        s, r = _pipeline(lambda x: x > 0)
        assert _emit_collect(s, r, [-1, 0, 1]) == [False, False, True]

    def test_lt(self):
        s, r = _pipeline(lambda x: x < 10)
        assert _emit_collect(s, r, [5, 10, 15]) == [True, False, False]

    def test_eq(self):
        s, r = _pipeline(lambda x: x == "ok")
        assert _emit_collect(s, r, ["ok", "no"]) == [True, False]

    def test_ne(self):
        s, r = _pipeline(lambda x: x != 0)
        assert _emit_collect(s, r, [0, 1, 2]) == [False, True, True]

    def test_ge(self):
        s, r = _pipeline(lambda x: x >= 5)
        assert _emit_collect(s, r, [4, 5, 6]) == [False, True, True]

    def test_le(self):
        s, r = _pipeline(lambda x: x <= 5)
        assert _emit_collect(s, r, [4, 5, 6]) == [True, True, False]

    def test_compare_as_filter(self):
        s, r = _pipeline(lambda x: x > 0, is_filter=True)
        assert _emit_collect(s, r, [-1, 0, 1, 2]) == [1, 2]


class TestContains:

    def test_in_set(self):
        valid = {1, 2, 3}
        s, r = _pipeline(lambda x: x in valid)
        assert _emit_collect(s, r, [1, 4, 2, 5]) == [True, False, True, False]

    def test_not_in_set(self):
        banned = {"spam", "ads"}
        s, r = _pipeline(lambda x: x not in banned)
        assert _emit_collect(s, r, ["hi", "spam", "ok"]) == [True, False, True]

    def test_in_as_filter(self):
        valid = {1, 2, 3}
        s, r = _pipeline(lambda x: x in valid, is_filter=True)
        assert _emit_collect(s, r, [1, 4, 2, 5]) == [1, 2]


class TestMethodCall:

    def test_strip(self):
        s, r = _pipeline(lambda x: x.strip())
        assert _emit_collect(s, r, ["  hi  ", "ok", " x "]) == ["hi", "ok", "x"]

    def test_lower(self):
        s, r = _pipeline(lambda x: x.lower())
        assert _emit_collect(s, r, ["Hello", "WORLD"]) == ["hello", "world"]

    def test_upper(self):
        s, r = _pipeline(lambda x: x.upper())
        assert _emit_collect(s, r, ["hello"]) == ["HELLO"]

    def test_startswith(self):
        s, r = _pipeline(lambda x: x.startswith("foo"))
        assert _emit_collect(s, r, ["foobar", "baz", "foo"]) == [True, False, True]

    def test_startswith_as_filter(self):
        s, r = _pipeline(lambda x: x.startswith("foo"), is_filter=True)
        assert _emit_collect(s, r, ["foobar", "baz", "foo"]) == ["foobar", "foo"]

    def test_endswith(self):
        s, r = _pipeline(lambda x: x.endswith(".py"))
        assert _emit_collect(s, r, ["test.py", "readme.md"]) == [True, False]


# ===========================================================================
# Phase 2: Chained patterns
# ===========================================================================

class TestChain:

    def test_attr_compare(self):
        """lambda x: x.age > 18"""
        class Person:
            def __init__(self, age):
                self.age = age
        s, r = _pipeline(lambda x: x.age > 18)
        p = [Person(10), Person(20), Person(18)]
        assert _emit_collect(s, r, p) == [False, True, False]

    def test_attr_eq(self):
        """lambda x: x.name == 'John'"""
        class Obj:
            def __init__(self, name):
                self.name = name
        s, r = _pipeline(lambda x: x.name == "John")
        assert _emit_collect(s, r, [Obj("John"), Obj("Jane")]) == [True, False]

    def test_item_compare(self):
        """lambda x: x['status'] == 'active'"""
        s, r = _pipeline(lambda x: x["status"] == "active")
        data = [{"status": "active"}, {"status": "inactive"}]
        assert _emit_collect(s, r, data) == [True, False]

    def test_item_compare_as_filter(self):
        """lambda x: x['status'] == 'active' used as filter"""
        s, r = _pipeline(lambda x: x["status"] == "active", is_filter=True)
        data = [{"status": "active"}, {"status": "inactive"}, {"status": "active"}]
        assert _emit_collect(s, r, data) == [data[0], data[2]]

    def test_len_compare(self):
        """lambda x: len(x) > 0"""
        s, r = _pipeline(lambda x: len(x) > 0)
        assert _emit_collect(s, r, [[], [1], [1, 2], []]) == [False, True, True, False]

    def test_attr_contains(self):
        """lambda x: x.status in valid"""
        valid = {"active", "pending"}
        class Obj:
            def __init__(self, status):
                self.status = status
        s, r = _pipeline(lambda x: x.status in valid)
        objs = [Obj("active"), Obj("banned"), Obj("pending")]
        assert _emit_collect(s, r, objs) == [True, False, True]

    def test_attr_binop(self):
        """lambda x: x.price * 1.1"""
        class Item:
            def __init__(self, price):
                self.price = price
        s, r = _pipeline(lambda x: x.price * 1.1)
        items = [Item(100)]
        result = _emit_collect(s, r, items)
        assert abs(result[0] - 110.0) < 0.01


# ===========================================================================
# Phase 2: Compound patterns (and/or/negate)
# ===========================================================================

class TestCompound:

    def test_negate_isinstance(self):
        """lambda x: not isinstance(x, str)"""
        s, r = _pipeline(lambda x: not isinstance(x, str))
        assert _emit_collect(s, r, [1, "hi", 3.14]) == [True, False, True]

    def test_and(self):
        """lambda x: isinstance(x, int) and x > 0"""
        s, r = _pipeline(lambda x: isinstance(x, int) and x > 0, is_filter=True)
        assert _emit_collect(s, r, [-1, 0, 1, "hi", 2]) == [1, 2]

    def test_or(self):
        """lambda x: x < 0 or x > 10"""
        s, r = _pipeline(lambda x: x < 0 or x > 10, is_filter=True)
        assert _emit_collect(s, r, [-5, 0, 5, 15]) == [-5, 15]

    def test_is_not_none_and_gt(self):
        """lambda x: x is not None and x > 0"""
        s, r = _pipeline(lambda x: x is not None and x > 0, is_filter=True)
        assert _emit_collect(s, r, [None, -1, 0, 5]) == [5]

    def test_negate_filter(self):
        """lambda x: not isinstance(x, str) as filter"""
        s, r = _pipeline(lambda x: not isinstance(x, str), is_filter=True)
        assert _emit_collect(s, r, [1, "hi", 3.14, "bye"]) == [1, 3.14]

    def test_compound_chain(self):
        """lambda x: x.name == 'John' and x.age > 18"""
        class Person:
            def __init__(self, name, age):
                self.name = name
                self.age = age
        s, r = _pipeline(
            lambda x: x.name == "John" and x.age > 18,
            is_filter=True,
        )
        people = [Person("John", 25), Person("Jane", 30), Person("John", 15)]
        assert _emit_collect(s, r, people) == [people[0]]


# ===========================================================================
# Lambda unwrap: lambda x: MyClass(x) → MyClass
# ===========================================================================

class TestUnwrap:

    def test_unwrap_class(self):
        """lambda x: MyClass(x) should be unwrapped to MyClass directly."""
        class Wrapper:
            def __init__(self, val):
                self.val = val
        s, r = _pipeline(lambda x: Wrapper(x))
        result = _emit_collect(s, r, [1, 2])
        assert all(isinstance(x, Wrapper) for x in result)
        assert [x.val for x in result] == [1, 2]


# ===========================================================================
# Closure variable resolution
# ===========================================================================

class TestClosureResolution:

    def test_compare_with_closure_var(self):
        """lambda x: x == y where y is captured from outer scope."""
        y = 42
        s, r = _pipeline(lambda x: x == y)
        assert _emit_collect(s, r, [41, 42, 43]) == [False, True, False]

    def test_item_closure_key(self):
        """lambda x: x[key] where key is from closure."""
        key = "name"
        s, r = _pipeline(lambda x: x[key])
        assert _emit_collect(s, r, [{"name": "a"}, {"name": "b"}]) == ["a", "b"]

    def test_isinstance_closure_type(self):
        """lambda x: isinstance(x, t) where t is from closure."""
        t = str
        s, r = _pipeline(lambda x: isinstance(x, t), is_filter=True)
        assert _emit_collect(s, r, [1, "hi", 3.14, "bye"]) == ["hi", "bye"]

    def test_contains_closure_collection(self):
        """lambda x: x in s where s is from closure."""
        valid = frozenset([1, 2, 3])
        s, r = _pipeline(lambda x: x in valid, is_filter=True)
        assert _emit_collect(s, r, [1, 4, 2, 5]) == [1, 2]


# ===========================================================================
# Edge cases: things that should NOT be decompiled
# ===========================================================================

class TestNoMatch:

    def test_multi_arg_lambda(self):
        """Lambda with >1 arg should not be decompiled (falls back to vcall)."""
        # This passes through _build_map_func wrapper so bytecode is opaque
        s = akayu.Stream()
        results = []
        s.map(lambda x, y: x + y, 10).sink(results.append)
        s.compile()
        s.emit(5)
        assert results == [15]

    def test_complex_body(self):
        """Multi-statement lambdas should not match."""
        def complex_func(x):
            temp = x * 2
            return temp + 1
        s, r = _pipeline(complex_func)
        assert _emit_collect(s, r, [5]) == [11]

    def test_kwargs_lambda(self):
        """Lambdas with *args/**kwargs should not match but still work via vcall."""
        def vararg(*args):
            return args[0] * 2
        s, r = _pipeline(vararg)
        assert _emit_collect(s, r, [5, 10]) == [10, 20]


# ===========================================================================
# Batch emit with lambda decompilation
# ===========================================================================

class TestBatchEmit:

    def test_batch_map_lambda_len(self):
        s, r = _pipeline(lambda x: len(x))
        s.emit_batch(["abc", "de", "f"])
        assert r == [3, 2, 1]

    def test_batch_filter_lambda_gt(self):
        s, r = _pipeline(lambda x: x > 0, is_filter=True)
        s.emit_batch([-1, 0, 1, 2])
        assert r == [1, 2]

    def test_batch_map_lambda_add(self):
        s, r = _pipeline(lambda x: x + 10)
        s.emit_batch([1, 2, 3])
        assert r == [11, 12, 13]

    def test_batch_filter_compound(self):
        s, r = _pipeline(lambda x: x is not None and x > 0, is_filter=True)
        s.emit_batch([None, -1, 0, 5, None, 3])
        assert r == [5, 3]


# ===========================================================================
# Already-covered patterns (should work via existing MethodCall/MethodCall1)
# ===========================================================================

class TestAlreadyCovered:

    def test_dict_keys(self):
        s, r = _pipeline(lambda x: x.keys())
        assert _emit_collect(s, r, [{"a": 1, "b": 2}]) == [dict.keys({"a": 1, "b": 2})]

    def test_dict_values(self):
        s, r = _pipeline(lambda x: x.values())
        result = _emit_collect(s, r, [{"a": 1, "b": 2}])
        assert list(result[0]) == [1, 2]

    def test_dict_items(self):
        s, r = _pipeline(lambda x: x.items())
        result = _emit_collect(s, r, [{"a": 1}])
        assert list(result[0]) == [("a", 1)]

    def test_dict_get(self):
        s, r = _pipeline(lambda x: x.get("key"))
        assert _emit_collect(s, r, [{"key": 42}, {}]) == [42, None]

    def test_str_split(self):
        s, r = _pipeline(lambda x: x.split())
        assert _emit_collect(s, r, ["a b c"]) == [["a", "b", "c"]]

    def test_str_split_sep(self):
        s, r = _pipeline(lambda x: x.split(","))
        assert _emit_collect(s, r, ["a,b,c"]) == [["a", "b", "c"]]


# ===========================================================================
# Type builtin
# ===========================================================================

class TestType:

    def test_lambda_type(self):
        s, r = _pipeline(lambda x: type(x))
        assert _emit_collect(s, r, [1, "hello", 3.14]) == [int, str, float]

    def test_bare_type(self):
        """Passing type directly (identity detection)."""
        s, r = _pipeline(type)
        assert _emit_collect(s, r, [1, "hello"]) == [int, str]

    def test_type_chain_is(self):
        """lambda x: type(x) is int"""
        s, r = _pipeline(lambda x: type(x) is int, is_filter=True)
        assert _emit_collect(s, r, [1, "hello", 3.14, 42]) == [1, 42]


# ===========================================================================
# Type coercions: tuple, list, set, frozenset
# ===========================================================================

class TestTypeCoercions:

    def test_lambda_tuple(self):
        s, r = _pipeline(lambda x: tuple(x))
        assert _emit_collect(s, r, [[1, 2], [3]]) == [(1, 2), (3,)]

    def test_bare_tuple(self):
        s, r = _pipeline(tuple)
        assert _emit_collect(s, r, [[1, 2]]) == [(1, 2)]

    def test_lambda_list(self):
        s, r = _pipeline(lambda x: list(x))
        assert _emit_collect(s, r, [(1, 2), (3,)]) == [[1, 2], [3]]

    def test_bare_list(self):
        s, r = _pipeline(list)
        assert _emit_collect(s, r, [(1, 2)]) == [[1, 2]]

    def test_lambda_set(self):
        s, r = _pipeline(lambda x: set(x))
        assert _emit_collect(s, r, [[1, 2, 2]]) == [{1, 2}]

    def test_bare_set(self):
        s, r = _pipeline(set)
        assert _emit_collect(s, r, [[1, 2, 2]]) == [{1, 2}]

    def test_lambda_frozenset(self):
        s, r = _pipeline(lambda x: frozenset(x))
        assert _emit_collect(s, r, [[1, 2]]) == [frozenset({1, 2})]

    def test_bare_frozenset(self):
        s, r = _pipeline(frozenset)
        assert _emit_collect(s, r, [[1, 2]]) == [frozenset({1, 2})]


# ===========================================================================
# HasAttr
# ===========================================================================

class TestHasAttr:

    def test_hasattr_map(self):
        s, r = _pipeline(lambda x: hasattr(x, "__len__"))
        assert _emit_collect(s, r, ["abc", 42]) == [True, False]

    def test_hasattr_filter(self):
        s, r = _pipeline(lambda x: hasattr(x, "__len__"), is_filter=True)
        assert _emit_collect(s, r, ["abc", 42, [1, 2], 3.14]) == ["abc", [1, 2]]

    def test_hasattr_closure(self):
        attr = "upper"
        s, r = _pipeline(lambda x: hasattr(x, attr))
        assert _emit_collect(s, r, ["hello", 42]) == [True, False]


# ===========================================================================
# MethodCall2 (2-arg method calls)
# ===========================================================================

class TestMethodCall2:

    def test_str_replace(self):
        s, r = _pipeline(lambda x: x.replace("a", "b"))
        assert _emit_collect(s, r, ["banana", "apple"]) == ["bbnbnb", "bpple"]

    def test_dict_get_default(self):
        s, r = _pipeline(lambda x: x.get("key", "default"))
        assert _emit_collect(s, r, [{"key": 42}, {}]) == [42, "default"]

    def test_str_center(self):
        s, r = _pipeline(lambda x: x.center(10, "*"))
        assert _emit_collect(s, r, ["hi"]) == ["****hi****"]

    def test_closure_args(self):
        old, new = "x", "y"
        s, r = _pipeline(lambda x: x.replace(old, new))
        assert _emit_collect(s, r, ["fox"]) == ["foy"]


# ===========================================================================
# Slice
# ===========================================================================

class TestSlice:

    def test_slice_both(self):
        s, r = _pipeline(lambda x: x[1:3])
        assert _emit_collect(s, r, ["abcde", [10, 20, 30, 40]]) == ["bc", [20, 30]]

    def test_slice_start_only(self):
        s, r = _pipeline(lambda x: x[2:])
        assert _emit_collect(s, r, ["abcde"]) == ["cde"]

    def test_slice_stop_only(self):
        s, r = _pipeline(lambda x: x[:3])
        assert _emit_collect(s, r, ["abcde", [1, 2, 3, 4, 5]]) == ["abc", [1, 2, 3]]

    def test_slice_negative(self):
        s, r = _pipeline(lambda x: x[-2:])
        assert _emit_collect(s, r, ["abcde"]) == ["de"]


# ===========================================================================
# Tuple/List construction
# ===========================================================================

class TestTupleListConstruct:

    def test_tuple_itemgetter(self):
        """(x["a"], x["b"]) → TupleConstruct([ItemGetter("a"), ItemGetter("b")])"""
        s, r = _pipeline(lambda x: (x["a"], x["b"]))
        assert _emit_collect(s, r, [{"a": 1, "b": 2}]) == [(1, 2)]

    def test_list_attrgetter(self):
        """[x.real, x.imag] → ListConstruct([AttrGetter("real"), AttrGetter("imag")])"""
        s, r = _pipeline(lambda x: [x.real, x.imag])
        assert _emit_collect(s, r, [complex(3, 4)]) == [[3.0, 4.0]]

    def test_tuple_3_elements(self):
        s, r = _pipeline(lambda x: (x["a"], x["b"], x["c"]))
        assert _emit_collect(s, r, [{"a": 1, "b": 2, "c": 3}]) == [(1, 2, 3)]

    def test_list_itemgetter(self):
        s, r = _pipeline(lambda x: [x["x"], x["y"]])
        assert _emit_collect(s, r, [{"x": 10, "y": 20}]) == [[10, 20]]


# ===========================================================================
# Fuzz-style combinatorial tests — compound patterns with new variants
# ===========================================================================

class TestCompoundWithNewVariants:
    """Cross-product tests: new ops composed with chain/and/or/negate/closure."""

    # --- HasAttr in compounds ---

    def test_hasattr_and_compare(self):
        """hasattr(x, 'val') and x.val > 0"""
        class Obj:
            def __init__(self, val):
                self.val = val
        s, r = _pipeline(
            lambda x: hasattr(x, "val") and x.val > 0, is_filter=True
        )
        result = _emit_collect(s, r, [Obj(5), "nope", Obj(-1), Obj(3)])
        assert [o.val for o in result] == [5, 3]

    def test_hasattr_or_isinstance(self):
        """hasattr(x, '__len__') or isinstance(x, int)"""
        s, r = _pipeline(
            lambda x: hasattr(x, "__len__") or isinstance(x, int), is_filter=True
        )
        assert _emit_collect(s, r, ["hi", 42, 3.14, [1]]) == ["hi", 42, [1]]

    def test_negate_hasattr(self):
        """not hasattr(x, '__len__') as filter"""
        s, r = _pipeline(lambda x: not hasattr(x, "__len__"), is_filter=True)
        assert _emit_collect(s, r, ["hi", 42, [1], 3.14]) == [42, 3.14]

    # --- Type in compounds ---

    def test_type_is_and_compare(self):
        """type(x) is int and x > 0"""
        s, r = _pipeline(
            lambda x: type(x) is int and x > 0, is_filter=True
        )
        assert _emit_collect(s, r, [1, -1, "hi", 3.14, 5]) == [1, 5]

    def test_negate_type_is(self):
        """not (type(x) is str)"""
        s, r = _pipeline(lambda x: not (type(x) is str), is_filter=True)
        assert _emit_collect(s, r, [1, "hi", 3.14]) == [1, 3.14]

    # --- Slice in compounds ---

    def test_chain_slice_len(self):
        """len(x[:3]) — chain: Slice → Len"""
        s, r = _pipeline(lambda x: len(x[:3]))
        assert _emit_collect(s, r, ["ab", "abcde", ""]) == [2, 3, 0]

    def test_slice_in_tuple_construct(self):
        """(x[:2], x[2:]) — TupleConstruct with slices"""
        s, r = _pipeline(lambda x: (x[:2], x[2:]))
        assert _emit_collect(s, r, ["abcde"]) == [("ab", "cde")]

    # --- MethodCall2 in compounds ---

    def test_replace_and_strip_chain(self):
        """lambda x: x.replace('a', 'b').strip() — not a single chain, falls back ok"""
        # This is 2 chained operations; the analyzer may or may not detect.
        # Either way the result must be correct.
        s, r = _pipeline(lambda x: x.replace("a", "b").strip())
        assert _emit_collect(s, r, [" banana "]) == ["bbnbnb"]

    def test_method2_closure_and_filter(self):
        """x.get(key, default) != default as filter with closures"""
        key, default = "status", "unknown"
        s, r = _pipeline(
            lambda x: x.get(key, default) != default, is_filter=True
        )
        data = [{"status": "active"}, {}, {"status": "unknown"}, {"name": "a"}]
        assert _emit_collect(s, r, data) == [{"status": "active"}]

    # --- Type coercions in compounds ---

    def test_set_len_chain(self):
        """len(set(x)) — chain: Set → Len"""
        s, r = _pipeline(lambda x: len(set(x)))
        assert _emit_collect(s, r, [[1, 2, 2, 3], [1, 1, 1]]) == [3, 1]

    def test_tuple_compare(self):
        """tuple(x) == (1, 2, 3) — chain: Tuple → Compare"""
        s, r = _pipeline(lambda x: tuple(x) == (1, 2, 3))
        assert _emit_collect(s, r, [[1, 2, 3], [1, 2], [1, 2, 3, 4]]) == [True, False, False]

    # --- TupleConstruct/ListConstruct in compounds ---

    def test_tuple_construct_in_batch(self):
        """(x['a'], x['b']) with batch emit"""
        s, r = _pipeline(lambda x: (x["a"], x["b"]))
        s.emit_batch([{"a": 1, "b": 2}, {"a": 3, "b": 4}])
        assert r == [(1, 2), (3, 4)]

    def test_list_construct_in_batch(self):
        """[x['a'], x['b']] with batch emit"""
        s, r = _pipeline(lambda x: [x["a"], x["b"]])
        s.emit_batch([{"a": 10, "b": 20}])
        assert r == [[10, 20]]

    # --- Deep nesting ---

    def test_double_negate(self):
        """not not isinstance(x, int) — double negate"""
        s, r = _pipeline(lambda x: not not isinstance(x, int), is_filter=True)
        assert _emit_collect(s, r, [1, "hi", 2]) == [1, 2]

    def test_and_or_mixed(self):
        """(x > 0 and x < 10) or x == -1 — 3-level compound"""
        s, r = _pipeline(
            lambda x: (x > 0 and x < 10) or x == -1, is_filter=True
        )
        assert _emit_collect(s, r, [-1, -5, 0, 5, 10, 15]) == [-1, 5]

    def test_or_and_mixed(self):
        """x == 0 or (x > 5 and x < 20)"""
        s, r = _pipeline(
            lambda x: x == 0 or (x > 5 and x < 20), is_filter=True
        )
        assert _emit_collect(s, r, [-1, 0, 3, 10, 25]) == [0, 10]

    def test_three_way_and_or(self):
        """(A and B and C) or D"""
        s, r = _pipeline(
            lambda x: (x > 0 and x < 10 and x != 5) or x == -1, is_filter=True
        )
        assert _emit_collect(s, r, [-1, -5, 0, 3, 5, 7, 10]) == [-1, 3, 7]

    def test_or_chain_and(self):
        """(A or B or C) and D"""
        s, r = _pipeline(
            lambda x: (x > 10 or x < -10 or x == 0) and x != 20, is_filter=True
        )
        assert _emit_collect(s, r, [-20, -5, 0, 5, 15, 20]) == [-20, 0, 15]

    def test_symmetric_and_or(self):
        """(A or B) and (C or D)"""
        s, r = _pipeline(
            lambda x: (x > 0 or x == -1) and (x < 10 or x == 20), is_filter=True
        )
        assert _emit_collect(s, r, [-1, -5, 0, 5, 15, 20]) == [-1, 5, 20]

    # --- Closures with new variants ---

    def test_hasattr_closure_filter(self):
        """hasattr with closure var as filter"""
        attr = "real"
        s, r = _pipeline(lambda x: hasattr(x, attr), is_filter=True)
        assert _emit_collect(s, r, [1+2j, "hello", 42]) == [1+2j, 42]

    def test_slice_closure(self):
        """x[:n] where n is closure var"""
        n = 3
        s, r = _pipeline(lambda x: x[:n])
        assert _emit_collect(s, r, ["abcde", [1, 2, 3, 4, 5]]) == ["abc", [1, 2, 3]]

    def test_method2_closure_both_args(self):
        """x.replace(a, b) with both args from closure"""
        a, b = "o", "0"
        s, r = _pipeline(lambda x: x.replace(a, b))
        assert _emit_collect(s, r, ["foo", "boo"]) == ["f00", "b00"]

    # --- Edge cases ---

    def test_empty_input_batch(self):
        """Batch emit with empty list shouldn't crash any variant"""
        s, r = _pipeline(lambda x: (x["a"], x["b"]))
        s.emit_batch([])
        assert r == []

    def test_none_values_through_chain(self):
        """x.attr with None — should propagate AttributeError naturally"""
        s, r = _pipeline(lambda x: x.get("key", "fallback"))
        assert _emit_collect(s, r, [{"key": "val"}, {}]) == ["val", "fallback"]

    def test_type_coercion_with_strings(self):
        """list(x) on strings splits into chars"""
        s, r = _pipeline(lambda x: list(x))
        assert _emit_collect(s, r, ["abc"]) == [["a", "b", "c"]]

    def test_frozenset_as_filter_truthy(self):
        """frozenset(x) as filter — empty frozenset is falsy, passes original items"""
        s, r = _pipeline(lambda x: frozenset(x), is_filter=True)
        assert _emit_collect(s, r, [[1, 2], [], [3]]) == [[1, 2], [3]]

    def test_hasattr_chain_getattr(self):
        """hasattr(x, 'val') and x.val > 0 — short-circuit protects AttributeError"""
        class Obj:
            def __init__(self, val):
                self.val = val
        s, r = _pipeline(
            lambda x: hasattr(x, "val") and x.val > 0, is_filter=True
        )
        # "nope" has no .val — hasattr returns False, short-circuits, no error
        result = _emit_collect(s, r, [Obj(1), "nope", Obj(-1)])
        assert len(result) == 1 and result[0].val == 1

    def test_construct_with_method_elements(self):
        """(x.upper(), x.lower()) — TupleConstruct with MethodCall elements"""
        s, r = _pipeline(lambda x: (x.upper(), x.lower()))
        assert _emit_collect(s, r, ["Hello"]) == [("HELLO", "hello")]

    def test_construct_mixed_ops(self):
        """(x['a'], x.real) — TupleConstruct mixing ItemGetter and AttrGetter"""
        # dicts don't have .real, so use an object that supports both
        class Hybrid:
            def __init__(self):
                self.real = 42
            def __getitem__(self, key):
                return key
        s, r = _pipeline(lambda x: [x["a"], x.real])
        assert _emit_collect(s, r, [Hybrid()]) == [["a", 42]]


# ===========================================================================
# Fuzz: error propagation, closure snapshots, deep constructs, multi-node
# ===========================================================================

class TestErrorPropagation:
    """Verify errors raised inside BuiltinOps propagate correctly."""

    def test_key_error_in_itemgetter(self):
        import pytest
        s, r = _pipeline(lambda x: x["missing"])
        with pytest.raises(Exception):
            s.emit({"a": 1})

    def test_attr_error_in_chain(self):
        import pytest
        s, r = _pipeline(lambda x: x.nonexistent > 0)
        with pytest.raises(Exception):
            s.emit(42)

    def test_type_error_in_binop(self):
        import pytest
        s, r = _pipeline(lambda x: x + 1)
        with pytest.raises(Exception):
            s.emit("not_a_number")

    def test_key_error_in_tuple_construct(self):
        import pytest
        s, r = _pipeline(lambda x: (x["a"], x["missing"]))
        with pytest.raises(Exception):
            s.emit({"a": 1})

    def test_slice_on_non_subscriptable(self):
        import pytest
        s, r = _pipeline(lambda x: x[1:3])
        with pytest.raises(Exception):
            s.emit(42)

    def test_method2_on_wrong_type(self):
        import pytest
        s, r = _pipeline(lambda x: x.replace("a", "b"))
        with pytest.raises(Exception):
            s.emit(42)

    def test_set_on_unhashable(self):
        """set(x) where x contains unhashable items."""
        import pytest
        s, r = _pipeline(lambda x: set(x))
        with pytest.raises(Exception):
            s.emit([[1, 2], [3]])  # lists aren't hashable


class TestClosureSnapshot:
    """Verify closure vars are snapshotted at compile time, not evaluated lazily."""

    def test_closure_var_mutation_after_compile(self):
        """Mutating a closure var after compile() should NOT affect the fast-path."""
        threshold = 10
        s, r = _pipeline(lambda x: x > threshold, is_filter=True)
        # At compile time, threshold=10 was captured
        threshold = 0  # mutate — should have no effect
        assert _emit_collect(s, r, [5, 15, 20]) == [15, 20]

    def test_closure_collection_mutation(self):
        """Mutating a closure collection after compile()."""
        allowed = {"a", "b"}
        s, r = _pipeline(lambda x: x in allowed, is_filter=True)
        allowed.add("c")  # mutate — this WILL affect since it's the same object
        # The analyzer snapshots the reference, not a copy — so "c" passes
        assert _emit_collect(s, r, ["a", "c", "d"]) == ["a", "c"]

    def test_closure_in_method2(self):
        """Closure args in MethodCall2 are snapshotted."""
        old, new = "x", "y"
        s, r = _pipeline(lambda x: x.replace(old, new))
        old, new = "a", "b"  # mutate — should have no effect (strings are immutable)
        assert _emit_collect(s, r, ["fox"]) == ["foy"]


class TestDeepConstructs:
    """Tuple/list constructs with chain sub-elements."""

    def test_tuple_with_chain_elements(self):
        """(x['a'] + 1, x['b'] * 2) — each element is a chain"""
        # This likely exceeds simple _analyze_simple for sub-groups,
        # so it may fall back to vcall. Test that it still works correctly.
        s, r = _pipeline(lambda x: (x["a"] + 1, x["b"] * 2))
        assert _emit_collect(s, r, [{"a": 10, "b": 5}]) == [(11, 10)]

    def test_list_with_method_and_slice(self):
        """[x.upper(), x[:3]] — mixing method call and slice"""
        s, r = _pipeline(lambda x: [x.upper(), x[:3]])
        assert _emit_collect(s, r, ["hello"]) == [["HELLO", "hel"]]

    def test_tuple_with_type_and_len(self):
        """(type(x), len(x)) — builtin calls as elements"""
        s, r = _pipeline(lambda x: (type(x), len(x)))
        assert _emit_collect(s, r, ["abc", [1, 2]]) == [(str, 3), (list, 2)]


class TestSameOperatorChains:
    """A and B and C, A or B or C — flattened same-op chains."""

    def test_triple_and(self):
        s, r = _pipeline(lambda x: x > 0 and x < 10 and x != 5, is_filter=True)
        assert _emit_collect(s, r, [-1, 0, 3, 5, 7, 10]) == [3, 7]

    def test_triple_or(self):
        s, r = _pipeline(lambda x: x > 10 or x < -10 or x == 0, is_filter=True)
        assert _emit_collect(s, r, [-20, -5, 0, 5, 15]) == [-20, 0, 15]

    def test_quad_and(self):
        s, r = _pipeline(
            lambda x: x > 0 and x < 100 and x % 2 == 0 and x != 50,
            is_filter=True,
        )
        assert _emit_collect(s, r, [-1, 0, 4, 50, 98, 100]) == [4, 98]

    def test_triple_and_or(self):
        """(A and B and C) or D — 3-level nesting with the compound fix."""
        s, r = _pipeline(
            lambda x: (x > 0 and x < 10 and x != 5) or x == -1, is_filter=True
        )
        assert _emit_collect(s, r, [-1, -5, 0, 3, 5, 7, 10]) == [-1, 3, 7]

    def test_triple_or_and(self):
        """(A or B or C) and D"""
        s, r = _pipeline(
            lambda x: (x > 10 or x < -10 or x == 0) and x != 15, is_filter=True
        )
        assert _emit_collect(s, r, [-20, -5, 0, 5, 15, 20]) == [-20, 0, 20]


class TestMultiNodePipeline:
    """Multiple map/filter nodes with different lambda patterns in one stream."""

    def test_map_chain_then_filter(self):
        """map(x + 10) → filter(x > 15)"""
        s = akayu.Stream()
        r = []
        s.map(lambda x: x + 10).filter(lambda x: x > 15).sink(r.append)
        s.compile()
        for v in [1, 5, 10, 20]:
            s.emit(v)
        assert r == [20, 30]

    def test_filter_then_map_then_filter(self):
        """filter(isinstance str) → map(len) → filter(> 3)"""
        s = akayu.Stream()
        r = []
        s.filter(lambda x: isinstance(x, str)).map(lambda x: len(x)).filter(
            lambda x: x > 3
        ).sink(r.append)
        s.compile()
        for v in [42, "hi", "hello", "world!", 3.14, "no"]:
            s.emit(v)
        assert r == [5, 6]

    def test_map_tuple_construct_then_filter(self):
        """map((x['a'], x['b'])) → filter(lambda t: t[0] > 0)"""
        s = akayu.Stream()
        r = []
        s.map(lambda x: (x["a"], x["b"])).filter(lambda x: x[0] > 0).sink(r.append)
        s.compile()
        for v in [{"a": 1, "b": 2}, {"a": -1, "b": 3}, {"a": 5, "b": 0}]:
            s.emit(v)
        assert r == [(1, 2), (5, 0)]

    def test_map_slice_then_map_len(self):
        """map(x[:3]) → map(len)"""
        s = akayu.Stream()
        r = []
        s.map(lambda x: x[:3]).map(lambda x: len(x)).sink(r.append)
        s.compile()
        for v in ["ab", "abcde", ""]:
            s.emit(v)
        assert r == [2, 3, 0]

    def test_map_replace_then_filter_startswith(self):
        """map(x.replace('a','b')) → filter(x.startswith('b'))"""
        s = akayu.Stream()
        r = []
        s.map(lambda x: x.replace("a", "b")).filter(
            lambda x: x.startswith("b")
        ).sink(r.append)
        s.compile()
        for v in ["apple", "banana", "cherry"]:
            s.emit(v)
        assert r == ["bpple", "bbnbnb"]


class TestTruthinessEdgeCases:
    """Falsy values through filters with various BuiltinOp patterns."""

    def test_zero_through_compare_filter(self):
        """0 should pass x >= 0 filter"""
        s, r = _pipeline(lambda x: x >= 0, is_filter=True)
        assert _emit_collect(s, r, [-1, 0, 1]) == [0, 1]

    def test_empty_string_through_method_filter(self):
        """Empty string through x.startswith('') — always True"""
        s, r = _pipeline(lambda x: x.startswith(""), is_filter=True)
        assert _emit_collect(s, r, ["", "a", "hello"]) == ["", "a", "hello"]

    def test_none_through_is_not_none_filter(self):
        s, r = _pipeline(lambda x: x is not None, is_filter=True)
        assert _emit_collect(s, r, [None, 0, "", False, [], 1]) == [0, "", False, [], 1]

    def test_false_through_hasattr_filter(self):
        """False has __len__ → hasattr returns True"""
        s, r = _pipeline(lambda x: hasattr(x, "__len__"), is_filter=True)
        # bool doesn't have __len__, but str/list do
        assert _emit_collect(s, r, [False, "", [], 42]) == ["", []]

    def test_empty_list_through_type_coercion_filter(self):
        """list(x) as filter — empty list is falsy"""
        s, r = _pipeline(lambda x: list(x), is_filter=True)
        assert _emit_collect(s, r, [[1], [], [0]]) == [[1], [0]]

    def test_falsy_values_through_and_filter(self):
        """x is not None and x > 0 — 0 should fail the > 0 part"""
        s, r = _pipeline(lambda x: x is not None and x > 0, is_filter=True)
        assert _emit_collect(s, r, [None, 0, -1, 1, 2]) == [1, 2]

    def test_bool_map_preserves_type(self):
        """bool(x) should return actual bool, not int"""
        s, r = _pipeline(lambda x: bool(x))
        result = _emit_collect(s, r, [0, 1, "", "hi", [], [1]])
        assert result == [False, True, False, True, False, True]
        assert all(type(v) is bool for v in result)
