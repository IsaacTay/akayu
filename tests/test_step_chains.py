"""compile() runs chains of single steps natively: `x % 2 == 0`,
`x["a"]["b"]`, `str(x).upper()`, ... Compiled results (native) must match
eager ones (the lambda itself) on every input, errors included."""

from types import SimpleNamespace

import pytest

import akayu

FUNCS = {
    "mod-eq": (lambda x: x % 2 == 0, [0, 1, 2.0, -3, True]),
    "mul-add": (lambda x: x * 2 + 1, [1, 2.5, -4]),
    "item-item": (lambda x: x["a"]["b"], [{"a": {"b": 1}}, {"a": {"b": None}}]),
    "attr-attr": (lambda x: x.a.b, [SimpleNamespace(a=SimpleNamespace(b=7))]),
    "str-method": (lambda x: str(x).upper(), ["ab", 12, None]),
    "method-method": (lambda x: x.strip().lower(), ["  Hi ", "X"]),
    "get-compare": (lambda x: x["a"].get("b", 0) > 1, [{"a": {"b": 2}}, {"a": {}}]),
    "mod-eq-and": (lambda x: x % 2 == 0 and x > 3, [2, 4, 5, 6]),
}
ERRORS = {
    "item-item": [{"a": {}}, {"x": 1}],
    "attr-attr": [SimpleNamespace(a=1)],
    "mul-add": ["s"],
    "method-method": [3],
}


def run(func, items, compiled, op):
    s, out = akayu.Stream(), []
    getattr(s, op)(func).sink(out.append)
    if compiled:
        s.compile()
    for x in items:
        s.emit(x)
    return out


@pytest.mark.parametrize("op", ["map", "filter"])
@pytest.mark.parametrize("name", FUNCS)
def test_compiled_matches_eager(name, op):
    func, items = FUNCS[name]
    assert run(func, items, True, op) == run(func, items, False, op)


@pytest.mark.parametrize("name", ERRORS)
def test_errors_match(name):
    func, _ = FUNCS[name]
    for item in ERRORS[name]:
        causes = []
        for compiled in (False, True):
            with pytest.raises(RuntimeError) as info:
                run(func, [item], compiled, "map")
            causes.append(type(info.value.__cause__))
        assert causes[0] is causes[1]
