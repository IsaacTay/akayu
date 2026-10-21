"""explain(): how each node of a graph runs, one line per node.

It answers what compile() did, which nothing showed (repr said
`<Stream(nodes=1, compiled=true)>`): which nodes it merged into chains,
which functions run natively, which run on machine numbers. The profile
report's tree carries the same tags.
"""

import contextlib
import io
import re

import pytest

from akayu import Stream

PARSED = []


def parse(line):
    PARSED.append(line)  # a side effect: no native operation can stand in for it
    return int(line)


def lookup(x):
    return x


def build():
    """A pipeline with a Python function, native lambdas, a run of numeric
    steps, a par() and a prefetch()."""
    s = Stream(name="events")
    numbers = s.map(parse).filter(lambda x: x >= 0)
    branches = numbers.par()
    branches.map(lambda x: x + 1).map(lambda x: x * 2).sink(print)
    branches.prefetch(2).map(lookup).sink(print)
    return s


def lines_by_node(text):
    """{node name: its line}, for the lines below the header."""
    return {line.lstrip("│├└─ ").split()[0]: line for line in text.splitlines()[2:] if line.strip()}


def test_an_uncompiled_graph():
    s = build()
    text = s.explain()
    header = text.splitlines()[0]
    assert "'events'" in header and "not compiled" in header
    nodes = lines_by_node(text)
    assert nodes["map(parse)"].rstrip().endswith("Python: 2 statements")
    # Analyzed lambdas run natively even before compile().
    assert nodes["filter(<lambda>)"].rstrip().endswith("native")
    assert "chain" not in text


def test_a_compiled_graph():
    s = build()
    s.compile()
    text = s.explain()
    assert "compiled" in text.splitlines()[0] and "not compiled" not in text
    nodes = lines_by_node(text)
    assert "Python" in nodes["map(parse)"] and "chain 1" in nodes["map(parse)"]
    assert "native" in nodes["filter(<lambda>)"] and "chain 1" in nodes["filter(<lambda>)"]
    assert "threads" in nodes["par"]
    # Two arithmetic maps in a row run on machine numbers.
    unboxed = [line for line in text.splitlines() if "map(<lambda>)" in line]
    assert len(unboxed) == 2 and all("native, unboxed" in line for line in unboxed)
    assert "Python, threads" in next(line for line in text.splitlines() if "prefetch" in line)


def test_where_each_node_was_built():
    s = Stream()
    s.map(parse).sink(print)
    line = s.explain().splitlines()[3]
    assert "test_explain.py:" in line


def test_one_line_per_node_however_long_the_chain():
    """Not a verbose mess: a header, then a line per node."""
    s = Stream()
    node = s
    for _ in range(60):
        node = node.map(lambda x: x + 1)
    node.sink(print)
    s.compile()
    text = s.explain()
    assert len([line for line in text.splitlines() if line.strip()]) == 1 + 62
    assert max(len(line) for line in text.splitlines()) < 140


def test_columns_line_up():
    s = build()
    s.compile()
    rows = [line for line in s.explain().splitlines()[2:] if "test_explain.py" in line]
    assert len({line.index("test_explain.py") for line in rows}) == 1


def test_the_profile_tree_carries_the_tags():
    s = build()
    s.compile()
    with s.profile() as p:
        for x in ["1", "2", "-3"]:
            s.emit(x)
    text = str(p.report)
    assert "native, unboxed" in text and "chain 1" in text
    # Unboxed steps run one at a time while profiling: said once, at the end.
    assert text.count("unboxed steps") == 1
    nodes = {n["name"]: n for n in p.report.to_dict()["nodes"]}
    assert (nodes["map(parse)"]["how"], nodes["map(parse)"]["chain"]) == ("Python", 1)
    assert nodes["events"]["chain"] is None


def test_no_caveat_without_unboxed_steps():
    s = Stream()
    s.map(parse).sink(print)
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        report = s.emit_profiled("1")
    assert "unboxed" not in str(report)


# Why a node is Python. These functions are defined here, at module level,
# so their source can be read.

TABLE = {"a": 1}


def two_statements(x):
    y = x + 1
    return y * 2


def guarded(x):
    try:
        return int(x)
    except ValueError:
        return None


def doubled(x):
    return x * 2


def loads(text, **options):
    return text


class Point:
    def __init__(self, x):
        self.x = x


def scaled_and_shown(k):
    return lambda x: (x * k, f"{x}!")


WHY = {
    "calls a function": (lambda x: lookup(x) + 1, "calls lookup()"),
    "calls a method of a global": (lambda x: TABLE.get(x), "calls TABLE.get()"),
    "the part shown": (lambda x: x["a"] + x["b"], "x['a'] + x['b']"),
    "an f-string": (lambda x: f"{x}!", "an f-string"),
    # For its if/else, whatever its parts are.
    "if/else": (lambda x: x if x > 0 else -x, "if/else"),
    "statements": (two_statements, "2 statements"),
    "try": (guarded, "try/except"),
    "parameters": (loads, "takes **kwargs"),
    # k is the closure's: x * k has a native form, the f-string doesn't.
    "a closure": (scaled_and_shown(3), "an f-string"),
    "a default for the item": (lambda x=0: f"{x}", "an f-string"),
    "too long": (lambda x: x.a.b.c.d.e.f, "too long"),
}


def how(func, compile=True):
    """How explain() says map(func) runs: its tag, without the chain."""
    s = Stream()
    s.map(func).sink(print)
    if compile:
        s.compile()
    line = s.explain().splitlines()[3]
    return re.fullmatch(r".*?test_explain\.py:\d+  (.*?)(?:  +chain \d+)?", line)[1]


@pytest.mark.parametrize("case", WHY)
def test_why_a_function_stays_python(case):
    func, why = WHY[case]
    assert how(func) == f"Python: {why}"


def test_a_lambda_compile_replaced_with_the_function_it_calls():
    # compile() calls lookup itself, without the lambda.
    assert how(lambda x: lookup(x)) == "Python: calls lookup()"
    assert how(lambda x: Point(x)) == "Python: calls Point()"


def test_until_compile():
    # A def runs natively only once compiled (its body would no longer run,
    # which coverage and breakpoints would notice).
    assert how(doubled, compile=False) == "Python until compile()"
    assert how(doubled) == "native"


def test_nothing_to_add():
    # A class, a C function, a lambda whose source can't be read.
    for func in (Point, abs, eval("lambda x: x * x")):
        assert how(func) == "Python"


def test_long_parts_are_shortened():
    # Short in bytecode (on 3.14 too, whose method calls are longer), long as text.
    why = how(lambda x: x["a_long_key"] * x["another_long_key"]).removeprefix("Python: ")
    assert len(why) == 28 and why.endswith("…")


def test_only_functions_explain_can_reason_about():
    s = Stream()
    s.map(lambda x: x + 1).batch_map(lambda xs: xs).sink(lambda x: print(x))
    s.compile()
    text = s.explain()
    assert "Python:" not in text and "until compile()" not in text


def test_the_chain_column_lines_up_after_a_reason():
    s = Stream()
    s.map(lookup).map(lambda x: x + 1).map(two_statements).sink(print)
    s.compile()
    rows = [line for line in s.explain().splitlines() if "  chain " in line]
    assert len(rows) == 4 and len({line.index("chain") for line in rows}) == 1
