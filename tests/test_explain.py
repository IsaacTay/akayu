"""explain(): how each node of a graph runs, one line per node.

It answers what compile() did, which nothing showed (repr said
`<Stream(nodes=1, compiled=true)>`): which nodes it merged into chains,
which functions run natively, which run on machine numbers. The profile
report's tree carries the same tags.
"""

import contextlib
import io

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
    assert nodes["map(parse)"].rstrip().endswith("Python")
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
