import random
import time

import pytest

import akayu
from akayu import Stream, union


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


def _id(x):
    return x


def _const(x):
    return [x]


def _true(x):
    return True


def _last(a, x):
    return x


def test_a_huge_graph_compiles_in_time():
    """20 sources, chains, par() branches, merges of merges and buffering
    nodes, built in 200 random layers: compile() must finish quickly (it
    once took time quadratic in the number of sources) and, in debug
    builds, leave every link consistent (`assert_graph_consistent`)."""
    rng = random.Random(0xDEADBEEF)
    n_sources = 20
    sources = [Stream() for _ in range(n_sources)]

    def rchain(node, depth):
        cur = node
        for _ in range(depth):
            op = rng.choice(["map", "map", "filter", "flatten", "accumulate", "batch_map"])
            if op == "map":
                cur = cur.map(_id)
            elif op == "filter":
                cur = cur.filter(_true)
            elif op == "flatten":
                cur = cur.map(_const).flatten()
            elif op == "accumulate":
                cur = cur.accumulate(_last, start=None)
            elif op == "batch_map":
                cur = cur.batch_map(lambda b: list(b))
        return cur

    layer = [rchain(src, rng.randint(5, 15)) for src in sources]

    for phase in range(200):
        next_layer = []
        for node in layer:
            r = rng.random()
            if r < 0.35:
                next_layer.append(rchain(node, rng.randint(2, 8)))
            if r < 0.55:
                p = node.par()
                branches = [rchain(p, rng.randint(1, 5)) for _ in range(rng.randint(2, 6))]
                next_layer.append(union(*branches))
            if rng.random() < 0.15:
                next_layer.append(node.collect())
            if rng.random() < 0.1:
                next_layer.append(
                    node.chunk_by(lambda x: isinstance(x, int) and x == -1, include_delimiter=True).map(tuple)
                )
            if rng.random() < 0.08:
                next_layer.append(node.accumulate(_last, start=None).map(_id).filter(_true))
            if rng.random() < 0.05:
                next_layer.append(node.batch_map(lambda b: list(b)).flatten())

        while len(next_layer) > 30:
            n = rng.randint(2, min(6, len(next_layer)))
            group = next_layer[:n]
            next_layer = next_layer[n:]
            c = rng.choice(["union", "zip", "combine_latest"])
            if c == "union":
                next_layer.append(union(*group).map(_id))
            elif c == "zip":
                next_layer.append(group[0].zip(*group[1:]).map(_id))
            else:
                next_layer.append(group[0].combine_latest(*group[1:], emit_on=group[0]).map(_id))

        layer = next_layer

    for node in layer:
        node.sink(lambda x: None)

    t0 = time.perf_counter()
    sources[0].compile()
    t1 = time.perf_counter()
    elapsed_ms = (t1 - t0) * 1000
    assert elapsed_ms < 10000, f"compile took {elapsed_ms:.0f}ms — too slow"
