import random
import pytest
import akayu
from akayu import Stream, union


def _id(x):
    return x


def _const(x):
    return [x]


def _true(x):
    return True


def _last(a, x):
    return x


class TestCompileFuzz:
    def test_random_chain_compiles(self):
        ops = ["map", "filter", "flatten", "accumulate", "map", "filter"]
        for seed in range(50):
            rng = random.Random(seed)
            s = Stream()
            cur = s
            for _ in range(rng.randint(1, 10)):
                op = rng.choice(ops)
                if op == "map":
                    cur = cur.map(_id)
                elif op == "filter":
                    cur = cur.filter(_true)
                elif op == "flatten":
                    cur = cur.map(_const).flatten()
                elif op == "accumulate":
                    cur = cur.accumulate(_last, start=None)
            cur.sink(lambda x: None)
            s.compile()
            s.emit_batch([1, 2, 3])
            s.flush()

    def test_random_diamond_compiles(self):
        for seed in range(30):
            rng = random.Random(seed)
            s = Stream()
            branches = [s]
            n_branches = rng.randint(2, 5)
            for _ in range(n_branches):
                b = s
                for _ in range(rng.randint(1, 4)):
                    b = b.map(_id)
                branches.append(b)
            combined = union(*branches)
            combined.sink(lambda x: None)
            s.compile()
            s.emit_batch([1, 2, 3])

    def test_random_par_compiles(self):
        for seed in range(30):
            rng = random.Random(seed)
            s = Stream()
            p = s.par()
            for _ in range(rng.randint(1, 5)):
                branch = p
                for _ in range(rng.randint(1, 3)):
                    branch = branch.map(_id)
                branch.sink(lambda x: None)
            s.compile()
            s.emit_batch([1, 2, 3])

    def test_random_combine_latest_compiles(self):
        for seed in range(20):
            rng = random.Random(seed)
            n_sources = rng.randint(2, 4)
            sources = [Stream() for _ in range(n_sources)]
            streams = []
            for src in sources:
                cur = src
                for _ in range(rng.randint(0, 3)):
                    cur = cur.map(_id)
                streams.append(cur)
            emit_targets = rng.sample(streams, k=min(rng.randint(1, n_sources), n_sources))
            cl = streams[0].combine_latest(*streams[1:], emit_on=emit_targets[0])
            cl.sink(lambda x: None)
            sources[0].compile()
            for src in sources:
                src.emit(1)
            sources[0].flush()

    def test_random_zip_compiles(self):
        for seed in range(20):
            rng = random.Random(seed)
            n = rng.randint(2, 4)
            sources = [Stream() for _ in range(n)]
            mapped = [src.map(_id) for src in sources]
            z = mapped[0].zip(*mapped[1:])
            z.sink(lambda x: None)
            sources[0].compile()
            for src in sources:
                src.emit(1)
                src.emit(2)

    def test_random_union_compiles(self):
        for seed in range(20):
            rng = random.Random(seed)
            n = rng.randint(2, 5)
            sources = [Stream() for _ in range(n)]
            mapped = [src.map(_id) for src in sources]
            u = union(*mapped)
            u.filter(_true).map(_id).sink(lambda x: None)
            sources[0].compile()
            for src in sources:
                src.emit_batch([1, 2])

    def test_random_collect_flush_compiles(self):
        for seed in range(20):
            rng = random.Random(seed)
            s = Stream()
            c = s.map(_id).collect()
            c.sink(lambda x: None)
            s.compile()
            s.emit_batch([1, 2, 3])
            c.flush()

    def test_random_chunk_by_compiles(self):
        for seed in range(20):
            rng = random.Random(seed)
            s = Stream()
            cb = s.chunk_by(lambda x: x == 0, include_delimiter=rng.choice([True, False]))
            cb.map(tuple).sink(lambda x: None)
            s.compile()
            s.emit_batch([1, 2, 0, 3, 4, 0])
            s.flush()

    def test_random_mixed_dag_compiles(self):
        for seed in range(30):
            rng = random.Random(seed)
            n_sources = rng.randint(1, 3)
            sources = [Stream() for _ in range(n_sources)]
            leaves = []

            for src in sources:
                cur = src
                depth = rng.randint(1, 5)
                for _ in range(depth):
                    op = rng.choice(["map", "filter", "flatten", "accumulate", "collect", "chunk_by"])
                    if op == "map":
                        cur = cur.map(_id)
                    elif op == "filter":
                        cur = cur.filter(_true)
                    elif op == "flatten":
                        cur = cur.map(_const).flatten()
                    elif op == "accumulate":
                        cur = cur.accumulate(_last, start=None)
                    elif op == "collect":
                        cur = cur.collect()
                    elif op == "chunk_by":
                        cur = cur.chunk_by(lambda x: isinstance(x, int) and x == 0, include_delimiter=True)
                leaves.append(cur)

            combiner = rng.choice(["union", "zip", "combine_latest", "none"])
            if combiner == "union" and len(leaves) >= 2:
                u = union(*leaves)
                u.sink(lambda x: None)
            elif combiner == "zip" and len(leaves) >= 2:
                z = leaves[0].zip(*leaves[1:])
                z.sink(lambda x: None)
            elif combiner == "combine_latest" and len(leaves) >= 2:
                cl = leaves[0].combine_latest(*leaves[1:], emit_on=leaves[0])
                cl.sink(lambda x: None)
            else:
                for leaf in leaves:
                    leaf.sink(lambda x: None)

            sources[0].compile()
            for src in sources:
                src.emit_batch([1, 2, 3])
                src.flush()

    def test_random_par_with_combiners_compiles(self):
        for seed in range(20):
            rng = random.Random(seed)
            s = Stream()
            p = s.par()
            branches = []
            for _ in range(rng.randint(2, 4)):
                b = p
                for _ in range(rng.randint(1, 3)):
                    b = b.map(_id)
                branches.append(b)
            if len(branches) >= 2:
                combiner = rng.choice(["union", "zip", "combine_latest"])
                if combiner == "union":
                    combined = union(*branches)
                elif combiner == "zip":
                    combined = branches[0].zip(*branches[1:])
                else:
                    combined = branches[0].combine_latest(*branches[1:], emit_on=branches[0])
                combined.sink(lambda x: None)
            else:
                branches[0].sink(lambda x: None)
            s.compile()
            s.emit_batch([1, 2, 3])

    def test_deep_wide_dag_compiles(self):
        rng = random.Random(42)
        n_sources = 4
        sources = [Stream() for _ in range(n_sources)]
        layer = [src.map(_id) for src in sources]

        for layer_idx in range(5):
            next_layer = []
            for node in layer:
                if rng.random() < 0.5:
                    next_layer.append(node.map(_id))
                if rng.random() < 0.3:
                    next_layer.append(node.filter(_true))
            if len(next_layer) >= 2 and rng.random() < 0.4:
                u = union(*next_layer[:2])
                next_layer = next_layer[2:] + [u.map(_id)]
            layer = next_layer if next_layer else layer

        for node in layer:
            node.sink(lambda x: None)

        sources[0].compile()
        for src in sources:
            src.emit_batch(list(range(5)))

    def test_mega_dag_compiles(self):
        rng = random.Random(12345)
        n_sources = 8
        sources = [Stream() for _ in range(n_sources)]

        def random_chain(node, depth):
            cur = node
            for _ in range(depth):
                op = rng.choice(["map", "map", "filter", "flatten", "accumulate"])
                if op == "map":
                    cur = cur.map(_id)
                elif op == "filter":
                    cur = cur.filter(_true)
                elif op == "flatten":
                    cur = cur.map(_const).flatten()
                elif op == "accumulate":
                    cur = cur.accumulate(_last, start=None)
            return cur

        layer = [random_chain(src, rng.randint(2, 5)) for src in sources]

        for phase in range(4):
            next_layer = []

            for node in layer:
                if rng.random() < 0.6:
                    next_layer.append(random_chain(node, rng.randint(1, 3)))
                if rng.random() < 0.4:
                    p = node.par()
                    n_par = rng.randint(2, 4)
                    par_branches = [random_chain(p, rng.randint(1, 3)) for _ in range(n_par)]
                    if len(par_branches) >= 2:
                        next_layer.append(union(*par_branches))
                    else:
                        next_layer.append(par_branches[0])
                if rng.random() < 0.2:
                    next_layer.append(node.collect())

            merged = []
            while len(next_layer) >= 2:
                n = rng.choice([2, 2, 3])
                group = next_layer[:n]
                next_layer = next_layer[n:]
                combiner = rng.choice(["union", "zip", "combine_latest"])
                if combiner == "union":
                    merged.append(union(*group).map(_id))
                elif combiner == "zip":
                    merged.append(group[0].zip(*group[1:]).map(_id))
                else:
                    merged.append(group[0].combine_latest(*group[1:], emit_on=group[0]).map(_id))
            merged.extend(next_layer)

            for node in merged:
                if rng.random() < 0.3:
                    cb = node.chunk_by(lambda x: isinstance(x, int) and x == -1, include_delimiter=True)
                    i = merged.index(node)
                    merged[i] = cb.map(tuple)

            layer = merged

        for node in layer:
            node.sink(lambda x: None)

        sources[0].compile()

        for src in sources:
            src.emit_batch(list(range(10)) + [-1] + list(range(5)))
            src.flush()

    def test_mega_dag_100_seeds(self):
        for seed in range(100):
            rng = random.Random(seed)
            n_sources = rng.randint(2, 6)
            sources = [Stream() for _ in range(n_sources)]

            def rchain(node, depth):
                cur = node
                for _ in range(depth):
                    op = rng.choice(["map", "filter", "flatten", "accumulate", "collect", "chunk_by"])
                    if op == "map":
                        cur = cur.map(_id)
                    elif op == "filter":
                        cur = cur.filter(_true)
                    elif op == "flatten":
                        cur = cur.map(_const).flatten()
                    elif op == "accumulate":
                        cur = cur.accumulate(_last, start=None)
                    elif op == "collect":
                        cur = cur.collect()
                    elif op == "chunk_by":
                        cur = cur.chunk_by(lambda x: isinstance(x, int) and x == -1, include_delimiter=True)
                return cur

            layer = [rchain(src, rng.randint(1, 4)) for src in sources]

            n_phases = rng.randint(2, 5)
            for phase in range(n_phases):
                next_layer = []
                for node in layer:
                    if rng.random() < 0.7:
                        next_layer.append(rchain(node, rng.randint(0, 3)))
                    if rng.random() < 0.3:
                        p = node.par()
                        branches = [rchain(p, rng.randint(1, 3)) for _ in range(rng.randint(2, 3))]
                        next_layer.append(union(*branches))
                    if rng.random() < 0.15:
                        next_layer.append(node.collect())

                merged = []
                while len(next_layer) >= 2:
                    n = min(rng.randint(2, 3), len(next_layer))
                    group = next_layer[:n]
                    next_layer = next_layer[n:]
                    c = rng.choice(["union", "zip", "combine_latest"])
                    if c == "union":
                        merged.append(union(*group).map(_id))
                    elif c == "zip":
                        merged.append(group[0].zip(*group[1:]).map(_id))
                    else:
                        merged.append(group[0].combine_latest(*group[1:], emit_on=group[0]).map(_id))
                merged.extend(next_layer)
                layer = merged

            for node in layer:
                node.sink(lambda x: None)

            sources[0].compile()
            for src in sources:
                src.emit_batch([1, 2, 3])
                src.flush()

    def test_monster_dag_compiles(self):
        rng = random.Random(99999)
        n_sources = 10
        sources = [Stream() for _ in range(n_sources)]

        def rchain(node, depth):
            cur = node
            for _ in range(depth):
                op = rng.choice(["map", "map", "filter", "flatten", "accumulate"])
                if op == "map":
                    cur = cur.map(_id)
                elif op == "filter":
                    cur = cur.filter(_true)
                elif op == "flatten":
                    cur = cur.map(_const).flatten()
                elif op == "accumulate":
                    cur = cur.accumulate(_last, start=None)
            return cur

        layer = [rchain(src, rng.randint(3, 8)) for src in sources]

        for phase in range(100):
            next_layer = []
            for node in layer:
                if rng.random() < 0.5:
                    next_layer.append(rchain(node, rng.randint(1, 4)))
                if rng.random() < 0.3:
                    p = node.par()
                    branches = [rchain(p, rng.randint(1, 3)) for _ in range(rng.randint(2, 5))]
                    next_layer.append(union(*branches))
                if rng.random() < 0.1:
                    next_layer.append(node.collect())
                if rng.random() < 0.1:
                    next_layer.append(
                        node.chunk_by(lambda x: isinstance(x, int) and x == -1, include_delimiter=True).map(tuple)
                    )

            while len(next_layer) > 20:
                n = rng.randint(2, min(5, len(next_layer)))
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

        sources[0].compile()

        for src in sources:
            src.emit_batch(list(range(20)) + [-1])
            src.flush()

    def test_kaiju_dag_compiles(self):
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

        import time

        t0 = time.perf_counter()
        sources[0].compile()
        t1 = time.perf_counter()
        elapsed_ms = (t1 - t0) * 1000
        assert elapsed_ms < 10000, f"compile took {elapsed_ms:.0f}ms — too slow"
