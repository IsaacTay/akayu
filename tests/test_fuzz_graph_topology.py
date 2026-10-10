import random
import traceback

import pytest

import akayu


def _inc(x):
    return x + 1


def _always_true(x):
    return True


def _echo(acc, x):
    return acc


def _identity_batch(batch):
    return batch


def _check_no_panic(exc):
    msg = str(exc).lower()
    assert "panic" not in msg, f"Possible Rust panic: {exc}"


class TestFuzzCompiledGraphs:
    def test_fuzz_compile_then_emit(self):
        for seed in range(30):
            self._run_compile_emit(seed)

    def _run_compile_emit(self, seed):
        random.seed(seed)
        source = akayu.Stream()
        streams = [source]
        results = []

        num_ops = random.randint(5, 20)
        for _ in range(num_ops):
            upstream = random.choice(streams)
            op = random.choice(
                ["map", "filter", "sink", "accumulate", "flatten", "collect", "batch_map", "starmap", "chunk_by"]
            )
            try:
                if op == "map":
                    s = upstream.map(_inc)
                elif op == "filter":
                    s = upstream.filter(_always_true)
                elif op == "sink":
                    s = upstream.sink(results.append)
                    streams.append(s)
                    continue
                elif op == "accumulate":
                    s = upstream.accumulate(_echo, start=0)
                elif op == "flatten":
                    s = upstream.flatten()
                elif op == "collect":
                    s = upstream.collect()
                elif op == "batch_map":
                    s = upstream.batch_map(_identity_batch)
                elif op == "starmap":
                    s = upstream.starmap(lambda *args: args)
                elif op == "chunk_by":
                    s = upstream.chunk_by(lambda x: x == 0)
                else:
                    continue
                streams.append(s)
            except Exception:
                pass

        try:
            source.compile()
        except Exception:
            return

        for val in range(5):
            try:
                source.emit(val)
            except TypeError:
                pass
            except RuntimeError:
                pass
            except Exception as e:
                _check_no_panic(e)

        for s in streams:
            try:
                s.flush()
            except Exception:
                pass


class TestFuzzDeepChains:
    def test_fuzz_deep_map_chains(self):
        for seed in range(10):
            self._run_deep_chain(seed)

    def _run_deep_chain(self, seed):
        random.seed(seed)
        source = akayu.Stream()
        depth = random.randint(50, 200)
        node = source
        for _ in range(depth):
            op = random.choice(["map", "filter"])
            if op == "map":
                node = node.map(_inc)
            else:
                node = node.filter(_always_true)

        results = []
        node.sink(results.append)

        if random.random() < 0.5:
            source.compile()

        source.emit_batch(list(range(10)))
        assert len(results) == 10


class TestFuzzWideBranching:
    def test_fuzz_wide_branches(self):
        for seed in range(15):
            self._run_wide(seed)

    def _run_wide(self, seed):
        random.seed(seed)
        source = akayu.Stream()
        branch_count = random.randint(10, 50)
        results = {}

        for i in range(branch_count):
            branch = source.map(lambda x, idx=i: (idx, x))
            branch_results = []
            branch.sink(branch_results.append)
            results[i] = branch_results

        if random.random() < 0.5:
            source.compile()

        source.emit_batch(list(range(5)))

        for i, r in results.items():
            assert len(r) == 5, f"Branch {i} got {len(r)} items, expected 5"


class TestFuzzParSeqNesting:
    def test_fuzz_par_seq_nested(self):
        for seed in range(20):
            self._run_nested(seed)

    def _run_nested(self, seed):
        random.seed(seed)
        source = akayu.Stream()
        streams = [source]
        results = []

        num_ops = random.randint(5, 15)
        for _ in range(num_ops):
            upstream = random.choice(streams)
            op = random.choice(["par", "seq", "map", "filter", "sink"])

            try:
                if op == "par":
                    s = upstream.par()
                elif op == "seq":
                    s = upstream.seq()
                elif op == "map":
                    s = upstream.map(_inc)
                elif op == "filter":
                    s = upstream.filter(_always_true)
                elif op == "sink":
                    s = upstream.sink(results.append)
                    streams.append(s)
                    continue
                streams.append(s)
            except Exception:
                pass

        source.emit(42)
        source.emit_batch([1, 2, 3])


class TestFuzzCombinators:
    def test_fuzz_union_zip_combine(self):
        for seed in range(30):
            self._run_combinator(seed)

    def _run_combinator(self, seed):
        random.seed(seed)
        sources = [akayu.Stream() for _ in range(random.randint(2, 5))]
        all_streams = list(sources)
        results = []

        num_ops = random.randint(5, 15)
        for _ in range(num_ops):
            op = random.choice(["union", "zip", "combine_latest", "map", "filter"])
            try:
                if op == "union":
                    args = random.sample(all_streams, k=random.randint(2, len(all_streams)))
                    s = akayu.union(*args)
                elif op == "zip":
                    args = random.sample(all_streams, k=random.randint(2, len(all_streams)))
                    s = akayu.zip(*args)
                elif op == "combine_latest":
                    args = random.sample(all_streams, k=random.randint(2, len(all_streams)))
                    s = akayu.combine_latest(*args)
                elif op == "map":
                    upstream = random.choice(all_streams)
                    s = upstream.map(_inc)
                elif op == "filter":
                    upstream = random.choice(all_streams)
                    s = upstream.filter(_always_true)
                else:
                    continue
                all_streams.append(s)
                if random.random() < 0.3:
                    s.sink(results.append)
            except (ValueError, RuntimeError):
                pass
            except Exception as e:
                _check_no_panic(e)

        emit_counts = [random.randint(0, 5) for _ in sources]
        for src, count in zip(sources, emit_counts):
            for val in range(count):
                try:
                    src.emit(val)
                except Exception as e:
                    _check_no_panic(e)


class TestFuzzSelfReferential:
    def test_fuzz_self_connect(self):
        for seed in range(15):
            self._run_self_ref(seed)

    def _run_self_ref(self, seed):
        random.seed(seed)
        source = akayu.Stream()
        streams = [source]

        num_ops = random.randint(3, 10)
        for _ in range(num_ops):
            upstream = random.choice(streams)
            op = random.choice(["map", "filter", "union", "zip", "combine_latest"])
            try:
                if op in ("map", "filter"):
                    others = random.sample(streams, k=random.randint(1, min(3, len(streams))))
                    targets = [source, upstream] + others
                    for t in targets[: random.randint(1, len(targets))]:
                        if op == "map":
                            s = t.map(_inc)
                        else:
                            s = t.filter(_always_true)
                        streams.append(s)
                elif op == "union":
                    s = upstream.union(upstream)
                    streams.append(s)
                elif op == "zip":
                    s = upstream.zip(upstream)
                    streams.append(s)
                elif op == "combine_latest":
                    s = upstream.combine_latest(upstream)
                    streams.append(s)
            except (ValueError, RuntimeError):
                pass
            except Exception as e:
                _check_no_panic(e)

        try:
            source.emit(1)
        except RuntimeError:
            pass
        try:
            source.emit_batch([2, 3])
        except RuntimeError:
            pass


class TestFuzzCompileFreeze:
    def test_fuzz_modify_after_compile(self):
        for seed in range(20):
            self._run_compile_freeze(seed)

    def _run_compile_freeze(self, seed):
        random.seed(seed)
        source = akayu.Stream()
        source.map(_inc).filter(_always_true).sink(lambda x: None)
        source.compile()

        modify_ops = [
            lambda: source.map(_inc),
            lambda: source.filter(_always_true),
            lambda: source.sink(lambda x: None),
            lambda: source.flatten(),
            lambda: source.collect(),
            lambda: source.accumulate(_echo, start=0),
            lambda: source.batch_map(_identity_batch),
            lambda: source.prefetch(2),
            lambda: source.chunk_by(lambda x: x == 0),
            lambda: source.starmap(lambda *a: a),
            lambda: source.par(),
            lambda: source.seq(),
            lambda: source.union(akayu.Stream()),
            lambda: source.zip(akayu.Stream()),
            lambda: source.combine_latest(akayu.Stream()),
        ]

        for op_fn in random.sample(modify_ops, k=len(modify_ops)):
            with pytest.raises(RuntimeError, match="[Cc]ompiled|[Ff]rozen"):
                op_fn()


class TestFuzzEdgeEmissions:
    def test_fuzz_empty_and_single(self):
        for seed in range(15):
            self._run_edge_emissions(seed)

    def _run_edge_emissions(self, seed):
        random.seed(seed)
        source = akayu.Stream()
        results = []
        source.map(_inc).sink(results.append)

        if random.random() < 0.5:
            source.compile()

        pattern = random.choice(
            [
                "empty",
                "single",
                "batch_empty",
                "batch_single",
                "batch_many",
            ]
        )

        if pattern == "empty":
            pass
        elif pattern == "single":
            source.emit(1)
            assert results == [2]
        elif pattern == "batch_empty":
            source.emit_batch([])
            assert results == []
        elif pattern == "batch_single":
            source.emit_batch([10])
            assert results == [11]
        elif pattern == "batch_many":
            source.emit_batch(list(range(100)))
            assert len(results) == 100


class TestFuzzCollectFlush:
    def test_fuzz_collect_flush_patterns(self):
        for seed in range(15):
            self._run_collect_flush(seed)

    def _run_collect_flush(self, seed):
        random.seed(seed)
        source = akayu.Stream()
        collected = source.collect()
        results = []
        collected.sink(results.append)

        if random.random() < 0.5:
            source.compile()

        n = random.randint(0, 20)
        for i in range(n):
            source.emit(i)

        flush_times = random.randint(0, 3)
        for _ in range(flush_times):
            try:
                collected.flush()
            except Exception:
                pass

        source.emit(99)
        try:
            collected.flush()
        except Exception:
            pass


class TestFuzzMultiLevelPar:
    def test_fuzz_par_par_and_par_zip(self):
        for seed in range(15):
            self._run_multi_par(seed)

    def _run_multi_par(self, seed):
        random.seed(seed)
        source = akayu.Stream()
        results_a = []
        results_b = []
        results_c = []

        p1 = source.par()
        a = p1.map(_inc)
        b = p1.map(lambda x: x * 2)

        p2 = a.par()
        c = p2.map(_inc)
        d = p2.map(lambda x: x + 10)

        merged = akayu.union(b, c, d)
        merged.sink(results_a.append)

        zipped = akayu.zip(a, b)
        zipped.sink(results_b.append)

        cl = a.combine_latest(b)
        cl.sink(results_c.append)

        if random.random() < 0.5:
            source.compile()

        source.emit_batch([1, 2, 3, 4, 5])


class TestFuzzChunkByEdgeCases:
    def test_fuzz_chunk_by_patterns(self):
        for seed in range(15):
            self._run_chunk_by(seed)

    def _run_chunk_by(self, seed):
        random.seed(seed)
        source = akayu.Stream()
        chunks = []
        chunker = source.chunk_by(lambda x: x < 0, include_delimiter=random.choice([True, False]))
        chunker.sink(chunks.append)

        if random.random() < 0.5:
            source.compile()

        data = [random.randint(-5, 5) for _ in range(random.randint(5, 30))]
        source.emit_batch(data)

        try:
            chunker.flush()
        except Exception:
            pass


class TestFuzzPrefetchPar:
    def test_fuzz_prefetch_with_par(self):
        for seed in range(15):
            self._run_prefetch_par(seed)

    def _run_prefetch_par(self, seed):
        random.seed(seed)
        source = akayu.Stream()
        results = []

        p = source.par()
        a = p.map(_inc).prefetch(random.randint(1, 4))
        b = p.map(lambda x: x * 2).prefetch(random.randint(1, 4))

        merged = akayu.union(a, b)
        merged.sink(results.append)

        if random.random() < 0.5:
            source.compile()

        source.emit_batch(list(range(20)))


class TestFuzzEmitOnNonSource:
    def test_fuzz_emit_non_source(self):
        for seed in range(15):
            self._run_emit_non_source(seed)

    def _run_emit_non_source(self, seed):
        random.seed(seed)
        source = akayu.Stream()
        streams = [source]

        for _ in range(random.randint(1, 10)):
            upstream = random.choice(streams)
            op = random.choice(
                [
                    "map",
                    "filter",
                    "flatten",
                    "collect",
                    "par",
                    "seq",
                    "accumulate",
                    "batch_map",
                    "prefetch",
                    "chunk_by",
                    "starmap",
                    "sink",
                ]
            )
            try:
                if op == "map":
                    s = upstream.map(_inc)
                elif op == "filter":
                    s = upstream.filter(_always_true)
                elif op == "flatten":
                    s = upstream.flatten()
                elif op == "collect":
                    s = upstream.collect()
                elif op == "par":
                    s = upstream.par()
                elif op == "seq":
                    s = upstream.seq()
                elif op == "accumulate":
                    s = upstream.accumulate(_echo, start=0)
                elif op == "batch_map":
                    s = upstream.batch_map(_identity_batch)
                elif op == "prefetch":
                    s = upstream.prefetch(2)
                elif op == "chunk_by":
                    s = upstream.chunk_by(lambda x: False)
                elif op == "starmap":
                    s = upstream.starmap(lambda *a: a)
                elif op == "sink":
                    s = upstream.sink(lambda x: None)
                else:
                    continue
                streams.append(s)
            except Exception:
                pass

        non_sources = [s for s in streams if s is not source]
        if not non_sources:
            return

        target = random.choice(non_sources)
        with pytest.raises(RuntimeError, match="[Nn]on-source|[Ee]mit"):
            target.emit(42)

        with pytest.raises(RuntimeError, match="[Nn]on-source|[Ee]mit"):
            target.emit_batch([1, 2, 3])
