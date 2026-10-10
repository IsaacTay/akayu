"""Complex topology benchmarks: akayu vs streamz vs pure Python.

Topologies from docs/benchmarks.md:
- Deep pipeline (10 chained maps)
- Diamond (split -> 2 branches -> union)
- Fan-out (5 parallel branches)
- Complex DAG (split, flatten, union)
- Multi-union (4 sources)
- Combine latest (3 sources)
- Zip (3 sources)
- Accumulate (3 branches)
"""

import time

N = 500_000


def bench(name, fn, rounds=5):
    """Run fn `rounds` times, print mean ms and items/sec."""
    # warmup
    fn()
    times = []
    for _ in range(rounds):
        start = time.perf_counter()
        fn()
        elapsed = time.perf_counter() - start
        times.append(elapsed)
    mean_s = sum(times) / len(times)
    mean_ms = mean_s * 1000
    ips = N / mean_s
    return mean_ms, ips


def fmt(ms, ips):
    if ips >= 1_000_000:
        return f"{ms:>9.1f} ms  {ips / 1_000_000:.2f}M items/s"
    else:
        return f"{ms:>9.1f} ms  {ips / 1_000:.0f}k items/s"


# ============================================================
# 1. Deep Pipeline (10 chained maps)
# ============================================================


def deep_akayu():
    import akayu

    s = akayu.Stream()
    count = 0

    def inc(x):
        nonlocal count
        count += 1

    node = s
    for _ in range(10):
        node = node.map(lambda x: x + 1)
    node.sink(inc)
    s.compile()
    for i in range(N):
        s.emit(i)
    assert count == N


def deep_akayu_batch():
    import akayu

    s = akayu.Stream()
    count = 0

    def inc(x):
        nonlocal count
        count += 1

    node = s
    for _ in range(10):
        node = node.map(lambda x: x + 1)
    node.sink(inc)
    s.compile()
    s.emit_batch(list(range(N)))
    assert count == N


def deep_streamz():
    import streamz

    s = streamz.Stream()
    count = 0

    def inc(x):
        nonlocal count
        count += 1

    node = s
    for _ in range(10):
        node = node.map(lambda x: x + 1)
    node.sink(inc)
    for i in range(N):
        s.emit(i)
    assert count == N


def deep_python():
    count = 0
    for i in range(N):
        x = i
        for _ in range(10):
            x = x + 1
        count += 1
    assert count == N


# ============================================================
# 2. Diamond (split -> 2 branches -> union)
# Source -> branch1: map(*2), branch2: map(*3) -> union -> sink
# ============================================================


def diamond_akayu():
    import akayu

    s = akayu.Stream()
    count = 0

    def inc(x):
        nonlocal count
        count += 1

    b1 = s.map(lambda x: x * 2)
    b2 = s.map(lambda x: x * 3)
    b1.union(b2).sink(inc)
    s.compile()
    for i in range(N):
        s.emit(i)
    assert count == N * 2


def diamond_akayu_batch():
    import akayu

    s = akayu.Stream()
    count = 0

    def inc(x):
        nonlocal count
        count += 1

    b1 = s.map(lambda x: x * 2)
    b2 = s.map(lambda x: x * 3)
    b1.union(b2).sink(inc)
    s.compile()
    s.emit_batch(list(range(N)))
    assert count == N * 2


def diamond_streamz():
    import streamz

    s = streamz.Stream()
    count = 0

    def inc(x):
        nonlocal count
        count += 1

    b1 = s.map(lambda x: x * 2)
    b2 = s.map(lambda x: x * 3)
    b1.union(b2).sink(inc)
    for i in range(N):
        s.emit(i)
    assert count == N * 2


def diamond_python():
    count = 0
    for i in range(N):
        _ = i * 2
        count += 1
        _ = i * 3
        count += 1
    assert count == N * 2


# ============================================================
# 3. Fan-out (5 parallel map -> filter -> sink branches)
# ============================================================


def fanout_akayu():
    import akayu

    s = akayu.Stream()
    count = 0

    def inc(x):
        nonlocal count
        count += 1

    for k in range(5):
        s.map(lambda x, k=k: x + k).filter(lambda x: x % 2 == 0).sink(inc)
    s.compile()
    for i in range(N):
        s.emit(i)


def fanout_akayu_batch():
    import akayu

    s = akayu.Stream()
    count = 0

    def inc(x):
        nonlocal count
        count += 1

    for k in range(5):
        s.map(lambda x, k=k: x + k).filter(lambda x: x % 2 == 0).sink(inc)
    s.compile()
    s.emit_batch(list(range(N)))


def fanout_streamz():
    import streamz

    s = streamz.Stream()
    count = 0

    def inc(x):
        nonlocal count
        count += 1

    for k in range(5):
        s.map(lambda x, k=k: x + k).filter(lambda x: x % 2 == 0).sink(inc)
    for i in range(N):
        s.emit(i)


def fanout_python():
    count = 0
    for i in range(N):
        for k in range(5):
            x = i + k
            if x % 2 == 0:
                count += 1


# ============================================================
# 4. Complex DAG (split, flatten, union)
# Source -> map -> split:
#   branch1: map(*2) -> filter(%4) -> union
#   branch2: map(list) -> flatten -> filter(%3) -> union
# union -> map -> sink
# ============================================================


def complex_dag_akayu():
    import akayu

    s = akayu.Stream()
    count = 0

    def inc(x):
        nonlocal count
        count += 1

    m = s.map(lambda x: x + 1)
    b1 = m.map(lambda x: x * 2).filter(lambda x: x % 4 == 0)
    b2 = m.map(lambda x: [x]).flatten().filter(lambda x: x % 3 == 0)
    b1.union(b2).map(lambda x: x + 1).sink(inc)
    s.compile()
    for i in range(N):
        s.emit(i)


def complex_dag_akayu_batch():
    import akayu

    s = akayu.Stream()
    count = 0

    def inc(x):
        nonlocal count
        count += 1

    m = s.map(lambda x: x + 1)
    b1 = m.map(lambda x: x * 2).filter(lambda x: x % 4 == 0)
    b2 = m.map(lambda x: [x]).flatten().filter(lambda x: x % 3 == 0)
    b1.union(b2).map(lambda x: x + 1).sink(inc)
    s.compile()
    s.emit_batch(list(range(N)))


def complex_dag_streamz():
    import streamz

    s = streamz.Stream()
    count = 0

    def inc(x):
        nonlocal count
        count += 1

    m = s.map(lambda x: x + 1)
    b1 = m.map(lambda x: x * 2).filter(lambda x: x % 4 == 0)
    b2 = m.map(lambda x: [x]).flatten().filter(lambda x: x % 3 == 0)
    b1.union(b2).map(lambda x: x + 1).sink(inc)
    for i in range(N):
        s.emit(i)


def complex_dag_python():
    count = 0
    for i in range(N):
        x = i + 1
        # branch 1
        b1 = x * 2
        if b1 % 4 == 0:
            _ = b1 + 1
            count += 1
        # branch 2
        if x % 3 == 0:
            _ = x + 1
            count += 1


# ============================================================
# 5. Multi-union (4 sources merged)
# ============================================================


def multi_union_akayu():
    import akayu

    s1 = akayu.Stream()
    s2 = akayu.Stream()
    s3 = akayu.Stream()
    s4 = akayu.Stream()
    count = 0

    def inc(x):
        nonlocal count
        count += 1

    s1.union(s2).union(s3).union(s4).map(lambda x: x + 1).sink(inc)
    s1.compile()
    per = N // 4
    for i in range(per):
        s1.emit(i)
        s2.emit(i)
        s3.emit(i)
        s4.emit(i)
    assert count == N


def multi_union_streamz():
    import streamz

    s1 = streamz.Stream()
    s2 = streamz.Stream()
    s3 = streamz.Stream()
    s4 = streamz.Stream()
    count = 0

    def inc(x):
        nonlocal count
        count += 1

    s1.union(s2).union(s3).union(s4).map(lambda x: x + 1).sink(inc)
    per = N // 4
    for i in range(per):
        s1.emit(i)
        s2.emit(i)
        s3.emit(i)
        s4.emit(i)
    assert count == N


def multi_union_python():
    count = 0
    per = N // 4
    for i in range(per):
        for _ in range(4):
            _ = i + 1
            count += 1
    assert count == N


# ============================================================
# 6. Accumulate (3 branches: sum, count, max)
# ============================================================


def accumulate_akayu():
    import akayu

    s = akayu.Stream()
    count = 0

    def inc(x):
        nonlocal count
        count += 1

    s.accumulate(lambda a, b: a + b, start=0).sink(inc)
    s.accumulate(lambda a, b: a + 1, start=0).sink(inc)
    s.accumulate(lambda a, b: max(a, b), start=0).sink(inc)
    s.compile()
    for i in range(N):
        s.emit(i)
    assert count == N * 3


def accumulate_streamz():
    import streamz

    s = streamz.Stream()
    count = 0

    def inc(x):
        nonlocal count
        count += 1

    s.accumulate(lambda a, b: a + b, start=0).sink(inc)
    s.accumulate(lambda a, b: a + 1, start=0).sink(inc)
    s.accumulate(lambda a, b: max(a, b), start=0).sink(inc)
    for i in range(N):
        s.emit(i)
    assert count == N * 3


def accumulate_python():
    count = 0
    acc_sum = 0
    acc_count = 0
    acc_max = 0
    for i in range(N):
        acc_sum += i
        count += 1
        acc_count += 1
        count += 1
        acc_max = max(acc_max, i)
        count += 1
    assert count == N * 3


# ============================================================
# 7. Zip (3 sources)
# ============================================================


def zip_akayu():
    import akayu

    s1 = akayu.Stream()
    s2 = akayu.Stream()
    s3 = akayu.Stream()
    count = 0

    def inc(x):
        nonlocal count
        count += 1

    s1.zip(s2, s3).sink(inc)
    s1.compile()
    for i in range(N):
        s1.emit(i)
        s2.emit(i)
        s3.emit(i)
    assert count == N


def zip_streamz():
    import streamz

    s1 = streamz.Stream()
    s2 = streamz.Stream()
    s3 = streamz.Stream()
    count = 0

    def inc(x):
        nonlocal count
        count += 1

    s1.zip(s2, s3).sink(inc)
    for i in range(N):
        s1.emit(i)
        s2.emit(i)
        s3.emit(i)
    assert count == N


def zip_python():
    count = 0
    r = range(N)
    for a, b, c in zip(r, r, r, strict=True):
        count += 1
    assert count == N


# ============================================================
# 8. Combine Latest (3 sources)
# ============================================================


def combine_latest_akayu():
    import akayu

    s1 = akayu.Stream()
    s2 = akayu.Stream()
    s3 = akayu.Stream()
    count = 0

    def inc(x):
        nonlocal count
        count += 1

    s1.combine_latest(s2, s3).sink(inc)
    s1.compile()
    # prime s2 and s3 so combine_latest emits on every s1.emit
    s2.emit(0)
    s3.emit(0)
    count = 0  # reset after priming
    for i in range(N):
        s1.emit(i)
    assert count == N


def combine_latest_streamz():
    import streamz

    s1 = streamz.Stream()
    s2 = streamz.Stream()
    s3 = streamz.Stream()
    count = 0

    def inc(x):
        nonlocal count
        count += 1

    s1.combine_latest(s2, s3).sink(inc)
    # prime
    s2.emit(0)
    s3.emit(0)
    count = 0
    for i in range(N):
        s1.emit(i)
    assert count == N


def combine_latest_python():
    count = 0
    latest = [0, 0]
    for i in range(N):
        _ = (i, latest[0], latest[1])
        count += 1
    assert count == N


# ============================================================
# Run all benchmarks
# ============================================================

if __name__ == "__main__":
    topologies = [
        (
            "Deep Pipeline (10 maps)",
            [
                ("akayu (compile)", deep_akayu),
                ("akayu batch+compile", deep_akayu_batch),
                ("streamz", deep_streamz),
                ("pure python", deep_python),
            ],
        ),
        (
            "Diamond (split->union)",
            [
                ("akayu (compile)", diamond_akayu),
                ("akayu batch+compile", diamond_akayu_batch),
                ("streamz", diamond_streamz),
                ("pure python", diamond_python),
            ],
        ),
        (
            "Fan-out (5 branches)",
            [
                ("akayu (compile)", fanout_akayu),
                ("akayu batch+compile", fanout_akayu_batch),
                ("streamz", fanout_streamz),
                ("pure python", fanout_python),
            ],
        ),
        (
            "Complex DAG",
            [
                ("akayu (compile)", complex_dag_akayu),
                ("akayu batch+compile", complex_dag_akayu_batch),
                ("streamz", complex_dag_streamz),
                ("pure python", complex_dag_python),
            ],
        ),
        (
            "Multi-union (4 sources)",
            [
                ("akayu (compile)", multi_union_akayu),
                ("streamz", multi_union_streamz),
                ("pure python", multi_union_python),
            ],
        ),
        (
            "Accumulate (3 branches)",
            [
                ("akayu (compile)", accumulate_akayu),
                ("streamz", accumulate_streamz),
                ("pure python", accumulate_python),
            ],
        ),
        (
            "Zip (3 sources)",
            [
                ("akayu (compile)", zip_akayu),
                ("streamz", zip_streamz),
                ("pure python", zip_python),
            ],
        ),
        (
            "Combine Latest (3 sources)",
            [
                ("akayu (compile)", combine_latest_akayu),
                ("streamz", combine_latest_streamz),
                ("pure python", combine_latest_python),
            ],
        ),
    ]

    for topo_name, variants in topologies:
        print(f"\n{'=' * 60}")
        print(f"  {topo_name}  ({N:,} items)")
        print(f"{'=' * 60}")
        for label, fn in variants:
            try:
                ms, ips = bench(label, fn)
                print(f"  {label:<25s} {fmt(ms, ips)}")
            except Exception as e:
                print(f"  {label:<25s} ERROR: {e}")
