"""Benchmark: lambda decompilation vs direct builtin vs unoptimized."""

import pytest

import akayu

N = 1_000_000


def _build_and_run(func, data, *, is_filter=False, compile=True):
    """Build pipeline, compile if requested, emit_batch data, return results."""
    s = akayu.Stream()
    results = []
    if is_filter:
        s.filter(func).sink(results.append)
    else:
        s.map(func).sink(results.append)
    if compile:
        s.compile()
    s.emit_batch(data)
    return results


# --- Map: len ---

@pytest.mark.benchmark(group="map-len")
def test_map_len_direct(benchmark):
    """s.map(len) — identity-detected BuiltinOp::Len"""
    data = list(range(N))
    data_str = [str(x) for x in data]
    def run():
        return _build_and_run(len, data_str)
    benchmark(run)

@pytest.mark.benchmark(group="map-len")
def test_map_len_lambda(benchmark):
    """s.map(lambda x: len(x)) — lambda-decompiled to BuiltinOp::Len"""
    data_str = [str(x) for x in range(N)]
    def run():
        return _build_and_run(lambda x: len(x), data_str)
    benchmark(run)

@pytest.mark.benchmark(group="map-len")
def test_map_len_lambda_no_compile(benchmark):
    """s.map(lambda x: len(x)) — no compile, pure vcall"""
    data_str = [str(x) for x in range(N)]
    def run():
        return _build_and_run(lambda x: len(x), data_str, compile=False)
    benchmark(run)


# --- Map: x + 1 ---

@pytest.mark.benchmark(group="map-add")
def test_map_add_lambda(benchmark):
    """s.map(lambda x: x + 1) — decompiled to BuiltinOp::BinOp(Add)"""
    data = list(range(N))
    def run():
        return _build_and_run(lambda x: x + 1, data)
    benchmark(run)

@pytest.mark.benchmark(group="map-add")
def test_map_add_lambda_no_compile(benchmark):
    """s.map(lambda x: x + 1) — no compile, pure vcall"""
    data = list(range(N))
    def run():
        return _build_and_run(lambda x: x + 1, data, compile=False)
    benchmark(run)


# --- Map: x["key"] ---

@pytest.mark.benchmark(group="map-itemgetter")
def test_map_itemgetter_direct(benchmark):
    """s.map(operator.itemgetter('k')) — identity-detected"""
    import operator
    data = [{"k": i} for i in range(N)]
    def run():
        return _build_and_run(operator.itemgetter("k"), data)
    benchmark(run)

@pytest.mark.benchmark(group="map-itemgetter")
def test_map_itemgetter_lambda(benchmark):
    """s.map(lambda x: x['k']) — lambda-decompiled to ItemGetter"""
    data = [{"k": i} for i in range(N)]
    def run():
        return _build_and_run(lambda x: x["k"], data)
    benchmark(run)

@pytest.mark.benchmark(group="map-itemgetter")
def test_map_itemgetter_lambda_no_compile(benchmark):
    """s.map(lambda x: x['k']) — no compile, pure vcall"""
    data = [{"k": i} for i in range(N)]
    def run():
        return _build_and_run(lambda x: x["k"], data, compile=False)
    benchmark(run)


# --- Map: x.strip() ---

@pytest.mark.benchmark(group="map-method")
def test_map_strip_lambda(benchmark):
    """s.map(lambda x: x.strip()) — decompiled to MethodCall"""
    data = [f"  {i}  " for i in range(N)]
    def run():
        return _build_and_run(lambda x: x.strip(), data)
    benchmark(run)

@pytest.mark.benchmark(group="map-method")
def test_map_strip_lambda_no_compile(benchmark):
    """s.map(lambda x: x.strip()) — no compile, pure vcall"""
    data = [f"  {i}  " for i in range(N)]
    def run():
        return _build_and_run(lambda x: x.strip(), data, compile=False)
    benchmark(run)


# --- Filter: x > 0 ---

@pytest.mark.benchmark(group="filter-compare")
def test_filter_gt_lambda(benchmark):
    """s.filter(lambda x: x > 0) — decompiled to Compare(Gt)"""
    data = list(range(-N//2, N//2))
    def run():
        return _build_and_run(lambda x: x > 0, data, is_filter=True)
    benchmark(run)

@pytest.mark.benchmark(group="filter-compare")
def test_filter_gt_lambda_no_compile(benchmark):
    """s.filter(lambda x: x > 0) — no compile, pure vcall"""
    data = list(range(-N//2, N//2))
    def run():
        return _build_and_run(lambda x: x > 0, data, is_filter=True, compile=False)
    benchmark(run)


# --- Filter: isinstance(x, int) ---

@pytest.mark.benchmark(group="filter-isinstance")
def test_filter_isinstance_direct(benchmark):
    """s.filter(isinstance, int) — identity-detected via wrapper"""
    data = list(range(N))
    def run():
        s = akayu.Stream()
        results = []
        s.filter(isinstance, int).sink(results.append)
        s.compile()
        s.emit_batch(data)
        return results
    benchmark(run)

@pytest.mark.benchmark(group="filter-isinstance")
def test_filter_isinstance_lambda(benchmark):
    """s.filter(lambda x: isinstance(x, int)) — lambda-decompiled"""
    data = list(range(N))
    def run():
        return _build_and_run(lambda x: isinstance(x, int), data, is_filter=True)
    benchmark(run)

@pytest.mark.benchmark(group="filter-isinstance")
def test_filter_isinstance_lambda_no_compile(benchmark):
    """s.filter(lambda x: isinstance(x, int)) — no compile, pure vcall"""
    data = list(range(N))
    def run():
        return _build_and_run(lambda x: isinstance(x, int), data, is_filter=True, compile=False)
    benchmark(run)


# --- Filter: x is not None and x > 0 (compound) ---

@pytest.mark.benchmark(group="filter-compound")
def test_filter_compound_lambda(benchmark):
    """s.filter(lambda x: x is not None and x > 0) — decompiled compound"""
    data = [None if i % 3 == 0 else i - N//2 for i in range(N)]
    def run():
        return _build_and_run(lambda x: x is not None and x > 0, data, is_filter=True)
    benchmark(run)

@pytest.mark.benchmark(group="filter-compound")
def test_filter_compound_lambda_no_compile(benchmark):
    """s.filter(lambda x: x is not None and x > 0) — no compile, pure vcall"""
    data = [None if i % 3 == 0 else i - N//2 for i in range(N)]
    def run():
        return _build_and_run(lambda x: x is not None and x > 0, data, is_filter=True, compile=False)
    benchmark(run)


# --- Filter: x["status"] == "active" (chain) ---

@pytest.mark.benchmark(group="filter-chain")
def test_filter_chain_lambda(benchmark):
    """s.filter(lambda x: x['s'] == 'a') — decompiled chain"""
    data = [{"s": "a" if i % 2 == 0 else "b"} for i in range(N)]
    def run():
        return _build_and_run(lambda x: x["s"] == "a", data, is_filter=True)
    benchmark(run)

@pytest.mark.benchmark(group="filter-chain")
def test_filter_chain_lambda_no_compile(benchmark):
    """s.filter(lambda x: x['s'] == 'a') — no compile, pure vcall"""
    data = [{"s": "a" if i % 2 == 0 else "b"} for i in range(N)]
    def run():
        return _build_and_run(lambda x: x["s"] == "a", data, is_filter=True, compile=False)
    benchmark(run)


# --- Map: x in set (contains) ---

@pytest.mark.benchmark(group="map-contains")
def test_map_contains_lambda(benchmark):
    """s.map(lambda x: x in valid) — decompiled to Contains"""
    valid = set(range(0, N, 2))
    data = list(range(N))
    def run():
        return _build_and_run(lambda x: x in valid, data)
    benchmark(run)

@pytest.mark.benchmark(group="map-contains")
def test_map_contains_lambda_no_compile(benchmark):
    """s.map(lambda x: x in valid) — no compile, pure vcall"""
    valid = set(range(0, N, 2))
    data = list(range(N))
    def run():
        return _build_and_run(lambda x: x in valid, data, compile=False)
    benchmark(run)
