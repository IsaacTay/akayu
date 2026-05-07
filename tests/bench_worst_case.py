"""Find worst-case pipelines where akayu is significantly slower than Python.

Focus on operations that:
- Python handles with specialized bytecodes or inline
- akayu must go through generic C-API or can't compile to builtins
- Involve attribute access, subscripting, method calls, closures with state
"""

import time
from operator import itemgetter

import akayu

N = 500_000


def bench(fn, rounds=7):
    fn()  # warmup
    times = []
    for _ in range(rounds):
        start = time.perf_counter()
        fn()
        elapsed = time.perf_counter() - start
        times.append(elapsed)
    times.sort()
    return times[len(times) // 2]


def compare(name, make_akayu, make_python):
    a_s = bench(make_akayu())
    p_s = bench(make_python())
    a_ns = a_s / N * 1e9
    p_ns = p_s / N * 1e9
    ratio = a_ns / p_ns if p_ns > 0 else float('inf')
    verdict = "FASTER" if ratio < 0.9 else ("SLOWER" if ratio > 1.1 else "~SAME")
    print(f"  {name:<40s}  {a_ns:>7.1f}   {p_ns:>7.1f}   {ratio:>6.2f}x  {verdict}")
    return ratio


if __name__ == "__main__":
    print(f"\n{'='*80}")
    print(f"  Worst-case search  ({N:,} items, absolute ns/item)")
    print(f"{'='*80}")
    print(f"  {'operation':<40s}  {'akayu':>7s}   {'python':>7s}   {'ratio':>6s}  verdict")
    print(f"  {'-'*40}  {'-'*7}   {'-'*7}   {'-'*6}  {'-'*7}")

    int_inputs = list(range(N))

    # --- Non-compilable lambdas (closure state, multiple args, etc.) ---
    # These can't become builtins, so akayu calls through vectorcall

    print("\n  ── Lambdas that can't compile to builtins ──")

    # closure over mutable state
    def _akayu_closure():
        def make():
            s = akayu.Stream()
            results = []
            offset = 42
            s.map(lambda x: x + offset).sink(results.append)
            s.compile()
            s.emit_batch(int_inputs)
        return make

    def _python_closure():
        def make():
            results = []
            offset = 42
            for i in range(N):
                results.append(i + offset)
        return make

    compare("closure: x + offset", _akayu_closure, _python_closure)

    # multi-expression lambda
    def _akayu_multi():
        def make():
            s = akayu.Stream()
            results = []
            s.map(lambda x: (x * 2 + 1) % 100).sink(results.append)
            s.compile()
            s.emit_batch(int_inputs)
        return make

    def _python_multi():
        def make():
            results = []
            for i in range(N):
                results.append((i * 2 + 1) % 100)
        return make

    compare("multi-expr: (x*2+1)%100", _akayu_multi, _python_multi)

    # conditional expression
    def _akayu_ternary():
        def make():
            s = akayu.Stream()
            results = []
            s.map(lambda x: x * 2 if x > 100 else x + 1).sink(results.append)
            s.compile()
            s.emit_batch(int_inputs)
        return make

    def _python_ternary():
        def make():
            results = []
            for i in range(N):
                results.append(i * 2 if i > 100 else i + 1)
        return make

    compare("ternary: x*2 if x>100 else x+1", _akayu_ternary, _python_ternary)

    # --- Dict/object operations ---
    print("\n  ── Dict/object access ──")

    dict_inputs = [{"a": i, "b": i * 2, "val": str(i)} for i in range(N)]

    # dict subscript
    def _akayu_dict_sub():
        def make():
            s = akayu.Stream()
            results = []
            s.map(lambda x: x["a"]).sink(results.append)
            s.compile()
            s.emit_batch(dict_inputs)
        return make

    def _python_dict_sub():
        def make():
            results = []
            for d in dict_inputs:
                results.append(d["a"])
        return make

    compare("d['a'] (lambda)", _akayu_dict_sub, _python_dict_sub)

    # itemgetter
    def _akayu_itemgetter():
        def make():
            s = akayu.Stream()
            results = []
            s.map(itemgetter("a")).sink(results.append)
            s.compile()
            s.emit_batch(dict_inputs)
        return make

    compare("d['a'] (itemgetter)", _akayu_itemgetter, _python_dict_sub)

    # dict multi-key access
    def _akayu_dict_multi():
        def make():
            s = akayu.Stream()
            results = []
            s.map(lambda x: (x["a"], x["b"])).sink(results.append)
            s.compile()
            s.emit_batch(dict_inputs)
        return make

    def _python_dict_multi():
        def make():
            results = []
            for d in dict_inputs:
                results.append((d["a"], d["b"]))
        return make

    compare("(d['a'], d['b']) tuple", _akayu_dict_multi, _python_dict_multi)

    # --- String operations ---
    print("\n  ── String operations ──")

    str_inputs = [f"  hello world {i}  " for i in range(N)]

    def _akayu_str_strip():
        def make():
            s = akayu.Stream()
            results = []
            s.map(lambda x: x.strip()).sink(results.append)
            s.compile()
            s.emit_batch(str_inputs)
        return make

    def _python_str_strip():
        def make():
            results = []
            for s in str_inputs:
                results.append(s.strip())
        return make

    compare("x.strip()", _akayu_str_strip, _python_str_strip)

    def _akayu_str_upper():
        def make():
            s = akayu.Stream()
            results = []
            s.map(lambda x: x.upper()).sink(results.append)
            s.compile()
            s.emit_batch(str_inputs)
        return make

    def _python_str_upper():
        def make():
            results = []
            for s in str_inputs:
                results.append(s.upper())
        return make

    compare("x.upper()", _akayu_str_upper, _python_str_upper)

    def _akayu_str_split():
        def make():
            s = akayu.Stream()
            results = []
            s.map(lambda x: x.split()).sink(results.append)
            s.compile()
            s.emit_batch(str_inputs)
        return make

    def _python_str_split():
        def make():
            results = []
            for s in str_inputs:
                results.append(s.split())
        return make

    compare("x.split()", _akayu_str_split, _python_str_split)

    # --- Tuple/list construction ---
    print("\n  ── Construction ──")

    def _akayu_tuple_pack():
        def make():
            s = akayu.Stream()
            results = []
            s.map(lambda x: (x, x + 1, x + 2)).sink(results.append)
            s.compile()
            s.emit_batch(int_inputs)
        return make

    def _python_tuple_pack():
        def make():
            results = []
            for i in range(N):
                results.append((i, i + 1, i + 2))
        return make

    compare("(x, x+1, x+2) tuple", _akayu_tuple_pack, _python_tuple_pack)

    def _akayu_list_comp():
        def make():
            s = akayu.Stream()
            results = []
            s.map(lambda x: [x * i for i in range(5)]).sink(results.append)
            s.compile()
            s.emit_batch(int_inputs)
        return make

    def _python_list_comp():
        def make():
            results = []
            for x in range(N):
                results.append([x * i for i in range(5)])
        return make

    compare("[x*i for i in range(5)]", _akayu_list_comp, _python_list_comp)

    # --- Chained operations (map + filter combos) ---
    print("\n  ── Multi-step pipelines (absolute) ──")

    # Simple ETL: parse → transform → filter → output
    def _akayu_etl():
        def make():
            s = akayu.Stream()
            results = []
            (s.map(lambda x: x * 2 + 1)
              .filter(lambda x: x % 3 == 0)
              .map(lambda x: x * x)
              .sink(results.append))
            s.compile()
            s.emit_batch(int_inputs)
        return make

    def _python_etl():
        def make():
            results = []
            for i in range(N):
                x = i * 2 + 1
                if x % 3 == 0:
                    results.append(x * x)
        return make

    compare("map→filter→map (arithmetic)", _akayu_etl, _python_etl)

    # Dict ETL: extract → transform → filter
    def _akayu_dict_etl():
        def make():
            s = akayu.Stream()
            results = []
            (s.map(lambda x: x["a"])
              .filter(lambda x: x > 100)
              .map(lambda x: x * 2)
              .sink(results.append))
            s.compile()
            s.emit_batch(dict_inputs)
        return make

    def _python_dict_etl():
        def make():
            results = []
            for d in dict_inputs:
                a = d["a"]
                if a > 100:
                    results.append(a * 2)
        return make

    compare("dict: d['a']→filter>100→*2", _akayu_dict_etl, _python_dict_etl)

    # --- Accumulate (stateful) ---
    print("\n  ── Stateful operations ──")

    def _akayu_accum():
        def make():
            s = akayu.Stream()
            results = []
            s.accumulate(lambda a, b: a + b, start=0).sink(results.append)
            s.compile()
            s.emit_batch(int_inputs)
        return make

    def _python_accum():
        def make():
            results = []
            acc = 0
            for i in range(N):
                acc = acc + i
                results.append(acc)
        return make

    compare("accumulate(sum)", _akayu_accum, _python_accum)

    # --- Sink-heavy (the sink IS the work) ---
    print("\n  ── Sink-heavy ──")

    def _akayu_identity_sink():
        def make():
            s = akayu.Stream()
            results = []
            s.sink(results.append)
            s.compile()
            s.emit_batch(int_inputs)
        return make

    def _python_identity_sink():
        def make():
            results = []
            for i in range(N):
                results.append(i)
        return make

    compare("source→sink (just append)", _akayu_identity_sink, _python_identity_sink)

    # noop sink
    def _akayu_noop():
        def make():
            s = akayu.Stream()
            s.sink(lambda x: None)
            s.compile()
            s.emit_batch(int_inputs)
        return make

    def _python_noop():
        def make():
            def noop(x):
                pass
            for i in range(N):
                noop(i)
        return make

    compare("source→sink (noop lambda)", _akayu_noop, _python_noop)
