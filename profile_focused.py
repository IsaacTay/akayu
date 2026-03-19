"""Focused profiling: one scenario at a time for higher resolution."""
import sys
import akayu

N = 5_000_000

scenario = sys.argv[1] if len(sys.argv) > 1 else "compiled_emit"

def run_compiled_emit():
    s = akayu.Stream(asynchronous=False)
    count = [0]
    def inc(x):
        count[0] += 1
    s.map(lambda x: x + 1).filter(lambda x: x % 2 == 0).map(lambda x: x * 2).sink(inc)
    s.compile()
    for i in range(N):
        s.emit(i)

def run_compiled_batch():
    s = akayu.Stream(asynchronous=False)
    count = [0]
    def inc(x):
        count[0] += 1
    s.map(lambda x: x + 1).filter(lambda x: x % 2 == 0).map(lambda x: x * 2).sink(inc)
    s.compile()
    batch = list(range(10_000))
    for _ in range(N // 10_000):
        s.emit_batch(batch)

def run_deep_chain():
    s = akayu.Stream(asynchronous=False)
    node = s
    for _ in range(10):
        node = node.map(lambda x: x + 1)
    count = [0]
    def inc(x):
        count[0] += 1
    node.sink(inc)
    s.compile()
    for i in range(N):
        s.emit(i)

def run_fanout():
    s = akayu.Stream(asynchronous=False)
    for j in range(5):
        def make_inc():
            c = [0]
            def inc(x): c[0] += 1
            return inc
        s.map(lambda x: x + 1).filter(lambda x: x % 2 == 0).sink(make_inc())
    s.compile()
    for i in range(N):
        s.emit(i)

scenarios = {
    "compiled_emit": run_compiled_emit,
    "compiled_batch": run_compiled_batch,
    "deep_chain": run_deep_chain,
    "fanout": run_fanout,
}

scenarios[scenario]()
