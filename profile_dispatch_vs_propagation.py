"""Isolate dispatch vs propagation overhead.

By varying chain depth (all builtin, no vectorcall), we can see:
- 1 node: dispatch + propagation baseline
- N nodes: dispatch * N + propagation * N
- The slope = per-node (dispatch + propagation) cost
- Compare with sink-only = just propagation + dispatch for 1 node

Also compare compiled chain (flat loop, no propagation between nodes)
vs uncompiled (propagate between every node).
"""
import time
import akayu

N = 5_000_000
REPS = 3
BATCH_SIZE = 10_000

def bench(label, setup_fn):
    times = []
    for _ in range(REPS):
        s, batch = setup_fn()
        t0 = time.perf_counter()
        for _ in range(N // BATCH_SIZE):
            s.emit_batch(batch)
        elapsed = time.perf_counter() - t0
        times.append(elapsed)
    best = min(times)
    throughput = N / best / 1e6
    ns_per_item = best / N * 1e9
    print(f"  {label:50s}: {best:5.3f}s  {throughput:6.2f}M/s  {ns_per_item:5.1f} ns/item")
    return ns_per_item

print(f"=== Dispatch vs Propagation ({N:,} items, best of {REPS}) ===\n")

# --- Compiled chain: dispatch only, no per-node propagation ---
# Chain collapses N nodes into a flat loop, eliminating propagation between them.
print("COMPILED (chain optimization — no inter-node propagation):")
compiled_results = {}
for depth in [1, 2, 4, 8, 16]:
    def make_setup(d):
        def setup():
            s = akayu.Stream(asynchronous=False)
            node = s
            for _ in range(d):
                node = node.map(bool)  # builtin, no vectorcall
            node.sink(lambda x: None)
            s.compile()
            return s, list(range(BATCH_SIZE))
        return setup
    ns = bench(f"depth={depth:2d}  (map(bool) x{depth} -> sink)", make_setup(depth))
    compiled_results[depth] = ns

# --- Uncompiled: dispatch + propagation per node ---
print("\nUNCOMPILED (propagation between every node):")
uncompiled_results = {}
for depth in [1, 2, 4, 8, 16]:
    def make_setup(d):
        def setup():
            s = akayu.Stream(asynchronous=False)
            node = s
            for _ in range(d):
                node = node.map(bool)  # builtin, no vectorcall
            node.sink(lambda x: None)
            # NO compile — propagation happens between nodes
            return s, list(range(BATCH_SIZE))
        return setup
    ns = bench(f"depth={depth:2d}  (map(bool) x{depth} -> sink)", make_setup(depth))
    uncompiled_results[depth] = ns

# --- Analysis ---
print("\n=== Analysis ===")
print("\nCompiled (chain — flat loop, no propagation between nodes):")
if 1 in compiled_results and 16 in compiled_results:
    slope = (compiled_results[16] - compiled_results[1]) / 15
    print(f"  Per-node cost (slope): {slope:.1f} ns/item  (= dispatch only)")
    print(f"  Fixed overhead (intercept): {compiled_results[1] - slope:.1f} ns/item  (= entry + sink + propagation)")

print("\nUncompiled (propagation between every node):")
if 1 in uncompiled_results and 16 in uncompiled_results:
    slope = (uncompiled_results[16] - uncompiled_results[1]) / 15
    print(f"  Per-node cost (slope): {slope:.1f} ns/item  (= dispatch + propagation)")
    print(f"  Fixed overhead (intercept): {uncompiled_results[1] - slope:.1f} ns/item  (= entry + sink)")

if 1 in compiled_results and 1 in uncompiled_results:
    compiled_slope = (compiled_results[16] - compiled_results[1]) / 15
    uncompiled_slope = (uncompiled_results[16] - uncompiled_results[1]) / 15
    prop_cost = uncompiled_slope - compiled_slope
    print(f"\n  Propagation cost per node: {prop_cost:.1f} ns/item  (uncompiled_slope - compiled_slope)")
    print(f"  Dispatch cost per node:    {compiled_slope:.1f} ns/item  (compiled slope)")
