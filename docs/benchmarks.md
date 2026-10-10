# Benchmarks

`akayu` against the original `streamz` library and hand-written Python, on the topologies
described under [Benchmark Details](#benchmark-details).

**Environment:** Python 3.13.11, Linux x86_64 (AMD Ryzen 7 PRO 4750G, shared machine),
500,000 items per run, mean of 5 runs after a warm-up, `tests/bench_complex.py`.

## Summary

| Topology Type | Scenario                    | Speedup over streamz |
| ------------- | --------------------------- | -------------------- |
| Deep Chains   | 10 chained maps             | **~51x**             |
| Branching     | Fan-out, diamond            | **~30x - 44x**       |
| Merging       | Union, zip, combine_latest  | **~30x - 60x**       |
| Complex DAG   | Split, flatten, union mixed | **~30x**             |

## Complex Flow Topologies (500k items)

Every akayu graph is compiled with `compile()`. "Pure Python" is a hand-written loop with the
operations inlined: no functions are called, so it is a floor rather than an equivalent
program.

| Topology                                        | akayu (ms) | akayu `emit_batch` (ms) | streamz (ms) | Pure Python (ms) | vs streamz     |
| ----------------------------------------------- | ---------: | ----------------------: | -----------: | ---------------: | -------------- |
| [Deep pipeline (10 maps)](#deep-pipeline)       |        373 |                     264 |       19,066 |              240 | **51x faster** |
| [Diamond (split → union)](#diamond)             |        226 |                     215 |        9,888 |               80 | **44x faster** |
| [Fan-out (5 branches)](#fan-out)                |        603 |                     595 |       18,224 |              264 | **30x faster** |
| [Complex DAG](#complex-dag)                     |        481 |                     428 |       14,254 |              106 | **30x faster** |
| [Multi-union (4 sources)](#multi-union)         |        129 |                       - |        7,653 |               44 | **60x faster** |
| [Accumulate (3 branches)](#accumulate-branches) |        422 |                       - |        9,124 |              119 | **22x faster** |
| [Zip (3 sources)](#zip)                         |        208 |                       - |        9,390 |               51 | **45x faster** |
| [Combine latest (3 sources)](#combine-latest)   |        142 |                       - |        4,290 |               51 | **30x faster** |

**Observations:**

- akayu is 22x - 60x faster than streamz on every topology.
- Most of akayu's remaining time is CPython running the pipeline's own functions: measured in
  instructions, akayu's own code is about 17% of a plain pipeline. The gap to hand-written Python
  is mostly the function calls a hand-written loop doesn't make.
- `emit_batch()` helps most on long linear chains (deep pipeline: 373 -> 264 ms). Below a
  fan-out, items still go through the branches one at a time, so batching gains little there.

## Running Benchmarks

Wall-clock comparisons with streamz and plain Python (the tables above):

```bash
just build-release
uv run python tests/bench_complex.py
```

Instructions per item for every workload and mode, plus memory per node. Instruction counts don't
change with machine load, so they can be compared between runs on a busy machine:

```bash
just bench-cost --save before.json   # ...change something...
just bench-cost --compare before.json
```

The pytest-benchmark suite: `just bench`.

## Benchmark Details

Each topology processes 500,000 items. Timing includes building and compiling the graph.

### Basic pipeline (`just bench`)

The pytest-benchmark suite's main pipeline:

- `map(lambda x: x + 1)`
- `filter(lambda x: x % 2 == 0)`
- `sink(results.append)`

```mermaid
flowchart LR
    S[Source] --> M["map(x + 1)"]
    M --> F["filter(x % 2 == 0)"]
    F --> K["sink(append)"]
```

### Multi-union

4 separate sources merged via union chain.

```mermaid
flowchart LR
    S1[Source 1] --> U1[Union]
    S2[Source 2] --> U1
    S3[Source 3] --> U2[Union]
    S4[Source 4] --> U3[Union]
    U1 --> U2
    U2 --> U3
    U3 --> M[map] --> K[sink]
```

### Combine latest

3 sources combined, emitting tuples of latest values.

```mermaid
flowchart LR
    S1[Source 1] --- C[CombineLatest]
    S2[Source 2] --- C
    S3[Source 3] --- C
    C --> K[sink]
```

### Diamond

Single source splits to 2 branches, then merges via union.

```mermaid
flowchart LR
    S[Source] --> M1["map(*2)"]
    S --> M2["map(*3)"]
    M1 --> U[Union]
    M2 --> U
    U --> K[sink]
```

### Accumulate branches

Single source with 3 parallel accumulate operations (sum, count, max).

```mermaid
flowchart LR
    S[Source] --> A1["accumulate(sum)"] --> K1[sink]
    S --> A2["accumulate(count)"] --> K2[sink]
    S --> A3["accumulate(max)"] --> K3[sink]
```

### Zip

3 sources zipped together element-by-element.

```mermaid
flowchart LR
    S1[Source 1] --- Z[Zip]
    S2[Source 2] --- Z
    S3[Source 3] --- Z
    Z --> K[sink]
```

### Deep pipeline

10 chained map operations.

```mermaid
flowchart LR
    S[Source] --> M1[map] --> M2[map] --> M3[...] --> M10[map] --> K[sink]
```

### Complex DAG

Split with one branch using flatten, then union.

```mermaid
flowchart LR
    S[Source] --> M1[map] --> SP[Split]
    SP --> B1M1["map(*2)"] --> B1F["filter(%4)"] --> U[Union]
    SP --> B2M1["map(list)"] --> B2FL[flatten] --> B2F["filter(%3)"] --> U
    U --> M3[map] --> K[sink]
```

### Fan-out

Single source with 5 parallel map→filter→sink branches.

```mermaid
flowchart LR
    S[Source]
    
    S --> M1["map"] --> F1["filter"] --> K1["sink"]
    S --> M2["map"] --> F2["filter"] --> K2["sink"]
    S --> M3["..."]
    S --> M5["map"] --> F5["filter"] --> K5["sink"]
```
