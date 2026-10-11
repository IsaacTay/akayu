# Concurrency

akayu provides two mechanisms for concurrent processing that work efficiently together: `par()` for parallel branch execution and `prefetch()` for ordered concurrent processing within a single branch.

## `par()` - Parallel Branch Execution

The `par()` method creates a branch point where all downstream branches execute concurrently in a shared thread pool:

<!--pytest.mark.skip-->

```python
source = akayu.Stream()
p = source.par()

# These branches run in parallel
a = p.map(load_from_disk)
b = p.map(fetch_from_api)

# Merge results
merged = akayu.union(a, b)

source.emit(filename)  # a and b execute concurrently
```

### How `par()` Works

```mermaid
flowchart LR
    S[source]:::source --> P[par]:::process
    P -- "Branch A" --> A[Map/Filter]:::process
    P -- "Branch B" --> B[Map/Filter]:::process
    P -- "Branch C" --> C[Map/Filter]:::process

    subgraph pool [Thread Pool]
        W1[worker 1]:::process
        W2[worker 2]:::process
        W3[worker 3]:::process
    end

    A -.-> W1
    B -.-> W2
    C -.-> W3
```

When a value reaches a `par()` node:

1. The value is submitted to all downstream branches
1. Each branch executes in a worker thread from the shared pool (scales to CPU count)
1. Results flow to convergence points (like `union`) as they complete

**Timing comparison:**

| Mode       | Branch A | Branch B | Branch C | Total    |
| ---------- | -------- | -------- | -------- | -------- |
| Sequential | 0-10ms   | 10-20ms  | 20-30ms  | **30ms** |
| Parallel   | 0-10ms   | 0-10ms   | 0-10ms   | **10ms** |

### Use Cases for `par()`

- **I/O-bound operations**: When branches wait on disk, network, or database
- **Independent processing**: When branches don't depend on each other's results
- **Fan-out patterns**: Processing the same input multiple ways

<!--pytest.mark.skip-->

```python
source = akayu.Stream()
p = source.par()

# Fan-out: same data processed three ways
p.map(save_to_database).sink(log_db_result)
p.map(send_notification).sink(log_notification)
p.map(update_cache).sink(log_cache_update)

source.compile()
```

```mermaid
flowchart TD
    S[source]:::source --> P[par]:::process
    P --> DB[save_to_database]:::process
    P --> N[send_notification]:::process
    P --> C[update_cache]:::process
    DB --> L1[log_db]:::sink
    N --> L2[log_notif]:::sink
    C --> L3[log_cache]:::sink
```

### Returning to Sequential

Use `seq()` after merging parallel branches to explicitly mark sequential execution:

<!--pytest.mark.skip-->

```python
merged = akayu.union(a, b).seq()
merged.map(sequential_op).sink(print)
```

## `prefetch()` - Ordered Concurrent Processing

The `prefetch(n)` method processes up to `n` items concurrently while **preserving output order**:

<!--pytest.mark.skip-->

```python
source = akayu.Stream()
source.prefetch(4).map(slow_operation).sink(results.append)
source.compile()

# Items processed concurrently but results arrive in order
source.emit_batch([1, 2, 3, 4, 5, 6, 7, 8])
```

### How `prefetch()` Works

```mermaid
flowchart TD
    subgraph input [Input Queue]
        I1[1]:::source --> I2[2]:::source --> I3[3]:::source --> I4[4]:::source --> I5["5,6,7,8..."]:::source
    end

    subgraph flight [In-Flight - capacity=4]
        F1["[1]"]:::process
        F2["[2]"]:::process
        F3["[3]"]:::process
        F4["[4]"]:::process
    end

    subgraph pool [Thread Pool]
        W["workers processing..."]:::process
    end

    subgraph reorder [Reorder Buffer]
        R["emit in sequence order"]:::process
    end

    input --> flight
    flight --> pool
    pool --> reorder
    reorder --> O[Output: 1,2,3,4,5...]:::sink
```

1. Each incoming item gets a sequence number
1. Up to `n` items are submitted to the thread pool simultaneously
1. Results are buffered and released in sequence number order
1. Backpressure: when `n` items are in flight, new items wait

**Order preservation:**

```mermaid
sequenceDiagram
    participant In as Input
    participant Pool as Thread Pool
    participant Buf as Reorder Buffer
    participant Out as Output

    In->>Pool: item 1 (seq#0)
    In->>Pool: item 2 (seq#1)
    In->>Pool: item 3 (seq#2)
    Pool->>Buf: result 2 (seq#1)
    Pool->>Buf: result 1 (seq#0)
    Buf->>Out: result 1 ✓
    Buf->>Out: result 2 ✓
    Pool->>Buf: result 3 (seq#2)
    Buf->>Out: result 3 ✓
```

Even if item 2 finishes before item 1, it waits in the buffer until item 1 is emitted.

### Prefetch with Different Operations

`prefetch()` automatically fuses with the following operation:

<!--pytest.mark.skip-->

```python
# Prefetch + map = PrefetchMap
source.prefetch(4).map(func)

# Prefetch + filter = PrefetchFilter
source.prefetch(4).filter(pred)

# Prefetch + batch_map = PrefetchBatchMap
source.prefetch(4).batch_map(vectorized_func)
```

```mermaid
flowchart LR
    subgraph before [Before]
        A[prefetch]:::process --> B[map func]:::process
    end

    subgraph after [After]
        C[PrefetchMap]:::process
    end

    before -.->|compile| after
```

### When to Use `prefetch()`

- **Slow per-item operations**: API calls, file I/O, image processing
- **Order-sensitive pipelines**: When downstream logic depends on item order
- **Rate-limited resources**: Control concurrency with the capacity parameter

<!--pytest.mark.skip-->

```python
# Process 4 images concurrently, results in original order
source.prefetch(4).map(process_image).sink(save_result)
```

## Combining `par()` and `prefetch()`

`par()` and `prefetch()` share the same thread pool (scales to CPU count), so combining them has minimal overhead:

<!--pytest.mark.skip-->

```python
source = akayu.Stream()
p = source.par()

# Branch 1: prefetch for ordered concurrent I/O
a = p.prefetch(4).map(fetch_from_api_a)

# Branch 2: prefetch for ordered concurrent I/O
b = p.prefetch(4).map(fetch_from_api_b)

merged = akayu.union(a, b)
source.compile()
```

```mermaid
flowchart TD
    S[source]:::source --> P[par]:::process

    P --> PF1["prefetch(4)"]:::process
    P --> PF2["prefetch(4)"]:::process

    PF1 --> M1[map API_A]:::process
    PF2 --> M2[map API_B]:::process

    M1 --> U[union]:::process
    M2 --> U

    subgraph pool [Shared Thread Pool]
        W["workers"]:::process
    end

    P -.-> pool
    PF1 -.-> pool
    PF2 -.-> pool
```

### Thread Safety at Convergence Points

When `prefetch()` is used inside `par()` branches, akayu automatically applies locking at convergence points (like `union`) to prevent race conditions:

```mermaid
flowchart TD
    S[source]:::source --> P[par]:::process
    P --> A["prefetch + map"]:::process
    P --> B["prefetch + map"]:::process
    A --> U["union 🔒"]:::warn
    B --> U
    U --> K[sink]:::sink

    style U stroke:#d97706,stroke-width:3px
```

The lock (🔒) ensures thread-safe updates when multiple prefetch workers emit simultaneously.

<!--pytest.mark.skip-->

```python
p = source.par()
a = p.prefetch(4).map(f1)  # Internal threading
b = p.prefetch(4).map(f2)  # Internal threading
merged = akayu.union(a, b)  # Lock applied here
```

The locking is optimized:

- Only convergence points (nodes reachable from multiple branches) are locked
- Individual branch nodes don't need locks since they're not shared
- Detected automatically during `compile()`

Merges of separate sources (`union`, `zip`, `combine_latest`) lock their shared node in the same way,
so the sources can be emitted from different threads. In a graph without `par()`, `compile()` gives
every node one shared lock instead: each `emit()` already takes it, so the per-item locks on merges
are dropped.

### Callbacks, Re-entrancy and Limits

- `flush()` and `drain()` may be called from another thread (a timer flushing a `collect()`, an
  `atexit` handler) while other threads emit: they wait for a running emit to finish, and an
  `emit()`, or an item reaching the node being flushed, waits for a running flush. In an
  uncompiled graph, if that flush's callbacks emit into the source the waiting item came from,
  the item fails with an error instead of deadlocking; `compile()` lets the two take turns.
- A callback may emit into another source. Emitting into a source that is itself mid-emit (from
  its own graph, through a `sink(source.emit)` or a loop of sources) raises "Re-entrant emit
  detected", and an item that comes back into a node still processing another raises "An item
  re-entered a node". Emitting into a source from a callback of its own `flush()` raises too. To
  feed results back, collect them and emit them after `emit()` returns.
- Adding nodes, compiling or stopping a profile from a callback, on the part of the graph that is
  running, raises a `RuntimeError`; the graph keeps working afterwards. The same goes for another
  thread doing so while an item passes through: build the graph before threads emit into it.
- Two threads whose callbacks emit into each other's sources, when those sources merge (`union`,
  `zip`, `combine_latest`), can deadlock in an uncompiled graph: each holds its own source's lock
  and waits for the other's. `compile()` gives the connected graph one lock, which avoids it.
- Each node an item passes adds to the thread's native stack. A graph thousands of nodes deep, a
  long chain of sources feeding each other, or a thread with a small stack raises `RecursionError`
  when the stack is nearly used up, rather than crashing; so do `flush()` and `drain()`.
- `KeyboardInterrupt`, `SystemExit` and `GeneratorExit` raised in a callback reach the caller as
  they are; any other exception is wrapped in a `RuntimeError` naming the node, with the original
  as its `__cause__`.
- Locks belong to OS threads. Greenlets (gevent, eventlet) share one, so a greenlet emitting into
  a source that another greenlet is mid-emit in looks like a re-entrant emit and raises. Give each
  source a greenlet lock, or emit from one greenlet.
- After `os.fork()`, a graph inherited from the parent is safe to use only if no other parent
  thread was emitting into it at the time: a lock that thread held stays held in the child. Graphs
  built in the child are always fine.
- Free-threaded Python (3.13t, 3.14t): akayu declares that it needs the GIL, so importing it turns
  the GIL back on (with a warning). On 3.13t, daemon threads still emitting when the interpreter
  exits can abort the process; join them first, or use 3.14t.

### Performance Characteristics

| Configuration           | Behavior                                        |
| ----------------------- | ----------------------------------------------- |
| `prefetch(n)` alone     | n concurrent items, ordered output              |
| `par()` alone           | Branches run in parallel                        |
| `par()` + `prefetch(n)` | Parallel branches, each with n concurrent items |

Benchmark showing minimal overhead when combining:

| Configuration           | Time (50 items, 10ms I/O each) |
| ----------------------- | ------------------------------ |
| `prefetch(4)` alone     | ~135ms                         |
| `par()` + `prefetch(4)` | ~136ms                         |

### Context, Errors and Nesting

- Each `par()` branch and each prefetched call runs in a copy of the calling thread's `contextvars` context: values set before `emit()` are visible, and changes made inside don't leak back.
- The last `par()` branch runs on the calling thread. A thread waiting for its branches or prefetched items runs any of them that no worker has started yet, so nesting `par()` deeper than the pool can't deadlock.
- `par()` waits for every branch, then raises the error of the first failing branch (in the order the branches were added). A failed prefetched item raises once, from the `emit()` or `flush()` that reaches it; later items carry on.
- Ctrl+C interrupts a thread that is waiting for branches or prefetched items. An `emit()`
  interrupted while waiting for a prefetched result has already started its item: the next
  `flush()` delivers it, so retrying the `emit()` would deliver it twice.

```python
import contextvars

import akayu

request_id = contextvars.ContextVar("request_id")
seen = []

source = akayu.Stream()
p = source.par()
p.map(lambda x: seen.append(("a", request_id.get())))
p.map(lambda x: seen.append(("b", request_id.get())))

request_id.set("req-1")
source.emit(1)
assert sorted(seen) == [("a", "req-1"), ("b", "req-1")]
```

## Flush for Prefetch

When using `prefetch()`, call `flush()` to wait for all in-flight items to complete:

<!--pytest.mark.skip-->

```python
source = akayu.Stream()
source.prefetch(4).map(slow_op).sink(results.append)
source.compile()

source.emit_batch(items)
source.flush()  # Wait for all items to complete

print(results)  # All results now available
```

```mermaid
flowchart LR
    subgraph before ["Before flush()"]
        F1["In-flight: [a][b][c][d]"]:::process
        C1["Completed: [1][2][3]"]:::sink
    end

    subgraph after ["After flush()"]
        F2["In-flight: (empty)"]:::process
        C2["Completed: [1][2][3][a][b][c][d]"]:::sink
    end

    before -->|"flush()"| after
```

## Example: Parallel API Fetching

```python
import akayu

def fetch_user(user_id):
    # Simulated API call
    return {"id": user_id, "name": f"User {user_id}"}

def fetch_orders(user_id):
    # Simulated API call
    return {"user_id": user_id, "orders": []}

source = akayu.Stream()
p = source.par()

users = []
orders = []

# Fetch users and orders in parallel
p.prefetch(4).map(fetch_user).sink(users.append)
p.prefetch(4).map(fetch_orders).sink(orders.append)

source.compile()

# Process user IDs
source.emit_batch([1, 2, 3, 4, 5])
source.flush()

# Both users and orders fetched concurrently
print(f"Users: {len(users)}, Orders: {len(orders)}")
```

```mermaid
flowchart TD
    I["[1, 2, 3, 4, 5]<br/>user IDs"]:::source --> S[source]:::source
    S --> P[par]:::process

    P --> PF1["prefetch(4)"]:::process
    P --> PF2["prefetch(4)"]:::process

    PF1 --> U["fetch_user"]:::process
    PF2 --> O["fetch_orders"]:::process

    U --> US["users[]"]:::sink
    O --> OS["orders[]"]:::sink
```

Both API endpoints are called concurrently for each user ID, with 4 concurrent requests per branch = up to 8 total concurrent I/O operations.
