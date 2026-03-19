"""High-performance reactive streams for Python."""

from typing import Any, Callable, Coroutine, Iterable, Optional, TypeVar, List, Union, ContextManager

T = TypeVar("T")
U = TypeVar("U")

class ProfileReport:
    """Profiling report for a pipeline execution.

    Contains per-node timing and item flow data, preserving the graph
    structure for tree-view display.
    """

    def print(self, sort_by: Optional[str] = None) -> None:
        """Print the profiling report.

        Args:
            sort_by: If "time", print a flat list sorted by func_time descending.
                     If None (default), print a tree view showing graph structure.
        """
        ...

    def to_dict(self) -> dict[str, Any]:
        """Return profiling data as a dictionary."""
        ...

    def to_list(self) -> list[dict[str, Any]]:
        """Return profiling data as a list of node dicts."""
        ...

    @property
    def total_func_time_ms(self) -> float:
        """Total function time across all nodes in milliseconds."""
        ...

    @property
    def total_overhead_ms(self) -> float:
        """Total framework overhead in milliseconds."""
        ...

    @property
    def total_time_ms(self) -> float:
        """Total wall time in milliseconds."""
        ...

    @property
    def emit_count(self) -> int:
        """Number of emit calls profiled."""
        ...

    def __repr__(self) -> str:
        """Return string representation of the profile report."""
        ...

class Stream:
    """A reactive stream for processing data through a pipeline of operations.

    Stream is the core building block for creating data processing pipelines.
    Data flows through a directed graph of stream nodes, where each node applies
    a transformation (map, filter, etc.) before passing results downstream.

    Example:
        ```python
        source = Stream()
        result = []
        source.map(lambda x: x * 2).sink(result.append)
        source.emit(5)
        result  # [10]
        ```
    """

    def __init__(
        self, name: Optional[str] = None, asynchronous: Optional[bool] = None
    ) -> None:
        """Create a new source Stream.

        Args:
            name: Optional name for the stream (defaults to "source").
            asynchronous: If False, skip async checking for better performance.
                If None or True, async coroutines are automatically awaited.
        """
        ...

    def __repr__(self) -> str:
        """Return string representation of the stream."""
        ...

    def __enter__(self) -> "Stream":
        """Enter the stream context manager (returns self)."""
        ...

    def __exit__(self, *args: Any) -> None:
        """Exit the stream context manager."""
        ...

    def map(self, func: Callable[..., T], *args: Any, stream_name: Optional[str] = None, **kwargs: Any) -> "Stream":
        """Apply a function to each element in the stream.

        Args:
            func: Function to apply to each element.
            *args: Additional positional arguments passed to func.
            stream_name: Optional custom name for this node.
            **kwargs: Additional keyword arguments passed to func.

        Returns:
            A new Stream emitting the transformed values.
        """
        ...

    def filter(
        self, predicate: Callable[..., bool], *args: Any, stream_name: Optional[str] = None, **kwargs: Any
    ) -> "Stream":
        """Filter elements based on a predicate function.

        Only elements for which the predicate returns a truthy value
        are passed downstream.

        Args:
            predicate: Function that returns True for elements to keep.
            *args: Additional positional arguments passed to predicate.
            stream_name: Optional custom name for this node.
            **kwargs: Additional keyword arguments passed to predicate.

        Returns:
            A new Stream emitting only elements that pass the filter.
        """
        ...

    def starmap(self, func: Callable[..., T], *args: Any, stream_name: Optional[str] = None, **kwargs: Any) -> "Stream":
        """Apply a function to each element, unpacking the element as arguments.

        Similar to map, but each element is unpacked with *element before
        being passed to the function. Useful when elements are tuples.

        Args:
            func: Function to apply (receives unpacked element).
            *args: Additional positional arguments passed to func.
            stream_name: Optional custom name for this node.
            **kwargs: Additional keyword arguments passed to func.

        Returns:
            A new Stream emitting the transformed values.
        """
        ...

    def batch_map(
        self, func: Callable[[List[Any]], Iterable[T]], *args: Any, stream_name: Optional[str] = None, **kwargs: Any
    ) -> "Stream":
        """Apply a batch-aware function to process entire batches at once.

        Unlike `map()` which calls the function once per item, `batch_map()` calls
        the function once per batch with all items as a list. This is ideal for
        vectorized operations like NumPy functions that can process arrays efficiently.

        Args:
            func: Function that receives a list of items and returns an iterable of results.
            *args: Additional positional arguments passed to func.
            stream_name: Optional custom name for this node.
            **kwargs: Additional keyword arguments passed to func.

        Returns:
            A new Stream emitting the transformed values.

        Example:
            ```python
            import numpy as np
            s = Stream()
            s.batch_map(np.sqrt).sink(print)
            s.emit_batch([1, 4, 9, 16])  # prints 1.0, 2.0, 3.0, 4.0
            ```
        """
        ...

    def flatten(self) -> "Stream":
        """Flatten an iterable element into individual elements.

        Each element is expected to be iterable. The items within each
        element are emitted individually downstream.

        Returns:
            A new Stream emitting the flattened elements.
        """
        ...

    def collect(self) -> "Stream":
        """Collect elements into a buffer until `flush()` is called.

        Elements are accumulated in an internal buffer. When `flush()` is
        called, all collected elements are emitted as a single tuple.

        Returns:
            A new Stream that emits collected tuples on flush.
        """
        ...

    def chunk_by(
        self, func: Callable[[Any], Any], *, include_delimiter: bool = False
    ) -> "Stream":
        """Buffer items until a predicate matches, then emit the buffer as a tuple.

        Each item is buffered. When the predicate returns True for an item,
        the buffer (including the delimiter if `include_delimiter=True`) is
        emitted as a tuple downstream, and the buffer is cleared.

        Any remaining items in the buffer are emitted on `flush()`.

        Args:
            func: Function that returns True for delimiter items.
            include_delimiter: If True, the delimiter item is included
                in the emitted tuple. If False, it is excluded.

        Returns:
            A new Stream emitting tuples of chunked items.
        """
        ...

    def drain(self, *args: Any, **kwargs: Any) -> Optional[Coroutine[Any, Any, Any]]:
        """Recursively flush this node and all downstream nodes.

        Unlike `flush()`, which only flushes this node's buffer,
        `drain()` flushes this node and then recursively drains every
        downstream node. Use this when you want to flush an entire
        pipeline (e.g. at the end of processing).

        Args:
            *args: Ignored (for compatibility with streamz).
            **kwargs: Ignored (for compatibility with streamz).

        Returns:
            A coroutine if async processing is triggered, otherwise None.
        """
        ...

    def flush(self, *args: Any, **kwargs: Any) -> Optional[Coroutine[Any, Any, Any]]:
        """Flush collected elements downstream.

        For `collect()` nodes, emits all buffered elements as a tuple
        and clears the buffer. Has no effect on other node types.

        Args:
            *args: Ignored (for compatibility with streamz).
            **kwargs: Ignored (for compatibility with streamz).

        Returns:
            A coroutine if async processing is triggered, otherwise None.
        """
        ...

    def sink(self, func: Callable[..., Any], *args: Any, stream_name: Optional[str] = None, **kwargs: Any) -> "Stream":
        """Apply a function to each element for side effects.

        Similar to map, but the return value of func is discarded.
        Use this for terminal operations like printing or storing results.

        Args:
            func: Function to call with each element.
            *args: Additional positional arguments passed to func.
            stream_name: Optional custom name for this node.
            **kwargs: Additional keyword arguments passed to func.

        Returns:
            A new Stream (for chaining, though sink typically ends a pipeline).
        """
        ...

    def to_list(self) -> list[Any]:
        """Collect all emitted items into a Python list.

        Creates a sink that appends each item to a list, then returns the list.
        Items are collected as they flow through the stream.

        Returns:
            A Python list containing all emitted items.

        Example:
            >>> s = Stream()
            >>> results = s.map(lambda x: x * 2).to_list()
            >>> s.emit_batch([1, 2, 3])
            >>> results
            [2, 4, 6]
        """
        ...

    def to_set(self) -> set[Any]:
        """Collect all emitted items into a Python set (deduplicates).

        Creates a sink that adds each item to a set, then returns the set.
        Items are collected as they flow through the stream.

        Returns:
            A Python set containing all emitted items.

        Example:
            >>> s = Stream()
            >>> results = s.map(lambda x: x % 3).to_set()
            >>> s.emit_batch([1, 2, 3, 4, 5])
            >>> results
            {0, 1, 2}
        """
        ...

    def collect_context(self) -> "CollectGuard":
        """Return a context manager that auto-flushes the collect buffer on exit.

        This is a convenience wrapper around collect() that automatically
        calls flush() when exiting the context manager.

        Returns:
            A context manager that flushes the collect buffer on exit.

        Example:
            >>> s = Stream()
            >>> with s.collect_context() as c:
            ...     c.sink(print)
            ...     s.emit(1)
            ...     s.emit(2)
            (1, 2)  # printed when context exits
        """
        ...

    def accumulate(
        self,
        func: Callable[[T, Any], T],
        start: T,
        returns_state: bool = False,
    ) -> "Stream":
        """Accumulate values using a function, emitting each intermediate state.

        Similar to functools.reduce, but emits every intermediate result.
        The function receives (current_state, new_element) and returns the new state.

        Args:
            func: Accumulator function (state, element) -> new_state.
                If returns_state=True, function should return (new_state, result).
            start: Initial state value.
            returns_state: If True, func returns (state, result) tuple where state
                is passed to next call and result is emitted. Default False.

        Returns:
            A new Stream emitting accumulated states (or results if returns_state=True).
        """
        ...

    def prefetch(self, n: int) -> "Stream":
        """Enable prefetching for the next map operation.

        When followed by a `.map()`, this causes the map to process up to `n` items
        concurrently while preserving output order. Uses a shared global thread pool,
        making it efficient to combine with `par()` for parallel branch execution.

        Args:
            n: Maximum number of items to process concurrently.

        Returns:
            A new Stream that will modify the next map to use prefetching.
        """
        ...

    def emit(self, x: Any) -> Optional[Coroutine[Any, Any, Any]]:
        """Emit a single value into the stream.

        This is the primary way to push data into a source stream.
        The value propagates through all downstream nodes.

        Args:
            x: The value to emit.

        Returns:
            A coroutine if async processing is triggered, otherwise None.
        """
        ...

    def emit_batch(self, items: List[Any]) -> Optional[Coroutine[Any, Any, Any]]:
        """Emit multiple values into the stream as a batch.

        More efficient than calling `emit()` multiple times when processing
        many items. Values are processed together through the pipeline.

        Args:
            items: List of values to emit.

        Returns:
            A coroutine if async processing is triggered, otherwise None.
        """
        ...

    def start_profiling(self) -> None:
        """Start collecting profiling data for subsequent emit() calls.

        Call `stop_profiling()` to end the session and retrieve the report.
        Profiling adds minimal overhead (~80ns per node per item).

        Raises:
            RuntimeError: If called on a non-source node or if profiling is already active.
        """
        ...

    def stop_profiling(self) -> ProfileReport:
        """Stop profiling and return the collected report.

        Returns:
            A ProfileReport with per-node timing and item flow data.
        """
        ...

    def profile(self) -> ContextManager[Any]:
        """Return a Profiler context manager for this source.

        Usage:
            >>> with source.profile() as p:
            ...     source.emit(42)
            >>> print(p.report)

        Returns:
            A context manager that profiles all emits within the block.
        """
        ...

    def emit_profiled(self, x: Any) -> ProfileReport:
        """Emit a single value and return a profiling report.

        Convenience method combining start_profiling + emit + stop_profiling.

        Args:
            x: The value to emit.

        Returns:
            A ProfileReport for this single emit.
        """
        ...

    def emit_batch_profiled(self, items: List[Any]) -> ProfileReport:
        """Emit a batch of values and return a profiling report.

        Args:
            items: List of values to emit.

        Returns:
            A ProfileReport for this batch emit.
        """
        ...

    def compile(self) -> "Stream":
        """Freeze and optimize the stream graph.

        Triggers topology optimizations (like chain fusion) and prevents
        further modifications to the graph. Call this after building your
        complete pipeline but before emitting data.

        This enables optimizations that are unsafe for dynamic topologies:
        - Consecutive map operations are fused into a single composed map
        - Consecutive filter operations are fused into a single composed filter
        - Prefetch nodes are fused with their following map/filter operations

        After calling compile(), attempting to add new nodes to any part of
        the graph will raise a RuntimeError.

        Returns:
            self (for chaining)

        Raises:
            RuntimeError: If the graph is already compiled/frozen.

        Example:
            ```python
            s = Stream()
            s.map(f1).map(f2).sink(print)
            s.compile()  # f1 and f2 are fused into a single map
            s.emit(1)    # Fast path with optimized graph
            ```
        """
        ...

    def par(self) -> "Stream":
        """Create a parallel branch point that executes all downstreams concurrently.

        When values flow through a parallel node, all downstream branches are
        executed in parallel threads rather than sequentially. This is useful for
        I/O-bound operations where branches can overlap their waiting time.

        Uses a shared global thread pool with `prefetch()`, so combining them has
        minimal overhead. Locks are only applied at convergence points (union/zip)
        for efficient parallel execution.

        Use `seq()` to switch back to sequential execution after parallel branches.

        Returns:
            A new Stream that will propagate values to its downstreams in parallel.

        Example:
            ```python
            s = Stream()
            p = s.par()  # Create parallel branch point
            a = p.map(load_from_disk)   # Branch 1
            b = p.map(fetch_from_api)   # Branch 2
            merged = union(a, b).seq()  # Back to sequential
            s.emit(filename)  # a and b run in parallel
            ```
        """
        ...

    def parallel(self) -> "Stream":
        """Alias for `par()`. Create a parallel branch point."""
        ...

    def seq(self) -> "Stream":
        """Create a sequential branch point (explicit synchronization point).

        This is the default behavior for streams, but can be used explicitly
        after parallel sections to clearly indicate sequential execution resumes.

        Returns:
            A new Stream that will propagate values to its downstreams sequentially.

        Example:
            ```python
            s = Stream()
            p = s.par()
            a = p.map(slow_op1)
            b = p.map(slow_op2)
            merged = union(a, b).seq()  # Explicit sequential point
            merged.map(f1).sink(...)    # Sequential from here
            merged.map(f2).sink(...)
            ```
        """
        ...

    def union(self, *others: "Stream") -> "Stream":
        """Merge multiple streams into one.

        Creates a new stream that emits elements from this stream and all
        other streams. Elements are emitted in the order they arrive.

        Args:
            *others: Other streams to merge with this one.

        Returns:
            A new Stream emitting elements from all input streams.
        """
        ...

    def combine_latest(
        self,
        *others: "Stream",
        emit_on: Optional[Union["Stream", List["Stream"]]] = None,
    ) -> "Stream":
        """Combine the latest values from multiple streams.

        Emits a tuple of the most recent values from each stream whenever
        any stream emits. Only starts emitting after all streams have
        emitted at least one value.

        Args:
            *others: Other streams to combine with this one.
            emit_on: Stream or list of streams that trigger emission.
                If None (default), emit on update from any stream.

        Returns:
            A new Stream emitting tuples of latest values.
        """
        ...

    def zip(self, *others: "Stream") -> "Stream":
        """Zip multiple streams together element-by-element.

        Emits tuples containing one element from each stream. Waits until
        all streams have an element available before emitting.

        Args:
            *others: Other streams to zip with this one.

        Returns:
            A new Stream emitting zipped tuples.
        """
        ...

class CollectGuard:
    """Context manager for collect that auto-flushes on exit.

    Returned by Stream.collect_context(). Use as a context manager
    to automatically flush the collect buffer when exiting.
    """

    def __enter__(self) -> "CollectGuard":
        """Enter the context manager (returns self)."""
        ...

    def __exit__(self, *args: Any) -> None:
        """Exit the context manager and flush the collect buffer."""
        ...

    def sink(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> "CollectGuard":
        """Apply a function to each element for side effects.

        Similar to Stream.sink but returns self for chaining.

        Args:
            func: Function to call with each element.
            *args: Additional positional arguments passed to func.
            **kwargs: Additional keyword arguments passed to func.

        Returns:
            self (for chaining).
        """
        ...

def union(*streams: Stream) -> Stream:
    """Merge multiple streams into one.

    Creates a new stream that emits elements from all input streams.
    Elements are emitted in the order they arrive from any source.

    ```mermaid
    gitGraph
        commit id: " "
        branch s1
        branch s2
        checkout s1
        commit id: "a"
        checkout s2
        commit id: "x"
        checkout s1
        commit id: "b"
        checkout s2
        commit id: "y"
        checkout main
        merge s1
        merge s2 id: "union" type: HIGHLIGHT
    ```

    Args:
        *streams: Two or more streams to merge.

    Returns:
        A new Stream emitting elements from all input streams.

    Example:
        ```python
        s1 = Stream()
        s2 = Stream()
        merged = union(s1, s2)
        ```
    """
    ...

def combine_latest(
    *streams: Stream, emit_on: Optional[Union[Stream, List[Stream]]] = None
) -> Stream:
    """Combine the latest values from multiple streams.

    Creates a new stream that emits a tuple of the most recent values
    from each input stream whenever any stream emits. Only starts
    emitting after all streams have emitted at least one value.

    ```mermaid
    gitGraph
        commit id: " "
        branch s1
        branch s2
        checkout s1
        commit id: "a"
        checkout s2
        commit id: "1"
        checkout main
        merge s1
        merge s2 id: "(a,1)" type: HIGHLIGHT
        checkout s1
        commit id: "b"
        checkout main
        merge s1 id: "(b,1)" type: HIGHLIGHT
        checkout s2
        commit id: "2"
        checkout main
        merge s2 id: "(b,2)" type: HIGHLIGHT
    ```

    Args:
        *streams: Two or more streams to combine.
        emit_on: Stream or list of streams that trigger emission.
            If None (default), emit on update from any stream.

    Returns:
        A new Stream emitting tuples of latest values.

    Example:
        ```python
        s1 = Stream()
        s2 = Stream()
        combined = combine_latest(s1, s2)
        combined_on_s1 = combine_latest(s1, s2, emit_on=s1)
        ```
    """
    ...

def zip(*streams: Stream) -> Stream:
    """Zip multiple streams together.

    Creates a new stream that waits for one item from each input stream,
    then emits them as a tuple. Buffers items until all streams have
    contributed one value.

    ```mermaid
    gitGraph
        commit id: " "
        branch s1
        branch s2
        checkout s1
        commit id: "a"
        checkout s2
        commit id: "1"
        checkout main
        merge s1
        merge s2 id: "(a,1)" type: HIGHLIGHT
        checkout s1
        commit id: "b"
        commit id: "c"
        checkout s2
        commit id: "2"
        checkout main
        merge s1
        merge s2 id: "(b,2)" type: HIGHLIGHT
    ```

    Args:
        *streams: Two or more streams to zip together.

    Returns:
        A new stream that emits tuples of values from all input streams.
    """
    ...

def to_text_file(
    stream: Stream,
    filename: str,
    mode: str = "a",
    encoding: str = "utf-8",
) -> Stream:
    """Write stream elements to a text file.

    Each element is converted to a string and written as a line.

    Args:
        stream: Source stream.
        filename: Path to output file.
        mode: File mode ('w' for write, 'a' for append). Default 'a'.
        encoding: File encoding. Default 'utf-8'.

    Returns:
        A new Stream (for chaining).
    """
    ...

def from_text_file(filename: str, encoding: str = "utf-8") -> Stream:
    """Create a stream that reads lines from a text file.

    Args:
        filename: Path to input file.
        encoding: File encoding. Default 'utf-8'.

    Returns:
        A new Stream. Call emit(None) to trigger reading the file.
    """
    ...
