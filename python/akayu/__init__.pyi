"""High-performance reactive streams for Python."""

from collections.abc import Callable, Iterable, Iterator, Sequence
from contextlib import AbstractContextManager
from typing import (
    Any,
    Self,
    TypeVar,
    Union,
    final,
    overload,
)

from typing_extensions import disjoint_base

_T = TypeVar("_T")
_U = TypeVar("_U")

__version__: str

_StreamOrMultiStream = Union["Stream", "MultiStream"]

@final
class ProfileReport:
    """Profiling report for a pipeline execution.

    Contains per-node timing and item flow data, preserving the graph
    structure for tree-view display.
    """

    def print(self, sort_by: str | None = None) -> None:
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

@disjoint_base
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

    def __new__(cls, name: str | None = None) -> Self:
        """Create a new source Stream.

        Args:
            name: Optional name for the stream (defaults to "source").
        """
        ...

    def __repr__(self) -> str:
        """Return string representation of the stream."""
        ...

    @property
    def compiled(self) -> bool:
        """True if the stream graph is compiled (frozen)."""
        ...

    def map(self, func: Callable[..., _T], *args: Any, stream_name: str | None = None, **kwargs: Any) -> Stream:
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
        self, predicate: Callable[..., bool], *args: Any, stream_name: str | None = None, **kwargs: Any
    ) -> Stream:
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

    def starmap(self, func: Callable[..., _T], *args: Any, stream_name: str | None = None, **kwargs: Any) -> Stream:
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
        self, func: Callable[[list[Any]], Iterable[_T]], *args: Any, stream_name: str | None = None, **kwargs: Any
    ) -> Stream:
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

    def flatten(self, *, stream_name: str | None = None) -> Stream:
        """Flatten an iterable element into individual elements.

        Each element is expected to be iterable. The items within each
        element are emitted individually downstream.

        Returns:
            A new Stream emitting the flattened elements.
        """
        ...

    def collect(self, *, stream_name: str | None = None) -> Stream:
        """Collect elements into a buffer until `flush()` is called.

        Elements are accumulated in an internal buffer. When `flush()` is
        called, all collected elements are emitted as a single tuple.

        Returns:
            A new Stream that emits collected tuples on flush.
        """
        ...

    def chunk_by(
        self, predicate: Callable[[Any], Any], *, include_delimiter: bool = False, stream_name: str | None = None
    ) -> Stream:
        """Buffer items until a predicate matches, then emit the buffer as a tuple.

        Each item is buffered. When the predicate returns True for an item,
        the buffer (including the delimiter if `include_delimiter=True`) is
        emitted as a tuple downstream, and the buffer is cleared.

        Any remaining items in the buffer are emitted on `flush()`.

        Args:
            predicate: Function that returns True for delimiter items.
            include_delimiter: If True, the delimiter item is included
                in the emitted tuple. If False, it is excluded.

        Returns:
            A new Stream emitting tuples of chunked items.
        """
        ...

    def chunks_between(
        self, open: Callable[[Any], Any], close: Callable[[Any], Any], *, stream_name: str | None = None
    ) -> Stream:
        """Emit the items between an opening and a closing delimiter as a tuple.

        Outside a chunk only `open` is called; inside one only `close` is.
        Items are buffered until `close` matches, then emitted as a tuple.
        The delimiters themselves are not included, and empty chunks are not
        emitted. The same predicate can serve as both delimiters.

        `flush()`/`drain()` discard an incomplete chunk, return to the
        outside state, and continue on to downstream nodes.

        Args:
            open: Predicate that returns True for the opening delimiter.
            close: Predicate that returns True for the closing delimiter.
            stream_name: Optional custom name for this node.

        Returns:
            A new Stream emitting tuples of the items between delimiters.

        Example:
            ```python
            source = Stream()
            out = []
            source.chunks_between(lambda c: c == "[", lambda c: c == "]").sink(out.append)
            for ch in "a[bc]d[e]":
                source.emit(ch)
            out  # [('b', 'c'), ('e',)]
            ```
        """
        ...

    def drain(self, *args: Any, **kwargs: Any) -> None:
        """Recursively flush this node and all downstream nodes.

        Unlike `flush()`, which only flushes this node's buffer,
        `drain()` flushes this node and then recursively drains every
        downstream node. Use this when you want to flush an entire
        pipeline (e.g. at the end of processing).

        Args:
            *args: Ignored (for compatibility with streamz).
            **kwargs: Ignored (for compatibility with streamz).
        """
        ...

    def flush(self, *args: Any, **kwargs: Any) -> None:
        """Flush buffered elements downstream.

        `collect()` and `chunk_by()` nodes emit their buffer as a tuple and
        stop there; `chunks_between()` discards an incomplete chunk; prefetch
        nodes wait for items still in flight. Other nodes pass the flush on,
        so `source.flush()` reaches the first buffering node on every path.
        Use `drain()` to flush every node in the graph.

        Args:
            *args: Ignored (for compatibility with streamz).
            **kwargs: Ignored (for compatibility with streamz).
        """
        ...

    def sink(self, func: Callable[..., Any], *args: Any, stream_name: str | None = None, **kwargs: Any) -> Stream:
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

    def accumulate(
        self,
        func: Callable[[_T, Any], _T],
        start: _T,
        *,
        returns_state: bool = False,
        stream_name: str | None = None,
    ) -> Stream:
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

    def prefetch(self, n: int, *, stream_name: str | None = None) -> Stream:
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

    def emit(self, x: Any) -> None:
        """Emit a single value into the stream.

        This is the primary way to push data into a source stream.
        The value propagates through all downstream nodes.

        Args:
            x: The value to emit.
        """
        ...

    def emit_batch(self, items: list[Any]) -> None:
        """Emit multiple values into the stream as a batch.

        More efficient than calling `emit()` multiple times when processing
        many items. Values are processed together through the pipeline.

        Args:
            items: List of values to emit.
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

    def profile(self) -> AbstractContextManager[Any]:
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

    def emit_batch_profiled(self, items: list[Any]) -> ProfileReport:
        """Emit a batch of values and return a profiling report.

        Args:
            items: List of values to emit.

        Returns:
            A ProfileReport for this batch emit.
        """
        ...

    def compile(self) -> Stream:
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

    def par(self) -> Stream:
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

    def parallel(self) -> Stream:
        """Alias for `par()`. Create a parallel branch point."""
        ...

    def seq(self) -> Stream:
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

    def union(self, *others: Stream) -> Stream:
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
        *others: Stream,
        emit_on: Stream | list[Stream] | None = None,
    ) -> Stream:
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

    def zip(self, *others: Stream) -> Stream:
        """Zip multiple streams together element-by-element.

        Emits tuples containing one element from each stream. Waits until
        all streams have an element available before emitting.

        Args:
            *others: Other streams to zip with this one.

        Returns:
            A new Stream emitting zipped tuples.
        """
        ...

class MultiStream:
    """A batch of Stream instances with element-wise operations.

    Applies operations to each stream in the batch. Supports broadcasting
    for combinator operations (union, combine_latest, zip) where a single
    Stream or length-1 MultiStream is expanded to match the batch size.

    Example:
        ```python
        sources = [Stream() for _ in range(3)]
        results = MultiStream(sources).map(lambda x: x * 2).to_list()
        ```
    """

    @overload
    def __init__(self, streams: Sequence[Stream]) -> None: ...
    @overload
    def __init__(self, streams: dict[str, Stream]) -> None:
        """Create a MultiStream batch from a sequence or dict of Stream instances.

        Args:
            streams: List, tuple, or dict of Stream instances.

        Raises:
            TypeError: If any element is not a Stream.
        """
        ...

    @property
    def keyed(self) -> bool:
        """True if constructed with a dict, False if constructed with a list."""
        ...

    def __len__(self) -> int:
        """Return the number of streams in the batch."""
        ...

    def __iter__(self) -> Iterator[Stream]:
        """Iterate over the individual Stream instances."""
        ...

    @overload
    def __getitem__(self, key: int) -> Stream: ...
    @overload
    def __getitem__(self, key: slice) -> MultiStream: ...
    @overload
    def __getitem__(self, key: str) -> Stream:
        """Get stream(s) by index, slice, or key.

        In list mode: supports int and slice only. str raises TypeError.
        In dict mode: supports str (returns Stream, raises KeyError if missing) and int/slice.

        Args:
            key: int index, slice, or str key (dict mode only).

        Returns:
            Stream for single int/str key, MultiStream for slice.

        Raises:
            KeyError: If key not found in dict mode.
            TypeError: If str key used in list mode, or wrong key type.
        """
        ...

    def __setitem__(self, key: int | str, value: Stream | MultiStream) -> None:
        """Replace the stream at an index or key.

        In dict mode, assigning to a new str key appends it.

        Raises:
            TypeError: If value is not a Stream or MultiStream, or a str key
                is used in list mode.
        """
        ...

    def keys(self) -> Iterator[str]:
        """Iterate over keys in dict mode.

        Raises:
            TypeError: If in list mode.
        """
        ...

    def values(self) -> Iterator[Stream]:
        """Iterate over stream values in dict mode.

        Raises:
            TypeError: If in list mode.
        """
        ...

    def items(self) -> Iterator[tuple[str, Stream]]:
        """Iterate over (key, stream) pairs in dict mode.

        Raises:
            TypeError: If in list mode.
        """
        ...

    def to_dict(self) -> dict[str, Stream]:
        """Convert to dict of streams in dict mode.

        Raises:
            TypeError: If in list mode.
        """
        ...

    def __repr__(self) -> str:
        """Return string representation."""
        ...

    def map(self, func: Callable[..., _T], *args: Any, stream_name: str | None = None, **kwargs: Any) -> MultiStream:
        """Apply a map to each stream in the batch."""
        ...

    def filter(
        self,
        predicate: Callable[..., bool],
        *args: Any,
        stream_name: str | None = None,
        **kwargs: Any,
    ) -> MultiStream:
        """Apply a filter to each stream in the batch."""
        ...

    def sink(self, func: Callable[..., Any], *args: Any, stream_name: str | None = None, **kwargs: Any) -> MultiStream:
        """Apply a sink to each stream in the batch."""
        ...

    def starmap(
        self, func: Callable[..., _T], *args: Any, stream_name: str | None = None, **kwargs: Any
    ) -> MultiStream:
        """Apply a starmap to each stream in the batch."""
        ...

    def accumulate(
        self, func: Callable[[_T, Any], _T], start: _T, *, returns_state: bool = False, stream_name: str | None = None
    ) -> MultiStream:
        """Apply an accumulate to each stream in the batch."""
        ...

    def flatten(self, *, stream_name: str | None = None) -> MultiStream:
        """Flatten each stream in the batch."""
        ...

    def collect(self, *, stream_name: str | None = None) -> MultiStream:
        """Apply collect to each stream in the batch."""
        ...

    def batch_map(
        self,
        func: Callable[[list[Any]], Iterable[_T]],
        *args: Any,
        stream_name: str | None = None,
        **kwargs: Any,
    ) -> MultiStream:
        """Apply a batch_map to each stream in the batch."""
        ...

    def chunk_by(
        self,
        predicate: Callable[[Any], Any],
        *,
        include_delimiter: bool = False,
        stream_name: str | None = None,
        **kwargs: Any,
    ) -> MultiStream:
        """Apply chunk_by to each stream in the batch."""
        ...

    def chunks_between(
        self, open: Callable[[Any], Any], close: Callable[[Any], Any], *, stream_name: str | None = None, **kwargs: Any
    ) -> MultiStream:
        """Apply chunks_between to each stream in the batch."""
        ...

    def to_list(self) -> list[list[Any]] | dict[str, list[Any]]:
        """Collect each stream into a list.

        In list mode: returns list of lists.
        In dict mode: returns dict of lists.
        """
        ...

    def to_set(self) -> list[set[Any]] | dict[str, set[Any]]:
        """Collect each stream into a set.

        In list mode: returns list of sets.
        In dict mode: returns dict of sets.
        """
        ...

    def flush(self, *args: Any, **kwargs: Any) -> None:
        """Flush all streams in the batch."""
        ...

    def drain(self, *args: Any, **kwargs: Any) -> None:
        """Drain all streams in the batch."""
        ...

    def compile(self) -> MultiStream:
        """Compile all streams in the batch. Returns self for chaining."""
        ...

    def par(self, **kwargs: Any) -> MultiStream:
        """Apply par() to each stream in the batch."""
        ...

    def parallel(self, **kwargs: Any) -> MultiStream:
        """Alias for `par()`."""
        ...

    def seq(self, **kwargs: Any) -> MultiStream:
        """Apply seq() to each stream in the batch."""
        ...

    def prefetch(self, n: int, **kwargs: Any) -> MultiStream:
        """Apply prefetch(n) to each stream in the batch."""
        ...

    def profile(self) -> list[AbstractContextManager[Any]] | dict[str, AbstractContextManager[Any]]:
        """Return a profiler for each stream.

        In list mode: returns a list of profilers.
        In dict mode: returns a dict of profilers.
        """
        ...

    def union(self, *others: _StreamOrMultiStream) -> MultiStream:
        """Pairwise union with broadcasting.

        Args:
            *others: Stream or MultiStream instances to union with.

        Returns:
            Always MultiStream (instance methods always return MultiStream).
        """
        ...

    def combine_latest(
        self,
        *others: _StreamOrMultiStream,
        emit_on: _StreamOrMultiStream | None = None,
    ) -> MultiStream:
        """Pairwise combine_latest with broadcasting.

        Args:
            *others: Stream or MultiStream instances to combine with.
            emit_on: Stream or MultiStream that triggers emission.

        Returns:
            Always MultiStream (instance methods always return MultiStream).
        """
        ...

    def zip(self, *others: _StreamOrMultiStream) -> MultiStream:
        """Pairwise zip with broadcasting.

        Args:
            *others: Stream or MultiStream instances to zip with.

        Returns:
            Always MultiStream (instance methods always return MultiStream).
        """
        ...

@overload
def union(*streams: Stream) -> Stream: ...  # type: ignore[overload-overlap]
@overload
def union(*streams: _StreamOrMultiStream) -> MultiStream:
    """Merge multiple streams into one with broadcasting support.

    Requires at least two streams.

    Return type is deterministic from input types:
    - All Stream inputs: returns Stream.
    - Any MultiStream input: returns MultiStream.

    Args:
        *streams: Stream or MultiStream instances to merge.

    Returns:
        Stream if all inputs are Stream, otherwise MultiStream.
    """
    ...

@overload
def combine_latest(*streams: Stream, emit_on: Stream | list[Stream] | None = None) -> Stream: ...  # type: ignore[overload-overlap]
@overload
def combine_latest(
    *streams: _StreamOrMultiStream,
    emit_on: _StreamOrMultiStream | list[_StreamOrMultiStream] | None = None,
) -> MultiStream:
    """Combine the latest values from multiple streams with broadcasting support.

    Requires at least two streams.

    Return type is deterministic from input types:
    - All Stream inputs (and Stream/no emit_on): returns Stream.
    - Any MultiStream input: returns MultiStream.

    Args:
        *streams: Stream or MultiStream instances to combine.
        emit_on: Stream, MultiStream, or list thereof that triggers emission.
            If None (default), emit on update from any stream.

    Returns:
        Stream if all inputs are Stream, otherwise MultiStream.
    """
    ...

@overload
def zip(*streams: Stream) -> Stream: ...  # type: ignore[overload-overlap]
@overload
def zip(*streams: _StreamOrMultiStream) -> MultiStream:
    """Zip multiple streams together with broadcasting support.

    Requires at least two streams.

    Return type is deterministic from input types:
    - All Stream inputs: returns Stream.
    - Any MultiStream input: returns MultiStream.

    Args:
        *streams: Stream or MultiStream instances to zip.

    Returns:
        Stream if all inputs are Stream, otherwise MultiStream.
    """
    ...

def to_text_file(data: Any, path: str) -> None:
    """Append `str(data)` and a newline to a file, creating it if needed.

    Meant to be used as a sink::

        source.sink(akayu.to_text_file, path="output.txt")

    Args:
        data: The item to write.
        path: The file to append to.
    """
    ...

class TextFileSource(Stream):
    """A source Stream that emits the lines of a text file.

    Created by `from_text_file()`. Build the pipeline first, then call
    `start()`: lines are read in a background thread and emitted in order,
    without their line endings. `join()` waits for the last line and
    re-raises any error from reading the file or from the pipeline.
    """

    path: str
    interval: float | None

    def __new__(cls, path: str, interval: float | None = None) -> Self: ...
    def __init__(self, path: str, interval: float | None = None) -> None: ...
    def start(self) -> Self:
        """Start reading the file in a background thread. Returns self."""
        ...

    def join(self, timeout: float | None = None) -> None:
        """Wait until every line has been emitted, re-raising any error."""
        ...

def from_text_file(path: str, interval: float | None = None) -> TextFileSource:
    """Create a source Stream that emits the lines of a text file.

    Nothing is read until `start()` is called, so build the pipeline first::

        source = akayu.from_text_file("data.txt")
        source.map(str.upper).sink(print)
        source.start().join()

    Args:
        path: The file to read (UTF-8).
        interval: Optional delay in seconds between emitting lines. The file
            is read once; lines appended later are not picked up.
    """
    ...

__all__ = [
    "MultiStream",
    "ProfileReport",
    "Stream",
    "TextFileSource",
    "combine_latest",
    "from_text_file",
    "to_text_file",
    "union",
    "zip",
]
