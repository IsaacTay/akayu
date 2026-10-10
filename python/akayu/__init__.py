import builtins as _builtins
import threading as _threading
import time as _time
from importlib.metadata import version as _version
from pathlib import Path as _Path

from akayu.akayu import (
    ProfileReport,
    to_text_file,
)
from akayu.akayu import Stream as _Stream
from akayu.akayu import (
    combine_latest as _rust_combine_latest,
)
from akayu.akayu import (
    union as _rust_union,
)
from akayu.akayu import (
    zip as _rust_zip,
)

Stream = _Stream

__version__ = _version("akayu")

_UNSUPPORTED = frozenset(
    {
        "emit",
        "emit_batch",
        "start_profiling",
        "stop_profiling",
        "emit_profiled",
        "emit_batch_profiled",
    }
)


def _as_streams(obj, label=None):
    if isinstance(obj, Stream):
        return MultiStream([obj])
    if isinstance(obj, MultiStream):
        return obj
    ctx = f" (argument '{label}')" if label else ""
    raise TypeError(
        f"Expected Stream or MultiStream{ctx}, got {type(obj).__name__}. "
        f"Wrap a single Stream in MultiStream([s]) if you want to broadcast it."
    )


def _broadcast(*streams_args):
    lengths = [len(s) for s in streams_args]
    if max(lengths) == 0:
        raise ValueError(
            "Cannot broadcast: all MultiStream arguments are empty. Provide at least one non-empty MultiStream."
        )
    target = max(lengths)
    for i, length in enumerate(lengths):
        if length != 1 and length != target:
            raise ValueError(
                f"Cannot broadcast: argument {i} has {length} streams "
                f"but others have {target}. "
                f"All MultiStream arguments must have the same length, "
                f"or length 1 (which expands to match)."
            )
    return [s._expand(target) for s in streams_args]


def _combine_batch(broadcasted, combinator, **extra):
    streams_per_row = _builtins.zip(*[b._streams for b in broadcasted], strict=True)
    if "emit_on_streams" in extra:
        return [
            combinator(*row, emit_on=eo)
            for row, eo in _builtins.zip(streams_per_row, extra["emit_on_streams"], strict=True)
        ]
    return [combinator(*row) for row in streams_per_row]


def _wrap_results(keys, results):
    if keys is not None:
        return dict(_builtins.zip(keys, results, strict=True))
    return results


def _check_keys(ms_list):
    keyed = [(i, ms) for i, ms in enumerate(ms_list) if ms._keys is not None]
    if not keyed:
        return None
    first_i, first = keyed[0]
    ref_keys = first._keys
    for i, ms in keyed[1:]:
        if ms._keys != ref_keys:
            raise ValueError(
                f"Key mismatch in combinator: argument {first_i} has keys "
                f"{_builtins.tuple(ref_keys)} but argument {i} has keys "
                f"{_builtins.tuple(ms._keys)}. All dict-backed MultiStreams "
                f"must have identical keys."
            )
    non_broadcast = [(i, ms) for i, ms in enumerate(ms_list) if ms._keys is None and len(ms) != 1]
    if non_broadcast and keyed:
        i, ms = non_broadcast[0]
        raise TypeError(
            f"Cannot combine dict-backed MultiStream (argument {first_i}) with "
            f"list-backed MultiStream (argument {i}). Use matching keys or a "
            f"single Stream for broadcasting."
        )
    return ref_keys


class MultiStream:
    __slots__ = ("_keys", "_streams")

    def __init__(self, streams):
        if isinstance(streams, dict):
            keys = []
            stream_list = []
            for k, v in streams.items():
                if not isinstance(v, (Stream, MultiStream)):
                    raise TypeError(
                        f"Value for key '{k}' is not a Stream or MultiStream (got {type(v).__name__}). "
                        f"All values must be Stream or MultiStream instances."
                    )
                keys.append(k)
                stream_list.append(v)
            self._keys = tuple(keys)
            self._streams = stream_list
        elif isinstance(streams, (list, tuple)):
            for i, s in enumerate(streams):
                if not isinstance(s, (Stream, MultiStream)):
                    raise TypeError(
                        f"Element {i} is not a Stream or MultiStream (got {type(s).__name__}). "
                        f"All elements must be Stream or MultiStream instances."
                    )
            self._keys = None
            self._streams = list(streams)
        else:
            raise TypeError(
                f"MultiStream expects a dict, list, or tuple of Stream instances, "
                f"got {type(streams).__name__}. "
                f"Usage: MultiStream([s1, s2, ...]) or MultiStream({{'a': s1, 'b': s2}})"
            )

    @property
    def keyed(self):
        return self._keys is not None

    def __len__(self):
        return len(self._streams)

    def __iter__(self):
        return iter(self._streams)

    def __getitem__(self, key):
        if isinstance(key, str):
            if self._keys is None:
                raise TypeError(
                    f"Cannot use string key '{key}' on a list-backed MultiStream. "
                    f"Use integer indices or create with a dict."
                )
            try:
                idx = self._keys.index(key)
            except ValueError:
                raise KeyError(key) from None
            return self._streams[idx]
        result = self._streams[key]
        if isinstance(result, list):
            if self._keys is not None:
                start = key.start if key.start is not None else 0
                stop = key.stop if key.stop is not None else len(self._streams)
                step = key.step if key.step is not None else 1
                indices = list(range(start, stop, step))
                sub_keys = tuple(self._keys[i] for i in indices)
                ms = MultiStream.__new__(MultiStream)
                ms._streams = result
                ms._keys = sub_keys
                return ms
            return MultiStream(result)
        return result

    def __setitem__(self, key, value):
        if not isinstance(value, (Stream, MultiStream)):
            raise TypeError(f"Cannot assign {type(value).__name__} to MultiStream. Expected Stream or MultiStream.")
        if isinstance(key, str):
            if self._keys is None:
                raise TypeError(f"Cannot use string key '{key}' on a list-backed MultiStream.")
            try:
                idx = self._keys.index(key)
            except ValueError:
                self._keys = (*self._keys, key)
                self._streams.append(value)
            else:
                self._streams[idx] = value
        else:
            self._streams[key] = value

    def __repr__(self):
        if self._keys is not None:
            return f"MultiStream({{{', '.join(repr(k) for k in self._keys)}}})"
        return f"MultiStream({len(self._streams)})"

    def _expand(self, target_len):
        if len(self._streams) == target_len:
            return self
        if len(self._streams) == 1:
            ms = MultiStream.__new__(MultiStream)
            ms._streams = list(self._streams) * target_len
            ms._keys = None
            return ms
        raise ValueError(
            f"Cannot expand MultiStream({len(self._streams)}) to length {target_len}. "
            f"Only MultiStream(1) can be broadcast to a larger size."
        )

    def _apply(self, method, *args, **kwargs):
        name = kwargs.pop("stream_name", None)
        streams = []
        for i, s in enumerate(self._streams):
            if name is not None:
                kwargs["stream_name"] = f"{name}_{i}"
            streams.append(getattr(s, method)(*args, **kwargs))
        ms = MultiStream.__new__(MultiStream)
        ms._streams = streams
        ms._keys = self._keys
        return ms

    def __getattr__(self, name):
        if name in _UNSUPPORTED:
            raise AttributeError(
                f"MultiStream does not support '{name}'. "
                f"This operation only makes sense on a single Stream — "
                f"iterate over the MultiStream and call it on each Stream individually."
            )
        raise AttributeError(
            f"MultiStream has no attribute '{name}'. "
            f"Supported: transforms (map, filter, sink, ...), "
            f"collectors (to_list, to_set), "
            f"actions (flush, drain, compile), "
            f"combinators (union, combine_latest, zip)."
        )

    # --- dict-like accessors ---

    def keys(self):
        if self._keys is None:
            raise TypeError("keys() is only available on dict-backed MultiStream.")
        return iter(self._keys)

    def values(self):
        return iter(self._streams)

    def items(self):
        if self._keys is None:
            raise TypeError("items() is only available on dict-backed MultiStream.")
        return _builtins.zip(self._keys, self._streams, strict=True)

    def to_dict(self):
        if self._keys is None:
            raise TypeError("to_dict() is only available on dict-backed MultiStream.")
        return dict(_builtins.zip(self._keys, self._streams, strict=True))

    # --- transforms (return MultiStream, preserving keys) ---

    def map(self, func, *args, **kwargs):
        return self._apply("map", func, *args, **kwargs)

    def filter(self, predicate, *args, **kwargs):
        return self._apply("filter", predicate, *args, **kwargs)

    def sink(self, func, *args, **kwargs):
        return self._apply("sink", func, *args, **kwargs)

    def starmap(self, func, *args, **kwargs):
        return self._apply("starmap", func, *args, **kwargs)

    def accumulate(self, func, start, *, returns_state=False, stream_name=None):
        ms = MultiStream.__new__(MultiStream)
        ms._streams = [
            s.accumulate(func, start, returns_state=returns_state, stream_name=stream_name) for s in self._streams
        ]
        ms._keys = self._keys
        return ms

    def flatten(self, *, stream_name=None):
        ms = MultiStream.__new__(MultiStream)
        ms._streams = [s.flatten(stream_name=stream_name) for s in self._streams]
        ms._keys = self._keys
        return ms

    def collect(self, *, stream_name=None):
        ms = MultiStream.__new__(MultiStream)
        ms._streams = [s.collect(stream_name=stream_name) for s in self._streams]
        ms._keys = self._keys
        return ms

    def batch_map(self, func, *args, **kwargs):
        return self._apply("batch_map", func, *args, **kwargs)

    def chunk_by(self, predicate, *, include_delimiter=False, stream_name=None, **kwargs):
        ms = MultiStream.__new__(MultiStream)
        ms._streams = [
            s.chunk_by(predicate, include_delimiter=include_delimiter, stream_name=stream_name, **kwargs)
            for s in self._streams
        ]
        ms._keys = self._keys
        return ms

    def chunks_between(self, open, close, *, stream_name=None, **kwargs):
        ms = MultiStream.__new__(MultiStream)
        ms._streams = [s.chunks_between(open, close, stream_name=stream_name, **kwargs) for s in self._streams]
        ms._keys = self._keys
        return ms

    # --- collectors (return list or dict depending on mode) ---

    def to_list(self):
        results = [s.to_list() for s in self._streams]
        return _wrap_results(self._keys, results)

    def to_set(self):
        results = [s.to_set() for s in self._streams]
        return _wrap_results(self._keys, results)

    # --- actions (call on each) ---

    def flush(self, *args, **kwargs):
        for s in self._streams:
            s.flush(*args, **kwargs)

    def drain(self, *args, **kwargs):
        for s in self._streams:
            s.drain(*args, **kwargs)

    def compile(self):
        for s in self._streams:
            s.compile()
        return self

    def profile(self):
        profilers = [s.profile() for s in self._streams]
        if self._keys is not None:
            return dict(_builtins.zip(self._keys, profilers, strict=True))
        return profilers

    def par(self, **kwargs):
        return self._apply("par", **kwargs)

    def parallel(self, **kwargs):
        return self._apply("parallel", **kwargs)

    def seq(self, **kwargs):
        return self._apply("seq", **kwargs)

    def prefetch(self, n, **kwargs):
        return self._apply("prefetch", n, **kwargs)

    # --- broadcasting combinators (lose keys, return list-backed) ---

    def union(self, *others):
        if not others:
            raise ValueError(
                "union() needs at least one other stream to merge with. Usage: ms.union(other) or ms.union(a, b, ...)"
            )
        if any(isinstance(s, MultiStream) for s in self._streams):
            raise TypeError(
                "union() on nested MultiStream is not yet supported. "
                "Iterate and call union on each inner MultiStream individually."
            )
        args = [_as_streams(self)] + [_as_streams(o) for o in others]
        keys = _check_keys(args)
        if keys is not None:
            expanded = [ms._expand(len(keys))._streams if len(ms) == 1 else ms._streams for ms in args]
            results = [_rust_union(*_builtins.tuple(s[i] for s in expanded)) for i in range(len(keys))]
            ms = MultiStream.__new__(MultiStream)
            ms._streams = results
            ms._keys = keys
            return ms
        broadcasted = _broadcast(*args)
        results = [_rust_union(*pair) for pair in _builtins.zip(*[b._streams for b in broadcasted], strict=True)]
        return MultiStream(results)

    def combine_latest(self, *others, emit_on=None):
        if not others:
            raise ValueError(
                "combine_latest() needs at least one other stream. "
                "Usage: ms.combine_latest(other) or ms.combine_latest(a, b, emit_on=c)"
            )
        if any(isinstance(s, MultiStream) for s in self._streams):
            raise TypeError(
                "combine_latest() on nested MultiStream is not yet supported. "
                "Iterate and call combine_latest on each inner MultiStream individually."
            )
        args = [_as_streams(self)] + [_as_streams(o) for o in others]
        keys = _check_keys(args)
        if keys is not None:
            expanded = [ms._expand(len(keys))._streams if len(ms) == 1 else ms._streams for ms in args]
            if emit_on is not None:
                emit_on_s = _as_streams(emit_on, label="emit_on")
                if emit_on_s._keys is not None and emit_on_s._keys != keys:
                    raise ValueError(
                        f"Key mismatch for emit_on: arguments have keys "
                        f"{_builtins.tuple(keys)} but emit_on has keys "
                        f"{_builtins.tuple(emit_on_s._keys)}."
                    )
                if emit_on_s._keys is None and len(emit_on_s) != 1:
                    raise TypeError("Cannot use list-backed MultiStream as emit_on with dict-backed arguments.")
                emit_on_streams = emit_on_s._streams * len(keys) if len(emit_on_s) == 1 else emit_on_s._streams
            else:
                emit_on_streams = [None] * len(keys)
            results = []
            for i in range(len(keys)):
                row = _builtins.tuple(s[i] for s in expanded)
                results.append(_rust_combine_latest(*row, emit_on=emit_on_streams[i]))
            ms = MultiStream.__new__(MultiStream)
            ms._streams = results
            ms._keys = keys
            return ms
        broadcasted = _broadcast(*args)
        if emit_on is not None:
            emit_on_s = _as_streams(emit_on, label="emit_on")
            emit_on_expanded = emit_on_s._expand(len(broadcasted[0]))
            emit_on_streams = emit_on_expanded._streams
        else:
            emit_on_streams = [None] * len(broadcasted[0])
        results = []
        for pair, eo in _builtins.zip(
            _builtins.zip(*[b._streams for b in broadcasted], strict=True), emit_on_streams, strict=True
        ):
            results.append(_rust_combine_latest(*pair, emit_on=eo))
        return MultiStream(results)

    def zip(self, *others):
        if not others:
            raise ValueError(
                "zip() needs at least one other stream to pair with. Usage: ms.zip(other) or ms.zip(a, b, ...)"
            )
        if any(isinstance(s, MultiStream) for s in self._streams):
            raise TypeError(
                "zip() on nested MultiStream is not yet supported. "
                "Iterate and call zip on each inner MultiStream individually."
            )
        args = [_as_streams(self)] + [_as_streams(o) for o in others]
        keys = _check_keys(args)
        if keys is not None:
            expanded = [ms._expand(len(keys))._streams if len(ms) == 1 else ms._streams for ms in args]
            results = [_rust_zip(*_builtins.tuple(s[i] for s in expanded)) for i in range(len(keys))]
            ms = MultiStream.__new__(MultiStream)
            ms._streams = results
            ms._keys = keys
            return ms
        broadcasted = _broadcast(*args)
        results = [_rust_zip(*pair) for pair in _builtins.zip(*[b._streams for b in broadcasted], strict=True)]
        return MultiStream(results)


def union(*args):
    if len(args) < 2:
        raise ValueError("akayu.union() needs at least two streams to merge. Usage: union(a, b) or union(a, b, c, ...)")
    if all(isinstance(a, Stream) for a in args):
        return _rust_union(*args)
    converted = [_as_streams(a) for a in args]
    keys = _check_keys(converted)
    if keys is not None:
        expanded = [ms._expand(len(keys))._streams if len(ms) == 1 else ms._streams for ms in converted]
        results = [_rust_union(*_builtins.tuple(s[i] for s in expanded)) for i in range(len(keys))]
        ms = MultiStream.__new__(MultiStream)
        ms._streams = results
        ms._keys = keys
        return ms
    broadcasted = _broadcast(*converted)
    results = [_rust_union(*pair) for pair in _builtins.zip(*[b._streams for b in broadcasted], strict=True)]
    return MultiStream(results)


def combine_latest(*args, emit_on=None):
    if len(args) < 2:
        raise ValueError(
            "akayu.combine_latest() needs at least two streams. "
            "Usage: combine_latest(a, b) or combine_latest(a, b, emit_on=c)"
        )
    if all(isinstance(a, Stream) for a in args) and (
        emit_on is None
        or isinstance(emit_on, Stream)
        or (isinstance(emit_on, list) and all(isinstance(s, Stream) for s in emit_on))
    ):
        return _rust_combine_latest(*args, emit_on=emit_on)
    converted = [_as_streams(a) for a in args]
    keys = _check_keys(converted)
    if keys is not None:
        expanded = [ms._expand(len(keys))._streams if len(ms) == 1 else ms._streams for ms in converted]
        if emit_on is not None:
            emit_on_s = _as_streams(emit_on, label="emit_on")
            if emit_on_s._keys is not None and emit_on_s._keys != keys:
                raise ValueError(
                    f"Key mismatch for emit_on: arguments have keys "
                    f"{_builtins.tuple(keys)} but emit_on has keys "
                    f"{_builtins.tuple(emit_on_s._keys)}."
                )
            if emit_on_s._keys is None and len(emit_on_s) != 1:
                raise TypeError("Cannot use list-backed MultiStream as emit_on with dict-backed arguments.")
            emit_on_streams = emit_on_s._streams * len(keys) if len(emit_on_s) == 1 else emit_on_s._streams
        else:
            emit_on_streams = [None] * len(keys)
        results = []
        for i in range(len(keys)):
            row = _builtins.tuple(s[i] for s in expanded)
            results.append(_rust_combine_latest(*row, emit_on=emit_on_streams[i]))
        ms = MultiStream.__new__(MultiStream)
        ms._streams = results
        ms._keys = keys
        return ms
    broadcasted = _broadcast(*converted)
    if emit_on is not None:
        emit_on_s = _as_streams(emit_on, label="emit_on")
        emit_on_expanded = emit_on_s._expand(len(broadcasted[0]))
        emit_on_streams = emit_on_expanded._streams
    else:
        emit_on_streams = [None] * len(broadcasted[0])
    results = _combine_batch(broadcasted, _rust_combine_latest, emit_on_streams=emit_on_streams)
    return MultiStream(results)


def zip(*args):
    if len(args) < 2:
        raise ValueError("akayu.zip() needs at least two streams to pair. Usage: zip(a, b) or zip(a, b, c, ...)")
    if all(isinstance(a, Stream) for a in args):
        return _rust_zip(*args)
    converted = [_as_streams(a) for a in args]
    keys = _check_keys(converted)
    if keys is not None:
        expanded = [ms._expand(len(keys))._streams if len(ms) == 1 else ms._streams for ms in converted]
        results = [_rust_zip(*_builtins.tuple(s[i] for s in expanded)) for i in range(len(keys))]
        ms = MultiStream.__new__(MultiStream)
        ms._streams = results
        ms._keys = keys
        return ms
    broadcasted = _broadcast(*converted)
    results = [_rust_zip(*pair) for pair in _builtins.zip(*[b._streams for b in broadcasted], strict=True)]
    return MultiStream(results)


class TextFileSource(Stream):
    """A source Stream that emits the lines of a text file.

    Created by `from_text_file()`. Build the pipeline first, then call
    `start()`: lines are read in a background thread and emitted in order,
    without their line endings. `join()` waits for the last line and
    re-raises any error from reading the file or from the pipeline.
    """

    def __new__(cls, path, interval=None):  # noqa: ARG004 - same signature as __init__
        return super().__new__(cls, name="from_text_file")

    def __init__(self, path, interval=None):
        self.path = path
        self.interval = interval
        self._thread = None
        self._error = None

    def start(self):
        """Start reading the file in a background thread. Returns self."""
        if self._thread is not None:
            raise RuntimeError(f"from_text_file({self.path!r}) was already started")
        self._thread = _threading.Thread(target=self._read, name=f"akayu-from_text_file({self.path})", daemon=True)
        self._thread.start()
        return self

    def join(self, timeout=None):
        """Wait until every line has been emitted, re-raising any error."""
        if self._thread is None:
            raise RuntimeError(f"from_text_file({self.path!r}) was never started; call start() first")
        self._thread.join(timeout)
        if self._thread.is_alive():
            raise TimeoutError(f"still reading {self.path!r} after {timeout}s")
        if self._error is not None:
            raise self._error

    def _read(self):
        try:
            with _Path(self.path).open(encoding="utf-8") as f:
                for line in f:
                    self.emit(line.rstrip("\r\n"))
                    if self.interval:
                        _time.sleep(self.interval)
        except BaseException as e:  # surfaced by join()
            self._error = e


def from_text_file(path, interval=None):
    """Create a source Stream that emits the lines of a text file.

    Nothing is read until `start()` is called, so build the pipeline first::

        source = akayu.from_text_file("data.txt")
        source.map(str.upper).sink(print)
        source.start().join()

    Args:
        path: The file to read (UTF-8).
        interval: Optional delay in seconds between emitting lines. The file
            is read once; lines appended later are not picked up.

    Returns:
        A `TextFileSource`.
    """
    return TextFileSource(path, interval)


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
