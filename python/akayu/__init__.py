import builtins as _builtins

from akayu.akayu import (
    ProfileReport,
    from_text_file,
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

_UNSUPPORTED = frozenset({
    "emit", "emit_batch", "prefetch", "par", "parallel", "seq",
    "start_profiling", "stop_profiling", "emit_profiled", "emit_batch_profiled",
    "profile",
})


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
            "Cannot broadcast: all MultiStream arguments are empty. "
            "Provide at least one non-empty MultiStream."
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
    streams_per_row = _builtins.zip(*[b._streams for b in broadcasted])
    if "emit_on_streams" in extra:
        return [combinator(*row, emit_on=eo) for row, eo in _builtins.zip(streams_per_row, extra["emit_on_streams"])]
    return [combinator(*row) for row in streams_per_row]


class MultiStream:
    __slots__ = ("_streams",)

    def __init__(self, streams):
        if not isinstance(streams, (list, tuple)):
            raise TypeError(
                f"MultiStream expects a list or tuple of Stream instances, "
                f"got {type(streams).__name__}. "
                f"Usage: MultiStream([Stream(), Stream(), ...])"
            )
        for i, s in enumerate(streams):
            if not isinstance(s, Stream):
                raise TypeError(
                    f"Element {i} is not a Stream (got {type(s).__name__}). "
                    f"All elements must be Stream instances."
                )
        self._streams = list(streams)

    def __len__(self):
        return len(self._streams)

    def __iter__(self):
        return iter(self._streams)

    def __getitem__(self, key):
        result = self._streams[key]
        if isinstance(result, list):
            return MultiStream(result)
        return result

    def __repr__(self):
        return f"MultiStream({len(self._streams)})"

    def _expand(self, target_len):
        if len(self._streams) == target_len:
            return self
        if len(self._streams) == 1:
            return MultiStream(list(self._streams) * target_len)
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
        return MultiStream(streams)

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

    # --- transforms (return MultiStream) ---

    def map(self, func, *args, **kwargs):
        return self._apply("map", func, *args, **kwargs)

    def filter(self, func, *args, **kwargs):
        return self._apply("filter", func, *args, **kwargs)

    def sink(self, func, *args, **kwargs):
        return self._apply("sink", func, *args, **kwargs)

    def starmap(self, func, *args, **kwargs):
        return self._apply("starmap", func, *args, **kwargs)

    def accumulate(self, func, start, **kwargs):
        return MultiStream([s.accumulate(func, start, **kwargs) for s in self._streams])

    def flatten(self, *, stream_name=None):
        return MultiStream([s.flatten(stream_name=stream_name) for s in self._streams])

    def collect(self, *, stream_name=None):
        return MultiStream([s.collect(stream_name=stream_name) for s in self._streams])

    def batch_map(self, func, *args, **kwargs):
        return self._apply("batch_map", func, *args, **kwargs)

    def chunk_by(self, predicate, *, include_delimiter=False, stream_name=None, **kwargs):
        return MultiStream([s.chunk_by(predicate, include_delimiter=include_delimiter, stream_name=stream_name, **kwargs) for s in self._streams])

    # --- collectors (return list of results) ---

    def to_list(self):
        return [s.to_list() for s in self._streams]

    def to_set(self):
        return [s.to_set() for s in self._streams]

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

    # --- broadcasting combinators ---

    def union(self, *others):
        if not others:
            raise ValueError(
                "union() needs at least one other stream to merge with. "
                "Usage: ms.union(other) or ms.union(a, b, ...)"
            )
        args = [_as_streams(self)] + [_as_streams(o) for o in others]
        broadcasted = _broadcast(*args)
        results = [_rust_union(*pair) for pair in _builtins.zip(*[b._streams for b in broadcasted])]
        return MultiStream(results)

    def combine_latest(self, *others, emit_on=None):
        if not others:
            raise ValueError(
                "combine_latest() needs at least one other stream. "
                "Usage: ms.combine_latest(other) or ms.combine_latest(a, b, emit_on=c)"
            )
        args = [_as_streams(self)] + [_as_streams(o) for o in others]
        broadcasted = _broadcast(*args)

        if emit_on is not None:
            emit_on_s = _as_streams(emit_on, label="emit_on")
            emit_on_expanded = emit_on_s._expand(len(broadcasted[0]))
            emit_on_streams = emit_on_expanded._streams
        else:
            emit_on_streams = [None] * len(broadcasted[0])

        results = []
        for pair, eo in _builtins.zip(_builtins.zip(*[b._streams for b in broadcasted]), emit_on_streams):
            results.append(_rust_combine_latest(*pair, emit_on=eo))
        return MultiStream(results)

    def zip(self, *others):
        if not others:
            raise ValueError(
                "zip() needs at least one other stream to pair with. "
                "Usage: ms.zip(other) or ms.zip(a, b, ...)"
            )
        args = [_as_streams(self)] + [_as_streams(o) for o in others]
        broadcasted = _broadcast(*args)
        results = [_rust_zip(*pair) for pair in _builtins.zip(*[b._streams for b in broadcasted])]
        return MultiStream(results)


def union(*args):
    if len(args) < 2:
        raise ValueError(
            "akayu.union() needs at least two streams to merge. "
            "Usage: union(a, b) or union(a, b, c, ...)"
        )
    if all(isinstance(a, Stream) for a in args):
        return _rust_union(*args)
    converted = [_as_streams(a) for a in args]
    broadcasted = _broadcast(*converted)
    results = [_rust_union(*pair) for pair in _builtins.zip(*[b._streams for b in broadcasted])]
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
        raise ValueError(
            "akayu.zip() needs at least two streams to pair. "
            "Usage: zip(a, b) or zip(a, b, c, ...)"
        )
    if all(isinstance(a, Stream) for a in args):
        return _rust_zip(*args)
    converted = [_as_streams(a) for a in args]
    broadcasted = _broadcast(*converted)
    results = [_rust_zip(*pair) for pair in _builtins.zip(*[b._streams for b in broadcasted])]
    return MultiStream(results)


__all__ = [
    "MultiStream",
    "ProfileReport",
    "Stream",
    "combine_latest",
    "from_text_file",
    "to_text_file",
    "union",
    "zip",
]
