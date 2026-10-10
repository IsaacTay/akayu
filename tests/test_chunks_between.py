import functools
import operator

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

import akayu


def test_basic_extract():
    """Basic case: extract content between [ and ]"""
    source = akayu.Stream()
    content = source.chunks_between(lambda c: c == "[", lambda c: c == "]")
    results = []
    content.sink(results.append)
    for ch in "abc[def]ghi[jkl]mno":
        source.emit(ch)
    source.flush()
    assert results == [("d", "e", "f"), ("j", "k", "l")]


def test_no_delimiters():
    """No delimiters → nothing emitted"""
    source = akayu.Stream()
    content = source.chunks_between(lambda c: c == "[", lambda c: c == "]")
    results = []
    content.sink(results.append)
    for ch in "abcdef":
        source.emit(ch)
    source.flush()
    assert results == []


def test_incomplete_no_close():
    """Incomplete chunk (no close) → discarded"""
    source = akayu.Stream()
    content = source.chunks_between(lambda c: c == "[", lambda c: c == "]")
    results = []
    content.sink(results.append)
    for ch in "abc[def":
        source.emit(ch)
    source.flush()
    assert results == []


def test_stray_close():
    """Stray ] without [ → ignored"""
    source = akayu.Stream()
    content = source.chunks_between(lambda c: c == "[", lambda c: c == "]")
    results = []
    content.sink(results.append)
    for ch in "a]b]c":
        source.emit(ch)
    source.flush()
    assert results == []


def test_empty_between():
    """Empty content between delimiters → not emitted"""
    source = akayu.Stream()
    content = source.chunks_between(lambda c: c == "[", lambda c: c == "]")
    results = []
    content.sink(results.append)
    for ch in "a[]b":
        source.emit(ch)
    source.flush()
    assert results == []


def test_multiple_opens_ignored():
    """Second [ inside is ignored (no nesting)"""
    source = akayu.Stream()
    content = source.chunks_between(lambda c: c == "[", lambda c: c == "]")
    results = []
    content.sink(results.append)
    for ch in "a[b[c]d":
        source.emit(ch)
    source.flush()
    # [ opens, buffers b, [ ignored, buffers c, ] emits (b,c), d is incomplete → discarded
    assert results == [("b", "[", "c")]


def test_with_strings():
    """Works with string items (not just chars)"""
    source = akayu.Stream()
    content = source.chunks_between(lambda x: x == "START", lambda x: x == "END")
    results = []
    content.sink(results.append)
    for item in ["ignore", "START", "hello", "world", "END", "ignore2", "START", "foo", "END"]:
        source.emit(item)
    source.flush()
    assert results == [("hello", "world"), ("foo",)]


def test_compiled():
    """Works after compile()"""
    source = akayu.Stream()
    content = source.chunks_between(lambda c: c == "[", lambda c: c == "]")
    results = []
    content.sink(results.append)
    source.compile()
    for ch in "abc[def]ghi[jkl]mno":
        source.emit(ch)
    source.flush()
    assert results == [("d", "e", "f"), ("j", "k", "l")]


def test_chained_with_map():
    """Can chain .map() after chunks_between"""
    source = akayu.Stream()
    content = source.chunks_between(lambda c: c == "[", lambda c: c == "]")
    joined = content.map(lambda t: "".join(t))
    results = []
    joined.sink(results.append)
    for ch in "abc[def]ghi[jkl]mno":
        source.emit(ch)
    source.flush()
    assert results == ["def", "jkl"]


def test_chained_with_filter():
    """Can chain .filter() after chunks_between"""
    source = akayu.Stream()
    content = source.chunks_between(lambda c: c == "[", lambda c: c == "]")
    filtered = content.filter(lambda t: len(t) > 2)
    results = []
    filtered.sink(results.append)
    for ch in "[a][bc][def][g]":
        source.emit(ch)
    source.flush()
    # (a,) filtered, (b,c) filtered, (d,e,f) passes, (g,) filtered
    assert results == [("d", "e", "f")]


def test_integer_stream():
    """Works with non-string items"""
    source = akayu.Stream()
    content = source.chunks_between(
        lambda x: x == 0,  # open on 0
        lambda x: x == -1,  # close on -1
    )
    results = []
    content.sink(results.append)
    for n in [1, 2, 0, 3, 4, -1, 5, 0, 6, -1, 7]:
        source.emit(n)
    source.flush()
    assert results == [(3, 4), (6,)]


def test_consecutive_pairs():
    """Multiple consecutive delimiter pairs"""
    source = akayu.Stream()
    content = source.chunks_between(lambda c: c == "<", lambda c: c == ">")
    results = []
    content.sink(results.append)
    for ch in "<a><b><c>":
        source.emit(ch)
    source.flush()
    assert results == [("a",), ("b",), ("c",)]


def test_multistream():
    """Works with MultiStream"""
    source = akayu.Stream()
    s1 = source.map(lambda x: x)
    s2 = source.map(lambda x: x)
    ms = akayu.MultiStream([s1, s2])
    extracted = ms.chunks_between(lambda c: c == "[", lambda c: c == "]")
    results = extracted.to_list()
    source.compile()
    for ch in "a[b]c":
        source.emit(ch)
    source.flush()
    assert len(results) == 2  # list of two lists


def test_emit_batch():
    """Works with emit_batch"""
    source = akayu.Stream()
    content = source.chunks_between(lambda c: c == "[", lambda c: c == "]")
    results = []
    content.sink(results.append)
    source.emit_batch(list("abc[def]ghi[jkl]mno"))
    source.flush()
    assert results == [("d", "e", "f"), ("j", "k", "l")]


def test_drain_discards_incomplete():
    """drain() also discards incomplete chunks"""
    source = akayu.Stream()
    content = source.chunks_between(lambda c: c == "[", lambda c: c == "]")
    results = []
    content.sink(results.append)
    for ch in "abc[def":
        source.emit(ch)
    source.drain()
    assert results == []


# --- Regression tests ---------------------------------------------------------


@pytest.mark.parametrize("compiled", [False, True], ids=["eager", "compiled"])
@pytest.mark.parametrize(
    "open_, close",
    [
        (lambda c: c == "[", "]".__eq__),
        ("[".__eq__, lambda c: c == "]"),
        (lambda c: c == "[", functools.partial(operator.eq, "]")),
        (functools.partial(operator.eq, "["), lambda c: c == "]"),
    ],
    ids=["lambda/method", "method/lambda", "lambda/partial", "partial/lambda"],
)
def test_mixed_callable_types(open_, close, compiled):
    """open and close may be different callable types.

    close used to be invoked through open's cached vectorcall pointer, which
    segfaulted after compile() when the two weren't the same type.
    """
    source = akayu.Stream()
    results = []
    source.chunks_between(open_, close).sink(results.append)
    if compiled:
        source.compile()
    for ch in "a[bc]d":
        source.emit(ch)
    assert results == [("b", "c")]


@pytest.mark.parametrize("method", ["flush", "drain"])
def test_flush_resets_inside_state(method):
    """Discarding an incomplete chunk also returns to the outside state."""
    source = akayu.Stream()
    results = []
    source.chunks_between(lambda c: c == "[", lambda c: c == "]").sink(results.append)
    for ch in "x[ab":
        source.emit(ch)
    getattr(source, method)()
    for ch in "cd]e[f]":
        source.emit(ch)
    assert results == [("f",)]


def test_flush_reaches_downstream_collect():
    """flush() passes through chunks_between, so a downstream collect emits."""
    source = akayu.Stream()
    results = []
    chunks = source.chunks_between(lambda c: c == "[", lambda c: c == "]")
    chunks.collect().sink(results.append)
    for ch in "[ab]x[cd":
        source.emit(ch)
    source.flush()
    for ch in "[e]":
        source.emit(ch)
    source.flush()
    assert results == [(("a", "b"),), (("e",),)]


def test_same_predicate_for_open_and_close():
    """One predicate can serve as both delimiters, e.g. quotes."""

    def is_quote(c):
        return c == '"'

    source = akayu.Stream()
    results = []
    source.chunks_between(is_quote, is_quote).sink(results.append)
    for ch in 'say "hi" and "bye"':
        source.emit(ch)
    assert results == [("h", "i"), ("b", "y", "e")]


def test_predicates_called_only_in_relevant_state():
    """open is only consulted outside a chunk, close only inside one."""
    open_calls, close_calls = [], []

    def is_open(c):
        open_calls.append(c)
        return c == "["

    def is_close(c):
        close_calls.append(c)
        return c == "]"

    source = akayu.Stream()
    source.chunks_between(is_open, is_close).sink(lambda _: None)
    for ch in "a[bc]d":
        source.emit(ch)
    assert open_calls == ["a", "[", "d"]
    assert close_calls == ["b", "c", "]"]


# --- Property test against a reference model ----------------------------------

# Delimiter predicates of different callable types, so mixed pairs exercise
# separate vectorcall caching for open and close.
_OPENERS = {
    "lambda": lambda x: x == 0,
    "method": (0).__eq__,
    "partial": functools.partial(operator.eq, 0),
}
_CLOSERS = {
    "lambda": lambda x: x == 1,
    "method": (1).__eq__,
    "partial": functools.partial(operator.eq, 1),
    "same-as-open": None,
}


def _reference(events, is_open, is_close):
    """Pure-Python model: returns (chunks, collect_output_per_flush)."""
    chunks, segments = [], []
    buf, inside, seg_start = [], False, 0
    for ev in events:
        if ev is None:  # flush
            buf, inside = [], False
            segments.append(tuple(chunks[seg_start:]))
            seg_start = len(chunks)
        elif inside:
            if is_close(ev):
                inside = False
                if buf:
                    chunks.append(tuple(buf))
                buf = []
            else:
                buf.append(ev)
        elif is_open(ev):
            inside = True
    return chunks, segments


@settings(max_examples=300, deadline=None)
@given(
    events=st.lists(st.one_of(st.integers(0, 3), st.none()), max_size=60),
    open_kind=st.sampled_from(sorted(_OPENERS)),
    close_kind=st.sampled_from(sorted(_CLOSERS)),
    compiled=st.booleans(),
    batched=st.booleans(),
)
def test_matches_reference_model(events, open_kind, close_kind, compiled, batched):
    """None in `events` is a flush; ints are emitted (0 opens, 1 closes)."""
    is_open = _OPENERS[open_kind]
    is_close = _CLOSERS[close_kind] or is_open

    source = akayu.Stream()
    chunks, segments = [], []
    extracted = source.chunks_between(is_open, is_close)
    extracted.sink(chunks.append)
    extracted.collect().sink(segments.append)
    if compiled:
        source.compile()

    pending = []
    for ev in [*events, None]:
        if ev is not None and batched:
            pending.append(ev)
            continue
        if pending:
            source.emit_batch(pending)
            pending = []
        if ev is None:
            source.flush()
        else:
            source.emit(ev)

    expected_chunks, expected_segments = _reference([*events, None], is_open, is_close)
    assert chunks == expected_chunks
    assert segments == expected_segments
