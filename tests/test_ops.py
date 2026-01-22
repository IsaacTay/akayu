from akayu import Stream


def test_map():
    L = []

    def inc(x):
        return x + 1

    s = Stream()
    s.map(inc).sink(L.append)

    s.emit(1)
    s.emit(2)

    assert L == [2, 3]


def test_filter():
    L = []
    s = Stream()
    s.filter(lambda x: x % 2 == 0).sink(L.append)

    s.emit(1)
    s.emit(2)
    s.emit(3)
    s.emit(4)

    assert L == [2, 4]


def test_accumulate():
    L = []
    s = Stream()
    s.accumulate(lambda state, x: state + x, start=0).sink(L.append)

    s.emit(1)
    s.emit(2)
    s.emit(3)

    assert L == [1, 3, 6]


def test_flatten():
    L = []
    s = Stream()
    s.flatten().sink(L.append)

    s.emit([1, 2])
    s.emit([3])
    s.emit([])
    s.emit([4, 5])

    assert L == [1, 2, 3, 4, 5]


def test_flatten_batch():
    L = []
    s = Stream(asynchronous=False)
    s.flatten().sink(L.append)

    s.emit_batch([[1, 2], [3], [], [4, 5]])

    assert L == [1, 2, 3, 4, 5]


def test_flatten_tuple():
    L = []
    s = Stream()
    s.flatten().sink(L.append)

    s.emit((1, 2))

    assert L == [1, 2]


def test_collect():
    L = []
    s = Stream()
    c = s.collect()
    c.sink(L.append)

    s.emit(1)
    s.emit(2)
    s.emit(3)

    assert L == []

    c.flush()
    assert L == [(1, 2, 3)]

    s.emit(4)
    c.flush()
    assert L == [(1, 2, 3), (4,)]


def test_collect_batch():
    L = []
    s = Stream(asynchronous=False)
    c = s.collect()
    c.sink(L.append)

    s.emit_batch([1, 2, 3])

    assert L == []
    c.flush()
    assert L == [(1, 2, 3)]


def test_zip():
    L = []
    s1 = Stream()
    s2 = Stream()

    s1.zip(s2).sink(L.append)

    s1.emit(1)
    assert L == []
    s2.emit("a")
    assert L == [(1, "a")]

    s2.emit("b")
    assert L == [(1, "a")]
    s1.emit(2)
    assert L == [(1, "a"), (2, "b")]


def test_zip_batch():
    L = []
    s1 = Stream(asynchronous=False)
    s2 = Stream(asynchronous=False)

    s1.zip(s2).sink(L.append)

    s1.emit_batch([1, 2, 3])
    assert L == []

    s2.emit_batch(["a", "b", "c"])
    # Should emit all 3 tuples
    assert L == [(1, "a"), (2, "b"), (3, "c")]


def test_batch_map():
    """Test batch_map processes entire batch at once."""
    s = Stream(asynchronous=False)
    L = []

    def process_batch(batch):
        return [x * 2 for x in batch]

    # batch_map already iterates results, no flatten needed
    s.batch_map(process_batch).sink(L.append)
    s.emit_batch([1, 2, 3])
    assert L == [2, 4, 6]


def test_batch_map_single_emit():
    """Test batch_map with single emit wraps in list."""
    s = Stream(asynchronous=False)
    L = []

    def process_batch(batch):
        # Must return an iterable since batch_map iterates the result
        return [sum(batch)]

    s.batch_map(process_batch).sink(L.append)
    s.emit(5)
    assert L == [5]  # single item batch


def test_accumulate_returns_state():
    """Test accumulate with returns_state=True returns (state, output) tuple."""
    s = Stream()
    L = []

    def reducer(state, x):
        new_state = state + x
        output = new_state * 2  # different from state
        return (new_state, output)

    s.accumulate(reducer, start=0, returns_state=True).sink(L.append)
    s.emit(1)  # state=1, output=2
    s.emit(2)  # state=3, output=6
    s.emit(3)  # state=6, output=12
    assert L == [2, 6, 12]


def test_combine_latest_emit_on():
    """Test combine_latest only emits when specified streams update."""
    s1 = Stream()
    s2 = Stream()
    L = []
    s1.combine_latest(s2, emit_on=s1).sink(L.append)  # only emit on s1 updates
    s1.emit(1)  # no emit yet (s2 not primed)
    s2.emit("a")  # no emit (emit_on=s1 only)
    assert L == []
    s1.emit(2)  # emit (2, "a")
    assert L == [(2, "a")]
    s2.emit("b")  # no emit (emit_on=s1 only)
    assert L == [(2, "a")]
    s1.emit(3)  # emit (3, "b")
    assert L == [(2, "a"), (3, "b")]


def test_zip_three_streams():
    """Test zip with three streams."""
    s1, s2, s3 = Stream(), Stream(), Stream()
    L = []
    s1.zip(s2, s3).sink(L.append)
    s1.emit(1)
    s2.emit("a")
    s3.emit(True)
    assert L == [(1, "a", True)]


def test_combine_latest_three_streams():
    """Test combine_latest with three streams."""
    s1, s2, s3 = Stream(), Stream(), Stream()
    L = []
    s1.combine_latest(s2, s3).sink(L.append)
    s1.emit(1)
    s2.emit(2)
    s3.emit(3)
    assert L == [(1, 2, 3)]
    s1.emit(10)
    assert L == [(1, 2, 3), (10, 2, 3)]


def test_zip_unbalanced_streams():
    """Test zip when streams emit at different rates."""
    s1 = Stream()
    s2 = Stream()
    L = []
    s1.zip(s2).sink(L.append)
    s1.emit(1)
    s1.emit(2)
    s1.emit(3)  # s1 ahead
    s2.emit("a")  # first pair
    assert L == [(1, "a")]
    s2.emit("b")  # second pair
    s2.emit("c")  # third pair
    assert L == [(1, "a"), (2, "b"), (3, "c")]


def test_combine_latest_never_emits():
    """Test combine_latest when one stream never emits."""
    s1 = Stream()
    s2 = Stream()
    L = []
    s1.combine_latest(s2).sink(L.append)
    s1.emit(1)
    s1.emit(2)
    s1.emit(3)
    assert L == []  # s2 never emitted, so no output
