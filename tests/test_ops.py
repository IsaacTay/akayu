from rstreamz import Stream


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
