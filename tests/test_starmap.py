import akayu


def test_starmap():
    s = akayu.Stream()
    res = []

    def adder(x, y, offset=0):
        return x + y + offset

    s.starmap(adder, offset=1).sink(res.append)

    # starmap expects iterables
    s.emit((1, 2))  # 1 + 2 + 1 = 4
    s.emit([3, 4])  # 3 + 4 + 1 = 8

    assert res == [4, 8]


def test_starmap_batch():
    """Test starmap with emit_batch."""
    s = akayu.Stream(asynchronous=False)
    L = []
    s.starmap(lambda a, b: a + b).sink(L.append)
    s.emit_batch([(1, 2), (3, 4), (5, 6)])
    assert L == [3, 7, 11]


if __name__ == "__main__":
    test_starmap()
