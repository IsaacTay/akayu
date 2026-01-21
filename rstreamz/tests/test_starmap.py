import rstreamz


def test_starmap():
    s = rstreamz.Stream()
    res = []

    def adder(x, y, offset=0):
        return x + y + offset

    s.starmap(adder, offset=1).sink(res.append)

    # starmap expects iterables
    s.emit((1, 2))  # 1 + 2 + 1 = 4
    s.emit([3, 4])  # 3 + 4 + 1 = 8

    assert res == [4, 8]


if __name__ == "__main__":
    test_starmap()
