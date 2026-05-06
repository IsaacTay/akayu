from akayu import Stream


class TestToList:
    def test_basic(self):
        s = Stream()
        result = s.map(lambda x: x * 2).to_list()
        s.emit(1)
        s.emit(2)
        s.emit(3)
        s.flush()
        assert list(result) == [2, 4, 6]

    def test_empty(self):
        s = Stream()
        result = s.to_list()
        s.flush()
        assert list(result) == []

    def test_compiled(self):
        s = Stream()
        result = s.map(lambda x: x + 10).to_list()
        s.compile()
        s.emit(1)
        s.emit(2)
        s.flush()
        assert list(result) == [11, 12]


class TestToSet:
    def test_deduplication(self):
        s = Stream()
        result = s.to_set()
        s.emit(1)
        s.emit(2)
        s.emit(1)
        s.emit(3)
        s.emit(2)
        s.flush()
        assert result == {1, 2, 3}

    def test_empty(self):
        s = Stream()
        result = s.to_set()
        s.flush()
        assert result == set()

    def test_with_filter(self):
        s = Stream()
        result = s.filter(lambda x: x > 5).to_set()
        s.emit(1)
        s.emit(10)
        s.emit(20)
        s.emit(10)
        s.flush()
        assert result == {10, 20}

    def test_compiled(self):
        s = Stream()
        result = s.map(lambda x: x % 3).to_set()
        s.compile()
        for i in range(10):
            s.emit(i)
        s.flush()
        assert result == {0, 1, 2}


class TestCollectContext:
    def test_auto_flush_on_exit(self):
        s = Stream()
        result = []
        collected = s.collect()
        collected.sink(result.append)
        s.emit(1)
        s.emit(2)
        s.emit(3)
        collected.flush()
        assert result == [(1, 2, 3)]

    def test_empty(self):
        s = Stream()
        result = []
        collected = s.collect()
        collected.sink(result.append)
        collected.flush()
        assert result == [()]


class TestRepr:
    def test_uncompiled(self):
        s = Stream()
        r = repr(s)
        assert "Stream" in r
        assert "compiled=false" in r

    def test_compiled(self):
        s = Stream()
        s.map(lambda x: x).sink(lambda _: None)
        s.compile()
        r = repr(s)
        assert "compiled=true" in r
