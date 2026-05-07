"""Tests for _analyze_lambda caching and pre-filter optimizations."""

import akayu


class TestLambdaCacheCorrectness:
    """Verify that cached analysis results are correct for repeated functions."""

    def test_same_lambda_two_pipelines(self):
        """Same lambda function reused across two compiled pipelines."""
        f = lambda x: x + 1

        # Pipeline 1
        s1 = akayu.Stream()
        r1 = []
        s1.map(f).sink(r1.append)
        s1.compile()
        s1.emit(10)
        assert r1 == [11]

        # Pipeline 2 — same lambda, should use cached analysis
        s2 = akayu.Stream()
        r2 = []
        s2.map(f).sink(r2.append)
        s2.compile()
        s2.emit(20)
        assert r2 == [21]

    def test_same_filter_two_pipelines(self):
        """Same filter lambda reused across two compiled pipelines."""
        pred = lambda x: x > 5

        s1 = akayu.Stream()
        r1 = []
        s1.filter(pred).sink(r1.append)
        s1.compile()
        s1.emit(3)
        s1.emit(7)
        assert r1 == [7]

        s2 = akayu.Stream()
        r2 = []
        s2.filter(pred).sink(r2.append)
        s2.compile()
        s2.emit(2)
        s2.emit(9)
        assert r2 == [9]

    def test_different_lambdas_same_pipeline(self):
        """Multiple different lambdas in same pipeline — cache handles all."""
        s = akayu.Stream()
        r = []
        s.map(lambda x: x + 1)\
         .map(lambda x: x * 2)\
         .filter(lambda x: x > 5)\
         .sink(r.append)
        s.compile()
        s.emit(1)  # 1 -> 2 -> 4 -> filtered
        s.emit(3)  # 3 -> 4 -> 8 -> kept
        assert r == [8]

    def test_closure_lambda_cached(self):
        """Lambda with closure variable — cache works (same function, same closure)."""
        threshold = 10
        f = lambda x: x > threshold

        s = akayu.Stream()
        r = []
        s.filter(f).sink(r.append)
        s.compile()
        s.emit(5)
        s.emit(15)
        assert r == [15]

        # Reuse same lambda in another pipeline
        s2 = akayu.Stream()
        r2 = []
        s2.filter(f).sink(r2.append)
        s2.compile()
        s2.emit(3)
        s2.emit(20)
        assert r2 == [20]

    def test_chained_lambda_cached(self):
        """Chained lambda (x.attr > val) is cached and works on reuse."""
        f = lambda x: x.value > 0

        for _ in range(3):
            s = akayu.Stream()
            r = []
            s.filter(f).sink(r.append)
            s.compile()
            s.emit(type('Obj', (), {'value': -1})())
            s.emit(type('Obj', (), {'value': 5})())
            assert len(r) == 1 and r[0].value == 5, f"Failed on iteration {_}"

    def test_isinstance_filter_cached(self):
        """isinstance filter lambda is cached and reused correctly."""
        f = lambda x: isinstance(x, int)

        for _ in range(3):
            s = akayu.Stream()
            r = []
            s.filter(f).sink(r.append)
            s.compile()
            s.emit(42)
            s.emit("hello")
            s.emit(3.14)
            assert r == [42]

    def test_unanalyzable_cached_as_none(self):
        """Unanalyzable function is cached as None and doesn't break execution."""
        def custom(x):
            return complex_processing(x)

        def complex_processing(x):
            return x

        s = akayu.Stream()
        r = []
        s.map(custom).sink(r.append)
        s.compile()
        s.emit(42)
        assert r == [42]

        # Same function in another pipeline — cached None is correct
        s2 = akayu.Stream()
        r2 = []
        s2.map(custom).sink(r2.append)
        s2.compile()
        s2.emit(99)
        assert r2 == [99]


class TestBytecodeLengthFilter:
    """Verify that complex functions are rejected before expensive disassembly."""

    def test_complex_function_still_works(self):
        """Functions exceeding the bytecode length limit still execute correctly."""
        # This function is too complex for bytecode analysis
        # but should still work as a regular map function
        def complex_func(x):
            result = 0
            for i in range(10):
                result += x + i
            return result

        s = akayu.Stream()
        r = []
        s.map(complex_func).sink(r.append)
        s.compile()
        s.emit(1)
        # 1+0 + 1+1 + ... + 1+9 = 10 + 45 = 55
        assert r == [55]

    def test_lambda_with_list_comp_still_works(self):
        """List comprehension lambda exceeds length but still works."""
        f = lambda x: [i for i in range(x)]

        s = akayu.Stream()
        r = []
        s.map(f).sink(r.append)
        s.compile()
        s.emit(3)
        assert r == [[0, 1, 2]]

    def test_boundary_pattern_still_analyzed(self):
        """Patterns near the bytecode length boundary are still analyzed."""
        # Compound expression: x > 0 and isinstance(x, int)
        # This is ~78 bytes — right at the boundary
        f = lambda x: x > 0 and isinstance(x, int)

        s = akayu.Stream()
        r = []
        s.filter(f).sink(r.append)
        s.compile()
        s.emit(5)
        s.emit(-1)
        s.emit(3.14)
        assert r == [5]


class TestCacheDoesNotCorruptAcrossTypes:
    """Ensure the cache doesn't confuse different function types."""

    def test_map_vs_filter_same_body(self):
        """Same lambda body used as map and filter — cache handles both."""
        f = lambda x: x > 0

        # As filter
        s1 = akayu.Stream()
        r1 = []
        s1.filter(f).sink(r1.append)
        s1.compile()
        s1.emit(5)
        s1.emit(-1)
        assert r1 == [5]

        # As map (same function object)
        s2 = akayu.Stream()
        r2 = []
        s2.map(f).sink(r2.append)
        s2.compile()
        s2.emit(5)
        s2.emit(-1)
        assert r2 == [True, False]

    def test_sink_with_print_cached(self):
        """Sink functions are analyzed and cached correctly."""
        r = []
        f = r.append

        for _ in range(3):
            s = akayu.Stream()
            s.sink(f)
            s.compile()
            s.emit(42)
        assert r == [42, 42, 42]
