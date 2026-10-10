"""
Example 10: Testing Patterns

This example demonstrates:
- Unit testing individual stream components
- Mocking and isolation strategies
- Testing async stream operations
- Property-based testing with streams
- Testing compiled vs uncompiled behavior

Writing tests for stream processing code requires understanding
how to test components in isolation and how to verify pipeline behavior.
"""

import unittest
from unittest.mock import Mock

from akayu import Stream, union

# =============================================================================
# SECTION: Testing Individual Operations
# =============================================================================


class TestMapOperation(unittest.TestCase):
    """Test map() operation in isolation"""

    def test_map_doubles_values(self):
        results = []
        source = Stream()
        source.map(lambda x: x * 2).sink(results.append)

        source.emit(5)
        self.assertEqual(results, [10])

    def test_map_with_multiple_args(self):
        results = []
        source = Stream()
        source.map(lambda x, factor: x * factor, 3).sink(results.append)

        source.emit(5)
        self.assertEqual(results, [15])

    def test_map_preserves_input_order(self):
        results = []
        source = Stream()
        source.map(lambda x: x * 2).sink(results.append)

        for i in range(5):
            source.emit(i)

        self.assertEqual(results, [0, 2, 4, 6, 8])


class TestFilterOperation(unittest.TestCase):
    """Test filter() operation in isolation"""

    def test_filter_keeps_matching(self):
        results = []
        source = Stream()
        source.filter(lambda x: x > 3).sink(results.append)

        for i in range(5):
            source.emit(i)

        self.assertEqual(results, [4])

    def test_filter_with_predicate_args(self):
        results = []
        source = Stream()
        source.filter(lambda x, threshold: x > threshold, 10).sink(results.append)

        for i in [5, 15, 8, 20]:
            source.emit(i)

        self.assertEqual(results, [15, 20])


# =============================================================================
# SECTION: Testing Pipeline Components
# =============================================================================


class TestPipelineComponents(unittest.TestCase):
    """Test combined pipeline components"""

    def test_pipeline_transform_and_filter(self):
        results = []
        source = Stream()
        (source.map(lambda x: x * 2).filter(lambda x: x > 5).sink(results.append))

        source.emit_batch([1, 2, 3, 4, 5])

        self.assertEqual(results, [6, 8, 10])

    def test_pipeline_with_flatten(self):
        results = []
        source = Stream()
        source.flatten().sink(results.append)

        source.emit_batch([[1, 2], [3, 4], [5]])

        self.assertEqual(results, [1, 2, 3, 4, 5])


class TestAccumulateOperation(unittest.TestCase):
    """Test accumulate() operation"""

    def test_accumulate_running_sum(self):
        results = []
        source = Stream()
        source.accumulate(lambda state, x: state + x, start=0).sink(results.append)

        source.emit_batch([1, 2, 3, 4])

        self.assertEqual(results, [1, 3, 6, 10])

    def test_accumulate_with_returns_state(self):
        results = []
        source = Stream()

        def reducer(state, x):
            new_state = state + x
            return (new_state, new_state * 2)

        source.accumulate(reducer, start=0, returns_state=True).sink(results.append)

        source.emit(1)
        source.emit(2)
        source.emit(3)

        self.assertEqual(results, [2, 6, 12])


# =============================================================================
# SECTION: Testing Parallel Branches
# =============================================================================


class TestParallelBranches(unittest.TestCase):
    """Test par() and parallel execution"""

    def test_par_broadcasts_to_all_branches(self):
        results_a = []
        results_b = []

        source = Stream()
        p = source.par()

        p.map(lambda x: x * 2).sink(results_a.append)
        p.map(lambda x: x + 100).sink(results_b.append)

        source.emit(5)

        self.assertEqual(results_a, [10])
        self.assertEqual(results_b, [105])

    def test_union_merges_branches(self):
        merged = []

        source = Stream()
        p = source.par()

        branch1 = p.map(lambda x: x * 2)
        branch2 = p.map(lambda x: x + 100)

        merged_stream = union(branch1, branch2)
        merged_stream.sink(merged.append)

        source.emit(5)

        # Both branches emit to the merged stream
        self.assertEqual(sorted(merged), [10, 105])


# =============================================================================
# SECTION: Testing Async Operations
# =============================================================================


class TestAsyncOperations(unittest.TestCase):
    """Test async stream operations"""

    async def test_async_map(self):
        results = []

        async def async_double(x):
            return x * 2

        source = Stream()
        source.map(async_double).sink(results.append)

        await source.emit(10)
        await source.emit(20)

        self.assertEqual(results, [20, 40])

    async def test_async_filter(self):
        results = []

        async def async_gt_5(x):
            return x > 5

        source = Stream()
        source.filter(async_gt_5).sink(results.append)

        await source.emit(3)
        await source.emit(8)
        await source.emit(10)

        self.assertEqual(results, [8, 10])


# =============================================================================
# SECTION: Testing with Mocks
# =============================================================================


class TestWithMocks(unittest.TestCase):
    """Test using mock objects"""

    def test_sink_mock_called_correctly(self):
        mock_sink = Mock()
        source = Stream()
        source.map(lambda x: x * 2).sink(mock_sink)

        source.emit(5)

        mock_sink.assert_called_once_with(10)

    def test_map_function_mocked(self):
        mock_map = Mock(side_effect=lambda x: x * 3)
        results = []

        source = Stream()
        source.map(mock_map).sink(results.append)

        source.emit(5)
        source.emit(10)

        self.assertEqual(results, [15, 30])
        self.assertEqual(mock_map.call_count, 2)

    def test_filter_with_mock_predicate(self):
        mock_pred = Mock(side_effect=lambda x: x > 5)
        results = []

        source = Stream()
        source.filter(mock_pred).sink(results.append)

        source.emit_batch([3, 6, 8, 4, 9])

        self.assertEqual(results, [6, 8, 9])
        self.assertEqual(mock_pred.call_count, 5)


# =============================================================================
# SECTION: Testing Compile Behavior
# =============================================================================


class TestCompiledStreams(unittest.TestCase):
    """Test compiled stream behavior"""

    def test_compiled_produces_same_output(self):
        results_compiled = []
        results_uncompiled = []

        # Compiled version
        source1 = Stream()
        source1.map(lambda x: x * 2).map(lambda x: x + 1).sink(results_compiled.append)
        source1.compile()

        # Uncompiled version (same pipeline)
        source2 = Stream()
        source2.map(lambda x: x * 2).map(lambda x: x + 1).sink(results_uncompiled.append)

        # Both should produce identical output
        for i in range(5):
            source1.emit(i)
            source2.emit(i)

        self.assertEqual(results_compiled, results_uncompiled)

    def test_compiled_rejects_new_operations(self):
        source = Stream()
        source.map(lambda x: x * 2).sink(lambda _: None)
        source.compile()

        # After compile, adding more operations should fail
        with self.assertRaises(RuntimeError):
            source.map(lambda x: x + 1)


# =============================================================================
# SECTION: Property-Based Testing Ideas
# =============================================================================


class TestPropertyBasedPatterns(unittest.TestCase):
    """Patterns for property-based testing with streams"""

    def test_identity_map_preserves_all_items(self):
        """Mapping with identity function should preserve all items"""
        for test_data in [[], [1], [1, 2, 3], list(range(100))]:
            results = []
            source = Stream()
            source.map(lambda x: x).sink(results.append)

            source.emit_batch(test_data)

            self.assertEqual(results, test_data)

    def test_filter_never_adds_items(self):
        """Filtering should never produce more items than input"""
        for test_data in [[1, 2, 3], [1], [], list(range(50))]:
            results = []
            source = Stream()
            source.filter(lambda _: True).sink(results.append)  # Keep all

            source.emit_batch(test_data)

            self.assertLessEqual(len(results), len(test_data))
            self.assertEqual(len(results), len(test_data))  # With predicate=True

    def test_scaling_order_does_not_matter(self):
        """Scaling operations commute, so their order shouldn't affect the result"""
        results_a = []
        results_b = []

        # (x * 2) * 3
        source_a = Stream()
        source_a.map(lambda x: x * 2).map(lambda x: x * 3).sink(results_a.append)

        # (x * 3) * 2
        source_b = Stream()
        source_b.map(lambda x: x * 3).map(lambda x: x * 2).sink(results_b.append)

        test_values = list(range(20))

        source_a.emit_batch(test_values)
        source_b.emit_batch(test_values)

        self.assertEqual(results_a, results_b)


# =============================================================================
# SECTION: Integration Test Example
# =============================================================================


class TestEndToEndPipeline(unittest.TestCase):
    """Integration tests for complete pipelines"""

    def test_sensor_data_pipeline(self):
        """Test the sensor data pipeline from example 08"""
        sensor_readings = [
            {"sensor_id": "A1", "temperature": 22.5, "humidity": 45.0},
            {"sensor_id": "A1", "temperature": 23.1, "humidity": 46.2},
            {"sensor_id": "B2", "temperature": 18.9, "humidity": 52.1},
        ]

        valid_readings = []

        source = Stream()
        source.map(lambda r: r["temperature"]).filter(lambda t: -50 <= t <= 100).sink(valid_readings.append)

        for reading in sensor_readings:
            source.emit(reading)

        self.assertEqual(len(valid_readings), 3)
        self.assertEqual(valid_readings, [22.5, 23.1, 18.9])


def run_tests():
    """Run all tests with pytest"""
    print("=" * 60)
    print("AKAYU EXAMPLE 10: Testing Patterns")
    print("=" * 60)

    print("\n--- Running unit tests ---\n")

    # Create a test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    # Add all test classes
    suite.addTests(loader.loadTestsFromTestCase(TestMapOperation))
    suite.addTests(loader.loadTestsFromTestCase(TestFilterOperation))
    suite.addTests(loader.loadTestsFromTestCase(TestPipelineComponents))
    suite.addTests(loader.loadTestsFromTestCase(TestAccumulateOperation))
    suite.addTests(loader.loadTestsFromTestCase(TestParallelBranches))
    suite.addTests(loader.loadTestsFromTestCase(TestWithMocks))
    suite.addTests(loader.loadTestsFromTestCase(TestCompiledStreams))
    suite.addTests(loader.loadTestsFromTestCase(TestPropertyBasedPatterns))
    suite.addTests(loader.loadTestsFromTestCase(TestEndToEndPipeline))

    # Run with verbosity
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    print("\n--- Test patterns demonstrated ---")
    print("1. Testing individual operations (map, filter)")
    print("2. Testing combined pipeline components")
    print("3. Testing parallel branches with par()")
    print("4. Testing async operations with asyncio")
    print("5. Testing with mock objects")
    print("6. Testing compiled vs uncompiled behavior")
    print("7. Property-based testing patterns")
    print("8. End-to-end integration tests")

    print("\n" + "=" * 60)
    print("EXAMPLE 10 COMPLETE")
    print("=" * 60)

    return result.wasSuccessful()


if __name__ == "__main__":
    success = run_tests()
    exit(0 if success else 1)
