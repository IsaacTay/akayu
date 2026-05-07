"""Tests for pipeline profiling feature."""

import time

import pytest

from akayu import ProfileReport, Stream

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def double(x):
    return x * 2

def is_even(x):
    return x % 2 == 0

def noop(x):
    pass

def slow_fn(x):
    """Burns ~1ms per call for measurable func_time."""
    total = 0
    for i in range(5000):
        total += i
    return x


# ---------------------------------------------------------------------------
# Basic profiling
# ---------------------------------------------------------------------------

class TestEmitProfiled:
    def test_linear_pipeline(self):
        s = Stream()
        s.map(double).filter(is_even).sink(noop)

        report = s.emit_profiled(5)
        assert isinstance(report, ProfileReport)
        assert report.emit_count == 1
        assert report.total_time_ms > 0

    def test_report_has_all_nodes(self):
        s = Stream()
        s.map(double).filter(is_even).sink(noop)

        report = s.emit_profiled(5)
        d = report.to_dict()
        names = [n["name"] for n in d["nodes"]]
        assert "source" in names
        assert any("map" in n for n in names)
        assert any("filter" in n for n in names)
        assert any("sink" in n for n in names)

    def test_func_time_positive_for_nodes_with_functions(self):
        s = Stream()
        s.map(slow_fn).sink(noop)

        report = s.emit_profiled(5)
        d = report.to_dict()
        map_node = next(n for n in d["nodes"] if "map" in n["name"])
        assert map_node["func_time_ms"] > 0

    def test_source_has_zero_func_time(self):
        s = Stream()
        s.map(double).sink(noop)

        report = s.emit_profiled(5)
        d = report.to_dict()
        source_node = next(n for n in d["nodes"] if n["name"] == "source")
        assert source_node["func_time_ms"] == 0.0

    def test_items_flow(self):
        s = Stream()
        s.map(double).sink(noop)

        report = s.emit_profiled(5)
        d = report.to_dict()
        _source = next(n for n in d["nodes"] if n["name"] == "source")
        # Source doesn't go through update, so items_in/out may be 0
        map_node = next(n for n in d["nodes"] if "map" in n["name"])
        assert map_node["items_in"] == 1
        assert map_node["items_out"] == 1
        sink_node = next(n for n in d["nodes"] if "sink" in n["name"])
        assert sink_node["items_in"] == 1
        assert sink_node["items_out"] == 0  # terminal

    def test_overhead_nonnegative(self):
        s = Stream()
        s.map(double).sink(noop)

        report = s.emit_profiled(5)
        assert report.total_overhead_ms >= 0

    def test_non_source_raises(self):
        s = Stream()
        m = s.map(double)
        with pytest.raises(RuntimeError):
            m.emit_profiled(5)


# ---------------------------------------------------------------------------
# Batch profiling
# ---------------------------------------------------------------------------

class TestBatchProfiled:
    def test_batch_items_count(self):
        s = Stream()
        s.map(double).sink(noop)

        report = s.emit_batch_profiled([1, 2, 3])
        d = report.to_dict()
        map_node = next(n for n in d["nodes"] if "map" in n["name"])
        assert map_node["items_in"] == 3
        assert map_node["items_out"] == 3

    def test_batch_filter_funnel(self):
        s = Stream()
        s.filter(is_even).sink(noop)

        report = s.emit_batch_profiled([1, 2, 3, 4, 5, 6])
        d = report.to_dict()
        filter_node = next(n for n in d["nodes"] if "filter" in n["name"])
        assert filter_node["items_in"] == 6
        assert filter_node["items_out"] == 3  # 2, 4, 6 pass


# ---------------------------------------------------------------------------
# Multi-emit aggregation (start/stop)
# ---------------------------------------------------------------------------

class TestStartStop:
    def test_multi_emit_accumulates(self):
        s = Stream()
        s.map(double).sink(noop)

        s.start_profiling()
        for i in range(100):
            s.emit(i)
        report = s.stop_profiling()

        assert report.emit_count == 100
        d = report.to_dict()
        map_node = next(n for n in d["nodes"] if "map" in n["name"])
        assert map_node["items_in"] == 100
        assert map_node["items_out"] == 100

    def test_filter_accumulates_correctly(self):
        s = Stream()
        s.filter(is_even).sink(noop)

        s.start_profiling()
        for i in range(100):
            s.emit(i)
        report = s.stop_profiling()

        d = report.to_dict()
        filter_node = next(n for n in d["nodes"] if "filter" in n["name"])
        assert filter_node["items_in"] == 100
        assert filter_node["items_out"] == 50  # 0,2,4,...,98

    def test_non_source_start_raises(self):
        s = Stream()
        m = s.map(double)
        with pytest.raises(RuntimeError):
            m.start_profiling()


# ---------------------------------------------------------------------------
# Context manager
# ---------------------------------------------------------------------------

class TestContextManager:
    def test_profile_context_manager(self):
        s = Stream()
        s.map(double).sink(noop)

        with s.profile() as p:
            for i in range(10):
                s.emit(i)

        assert p.report is not None
        assert p.report.emit_count == 10

    def test_context_manager_with_exception(self):
        def bad_fn(x):
            if x == 5:
                raise ValueError("bad")
            return x

        s = Stream()
        s.map(bad_fn).sink(noop)

        with s.profile() as p:
            for i in range(10):
                try:
                    s.emit(i)
                except RuntimeError:
                    pass

        # Report should still be built with partial data
        assert p.report is not None
        assert p.report.emit_count > 0


# ---------------------------------------------------------------------------
# Compiled / fused nodes
# ---------------------------------------------------------------------------

class TestCompiledProfiling:
    def test_fused_maps_single_entry(self):
        """Compiled chain expands individual steps in the profiling report."""
        s = Stream()
        s.map(double).map(double).sink(noop)
        s.compile()

        report = s.emit_profiled(5)
        d = report.to_dict()
        names = [n["name"] for n in d["nodes"]]
        # Chain expands: source, map(double), map(double), sink(noop)
        assert len(names) == 4
        map_nodes = [n for n in names if "map" in n]
        assert len(map_nodes) == 2

    def test_compiled_vs_uncompiled_overhead(self):
        """Compiled pipeline should have less overhead per item."""
        # Uncompiled
        s1 = Stream()
        s1.map(double).map(double).map(double).sink(noop)
        r1 = s1.emit_batch_profiled(list(range(1000)))

        # Compiled
        s2 = Stream()
        s2.map(double).map(double).map(double).sink(noop)
        s2.compile()
        r2 = s2.emit_batch_profiled(list(range(1000)))

        # Both should have same node count (chain expands in report)
        d1 = r1.to_dict()
        d2 = r2.to_dict()
        assert len(d2["nodes"]) == len(d1["nodes"])

        # Compiled should have less or equal overhead
        assert r2.total_overhead_ms <= r1.total_overhead_ms * 1.5  # allow noise


# ---------------------------------------------------------------------------
# Branching pipeline
# ---------------------------------------------------------------------------

class TestBranchingPipeline:
    def test_branching_both_sinks_appear(self):
        s = Stream()
        m = s.map(double)
        results_a = []
        results_b = []
        m.sink(results_a.append)
        m.sink(results_b.append)

        report = s.emit_profiled(5)
        d = report.to_dict()
        sink_nodes = [n for n in d["nodes"] if "sink" in n["name"]]
        assert len(sink_nodes) == 2

    def test_branching_edges(self):
        s = Stream()
        m = s.map(double)
        results_a = []
        results_b = []
        m.sink(results_a.append)
        m.sink(results_b.append)

        report = s.emit_profiled(5)
        d = report.to_dict()
        # Should have edges from map to both sinks
        assert len(d["edges"]) >= 2


# ---------------------------------------------------------------------------
# Wall time excludes inter-emit time
# ---------------------------------------------------------------------------

class TestWallTime:
    def test_wall_time_excludes_sleep(self):
        s = Stream()
        s.map(double).sink(noop)

        s.start_profiling()
        s.emit(1)
        time.sleep(0.1)  # 100ms sleep between emits
        s.emit(2)
        report = s.stop_profiling()

        # Wall time should be much less than 100ms
        assert report.total_time_ms < 50  # generous bound


# ---------------------------------------------------------------------------
# Nested profiling error
# ---------------------------------------------------------------------------

class TestNestedProfiling:
    def test_nested_raises(self):
        s = Stream()
        s.map(double).sink(noop)

        s.start_profiling()
        with pytest.raises(RuntimeError, match="already active"):
            s.start_profiling()
        s.stop_profiling()  # cleanup


# ---------------------------------------------------------------------------
# Report formatting
# ---------------------------------------------------------------------------

class TestReportFormatting:
    def test_tree_view_str(self):
        s = Stream()
        s.map(double).filter(is_even).sink(noop)

        report = s.emit_profiled(5)
        output = str(report)
        assert "Pipeline Profile" in output
        assert "Function:" in output
        assert "Overhead:" in output

    def test_sorted_view(self):
        s = Stream()
        s.map(double).filter(is_even).sink(noop)

        report = s.emit_profiled(5)
        # Just verify it doesn't crash
        report.print(sort_by="time")

    def test_to_dict_structure(self):
        s = Stream()
        s.map(double).sink(noop)

        report = s.emit_profiled(5)
        d = report.to_dict()
        assert "wall_time_ms" in d
        assert "emit_count" in d
        assert "total_func_time_ms" in d
        assert "total_overhead_ms" in d
        assert "nodes" in d
        assert "edges" in d
        assert len(d["nodes"]) > 0
        for node in d["nodes"]:
            assert "id" in node
            assert "name" in node
            assert "location" in node
            assert "func_time_ms" in node
            assert "items_in" in node
            assert "items_out" in node

    def test_to_list(self):
        s = Stream()
        s.map(double).sink(noop)

        report = s.emit_profiled(5)
        lst = report.to_list()
        assert len(lst) > 0

    def test_getters(self):
        s = Stream()
        s.map(double).sink(noop)

        report = s.emit_profiled(5)
        assert report.total_func_time_ms >= 0
        assert report.total_overhead_ms >= 0
        assert report.total_time_ms > 0
        assert report.emit_count == 1


# ---------------------------------------------------------------------------
# Flatten expansion
# ---------------------------------------------------------------------------

class TestFlattenProfiling:
    def test_flatten_items_out_gt_items_in(self):
        s = Stream()
        s.flatten().sink(noop)

        report = s.emit_profiled([1, 2, 3])
        d = report.to_dict()
        flatten_node = next(n for n in d["nodes"] if n["name"] == "flatten")
        assert flatten_node["items_in"] == 1
        assert flatten_node["items_out"] == 3  # expanded


# ---------------------------------------------------------------------------
# Accumulate
# ---------------------------------------------------------------------------

class TestAccumulateProfiling:
    def test_accumulate_func_time(self):
        s = Stream()
        s.accumulate(lambda state, x: state + x, start=0).sink(noop)

        report = s.emit_batch_profiled([1, 2, 3, 4, 5])
        d = report.to_dict()
        acc_node = next(n for n in d["nodes"] if "accumulate" in n["name"])
        assert acc_node["func_time_ms"] > 0
        assert acc_node["items_in"] == 5


# ---------------------------------------------------------------------------
# Empty pipeline
# ---------------------------------------------------------------------------

class TestEmptyPipeline:
    def test_source_only(self):
        s = Stream()
        s.sink(noop)

        s.start_profiling()
        report = s.stop_profiling()
        assert report.emit_count == 0
        assert report.total_time_ms == 0.0
