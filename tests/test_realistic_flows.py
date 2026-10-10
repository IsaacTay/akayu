import pytest

import akayu
from akayu import MultiStream, Stream


def _sources(n):
    return [Stream() for _ in range(n)]


class TestLogProcessing:
    def test_filter_by_level_and_extract_message(self):
        src = Stream()
        errors = []
        warning_count = [0]

        parsed = src.map(lambda line: line.split(" ", 2))
        parsed.starmap(lambda level, ts, msg: errors.append(msg) if level == "ERROR" else None)
        parsed.filter(lambda parts: parts[0] == "WARN").sink(
            lambda _: warning_count.__setitem__(0, warning_count[0] + 1)
        )

        lines = [
            "INFO 10:00 starting",
            "WARN 10:01 slow query",
            "ERROR 10:02 db down",
            "WARN 10:03 retrying",
            "ERROR 10:04 timeout",
            "INFO 10:05 recovered",
        ]
        for line in lines:
            src.emit(line)

        assert errors == ["db down", "timeout"]
        assert warning_count[0] == 2

    def test_multi_source_log_aggregation(self):
        sources = _sources(3)
        ms = MultiStream(sources)
        all_logs = []
        ms.sink(all_logs.append)

        server_logs = [
            ["req /a 200", "req /b 404"],
            ["req /c 200", "req /d 500", "req /e 200"],
            ["req /f 200"],
        ]
        for src, logs in zip(sources, server_logs):
            for line in logs:
                src.emit(line)

        assert len(all_logs) == 6
        assert "req /d 500" in all_logs

    def test_request_rate_tracking(self):
        src = Stream()
        counts = []
        src.accumulate(lambda acc, _: acc + 1, start=0).sink(counts.append)

        for _ in range(5):
            src.emit("request")

        assert counts == [1, 2, 3, 4, 5]

    def test_error_rate_per_server(self):
        sources = _sources(3)
        ms = MultiStream(sources)
        error_counts = ms.filter(lambda line: "500" in line).accumulate(lambda acc, _: acc + 1, start=0).to_list()

        server_logs = [
            ["req 200", "req 500", "req 200"],
            ["req 200", "req 200"],
            ["req 500", "req 500", "req 200", "req 500"],
        ]
        for src, logs in zip(sources, server_logs):
            for line in logs:
                src.emit(line)

        assert [list(c) for c in error_counts] == [[1], [], [1, 2, 3]]


class TestETLPipeline:
    def test_clean_transform_load(self):
        src = Stream()
        results = []
        (src.map(lambda raw: raw.strip()).filter(lambda s: s != "").map(lambda s: s.upper()).sink(results.append))

        for d in ["  hello  ", "", "  world  ", "  ", "  foo  "]:
            src.emit(d)

        assert results == ["HELLO", "WORLD", "FOO"]

    def test_multi_source_etl_with_union(self):
        src_a, src_b = _sources(2)
        results = []
        a = src_a.map(lambda x: f"A:{x}")
        b = src_b.map(lambda x: f"B:{x}")
        merged = akayu.union(a, b)
        merged.sink(results.append)

        src_a.emit(1)
        src_a.emit(2)
        src_b.emit(10)
        src_b.emit(20)

        assert sorted(results) == ["A:1", "A:2", "B:10", "B:20"]

    def test_batch_transform(self):
        src = Stream()
        results = []
        src.batch_map(lambda batch: [x * 100 for x in batch]).sink(results.append)

        src.emit(1)
        src.emit(2)
        src.emit(3)

        assert results == [100, 200, 300]

    def test_enrich_and_route(self):
        orders = Stream()
        high_value = []
        standard = []

        enriched = orders.map(lambda o: {**o, "total": o["price"] * o["qty"]})
        enriched.filter(lambda o: o["total"] > 100).sink(high_value.append)
        enriched.filter(lambda o: o["total"] <= 100).sink(standard.append)

        orders.emit({"id": 1, "price": 50, "qty": 1})
        orders.emit({"id": 2, "price": 30, "qty": 5})
        orders.emit({"id": 3, "price": 10, "qty": 2})

        assert len(high_value) == 1
        assert high_value[0]["id"] == 2
        assert len(standard) == 2

    def test_running_aggregation(self):
        src = Stream()
        totals = []
        src.accumulate(
            lambda acc, val: {"sum": acc["sum"] + val, "count": acc["count"] + 1},
            start={"sum": 0, "count": 0},
        ).sink(totals.append)

        for v in [10, 20, 30]:
            src.emit(v)

        assert totals[-1]["sum"] == 60
        assert totals[-1]["count"] == 3


class TestSensorDataFusion:
    def test_correlate_two_sensors(self):
        temp_src, humid_src = _sources(2)
        readings = []

        temp = temp_src.map(lambda v: ("temp", v))
        humid = humid_src.map(lambda v: ("humid", v))

        paired = akayu.combine_latest(temp, humid)
        paired.sink(readings.append)

        temp_src.emit(22.5)
        assert len(readings) == 0

        humid_src.emit(65.0)
        assert len(readings) == 1
        assert readings[0] == (("temp", 22.5), ("humid", 65.0))

        temp_src.emit(23.0)
        assert len(readings) == 2
        assert readings[1] == (("temp", 23.0), ("humid", 65.0))

    def test_alarm_on_threshold_per_sensor(self):
        sensors = _sources(4)
        ms = MultiStream(sensors)
        alarms = ms.filter(lambda v: v > 100).to_list()

        readings = [50, 150, 80, 200]
        for src, val in zip(sensors, readings):
            src.emit(val)

        assert list(alarms[0]) == []
        assert list(alarms[1]) == [150]
        assert list(alarms[2]) == []
        assert list(alarms[3]) == [200]

    def test_zip_paired_readings(self):
        a_src, b_src = _sources(2)
        a = a_src.map(lambda v: ("A", v))
        b = b_src.map(lambda v: ("B", v))
        paired = akayu.zip(a, b)
        results = []
        paired.sink(results.append)

        a_src.emit(1)
        b_src.emit(10)
        a_src.emit(2)
        b_src.emit(20)

        assert results == [(("A", 1), ("B", 10)), (("A", 2), ("B", 20))]

    def test_multi_sensor_accumulate(self):
        sensors = _sources(3)
        ms = MultiStream(sensors)
        totals = ms.accumulate(lambda acc, v: acc + v, start=0).to_list()

        for i, src in enumerate(sensors):
            src.emit(i + 1)
            src.emit(i + 10)

        assert [list(t) for t in totals] == [[1, 11], [2, 13], [3, 15]]


class TestStreamFanout:
    def test_one_source_multiple_downstreams(self):
        src = Stream()
        squared = []
        doubled = []
        stringified = []

        src.map(lambda x: x**2).sink(squared.append)
        src.map(lambda x: x * 2).sink(doubled.append)
        src.map(lambda x: str(x)).sink(stringified.append)

        src.emit(3)
        src.emit(5)

        assert squared == [9, 25]
        assert doubled == [6, 10]
        assert stringified == ["3", "5"]

    def test_fanout_with_collect_and_delimiter(self):
        src = Stream()
        batches = []

        c = src.collect()
        c.sink(batches.append)
        src.filter(lambda x: x == "|").sink(lambda _: c.flush())

        for ch in "abc|def|ghi":
            src.emit(ch)
        c.flush()

        assert batches == [
            ("a", "b", "c", "|"),
            ("d", "e", "f", "|"),
            ("g", "h", "i"),
        ]


class TestTextProcessing:
    def test_word_extraction_flatten(self):
        src = Stream()
        words = []
        src.flatten().sink(words.append)

        for s in ["hello world", "hello akayu"]:
            src.emit(s.split())

        assert words == ["hello", "world", "hello", "akayu"]

    def test_multi_document_unique_words(self):
        sources = _sources(3)
        ms = MultiStream(sources)
        word_sets = ms.flatten().to_set()

        docs = [
            "the quick brown fox".split(),
            "jumped over the lazy".split(),
            "dog the quick".split(),
        ]
        for src, words in zip(sources, docs):
            src.emit(words)

        assert "the" in word_sets[0]
        assert "lazy" in word_sets[1]
        assert "dog" in word_sets[2]


class TestCompiledPipeline:
    def test_compiled_matches_uncompiled(self):
        def build(compile_it=False):
            src = Stream()
            results = []
            pipeline = src.map(lambda x: x.strip()).filter(lambda x: len(x) > 0).map(lambda x: int(x) * 2)
            pipeline.sink(results.append)
            if compile_it:
                pipeline.compile()
                src.compile()
            return src, results

        src_u, res_u = build(False)
        src_c, res_c = build(True)

        for d in [" 1 ", "", "  2", "3  ", "  "]:
            src_u.emit(d)
            src_c.emit(d)

        assert res_u == [2, 4, 6]
        assert res_c == res_u

    def test_multi_stream_compile(self):
        sources = _sources(3)
        ms = MultiStream(sources)
        results = ms.map(lambda x: x * 10).to_list()
        ms.compile()

        for i, src in enumerate(sources):
            src.emit(i + 1)

        assert [list(r) for r in results] == [[10], [20], [30]]


class TestMultiStreamChainedCombinators:
    def test_union_of_three_batches(self):
        a_srcs, b_srcs, c_srcs = _sources(2), _sources(2), _sources(2)
        a = MultiStream(a_srcs).map(lambda x: f"a{x}")
        b = MultiStream(b_srcs).map(lambda x: f"b{x}")
        c = MultiStream(c_srcs).map(lambda x: f"c{x}")

        merged = a.union(b, c)
        results = merged.to_list()

        for i, (sa, sb, sc) in enumerate(zip(a_srcs, b_srcs, c_srcs)):
            sa.emit(i)
            sb.emit(i + 10)
            sc.emit(i + 20)

        assert sorted(list(results[0])) == ["a0", "b10", "c20"]
        assert sorted(list(results[1])) == ["a1", "b11", "c21"]

    def test_broadcast_shared_stream_across_batch(self):
        shared_src = Stream()
        batch_srcs = _sources(3)
        batch = MultiStream(batch_srcs).map(lambda x: x * 2)

        shared = shared_src.map(lambda x: f"shared:{x}")
        merged = batch.union(shared)

        results = merged.to_list()
        for src in batch_srcs:
            src.emit(5)
        shared_src.emit(99)

        for r in results:
            items = sorted(list(r), key=str)
            assert items == [10, "shared:99"]


class TestEdgeCaseFlows:
    def test_empty_source_empty_collector(self):
        src = Stream()
        result = src.map(lambda x: x * 2).to_list()
        assert list(result) == []

    def test_filter_removes_everything(self):
        src = Stream()
        result = src.filter(lambda x: False).to_list()
        for i in range(100):
            src.emit(i)
        assert list(result) == []

    def test_accumulate_running_sum(self):
        src = Stream()
        sums = []
        src.accumulate(lambda acc, x: acc + x, start=0).sink(sums.append)

        for v in [10, 20, 30, 40]:
            src.emit(v)

        assert sums == [10, 30, 60, 100]

    def test_multi_stream_empty_batch(self):
        ms = MultiStream([])
        assert ms.map(lambda x: x).to_list() == []
        assert ms.to_set() == []
        assert ms.flush() is None

    def test_nested_flatten(self):
        src = Stream()
        flat = []
        src.flatten().flatten().sink(flat.append)

        src.emit([[1, 2], [3, 4]])
        src.emit([[5, 6]])

        assert flat == [1, 2, 3, 4, 5, 6]


class TestMultiStreamCollectFlush:
    def test_collect_flush_per_stream(self):
        sources = _sources(3)
        ms = MultiStream(sources)
        collected = ms.collect()
        results = collected.to_list()

        sources[0].emit(1)
        sources[0].emit(2)
        sources[0].emit(3)
        sources[1].emit(10)
        sources[1].emit(20)
        sources[2].emit(100)

        ms.flush()

        assert list(results[0]) == [(1, 2, 3)]
        assert list(results[1]) == [(10, 20)]
        assert list(results[2]) == [(100,)]

    def test_collect_multiple_batches(self):
        sources = _sources(2)
        ms = MultiStream(sources)
        collected = ms.collect()
        results = collected.to_list()

        sources[0].emit("a")
        sources[1].emit("x")
        ms.flush()

        sources[0].emit("b")
        sources[0].emit("c")
        sources[1].emit("y")
        ms.flush()

        assert list(results[0]) == [("a",), ("b", "c")]
        assert list(results[1]) == [("x",), ("y",)]

    def test_collect_then_transform(self):
        sources = _sources(2)
        ms = MultiStream(sources)
        results = ms.collect().map(lambda tup: sum(tup)).to_list()

        sources[0].emit(10)
        sources[0].emit(20)
        sources[0].emit(30)
        sources[1].emit(1)
        sources[1].emit(2)
        ms.flush()

        assert list(results[0]) == [60]
        assert list(results[1]) == [3]

    def test_drain_flushes_all(self):
        sources = _sources(2)
        ms = MultiStream(sources)
        collected = ms.collect()
        results = collected.to_list()

        sources[0].emit(1)
        sources[0].emit(2)
        sources[1].emit(10)
        ms.drain()

        assert list(results[0]) == [(1, 2)]
        assert list(results[1]) == [(10,)]

    def test_collect_filter_flush_pipeline(self):
        sources = _sources(2)
        ms = MultiStream(sources)
        results = ms.filter(lambda x: x > 0).collect().to_list()

        sources[0].emit(-1)
        sources[0].emit(5)
        sources[0].emit(-3)
        sources[0].emit(10)
        sources[1].emit(0)
        sources[1].emit(7)
        ms.flush()

        assert list(results[0]) == [(5, 10)]
        assert list(results[1]) == [(7,)]

    def test_collect_empty_batch(self):
        ms = MultiStream([])
        collected = ms.collect()
        assert collected.to_list() == []
        assert collected.flush() is None


class TestWeirdEdgeCases:
    def test_same_stream_object_twice(self):
        s = Stream()
        ms = MultiStream([s, s])
        assert len(ms) == 2
        assert ms[0] is ms[1]

    def test_same_stream_union_itself(self):
        s = Stream()
        result = []
        mapped = s.map(lambda x: x * 2)
        merged = akayu.union(mapped, mapped)
        merged.sink(result.append)
        s.emit(5)
        assert sorted(result) == [10, 10]

    def test_shared_stream_across_multistreams(self):
        shared = Stream()
        ms_a = MultiStream([Stream(), shared])
        ms_b = MultiStream([shared, Stream()])
        result = ms_a.union(ms_b)
        assert isinstance(result, MultiStream)
        assert len(result) == 2

    def test_collect_without_flush_yields_nothing(self):
        src = Stream()
        collected = MultiStream([src]).collect().to_list()
        src.emit(1)
        src.emit(2)
        src.emit(3)
        assert list(collected[0]) == []

    def test_empty_multistream_in_combinator_raises(self):
        with pytest.raises(ValueError, match="Cannot broadcast"):
            MultiStream([]).union(MultiStream([Stream()]))

    def test_combine_latest_emit_on_is_one_of_the_inputs(self):
        a_src, b_src = Stream(), Stream()
        readings = []
        a = a_src.map(lambda v: ("A", v))
        b = b_src.map(lambda v: ("B", v))
        paired = akayu.combine_latest(a, b, emit_on=a)
        paired.sink(readings.append)

        b_src.emit(1)
        assert len(readings) == 0

        a_src.emit(10)
        assert len(readings) == 1
        assert readings[0] == (("A", 10), ("B", 1))

        b_src.emit(2)
        assert len(readings) == 1

    def test_broadcast_one_stream_to_many(self):
        sources = [Stream() for _ in range(100)]
        ms = MultiStream(sources)
        shared = Stream()
        merged = ms.union(shared)
        assert isinstance(merged, MultiStream)
        assert len(merged) == 100

    def test_chained_combinators(self):
        a_src, b_src, c_src = Stream(), Stream(), Stream()
        results = []
        a = a_src.map(lambda x: f"a{x}")
        b = b_src.map(lambda x: f"b{x}")
        c = c_src.map(lambda x: f"c{x}")
        ab = akayu.union(a, b)
        abc = akayu.combine_latest(ab, c)
        abc.sink(results.append)

        a_src.emit(1)
        assert len(results) == 0
        b_src.emit(2)
        c_src.emit(3)
        assert len(results) >= 1

    def test_slice_then_combine(self):
        ms = MultiStream([Stream() for _ in range(4)])
        sliced = ms[1:3]
        assert len(sliced) == 2
        result = sliced.union(MultiStream([Stream(), Stream()]))
        assert isinstance(result, MultiStream)
        assert len(result) == 2

    def test_negative_index_and_step_slice(self):
        ms = MultiStream([Stream() for _ in range(5)])
        assert isinstance(ms[-1], Stream)
        stepped = ms[::2]
        assert isinstance(stepped, MultiStream)
        assert len(stepped) == 3

    def test_50_streams_10_maps(self):
        sources = [Stream() for _ in range(50)]
        ms = MultiStream(sources)
        p = ms
        for _ in range(10):
            p = p.map(lambda x: x + 1)
        collected = p.to_list()
        for s in sources:
            s.emit(0)
        assert list(collected[0]) == [10]
        assert list(collected[-1]) == [10]

    def test_double_flush_produces_empty_batch(self):
        sources = [Stream() for _ in range(3)]
        ms = MultiStream(sources)
        collected = ms.collect().to_list()
        sources[0].emit(1)
        ms.flush()
        ms.flush()
        assert list(collected[0]) == [(1,), ()]

    def test_drain_empty_multistream(self):
        ms = MultiStream([])
        ret = ms.drain()
        assert ret is None

    def test_flush_before_emit_produces_empty_tuple(self):
        sources = [Stream() for _ in range(2)]
        ms = MultiStream(sources)
        collected = ms.collect().to_list()
        ms.flush()
        sources[0].emit(1)
        sources[1].emit(2)
        assert list(collected[0]) == [()]

    def test_union_unwrap_single(self):
        a = Stream()
        b = Stream()
        result = akayu.union(a, b)
        assert isinstance(result, Stream)

    def test_multistream_repr(self):
        assert repr(MultiStream([Stream(), Stream(), Stream()])) == "MultiStream(3)"
