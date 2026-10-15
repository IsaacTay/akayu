import pytest

import akayu
from akayu import MultiStream, Stream, union


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
        for src, logs in zip(sources, server_logs, strict=True):
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
        for src, logs in zip(sources, server_logs, strict=True):
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
        for src, val in zip(sensors, readings, strict=True):
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
        for src, words in zip(sources, docs, strict=True):
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

        for i, (sa, sb, sc) in enumerate(zip(a_srcs, b_srcs, c_srcs, strict=True)):
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


# --- More application-shaped pipelines (was test_real_world_scenarios.py) ---


class TestLogProcessingPipeline:
    def test_parse_log_lines_filter_errors_count(self):
        s = Stream()
        errors = []
        s.map(lambda line: line.split(" ")[0]).filter(lambda level: level == "ERROR").sink(errors.append)
        log_lines = ["INFO starting", "ERROR failed", "WARN retry", "ERROR crash", "DEBUG done"]
        for line in log_lines:
            s.emit(line)
        assert len(errors) == 2
        assert errors == ["ERROR", "ERROR"]

    def test_multi_source_log_processing_union(self):
        s1 = Stream()
        s2 = Stream()
        combined = union(s1, s2)
        errors = []
        combined.filter(lambda line: "ERROR" in line).sink(errors.append)
        s1.emit("INFO app start")
        s2.emit("ERROR db conn")
        s1.emit("ERROR parse fail")
        s2.emit("DEBUG test")
        assert len(errors) == 2

    def test_log_processing_par_parallel_parsing(self):
        s = Stream()
        results = []
        p = s.par()
        p.map(lambda line: line.split(" ")[0]).sink(lambda x: results.append(x))
        p.map(lambda line: line.split(" ")[1]).sink(lambda x: results.append(x))
        s.emit("2024-01-15 ERROR critical")
        assert set(results) == {"2024-01-15", "ERROR"}

    def test_log_aggregation_with_accumulate(self):
        s = Stream()
        counts = {}

        def count(acc, line):
            level = line.split(" ")[0]
            acc[level] = acc.get(level, 0) + 1
            return acc

        s.accumulate(count, start={}).sink(lambda d: counts.update(d))
        s.emit("INFO test")
        s.emit("ERROR fail")
        s.emit("INFO again")
        s.emit("ERROR crash")
        s.emit("WARN warn")
        assert counts["INFO"] == 2
        assert counts["ERROR"] == 2
        assert counts["WARN"] == 1

    def test_compile_log_pipeline(self):
        s = Stream()
        results = []
        s.filter(lambda line: "ERROR" in line).map(lambda line: line.upper()).sink(results.append)
        s.compile()
        s.emit("ERROR failed")
        s.emit("INFO ok")
        s.emit("ERROR crash")
        assert results == ["ERROR FAILED", "ERROR CRASH"]


class TestETLPipelineStages:
    def test_basic_extract_transform_load(self):
        s = Stream()
        loaded = []
        s.map(lambda r: {"id": r["id"], "value": int(r["value"]) * 10}).sink(loaded.append)
        records = [{"id": "a", "value": "1"}, {"id": "b", "value": "2"}]
        s.emit_batch(records)
        assert loaded == [{"id": "a", "value": 10}, {"id": "b", "value": 20}]

    def test_etl_with_par_parallel_transforms(self):
        s = Stream()
        enriched = []
        normalized = []
        p = s.par()
        p.map(lambda r: {**r, "enriched": True}).sink(enriched.append)
        p.map(lambda r: {**r, "normalized": r["name"].lower()}).sink(normalized.append)
        s.emit({"name": "ALICE", "age": 30})
        assert len(enriched) == 1
        assert len(normalized) == 1
        assert enriched[0]["enriched"] is True
        assert normalized[0]["normalized"] == "alice"

    def test_etl_with_compile(self):
        s = Stream()
        results = []
        s.map(lambda x: x * 2).filter(lambda x: x > 5).sink(results.append)
        s.compile()
        s.emit_batch([1, 2, 3, 4, 5])
        assert results == [6, 8, 10]

    def test_etl_with_data_type_conversion(self):
        s = Stream()
        processed = []
        s.map(
            lambda row: {
                "id": int(row["id"]),
                "amount": float(row["amount"].replace("$", "")),
                "active": row["status"] == "active",
            }
        ).sink(processed.append)
        s.emit({"id": "42", "amount": "$99.50", "status": "active"})
        s.emit({"id": "17", "amount": "$10.00", "status": "inactive"})
        assert processed[0]["id"] == 42
        assert processed[0]["amount"] == 99.50
        assert processed[0]["active"] is True
        assert processed[1]["active"] is False

    def test_etl_with_batch_map_bulk_operations(self):
        s = Stream()
        results = []
        s.batch_map(lambda batch: [{"id": r["id"], "score": r["score"] * 2} for r in batch if r["score"] > 0]).sink(
            results.append
        )
        s.emit({"id": 1, "score": 5})
        s.emit({"id": 2, "score": -1})
        s.emit({"id": 3, "score": 10})
        assert results == [{"id": 1, "score": 10}, {"id": 3, "score": 20}]

    def test_etl_with_error_handling_filter_bad_records(self):
        s = Stream()
        valid = []
        s.filter(lambda r: "name" in r and r["name"] != "").map(lambda r: r["name"]).sink(valid.append)
        s.emit({"name": ""})
        s.emit({"name": "Alice"})
        s.emit({})
        s.emit({"name": "Bob"})
        assert valid == ["Alice", "Bob"]


class TestDataAggregation:
    def test_running_sum_with_accumulate(self):
        s = Stream()
        sums = []
        s.accumulate(lambda acc, x: acc + x, start=0).sink(sums.append)
        for v in [1, 2, 3, 4, 5]:
            s.emit(v)
        assert sums == [1, 3, 6, 10, 15]

    def test_running_average_with_accumulate_returns_state(self):
        s = Stream()
        avgs = []

        def avg(state, x):
            state["count"] += 1
            state["sum"] += x
            state["avg"] = state["sum"] / state["count"]
            return (state, state["avg"])

        s.accumulate(avg, start={"count": 0, "sum": 0, "avg": 0.0}, returns_state=True).sink(lambda s: avgs.append(s))
        s.emit(10)
        s.emit(20)
        s.emit(30)
        assert avgs[0] == 10.0
        assert avgs[1] == 15.0
        assert avgs[2] == 20.0

    def test_windowed_aggregation_with_chunk_by(self):
        s = Stream()
        windows = []
        s.chunk_by(lambda x: x == ";", include_delimiter=False).map(lambda chunk: sum(chunk)).sink(windows.append)
        s.emit_batch([1, 2, 3, ";", 4, 5, ";", 6])
        s.flush()
        assert windows == [6, 9, 6]

    def test_multi_level_aggregation(self):
        s = Stream()
        final = []
        s.map(lambda x: x * 2).accumulate(lambda acc, x: acc + x, start=0).sink(final.append)
        s.emit(1)
        s.emit(2)
        s.emit(3)
        assert final == [2, 6, 12]

    def test_aggregate_with_combine_latest(self):
        s1 = Stream()
        s2 = Stream()
        combined = akayu.combine_latest(s1, s2, emit_on=None)
        products = []
        combined.map(lambda pair: pair[0] * pair[1] if pair[1] is not None else None).filter(
            lambda x: x is not None
        ).sink(products.append)
        s1.emit(2)
        s2.emit(3)
        s1.emit(5)
        s2.emit(7)
        assert products == [6, 15, 35]


class TestMultiConsumer:
    def test_single_source_multiple_sinks_via_par(self):
        s = Stream()
        out1, out2, out3 = [], [], []
        p = s.par()
        p.map(lambda x: x * 2).sink(out1.append)
        p.map(lambda x: x + 1).sink(out2.append)
        p.filter(lambda x: x > 10).sink(out3.append)
        s.emit(5)
        s.emit(20)
        assert out1 == [10, 40]
        assert out2 == [6, 21]
        assert out3 == [20]

    def test_different_transforms_on_each_branch(self):
        s = Stream()
        doubled, filtered, accumulated = [], [], []
        p = s.par()
        p.map(lambda x: x * 2).sink(doubled.append)
        p.filter(lambda x: x % 2 == 0).sink(filtered.append)

        def acc(acc, x):
            return acc + x

        p.accumulate(acc, start=0).sink(accumulated.append)
        s.emit(1)
        s.emit(2)
        s.emit(3)
        assert doubled == [2, 4, 6]
        assert filtered == [2]
        assert accumulated == [1, 3, 6]

    def test_par_with_union_merge_results(self):
        s = Stream()
        p = s.par()
        branch1 = p.map(lambda x: f"A:{x}")
        branch2 = p.map(lambda x: f"B:{x}")
        merged = union(branch1, branch2)
        results = []
        merged.sink(results.append)
        s.emit(1)
        s.emit(2)
        assert len(results) == 4
        assert sorted(results) == ["A:1", "A:2", "B:1", "B:2"]

    def test_par_with_four_branches(self):
        s = Stream()
        out1, out2, out3, out4 = [], [], [], []
        p = s.par()
        p.map(lambda x: x + 1).sink(out1.append)
        p.map(lambda x: x + 2).sink(out2.append)
        p.map(lambda x: x + 3).sink(out3.append)
        p.map(lambda x: x + 4).sink(out4.append)
        s.emit(10)
        assert out1 == [11]
        assert out2 == [12]
        assert out3 == [13]
        assert out4 == [14]

    def test_multi_consumer_continuous_100_plus_emits(self):
        s = Stream()
        out1, out2, out3 = [], [], []
        p = s.par()
        p.map(lambda x: x * 2).sink(out1.append)
        p.filter(lambda x: x % 3 == 0).sink(out2.append)
        p.map(lambda x: x - 1).sink(out3.append)
        for i in range(150):
            s.emit(i)
        assert len(out1) == 150
        assert len(out2) == 50
        assert len(out3) == 150
        assert out1[149] == 298
        assert out2 == [i for i in range(150) if (i * 2) % 3 == 0]


class TestContinuousEmission:
    def test_emit_1000_values_through_map_filter_sink(self):
        s = Stream()
        results = []
        s.map(lambda x: x * 3).filter(lambda x: x % 5 == 0).sink(results.append)
        for i in range(1000):
            s.emit(i)
        assert len(results) == 200

    def test_continuous_emission_with_accumulate_verify_state(self):
        s = Stream()
        states = []
        s.accumulate(lambda acc, x: acc + x, start=0).sink(states.append)
        for i in range(1, 101):
            s.emit(i)
        assert states[-1] == 5050
        assert len(states) == 100

    def test_continuous_emission_with_batch_map(self):
        s = Stream()
        results = []
        s.batch_map(lambda batch: [x * 2 for x in batch]).sink(results.append)
        for i in range(50):
            s.emit(i)
        assert len(results) == 50
        assert results[49] == 98

    def test_continuous_emission_with_par_no_lost_data(self):
        s = Stream()
        out1, out2 = [], []
        p = s.par()
        p.map(lambda x: x * 2).sink(out1.append)
        p.map(lambda x: x * 3).sink(out2.append)
        for i in range(200):
            s.emit(i)
        assert len(out1) == 200
        assert len(out2) == 200
        assert out1[-1] == 398
        assert out2[-1] == 597

    def test_continuous_emission_with_union_two_sources(self):
        s1 = Stream()
        s2 = Stream()
        combined = union(s1, s2)
        results = []
        combined.sink(results.append)
        for i in range(100):
            s1.emit(i)
        for i in range(100, 200):
            s2.emit(i)
        assert len(results) == 200
        assert results[0] == 0
        assert results[99] == 99
        assert results[100] == 100

    def test_continuous_emission_then_compile_then_more_emits(self):
        s = Stream()
        results = []
        s.map(lambda x: x * 2).sink(results.append)
        s.compile()
        for i in range(5):
            s.emit(i)
        s.emit(999)
        assert results == [0, 2, 4, 6, 8, 1998]


class TestDataRouting:
    def test_route_data_based_on_filter_conditions(self):
        s = Stream()
        positive, negative, zero = [], [], []
        s.filter(lambda x: x > 0).sink(positive.append)
        s.filter(lambda x: x < 0).sink(negative.append)
        s.filter(lambda x: x == 0).sink(zero.append)
        for v in [-5, 0, 3, -1, 7, 0, -10]:
            s.emit(v)
        assert positive == [3, 7]
        assert negative == [-5, -1, -10]
        assert zero == [0, 0]

    def test_route_positive_negative_to_different_sinks(self):
        s = Stream()
        pos_out, neg_out = [], []
        s.filter(lambda x: x >= 0).sink(pos_out.append)
        s.filter(lambda x: x < 0).sink(neg_out.append)
        s.emit(10)
        s.emit(-5)
        s.emit(3)
        s.emit(-1)
        assert pos_out == [10, 3]
        assert neg_out == [-5, -1]

    def test_route_with_multiple_filter_stages(self):
        s = Stream()
        results = []
        s.filter(lambda x: x > 0).filter(lambda x: x % 2 == 0).map(lambda x: x // 2).sink(results.append)
        s.emit(4)
        s.emit(5)
        s.emit(6)
        s.emit(7)
        assert results == [2, 3]

    def test_route_with_chunk_by_batching(self):
        s = Stream()
        small_batches, large_batches = [], []

        def route(batch):
            if len(batch) <= 3:
                small_batches.append(batch)
            else:
                large_batches.append(batch)

        s.chunk_by(lambda x: x == ";", include_delimiter=True).sink(route)
        s.emit_batch([1, 2, ";", 3, 4, 5, 6, ";", 7])
        assert len(small_batches) == 1
        assert len(large_batches) == 1
        assert small_batches[0] == (1, 2, ";")
        assert large_batches[0] == (3, 4, 5, 6, ";")

    def test_route_with_par_parallel_routing(self):
        s = Stream()
        evens, odds = [], []
        p = s.par()
        p.filter(lambda x: x % 2 == 0).sink(evens.append)
        p.filter(lambda x: x % 2 == 1).sink(odds.append)
        for i in range(50):
            s.emit(i)
        assert len(evens) == 25
        assert len(odds) == 25


class TestNestedTransforms:
    def test_map_map_map_sink_deep_chain(self):
        s = Stream()
        result = []
        s.map(lambda x: x + 1).map(lambda x: x * 2).map(lambda x: x - 3).sink(result.append)
        s.emit(10)
        s.emit(20)
        assert result == [(10 + 1) * 2 - 3, (20 + 1) * 2 - 3]
        assert result == [19, 39]

    def test_filter_map_filter_map_sink(self):
        s = Stream()
        result = []
        s.filter(lambda x: x > 0).map(lambda x: x * 10).filter(lambda x: x >= 50).map(lambda x: x + 5).sink(
            result.append
        )
        s.emit(-5)
        s.emit(3)
        s.emit(5)
        s.emit(10)
        assert result == [55, 105]

    def test_flatten_after_map_produces_lists(self):
        s = Stream()
        result = []
        s.map(lambda x: list(range(x))).flatten().sink(result.append)
        s.emit(3)
        s.emit(1)
        s.emit(4)
        assert result == [0, 1, 2, 0, 0, 1, 2, 3]

    def test_collect_then_flatten_batch_processing(self):
        s = Stream()
        result = []
        collected = s.collect()
        collected.flatten().map(lambda x: x * 2).sink(result.append)
        s.emit(1)
        s.emit(2)
        s.emit(3)
        collected.flush()
        assert result == [2, 4, 6]

    def test_deep_chain_with_compile(self):
        s = Stream()
        result = []
        s.map(lambda x: x * 2).filter(lambda x: x > 5).map(lambda x: x + 1).accumulate(
            lambda a, x: a + x, start=0
        ).sink(result.append)
        s.compile()
        s.emit(1)
        s.emit(5)
        s.emit(3)
        assert result == [11, 18]


class TestStatefulProcessing:
    def test_accumulate_running_statistics(self):
        s = Stream()
        stats = []

        def stats_fn(acc, val):
            acc["n"] += 1
            acc["sum"] += val
            acc["mean"] = acc["sum"] / acc["n"]
            m2 = acc.get("m2", 0)
            delta = val - acc["mean"]
            acc["m2"] = m2 + delta * delta
            return acc

        s.accumulate(stats_fn, start={"n": 0, "sum": 0, "mean": 0, "m2": 0}).sink(stats.append)
        s.emit(10)
        s.emit(20)
        s.emit(30)
        assert stats[2]["n"] == 3
        assert stats[2]["sum"] == 60

    def test_accumulate_with_returns_state_complex_state(self):
        s = Stream()
        history = []

        def stateful(acc, item):
            acc["items"].append(item)
            acc["last"] = item
            return (acc, item)

        s.accumulate(stateful, start={"items": [], "last": None}, returns_state=True).sink(history.append)
        s.emit("a")
        s.emit("b")
        s.emit("c")
        assert history == ["a", "b", "c"]

    def test_chunk_by_for_batching_on_delimiter(self):
        s = Stream()
        batches = []
        s.chunk_by(lambda x: x == ";", include_delimiter=True).sink(batches.append)
        s.emit(1)
        s.emit(2)
        s.emit(";")
        s.emit(3)
        s.emit(4)
        s.emit(";")
        s.flush()
        assert len(batches) == 2
        assert batches[0] == (1, 2, ";")
        assert batches[1] == (3, 4, ";")

    def test_collect_flatten_micro_batching(self):
        s = Stream()
        processed = []
        s.collect().flatten().map(lambda x: x * 2).sink(processed.append)
        s.emit(1)
        s.emit(2)
        s.emit(3)
        s.flush()
        assert processed == [2, 4, 6]

    def test_stateful_processing_with_compile(self):
        s = Stream()
        results = []
        s.map(lambda x: x * 3).accumulate(lambda a, x: a + x, start=0).sink(results.append)
        s.compile()
        s.emit(1)
        s.emit(2)
        s.emit(3)
        assert results == [3, 9, 18]


class TestCombinatorWorkflows:
    def test_union_three_plus_sources(self):
        s1 = Stream()
        s2 = Stream()
        s3 = Stream()
        combined = union(s1, s2, s3)
        result = []
        combined.sink(result.append)
        s1.emit("a")
        s2.emit("b")
        s3.emit("c")
        s1.emit("d")
        assert len(result) == 4

    def test_combine_latest_merging_sensor_data(self):
        temp = Stream()
        humidity = Stream()
        combined = akayu.combine_latest(temp, humidity, emit_on=None)
        readings = []
        combined.map(lambda pair: f"{pair[0]}C-{pair[1]}%").sink(readings.append)
        temp.emit(25)
        humidity.emit(60)
        temp.emit(27)
        humidity.emit(65)
        assert "25C-60%" in readings
        assert "27C-65%" in readings

    def test_zip_pairing_related_streams(self):
        ids = Stream()
        names = Stream()
        zipped = akayu.zip(ids, names)
        paired = []
        zipped.map(lambda p: {"id": p[0], "name": p[1]}).sink(paired.append)
        ids.emit(1)
        names.emit("Alice")
        ids.emit(2)
        names.emit("Bob")
        assert paired[0]["id"] == 1
        assert paired[0]["name"] == "Alice"
        assert paired[1]["id"] == 2
        assert paired[1]["name"] == "Bob"

    def test_combine_latest_with_emit_on_parameter(self):
        s1 = Stream()
        s2 = Stream()
        combined = akayu.combine_latest(s1, s2, emit_on=s2)
        results = []
        combined.sink(results.append)
        s1.emit("A")
        s1.emit("B")
        s2.emit("X")
        s1.emit("C")
        s2.emit("Y")
        assert len(results) == 2
        assert results[0] == ("B", "X")
        assert results[1] == ("C", "Y")

    def test_combinators_with_downstream_transforms(self):
        s1 = Stream()
        s2 = Stream()
        combined = union(s1, s2)
        result = []
        combined.map(lambda x: x * 10).filter(lambda x: x > 50).sink(result.append)
        s1.emit(3)
        s2.emit(7)
        s1.emit(8)
        s2.emit(4)
        assert result == [70, 80]


class TestStreamingMetrics:
    def test_count_events_per_time_window_using_chunk_by(self):
        s = Stream()
        counts = []
        s.chunk_by(lambda x: x == ";", include_delimiter=True).map(len).sink(counts.append)
        s.emit_batch([1, 2, 3, ";", 4, 5, ";", 6, 7, 8, 9])
        s.flush()
        assert counts == [4, 3, 4]

    def test_calculate_running_percentiles_with_accumulate(self):
        s = Stream()
        medians = []

        def running_median(acc, val):
            acc["values"].append(val)
            acc["values"].sort()
            n = len(acc["values"])
            if n % 2 == 0:
                median = (acc["values"][n // 2 - 1] + acc["values"][n // 2]) / 2
            else:
                median = acc["values"][n // 2]
            return (acc, median)

        s.accumulate(running_median, start={"values": [], "median": 0}, returns_state=True).sink(
            lambda m: medians.append(m)
        )
        for v in [5, 1, 9, 3, 7, 2]:
            s.emit(v)
        assert medians[-1] == 4.0

    def test_track_unique_values_with_accumulate_set_state(self):
        s = Stream()
        unique_counts = []

        def track_unique(acc, val):
            acc.add(val)
            return acc

        s.accumulate(track_unique, start=set()).sink(lambda s: unique_counts.append(len(s)))
        s.emit("a")
        s.emit("b")
        s.emit("a")
        s.emit("c")
        s.emit("b")
        assert unique_counts == [1, 2, 2, 3, 3]

    def test_rate_calculation_events_per_emit_with_accumulate(self):
        s = Stream()
        rates = []

        def calc_rate(acc, val):
            acc["count"] += 1
            acc["total"] += val
            acc["rate"] = acc["total"] / acc["count"]
            return (acc, acc["rate"])

        s.accumulate(calc_rate, start={"count": 0, "total": 0, "rate": 0.0}, returns_state=True).sink(
            lambda r: rates.append(r)
        )
        s.emit(10)
        s.emit(20)
        s.emit(30)
        s.emit(40)
        assert rates == [10.0, 15.0, 20.0, 25.0]

    def test_multi_metric_pipeline_with_par(self):
        s = Stream()
        sums, counts, maxes = [], [], []
        p = s.par()
        p.accumulate(lambda a, x: a + x, start=0).sink(sums.append)
        p.map(lambda x: 1).accumulate(lambda a, x: a + x, start=0).sink(counts.append)

        def max_fn(acc, x):
            return max(acc, x)

        p.accumulate(max_fn, start=0).sink(maxes.append)
        for i in [5, 3, 9, 1, 7, 2]:
            s.emit(i)
        assert sums[-1] == 27
        assert counts[-1] == 6
        assert maxes[-1] == 9


class TestCompileOptimizationsRealWorld:
    def test_full_pipeline_compile(self):
        s = Stream()
        result = []
        s.map(lambda x: x * 2).filter(lambda x: x > 5).accumulate(lambda a, x: a + x, start=0).sink(result.append)
        s.compile()
        s.emit_batch([1, 2, 3, 4, 5])
        assert result == [6, 14, 24]

    def test_compile_with_par_branches(self):
        s = Stream()
        out1, out2 = [], []
        p = s.par()
        p.map(lambda x: x + 1).sink(out1.append)
        p.map(lambda x: x * 2).sink(out2.append)
        s.compile()
        s.emit(10)
        s.emit(20)
        assert out1 == [11, 21]
        assert out2 == [20, 40]

    def test_compile_with_union(self):
        s1 = Stream()
        s2 = Stream()
        combined = union(s1, s2)
        result = []
        combined.map(lambda x: x * 10).sink(result.append)
        s1.compile()
        s1.emit(5)
        s2.emit(3)
        assert 50 in result
        assert 30 in result

    def test_compile_with_flatten(self):
        s = Stream()
        result = []
        s.map(lambda x: list(x)).flatten().sink(result.append)
        s.compile()
        s.emit("abc")
        s.emit("xy")
        assert result == ["a", "b", "c", "x", "y"]

    def test_compile_with_chunk_by(self):
        s = Stream()
        result = []
        s.chunk_by(lambda x: x == ",", include_delimiter=True).map(tuple).sink(result.append)
        s.compile()
        s.emit_batch(list("ab,cd,e"))
        s.flush()
        assert result == [("a", "b", ","), ("c", "d", ","), ("e",)]

    def test_verify_compile_does_not_change_results(self):
        s1 = Stream()
        result1 = []
        s1.map(lambda x: x * 3).filter(lambda x: x > 5).sink(result1.append)
        s1.compile()
        s1.emit_batch([1, 2, 3, 4, 5])

        s2 = Stream()
        result2 = []
        s2.map(lambda x: x * 3).filter(lambda x: x > 5).sink(result2.append)
        s2.emit_batch([1, 2, 3, 4, 5])

        assert result1 == result2
        assert result1 == [6, 9, 12, 15]


class TestMixedWorkload:
    def test_pipeline_some_branches_compiled_some_not(self):
        s1 = Stream()
        s2 = Stream()
        out1, out2 = [], []
        s1.map(lambda x: x * 2).sink(out1.append)
        s2.map(lambda x: x + 1).sink(out2.append)
        s1.compile()
        s1.emit(5)
        s2.emit(3)
        assert out1 == [10]
        assert out2 == [4]

    def test_mix_of_par_and_sequential_transforms(self):
        s = Stream()
        result = []
        p = s.par()
        p.map(lambda x: x * 2).map(lambda x: x + 1).sink(result.append)
        p.map(lambda x: x + 10).sink(result.append)
        s.emit(5)
        assert 11 in result
        assert 15 in result

    def test_mix_of_batch_and_single_item_processing(self):
        s1 = Stream()
        s2 = Stream()
        results1 = []
        results2 = []
        s1.batch_map(lambda b: [x * 2 for x in b]).sink(results1.append)
        s2.map(lambda x: x + 100).sink(results2.append)
        s1.emit(1)
        s1.emit_batch([2, 3])
        s2.emit(1)
        assert results1 == [2, 4, 6]
        assert results2 == [101]

    def test_pipeline_multiple_compile_calls_different_streams(self):
        s1 = Stream()
        s2 = Stream()
        out1, out2 = [], []
        s1.map(lambda x: x * 2).sink(out1.append)
        s2.map(lambda x: x + 1).sink(out2.append)
        s1.compile()
        s2.compile()
        s1.emit(10)
        s2.emit(7)
        assert out1 == [20]
        assert out2 == [8]

    def test_long_pipeline_every_transform_type(self):
        s1 = Stream()
        s2 = Stream()
        out1, out2 = [], []
        s1.map(lambda x: x + 1).filter(lambda x: x > 0).map(lambda x: x * 2).sink(out1.append)
        s2.map(lambda x: [x]).flatten().sink(out2.append)
        s1.compile()
        s2.compile()
        s1.emit(5)
        s2.emit(7)
        assert out1 == [12]
        assert out2 == [7]
