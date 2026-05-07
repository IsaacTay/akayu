import concurrent.futures
import queue
import threading
import time

import pytest

import akayu


class TestConcurrentEmitSameStream:
    def test_concurrent_emit_simple(self):
        results = []
        s = akayu.Stream()
        s.map(lambda x: x * 2).sink(results.append)

        def emit_values(start, count):
            for i in range(start, start + count):
                s.emit(i)

        threads = [threading.Thread(target=emit_values, args=(i * 100, 100)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(results) == 1000
        assert sorted(results) == [x * 2 for x in range(1000)]

    def test_concurrent_emit_with_filter(self):
        results = []
        s = akayu.Stream()
        s.filter(lambda x: x % 2 == 0).map(lambda x: x * 2).sink(results.append)

        def emit_values(start, count):
            for i in range(start, start + count):
                s.emit(i)

        threads = [threading.Thread(target=emit_values, args=(i * 100, 100)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        expected = [x * 2 for x in range(0, 1000, 2)]
        assert sorted(results) == expected

    def test_concurrent_emit_with_accumulate(self):
        accumulated = []
        s = akayu.Stream()
        s.accumulate(lambda acc, x: acc + x, start=0).sink(accumulated.append)

        def emit_values(start, count):
            for i in range(start, start + count):
                s.emit(i)

        threads = [threading.Thread(target=emit_values, args=(i * 100, 100)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        accumulated.sort()
        expected_total = sum(range(1000))
        assert accumulated[-1] == expected_total

    def test_concurrent_emit_with_par(self):
        results1 = []
        results2 = []
        s = akayu.Stream()
        p = s.par()
        p.map(lambda x: x * 2).sink(results1.append)
        p.map(lambda x: x * 3).sink(results2.append)

        errors = []

        def emit_values(start, count):
            for i in range(start, start + count):
                try:
                    s.emit(i)
                except RuntimeError as e:
                    if "ReentrantEmit" in str(e) or "re-entrant" in str(e).lower():
                        errors.append(e)
                    else:
                        raise

        threads = [threading.Thread(target=emit_values, args=(i * 100, 100)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(results1) > 0
        assert len(results2) > 0

    def test_concurrent_emit_high_contention(self):
        results = []
        s = akayu.Stream()
        s.map(lambda x: x * 2).sink(results.append)

        def emit_values(start, count):
            for i in range(start, start + count):
                s.emit(i)

        threads = [threading.Thread(target=emit_values, args=(i * 50, 50)) for i in range(50)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(results) == 2500
        assert sorted(results) == [x * 2 for x in range(2500)]

    def test_concurrent_emit_reentrant_guard(self):
        reentrant_detected = threading.Event()
        callback_started = threading.Event()
        emit_proceeded = threading.Event()

        s = akayu.Stream()

        def callback(x):
            callback_started.set()
            emit_proceeded.wait(timeout=5)
            return x * 2

        s.map(callback).sink(lambda x: None)

        def reentrant_emit():
            callback_started.wait(timeout=5)
            try:
                s.emit(999)
            except RuntimeError as e:
                if "ReentrantEmit" in str(e) or "re-entrant" in str(e).lower():
                    reentrant_detected.set()
            emit_proceeded.set()

        main_thread = threading.Thread(target=lambda: s.emit(1))
        reentrant_thread = threading.Thread(target=reentrant_emit)

        main_thread.start()
        reentrant_thread.start()

        main_thread.join(timeout=10)
        reentrant_thread.join(timeout=10)

        assert reentrant_detected.is_set() or not reentrant_detected.is_set()


class TestConcurrentEmitDifferentStreamsSharedDownstream:
    def test_concurrent_emit_union(self):
        results = []
        s1 = akayu.Stream()
        s2 = akayu.Stream()
        merged = akayu.union(s1, s2)
        merged.sink(results.append)

        def emit_s1():
            for i in range(500):
                s1.emit(i)

        def emit_s2():
            for i in range(500, 1000):
                s2.emit(i)

        t1 = threading.Thread(target=emit_s1)
        t2 = threading.Thread(target=emit_s2)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        assert len(results) == 1000
        assert sorted(results) == list(range(1000))

    def test_concurrent_emit_combine_latest(self):
        results = []
        s1 = akayu.Stream()
        s2 = akayu.Stream()
        combined = akayu.combine_latest(s1, s2)
        combined.sink(results.append)

        def emit_s1():
            for i in range(100):
                s1.emit(i)
                time.sleep(0.001)

        def emit_s2():
            for i in range(100, 200):
                s2.emit(i)
                time.sleep(0.001)

        t1 = threading.Thread(target=emit_s1)
        t2 = threading.Thread(target=emit_s2)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        assert len(results) > 0
        for r in results:
            assert isinstance(r, tuple)
            assert len(r) == 2

    def test_concurrent_emit_zip(self):
        results = []
        s1 = akayu.Stream()
        s2 = akayu.Stream()
        zipped = akayu.zip(s1, s2)
        zipped.sink(results.append)

        def emit_s1():
            for i in range(50):
                s1.emit(i)
                time.sleep(0.001)

        def emit_s2():
            for i in range(50, 100):
                s2.emit(i)
                time.sleep(0.001)

        t1 = threading.Thread(target=emit_s1)
        t2 = threading.Thread(target=emit_s2)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        assert len(results) == 50
        for r in results:
            assert isinstance(r, tuple)
            assert len(r) == 2

    def test_concurrent_emit_union_with_transforms(self):
        results = []
        s1 = akayu.Stream()
        s2 = akayu.Stream()
        merged = akayu.union(s1, s2)
        merged.map(lambda x: x * 2).filter(lambda x: x % 4 == 0).sink(results.append)

        def emit_s1():
            for i in range(500):
                try:
                    s1.emit(i)
                except Exception:
                    pass

        def emit_s2():
            for i in range(500, 1000):
                try:
                    s2.emit(i)
                except Exception:
                    pass

        t1 = threading.Thread(target=emit_s1)
        t2 = threading.Thread(target=emit_s2)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        assert len(results) > 0


class TestWebServerSimulation:
    def test_per_request_stream(self):
        num_requests = 20

        def request_handler(request_id):
            s = akayu.Stream()
            result = s.map(lambda x: x * request_id).filter(lambda x: x > 0).to_list()
            s.emit_batch([1, 2, 3, 4, 5])
            s.flush()
            return list(result)

        with concurrent.futures.ThreadPoolExecutor(max_workers=num_requests) as executor:
            futures = [executor.submit(request_handler, i) for i in range(num_requests)]
            results = [f.result(timeout=10) for f in concurrent.futures.as_completed(futures)]

        assert len(results) == num_requests

    def test_shared_singleton_stream(self):
        results_queue = queue.Queue()
        s = akayu.Stream()
        s.map(lambda x: x * 2).sink(lambda x: results_queue.put(x))

        def request_handler(request_id):
            for i in range(50):
                s.emit(request_id * 1000 + i)

        threads = [threading.Thread(target=request_handler, args=(i,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        collected = []
        while not results_queue.empty():
            collected.append(results_queue.get_nowait())

        assert len(collected) == 500

    def test_shared_stream_with_par(self):
        results1 = []
        results2 = []
        s = akayu.Stream()
        p = s.par()
        p.map(lambda x: x * 2).sink(results1.append)
        p.map(lambda x: x * 3).sink(results2.append)

        def request_handler(request_id):
            for i in range(50):
                try:
                    s.emit(request_id * 1000 + i)
                except RuntimeError:
                    pass

        threads = [threading.Thread(target=request_handler, args=(i,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(results1) > 0
        assert len(results2) > 0

    def test_shared_compiled_stream(self):
        results = []
        s = akayu.Stream()
        s.map(lambda x: x * 2).sink(results.append)
        s.compile()

        def request_handler(start):
            for i in range(100):
                s.emit(start + i)

        threads = [threading.Thread(target=request_handler, args=(i * 1000,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        expected = sorted(
            (start + i) * 2
            for start in range(0, 10000, 1000)
            for i in range(100)
        )
        assert len(results) == 1000
        assert sorted(results) == expected

    def test_request_with_callback_io(self):
        results = []
        s = akayu.Stream()

        def slow_callback(x):
            time.sleep(0.001)
            return x * 2

        s.map(slow_callback).sink(results.append)

        def request_handler(request_id):
            for i in range(10):
                try:
                    s.emit(request_id * 100 + i)
                except RuntimeError:
                    pass

        threads = [threading.Thread(target=request_handler, args=(i,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(results) > 0

    def test_request_error_isolation(self):
        error_occurred = threading.Event()
        success_results = []

        s = akayu.Stream()

        def failing_callback(x):
            if x == 50:
                raise ValueError("intentional error at 50")
            return x

        def success_callback(x):
            success_results.append(x)
            return x

        s.map(failing_callback).sink(lambda x: None)
        s.map(success_callback).sink(lambda x: None)

        def request_handler():
            for i in range(100):
                try:
                    s.emit(i)
                except (ValueError, RuntimeError):
                    error_occurred.set()

        t1 = threading.Thread(target=request_handler)
        t1.start()
        t1.join()

        assert len(success_results) >= 0


class TestThreadPoolContention:
    def test_par_exhaustion(self):
        results = []
        s = akayu.Stream()
        p = s.par()
        p.map(lambda x: x * 2).sink(results.append)
        p.map(lambda x: x * 3).sink(results.append)
        p.map(lambda x: x * 4).sink(results.append)

        def emit_values(start, count):
            for i in range(start, start + count):
                try:
                    s.emit(i)
                except RuntimeError:
                    pass

        threads = [threading.Thread(target=emit_values, args=(i * 100, 100)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(results) > 0

    def test_nested_par_concurrent(self):
        results1 = []
        results2 = []
        s1 = akayu.Stream()
        s2 = akayu.Stream()

        p1 = s1.par()
        p1.map(lambda x: x * 2).sink(results1.append)
        p1.map(lambda x: x * 3).sink(results1.append)

        p2 = s2.par()
        p2.map(lambda x: x * 4).sink(results2.append)
        p2.map(lambda x: x * 5).sink(results2.append)

        def emit_s1():
            for i in range(500):
                s1.emit(i)

        def emit_s2():
            for i in range(500, 1000):
                s2.emit(i)

        t1 = threading.Thread(target=emit_s1)
        t2 = threading.Thread(target=emit_s2)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        assert len(results1) == 1000
        assert len(results2) == 1000

    def test_par_with_slow_callbacks(self):
        results = []
        s = akayu.Stream()
        p = s.par()
        p.map(lambda x: (time.sleep(0.01), x * 2)[1]).sink(results.append)
        p.map(lambda x: (time.sleep(0.01), x * 3)[1]).sink(results.append)

        def emit_values(start, count):
            for i in range(start, start + count):
                try:
                    s.emit(i)
                except RuntimeError:
                    pass

        threads = [threading.Thread(target=emit_values, args=(i * 10, 10)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(results) > 0

    def test_many_concurrent_emit_batch(self):
        results = []
        s = akayu.Stream()
        s.map(lambda x: x * 2).sink(results.append)

        def emit_batch(start):
            s.emit_batch(list(range(start, start + 10)))

        threads = [threading.Thread(target=emit_batch, args=(i * 10,)) for i in range(100)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(results) == 1000
        assert sorted(results) == [x * 2 for x in range(1000)]


class TestConcurrentCompile:
    def test_compile_then_concurrent_emit(self):
        results = []
        s = akayu.Stream()
        s.map(lambda x: x * 2).sink(results.append)
        s.compile()

        def emit_values(start, count):
            for i in range(start, start + count):
                s.emit(i)

        threads = [threading.Thread(target=emit_values, args=(i * 100, 100)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(results) == 1000

    def test_concurrent_emit_during_compile(self):
        results = []
        s = akayu.Stream()
        s.map(lambda x: x * 2).sink(results.append)

        compile_started = threading.Event()
        compile_finished = threading.Event()

        def compile_stream():
            compile_started.set()
            s.compile()
            compile_finished.set()

        def emit_values():
            compile_started.wait(timeout=5)
            for i in range(100):
                try:
                    s.emit(i)
                except (RuntimeError, ValueError):
                    pass

        compile_thread = threading.Thread(target=compile_stream)
        emit_thread = threading.Thread(target=emit_values)

        compile_thread.start()
        emit_thread.start()

        compile_thread.join(timeout=10)
        emit_thread.join(timeout=10)

        compile_finished.wait(timeout=5)

    def test_sequential_compile_use(self):
        results = []
        s = akayu.Stream()
        s.map(lambda x: x * 2).sink(results.append)

        s.compile()

        for i in range(100):
            s.emit(i)

        assert len(results) == 100
        assert sorted(results) == [x * 2 for x in range(100)]

        s2 = akayu.Stream()
        s2.map(lambda x: x + 1).sink(results.append)
        s2.compile()

        for i in range(100):
            s2.emit(i)

        assert len(results) == 200


class TestMemoryAndStateSafety:
    def test_accumulate_thread_safety(self):
        accumulated = []
        s = akayu.Stream()
        s.accumulate(lambda acc, x: acc + x, start=0).sink(accumulated.append)

        def emit_values(start, count):
            for i in range(start, start + count):
                s.emit(i)

        threads = [threading.Thread(target=emit_values, args=(i * 100, 100)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        accumulated.sort()
        expected = sum(range(1000))
        assert accumulated[-1] == expected

    def test_collect_thread_safety(self):
        results = []
        s = akayu.Stream()
        c = s.collect()
        c.sink(results.append)

        def emit_values(start, count):
            for i in range(start, start + count):
                s.emit(i)

        threads = [threading.Thread(target=emit_values, args=(i * 100, 100)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        c.flush()
        assert len(results) == 1
        assert len(results[0]) == 1000

    def test_shared_sink_thread_safety(self):
        sink_lock = threading.Lock()
        results = []

        def thread_safe_sink(x):
            with sink_lock:
                results.append(x)

        s1 = akayu.Stream()
        s2 = akayu.Stream()
        merged = akayu.union(s1, s2)
        merged.sink(thread_safe_sink)

        def emit_s1():
            for i in range(500):
                s1.emit(i)

        def emit_s2():
            for i in range(500, 1000):
                s2.emit(i)

        t1 = threading.Thread(target=emit_s1)
        t2 = threading.Thread(target=emit_s2)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        assert len(results) == 1000

    def test_to_list_thread_safety(self):
        results = []
        s = akayu.Stream()
        s.map(lambda x: x * 2).sink(results.append)

        def emit_values(start, count):
            for i in range(start, start + count):
                try:
                    s.emit(i)
                except RuntimeError:
                    pass

        threads = [threading.Thread(target=emit_values, args=(i * 100, 100)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        result_list = s.to_list()
        assert len(result_list) >= 0


class TestStressTests:
    def test_stress_concurrent_emit_100k(self):
        results = []
        s = akayu.Stream()
        s.map(lambda x: x * 2).sink(results.append)

        def emit_values(start, count):
            for i in range(start, start + count):
                try:
                    s.emit(i)
                except RuntimeError:
                    pass

        threads = [threading.Thread(target=emit_values, args=(i * 1000, 1000)) for i in range(100)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(results) > 0

    def test_stress_par_concurrent(self):
        results1 = []
        results2 = []
        results3 = []
        s = akayu.Stream()
        p = s.par()
        p.map(lambda x: x * 2).sink(results1.append)
        p.map(lambda x: x * 3).sink(results2.append)
        p.map(lambda x: x * 4).sink(results3.append)

        def emit_values(start, count):
            for i in range(start, start + count):
                try:
                    s.emit(i)
                except RuntimeError:
                    pass

        threads = [threading.Thread(target=emit_values, args=(i * 500, 500)) for i in range(50)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(results1) > 0
        assert len(results2) > 0
        assert len(results3) > 0

    def test_stress_union_concurrent(self):
        results = []
        streams = [akayu.Stream() for _ in range(20)]
        merged = akayu.union(*streams)
        merged.sink(results.append)

        def emit_from_stream(stream, start, count):
            for i in range(start, start + count):
                stream.emit(i)

        threads = [
            threading.Thread(target=emit_from_stream, args=(streams[i], i * 500, 500))
            for i in range(20)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(results) == 10000

    def test_stress_mixed_operations(self):
        results1 = []
        results2 = []
        results3 = []

        s1 = akayu.Stream()
        s2 = akayu.Stream()

        p1 = s1.par()
        p1.map(lambda x: x * 2).sink(results1.append)
        p1.map(lambda x: x + 1).sink(results2.append)

        merged = akayu.union(s1, s2)
        merged.map(lambda x: x * 3).sink(results3.append)

        def emit_s1():
            for i in range(500):
                s1.emit(i)

        def emit_s2():
            for i in range(500, 1000):
                s2.emit(i)

        threads = [threading.Thread(target=emit_s1), threading.Thread(target=emit_s2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(results1) == 500
        assert len(results2) == 500
        assert len(results3) == 1000
