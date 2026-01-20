import pytest
import rstreamz
import psutil
import os
import gc
import time

def get_process_memory_mb():
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024

def run_leak_test(setup_func, run_func, iterations=50_000, allowed_growth_mb=5.0):
    gc.collect()
    time.sleep(0.1)
    
    start_mem = get_process_memory_mb()
    
    # Setup the stream/pipeline
    ctx = setup_func()
    
    # Run the processing loop
    run_func(ctx, iterations)
    
    gc.collect()
    time.sleep(0.1)
    
    end_mem = get_process_memory_mb()
    growth = end_mem - start_mem
    
    print(f"Start: {start_mem:.2f}MB, End: {end_mem:.2f}MB, Growth: {growth:.2f}MB")
    
    assert growth < allowed_growth_mb, f"Memory leak detected! Grew by {growth:.2f}MB"

def test_leak_async_false_pipeline():
    """Test standard map/filter/sink pipeline with asynchronous=False."""
    def setup():
        s = rstreamz.Stream(asynchronous=False)
        # Chain typical nodes to exercise argument packing optimizations
        s.map(lambda x: x + 1).filter(lambda x: x % 2 == 0).sink(lambda x: None)
        return s
        
    def run(s, n):
        for i in range(n):
            s.emit(i)
            
    run_leak_test(setup, run, iterations=100_000)

def test_leak_emit_batch():
    """Test repeated emit_batch calls."""
    def setup():
        s = rstreamz.Stream(asynchronous=False)
        s.map(lambda x: x).sink(lambda x: None)
        return s
        
    def run(s, n):
        # Emit 100 batches of n/100 items
        batch_size = 1000
        batch = list(range(batch_size))
        batches = n // batch_size
        for _ in range(batches):
            s.emit_batch(batch)
            
    run_leak_test(setup, run, iterations=200_000)

def test_leak_split_union_cloning():
    """Test the cloning logic in propagate_batch at splits."""
    def setup():
        s = rstreamz.Stream(asynchronous=False)
        # 3 branches to force 2 clones + 1 move in propagate_batch
        b1 = s.map(lambda x: x)
        b2 = s.map(lambda x: x)
        b3 = s.map(lambda x: x)
        # Sink all
        b1.sink(lambda x: None)
        b2.sink(lambda x: None)
        b3.sink(lambda x: None)
        return s
        
    def run(s, n):
        batch = list(range(100))
        batches = n // 100
        for _ in range(batches):
            s.emit_batch(batch)
            
    run_leak_test(setup, run, iterations=100_000)

def test_leak_accumulate_state():
    """Test accumulate which holds state, ensuring old state is dropped."""
    def setup():
        s = rstreamz.Stream(asynchronous=False)
        s.accumulate(lambda acc, x: x, start=0).sink(lambda x: None)
        return s
        
    def run(s, n):
        for i in range(n):
            s.emit(i)
            
    run_leak_test(setup, run, iterations=50_000)
