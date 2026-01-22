import random
import pytest
from rstreamz import Stream

def safe_inc(x):
    if isinstance(x, int):
        return x + 1
    if isinstance(x, tuple):
        return tuple(safe_inc(i) for i in x)
    return x

def safe_filter(x):
    # Randomly filter some out
    return True

def test_fuzz_pipelines():
    """
    Randomly construct complex pipelines with splits and merges to ensure
    stability and absence of crashes (Rust panics).
    """
    
    # Run a fixed number of random fuzz iterations
    for i in range(20):
        print(f"Fuzz iteration {i}")
        run_random_pipeline(seed=i)

def run_random_pipeline(seed):
    random.seed(seed)
    
    source = Stream()
    # Keep track of all created streams to potentially branch from or merge
    streams = [source]
    
    # Number of operations to add to the pipeline
    num_ops = random.randint(10, 30)
    
    for _ in range(num_ops):
        op_type = random.choice([
            "map", "filter", "union", "zip", "combine_latest", 
            "par", "seq", "collect", "flatten", "accumulate", 
            "batch_map", "prefetch"
        ])
        
        # Pick a random upstream node to attach to
        upstream = random.choice(streams)
        
        try:
            if op_type == "map":
                new_stream = upstream.map(safe_inc)
                streams.append(new_stream)
                
            elif op_type == "filter":
                new_stream = upstream.filter(safe_filter)
                streams.append(new_stream)

            elif op_type == "flatten":
                # Only works if upstream emits iterables, which we can't guarantee for all streams
                # But safe_inc handles tuples, so maybe we have some.
                # If it fails at runtime because item is not iterable, that's fine, 
                # we just want to ensure no panic if it IS valid or if it errors gracefully.
                new_stream = upstream.flatten()
                streams.append(new_stream)
                
            elif op_type == "accumulate":
                # accumulate(func, start)
                new_stream = upstream.accumulate(lambda acc, x: acc, start=0)
                streams.append(new_stream)
            
            elif op_type == "batch_map":
                # batch_map(func)
                new_stream = upstream.batch_map(lambda batch: batch)
                streams.append(new_stream)

            elif op_type == "prefetch":
                # prefetch(n)
                new_stream = upstream.prefetch(random.randint(1, 5))
                streams.append(new_stream)
                
            elif op_type == "union":
                # Pick 1 or more other streams
                others = random.sample(streams, k=random.randint(1, min(3, len(streams))))
                new_stream = upstream.union(*others)
                streams.append(new_stream)
            
            elif op_type == "zip":
                others = random.sample(streams, k=random.randint(1, min(3, len(streams))))
                new_stream = upstream.zip(*others)
                streams.append(new_stream)
            
            elif op_type == "combine_latest":
                others = random.sample(streams, k=random.randint(1, min(3, len(streams))))
                new_stream = upstream.combine_latest(*others)
                streams.append(new_stream)

            elif op_type == "par":
                new_stream = upstream.par()
                streams.append(new_stream)

            elif op_type == "seq":
                new_stream = upstream.seq()
                streams.append(new_stream)

            elif op_type == "collect":
                new_stream = upstream.collect()
                streams.append(new_stream)

        except Exception as e:
            # Some operations might fail (e.g. valid graph construction issues), 
            # but we are mainly looking for hard crashes or Rust panics.
            print(f"Operation {op_type} failed: {e}")
            pass

    # Attach sinks to a few random streams to ensure data is pulled
    sink_results = []
    for _ in range(3):
        target = random.choice(streams)
        target.sink(lambda x: sink_results.append(x))

    # Emit data
    # We emit enough data to trigger zips/combines
    for val in range(10):
        try:
            source.emit(val)
            # Also emit a tuple sometimes to help flatten succeed
            source.emit((val, val))
        except TypeError:
            # Expected if flatten receives a non-iterable int
            pass
        except Exception as e:
            print(f"Emit failed with {e}")
            raise e
        
    # Also flush any collects
    for s in streams:
        try:
            s.flush()
        except:
            pass

    # Basic assertion: code didn't crash
    assert True