import random
import pytest
from akayu import Stream

def test_fuzz_dynamic_topology():
    """
    Randomly interleave emitting and adding nodes to verify dynamic topology safety.
    """
    for seed in range(20):
        run_dynamic_fuzz(seed)

def run_dynamic_fuzz(seed):
    random.seed(seed)
    source = Stream()
    
    # We maintain a list of active nodes to extend
    nodes = [source]
    
    for i in range(50):
        action = random.choice(["add_node", "emit"])
        
        if action == "add_node":
            parent = random.choice(nodes)
            op = random.choice(["map", "filter"])
            if op == "map":
                child = parent.map(lambda x: x + 1)
            else:
                child = parent.filter(lambda x: True)
            nodes.append(child)
            
            # Optional: add a sink to verify data flow
            if random.random() < 0.2:
                child.sink(lambda x: None)
                
        elif action == "emit":
            val = i
            try:
                source.emit(val)
            except Exception as e:
                pytest.fail(f"Emit failed at step {i} with seed {seed}: {e}")
