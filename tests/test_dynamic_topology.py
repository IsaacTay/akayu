from rstreamz import Stream

def test_add_node_after_emit():
    """Test adding a node to source after an emit."""
    source = Stream()
    results1 = []
    
    # partial build
    n1 = source.map(lambda x: x + 1)
    n1.sink(results1.append)
    
    # emit 1
    source.emit(1)
    assert results1 == [2]
    
    # add more to pipeline
    results2 = []
    n2 = source.map(lambda x: x * 10)
    n2.sink(results2.append)
    
    # emit 2 - should go to both
    source.emit(2)
    assert results1 == [2, 3]
    assert results2 == [20]

def test_add_chain_after_emit():
    """Test extending a chain after emit."""
    source = Stream()
    results = []
    
    n1 = source.map(lambda x: x + 1)
    
    # emit before n1 has downstream
    source.emit(1)
    
    # now extend n1
    n1.sink(results.append)
    
    # emit again
    source.emit(2)
    
    # n1 processes 2 -> 3, sinks to results
    assert results == [3]

def test_split_optimization_interleaved():
    """Test if optimization logic breaks when adding nodes interleaved with emits."""
    source = Stream()
    r1 = []
    r2 = []
    
    # Branch 1
    n1 = source.map(lambda x: x + 1) # map1
    n1.map(lambda x: x * 2).sink(r1.append) # map2, should fuse with map1 if optimized
    
    # Emit - triggers optimization on source and reachable nodes
    source.emit(1) # 1 -> +1 -> *2 = 4
    assert r1 == [4]
    
    # Add Branch 2 to n1 (which might have been fused/modified?)
    # If n1 was fused into a composed node, can we still add to it?
    # In rstreamz implementation, fusion modifies the *upstream* node to contain the composed logic
    # and points to the *downstream* node's downstreams.
    # Wait, if n1 -> n2. Fuse n1+n2.
    # n1.logic becomes composed. n1.downstreams becomes n2.downstreams.
    # n1 effectively swallows n2.
    # If I now do n1.map(...), it adds to n1.downstreams.
    
    n1.map(lambda x: x + 10).sink(r2.append)
    
    source.emit(2) 
    # Path 1: 2 -> n1(composed) -> r1.append. 
    # n1(composed) does (x+1)*2. So 2 -> 3 -> 6. r1 gets 6.
    
    # Path 2: n1 also has new downstream.
    # Does n1's new logic ((x+1)*2) apply to the new downstream?
    # Original n1 was just (x+1).
    # If n1 fused with its child, n1 NOW does (x+1)*2.
    # If we attach a new child to n1, that child receives the output of n1.
    # If n1 is now (x+1)*2, the new child gets (x+1)*2.
    # But originally n1 was just (x+1).
    # So we effectively CHANGED the definition of n1 for future subscribers!
    # This is the bug.
    
    assert r1 == [4, 6]
    # Expected for r2: 2 -> n1(original x+1) -> +10 -> 13.
    # Actual (likely): 2 -> n1(fused x+1 then *2) -> +10 -> 16.
    assert r2 == [13] 
