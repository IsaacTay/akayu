import akayu


def test_ordering_split_union_scalar_vs_batch():
    """
    Verifies that s.emit_batch() produces the same downstream event order
    as sequential s.emit() calls in a split-merge topology.

    Topology:
           /-> Map(x*10) -
    Source                 Union -> Sink
           \-> Map(x*1)  -/

    For input [1, 2]:
    Branches emit:
    Item 1:
      B1 -> 10
      B2 -> 1
    Item 2:
      B1 -> 20
      B2 -> 2

    If Depth-First (Streamz default) is preserved:
    Output order: [10, 1, 20, 2]

    If Batched Split (Breadth-First Optimization) was used:
    Output order: [10, 20, 1, 2] -> INCORRECT for Streamz compatibility.
    """

    # 1. Scalar Reference
    s_scalar = akayu.Stream(asynchronous=False)
    out_scalar = []

    b1_s = s_scalar.map(lambda x: x * 10)
    b2_s = s_scalar.map(lambda x: x * 1)
    b1_s.union(b2_s).sink(out_scalar.append)

    s_scalar.emit(1)
    s_scalar.emit(2)

    expected_order = [10, 1, 20, 2]
    assert out_scalar == expected_order, f"Scalar reference failed: {out_scalar}"

    # 2. Batch Verification
    s_batch = akayu.Stream(asynchronous=False)
    out_batch = []

    b1_b = s_batch.map(lambda x: x * 10)
    b2_b = s_batch.map(lambda x: x * 1)
    b1_b.union(b2_b).sink(out_batch.append)

    s_batch.emit_batch([1, 2])

    assert out_batch == expected_order, (
        f"Batch ordering failed. Got {out_batch}, expected {expected_order}"
    )


def test_ordering_complex_split():
    """
    More complex nested splits.
    """
    s = akayu.Stream(asynchronous=False)
    out = []

    #      /-> x*2
    # S --|
    #      \-> x+1

    s.map(lambda x: x * 2).sink(out.append)
    s.map(lambda x: x + 1).sink(out.append)

    s.emit_batch([10, 20])

    # Item 10:
    #   Br1: 20
    #   Br2: 11
    # Item 20:
    #   Br1: 40
    #   Br2: 21

    expected = [20, 11, 40, 21]
    assert out == expected
