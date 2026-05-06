import sys, types, pickle
from pathlib import Path
import numpy as np

ARTIFACTS = Path('test_artifacts')

class FakeTensor: pass
class FakeDevice: pass

class MockMod(types.ModuleType):
    def __getattr__(self, name):
        if name.startswith('_'):
            raise AttributeError(name)
        v = MockMod(f'{self.__name__}.{name}')
        setattr(self, name, v)
        return v
    def __call__(self, *a, **kw):
        return None

torch_mod = types.ModuleType('torch')
torch_mod.Tensor = FakeTensor
torch_mod.device = FakeDevice
torch_mod.nn = MockMod('torch.nn')
torch_mod.optim = MockMod('torch.optim')

for k in ['torchvision', 'torchvision.transforms', 'torchvision.transforms.v2',
           'torchvision.transforms.v2.functional', 'segmentation_models_pytorch',
           'src', 'src.ultrasound']:
    sys.modules[k] = MockMod(k)
sys.modules['torch'] = torch_mod


def make_default_flow_graph(streamz_lib, segment_events_list, trace_sinks=None):
    from test_example.nodes import (
        sitstand_calibration_node, pose_calibration_node,
        resync_node, rel_rotmat_node, merge_node,
    )
    from test_example.types import (
        Start, Stop, ExerciseInfo, Timestamp, Rotmat, Gyro, Accel,
        Image, Mask, SegmentStart, SegmentStop,
    )
    from test_example.utils import flush_segments, segment_sit_to_stand, segment_active_flexion, segment_walking

    streamz = streamz_lib

    segment_source = {k: streamz.Stream() for k in segment_events_list.keys()}
    segment_events = {k: events.flatten() for k, events in segment_source.items()}

    sit_exe_names = ["Sitting Calibration", "Calibration - Calibration 2 - Sitting"]
    stand_exe_names = ["Standing Calibration", "Calibration - Calibration 1 - Standing"]
    sts_exe_names = ["5 Repetition Slow Sit to Stand Test"]
    active_flexion_exe_names = ["Standing Active Flexion Test"]
    fast_walk_exe_names = ["10m Fast Walk Test", "10m Fast Walk"]

    for segment, events in segment_events.items():
        events = events.filter(lambda x: not isinstance(x, Image))
        events = events.filter(lambda x: not isinstance(x, Mask))
        events = sitstand_calibration_node(events, sit_exe_names, stand_exe_names)
        events = pose_calibration_node(events, stand_exe_names, sit_exe_names)
        segment_events[segment] = events

    sides = set(k.split("/")[0] for k in segment_events.keys())
    trace = trace_sinks or {}

    for side in sides:
        events_arr = [
            segment_events.get(f"{side}/{segment}")
            for segment in ("thigh", "shank")
        ]
        events_arr = resync_node(*events_arr)
        events_thigh, events_shank = events_arr
        events = rel_rotmat_node(events_thigh, events_shank)

        timestamp = events.filter(isinstance, Timestamp)
        relrot = events.filter(isinstance, Rotmat)
        thigh_gyro = events_thigh.filter(isinstance, Gyro)
        shank_gyro = events_shank.filter(isinstance, Gyro)
        shank_accel = events_shank.filter(isinstance, Accel)

        # Trace: count relrot events
        relrot_count = [0]
        relrot.sink(lambda x, c=relrot_count: c.__setitem__(0, c[0]+1))

        relrot_thigh_gyro_ts = relrot.combine_latest(thigh_gyro, timestamp, emit_on=relrot)
        relrot_shank_gyro_ts = relrot.combine_latest(shank_gyro, shank_accel, timestamp, emit_on=relrot)

        # Trace: count combine_latest outputs
        cl_thigh_count = [0]
        cl_shank_count = [0]
        relrot_thigh_gyro_ts.sink(lambda x, c=cl_thigh_count: c.__setitem__(0, c[0]+1))
        relrot_shank_gyro_ts.sink(lambda x, c=cl_shank_count: c.__setitem__(0, c[0]+1))

        relrot_thigh_collect = relrot_thigh_gyro_ts.collect()
        relrot_shank_collect = relrot_shank_gyro_ts.collect()

        exe_info = events.filter(isinstance, ExerciseInfo)
        exe_info.sink(relrot_thigh_collect.flush)
        exe_info.sink(relrot_shank_collect.flush)

        is_end = exe_info.map(isinstance, Stop)
        relrot_thigh_gyro_ts_collected = (
            is_end.zip(relrot_thigh_collect)
            .filter(lambda x: x[0])
            .map(lambda x: x[1])
        )
        relrot_shank_gyro_ts_collected = (
            is_end.zip(relrot_shank_collect)
            .filter(lambda x: x[0])
            .map(lambda x: x[1])
        )

        # Trace: count collected chunks
        thigh_chunks = []
        shank_chunks = []
        relrot_thigh_gyro_ts_collected.sink(lambda x, l=thigh_chunks: l.append(len(x)))
        relrot_shank_gyro_ts_collected.sink(lambda x, l=shank_chunks: l.append(len(x)))

        exe_stop = exe_info.filter(isinstance, Stop)

        sts_names_lc = {n.strip().lower() for n in sts_exe_names}
        active_flexion_names_lc = {n.strip().lower() for n in active_flexion_exe_names}
        fast_walk_names_lc = {n.strip().lower() for n in fast_walk_exe_names}

        seg_trace = []

        def pick_segment_samples(stop, thigh_samples, shank_samples, _trace=seg_trace,
                                  _sts=sts_names_lc, _af=active_flexion_names_lc,
                                  _fw=fast_walk_names_lc):
            name = stop.name.strip().lower()
            if name in _sts or "sit to stand" in name:
                seg_fn = segment_sit_to_stand
            elif name in _af or "active flexion" in name:
                seg_fn = segment_active_flexion
            elif name in _fw:
                seg_fn = segment_walking
            else:
                _trace.append((stop.name, 0, 'no_match', len(thigh_samples), len(shank_samples)))
                return []

            try:
                if seg_fn is segment_walking:
                    result = seg_fn(shank_samples)
                else:
                    rotmat_ts = [(s[0], s[-1]) for s in thigh_samples]
                    result = seg_fn(rotmat_ts)
                _trace.append((stop.name, len(result), 'ok', len(thigh_samples), len(shank_samples)))
                return result
            except Exception as e:
                _trace.append((stop.name, 0, f'error:{e}', len(thigh_samples), len(shank_samples)))
                return []

        segments = exe_stop.zip(
            relrot_thigh_gyro_ts_collected, relrot_shank_gyro_ts_collected
        ).starmap(pick_segment_samples)

        exe_start = exe_info.filter(isinstance, Start)
        exe_chunk = streamz.combine_latest(exe_start, exe_stop, emit_on=exe_stop)
        segments = (
            exe_chunk.zip(segments)
            .starmap(lambda exe_chunk, segs: [exe_chunk[0]] + segs + [exe_chunk[1]])
            .flatten()
        )

        events = merge_node(segments, events)
        segment_events[side] = events

        trace[side] = {
            'relrot_count': relrot_count,
            'cl_thigh_count': cl_thigh_count,
            'cl_shank_count': cl_shank_count,
            'thigh_chunks': thigh_chunks,
            'shank_chunks': shank_chunks,
            'seg_trace': seg_trace,
        }

    flushed = flush_segments(segment_events)

    for k, events in segment_source.items():
        events.emit(segment_events_list[k])

    result = flushed.flush()
    return result, trace


def run(lib_name, mod_name, pkl_name):
    for mod in list(sys.modules.keys()):
        if mod.startswith('test_example'):
            del sys.modules[mod]

    lib = __import__(mod_name)
    sys.modules['akayu'] = lib

    with open(ARTIFACTS / pkl_name, 'rb') as f:
        source = pickle.load(f)

    trace = {}
    result, trace = make_default_flow_graph(lib, source, trace)

    print(f"\n{'='*60}")
    print(f"  {lib_name.upper()}")
    print(f"{'='*60}")

    for side in ('left', 'right'):
        t = trace.get(side, {})
        print(f"\n  {side}:")
        print(f"    relrot events: {t.get('relrot_count', [0])[0]}")
        print(f"    combine_latest thigh: {t.get('cl_thigh_count', [0])[0]}")
        print(f"    combine_latest shank: {t.get('cl_shank_count', [0])[0]}")
        print(f"    collected thigh chunks ({len(t.get('thigh_chunks', []))}): {t.get('thigh_chunks', [])}")
        print(f"    collected shank chunks ({len(t.get('shank_chunks', []))}): {t.get('shank_chunks', [])}")

        for name, nseg, status, nt, ns in t.get('seg_trace', []):
            print(f"    seg: {name[:55]} → {nseg} segs ({status}) [t={nt} s={ns}]")

        seg_list = result.get(side, [])
        seg_starts = sum(1 for e in seg_list if type(e).__name__ == 'SegmentStart')
        print(f"    final: {len(seg_list)} events, {seg_starts} SegmentStarts")

    return trace


ak_trace = run('akayu', 'akayu', 'akayu_result.pkl')
sz_trace = run('streamz', 'streamz', 'streamz_result.pkl')

print(f"\n{'='*60}")
print(f"  COMPARISON")
print(f"{'='*60}")

for side in ('left', 'right'):
    ak = ak_trace.get(side, {})
    sz = sz_trace.get(side, {})
    print(f"\n  {side}:")
    print(f"    relrot:       akayu={ak.get('relrot_count', [0])[0]:>6}  streamz={sz.get('relrot_count', [0])[0]:>6}")
    print(f"    cl_thigh:     akayu={ak.get('cl_thigh_count', [0])[0]:>6}  streamz={sz.get('cl_thigh_count', [0])[0]:>6}")
    print(f"    cl_shank:     akayu={ak.get('cl_shank_count', [0])[0]:>6}  streamz={sz.get('cl_shank_count', [0])[0]:>6}")
    print(f"    thigh_chunks: akayu={ak.get('thigh_chunks', [])}  streamz={sz.get('thigh_chunks', [])}")
    print(f"    shank_chunks: akayu={ak.get('shank_chunks', [])}  streamz={sz.get('shank_chunks', [])}")

    ak_segs = ak.get('seg_trace', [])
    sz_segs = sz.get('seg_trace', [])
    max_i = max(len(ak_segs), len(sz_segs))
    for i in range(max_i):
        a = ak_segs[i] if i < len(ak_segs) else ('?', '?', 'MISSING', '?', '?')
        s = sz_segs[i] if i < len(sz_segs) else ('?', '?', 'MISSING', '?', '?')
        match = "✓" if a[1] == s[1] and a[2] == s[2] else "✗"
        print(f"    seg[{i}] {match}: ak=({a[1]} segs,{a[2][:35]}) sz=({s[1]} segs,{s[2][:35]})")
