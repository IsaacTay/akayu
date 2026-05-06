import sys, types, pickle
from pathlib import Path
import traceback

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


def run_side(lib_name, stream_mod, source_data, side):
    # Force reimport
    for mod in list(sys.modules.keys()):
        if mod.startswith('test_example'):
            del sys.modules[mod]
    sys.modules['akayu'] = stream_mod

    from test_example.nodes import (
        sitstand_calibration_node, pose_calibration_node,
        resync_node, rel_rotmat_node, merge_node,
    )
    from test_example.types import (
        Start, Stop, ExerciseInfo, Timestamp, Rotmat, Gyro, Accel,
        Image, Mask, SegmentStart, SegmentStop,
    )
    from test_example.utils import flush_segments, segment_sit_to_stand, segment_active_flexion, segment_walking

    streamz = stream_mod

    seg_source_thigh = streamz.Stream()
    seg_source_shank = streamz.Stream()
    flat_thigh = seg_source_thigh.flatten()
    flat_shank = seg_source_shank.flatten()

    sit_exe_names = ["Sitting Calibration", "Calibration - Calibration 2 - Sitting"]
    stand_exe_names = ["Standing Calibration", "Calibration - Calibration 1 - Standing"]

    thigh_events = flat_thigh.filter(lambda x: not isinstance(x, Image)).filter(lambda x: not isinstance(x, Mask))
    shank_events = flat_shank.filter(lambda x: not isinstance(x, Image)).filter(lambda x: not isinstance(x, Mask))
    thigh_events = sitstand_calibration_node(thigh_events, sit_exe_names, stand_exe_names)
    thigh_events = pose_calibration_node(thigh_events, stand_exe_names, sit_exe_names)
    shank_events = sitstand_calibration_node(shank_events, sit_exe_names, stand_exe_names)
    shank_events = pose_calibration_node(shank_events, stand_exe_names, sit_exe_names)

    events_arr = resync_node(thigh_events, shank_events)
    events_thigh, events_shank = events_arr
    events = rel_rotmat_node(events_thigh, events_shank)

    timestamp = events.filter(isinstance, Timestamp)
    relrot = events.filter(isinstance, Rotmat)
    thigh_gyro = events_thigh.filter(isinstance, Gyro)
    shank_gyro = events_shank.filter(isinstance, Gyro)
    shank_accel = events_shank.filter(isinstance, Accel)

    relrot_thigh_gyro_ts = relrot.combine_latest(thigh_gyro, timestamp, emit_on=relrot)
    relrot_shank_gyro_ts = relrot.combine_latest(shank_gyro, shank_accel, timestamp, emit_on=relrot)

    relrot_thigh_collect = relrot_thigh_gyro_ts.collect()
    relrot_shank_collect = relrot_shank_gyro_ts.collect()

    exe_info = events.filter(isinstance, ExerciseInfo)
    exe_info.sink(relrot_thigh_collect.flush)
    exe_info.sink(relrot_shank_collect.flush)

    is_end = exe_info.map(isinstance, Stop)
    thigh_collected = is_end.zip(relrot_thigh_collect).filter(lambda x: x[0]).map(lambda x: x[1])
    shank_collected = is_end.zip(relrot_shank_collect).filter(lambda x: x[0]).map(lambda x: x[1])

    exe_stop = exe_info.filter(isinstance, Stop)
    exe_start = exe_info.filter(isinstance, Start)

    # Collect intermediate data for comparison
    thigh_samples_list = []
    shank_samples_list = []
    thigh_collected.sink(lambda x: thigh_samples_list.append(x))
    shank_collected.sink(lambda x: shank_samples_list.append(x))

    # Also track what relrot_thigh_gyro_ts produces (before collect)
    cl_samples = []
    relrot_thigh_gyro_ts.sink(lambda x: cl_samples.append(x))

    # segmentation
    sts_exe_names = ["5 Repetition Slow Sit to Stand Test"]
    active_flexion_exe_names = ["Standing Active Flexion Test"]
    fast_walk_exe_names = ["10m Fast Walk Test", "10m Fast Walk"]

    sts_names_lc = {n.strip().lower() for n in sts_exe_names}
    active_flexion_names_lc = {n.strip().lower() for n in active_flexion_exe_names}
    fast_walk_names_lc = {n.strip().lower() for n in fast_walk_exe_names}

    seg_results = []

    def trace_pick(stop, thigh_samples, shank_samples):
        name = stop.name.strip().lower()
        if name in sts_names_lc or "sit to stand" in name:
            seg_fn = segment_sit_to_stand
        elif name in active_flexion_names_lc or "active flexion" in name:
            seg_fn = segment_active_flexion
        elif name in fast_walk_names_lc:
            seg_fn = segment_walking
        else:
            seg_results.append((stop.name, 0, 'no_match', len(thigh_samples), len(shank_samples)))
            return []

        try:
            if seg_fn is segment_walking:
                result = seg_fn(shank_samples)
            else:
                rotmat_ts = [(s[0], s[-1]) for s in thigh_samples]
                result = seg_fn(rotmat_ts)
            seg_results.append((stop.name, len(result), 'ok', len(thigh_samples), len(shank_samples)))
            return result
        except Exception as e:
            seg_results.append((stop.name, 0, f'error: {e}', len(thigh_samples), len(shank_samples)))
            return []

    segments_zip = exe_stop.zip(thigh_collected, shank_collected)
    segments_out = segments_zip.starmap(trace_pick)

    exe_chunk = streamz.combine_latest(exe_start, exe_stop, emit_on=exe_stop)
    final_segments = (
        exe_chunk.zip(segments_out)
        .starmap(lambda exe_chunk, segs: [exe_chunk[0]] + segs + [exe_chunk[1]])
        .flatten()
    )

    final_collect = final_segments.collect()

    seg_source_thigh.emit(source_data[f'{side}/thigh'])
    seg_source_shank.emit(source_data[f'{side}/shank'])

    final_list = final_collect.flush() or []

    return {
        'thigh_samples': thigh_samples_list,
        'shank_samples': shank_samples_list,
        'cl_samples_count': len(cl_samples),
        'seg_results': seg_results,
        'final_list': final_list,
    }


# ---- Run both ----
results = {}
for lib_name, mod_name in [('akayu', 'akayu'), ('streamz', 'streamz')]:
    print(f"\n{'='*60}")
    print(f"  {lib_name.upper()}")
    print(f"{'='*60}")

    for mod in list(sys.modules.keys()):
        if mod.startswith('test_example'):
            del sys.modules[mod]

    mod = __import__(mod_name)
    with open(ARTIFACTS / f'{lib_name}_result.pkl', 'rb') as f:
        source = pickle.load(f)

    for side in ('left',):
        r = run_side(lib_name, mod, source, side)
        results[f'{lib_name}_{side}'] = r

        print(f"\n  {side}:")
        print(f"    combine_latest samples (before collect): {r['cl_samples_count']}")
        print(f"    thigh collected chunks: {len(r['thigh_samples'])}")
        print(f"    shank collected chunks: {len(r['shank_samples'])}")
        for i, ts in enumerate(r['thigh_samples']):
            print(f"      thigh chunk {i}: {len(ts)} samples")
        for i, ss in enumerate(r['shank_samples']):
            print(f"      shank chunk {i}: {len(ss)} samples")

        for name, nseg, status, n_thigh, n_shank in r['seg_results']:
            print(f"    segmenter: {name[:60]} → {nseg} segs ({status}) [thigh={n_thigh} shank={n_shank}]")

        seg_starts = sum(1 for e in r['final_list'] if type(e).__name__ == 'SegmentStart')
        print(f"    final: {len(r['final_list'])} events, {seg_starts} SegmentStarts")


# ---- Compare intermediate data ----
print(f"\n{'='*60}")
print(f"  COMPARISON")
print(f"{'='*60}")

for side in ('left',):
    ak = results[f'akayu_{side}']
    sz = results[f'streamz_{side}']

    print(f"\n  {side}:")
    print(f"    cl_samples: akayu={ak['cl_samples_count']} streamz={sz['cl_samples_count']}")
    print(f"    thigh chunks: akayu={len(ak['thigh_samples'])} streamz={len(sz['thigh_samples'])}")
    print(f"    shank chunks: akayu={len(ak['shank_samples'])} streamz={len(sz['shank_samples'])}")

    for i in range(min(len(ak['thigh_samples']), len(sz['thigh_samples']))):
        at = ak['thigh_samples'][i]
        st = sz['thigh_samples'][i]
        match = "SAME" if len(at) == len(st) else "DIFF"
        print(f"    thigh chunk {i}: akayu={len(at)} streamz={len(st)} [{match}]")

    print(f"\n    Segmentation comparison:")
    for i in range(max(len(ak['seg_results']), len(sz['seg_results']))):
        if i < len(ak['seg_results']) and i < len(sz['seg_results']):
            a = ak['seg_results'][i]
            s = sz['seg_results'][i]
            match = "SAME" if a[1] == s[1] and a[2] == s[2] else "DIFF"
            print(f"    {i}: akayu=({a[1]} segs, {a[2][:40]}) streamz=({s[1]} segs, {s[2][:40]}) [{match}]")
        elif i < len(ak['seg_results']):
            a = ak['seg_results'][i]
            print(f"    {i}: akayu=({a[1]} segs, {a[2][:40]}) streamz=MISSING")
        else:
            s = sz['seg_results'][i]
            print(f"    {i}: akayu=MISSING streamz=({s[1]} segs, {s[2][:40]})")
