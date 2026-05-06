import sys, types, pickle
from pathlib import Path
import numpy as np
from types import SimpleNamespace

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

def make_default_flow_graph(streamz_lib, segment_events_list):
    """Build default_flow graph (sans bone/uls/pivot_shift) using given stream library."""
    streamz = streamz_lib
    
    from test_example.nodes import (
        sitstand_calibration_node, pose_calibration_node,
        resync_node, rel_rotmat_node, merge_node,
    )
    from test_example.types import (
        Start, Stop, ExerciseInfo, Timestamp, Rotmat, Gyro, Accel,
        Image, Mask, SegmentStart, SegmentStop,
    )
    from test_example.utils import flush_segments, segment_sit_to_stand, segment_active_flexion, segment_walking
    
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
        relrot_thigh_gyro_ts = relrot.combine_latest(
            thigh_gyro, timestamp, emit_on=relrot
        )
        relrot_shank_gyro_ts = relrot.combine_latest(
            shank_gyro, shank_accel, timestamp, emit_on=relrot
        )
        relrot_thigh_gyro_ts = relrot_thigh_gyro_ts.collect()
        relrot_shank_gyro_ts = relrot_shank_gyro_ts.collect()
        
        exe_info = events.filter(isinstance, ExerciseInfo)
        exe_info.sink(relrot_thigh_gyro_ts.flush)
        exe_info.sink(relrot_shank_gyro_ts.flush)
        
        is_end = exe_info.map(isinstance, Stop)
        relrot_thigh_gyro_ts = (
            is_end.zip(relrot_thigh_gyro_ts)
            .filter(lambda x: x[0])
            .map(lambda x: x[1])
        )
        relrot_shank_gyro_ts = (
            is_end.zip(relrot_shank_gyro_ts)
            .filter(lambda x: x[0])
            .map(lambda x: x[1])
        )
        exe_stop = exe_info.filter(isinstance, Stop)
        
        sts_names_lc = {n.strip().lower() for n in sts_exe_names}
        active_flexion_names_lc = {n.strip().lower() for n in active_flexion_exe_names}
        fast_walk_names_lc = {n.strip().lower() for n in fast_walk_exe_names}
        
        def pick_segmenter(exe_name):
            name = exe_name.strip().lower()
            if name in sts_names_lc or "sit to stand" in name:
                return segment_sit_to_stand
            if name in active_flexion_names_lc or "active flexion" in name:
                return segment_active_flexion
            if name in fast_walk_names_lc:
                return segment_walking
            return lambda _: []
        
        def pick_segment_samples(stop_name, thigh_samples, shank_samples):
            segmenter = pick_segmenter(stop_name)
            if segmenter is segment_walking:
                return segmenter(shank_samples)
            rotmat_ts = [(s[0], s[-1]) for s in thigh_samples]
            return segmenter(rotmat_ts)
        
        def safe_pick(stop, thigh_samples, shank_samples):
            try:
                return pick_segment_samples(stop.name, thigh_samples, shank_samples)
            except Exception as e:
                return []
        
        segments = exe_stop.zip(relrot_thigh_gyro_ts, relrot_shank_gyro_ts).starmap(safe_pick)
        
        exe_start = exe_info.filter(isinstance, Start)
        exe_chunk = streamz.combine_latest(exe_start, exe_stop, emit_on=exe_stop)
        segments = (
            exe_chunk.zip(segments)
            .starmap(lambda exe_chunk, segs: [exe_chunk[0]] + segs + [exe_chunk[1]])
            .flatten()
        )
        
        events = merge_node(segments, events)
        segment_events[side] = events
    
    flushed = flush_segments(segment_events)
    
    for k, events in segment_source.items():
        events.emit(segment_events_list[k])
    
    result = flushed.flush()
    return result


def summarize(result, label):
    print(f"\n=== {label} ===")
    for seg, events in result.items():
        starts = [e for e in events if type(e).__name__ == 'Start']
        stops = [e for e in events if type(e).__name__ == 'Stop']
        seg_starts = [e for e in events if type(e).__name__ == 'SegmentStart']
        seg_stops = [e for e in events if type(e).__name__ == 'SegmentStop']
        print(f'  {seg}: {len(events)} events, {len(starts)} starts, {len(stops)} stops, '
              f'{len(seg_starts)} seg_starts, {len(seg_stops)} seg_stops')


# ---- Run with AKAYU ----
print("=== AKAYU default_flow ===")
for mod in list(sys.modules.keys()):
    if mod.startswith('test_example'):
        del sys.modules[mod]

import akayu as akayu_lib
sys.modules['akayu'] = akayu_lib

with open(ARTIFACTS / 'akayu_result.pkl', 'rb') as f:
    akayu_source = pickle.load(f)

from test_example.nodes import (
    sitstand_calibration_node, pose_calibration_node,
    resync_node, rel_rotmat_node, merge_node,
)
from test_example.types import (
    Start, Stop, ExerciseInfo, Timestamp, Rotmat, Gyro, Accel,
    Image, Mask, SegmentStart, SegmentStop,
)
from test_example.utils import flush_segments

akayu_default = make_default_flow_graph(akayu_lib, akayu_source)
summarize(akayu_default, "AKAYU default_flow result")
with open(ARTIFACTS / 'akayu_default.pkl', 'wb') as f:
    pickle.dump(akayu_default, f)


# ---- Run with STREAMZ ----
print("\n=== STREAMZ default_flow ===")
for mod in list(sys.modules.keys()):
    if mod.startswith('test_example'):
        del sys.modules[mod]

import streamz as streamz_lib
sys.modules['akayu'] = streamz_lib

with open(ARTIFACTS / 'streamz_result.pkl', 'rb') as f:
    streamz_source = pickle.load(f)

from test_example.nodes import (
    sitstand_calibration_node, pose_calibration_node,
    resync_node, rel_rotmat_node, merge_node,
)
from test_example.types import (
    Start, Stop, ExerciseInfo, Timestamp, Rotmat, Gyro, Accel,
    Image, Mask, SegmentStart, SegmentStop,
)
from test_example.utils import flush_segments

streamz_default = make_default_flow_graph(streamz_lib, streamz_source)
summarize(streamz_default, "STREAMZ default_flow result")
with open(ARTIFACTS / 'streamz_default.pkl', 'wb') as f:
    pickle.dump(streamz_default, f)


# ---- Compare ----
print("\n=== COMPARISON ===")
all_keys = sorted(set(list(akayu_default.keys()) + list(streamz_default.keys())))
for seg in all_keys:
    a = akayu_default.get(seg, [])
    s = streamz_default.get(seg, [])
    
    a_types = [type(e).__name__ for e in a]
    s_types = [type(e).__name__ for e in s]
    
    a_starts = [e for e in a if type(e).__name__ == 'Start']
    s_starts = [e for e in s if type(e).__name__ == 'Start']
    a_stops = [e for e in a if type(e).__name__ == 'Stop']
    s_stops = [e for e in s if type(e).__name__ == 'Stop']
    a_seg_starts = [e for e in a if type(e).__name__ == 'SegmentStart']
    s_seg_starts = [e for e in s if type(e).__name__ == 'SegmentStart']
    
    match = (len(a) == len(s) and a_types == s_types)
    status = "IDENTICAL" if match else "DIFFERENT"
    
    print(f'{seg}: {status} (akayu={len(a)} streamz={len(s)})')
    if not match:
        print(f'  starts: akayu={len(a_starts)} streamz={len(s_starts)}')
        print(f'  stops: akayu={len(a_stops)} streamz={len(s_stops)}')
        print(f'  seg_starts: akayu={len(a_seg_starts)} streamz={len(s_seg_starts)}')
        if a_types[:30] != s_types[:30]:
            print(f'  first 30 types akayu:  {a_types[:30]}')
            print(f'  first 30 types streamz: {s_types[:30]}')
