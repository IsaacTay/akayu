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

# We'll run akayu first, then streamz, comparing key intermediate points

# ---- AKAYU ----
print("=== AKAYU ===")
for mod in list(sys.modules.keys()):
    if mod.startswith('test_example'):
        del sys.modules[mod]

import akayu as ak_lib
sys.modules['akayu'] = ak_lib

with open(ARTIFACTS / 'akayu_result.pkl', 'rb') as f:
    ak_source = pickle.load(f)

from test_example.nodes import (
    sitstand_calibration_node, pose_calibration_node,
    resync_node, rel_rotmat_node, merge_node,
)
from test_example.types import (
    Start, Stop, ExerciseInfo, Timestamp, Rotmat, Gyro, Accel,
    Image, Mask, SegmentStart, SegmentStop,
)
from test_example.utils import flush_segments, segment_sit_to_stand, segment_active_flexion, segment_walking

ak_segment_results = {}

for side in ('left', 'right'):
    print(f'\n--- Akayu {side} ---')
    streamz = ak_lib
    
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
    
    # Collect the intermediate results for inspection
    thigh_collected_list = []
    shank_collected_list = []
    is_end_list = []
    exe_stop_list = []
    
    thigh_collected.sink(lambda x: thigh_collected_list.append(x))
    shank_collected.sink(lambda x: shank_collected_list.append(x))
    is_end.sink(lambda x: is_end_list.append(x))
    
    exe_stop = exe_info.filter(isinstance, Stop)
    exe_stop.sink(lambda x: exe_stop_list.append(x))
    
    segments_zip = exe_stop.zip(thigh_collected, shank_collected)
    segments_zip_list = []
    segments_zip.sink(lambda x: segments_zip_list.append(x))
    
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
            seg_fn = None
        
        if seg_fn is None:
            seg_results.append((stop.name, 0, 'no_match'))
            return []
        
        try:
            if seg_fn is segment_walking:
                result = seg_fn(shank_samples)
            else:
                rotmat_ts = [(s[0], s[-1]) for s in thigh_samples]
                result = seg_fn(rotmat_ts)
            seg_results.append((stop.name, len(result), 'ok'))
            return result
        except Exception as e:
            seg_results.append((stop.name, 0, f'error: {e}'))
            return []
    
    segments_out = segments_zip.starmap(trace_pick)
    
    exe_start = exe_info.filter(isinstance, Start)
    exe_chunk = streamz.combine_latest(exe_start, exe_stop, emit_on=exe_stop)
    
    final_segments = (
        exe_chunk.zip(segments_out)
        .starmap(lambda exe_chunk, segs: [exe_chunk[0]] + segs + [exe_chunk[1]])
        .flatten()
    )
    
    final_collect = final_segments.collect()
    events_collect = events.collect()
    
    seg_source_thigh.emit(ak_source[f'{side}/thigh'])
    seg_source_shank.emit(ak_source[f'{side}/shank'])
    
    final_list = final_collect.flush()
    events_list = events_collect.flush()
    
    print(f'  is_end values: {is_end_list}')
    print(f'  exe_stop count: {len(exe_stop_list)}')
    print(f'  thigh_collected count: {len(thigh_collected_list)}')
    print(f'  shank_collected count: {len(shank_collected_list)}')
    print(f'  segments_zip count: {len(segments_zip_list)}')
    
    for name, nseg, status in seg_results:
        print(f'  segmenter: {name[:55]} → {nseg} segments ({status})')
    
    print(f'  final segments: {len(final_list)} events')
    print(f'    SegmentStarts: {sum(1 for e in final_list if type(e).__name__=="SegmentStart")}')
    print(f'    SegmentStops: {sum(1 for e in final_list if type(e).__name__=="SegmentStop")}')
    
    ak_segment_results[side] = {
        'thigh_collected': len(thigh_collected_list),
        'shank_collected': len(shank_collected_list),
        'segments_zip': len(segments_zip_list),
        'seg_results': seg_results,
        'final_list': final_list,
        'events_list': events_list,
    }

# ---- STREAMZ ----
print("\n\n=== STREAMZ ===")
for mod in list(sys.modules.keys()):
    if mod.startswith('test_example'):
        del sys.modules[mod]

import streamz as sz_lib
sys.modules['akayu'] = sz_lib

with open(ARTIFACTS / 'streamz_result.pkl', 'rb') as f:
    sz_source = pickle.load(f)

from test_example.nodes import (
    sitstand_calibration_node, pose_calibration_node,
    resync_node, rel_rotmat_node, merge_node,
)
from test_example.types import (
    Start, Stop, ExerciseInfo, Timestamp, Rotmat, Gyro, Accel,
    Image, Mask, SegmentStart, SegmentStop,
)
from test_example.utils import flush_segments, segment_sit_to_stand, segment_active_flexion, segment_walking

sz_segment_results = {}

for side in ('left', 'right'):
    print(f'\n--- Streamz {side} ---')
    streamz = sz_lib
    
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
    
    thigh_collected_list = []
    shank_collected_list = []
    is_end_list = []
    exe_stop_list = []
    
    thigh_collected.sink(lambda x: thigh_collected_list.append(x))
    shank_collected.sink(lambda x: shank_collected_list.append(x))
    is_end.sink(lambda x: is_end_list.append(x))
    
    exe_stop = exe_info.filter(isinstance, Stop)
    exe_stop.sink(lambda x: exe_stop_list.append(x))
    
    segments_zip = exe_stop.zip(thigh_collected, shank_collected)
    segments_zip_list = []
    segments_zip.sink(lambda x: segments_zip_list.append(x))
    
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
            seg_fn = None
        
        if seg_fn is None:
            seg_results.append((stop.name, 0, 'no_match'))
            return []
        
        try:
            if seg_fn is segment_walking:
                result = seg_fn(shank_samples)
            else:
                rotmat_ts = [(s[0], s[-1]) for s in thigh_samples]
                result = seg_fn(rotmat_ts)
            seg_results.append((stop.name, len(result), 'ok'))
            return result
        except Exception as e:
            seg_results.append((stop.name, 0, f'error: {e}'))
            return []
    
    segments_out = segments_zip.starmap(trace_pick)
    
    exe_start = exe_info.filter(isinstance, Start)
    exe_chunk = streamz.combine_latest(exe_start, exe_stop, emit_on=exe_stop)
    
    final_segments = (
        exe_chunk.zip(segments_out)
        .starmap(lambda exe_chunk, segs: [exe_chunk[0]] + segs + [exe_chunk[1]])
        .flatten()
    )
    
    final_collect = final_segments.collect()
    events_collect = events.collect()
    
    seg_source_thigh.emit(sz_source[f'{side}/thigh'])
    seg_source_shank.emit(sz_source[f'{side}/shank'])
    
    final_list = final_collect.flush()
    events_list = events_collect.flush()
    
    print(f'  is_end values: {is_end_list}')
    print(f'  exe_stop count: {len(exe_stop_list)}')
    print(f'  thigh_collected count: {len(thigh_collected_list)}')
    print(f'  shank_collected count: {len(shank_collected_list)}')
    print(f'  segments_zip count: {len(segments_zip_list)}')
    
    for name, nseg, status in seg_results:
        print(f'  segmenter: {name[:55]} → {nseg} segments ({status})')
    
    print(f'  final segments: {len(final_list)} events')
    print(f'    SegmentStarts: {sum(1 for e in final_list if type(e).__name__=="SegmentStart")}')
    print(f'    SegmentStops: {sum(1 for e in final_list if type(e).__name__=="SegmentStop")}')
    
    sz_segment_results[side] = {
        'thigh_collected': len(thigh_collected_list),
        'shank_collected': len(shank_collected_list),
        'segments_zip': len(segments_zip_list),
        'seg_results': seg_results,
        'final_list': final_list,
        'events_list': events_list,
    }

# ---- Compare ----
print("\n\n=== COMPARISON ===")
for side in ('left', 'right'):
    ak = ak_segment_results[side]
    sz = sz_segment_results[side]
    print(f'\n{side}:')
    print(f'  thigh_collected: akayu={ak["thigh_collected"]} streamz={sz["thigh_collected"]}')
    print(f'  shank_collected: akayu={ak["shank_collected"]} streamz={sz["shank_collected"]}')
    print(f'  segments_zip:    akayu={ak["segments_zip"]} streamz={sz["segments_zip"]}')
    
    print(f'  segmentation results (akayu):')
    for name, nseg, status in ak['seg_results']:
        print(f'    {name[:55]} → {nseg} ({status})')
    print(f'  segmentation results (streamz):')
    for name, nseg, status in sz['seg_results']:
        print(f'    {name[:55]} → {nseg} ({status})')
