import sys, types
from types import SimpleNamespace

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

from pathlib import Path
import akayu as streamz
from test_example.flows import source_v1_flow
from test_example.nodes import (
    sitstand_calibration_node, pose_calibration_node,
    resync_node, rel_rotmat_node, merge_node,
)
from test_example.types import (
    Start, Stop, ExerciseInfo, Timestamp, Rotmat, Gyro, Accel,
    Image, Mask, Angle, SegmentStart, SegmentStop,
)
from test_example.utils import flush_segments

session_path = Path('test_data/JAMIG393250326160019M')
print("=== Running source_v1_flow ===")
source_result = source_v1_flow(session_path, imu=True, ultrasound=True)

print("\n=== Building default_flow graph (sans bone/uls/pivot_shift) ===")

segment_events_list = source_result
segment_source = {k: streamz.Stream() for k in segment_events_list.keys()}
segment_events = {k: events.flatten() for k, events in segment_source.items()}

sit_exe_names = ["Sitting Calibration", "Calibration - Calibration 2 - Sitting"]
stand_exe_names = ["Standing Calibration", "Calibration - Calibration 1 - Standing"]
sts_exe_names = ["5 Repetition Slow Sit to Stand Test"]
active_flexion_exe_names = ["Standing Active Flexion Test"]
fast_walk_exe_names = ["10m Fast Walk Test", "10m Fast Walk"]

from test_example.utils import (
    segment_sit_to_stand, segment_active_flexion, segment_walking,
)

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

    def pick_segmenter(exe_name: str):
        name = exe_name.strip().lower()
        if name in sts_names_lc or "sit to stand" in name:
            return segment_sit_to_stand
        if name in active_flexion_names_lc or "active flexion" in name:
            return segment_active_flexion
        if name in fast_walk_names_lc:
            return segment_walking
        return lambda _: []

    def pick_segment_samples(stop_name: str, thigh_samples, shank_samples):
        segmenter = pick_segmenter(stop_name)
        if segmenter is segment_walking:
            return segmenter(shank_samples)
        rotmat_ts = [(s[0], s[-1]) for s in thigh_samples]
        return segmenter(rotmat_ts)

    def safe_pick_segment_samples(stop, thigh_samples, shank_samples):
        try:
            return pick_segment_samples(stop.name, thigh_samples, shank_samples)
        except Exception as e:
            print(f'  [WARN] segmenter failed for {stop.name}: {e}')
            return []

    segments = exe_stop.zip(relrot_thigh_gyro_ts, relrot_shank_gyro_ts).starmap(
        safe_pick_segment_samples
    )

    exe_start = exe_info.filter(isinstance, Start)
    exe_chunk = streamz.combine_latest(exe_start, exe_stop, emit_on=exe_stop)
    segments = (
        exe_chunk.zip(segments)
        .starmap(lambda exe_chunk, segs: [exe_chunk[0]] + segs + [exe_chunk[1]])
        .flatten()
    )

    events = merge_node(segments, events)
    segment_events[side] = events

# Skip pivot_shift_prediction_node (needs model file)
# segment_events["left"], segment_events["right"] = pivot_shift_prediction_node(...)

# THIS is the key part: use flush_segments like the real flow does
flushed = flush_segments(segment_events)

print("=== Emitting data ===")
for k, events in segment_source.items():
    events.emit(segment_events_list[k])

print("=== Flushing ===")
result = flushed.flush()

print("\n=== Final Results ===")
for k, events in result.items():
    starts = [e for e in events if type(e).__name__ == 'Start']
    stops = [e for e in events if type(e).__name__ == 'Stop']
    seg_starts = [e for e in events if type(e).__name__ == 'SegmentStart']
    seg_stops = [e for e in events if type(e).__name__ == 'SegmentStop']
    print(f'{k}: {len(events)} events, {len(starts)} starts, {len(stops)} stops, '
          f'{len(seg_starts)} seg_starts, {len(seg_stops)} seg_stops')
    for e in events:
        t = type(e).__name__
        if t in ('Start', 'Stop', 'SegmentStart', 'SegmentStop'):
            if hasattr(e, 'name'):
                print(f'  {t}: {e.name[:60]}')
            else:
                print(f'  {t}')
