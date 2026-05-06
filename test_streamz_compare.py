import sys, types, pickle, json
from pathlib import Path
import numpy as np

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

ARTIFACTS = Path('test_artifacts')
ARTIFACTS.mkdir(exist_ok=True)

def event_summary(e):
    t = type(e).__name__
    if t in ('Start', 'Stop'):
        return f"{t}({e.name[:50]} set={e.set})"
    return t

def segment_summary(events):
    starts = [e for e in events if type(e).__name__ == 'Start']
    stops = [e for e in events if type(e).__name__ == 'Stop']
    return {
        'total': len(events),
        'starts': len(starts),
        'stops': len(stops),
        'start_names': [e.name for e in starts],
        'stop_names': [e.name for e in stops],
    }

# ---- Run with AKAYU ----
print("=== Running with AKAYU ===")
import importlib
if 'test_example' in sys.modules:
    del sys.modules['test_example']
if 'test_example.flows' in sys.modules:
    del sys.modules['test_example.flows']
if 'test_example.nodes' in sys.modules:
    del sys.modules['test_example.nodes']
if 'test_example.utils' in sys.modules:
    del sys.modules['test_example.utils']
if 'test_example.types' in sys.modules:
    del sys.modules['test_example.types']
if 'test_example.rerun' in sys.modules:
    del sys.modules['test_example.rerun']

import akayu
sys.modules['streamz_saved'] = sys.modules.get('streamz', None)
# test_example uses "import akayu as streamz" - this is fine
from test_example.flows import source_v1_flow as akayu_source_v1_flow

session_path = Path('test_data/JAMIG393250326160019M')
akayu_result = akayu_source_v1_flow(session_path, imu=True, ultrasound=True)

akayu_summary = {}
for seg, events in akayu_result.items():
    akayu_summary[seg] = segment_summary(events)
    print(f'  {seg}: {akayu_summary[seg]["total"]} events, '
          f'{akayu_summary[seg]["starts"]} starts, {akayu_summary[seg]["stops"]} stops')

# Save akayu result for detailed comparison
with open(ARTIFACTS / 'akayu_result.pkl', 'wb') as f:
    pickle.dump(akayu_result, f)

# ---- Run with STREAMZ ----
print("\n=== Running with STREAMZ ===")
# Clear test_example modules
for mod in list(sys.modules.keys()):
    if mod.startswith('test_example'):
        del sys.modules[mod]

# Now make "akayu" point to streamz so test_example code uses streamz
import streamz as real_streamz
sys.modules['akayu'] = real_streamz

from test_example.flows import source_v1_flow as streamz_source_v1_flow

streamz_result = streamz_source_v1_flow(session_path, imu=True, ultrasound=True)

streamz_summary = {}
for seg, events in streamz_result.items():
    streamz_summary[seg] = segment_summary(events)
    print(f'  {seg}: {streamz_summary[seg]["total"]} events, '
          f'{streamz_summary[seg]["starts"]} starts, {streamz_summary[seg]["stops"]} stops')

with open(ARTIFACTS / 'streamz_result.pkl', 'wb') as f:
    pickle.dump(streamz_result, f)

# ---- Compare ----
print("\n=== COMPARISON ===")
for seg in akayu_result:
    if seg not in streamz_result:
        print(f'{seg}: MISSING from streamz result')
        continue
    a = akayu_summary[seg]
    s = streamz_summary[seg]
    if a == s:
        print(f'{seg}: IDENTICAL')
    else:
        print(f'{seg}: DIFFERENT')
        print(f'  akayu:   {a["total"]} events, {a["starts"]} starts, {a["stops"]} stops')
        print(f'  streamz: {s["total"]} events, {s["starts"]} starts, {s["stops"]} stops')
        # Compare exercise names
        if a['start_names'] != s['start_names']:
            print(f'  akayu start names:   {a["start_names"]}')
            print(f'  streamz start names: {s["start_names"]}')
        if a['stop_names'] != s['stop_names']:
            print(f'  akayu stop names:   {a["stop_names"]}')
            print(f'  streamz stop names: {s["stop_names"]}')

# Restore akayu module
sys.modules['akayu'] = akayu
