import sys, types

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
from test_example.flows import source_v1_flow

session_path = Path('test_data/JAMIG393250326160019M')
result = source_v1_flow(session_path, imu=True, ultrasound=True)

print(f'Segments: {list(result.keys())}')
for seg, events in result.items():
    starts = [e for e in events if type(e).__name__ == 'Start']
    stops = [e for e in events if type(e).__name__ == 'Stop']
    print(f'\n{seg}: {len(events)} events, {len(starts)} starts, {len(stops)} stops')
    for e in events:
        t = type(e).__name__
        if t in ('Start', 'Stop'):
            print(f'  {t}: {e.name[:55]} set={e.set}')
        else:
            print(f'  {t}')
