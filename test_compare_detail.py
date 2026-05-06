import sys, types, pickle
from pathlib import Path
import numpy as np

ARTIFACTS = Path('test_artifacts')

with open(ARTIFACTS / 'akayu_result.pkl', 'rb') as f:
    akayu_result = pickle.load(f)
with open(ARTIFACTS / 'streamz_result.pkl', 'rb') as f:
    streamz_result = pickle.load(f)

def event_key(e):
    t = type(e).__name__
    if t in ('Start', 'Stop'):
        return f"{t}({e.name} set={e.set} ts={e.timestamp})"
    if t == 'Timestamp':
        return f"Timestamp({int(e)})"
    if hasattr(e, 'data') and hasattr(e.data, 'shape'):
        return f"{t}(shape={e.data.shape})"
    return t

print("=== Detailed event-by-event comparison ===")
all_match = True
for seg in akayu_result:
    a_events = akayu_result[seg]
    s_events = streamz_result[seg]
    
    a_types = [type(e).__name__ for e in a_events]
    s_types = [type(e).__name__ for e in s_events]
    
    a_exe = [(i, e) for i, e in enumerate(a_events) if type(e).__name__ in ('Start', 'Stop')]
    s_exe = [(i, e) for i, e in enumerate(s_events) if type(e).__name__ in ('Start', 'Stop')]
    
    types_match = a_types == s_types
    exe_match = all(ae.name == se.name and ae.set == se.set and ae.timestamp == se.timestamp 
                     for (_, ae), (_, se) in zip(a_exe, s_exe))
    
    status = "IDENTICAL" if types_match and exe_match else "DIFFERENT"
    if not (types_match and exe_match):
        all_match = False
    
    print(f'{seg}: {status} (len={len(a_events)} vs {len(s_events)}, '
          f'types_match={types_match}, exe_match={exe_match})')
    
    if not exe_match:
        for (ai, ae), (si, se) in zip(a_exe, s_exe):
            if ae.name != se.name or ae.set != se.set or ae.timestamp != se.timestamp:
                print(f'  akayu[{ai}]: {event_key(ae)}')
                print(f'  sz[{si}]:   {event_key(se)}')

print(f'\nAll match: {all_match}')

# Deep comparison: compare data arrays at same indices
print("\n=== Data value comparison (sampling) ===")
for seg in akayu_result:
    a_events = akayu_result[seg]
    s_events = streamz_result[seg]
    
    diffs = 0
    for i, (ae, se) in enumerate(zip(a_events, s_events)):
        if type(ae).__name__ == 'Timestamp' and type(se).__name__ == 'Timestamp':
            if int(ae) != int(se):
                if diffs < 5:
                    print(f'{seg}[{i}]: Timestamp akayu={int(ae)} streamz={int(se)}')
                diffs += 1
        elif hasattr(ae, 'data') and hasattr(se, 'data'):
            if isinstance(ae.data, np.ndarray) and isinstance(se.data, np.ndarray):
                if ae.data.shape != se.data.shape:
                    if diffs < 5:
                        print(f'{seg}[{i}]: {type(ae).__name__} shape akayu={ae.data.shape} streamz={se.data.shape}')
                    diffs += 1
                elif not np.allclose(ae.data, se.data, equal_nan=True):
                    if diffs < 5:
                        diff_idx = np.where(~np.isclose(ae.data, se.data, equal_nan=True))
                        print(f'{seg}[{i}]: {type(ae).__name__} values differ at {diff_idx}')
                    diffs += 1
    
    if diffs == 0:
        print(f'{seg}: all data values match')
    else:
        print(f'{seg}: {diffs} value differences found')
