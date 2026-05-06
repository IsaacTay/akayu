from dataclasses import dataclass
from typing import Union, Optional
import numpy as np

class Timestamp(int): pass

class FixedArray:
    _shape = None

    def __init__(self, data):
        self.data = np.array(data, dtype=float).reshape(self._shape)

    def __array__(self, dtype=None, copy=None):
        if dtype is None:
            return self.data
        return self.data.astype(dtype)

    @property
    def T(self):
        return type(self)(self.data.T)

    def __matmul__(self, other):
        if isinstance(other, FixedArray):
            result = self.data @ other.data
            return type(other)(result)
        return NotImplemented

    def __getitem__(self, key):
        return self.data[key]

    def __repr__(self):
        return f"{self.__class__.__name__}({self.data.tolist()})"

class Accel(FixedArray): _shape = (3,)
class Gyro(FixedArray): _shape = (3,)
class Rotmat(FixedArray): _shape = (3, 3)
type IMU = Union[Timestamp, Accel, Gyro, Rotmat]
class Device(str): pass

class ImageBase(FixedArray):
    def __init__(self, data):
        self.data = np.array(data)

    def __repr__(self):
        return f"{self.__class__.__name__}({self.data.shape})"

class Image(ImageBase): pass
class Mask(ImageBase): pass

class Angle(float): pass

type Metadata = dict

@dataclass
class ExerciseInfo:
    name: str
    timestamp: int
    set: int
    addition_data: Optional[dict] = None

@dataclass
class Start(ExerciseInfo): pass
@dataclass
class Stop(ExerciseInfo): pass

@dataclass
class SegmentInfo:
    timestamp: int
    set: int
    addition_data: Optional[dict] = None

@dataclass
class SegmentStart(SegmentInfo): pass
@dataclass
class SegmentStop(SegmentInfo): pass

class UniversalSet:
    def __contains__(self, item):
        return True
    def __repr__(self):
        return "UniversalSet()"

universal_set = UniversalSet()

class PivotShift:
    def __init__(self, pivot_shift_grades):
        self.grade = max(pivot_shift_grades, key=pivot_shift_grades.get)
        self.confidence = max(pivot_shift_grades.values())