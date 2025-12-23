import time
from enum import IntEnum

# 最小检测时间间隔，滤波防抖
HAS_PART_THRESHOLD_MS = 1000


class DetectType(IntEnum):
    BOTH = 0
    ONLY_S1 = 1
    ONLY_S2 = 2

    @classmethod
    def create(cls, detect_type: int):
        try:
            return cls(detect_type)
        except ValueError:
            return cls.BOTH


class Shuttle:

    def __init__(self, detect_type: int = DetectType.BOTH):
        self.detect_type = DetectType.create(detect_type)

        self.pre_s1 = self.pre_s2 = False
        self.pre_read_t = self.pre_has_part_t = int(time.time() * 1000)
        self.has_part = False

        self._interval_between_parts = 0

    def check_part(self, s1: bool, s2: bool):
        t = int(time.time() * 1000)
        # 滤波
        if t - self.pre_has_part_t <= HAS_PART_THRESHOLD_MS:
            return False, t

        # 无零件转变为有零件
        if self.detect_type == DetectType.ONLY_S1:
            has_part = s1 and not self.pre_s1
        elif self.detect_type == DetectType.ONLY_S2:
            has_part = s2 and not self.pre_s2
        else:
            has_part = (s1 and s2) and (not self.pre_s1 or not self.pre_s2)

        self.update(s1=s1, s2=s2, t=t, has_part=has_part)

        return has_part, t

    def update(self, s1, s2, t, has_part):
        self.pre_s1, self.pre_s2 = s1, s2
        self.has_part = has_part
        if has_part:
            self._interval_between_parts = t - self.pre_has_part_t
            self.pre_read_t = self.pre_has_part_t = t
        else:
            self.pre_read_t = t

    def set_detect_type(self, detect_type: int):
        self.detect_type = DetectType.create(detect_type)

    @property
    def interval(self) -> float:
        return self._interval_between_parts / 1000
