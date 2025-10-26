import time
import typing
from collections import deque
from enum import StrEnum
from threading import Lock


QUEUE_MAX_LEN = 4           # 队列最大长度
QUEUE_MAX_AGE = 3           # 最大保留时间，秒
SIGNAL_DETECT_TIMES = 3     # 信号检测次数
SIGNAL_DETECT_INTERVAL_S = 0.5      # 信号检测间隔


class RunningType(StrEnum):
    RUNNING = "RUNNING"
    STANDBY = "STANDBY"
    STOPPED = "STOPPED"
    UNKNOWN = "UNKNOWN"

    def is_running(self) -> bool:
       return self is RunningType.RUNNING


class PressRunningStatus:

    def __init__(self):
        # 最大保留时间
        self.max_age = QUEUE_MAX_AGE
        # 队列
        self.queue = deque(maxlen=QUEUE_MAX_LEN)
        self.lock = Lock()

    def detect_in_loop(self):
        t = 0
        while True:
            light = yield None
            # 放入队列
            self.push(light)
            # 检测
            if t >= SIGNAL_DETECT_TIMES - 1:
                running = self.detect()
                return running
            t += 1

    def detect(self) -> RunningType:
        new = list(self.queue)[-1 * SIGNAL_DETECT_TIMES:]
        if len(new) < SIGNAL_DETECT_TIMES:
            return RunningType.UNKNOWN

        new_sum = sum(map(lambda x: x[1], new))
        if new_sum == SIGNAL_DETECT_TIMES:
            return RunningType.RUNNING
        elif new_sum == 0:
            return RunningType.STOPPED
        else:
            return RunningType.STANDBY

    def push(self, light_signal: bool):
        now = time.time()
        with self.lock:
            self.queue.append((now, light_signal))
            self.cleanup(now)

    def pop(self) -> typing.Optional[bool]:
        with self.lock:
            self.cleanup()
            if self.queue:
                return self.queue.popleft()[1]
            return None

    def cleanup(self, now: typing.Optional[float] = None):
        if self.max_age:
            now = now or time.time()
            while self.queue and now - self.queue[0][0] > self.max_age:
                self.queue.popleft()

    def __repr__(self):
        return str([item[1] for item in self.queue])
