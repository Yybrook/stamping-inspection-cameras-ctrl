import time
import typing
from collections import deque
from enum import StrEnum
from threading import Lock
import logging

_logger = logging.getLogger(__name__)


QUEUE_MAX_LEN = 4           # 队列最大长度
QUEUE_MAX_AGE = 3           # 最大保留时间，秒
SIGNAL_DETECT_TIMES = 3     # 信号检测次数
SIGNAL_DETECT_INTERVAL_S = 0.5      # 信号检测间隔


class RunningType(StrEnum):
    RUNNING = "RUNNING"
    STANDBY = "STANDBY"
    STOPPED = "STOPPED"

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
            _logger.debug(f"{self.identity} get[{light}], queue={self.__repr__()}")
            # 检测
            if t >= SIGNAL_DETECT_TIMES - 1:
                running = self.detect()
                _logger.debug(f"{self.identity} detect_in_loop()={running}")
                return running
            t += 1

    def detect(self) -> RunningType:
        new = list(self.queue)[-1 * SIGNAL_DETECT_TIMES:]
        if len(new) < SIGNAL_DETECT_TIMES:
            raise ValueError(f"queue length[{len(new)}] is less required[{SIGNAL_DETECT_TIMES}]")

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

    @property
    def identity(self):
        return "Press[RunningStatus]"

if __name__ == "__main__":
    import random

    def init_logger():
        # 创建logger对象
        logger = logging.getLogger()
        # 设置全局最低等级（让所有handler能接收到）
        logger.setLevel(logging.DEBUG)
        # 控制台 Handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        console_formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        console_handler.setFormatter(console_formatter)
        # 添加 handler 到 logger
        logger.addHandler(console_handler)

        # logging.getLogger('asyncio').setLevel(logging.INFO)

    def main():
        try:
            p = PressRunningStatus()
            while True:
                # 创建生成器
                gen = p.detect_in_loop()
                # 初始化 generator，进入 yield 状态
                next(gen)
                while True:
                    s = random.randint(0, 9 ) >= 5
                    try:
                        gen.send(s)
                    except StopIteration as err:
                        running = err.value
                        print(f"running={running}")
                        break
                    time.sleep(SIGNAL_DETECT_INTERVAL_S)
        except KeyboardInterrupt:
            print("中断")
        except Exception as err:
            print(f"错误: {err}")
        finally:
            print("退出")

    init_logger()
    main()

