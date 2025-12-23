import typing
import asyncio
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

import logging

from plc import Press1stReader, PressHeadReader
from redisDb import AsyncRedisDB
from .press_running_status import PressRunningStatus, SIGNAL_DETECT_INTERVAL_S, RunningType
from utils import async_run_in_executor

_logger = logging.getLogger(__name__)

# 间隔 1min 读取 program id
SCHEDULER_READ_PROGRAM_ID_INTERVAL_MIN = 1
# 间隔 4sec 读取 part counter
SCHEDULER_READ_PART_COUNTER_INTERVAL_SEC = 4
# 间隔 4sec 读取 running status
SCHEDULER_READ_RUNNING_STATUS_INTERVAL_SEC = 4


MAX_WORKERS = 10

class PressInfo:
    def __init__(self, press_line: str, redis_host: str, redis_port: int, redis_db: int, executor = None):
        # 冲压线名称
        self.press_line = press_line

        # redis
        self.redis: typing.Optional[AsyncRedisDB] = None
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_db = redis_db

        # 压机运行状态
        self.press_running_status = PressRunningStatus()

        self.program_id = None
        self.part_counter = None
        self.running_status = None

        # 执行器
        self.executor = executor or ThreadPoolExecutor(max_workers=MAX_WORKERS)
        # 标识是否是我们自己创建的 executor
        self._own_executor = executor is None

        # 定时器
        self.scheduler = AsyncIOScheduler()

        # 获取 loop
        self.loop = asyncio.get_running_loop()

    @classmethod
    async def create(cls, press_line, redis_host, redis_port, redis_db, executor: typing.Optional[ThreadPoolExecutor] = None) -> typing.Self:
        # 实例化
        press_info = cls(press_line, redis_host, redis_port, redis_db, executor)

        # 连接 redis
        press_info.redis = await AsyncRedisDB.create(
            host=press_info.redis_host,
            port=press_info.redis_port,
            db=press_info.redis_db,
            ping=True
        )

        # scheduler 任务
        # read_program_id
        press_info.scheduler.add_job(
            func=press_info.read_program_id,
            trigger=IntervalTrigger(minutes=SCHEDULER_READ_PROGRAM_ID_INTERVAL_MIN),
            name="read program id scheduler",
            misfire_grace_time=2,
            coalesce=True,
            max_instances=1,
            next_run_time=datetime.now(),
        )

        # read_running_status
        press_info.scheduler.add_job(
            func=press_info.read_running_status,
            trigger=IntervalTrigger(seconds=SCHEDULER_READ_RUNNING_STATUS_INTERVAL_SEC),
            name="read running status scheduler",
            misfire_grace_time=2,
            coalesce=True,
            max_instances=1,
            next_run_time=datetime.now(),
        )

        return press_info

    def work(self):
        self.scheduler.start()
        _logger.info(f"{self.identity} work() started")

    async def cleanup(self):
        # wait=False → 立即返回，不阻塞主线程
        # cancel_futures=True → 尝试取消线程池里还没开始执行的任务
        if self._own_executor:
            self.executor.shutdown()

        self.scheduler.shutdown()

        await self.redis.del_program_id(press_line=self.press_line)
        await self.redis.del_running_status(press_line=self.press_line)
        await self.redis.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """退出 async with 块时自动清理资源"""
        await self.cleanup()
        # 返回 False 以便异常继续抛出
        return False

    async def read_program_id(self):
        try:
            # 从 plc 中读取 program_id
            async with PressHeadReader(executor=self.executor) as reader:
                program_id = await reader.read_program_id()

            # program id 变化 -> 写入 redis
            if program_id != self.program_id:
                await self.redis.set_program_id(program_id=program_id, press_line=self.press_line)
                _logger.info(f"{self.identity} program id={program_id}")
                self.program_id = program_id

        except Exception as err:
            _logger.exception(f"{self.identity} read program id error: {err}")

    async def read_running_status(self):
        try:
            # 读取 running status
            running_status = await self._read_running_status()

            # running_status 变化 -> 接入redis
            if self.running_status is None or running_status.is_running() != self.running_status.is_running():
                await self.redis.set_running_status(
                    running_status=running_status.is_running(),
                    press_line=self.press_line
                )
                _logger.info(f"{self.identity} running status={running_status}")
                self.running_status = running_status

        except Exception as err:
            _logger.exception(f"{self.identity} read running status error: {err}")

    @async_run_in_executor
    def _read_running_status(self) -> RunningType:
        """读取 press_running"""
        with Press1stReader() as reader:
            gen = self.press_running_status.detect_in_loop()
            # 初始化 generator，进入 yield 状态
            next(gen)
            while True:
                # 读取灯信号
                light = reader.read_running_light()
                try:
                    gen.send(light)
                except StopIteration as err:
                    running = err.value
                    break
                time.sleep(SIGNAL_DETECT_INTERVAL_S)
        return running

    @property
    def identity(self):
        return f"PressInfo[{self.press_line}]"
