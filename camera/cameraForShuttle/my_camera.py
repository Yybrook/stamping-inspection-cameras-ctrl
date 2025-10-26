import asyncio
import os
import yaml
import typing
import json
import logging

from hikrobot_camera import HikrobotCamera
from redisDb import AsyncRedisDB
from rabbitmq import RabbitmqCameraConsumer
from utils import ForeverAsyncWorker


_logger = logging.getLogger(__name__)

MAX_WORKERS = 50
CAMERA_LOCATION = "shuttle"


class MyCamera(HikrobotCamera):

    def __init__(
            self,
            stop_event: typing.Optional[asyncio.Event],
            redis_host: str, redis_port: int, redis_db: int,
            rabbitmq_url: str,
            press_line: str,
            ip: str,
            camera_params_path: str,
            **kwargs
    ):
        super().__init__(ip=ip, camera_params_path=camera_params_path, **kwargs)

        # event，用于控制退出 work 循环
        self.stop_event = stop_event or asyncio.Event()
        self._own_stop_event = stop_event is None

        # redis
        self.redis: typing.Optional[AsyncRedisDB] = None
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_db = redis_db

        # rabbit mq
        self.rabbitmq_url = rabbitmq_url
        self.rabbitmq_consumer: typing.Optional[RabbitmqCameraConsumer] = None

        # async_worker
        self.async_worker = ForeverAsyncWorker(
            executor=None,
            max_workers=MAX_WORKERS,
            task_error_callback= lambda task_name, exc: _logger.exception(f"{self.identity} {task_name} run in async error: {exc}"),
        )

        # 冲压线名称
        self.press_line = press_line

        # 穿梭小车有零件的时间
        self.shuttle_has_part_t = None

    @classmethod
    async def create(
            cls,
            stop_event: typing.Optional[asyncio.Event],
            redis_host: str, redis_port: int, redis_db: int,
            rabbitmq_url: str,
            press_line: str,
            ip: str,
            camera_params_path: str,
            **kwargs
    ) -> typing.Self:

        # 创建相机实例
        camera = cls(
            stop_event=stop_event,
            redis_host=redis_host, redis_port=redis_port, redis_db=redis_db,
            rabbitmq_url=rabbitmq_url,
            press_line=press_line,
            ip=ip,
            camera_params_path=camera_params_path,
            **kwargs
        )

        # redis
        camera.redis = await AsyncRedisDB.create(
                host=camera.redis_host,
                port=camera.redis_port,
                db=camera.redis_db,
                ping=True,
            )

        # rabbit mq
        camera.rabbitmq_consumer = RabbitmqCameraConsumer(
            rabbitmq_url=camera.rabbitmq_url,
            camera_ip=camera.ip,
            location=CAMERA_LOCATION
        )
        await camera.rabbitmq_consumer.connect()

        return camera

    async def cleanup(self):
        # 关闭 event
        if self._own_stop_event:
            self.stop_event.set()
        # 关闭 async_worker
        self.async_worker.stop_loop()
        # 关闭 rabbitmq
        await self.rabbitmq_consumer.close()
        # 关闭 redis
        await self.redis.aclose()

    def get_one_frame_callback(self, pData, pFrameInfo, pUser):
        # 帧信息
        # nWidth = self.stFrameInfo.nWidth
        # nHeight = self.stFrameInfo.nHeight
        # enPixelType = self.stFrameInfo.enPixelType
        # nFrameNum = self.stFrameInfo.nFrameNum
        # nDevTimeStampHigh = self.stFrameInfo.nDevTimeStampHigh
        # nDevTimeStampLow = self.stFrameInfo.nDevTimeStampLow
        # nHostTimeStamp = self.stFrameInfo.nHostTimeStamp
        # nFrameLen = self.stFrameInfo.nFrameLen

        # frame
        image_data = super().get_one_frame_callback(pData, pFrameInfo, pUser)
        # 将图片数据放入队列
        self.async_worker.run_async(self._output_frame(image_data))
        return image_data

    async def _output_frame(self, image_data):
        try:
            # 从 redis 获取数据
            program_id_t, program_id = await self.redis.get_latest_program_id(press_line=self.press_line)
            part_counter_t, part_counter = await self.redis.get_latest_part_counter(press_line=self.press_line)
            # 放入 redis
            await self.redis.set_shuttle_frame(
                press_line=self.press_line,
                program_id=program_id,
                part_counter=part_counter,
                camera_ip=self.ip,
                camera_user_id=self.DeviceUserID,
                matrix=image_data,
                frame_num=self.stFrameInfo.nFrameNum,
                frame_t=self.stFrameInfo.nHostTimeStamp,
                has_part_t=self.shuttle_has_part_t,
            )
        except Exception as err:
            _logger.exception(f"{self.identity} output framer to redis error: {err}")

    def _handle_cmd(self, cmd: list) -> list:
        response = list()
        # 打开相机
        if cmd[0] == "open":
            self.async_worker.run_async(self.camera_work())
        # 关闭相机
        elif cmd[0] == "close":
            self.stop_event.set()
        # 设置参数
        elif cmd[0] == "set":
            # 特殊情况：
            # 1. 软触发，获取 shuttle_has_part_t
            if cmd[1] == "TriggerSoftware":
                self.shuttle_has_part_t = cmd[2]
            self.async_worker.run_async(self.setitem(key=cmd[1], value=cmd[2]))
        elif cmd[0] == "get":
            future = self.async_worker.run_async(self.getitem(key=cmd[1]))
            try:
                value = future.result()
                response = [*cmd, "done", value]
            except Exception as err:
                response = [*cmd, "error", str(err)]
        return response

    def handle_cmds(self, data: list) -> list:
        # 响应
        response = list()
        for cmd in data:
            try:
                res = self._handle_cmd(cmd)
                if res:
                    response.append(res)
            except Exception as err:
                _logger.exception(f"{self.identity} handle camera ctrl cmd{cmd} error: {err}")
        return response

    async def listener_work(self):
        # 获取异步生成器对象
        agen = self.rabbitmq_consumer.listener()
        # 监听队列
        async for data in agen:
            response = self.handle_cmds(data)
            response_msg = {"ip": self.ip, "response": response}
            # 将处理结果发回 listener
            await agen.asend(response_msg)

    async def camera_work(self):
        """相机工作，需要在子线程中进行"""
        # 复位 stop_event
        self.stop_event.clear()

        try:
            # 打开相机, 并取流
            async with self:
                while True:
                    # stop_event 被置为
                    if self.stop_event.is_set():
                        break
                    # 判断相机连接性
                    if not self.is_device_connected():
                        raise ConnectionAbortedError(f"lose connection")

                    # # 主动取流方式，在循环中调用 get_one_frame()
                    # self.get_one_frame()
                    # 被动取流方式, 等待即可
                    await asyncio.sleep(1)

        except Exception as err:
            _logger.exception(f"{self.identity} work in subprocess error: {err}")

    async def __aenter__(self) -> typing.Self:
        """
        Camera initialization : open, setup, and start grabbing frames from the device.
        :return:
        """
        super().__enter__()

        await self.redis.add_running_camera(ip=self.ip, press_line=self.press_line)

        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        """
        Run camera termination code: stop grabbing frames and close the device.
        :param exc_type:
        :param exc_value:
        :param traceback:
        :return:
        """
        super().__exit__(exc_type, exc_value, traceback)

        await self.redis.remove_running_camera(ip=self.ip, press_line=self.press_line)
        # 返回 False 以便异常继续抛出
        return False

    @classmethod
    def load_registered_cameras(cls, path: typing.Optional[str] = None) -> set:
        """
        加载 parts_info.yaml
        :param path:
        :return:
        """
        if cls._registered_cameras is None:
            if not path:
                basename = os.path.basename(__file__)
                path = __file__.replace(basename, "parts_info.yaml")
            # 确保文件存在
            if not os.path.isfile(path):
                raise FileNotFoundError(f"parts info yaml file not found: {path}")

            with open(path, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f)

            cls._registered_cameras = set(config.get("registered_cameras", set()))

        return cls._registered_cameras
