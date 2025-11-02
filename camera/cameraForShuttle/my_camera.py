import asyncio
import os
import yaml
import time
import typing
import json
import logging
from threading import Event, Thread
from concurrent.futures import ThreadPoolExecutor

from hikrobot_camera import HikrobotCamera
from redisDb import AsyncRedisDB
from rabbitmq import RabbitmqCameraConsumer


_logger = logging.getLogger(__name__)


CAMERA_LOCATION = "shuttle"
BLOCK_TIMEOUT_S = 1
WAIT_CAMERA_CLOSE_TIMEOUT_S = 5


class MyCamera(HikrobotCamera):

    def __init__(
            self,
            stop_event,
            redis_host: str, redis_port: int, redis_db: int,
            rabbitmq_url: str,
            press_line: str,
            ip: str,
            camera_params_path: str,
            **kwargs
    ):
        super().__init__(ip=ip, camera_params_path=camera_params_path, **kwargs)

        # event，用于控制退出 work 循环
        self.stop_event = stop_event or Event()
        self._own_stop_event = stop_event is None

        # redis
        self.redis: typing.Optional[AsyncRedisDB] = None
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_db = redis_db

        # rabbit mq
        self.rabbitmq_url = rabbitmq_url
        self.rabbitmq_consumer: typing.Optional[RabbitmqCameraConsumer] = None

        # 保存主线程事件循环
        self.loop = asyncio.get_running_loop()

        # 冲压线名称
        self.press_line = press_line

        # 穿梭小车有零件的时间
        self.shuttle_has_part_t = None

    @classmethod
    async def create(
            cls,
            stop_event,
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

        # rabbitmq
        camera.rabbitmq_consumer = RabbitmqCameraConsumer(
            rabbitmq_url=camera.rabbitmq_url,
            camera_ip=camera.ip,
            location=CAMERA_LOCATION
        )
        await camera.rabbitmq_consumer.connect()

        # 初始化 event
        camera.stop_event.clear()

        return camera

    # #################### 相机运行 ####################
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
        asyncio.run_coroutine_threadsafe(self._output_frame(image_data), self.loop)
        return image_data

    async def _output_frame(self, image_data):
        try:
            # todo 判断 program_id, part_counter 是否有效
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

    def camera_worker(self):
        """相机工作，需要在子线程中进行"""
        try:
            # 阻塞函数
            block_func = self.get_one_frame if not self.grab_method.is_passive() else lambda x=BLOCK_TIMEOUT_S: time.sleep(x)
            # 复位 stop_event
            self.stop_event.clear()
            # 打开相机, 并取流
            with self:
                while True:
                    # stop_event 被置为
                    if self.stop_event.is_set():
                        break
                    # 判断相机连接性
                    if not self.is_device_connected():
                        raise ConnectionAbortedError(f"lose connection")

                    # 主动取流方式，在循环中调用 get_one_frame()
                    # self.get_one_frame()
                    # 被动取流方式, 等待即可
                    # time.sleep(1)
                    block_func()

        # except KeyboardInterrupt:
        #     _logger.warning(f"{self.identity} camera_worker() cancelled")
        except Exception as err:
            _logger.exception(f"{self.identity} camera_worker() error: {err}")
        finally:
            _logger.info(f"{self.identity} camera_worker() ended")

    def __enter__(self) -> typing.Self:
        """
        Camera initialization : open, setup, and start grabbing frames from the device.
        :return:
        """
        super().__enter__()
        asyncio.run_coroutine_threadsafe(self.redis.add_running_camera(ip=self.ip, press_line=self.press_line), self.loop)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Run camera termination code: stop grabbing frames and close the device.
        :param exc_type:
        :param exc_value:
        :param traceback:
        :return:
        """
        super().__exit__(exc_type, exc_value, traceback)
        asyncio.run_coroutine_threadsafe(self.redis.remove_running_camera(ip=self.ip, press_line=self.press_line), self.loop)
        # 返回 False 以便异常继续抛出
        return False

    # #################### 相机控制 ####################
    def _handle_cmd(self, cmd: list) -> list:
        """
        处理控制命令 cmd
        :param cmd:
                    打开相机: ["open",]
                    关闭相机: ["close",]
                    设置参数: ["set", 参数节点 "TriggerSoftware", 设置值]
                    获取参数: ["get", 参数节点 "Width"]
        :return: 响应
                ["get", 参数节点 "Width", 参数值]
                ["get", 参数节点 "Width", "error", 错误信息]
        """
        response = list()

        try:
            # 打开相机
            if cmd[0] == "open":
                # 在子线程中打开相机
                t = Thread(target=self.camera_worker, daemon=True)
                t.start()

            # 关闭相机
            elif cmd[0] == "close":
                self.stop_event.set()

            # 设置参数
            elif cmd[0] == "set":
                # 特殊情况：
                # 1. 软触发，获取 shuttle_has_part_t
                if cmd[1] == "TriggerSoftware":
                    self.shuttle_has_part_t = cmd[2]
                self.setitem(key=cmd[1], value=cmd[2])

            # 获取参数
            elif cmd[0] == "get":
                value = self.getitem(key=cmd[1])
                response = [*cmd, value]

        except Exception as err:
            response = [*cmd, "error", str(err)]
            _logger.exception(f"{self.identity} _handle_cmd({cmd}) error: {err}")

        return response

    def handle_cmds(self, data: str) -> list:
        """
        将json格式的 data 转为 cmds，批量处理控制命令
        :param data: str: json格式, [["open",], ["set", 参数节点 "TriggerSoftware", 设置值]]
        :return: 响应, 目前只有 "get" 命令有响应
                [["get", 参数节点 "Width", 参数值], ["get", 参数节点 "Width", "error", 错误信息]]
        """
        # 响应
        responses = list()

        # 解析json
        cmds = json.loads(data)
        _logger.debug(f"{self.identity} rabbitmq received to json: {cmds}")

        for cmd in cmds:
            res = self._handle_cmd(cmd)
            if res:
                responses.append(res)

        return responses

    async def rabbitmq_worker(self):
        try:
            # 获取异步生成器对象
            agen = self.rabbitmq_consumer.listener()
            response = None
            # 监听队列
            while True:
                try:
                    # 将 response 发送到 listener
                    data = await agen.asend(response)
                    response = self.handle_cmds(data)
                    if response:
                        response = {"ip": self.ip, "response": response}
                        # 转为json
                        response = json.dumps(response)
                    else:
                        response = None
                except StopAsyncIteration:
                    break
                except Exception as err:
                    _logger.exception(f"{self.identity} handle rabbitmq received error: {err}")
        except Exception as err:
            _logger.exception(f"{self.identity} rabbitmq_worker() error: {err}")
        finally:
            await self.cleanup()
            _logger.info(f"{self.identity} rabbitmq_worker() ended")

    async def cleanup(self):
        # 关闭 event
        # if self._own_stop_event:
        self.stop_event.set()

        # 等待 相机关机
        start_t = time.time()
        while True:
            await asyncio.sleep(0.5)

            if not await self.redis.is_camera_running(ip=self.ip, press_line=self.press_line):
                break

            if time.time() - start_t >= WAIT_CAMERA_CLOSE_TIMEOUT_S:
                await self.redis.remove_running_camera(ip=self.ip, press_line=self.press_line)
                _logger.warning(f"{self.identity} wait camera_worker() end timeout")
                break

        # 关闭 rabbitmq
        await self.rabbitmq_consumer.close()
        # 关闭 redis
        await self.redis.aclose()

    @classmethod
    def load_registered_cameras(cls, path: typing.Optional[str] = None) -> set:
        """
        加载 parts_info.yaml
        :param path:
        :return:
        """
        if getattr(cls, "_registered_cameras", None) is None:
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
