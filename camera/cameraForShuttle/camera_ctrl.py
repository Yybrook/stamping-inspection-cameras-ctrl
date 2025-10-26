import asyncio
import yaml
import os
import typing
from concurrent.futures import ThreadPoolExecutor
import logging

from press import Shuttle, PartCounter
from plc import PressTailReader
from redisDb import AsyncRedisDB
from rabbitmq import RabbitmqCameraProducer
from modbus import CameraCtrlModbusClient, ModbusAddress

_logger = logging.getLogger(__name__)


# 软触发延时时间
DEFAULT_TRIGGER_DELAY_SEC = 0.5
LIGHT_DISABLE_AFTER_PRESS_STOP = 600

MAX_WORKERS = 50


class CameraCtrl:
    _parts = None
    _registered_cameras = None

    def __init__(
            self,
            stop_event: typing.Optional[asyncio.Event],
            executor: typing.Optional[ThreadPoolExecutor],
            press_line: str,
            redis_host: str, redis_port: int, redis_db: int,
            rabbitmq_url: str,
            modbus_host: str, modbus_port: int, modbus_slave: int,
            **kwargs
    ):

        # 加载 parts_info.yaml
        self.load_cameras_and_parts(path=kwargs.pop("parts_info_path", None))

        # 加载 modbus_address.yaml
        ModbusAddress.load_address(path=kwargs.pop("modbus_address_path", None))

        # 冲压线名称
        self.press_line = press_line

        # redis
        self.redis: typing.Optional[AsyncRedisDB] = None
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_db = redis_db

        # rabbit mq
        self.rabbitmq_url = rabbitmq_url
        self.rabbitmq_producer: typing.Optional[RabbitmqCameraProducer] = None

        # modbus
        self.modbus_host = modbus_host
        self.modbus_port = modbus_port
        self.modbus_slave = modbus_slave

        # 穿梭小车对象
        self.shuttle = Shuttle()

        # 执行器
        self.executor = executor or ThreadPoolExecutor(max_workers=MAX_WORKERS)
        # 标识是否是我们自己创建的 executor
        self._own_executor = executor is None

        # 触发延时
        self.trigger_delay = 0
        # light 使能
        self.light_enable = False

        # event
        self.stop_event = stop_event or asyncio.Event()
        self._own_stop_event = stop_event is None

        self.tasks = list()

    @classmethod
    async def create(
            cls,
            stop_event: typing.Optional[asyncio.Event],
            executor: typing.Optional[ThreadPoolExecutor],
            press_line: str,
            redis_host: str, redis_port: int, redis_db: int,
            rabbitmq_url: str,
            modbus_host: str, modbus_port: int, modbus_slave: int,
            **kwargs
    ) -> typing.Self:
        ctrl = cls(
            stop_event=stop_event,
            executor=executor,
            press_line=press_line,
            redis_host=redis_host, redis_port=redis_port, redis_db=redis_db,
            rabbitmq_url=rabbitmq_url,
            modbus_host=modbus_host, modbus_prt=modbus_port, modbus_slave=modbus_slave,
            **kwargs
        )
        ctrl.redis = await AsyncRedisDB.create(
                host=ctrl.redis_host,
                port=ctrl.redis_port,
                db=ctrl.redis_db,
                ping=True,
            )

        # rabbit mq
        ctrl.rabbitmq_producer = RabbitmqCameraProducer(
            rabbitmq_url=ctrl.rabbitmq_url,
            location = "shuttle"
        )
        await ctrl.rabbitmq_producer.connect()

        # 初始化 event
        ctrl.stop_event.clear()

        # 协程任务
        ctrl.tasks = [
            asyncio.create_task(ctrl.subscribe_program_id()),
            asyncio.create_task(ctrl.subscribe_running_status()),
            asyncio.create_task(ctrl.light_control()),
            asyncio.create_task(ctrl.shuttle_detect()),
        ]

        return ctrl

    async def __aenter__(self):
        """进入 async with 块"""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """退出 async with 块时自动清理资源"""
        await self.cleanup()
        # 返回 False 以便异常继续抛出
        return False

    async def cleanup(self):
        if self._own_stop_event:
            self.stop_event.set()

        await self.rabbitmq_producer.close()

        await self.redis.aclose()

        if self._own_executor:
            # wait=False → 立即返回，不阻塞主线程
            # cancel_futures=True → 尝试取消线程池里还没开始执行的任务
            self.executor.shutdown()

    # #################### 监控shuttle -> 拍照, 发布 ####################
    async def shuttle_detect(self):
        async with PressTailReader(executor=self.executor) as plc:
            while True:
                try:
                    # stop_event 被置为
                    if self.stop_event.is_set():
                        break

                    # 判断压机是否停机
                    _, running_status = await self.redis.get_latest_running_status(press_line=self.press_line)
                    if not running_status:
                        await asyncio.sleep(0.1)
                        continue

                    # 判断是否有相机打开
                    running_cameras_num = await self.redis.get_running_cameras_number(press_line=self.press_line)
                    if not running_cameras_num:
                        await asyncio.sleep(0.1)
                        continue

                    # 读取 shuttle 传感器
                    s1, s2 = await plc.read_shuttle_sensors()
                    # 判定是否有零件
                    has_part, has_part_t = self.shuttle.check_part(s1, s2)
                    if not has_part:
                        continue

                    # 读取 part_count
                    part_counter = await plc.read_part_counter()
                    # part_count 设置 bias
                    part_counter = PartCounter.on_shuttle(count=part_counter)

                    _logger.info(f"{self.identity} shuttle has part, counter[{part_counter}], interval[{self.shuttle.interval}]")

                    # 软触发
                    await self.delay_2_TriggerSoftware(value=has_part_t)

                    # 发布 part_count
                    await self.redis.set_part_counter(part_counter=part_counter, press_line=self.press_line)

                except Exception as err:
                    _logger.exception(f"{self.identity} shuttle detect error: {err}")

            _logger.info(f"{self.identity} shuttle detect ended")

    async def message_all_cameras(self, cmds):
        # 获取运行的相机ip
        camera_ips = await self.redis.get_running_cameras(press_line=self.press_line)
        # 给所有运行的相机 发送信息
        await self.rabbitmq_producer.publish_cmd(camera_ips=camera_ips, cmds=cmds)

    async def delay_2_TriggerSoftware(self, value):
        cmds = (("set", "TriggerSoftware", value),)
        await asyncio.sleep(self.trigger_delay)
        await self.message_all_cameras(cmds=cmds)

    # #################### 监控program id -> 打开/关闭相机 ####################
    async def subscribe_program_id(self):
        # 使用异步生成器获取运行状态
        async for timestamp, program_id in self.redis.get_program_id(
                press_line=self.press_line,
                block=1000,         # 阻塞1秒等待新消息
                include_last=True   # 先返回最后一条历史消息
        ):
            # stop_event 被置为
            if self.stop_event.is_set():
                break

            # 接收到新的 program_id
            if program_id is not None:
                try:
                    # 获取 program_id 信息
                    part_info = self._parts.get(program_id, dict())
                    # 获取 触发延时
                    self.trigger_delay = part_info.get("trigger_delay", DEFAULT_TRIGGER_DELAY_SEC)
                    # 改变 shuttle detect_type，默认 BOTH
                    self.shuttle.set_detect_type(part_info.get("shuttle_sensor_type", 0))
                    # 获取 camera_ips
                    required_camera_ips = set(part_info.get("cameras", list()))

                    # 获取运行中的相机
                    running_camera_ips = set(await self.redis.get_running_cameras(press_line=self.press_line))
                    # 要关闭的相机
                    to_close_camera_ips = (running_camera_ips - required_camera_ips) & self._registered_cameras
                    # 要打开的相机
                    to_open_camera_ips = (required_camera_ips - running_camera_ips) & self._registered_cameras

                    # 打开相机
                    if to_open_camera_ips:
                        await self.rabbitmq_producer.publish_cmd(camera_ips=list(to_open_camera_ips), cmds=(("open",),))
                    # 关闭相机
                    if to_close_camera_ips:
                        await self.rabbitmq_producer.publish_cmd(camera_ips=list(to_close_camera_ips), cmds=(("close",),))

                    # todo 确认相机关闭
                    # await asyncio.sleep(10)
                    # running_camera_ips = set(await self.redis.get_running_cameras(press_line=self.press_line))

                except Exception as err:
                    _logger.exception(f"{self.identity} handle program id error: {err}")

        _logger.info(f"{self.identity} subscribe program id ended")

    # #################### 监控running status -> 开灯, 延时关灯 ####################
    async def subscribe_running_status(self):
        # 使用异步生成器获取运行状态
        async for timestamp, running_status in self.redis.get_running_status(
                press_line=self.press_line,
                block=1000,         # 阻塞1秒等待新消息
                include_last=True   # 先返回最后一条历史消息
        ):
            # stop_event 被置为
            if self.stop_event.is_set():
                break

            try:
                # 接收到新的 running_status
                # 压机运行 -> 开灯
                if running_status:
                   await self.redis.set_light_enable(press_line=self.press_line, disable_after=None)
                # 无 running_status or 压机停机 -> 10分钟后关灯
                else:
                    await self.redis.set_light_disable(press_line=self.press_line, after=LIGHT_DISABLE_AFTER_PRESS_STOP)

            except Exception as err:
                _logger.exception(f"{self.identity} handle running status error: {err}")

        _logger.info(f"{self.identity} subscribe running status ended")

    # #################### 监控相机数量 -> 开/关灯 ####################
    async def light_control(self):
        while True:
            try:
                # stop_event 被置为
                if self.stop_event.is_set():
                    break

                light_enable = await self.redis.get_light_enable(press_line=self.press_line)
                if self.light_enable != light_enable:
                    self.light_enable = light_enable
                    _logger.info(f"{self.identity} light enable = {self.light_enable}")

                    async with CameraCtrlModbusClient(
                            host=self.modbus_host,
                            port=self.modbus_port,
                            slave=self.modbus_slave
                    ) as client:
                        client.write(registers={"light_enable": self.light_enable})

                await asyncio.sleep(1)

            except Exception as err:
                _logger.exception(f"{self.identity} light control error: {err}")

        _logger.info(f"{self.identity} light control ended")

    @classmethod
    def load_cameras_and_parts(cls, path: typing.Optional[str] = None) -> tuple[set, dict]:
        """
        加载 parts_info.yaml
        :param path:
        :return:
        """
        if cls._parts is None:
            if not path:
                basename = os.path.basename(__file__)
                path = __file__.replace(basename, "parts_info.yaml")
            # 确保文件存在
            if not os.path.isfile(path):
                raise FileNotFoundError(f"parts info yaml file not found: {path}")

            with open(path, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f)

            cls._registered_cameras = set(config.get("registered_cameras", set()))

            cls._parts = config.get('parts', dict())

        return cls._registered_cameras, cls._parts

    @property
    def identity(self):
        return f"[CameraCtrl|Shuttle]"
