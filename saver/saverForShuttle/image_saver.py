import numpy as np
import cv2
import functools
import asyncio
import os
from datetime import datetime
import typing
import logging
from tortoise import Tortoise
from concurrent.futures import ThreadPoolExecutor

from redisDb import AsyncRedisDB
from udpMulticast import AsyncUdpMulticastServer
from .models import ShuttleImage
from config.mssql_setting import TORTOISE_ORM
import utils


_logger = logging.getLogger(__name__)


MAX_WORKERS = 50


class ImageSaver:
    def __init__(
            self,
            stop_event: typing.Optional[asyncio.Event],
            executor: typing.Optional[ThreadPoolExecutor],
            redis_host: str, redis_port: int, redis_db: int,
            udp_multicast_ip: str, udp_multicast_port: int, udp_multicast_interface_ip: str,
            press_line: str,
            saved_dir: str,
            get_image_timeout: int,
            image_overwrite: bool,
            image_format: str,
            image_workers_number: int
    ):
        # redis
        self.redis: typing.Optional[AsyncRedisDB] = None
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_db = redis_db

        # udp multicast
        self.udp_multicast_ip = udp_multicast_ip
        self.udp_multicast_port = udp_multicast_port
        self.udp_multicast_interface_ip = udp_multicast_interface_ip

        # event
        self.stop_event = stop_event or asyncio.Event()
        self._own_stop_event = stop_event is None

        # 执行器
        self.executor = executor or ThreadPoolExecutor(max_workers=MAX_WORKERS)
        # 标识是否是我们自己创建的 executor
        self._own_executor = executor is None

        # loop
        self.loop = asyncio.get_running_loop()
        # queue
        self.queue = asyncio.Queue()

        # 冲压线名称
        self.press_line = press_line
        # 保存文件夹
        self.saved_dir = saved_dir
        # 获取图片 timeout
        self.get_image_timeout = get_image_timeout
        # 是否覆盖
        self.image_overwrite = image_overwrite
        # 图片格式
        self.image_format = image_format

        # workers 数量
        self.image_workers_number = image_workers_number

        self.tasks = list()

    @classmethod
    async def create(
            cls,
            stop_event: typing.Optional[asyncio.Event],
            executor: typing.Optional[ThreadPoolExecutor],
            redis_host: str, redis_port: int, redis_db: int,
            udp_multicast_ip: str, udp_multicast_port: int, udp_multicast_interface_ip: str,
            press_line: str,
            saved_dir: str,
            get_image_timeout: int,
            image_overwrite: bool,
            image_format: str,
            image_workers_number: int
    ) -> typing.Self:
        # 创建相机实例
        saver = cls(
            stop_event=stop_event,
            executor=executor,
            redis_host=redis_host, redis_port=redis_port, redis_db=redis_db,
            udp_multicast_ip=udp_multicast_ip, udp_multicast_port=udp_multicast_port, udp_multicast_interface_ip=udp_multicast_interface_ip,
            press_line=press_line,
            saved_dir=saved_dir,
            get_image_timeout=get_image_timeout,
            image_overwrite=image_overwrite,
            image_format=image_format,
            image_workers_number=image_workers_number,
        )

        # redis
        saver.redis = await AsyncRedisDB.create(
            host=saver.redis_host,
            port=saver.redis_port,
            db=saver.redis_db,
            ping=True,
        )

        # 初始化 event
        saver.stop_event.clear()

        # 协程任务
        saver.tasks = [
            asyncio.create_task(saver.subscribe_part_count()),
            asyncio.create_task(saver.save_images_for_one_part()),
        ]
        # for _ in range(saver.image_workers_number):
        #     saver.tasks.append(asyncio.create_task(saver.save_images_for_one_part()))

        return saver

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

        await self.redis.aclose()

        if self._own_executor:
            # wait=False → 立即返回，不阻塞主线程
            # cancel_futures=True → 尝试取消线程池里还没开始执行的任务
            self.executor.shutdown()

    async def subscribe_part_count(self):
        # 使用异步生成器获取运行状态
        async for timestamp, part_counter in self.redis.get_part_counter(
                press_line=self.press_line,
                block=1000,  # 阻塞1秒等待新消息
                include_last=False  # 先返回最后一条历史消息
        ):
            # stop_event 被置为
            if self.stop_event.is_set():
                await self.queue.put(None)
                break

            # 接收到新的 part_counter
            if part_counter is not None:
                try:
                    # 获取 program_id
                    program_id_t, program_id = await self.redis.get_latest_program_id(press_line=self.press_line)
                    # 放入队列
                    await self.queue.put((program_id, part_counter))
                except Exception as err:
                    _logger.exception(f"{self.identity} handle part counter error: {err}")

        _logger.info(f"{self.identity} subscribe part counter ended")

    async def save_images_for_one_part(self):
        while True:
            data = await self.queue.get()
            # 退出
            if not data:
                break

            try:
                program_id, part_counter = data
                # todo 根据 program_id 进行图片分析 -> 相同 program_id, 不同零件状态
                # 获取 frame
                images = await self.redis.get_all_shuttle_frames(
                    press_line=self.press_line,
                    program_id=program_id,
                    part_counter=part_counter,
                    timeout_sec=self.get_image_timeout
                )
                # 创建保存路径
                saved_dir = await self.make_saved_dir(program_id=program_id, part_counter=part_counter)

                # 连接数据库连接
                await Tortoise.init(config=TORTOISE_ORM)
                # 保存图片，写入数据库
                for camera_ip, (image, meta) in images.items():
                    # 定义图片名称
                    pic_name = self.define_picture_name(camera_user_id=meta.camera_user_id, pic_format=self.image_format)
                    # 保存图片
                    pic_path = await self.save_picture(image=image, saved_dir=saved_dir, picture_name=pic_name, overwrite=self.image_overwrite)
                    # 保存 数据库
                    frame_height, frame_width = meta.frame_shape[:2]
                    await ShuttleImage.create(
                        part_id=program_id,
                        part_count=part_counter,
                        camera_ip=camera_ip,
                        camera_user_id=meta.camera_user_id,
                        frame_num=meta.frame_num,
                        frame_t=meta.frame_t,
                        frame_width=frame_width,
                        frame_height=frame_height,
                        frame_size=meta.frame_size,
                        shuttle_has_part_t=meta.has_part_t,
                        image_path=pic_path,
                    )
                # 关闭数据库连接
                await Tortoise.close_connections()

                # 发送 udp multicast
                async with AsyncUdpMulticastServer(
                        multicast_ip=self.udp_multicast_ip,
                        multicast_port=self.udp_multicast_port,
                        interface_ip=self.udp_multicast_interface_ip
                ) as server:
                    await server.send("1")
            except Exception as err:
                _logger.exception(f"{self.identity} save images for {data} error: {err}")
            finally:
                self.queue.task_done()

        _logger.info(f"{self.identity} save images loop ended")

    @staticmethod
    def define_saved_dir(program_id: int, part_counter: int, saved_dir: str) -> str:
        # 获取当前时间
        now = datetime.now()
        # 定义并创建文件保存地址
        saved_dir = os.path.join(
            saved_dir,
            str(now.year),  # 年
            "{:02d}".format(now.month),  # 月
            "{:02d}".format(now.day),  # 日
            str(program_id),  # program_id
            str(part_counter)  # part_count
        )
        return saved_dir

    @staticmethod
    def define_picture_name(camera_user_id: str, prefix: str = "00", index: int = 0, pic_format: str = "png"):
        """定义 picture_name"""
        return f"{prefix}-{camera_user_id}-{index:02d}.{pic_format}"

    async def make_saved_dir(self, program_id: int, part_counter: int) -> str:
        # 定义文件夹路径
        saved_dir = ImageSaver.define_saved_dir(program_id=program_id, part_counter=part_counter, saved_dir=self.saved_dir)
        # 创建文件夹
        await self.loop.run_in_executor(self.executor, functools.partial(os.makedirs, saved_dir, exist_ok=True))
        return saved_dir

    async def save_picture(self, image: np.ndarray, saved_dir: str, picture_name: str, overwrite: bool = False):
        """保存 图片"""
        # 保存路径
        saved_pic_path = os.path.join(saved_dir, picture_name)
        # 文件存在
        if not overwrite and os.path.exists(saved_pic_path):
            raise FileExistsError(f"picture[{picture_name}] exists in saved dir[{saved_dir}] ")
        # 保存
        await self.loop.run_in_executor(
            self.executor,
            functools.partial(
                utils.save_image_by_cv,
                path=saved_pic_path,
                image=image,
                png_compression=0,
                jpg_quality=100,
            )
        )
        return saved_pic_path

    @staticmethod
    def files_counter(folder: str) -> int:
        """ 计算 文件夹 中 文件数量 """
        counter = 0
        with os.scandir(folder) as entries:
            for entry in entries:
                # 判断是否为文件
                if entry.is_file():
                    counter += 1
        return counter

    @property
    def identity(self):
        return f"[ImageSaver|Shuttle]"
