import asyncio
import time
from datetime import datetime, timedelta, timezone
import numpy as np
import typing
import redis
import logging

from .key import FrameMetaT, PressKey, ShuttleKey, ShuttleMeta

_logger = logging.getLogger(__name__)


'''
redis中数据形式：
    press:
        程序号 -> xadd:
            press:programId:pressLine -> dict {"program_id": "id"}
        运行状态 -> xadd:
            press:runningStatus:pressLine -> dict {"running_status": "0"}
        零件计数 -> publish:
            press:partCounter:pressLine -> str 0
            
    shuttle:
        运行相机 -> sadd：
            shuttle:runningCamera:pressLine -> set, {ips}
            例：shuttle:runningCamera:5-100 -> {192.168.1.1}
        matrix数组 -> set： 
            shuttle:matrix:pressLine:programId:partCounter:cameraIp -> matrix(bytes, numpy数组, expire)
            例：shuttle:matrix:5-100:1:1:192.168.1.1 -> matrix
        matrix元数据 -> hset： 
            shuttle:matrix:pressLine:programId:partCounter:cameraIp -> meta(Hash，键值对, expire)
            例：shuttle:matrix:5-100:1:1:192.168.1.1 -> meta
        已完成相机 -> set
            shuttle:photographed:pressLine:programId:partCounter -> set, {ips}
            例：shuttle:photographed:5-100:1:1 -> {192.168.1.1}
        灯 -> int
            shuttle:lightEnable:pressLine -> key, int

'''


def _decode_bytes(data):
    return data.decode() if isinstance(data, bytes) else data


def _decode_stream_msg(msg_id, msg_data):
    _data = {_decode_bytes(k): _decode_bytes(v) for k, v in msg_data.items()}
    _id =_decode_bytes(msg_id)
    return _id, _data


class AsyncRedisDB(redis.asyncio.Redis):
    def __init__(self, host, port, db):

        connection_pool = redis.asyncio.connection.ConnectionPool(
            host=host,
            port=port,
            db=db,
            decode_responses=False,     # 重要，用于图片保存
        )

        super().__init__(connection_pool=connection_pool)

    @classmethod
    async def create(cls, **kwargs) -> typing.Self:
        ping = kwargs.pop('ping', False)
        # 创建实例
        instance = cls(**kwargs)
        # 测试连接池
        # redis.exceptions.ConnectionError
        # redis.exceptions.TimeoutError
        if ping:
            await instance.ping()

        _logger.info(f"{instance.identity} redis connect successfully")

        return instance

    # --------------------------------------------------------------------------- #
    # stream 基础方法
    # --------------------------------------------------------------------------- #
    async def get_stream_tail(
            self,
            stream_key: str,
            block: typing.Union[None, int, float] = None,
            include_last: bool = True
    ) -> typing.AsyncGenerator[tuple[typing.Any, typing.Optional[dict[str, typing.Any]]], None]:
        """
        异步生成器，先返回最后一条消息（可选），然后持续返回新消息。
        :param stream_key:
        :param block: 阻塞时间，单位毫秒；None 或 0 表示无限阻塞
        :param include_last: 是否先返回最后一条历史消息
        :return: (msg_id, msg_data)
        """
        # 先取最后一条消息
        if not include_last:
            # 不包含最后一条消息，从最新开始
            last_id = "$"
        else:
            latest = await self.xrevrange(stream_key, count=1)
            if latest:
                last_id, last_data = latest[0]
                _id, _data = _decode_stream_msg(last_id, last_data)
                yield _id, _data
            else:
                # 没有消息或不返回历史，从最新开始
                last_id = "$"

        # 持续监听新消息
        while True:
            # block = None 或 0 表示无限阻塞
            timeout = None if block is None or block <= 0 else int(block)
            msgs = await self.xread({stream_key: last_id}, block=timeout, count=1)
            if msgs:
                # msgs 格式: [('stream key', [('1695025400000-0', {"program_id": "id"})])]
                for stream, events in msgs:
                    for msg_id, msg_data in events:
                        # 避免重复消费最后一条
                        if msg_id != last_id:
                            _id, _data = _decode_stream_msg(msg_id, msg_data)
                            yield _id, _data
                        last_id = msg_id
            else:
                # 没消息也交出一次控制权
                yield None, None

    async def get_latest_stream(self, stream_key: str) -> tuple[typing.Any, typing.Optional[dict[str, typing.Any]]]:
        """
        获取最后一条消息
        :param stream_key:
        :return:
        """
        latest = await self.xrevrange(stream_key, count=1)
        if latest:
            last_id, last_data = latest[0]
            _id, _data = _decode_stream_msg(last_id, last_data)
            return _id, _data
        else:
            raise ValueError(f"{stream_key} is empty")

    # --------------------------------------------------------------------------- #
    # subscribe 基础方法
    # --------------------------------------------------------------------------- #
    @staticmethod
    async def subscribe_reader(pubsub: redis.asyncio.client.PubSub, callback: typing.Callable):
        async for message in pubsub.listen():
            if message["type"] != "message":
                continue
            data = message["data"]
            data = _decode_bytes(data)
            await callback(data)

    # --------------------------------------------------------------------------- #
    # 通过 meta 从redis解析 np.ndarray
    # --------------------------------------------------------------------------- #
    @staticmethod
    def decode_frame_bytes(
            frame_bytes: bytes,
            frame_meat: dict,
            meta_class: typing.Type[FrameMetaT]
    ) -> tuple[np.ndarray, FrameMetaT]:
        # 元数据
        meta = {
            _decode_bytes(k): _decode_bytes(v) for k, v in frame_meat.items()
        }
        meta = meta_class.create(**meta)

        # 原始 numpy 二进制数据
        frame = np.frombuffer(
            buffer=frame_bytes,
            count=meta.frame_size,
            dtype=np.dtype(meta.frame_dtype),
            offset=0
        )
        frame = np.reshape(frame, meta.frame_shape)
        return frame, meta

    # --------------------------------------------------------------------------- #
    # press -> program_id
    # --------------------------------------------------------------------------- #
    async def set_program_id(self, program_id: int, press_line: str, maxlen: int = 1000):
        """
        发布 program_id, 加入 key.program_id_key stream
        :param maxlen:
        :param program_id:
        :param press_line:
        :return:
        """
        key = PressKey.create(press_line=press_line)
        # Add to stream
        await self.xadd(
            key.program_id_key,
            {"program_id": program_id},
            maxlen=maxlen,      # 限制最大长度
            approximate=True    # 使用 ~，近似裁剪，效率高
        )

    async def get_latest_program_id(self, press_line: str) -> tuple:
        key = PressKey.create(press_line=press_line)
        try:
            msg_id, msg_data = await self.get_latest_stream(key.program_id_key)
            # 解析 msg_data
            program_id = int(msg_data["program_id"]) if "program_id" in msg_data else None
            # 时间戳
            timestamp_ms = int(msg_id.split("-")[0])
            return timestamp_ms, program_id
        except Exception as err:
            raise ValueError(f"program id stream is empty") from err

    async def get_program_id(
            self,
            press_line: str,
            block: typing.Union[None, int, float] = None,
            include_last: bool = True
    ) -> typing.AsyncGenerator[tuple[typing.Optional[int], typing.Optional[int]], None]:
        """
        异步生成器，先返回最后一条消息（可选），然后持续返回新消息。
        :param press_line: 生产线
        :param block: 阻塞时间，单位毫秒；None 或 0 表示无限阻塞
        :param include_last: 是否先返回最后一条历史消息
        :return: int program_id
        """
        key = PressKey.create(press_line=press_line)
        async for msg_id, msg_data in self.get_stream_tail(
                stream_key=key.program_id_key,
                block=block,
                include_last=include_last
        ):
            # 阻塞后没有消息
            if msg_data is None:
                yield None, None
            else:
                # 解析 msg_data
                program_id = int(msg_data["program_id"]) if "program_id" in msg_data else None
                # 时间戳
                timestamp_ms = int(msg_id.split("-")[0])
                # 转换为 datetime
                dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                _logger.info(f"{self.identity} program id={program_id}, time={dt}")
                yield timestamp_ms, program_id

    async def del_program_id(self, press_line: str):
        key = PressKey.create(press_line=press_line)
        await self.delete(key.program_id_key)

    # --------------------------------------------------------------------------- #
    # press -> running_status
    # --------------------------------------------------------------------------- #
    async def set_running_status(self, running_status: bool, press_line: str, maxlen: int = 1000):
        """
        发布 running_status, 加入 key.running_status_key stream
        :param maxlen:
        :param running_status:
        :param press_line:
        :return:
        """
        key = PressKey.create(press_line=press_line)
        # Add to stream
        await self.xadd(
            key.running_status_key,
            {"running_status": int(running_status)},
            maxlen=maxlen,        # 限制最大长度
            approximate=True    # 使用 ~，近似裁剪，效率高
        )

    async def get_latest_running_status(self, press_line: str) -> tuple:
        key = PressKey.create(press_line=press_line)
        try:
            msg_id, msg_data = await self.get_latest_stream(key.running_status_key)
            # 解析 msg_data
            running_status = bool(int(msg_data["running_status"])) if "running_status" in msg_data else None
            # 时间戳
            timestamp_ms = int(msg_id.split("-")[0])
            return timestamp_ms, running_status
        except Exception as err:
            raise ValueError(f"running status stream is empty") from err

    async def get_running_status(
            self,
            press_line: str,
            block: typing.Union[None, int, float] = None,
            include_last: bool = True
    ) -> typing.AsyncGenerator[tuple[typing.Optional[int], typing.Optional[bool]], None]:
        """
        异步生成器，先返回最后一条消息（可选），然后持续返回新消息。
        :param press_line: 生产线
        :param block: 阻塞时间，单位毫秒；None 或 0 表示无限阻塞
        :param include_last: 是否先返回最后一条历史消息
        :return: bool running_status
        """
        key = PressKey.create(press_line=press_line)
        async for msg_id, msg_data in self.get_stream_tail(
                stream_key=key.running_status_key,
                block=block,
                include_last=include_last
        ):
            # 阻塞后没有消息
            if msg_data is None:
                yield None, None
            else:
                # 解析 msg_data
                running_status = bool(int(msg_data["running_status"])) if "running_status" in msg_data else None
                # 时间戳
                timestamp_ms = int(msg_id.split("-")[0])
                # 转换为 datetime
                dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                _logger.info(f"{self.identity} running status={running_status}, time={dt}")
                yield timestamp_ms, running_status

    async def del_running_status(self, press_line: str):
        key = PressKey.create(press_line=press_line)
        await self.delete(key.running_status_key)

    # --------------------------------------------------------------------------- #
    # press -> part_counter
    # --------------------------------------------------------------------------- #
    async def set_part_counter(self, part_counter: int, press_line: str, maxlen: int = 1000):
        key = PressKey.create(press_line=press_line)
        await self.xadd(
            key.part_counter_key,
            {"part_counter": part_counter},
            maxlen=maxlen,  # 限制最大长度
            approximate=True  # 使用 ~，近似裁剪，效率高
        )

    async def get_latest_part_counter(self, press_line: str) -> tuple:
        key = PressKey.create(press_line=press_line)
        try:
            msg_id, msg_data = await self.get_latest_stream(key.part_counter_key)
            # 解析 msg_data
            part_counter = int(msg_data["part_counter"]) if "part_counter" in msg_data else None
            # 时间戳
            timestamp_ms = int(msg_id.split("-")[0])
            return timestamp_ms, part_counter
        except Exception as err:
            raise ValueError(f"part counter stream is empty") from err

    async def get_part_counter(
            self,
            press_line: str,
            block: typing.Union[None, int, float] = None,
            include_last: bool = True
    ) -> typing.AsyncGenerator[tuple[typing.Optional[int], typing.Optional[int]], None]:
        """
        异步生成器，先返回最后一条消息（可选），然后持续返回新消息。
        :param press_line: 生产线
        :param block: 阻塞时间，单位毫秒；None 或 0 表示无限阻塞
        :param include_last: 是否先返回最后一条历史消息
        :return: int part_counter
        """
        key = PressKey.create(press_line=press_line)
        async for msg_id, msg_data in self.get_stream_tail(
                stream_key=key.part_counter_key,
                block=block,
                include_last=include_last
        ):
            # 阻塞后没有消息
            if msg_data is None:
                yield None, None
            else:
                # 解析 msg_data
                part_counter = int(msg_data["part_counter"]) if "part_counter" in msg_data else None
                # 时间戳
                timestamp_ms = int(msg_id.split("-")[0])
                # 转换为 datetime
                dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                _logger.info(f"{self.identity} part_counter={part_counter}, time={dt}")
                yield timestamp_ms, part_counter

    async def del_part_counter(self, press_line: str):
        key = PressKey.create(press_line=press_line)
        await self.delete(key.part_counter_key)

    # --------------------------------------------------------------------------- #
    # shuttle -> running_camera
    # --------------------------------------------------------------------------- #
    async def add_running_camera(self, ip: str, press_line: str):
        key = ShuttleKey.create(press_line=press_line)
        await self.sadd(key.running_camera_key, ip)

    async def remove_running_camera(self, ip: str, press_line: str):
        key = ShuttleKey.create(press_line=press_line)
        await self.srem(key.running_camera_key, ip)
        # 集合为空，删除
        if not await self.get_running_cameras_number(press_line=press_line):
            await self.delete(key.running_camera_key)

    async def get_running_cameras_number(self, press_line: str) -> int:
        key = ShuttleKey.create(press_line=press_line)
        return await self.scard(key.running_camera_key)

    async def is_camera_running(self, ip: str, press_line: str) -> bool:
        key = ShuttleKey.create(press_line=press_line)
        return bool(await self.sismember(key.running_camera_key, ip))

    async def get_running_cameras(self, press_line: str) -> list:
        key = ShuttleKey.create(press_line=press_line)
        ips = await self.smembers(key.running_camera_key)
        ips = [_decode_bytes(ip) for ip in ips]
        return ips

    # --------------------------------------------------------------------------- #
    # shuttle -> frame
    # --------------------------------------------------------------------------- #
    async def set_shuttle_frame(self, press_line: str, program_id: int, part_counter: int, camera_ip: str, matrix: np.ndarray, **kwargs):
        """
        将 shuttle_frame 数组 存入 Redis
        :param matrix:
        :param camera_ip:
        :param part_counter:
        :param program_id:
        :param press_line:
        :param kwargs:
        :return:
        """
        # 过期时间 60秒
        expire_sec = kwargs.pop("expire_sec", 60)

        key = ShuttleKey.create(
            press_line=press_line,
            program_id=program_id,
            part_counter=part_counter,
            camera_ip=camera_ip,
        )

        meta = ShuttleMeta.create(
            frame_shape=matrix.shape,
            frame_size=matrix.size,
            frame_dtype=str(matrix.dtype),
            program_id=program_id,
            part_counter=part_counter,
            camera_ip=camera_ip,
            **kwargs
        )

        async with self.pipeline(transaction=False) as pipe:
            # 保存 matrix
            pipe.set(key.matrix_key, matrix.tobytes(), ex=expire_sec)
            # 保存 meta
            pipe.hset(key.meta_key, mapping=meta.to_dict())
            pipe.expire(key.meta_key, expire_sec)
            # 已完成拍照相机
            pipe.sadd(key.photographed_key, meta.camera_ip)
            pipe.expire(key.photographed_key, expire_sec)

            await pipe.execute()

    async def get_photographed_number(self, press_line: str, program_id: int, part_counter: int) -> int:
        key = ShuttleKey.create(
            press_line=press_line,
            program_id=program_id,
            part_counter=part_counter,
        )
        return await self.scard(key.photographed_key)

    async def get_photographed_ips(self, press_line: str, program_id: int, part_counter: int) -> list:
        key = ShuttleKey.create(
            press_line=press_line,
            program_id=program_id,
            part_counter=part_counter,
        )
        ips =  await self.smembers(key.photographed_key)
        ips = [_decode_bytes(ip) for ip in ips]
        return ips

    async def get_unphotographed(self, press_line: str, program_id: int, part_counter: int) -> list:
        key= ShuttleKey.create(
            press_line=press_line,
            program_id=program_id,
            part_counter=part_counter,
        )
        unphotographed = await self.sdiff(key.running_camera_key, key.photographed_key)
        return unphotographed

    async def get_shuttle_frame(self, press_line: str, program_id: int, part_counter: int, camera_ip: str) -> tuple[np.ndarray, ShuttleMeta]:
        key = ShuttleKey.create(
            press_line=press_line,
            program_id=program_id,
            part_counter=part_counter,
            camera_ip=camera_ip,
        )
        async with self.pipeline(transaction=False) as pipe:
            pipe.get(key.matrix_key)
            pipe.hgetall(key.meta_key)
            res = await pipe.execute()
        # 获取 matrix
        raw_matrix, raw_meta = res
        # 解析
        return self.decode_frame_bytes(frame_bytes=raw_matrix, frame_meat=raw_meta, meta_class=ShuttleMeta)

    async def get_all_shuttle_frames(self, press_line: str, program_id: int, part_counter: int, timeout_sec: int = 20) -> dict[str, tuple[np.ndarray, ShuttleMeta]]:
        # 获取所有相机ip
        running_cameras = set(await self.get_running_cameras(press_line=press_line))
        # 等待所有相机拍照完成
        start = time.time()
        while True:
            photographed_ips = set(await self.get_photographed_ips(press_line=press_line, program_id=program_id, part_counter=part_counter))
            if not (unphotographed := running_cameras - photographed_ips):
                break
            if time.time() - start > timeout_sec:
                raise TimeoutError(f"camera{list(unphotographed)} get frame timeout")
            await asyncio.sleep(0.1)
        # 一次性批量取 Redis 数据
        async with self.pipeline(transaction=False) as pipe:
            for camera_ip in running_cameras:
                key = ShuttleKey.create(
                    press_line=press_line,
                    program_id=program_id,
                    part_counter=part_counter,
                    camera_ip=camera_ip,
                )
                pipe.get(key.matrix_key)
                pipe.hgetall(key.meta_key)
            res = await pipe.execute()
        # 解析结果，每个 ip 对应 2 个返回值（get 和 hgetall）
        frames = dict()
        for camera_ip, (raw_matrix, raw_meta) in zip(running_cameras, zip(res[::2], res[1::2])):
            frames[camera_ip] = self.decode_frame_bytes(frame_bytes=raw_matrix, frame_meat=raw_meta, meta_class=ShuttleMeta)
        return frames

    # --------------------------------------------------------------------------- #
    # shuttle -> lightEnable
    # --------------------------------------------------------------------------- #
    async def set_light_enable(self, press_line: str, disable_after: typing.Optional[int] = None):
        key = ShuttleKey.create(press_line=press_line)
        if await self.exists(key.light_enable_key):
            if disable_after is None:
                await self.persist(key.light_enable_key)
            else:
                await self.expire(key.light_enable_key, disable_after)
        else:
            if disable_after is None:
                await self.set(key.light_enable_key, True)
            else:
                await self.set(key.light_enable_key, True, ex=disable_after)

    async def set_light_disable(self, press_line: str, after: typing.Optional[int] = 600):
        key = ShuttleKey.create(press_line=press_line)
        if after is None:
            await self.delete(key.light_enable_key)
        else:
            await self.expire(key.light_enable_key, after)

    async def get_light_enable(self, press_line: str) -> bool:
        key = ShuttleKey.create(press_line=press_line)
        return bool(await self.exists(key.light_enable_key))

    async def get_light_enable_ttl(self, press_line: str) -> int:
        key = ShuttleKey.create(press_line=press_line)
        return await self.ttl(key.light_enable_key)


    # async def get_thermal(self, press_line: str, camera_serial: str, frame_count: int) -> tuple[np.ndarray, ThermalMeta]:
    #
    #     key = ThermalKey.create(press_line=press_line, camera_serial=camera_serial, frame_count=frame_count)
    #
    #     # 获取 matrix
    #     raw_matrix = await self.get(key.matrix_key)
    #     # 获取 meta
    #     raw_meta = await self.hgetall(key.meta_key)
    #
    #     # 未获取到
    #     if raw_matrix is None or not raw_meta:
    #         raise ValueError(f"get thermal failed with line[{press_line}], camera[{camera_serial}], frame[{frame_count}]")
    #
    #     # 解析
    #     matrix, meta = self.decode_thermal_matrix_bytes(raw_matrix, raw_meta)
    #
    #     return matrix, meta
    #
    # async def subscribe_thermal(self, press_line: str, camera_serial: str, callback: typing.Callable[[str, str, int], typing.Awaitable[None]]):
    #     """
    #     订阅某个 counter_key，当有新帧发布时调用 callback
    #     :param press_line:
    #     :param camera_serial:
    #     :param callback: async 函数，接收 frame_count(str, str, int)
    #     """
    #     async def reader():
    #         async for message in pubsub.listen():
    #             if message["type"] != "message":
    #                 continue
    #             try:
    #                 data = message["data"]
    #                 frame_count = int(data.decode() if isinstance(data, bytes) else data)
    #                 await callback(press_line, camera_serial, frame_count)
    #             except Exception as err:
    #                 _logger.exception(f"{self.identity} subscribe thermal callback error: {err}")
    #
    #     key = ThermalKey.create(press_line=press_line,camera_serial=camera_serial, frame_count=None)
    #
    #     pubsub: redis.asyncio.client.PubSub = self.pubsub()
    #     await pubsub.subscribe(key.counter_key)
    #
    #     # 启动一个后台任务监听
    #     task = asyncio.create_task(reader())
    #     return pubsub, task
    #
    # @staticmethod
    # def decode_thermal_matrix_bytes(raw_matrix: bytes, raw_meta: dict) -> tuple[np.ndarray, ThermalMeta]:
    #
    #     # 元数据
    #     meta = {
    #         k.decode() if isinstance(k, bytes) else k: v.decode() if isinstance(v, bytes) else v
    #         for k, v in raw_meta.items()
    #     }
    #     meta = ThermalMeta.create(**meta)
    #
    #     # 原始 numpy 二进制数据
    #     matrix = np.frombuffer(buffer=raw_matrix, count=meta.size, dtype=np.dtype(meta.dtype), offset=0)
    #     matrix = np.reshape(matrix, meta.shape)
    #
    #     return matrix, meta
    #
    # async def set_image(self, image: np.ndarray, press_line: str, camera_serial: str, frame_count: int, **kwargs):
    #     """
    #     将 image numpy 数组 存入 Redis
    #     :param image:
    #     :param press_line:
    #     :param camera_serial:
    #     :param frame_count:
    #     :return:
    #     """
    #     key = ImageKey.create(press_line=press_line, camera_serial=camera_serial, frame_count=frame_count)
    #     meta = ImageMeta.create(
    #         shape=image.shape,
    #         size=image.size,
    #         dtype=str(image.dtype),
    #         **kwargs,
    #     )
    #     async with self.pipeline(transaction=False) as pipe:
    #         # 保存 image
    #         pipe.set(key.matrix_key, image.tobytes(), ex=self.expire_sec)
    #         # 保存 meta
    #         pipe.hset(key.meta_key, mapping=meta.to_dict())
    #         pipe.expire(key.meta_key, self.expire_sec)
    #         # 发布
    #         pipe.publish(key.counter_key, str(frame_count))
    #         await pipe.execute()
    #
    # async def get_image(self, press_line: str, camera_serial: str, frame_count: int) -> tuple[np.ndarray, ImageMeta]:
    #     key = ImageKey.create(press_line=press_line, camera_serial=camera_serial, frame_count=frame_count)
    #
    #     # 获取 image
    #     raw_image = await self.get(key.matrix_key)
    #     # 获取 meta
    #     raw_meta = await self.hgetall(key.meta_key)
    #
    #     # 未获取到
    #     if raw_image is None or not raw_meta:
    #         raise ValueError(f"get image failed with line[{press_line}], camera[{camera_serial}], frame[{frame_count}]")
    #
    #     # 解析
    #     image, meta = self.decode_image_bytes(raw_image, raw_meta)
    #
    #     return image, meta
    #
    # async def subscribe_image(self, press_line: str, camera_serial: str, callback: typing.Callable[[str, str, int], typing.Awaitable[None]]):
    #     """
    #     订阅某个 counter_key，当有新帧发布时调用 callback
    #     :param press_line:
    #     :param camera_serial:
    #     :param callback: async 函数，接收 frame_count(str, str, int)
    #     """
    #     async def reader():
    #         async for message in pubsub.listen():
    #             if message["type"] != "message":
    #                 continue
    #             try:
    #                 data = message["data"]
    #                 frame_count = int(data.decode() if isinstance(data, bytes) else data)
    #                 await callback(press_line, camera_serial, frame_count)
    #             except Exception as err:
    #                 _logger.exception(f"{self.identity} subscribe image callback error: {err}")
    #
    #     key = ImageKey.create(press_line=press_line,camera_serial=camera_serial, frame_count=None)
    #
    #     pubsub: redis.asyncio.client.PubSub = self.pubsub()
    #     await pubsub.subscribe(key.counter_key)
    #
    #     # 启动一个后台任务监听
    #     task = asyncio.create_task(reader())
    #     return pubsub, task
    #
    # @staticmethod
    # def decode_image_bytes(raw_image: bytes, raw_meta: dict) -> tuple[np.ndarray, ImageMeta]:
    #     # 元数据
    #     meta = {
    #         k.decode() if isinstance(k, bytes) else k: v.decode() if isinstance(v, bytes) else v
    #         for k, v in raw_meta.items()
    #     }
    #     meta = ImageMeta.create(**meta)
    #
    #     # 原始 numpy 二进制数据
    #     image = np.frombuffer(buffer=raw_image, count=meta.size, dtype=np.dtype(meta.dtype), offset=0)
    #     image = np.reshape(image, meta.shape)
    #
    #     return image, meta
    #
    #
    #
    # async def set_matrix_process_params(self, press_line: str, camera_serial: str, program_id: int, params: dict):
    #     key = MatrixProcessKey.create(press_line=press_line, camera_serial=camera_serial, program_id=program_id)
    #     # 发布，将dict转为json
    #     await self.publish(key.params_key, json.dumps(params))
    #
    # async def subscribe_matrix_process_params(
    #         self,
    #         press_line: str,
    #         camera_serial: str,
    #         program_id: int,
    #         callback: typing.Callable[[str, str, dict], typing.Awaitable[None]]
    # ):
    #     async def reader():
    #         async for message in pubsub.listen():
    #             if message["type"] != "message":
    #                 continue
    #             data = message["data"]
    #             # decode 并反序列化 JSON
    #             params: dict = json.loads(data.decode() if isinstance(data, bytes) else data)
    #             try:
    #                 # 传递 dict
    #                 await callback(press_line, camera_serial, params)
    #             except Exception as err:
    #                 _logger.exception(f"{self.identity} subscribe matrix process params callback error: {err}")
    #
    #     key = MatrixProcessKey.create(press_line=press_line, camera_serial=camera_serial, program_id=program_id)
    #
    #     pubsub: redis.asyncio.client.PubSub = self.pubsub()
    #     await pubsub.subscribe(key.params_key)
    #
    #     # 启动一个后台任务监听
    #     task = asyncio.create_task(reader())
    #     return pubsub, task
    #
    # async def set_new_max_temp(
    #         self,
    #         press_line: str,
    #         camera_serial: str,
    #         program_id: int,
    #         start_date: datetime.date,
    #         counter: int
    # ):
    #     key = MaxTempKey.create(press_line=press_line, camera_serial=camera_serial)
    #     info = {
    #         "counter": counter,
    #         "program_id": program_id,
    #         "start_date": start_date.isoformat() if isinstance(start_date, datetime.date) else start_date
    #     }
    #     await self.publish(key.new_key, json.dumps(info))
    #
    # async def subscribe_new_max_temp(
    #         self,
    #         press_line: str,
    #         camera_serial: str,
    #         callback: typing.Callable[[str, str, dict], typing.Awaitable[None]]
    # ):
    #     async def reader():
    #         async for message in pubsub.listen():
    #             if message["type"] != "message":
    #                 continue
    #             data = message["data"]
    #             # decode 并反序列化 JSON
    #             info: dict = json.loads(data.decode() if isinstance(data, bytes) else data)
    #             try:
    #                 await callback(press_line, camera_serial, info)
    #             except Exception as err:
    #                 _logger.exception(f"{self.identity} subscribe new max temp callback error: {err}")
    #
    #     key = MaxTempKey.create(press_line=press_line, camera_serial=camera_serial)
    #
    #     pubsub: redis.asyncio.client.PubSub = self.pubsub()
    #     await pubsub.subscribe(key.new_key)
    #
    #     # 启动一个后台任务监听
    #     task = asyncio.create_task(reader())
    #     return pubsub, task

    @property
    def identity(self):
        db = self.connection_pool.connection_kwargs["db"]
        return f"Redis[{db}]"
