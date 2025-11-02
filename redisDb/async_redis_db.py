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
        零件计数 -> xadd:
            press:partCounter:pressLine -> dict {"part_counter": "0"}
            
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


def timestamp_ms_2_strf(timestamp_ms: int) -> str:
    dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone(timedelta(hours=8)))
    return dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]


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

        _logger.info(f"{instance.identity} connected successfully")

        return instance

    async def aclose(self, close_connection_pool: typing.Optional[bool] = None) -> None:
        await super().aclose(close_connection_pool=close_connection_pool)
        _logger.info(f"{self.identity} disconnected successfully")

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
                _logger.debug(f"{self.identity} get_stream_tail({stream_key})=({_id},{_data})")
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
                            _logger.debug(f"{self.identity} get_stream_tail({stream_key})=({_id},{_data})")
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
            _logger.debug(f"{self.identity} get_latest_stream({stream_key})=({_id},{_data})")
            return _id, _data
        else:
            raise ValueError(f"stream[{stream_key}] is empty")

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
            # todo add logger
            await callback(data)

    # --------------------------------------------------------------------------- #
    # 通过 meta 从redis解析 np.ndarray
    # --------------------------------------------------------------------------- #
    @staticmethod
    def decode_frame_bytes(frame_bytes: bytes, frame_meat: dict, meta_class: typing.Type[FrameMetaT]) -> tuple[np.ndarray, FrameMetaT]:
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
        msg_id, msg_data = await self.get_latest_stream(key.program_id_key)
        # 解析 msg_data
        program_id = int(msg_data["program_id"]) if "program_id" in msg_data else None
        # 时间戳
        timestamp_ms = int(msg_id.split("-")[0])
        _logger.debug(f"{self.identity} get_latest_program_id({press_line})=({timestamp_ms_2_strf(timestamp_ms)},{program_id})")
        return timestamp_ms, program_id

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
                _logger.debug(f"{self.identity} get_program_id({press_line})=({timestamp_ms_2_strf(timestamp_ms)},{program_id})")
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
        msg_id, msg_data = await self.get_latest_stream(key.running_status_key)
        # 解析 msg_data
        running_status = bool(int(msg_data["running_status"])) if "running_status" in msg_data else None
        # 时间戳
        timestamp_ms = int(msg_id.split("-")[0])
        _logger.debug(f"{self.identity} get_latest_running_status({press_line})=({timestamp_ms_2_strf(timestamp_ms)},{running_status})")
        return timestamp_ms, running_status

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
                _logger.debug(f"{self.identity} get_running_status({press_line})=({timestamp_ms_2_strf(timestamp_ms)},{running_status})")
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
        msg_id, msg_data = await self.get_latest_stream(key.part_counter_key)
        # 解析 msg_data
        part_counter = int(msg_data["part_counter"]) if "part_counter" in msg_data else None
        # 时间戳
        timestamp_ms = int(msg_id.split("-")[0])
        _logger.debug(f"{self.identity} get_latest_part_counter({press_line})=({timestamp_ms_2_strf(timestamp_ms)},{part_counter})")
        return timestamp_ms, part_counter

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
                _logger.debug(f"{self.identity} get_part_counter({press_line})=({timestamp_ms_2_strf(timestamp_ms)},{part_counter})")
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
        number = await self.scard(key.running_camera_key)
        _logger.debug(f"{self.identity} get_running_cameras_number({press_line})={number}")
        return number

    async def is_camera_running(self, ip: str, press_line: str) -> bool:
        key = ShuttleKey.create(press_line=press_line)
        is_running = bool(await self.sismember(key.running_camera_key, ip))
        _logger.debug(f"{self.identity} is_camera_running({ip},{press_line})={is_running}")
        return is_running

    async def get_running_cameras(self, press_line: str) -> list:
        key = ShuttleKey.create(press_line=press_line)
        ips = await self.smembers(key.running_camera_key)
        ips = [_decode_bytes(ip) for ip in ips]
        _logger.debug(f"{self.identity} get_running_cameras({press_line})={ips}")
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
        number = await self.scard(key.photographed_key)
        _logger.debug(f"{self.identity} get_photographed_number({press_line},{program_id},{part_counter})={number}")
        return number

    async def get_photographed_ips(self, press_line: str, program_id: int, part_counter: int) -> list:
        key = ShuttleKey.create(
            press_line=press_line,
            program_id=program_id,
            part_counter=part_counter,
        )
        ips =  await self.smembers(key.photographed_key)
        ips = [_decode_bytes(ip) for ip in ips]
        _logger.debug(f"{self.identity} get_photographed_ips({press_line},{program_id},{part_counter})={ips}")
        return ips

    async def get_unphotographed_ips(self, press_line: str, program_id: int, part_counter: int) -> list:
        key= ShuttleKey.create(
            press_line=press_line,
            program_id=program_id,
            part_counter=part_counter,
        )
        ips = await self.sdiff(key.running_camera_key, key.photographed_key)
        ips = [_decode_bytes(ip) for ip in ips]
        _logger.debug(f"{self.identity} get_unphotographed_ips({press_line},{program_id},{part_counter})={ips}")
        return ips

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
        # 等待所有相机拍照完成
        start = time.time()
        while True:
            unphotographed_ips = set(await self.get_unphotographed_ips(press_line=press_line, program_id=program_id, part_counter=part_counter))
            if not unphotographed_ips:
                break
            if time.time() - start > timeout_sec:
                raise TimeoutError(f"camera{list(unphotographed_ips)} get frame timeout")
            await asyncio.sleep(0.1)

        # 获取所有相机ip
        running_cameras = set(await self.get_running_cameras(press_line=press_line))

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
                await self.set(key.light_enable_key, int(True))
            else:
                await self.set(key.light_enable_key, int(True), ex=disable_after)

    async def set_light_disable(self, press_line: str, after: typing.Optional[int] = 600):
        key = ShuttleKey.create(press_line=press_line)
        if after is None:
            if await self.exists(key.light_enable_key):
                await self.delete(key.light_enable_key)
        else:
            # 没有设置 ttl
            if await self.get_light_enable_ttl(press_line) == -1:
                await self.expire(key.light_enable_key, after)

    async def get_light_enable(self, press_line: str) -> bool:
        """获取 light_enable"""
        key = ShuttleKey.create(press_line=press_line)
        enable = bool(await self.exists(key.light_enable_key))
        _logger.debug(f"{self.identity} get_light_enable({press_line})={enable}")
        return enable

    async def get_light_enable_ttl(self, press_line: str) -> int:
        """获取 light_enable 的剩余时间"""
        key = ShuttleKey.create(press_line=press_line)
        ttl = await self.ttl(key.light_enable_key)
        _logger.debug(f"{self.identity} get_light_enable_ttl({press_line})={ttl}")
        return ttl

    @property
    def identity(self):
        db = self.connection_pool.connection_kwargs["db"]
        return f"Redis[{db}]"


if __name__ == "__main__":

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


    async def main():
        # async with AsyncRedisDB(host="127.0.0.1", port=6379, db=0) as r:
        #     await r.get_light_enable("5-100")

        r = await AsyncRedisDB.create(host="127.0.0.1", port=6379, db=0)
        await r.set_program_id(1, "5-100")

        await r.get_latest_program_id("5-100")

        await r.aclose()

    init_logger()
    asyncio.run(main())
