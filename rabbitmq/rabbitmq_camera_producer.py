import aio_pika
import json
import typing
import logging
import uuid

_logger = logging.getLogger(__name__)

class RabbitmqCameraProducer:
    def __init__(self, rabbitmq_url: str, location: typing.Optional[str] = "shuttle"):
        self.rabbitmq_url = rabbitmq_url

        self.connection: typing.Optional[aio_pika.RobustConnection] = None
        self.channel: typing.Optional[aio_pika.RobustChannel] = None
        self.exchange: typing.Optional[aio_pika.Exchange] = None
        self.response_queue: typing.Optional[aio_pika.Queue] = None

        self.location = location if location is not None else ""

        self.exchange_name = f"{self.location}.camera.ctrl"
        self.response_queue_name = f"{self.location}.camera.response.{uuid.uuid4().hex[:8]}"
        self.broadcast_routing_key = f"{self.location}.broadcast"

    def p2p_routing_key(self, ip) -> str:
        return f"{self.location}.camera.{ip}"

    async def connect(self):
        if not self.connection:
            self.connection = await aio_pika.connect_robust(self.rabbitmq_url)
            self.channel = await self.connection.channel()
            self.exchange = await self.channel.declare_exchange(
                name=self.exchange_name,
                type=aio_pika.ExchangeType.DIRECT,
                auto_delete=True,
            )
            self.response_queue = await self.channel.declare_queue(
                name=self.response_queue_name,
                exclusive=True,
                auto_delete=True
            )
            _logger.info(f"{self.identity} connected rabbit mq successfully")

    async def close(self):
        if self.connection:
            await self.connection.close()
            self.connection = None
            _logger.info(f"{self.identity} disconnected rabbit mq successfully")

    async def publish_cmd(self, camera_ips: typing.Optional[list[str]], cmds: tuple[tuple]) -> None:
        try:
            # 连接
            await self.connect()
            # 生成消息
            json_cmd = json.dumps(cmds)
            msg = aio_pika.Message(
                body=json_cmd.encode(),
                reply_to=self.response_queue.name
            )
            # 广播消息
            if not camera_ips:
                routing_key = self.broadcast_routing_key
                await self.exchange.publish(msg, routing_key=routing_key)
                _logger.info(f"{self.identity} {routing_key}: {json_cmd}")
            # p2p消息
            else:
                for ip in camera_ips:
                    routing_key = self.p2p_routing_key(ip)
                    await self.exchange.publish(msg, routing_key=routing_key)
                _logger.info(f"{self.identity} {camera_ips}: {json_cmd}")
        except Exception as err:
            _logger.exception(f"{self.identity} publish cmd {"broadcast" if not camera_ips else f"p2p"} error: {err}")

    async def response_listener(self):
        async with self.response_queue.iterator() as it:
            async for message in it:
                # async with message.process():
                yield message

    @property
    def identity(self):
        return f"RabbitMQ[CameraCtrlProducer]"

if __name__ == "__main__":
    import asyncio

    def init_logger():
        # 创建logger对象
        logger = logging.getLogger()
        # 设置全局最低等级（让所有handler能接收到）
        logger.setLevel(logging.DEBUG)

        # === 控制台 Handler（只显示 WARNING 及以上） ===
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        console_handler.setFormatter(console_formatter)
        # 添加 handler 到 logger
        logger.addHandler(console_handler)

    async def get_input():
        cmd_input = await asyncio.to_thread(
            input,
            "请输入命令 (格式: 192.168.1.1,192.168.1.2 open set,fps,10) 或 exit 退出: "
        )
        cmd_input = cmd_input.strip()
        if cmd_input.lower() in ("exit", "quit"):
            print("quit")
            return None
        parts = cmd_input.split()

        ips = parts[0]
        if ips.lower() == "all":
            ips = None
        else:
            ips = ips.split(",")

        cmds = list()
        for cmd in parts[1:]:
            cmd = cmd.split(",")
            cmds.append(cmd)
        print(f"ips: {ips}, cmds: {cmds}")
        return ips, cmds

    async def handle_response(producer):
        async for message in producer.response_listener():
            async with message.process():
                try:
                    data = json.loads(message.body)
                    print("📩 收到响应:", data)
                except Exception as err:
                    print("解析失败:", err)

    async def main(rabbit_url):
        p = RabbitmqCameraProducer(rabbit_url)
        try:
            await p.connect()
            asyncio.create_task(handle_response(p))
            while True:
                _input = await get_input()
                if not _input:
                    break
                ips, cmds = _input
                await p.publish_cmd(camera_ips=ips, cmds=cmds)
        except KeyboardInterrupt:
            print("\n KeyboardInterrupt")
        finally:
            await p.close()


    url = "amqp://admin:123@localhost/"
    init_logger()
    asyncio.run(main(url))

