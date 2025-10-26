import aio_pika
import json
import typing
import logging

_logger = logging.getLogger(__name__)

class RabbitmqCameraConsumer:
    def __init__(self, rabbitmq_url: str, camera_ip: str, location: typing.Optional[str] = "shuttle"):
        self.rabbitmq_url = rabbitmq_url
        self.camera_ip = camera_ip

        self.connection: typing.Optional[aio_pika.RobustConnection] = None
        self.channel: typing.Optional[aio_pika.RobustChannel] = None
        self.exchange: typing.Optional[aio_pika.Exchange] = None
        self.queue: typing.Optional[aio_pika.Queue] = None

        self.location = location if location is not None else ""

        self.exchange_name = f"{self.location}.camera.ctrl"
        self.response_queue_name = f"{self.location}.camera.response"
        self.broadcast_routing_key = f"{self.location}.broadcast"

    @property
    def p2p_routing_key(self) -> str:
        return f"{self.location}.camera.{self.camera_ip}"

    async def connect(self):
        if not self.connection:
            self.connection = await aio_pika.connect_robust(self.rabbitmq_url)
            self.channel = await self.connection.channel()
            self.exchange = await self.channel.declare_exchange(
                name=self.exchange_name,
                type=aio_pika.ExchangeType.DIRECT,
                auto_delete=True,
            )
            self.queue = await self.channel.declare_queue(
                name=self.p2p_routing_key,
                exclusive=True,
                auto_delete=True
            )
            # 绑定 routing_key
            await self.queue.bind(self.exchange, routing_key=self.p2p_routing_key)
            await self.queue.bind(self.exchange, routing_key=self.broadcast_routing_key)

            _logger.info(f"{self.identity} connected rabbit mq successfully")

    async def close(self):
        if self.connection:
            await self.connection.close()
            self.connection = None
            _logger.info(f"{self.identity} disconnected rabbit mq successfully")

    async def listener(self):
        async with self.queue.iterator() as it:
            async for message in it:
                async with message.process():
                    try:
                        # data -> ((open), (trigger, has_part_t), (close))
                        data = json.loads(message.body)
                        _logger.debug(f"{self.identity} receive from rabbitmq: {data}")

                        # 接受响应
                        response_msg = yield data

                        # 发送响应
                        if message.reply_to:
                            msg = aio_pika.Message(
                                body=json.dumps(response_msg).encode(),
                            )
                            await self.channel.default_exchange.publish(
                                message=msg,
                                routing_key=message.reply_to
                            )

                    except Exception as err:
                        _logger.exception(f"{self.identity} listen from rabbitmq error: {err}")

    @property
    def identity(self):
        return f"RabbitMQ[CameraCtrlConsumer]"

if __name__ == "__main__":
    import asyncio
    import argparse

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

    async def main(rabbit_url, ip):
        c = RabbitmqCameraConsumer(rabbit_url, ip)
        try:
            await c.connect()
            async for message in c.listener():
                async with message.process():
                    try:
                        data = json.loads(message.body)
                        print("📩 收到响应:", data)
                        response = list()
                        for d in data:
                            response.append([*d, "ok"])
                        response_msg = {"ip": ip, "res": response}
                        await c.response(response_msg, message)
                    except Exception as err:
                        print("执行失败:", err)

        except KeyboardInterrupt:
            print("\n KeyboardInterrupt")
        finally:
            await c.close()


    url = "amqp://admin:123@localhost/"
    init_logger()

    parser = argparse.ArgumentParser(description="consumer")
    parser.add_argument("--ip", type=str, required=True, help="IP to listen to")
    args = parser.parse_args()

    asyncio.run(main(url, args.ip))