import asyncio
import aio_pika
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
        # self.response_queue_name = f"{self.location}.camera.response"
        self.broadcast_routing_key = f"{self.location}.camera.broadcast"

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

            _logger.info(f"{self.identity} connected successfully")

    async def close(self):
        if self.connection:
            await self.connection.close()
            self.connection = None
            _logger.info(f"{self.identity} disconnected successfully")

    async def listener(self):
        # 连接 rabbitmq
        await self.connect()

        try:
            # 监听队列
            async with self.queue.iterator() as q:
                async for message in q:
                    async with message.process():
                        try:
                            # data -> ((open), (trigger, has_part_t), (close))
                            data = message.body.decode()
                            _logger.debug(f"{self.identity} received: {data}")

                            # 接受响应
                            response = yield data

                            # 发送响应
                            if response and message.reply_to:
                                _logger.debug(f"{self.identity} reply to: {response}")
                                msg = aio_pika.Message(
                                    body=response.encode(),
                                )
                                await self.channel.default_exchange.publish(
                                    message=msg,
                                    routing_key=message.reply_to
                                )
                        except Exception as err:
                            _logger.exception(f"{self.identity} listen from rabbitmq error: {err}")
        except asyncio.CancelledError:
            raise
        finally:
            _logger.debug(f"{self.identity} listener ended")

    @property
    def identity(self):
        return f"Rabbitmq[CameraCtrlConsumer]"

if __name__ == "__main__":
    import argparse
    import json

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

        logging.getLogger('asyncio').setLevel(logging.INFO)
        logging.getLogger('aiormq.connection').setLevel(logging.INFO)
        logging.getLogger('aio_pika.robust_connection').setLevel(logging.INFO)
        logging.getLogger('aio_pika.connection').setLevel(logging.INFO)
        logging.getLogger('aio_pika.channel').setLevel(logging.INFO)
        logging.getLogger('aio_pika.queue').setLevel(logging.INFO)
        logging.getLogger('aio_pika.exchange').setLevel(logging.INFO)

    async def main(rabbit_url, ip):
        mq = RabbitmqCameraConsumer(rabbit_url, ip)
        try:
            # 获取异步生成器对象
            agen = mq.listener()
            response = None
            while True:
                try:
                    data = await agen.asend(response)
                    # 解析json
                    data = json.loads(data)
                    # print(f"收到消息: {data}")
                    # 响应
                    response = [[*d, "ok"] for d in data]
                    response = {"ip": ip, "res": response}
                    # 转为json
                    response = json.dumps(response)
                    # print(f"生成响应: {response}")
                except StopAsyncIteration as err:
                    print(f"StopIteration: {err}")
                    break
                except Exception as err:
                    print(f"解析消息错误: {err}")

        except (KeyboardInterrupt, asyncio.CancelledError):
            print("中断")
        except Exception as err:
            print(f"错误: {err}")
        finally:
            await mq.close()
            print("退出")


    url = "amqp://admin:123@localhost/"
    init_logger()

    parser = argparse.ArgumentParser(description="consumer")
    parser.add_argument("--ip", type=str, required=True, help="IP to listen to")
    args = parser.parse_args()

    asyncio.run(main(url, args.ip))