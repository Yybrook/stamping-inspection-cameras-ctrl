import asyncio
import aio_pika
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
        self.broadcast_routing_key = f"{self.location}.camera.broadcast"

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
            _logger.info(f"{self.identity} connected successfully")

    async def close(self):
        if self.connection:
            await self.connection.close()
            self.connection = None
            _logger.info(f"{self.identity} disconnected successfully")

    async def publish(self, camera_ips: typing.Optional[list[str]], data: str):
        # 连接
        await self.connect()
        # 生成消息
        msg = aio_pika.Message(
            body=data.encode(),
            reply_to=self.response_queue.name
        )
        # 广播消息
        if not camera_ips:
            routing_key = self.broadcast_routing_key
            await self.exchange.publish(msg, routing_key=routing_key)
            _logger.debug(f"{self.identity} publish to {routing_key}: {data}")
        # p2p消息
        else:
            for ip in camera_ips:
                routing_key = self.p2p_routing_key(ip)
                await self.exchange.publish(msg, routing_key=routing_key)
            _logger.debug(f"{self.identity} publish to {camera_ips}: {data}")

    async def listener(self):
        # 连接
        await self.connect()

        try:
            async with self.response_queue.iterator() as q:
                async for message in q:
                    async with message.process():
                        data = message.body.decode()
                        _logger.debug(f"{self.identity} received: {data}")
                        yield data
        except asyncio.CancelledError:
            raise
        finally:
            _logger.debug(f"{self.identity} listener ended")

    @property
    def identity(self):
        return f"Rabbitmq[CameraCtrlProducer]"

if __name__ == "__main__":
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

    async def get_input():
        input_str = await asyncio.to_thread(
            input,
            "请输入命令 (格式: 192.168.1.1,192.168.1.2/all open set,fps,10) 或 exit/quit: "
        )
        input_str = input_str.strip()
        if input_str.lower() in ("exit", "quit"):
            return None
        input_list = input_str.split()

        ips = input_list[0]
        if ips.lower() == "all":
            ips = None
        else:
            ips = ips.split(",")

        cmds = [cmd.split(",") for cmd in input_list[1:]]
        return ips, cmds

    async def handle_response(mq):
        async for data in mq.listener():
            try:
                data = json.loads(data)
                # print(f"收到响应: {data}")
            except Exception as err:
                print("解析响应失败:", err)

    async def output_cmds(mq):
        while True:
            try:
                input_cmds = await get_input()
                if input_cmds is None:
                    break
                camera_ips, cmds = input_cmds
                data = json.dumps(cmds)
                await mq.publish(camera_ips=camera_ips, data=data)
            except Exception as err:
                print(f"发送消息错误: {err}")
        print("结束输出监听")
        raise

    async def main(rabbit_url):
        mq = RabbitmqCameraProducer(rabbit_url)
        try:
            await mq.connect()
            tasks = [
                asyncio.create_task(handle_response(mq)),
                asyncio.create_task(output_cmds(mq)),
            ]
            results = await asyncio.gather(*tasks, return_exceptions=False)
            print(results)
        except (KeyboardInterrupt, asyncio.CancelledError):
            print("中断")
        except Exception as err:
            print(f"错误: {err}")
        finally:
            await mq.close()
            print("退出")



    url = "amqp://admin:123@localhost/"
    init_logger()
    asyncio.run(main(url))

