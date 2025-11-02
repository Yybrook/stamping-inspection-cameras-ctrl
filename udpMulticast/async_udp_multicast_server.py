import asyncio
import socket
import typing
import logging
import utils

_logger = logging.getLogger(__name__)


class MyProtocol(asyncio.DatagramProtocol):
    def datagram_received(self, data, addr):
        data = data.decode('utf-8')
        _logger.debug(f"{self.identity} receive from {addr}: {data}")

    def error_received(self, exc):
        _logger.exception(f"{self.identity} error received: {exc}")

    def connection_made(self, transport):
        _logger.info(f"{self.identity} connected successfully")

    def connection_lost(self, exc):
        if exc:
            _logger.exception(f"{self.identity} connection lost: {exc}")
        else:
            _logger.info(f"{self.identity} closed successfully")

    @property
    def identity(self):
        return "Udp[MulticastServer]"


class AsyncUdpMulticastServer:

    # windows中，强制使用 SelectorEventLoop
    if utils.is_win():
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    def __init__(
            self,
            multicast_ip, multicast_port,
            interface_ip: typing.Optional[str] = None,
            ttl: int = 1,       # 路由级数
            timeout: typing.Optional[int] = None
    ):
        self.multicast_ip = multicast_ip
        self.multicast_port = multicast_port
        self.interface_ip = interface_ip or socket.gethostbyname(socket.gethostname())

        self.timeout = timeout

        self.ttl = ttl
        self.transport = None
        self.protocol = None

        self.transport: typing.Optional[asyncio.transports.DatagramTransport] = None
        self.protocol: typing.Optional[asyncio.DatagramProtocol] = None
        self.loop = asyncio.get_running_loop()

    async def create(self):
        if not self.transport:
            # 创建套接字
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
            # 允许地址重用
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            # 设置超时时间
            if self.timeout:
                sock.settimeout(self.timeout)
            # 设置TTL
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, self.ttl)
            # 设置组播出口接口
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, socket.inet_aton(self.interface_ip))

            # 创建 datagram endpoint
            self.transport, self.protocol = await self.loop.create_datagram_endpoint(
                protocol_factory=lambda: MyProtocol(),
                sock=sock
            )

    async def close(self):
        if self.transport:
            self.transport.close()
            self.transport = None
            self.protocol = None


    async def __aenter__(self):
        await self.create()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
        # 返回 False 以便异常继续抛出
        return False

    async def send(self, data: str):
        if not self.transport:
            raise RuntimeError("transport not initialized. call create() first")
        self.transport.sendto(data.encode('utf-8'), (self.multicast_ip, self.multicast_port))

    @property
    def identity(self):
        return "Udp[MulticastServer]"


if __name__ == "__main__":
    import logging

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
        try:
            async with AsyncUdpMulticastServer(
                multicast_ip="224.0.0.1", multicast_port=1000,
                interface_ip=None,
                ttl=1,
                timeout=1,
            ) as server:
                i = 0
                while True:
                    await server.send(f'Hello, world! ({i})')
                    await asyncio.sleep(2)
                    i += 1
        except (KeyboardInterrupt, asyncio.CancelledError):
            print("中断")
        except Exception as err:
            print(f"错误: {err}")
        finally:
            print("退出")

    init_logger()
    asyncio.run(main())
