import socket
import typing
import struct
import logging

_logger = logging.getLogger(__name__)


class SyncUdpMulticastClient:
    def __init__(
            self,
            multicast_ip: str, multicast_port: int,
            interface_ip: typing.Optional[str] = None,
            receive_buf_size: int = 1024,
            timeout: typing.Optional[int] = None
    ):
        # 组播ip
        self.multicast_ip = multicast_ip
        # 服务器地址
        self.multicast_port = multicast_port
        # 本地网卡ip
        self.interface_ip = interface_ip or socket.gethostbyname(socket.gethostname())

        # socket 超时时间
        self.timeout = timeout
        # 接收消息大小
        self.receive_buf_size = receive_buf_size

        self.socket = None

    def create(self):
        if not self.socket:
            # 创建 UDP socket
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
            # 允许地址重用
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            # 设置超时时间
            if self.timeout:
                sock.settimeout(self.timeout)
            # 绑定服务器
            sock.bind(("", self.multicast_port))
            # 设置组播出口
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, socket.inet_aton(self.interface_ip))

            # 加入组播组
            mreq = struct.pack("4s4s", socket.inet_aton(self.multicast_ip), socket.inet_aton(self.interface_ip))
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

            self.socket = sock

    def close(self):
        """
        关闭 socket
        :return:
        """
        if self.socket:
            self.socket.close()
            self.socket = None

    def __enter__(self):
        self.create()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def send(self, data: str, addr: tuple):
        if not self.socket:
            raise RuntimeError("socket not initialized. Call create() first")
        self.socket.sendto(data.encode('utf-8'), addr)

    def receive(self) -> tuple:
        if not self.socket:
            raise RuntimeError("socket not initialized. Call create() first")
        data, addr = self.socket.recvfrom(self.receive_buf_size)
        data = data.decode('utf-8')
        _logger.debug(f"{self.identity} receive from {addr}: {data}")
        return data, addr

    def receiver(self):
        # 创建 客户端
        with self:
            while True:
                try:
                    data, addr = self.receive()
                    yield data, addr
                except TimeoutError:
                    pass

    @property
    def identity(self):
        return "[UdpMulticastClient]"


if __name__ == "__main__":
    import logging

    # 配置日志系统
    logging.basicConfig(
        level=logging.DEBUG,  # 设置全局日志级别
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),
        ]
    )

    with SyncUdpMulticastClient(
        multicast_ip="224.0.0.1", multicast_port=1000,
        interface_ip=None,
        receive_buf_size=1024,
        timeout=1,
    ) as client:
        for _data, _addr in client.receiver():
            # 回复消息
            client.send(data=f"Ack[{_data}]", addr=_addr)

    print("end")
