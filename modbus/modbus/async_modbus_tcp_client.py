import typing
from pymodbus.client import AsyncModbusTcpClient
import functools
import logging


_logger = logging.getLogger(__name__)


class MyAsyncModbusTCPClient(AsyncModbusTcpClient):

    def __init__(self, host, port, slave=0x01, **kwargs):

        super().__init__(host=host, port=port, **kwargs)

        self.slave = slave

    @property
    def server_address(self) -> tuple[str, int]:
        """
        远程服务器地址
        :return: (host, port)
        """
        return self.comm_params.host, self.comm_params.multicast_port

    async def connect(self) -> bool:
        """
        建立 modbus 连接
        :return:
        """
        res = await super().connect()
        if not res:
            raise OSError(f"connect to {self.server_address} error")
        # _logger.info(f"{self.identity} connect to host[{self.comm_params.host}], port[{self.comm_params.port}], slave[{self.slave}] successfully")
        return res

    def close(self):
        super().close()
        # _logger.info(f"{self.identity} disconnect from host[{self.comm_params.host}], port[{self.comm_params.port}], slave[{self.slave}] successfully")

    async def __aenter__(self):
        await super().__aenter__()
        return self

    async def __aexit__(self, klass, value, traceback):
        await super().__aexit__(klass, value, traceback)
        # 返回 False 以便异常继续抛出
        return False

    async def write(self, registers: dict[int, int]):
        """
        批量写入保持寄存器
        :param registers:
        :return:
        """
        # 错误信息字典
        err_addr = dict()

        for reg_addr, value in registers.items():
            try:
                response = await self.write_register(reg_addr, value, device_id=self.slave)
                if response.isError():
                    err_addr[reg_addr] = response
            except Exception as err:
                err_addr[reg_addr] = err

        if err_addr:
            error_details = "\n".join(f"  Addr {addr}: {err}" for addr, err in err_addr.items())
            raise Exception(f"write holding registers for slave[{self.slave}] error: \n{error_details}")

        _logger.info(f"{self.identity} write {registers} successfully")

    async def read(self, addr: int, count: int) -> dict:
        """
        批量读取保持寄存器
        :param addr:
        :param count:
        :return:
        """
        response = await self.read_holding_registers(addr, count=count, device_id=self.slave)
        if response.isError():
            raise Exception(f"read holding registers[{addr} - {addr + count - 1}] for slave={self.slave} error: \n{response}")

        res = {addr + i: val for i, val in enumerate(response.registers)}
        _logger.debug(f"{self.identity} read({addr}, {count}) = {res}")
        return res

    @property
    def identity(self):
        return f"[ModbusTCPClient]"
