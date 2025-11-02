import typing
from .modbus_address import ModbusAddress
from .modbus.async_modbus_tcp_client import MyAsyncModbusTCPClient


class CameraCtrlModbusClient(MyAsyncModbusTCPClient):
    address_config = None

    def __init__(self, host, port, slave=0x01, **kwargs):

        # 读配置文件
        modbus_address_path = kwargs.pop("modbus_address_path", None)

        super().__init__(host=host, port=port, slave=slave, **kwargs)


        self.address = ModbusAddress.load_address(path=modbus_address_path)

        # 保持寄存器 字典
        self.registers = {name: 0 for name in self.address}

    async def write(self, registers: dict[typing.Union[int, str], int]):
        """
        批量写入保持寄存器
        :param registers:
        :return:
        """
        _registers = dict()
        for reg, value in registers.items():
            if isinstance(reg, int):
                # 地址
                addr = reg
            else:
                # 名称
                addr = ModbusAddress[reg]

            _registers[addr] = value

        await super().write(_registers)

        # 更新
        self.registers.update(registers)

    async def read(self, addr: typing.Union[int, str], count: int) -> dict:
        # 获取 地址
        if isinstance(addr, str):
            addr = ModbusAddress[addr]

        # 读取
        registers = await super().read(addr, count)

        # 将 地址 映射为 名称
        _registers = dict()
        for _addr in range(addr, addr + count):
            name = self.address[_addr]
            _registers[name] = registers[_addr]

        # 更新
        self.registers.update(_registers)

        return _registers

    async def read_all(self):
        return await self.read(addr=0, count=len(self.address))

    @property
    def identity(self):
        return f"Modbus[TCPClient|CameraCtrl]"
