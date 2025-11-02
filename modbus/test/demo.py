import logging
import asyncio
from modbus import CameraCtrlModbusClient

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
    # logging.getLogger('snap7.client').setLevel(logging.INFO)


async def main():
    async with CameraCtrlModbusClient(host="127.0.0.1", port=502, slave=0x01, modbus_address_path=r"../../config/modbus_address.yml") as c:
        await c.write(registers={0:1})
        await c.read(addr=0, count=10)


if __name__ == "__main__":
    init_logger()
    asyncio.run(main())
