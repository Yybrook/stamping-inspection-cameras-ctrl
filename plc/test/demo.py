import logging
import time
from plc import PLCOperator, Press1stReader, PressHeadReader, PressTailReader


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
    try:
        with PLCOperator(ip="10.108.7.1", model="S7-300") as plc:
            print(f"is connected={plc.is_connected()}")

        with Press1stReader() as plc:
            pre_s = False
            start_t = pre_t = time.time()
            while True:
                s = plc.read_running_light()
                cur_t = time.time()
                if pre_s != s:
                    dur_t = (cur_t - pre_t) * 1000
                    pre_t = cur_t
                    pre_s = s
                    print(f"running light={s}, duration={dur_t}")
                if cur_t - start_t >= 10:
                    break

        with PressHeadReader(executor=None) as plc:
            print(f"multi vars=\n{await plc.read_multi_vars()}")

        with PressTailReader(executor=None) as reader:
            start_t = pre_t = time.time()
            while True:
                s1, s2 = await reader.read_shuttle_sensors()
                cur_t = time.time()
                dur_t = (cur_t - pre_t) * 1000
                pre_t = cur_t
                print(f"s1={s1}, s2={s2}, duration={dur_t}")
                if cur_t - start_t >= 10:
                    break

    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    import asyncio

    init_logger()
    asyncio.run(main())
