import logging
import time
from plc import PLCOperator, Press1stReader, PressHeadReader, PressTailReader

if __name__ == "__main__":
    # 配置日志系统
    logging.basicConfig(
        level=logging.DEBUG,  # 设置全局日志级别
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),  # 输出到控制台
            # logging.FileHandler("app.log", mode="w", encoding="utf-8")  # 输出到文件
        ]
    )

    with PLCOperator(ip="10.108.7.1", model="S7-300") as plc:
        print("is_connected: ", plc.is_connected())

    with Press1stReader() as plc:
        try:
            pre_s = False
            pre_t = time.time()
            while True:
                # print(plc.read_multi())
                s = plc.read_running_light()
                if pre_s != s:
                    t = time.time()
                    dur = (t - pre_t) * 1000
                    pre_t = t
                    pre_s = s
                    print(s, dur)
        except KeyboardInterrupt:
            pass

    with PressHeadReader() as plc:
        print(plc.read_multi())

    with PressTailReader(None) as reader:
        try:
            pre_t = time.time()
            while True:
                s1, s2 = reader.read_shuttle_sensors()
                t = time.time()
                dur = (t - pre_t) * 1000
                pre_t = t
                print(s1, s2, dur)
        except KeyboardInterrupt:
            pass
