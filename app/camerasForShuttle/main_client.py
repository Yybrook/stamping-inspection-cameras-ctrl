import os
import time
import asyncio
import datetime
import multiprocessing as mp
import signal
import logging
from logging.handlers import TimedRotatingFileHandler

from camera import MyCameraForShuttle
from config import config
import utils

_logger = logging.getLogger(__name__)

WAIT_PROCESS_END_TIMEOUT_S = 5

log_file = config.LOG_FILE_SHUTTLE_CAMERAS

press_line = config.PRESS_LINE

redis_con = {
    "redis_host": config.REDIS_HOST,
    "redis_port": config.REDIS_PORT,
    "redis_db": config.REDIS_DB,
}

rabbitmq_url = config.RABBITMQ_URL

camera_params_path = config.CAMERA_PARAMS_PATH
parts_info_path = config.PARTS_INFO_PATH

# 子进程列表
processes: dict[str, mp.Process] = dict()


async def run_camera(ip: str):
    """每个相机进程的主入口（异步执行）"""
    # 创建一个事件，用于等待退出信号
    stop_event = asyncio.Event()

    if not utils.is_win():
        # 获取事件循环
        loop = asyncio.get_running_loop()
        # 注册信号处理器：Ctrl+C 或 kill 时触发 stop_event
        loop.add_signal_handler(signal.SIGINT, stop_event.set)
        loop.add_signal_handler(signal.SIGTERM, stop_event.set)

    try:
        camera = await MyCameraForShuttle.create(
            stop_event=stop_event,
            ip=ip,
            press_line=press_line,
            rabbitmq_url=rabbitmq_url,
            camera_params_path=config.CAMERA_PARAMS_PATH,
            **redis_con,
        )
        # 监听 rabbitmq 消息
        await camera.rabbitmq_worker()

    except (KeyboardInterrupt, asyncio.CancelledError):
        _logger.warning(f"[Main] camera[{ip}] process cancelled")
    except Exception as err:
        _logger.exception(f"[Main] camera[{ip}] process error: {err}")
    finally:
        _logger.info(f"[Main] camera[{ip}] process ended")


def run_camera_in_process(ip: str):
    """子进程入口"""
    init_logger()
    asyncio.run(run_camera(ip))

def main():
    """主控程序：为每个相机启动独立进程"""
    try:
        # 相机 IP 列表
        camera_ips = MyCameraForShuttle.load_registered_cameras(path=parts_info_path)

        for ip in camera_ips:
            p = mp.Process(
                target=run_camera_in_process,
                kwargs={"ip": ip},
                daemon=True,      # 去掉守护进程，使子进程执行finally
            )
            p.start()
            processes[ip] = p
            _logger.info(f"[Main] camera[{ip}] run in process[{p.pid}] successfully")

        # 等待所有子进程
        for p in processes.values():
            p.join()

    except KeyboardInterrupt:
        _logger.warning(f"[Main] cameras client for shuttle cancelled")
    except Exception as err:
        _logger.exception(f"[Main] cameras client for shuttle error: {err}")
    finally:
        # 等待 所有相机子进程结束
        start_t = time.time()
        while True:
            time.sleep(0.5)
            alive = {ip: p for ip, p in processes.items() if p.is_alive()}
            if not alive:
                break

            if time.time() - start_t >= WAIT_PROCESS_END_TIMEOUT_S:
                _logger.warning(f"[Main] force camera process[{",".join([f"{ip}|{p.pid}" for ip, p in alive.items()])}] terminate")
                # 强制 子进程关闭
                for ip, p in alive.items():
                    if p.is_alive():
                        p.terminate()
                break

        _logger.info(f"[Main] cameras client for shuttle ended")


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

    # === 文件 Handler（每天切割，保留 7 天，记录 INFO 及以上）===
    grandparent_dir = os.path.dirname(os.path.dirname(__file__))
    log_path = os.path.join(grandparent_dir, log_file)
    # 创建log目录
    log_dir = os.path.dirname(log_path)
    os.makedirs(log_dir, exist_ok=True)

    file_handler = TimedRotatingFileHandler(
        filename=log_path,  # 文件名（会自动生成备份，如 app.log.2025-07-10）
        when="midnight",  # 每天午夜切割一次
        interval=1,  # 间隔单位（这里是 1 天）
        backupCount=7,  # 最多保留 7 个备份文件
        encoding="utf-8",
        utc=False  # 根据本地时间切割；如需使用 UTC，设为 True
    )
    file_handler.setLevel(logging.INFO)
    file_formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    file_handler.setFormatter(file_formatter)

    # 添加 handler 到 logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    # 设置特定日志等级
    # logging.getLogger('apscheduler.executors.default').setLevel(logging.WARNING)
    # logging.getLogger('apscheduler.scheduler').setLevel(logging.WARNING)
    # logging.getLogger('snap7.client').setLevel(logging.WARNING)


if __name__ == "__main__":
    # mp.set_start_method("spawn")  # Windows 需要 spawn

    # 初始化 logger
    init_logger()
    # 运行
    main()
