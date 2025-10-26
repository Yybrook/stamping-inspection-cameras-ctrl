import asyncio
import datetime
import multiprocessing as mp
import signal
import logging

from camera import MyCameraForShuttle
from config import config
import utils

_logger = logging.getLogger(__name__)

PROCESS_END_TIMEOUT_SEC = 60

log_file = config.LOG_FILE_READER_FOR_PRESS

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
    camera = await MyCameraForShuttle.create(
        ip=ip,
        press_line=press_line,
        rabbitmq_url=rabbitmq_url,
        camera_params_path=config.CAMERA_PARAMS_PATH,
        stop_event=None,
        **redis_con,
    )

    if not utils.is_win():
        # 获取事件循环
        loop = asyncio.get_running_loop()
        # 注册信号处理器：Ctrl+C 或 kill 时触发 stop_event
        loop.add_signal_handler(signal.SIGINT, camera.stop_event.set)
        loop.add_signal_handler(signal.SIGTERM, camera.stop_event.set)

    try:
        _logger.info(f"[Main] camera[{ip}] process started")
        # 监听 rabbitmq 消息
        await camera.listener_work()
    except (KeyboardInterrupt, asyncio.CancelledError):
        _logger.info(f"[Main] camera[{ip}] process cancelled")
    finally:
        await camera.cleanup()
        _logger.info(f"[Main] camera[{ip}] process ended")


def run_camera_in_process(ip: str):
    """子进程入口"""
    try:
        asyncio.run(run_camera(ip))
    except Exception as err:
        _logger.exception(f"[Main] camera[{ip}] process error: {err}")


def main():
    """主控程序：为每个相机启动独立进程"""
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


def terminate_process():
    for ip, p in processes.items():
        if p.is_alive():
            p.terminate()
            _logger.warning(f"[Main] camera[{ip}] terminate in process[{p.pid}]")


if __name__ == "__main__":
    # mp.set_start_method("spawn")  # Windows 需要 spawn
    try:
        main()
    except KeyboardInterrupt:
        pass
    finally:
        now = datetime.datetime.now()
        while datetime.datetime.now() - now < datetime.timedelta(seconds=PROCESS_END_TIMEOUT_SEC):
            alive = [p for p in processes.values() if p.is_alive()]
            if not alive:
                break
        else:
            terminate_process()

        _logger.info("[Main] camera process all ended")

