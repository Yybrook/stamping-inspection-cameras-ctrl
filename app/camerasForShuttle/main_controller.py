import os
import asyncio
import signal
import logging
from logging.handlers import TimedRotatingFileHandler

from camera import CameraCtrlForShuttle
from config import config
import utils

_logger = logging.getLogger(__name__)

log_file = config.LOG_FILE_CONTROLLER_FOR_SHUTTLE_CAMERAS

press_line = config.PRESS_LINE

redis_con = {
    "redis_host": config.REDIS_HOST,
    "redis_port": config.REDIS_PORT,
    "redis_db": config.REDIS_DB,
}

modbus_con = {
    "modbus_host": config.MODBUS_HOST,
    "modbus_port": config.MODBUS_PORT,
    "modbus_slave": config.MODBUS_SLAVE,
}

rabbitmq_url = config.RABBITMQ_URL

parts_info_path=config.PARTS_INFO_PATH

async def main():
    # 创建一个事件，用于等待退出信号
    stop_event = asyncio.Event()

    if not utils.is_win():
        # 获取事件循环
        loop = asyncio.get_running_loop()
        # 注册信号处理器：Ctrl+C 或 kill 时触发 stop_event
        loop.add_signal_handler(signal.SIGINT, stop_event.set)
        loop.add_signal_handler(signal.SIGTERM, stop_event.set)

    async with await CameraCtrlForShuttle.create(
            stop_event=stop_event,
            executor=None,
            press_line=press_line,
            rabbitmq_url=rabbitmq_url,
            parts_info_path=parts_info_path,
            **redis_con,
            **modbus_con,
    ) as camera_ctrl:
        # 等待所有任务运行
        try:
            _logger.info(f"[Main] controller for shuttle cameras started")
            tasks = [*camera_ctrl.tasks, stop_event.wait()]
            # return_exceptions=True -> CancelledError 不会向上抛出
            await asyncio.gather(*tasks, return_exceptions=False)
        finally:
            stop_event.set()
            _logger.info(f"[Main] controller for shuttle cameras ended")


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
    try:
        # 初始化 logger
        init_logger()
        # 协程运行
        asyncio.run(main())
    except (KeyboardInterrupt, asyncio.CancelledError):
        _logger.info(f"[Main] controller for shuttle cameras cancelled")
    except Exception as err:
        # traceback.print_exc()
        # traceback.format_exc()
        _logger.exception(f"[Main] controller for shuttle cameras error: {err}")
