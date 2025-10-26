import os
import asyncio
import signal
from concurrent.futures import ThreadPoolExecutor
import logging
from logging.handlers import TimedRotatingFileHandler

from press import PressInfo
from config import config
import utils

_logger = logging.getLogger(__name__)

log_file = config.LOG_FILE_READER_FOR_PRESS

press_line = config.PRESS_LINE

redis_con = {
    "redis_host": config.REDIS_HOST,
    "redis_port": config.REDIS_PORT,
    "redis_db": config.REDIS_DB,
}


async def main():
    # 创建一个事件，用于等待退出信号
    stop_event = asyncio.Event()

    if not utils.is_win():
        # 获取事件循环
        loop = asyncio.get_running_loop()
        # 注册信号处理器：Ctrl+C 或 kill 时触发 stop_event
        loop.add_signal_handler(signal.SIGINT, stop_event.set)
        loop.add_signal_handler(signal.SIGTERM, stop_event.set)

    # 实例化 press_info
    async with await PressInfo.create(press_line=press_line, executor=None,  **redis_con) as press_info:
        try:
            # 启动
            press_info.work()
            _logger.info(f"[Main] press reader started")

            # 等待事件触发
            await stop_event.wait()
        finally:
            _logger.info(f"[Main] press reader ended")


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
    logging.getLogger('apscheduler.executors.default').setLevel(logging.WARNING)
    logging.getLogger('apscheduler.scheduler').setLevel(logging.WARNING)
    logging.getLogger('snap7.client').setLevel(logging.WARNING)


if __name__ == "__main__":
    try:
        # 初始化 logger
        init_logger()
        # 协程运行
        asyncio.run(main())
    except (KeyboardInterrupt, asyncio.CancelledError):
        _logger.info(f"[Main] press reader cancelled")
    except Exception as err:
        # traceback.print_exc()
        # traceback.format_exc()
        _logger.exception(f"[Main] press reader error: {err}")
