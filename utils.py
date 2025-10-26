import os
import inspect
import typing
import asyncio
from concurrent.futures import ThreadPoolExecutor
import functools
import platform
import numpy as np
import cv2
# import subprocess
# import shutil
# import socket


def is_win() -> bool:
    """
    如果是windows系统，返回True，其他返回false
    :return:
    """
    system = platform.system().lower()
    if "windows" in system:
        return True
    else:
        return False


class CallContext(typing.NamedTuple):
    filename: str
    cls_name: typing.Optional[str]
    func_name: str
    lineno: int

    def __str__(self):
        """格式化显示：ClassName.func_name (file.py:line)"""
        if self.cls_name:
            return f"{self.cls_name}.{self.func_name} ({self.filename}:{self.lineno})"
        return f"{self.func_name} ({self.filename}:{self.lineno})"

    @classmethod
    def get_call_context(cls, depth: int = 1) -> typing.Self:
        """
        获取当前或上几层调用的上下文信息，包括：
          - 文件名
          - 类名（如果存在）
          - 函数名
          - 行号

        :param depth: 0 表示当前函数，1 表示上一级调用者，依此类推。
        :return: CallContext 对象
        """
        try:
            frame = inspect.currentframe()
            for _ in range(depth):
                if frame is None or frame.f_back is None:
                    break
                frame = frame.f_back

            if frame is None:
                return cls("<unknown>", None, "<unknown>", -1)

            code = frame.f_code
            func_name = code.co_name or "<unknown>"
            filename = os.path.basename(code.co_filename)
            lineno = frame.f_lineno

            locals_dict = frame.f_locals
            cls_name = None
            # 尝试推断类名
            if "self" in locals_dict:
                cls_name = locals_dict["self"].__class__.__name__
            elif "cls" in locals_dict:
                cls_name = locals_dict["cls"].__name__

            if func_name == "<module>":
                func_name = "module"

            return cls(filename, cls_name, func_name, lineno)

        finally:
            del frame


class ForeverAsyncWorker:
    def __init__(
            self,
            executor,
            max_workers: int = 10,
            task_error_callback: typing.Optional[typing.Callable[[str, BaseException], None]] = None,
            task_success_callback: typing.Optional[typing.Callable[[str, typing.Any], None]] = None,
    ):
        """
        异步任务永久运行器（后台事件循环线程）
        :param executor: 外部传入的线程池执行器（可选）。若为 None，则内部自动创建。
        :param max_workers:
        :param task_error_callback: 异步任务出错时的回调函数，参数为异常对象。
        :param task_success_callback: 异步任务成功完成时的回调函数，参数为返回结果。
        """
        # 执行器
        self.executor = executor or ThreadPoolExecutor(max_workers=max_workers)
        # 标识是否是我们自己创建的 executor
        self._own_executor = executor is None

        # 为异步任务创建一个独立事件循环线程
        self.loop = asyncio.new_event_loop()
        # 在执行器中运行事件循环
        self.loop_thread = self.executor.submit(self._run_loop)
        # 回调函数
        self.task_error_callback = task_error_callback
        self.task_success_callback = task_success_callback

    def _run_loop(self):
        """在独立线程中运行事件循环"""
        # 将当前线程的事件循环设置为创建的事件循环
        asyncio.set_event_loop(self.loop)
        try:
            self.loop.run_forever()
        finally:
            self.loop.run_until_complete(self._shutdown_async())
            self.loop.close()

    async def _shutdown_async(self):
        """关闭异步生成器和默认执行器"""
        await self.loop.shutdown_asyncgens()
        # await self.loop.shutdown_default_executor()

    def run_async(self, coro):
        """在线程安全地调度异步任务"""
        future = asyncio.run_coroutine_threadsafe(coro, self.loop)

        coro_name = getattr(coro, "__name__", str(coro))

        def _done(f):
            try:
                result = f.result()
            except Exception as exc:
                if self.task_error_callback:
                    self.task_error_callback(coro_name, exc)
            else:
                if self.task_success_callback:
                    self.task_success_callback(coro_name, result)

        future.add_done_callback(_done)
        return future

    def stop_loop(self, timeout: float = 5.0):
        """安全停止事件循环"""
        if not self.loop.is_running():
            return
        # 请求停止
        self.loop.call_soon_threadsafe(self.loop.stop)
        # 等待线程结束
        try:
            self.loop_thread.result(timeout=timeout)
        except TimeoutError:
            for task in asyncio.all_tasks(self.loop):
                task.cancel()
            self.loop.call_soon_threadsafe(self.loop.stop)

    def __enter__(self):
        """进入上下文时自动启动事件循环"""
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        """退出上下文时安全关闭事件循环和执行器"""
        self.stop_loop()
        if self._own_executor:
            self.executor.shutdown()
        # 不抑制异常
        return False


def async_run_in_executor(func):
    """
    装饰器：将同步函数放入 self.executor 中执行，返回一个异步函数。
    要求被装饰的函数是类方法（第一个参数为 self）
    """

    # 定义一个内部函数，它接受任意数量和类型的位置参数和关键字参数
    @functools.wraps(func)
    async def wrapper(self, *args, **kwargs):
        # 方案1 使用 loop.run_in_executor 在 executor 中运行
        # 获取事件循环
        loop = getattr(self, "loop", asyncio.get_running_loop())
        # 优先使用类自己的 executor，否则用默认线程池
        executor = getattr(self, "executor", None)
        result = await loop.run_in_executor(executor, functools.partial(func, self, *args, **kwargs))

        # # 方案2 使用 asyncio.to_thread 在线程池中运行
        # result = await asyncio.to_thread(func, *args, **kwargs)

        # 返回被装饰函数的返回值
        return result

    # 返回内部函数，以便它可以被调用
    return wrapper


def save_image_by_cv(path: str, image: np.ndarray, **kwargs) -> int:
    """
    保存图片，使用opencv，支持 .jpg, .png, .bmp
    :param path:
    :param image:       图像 numpy数组
    :param kwargs:      jpg_quality，JPEG图片质量[0,100]，默认100
                        png_compression，PNG压缩等级[0,9]，默认0（0:无压缩，9:最大压缩, 数值越大，文件越小但压缩越慢）
    :return:
    """
    # 文件格式
    _, file_format = os.path.splitext(path)
    file_format = file_format[1:].lower()
    if file_format not in ["jpg", "jpeg", "png", "bmp"]:
        raise TypeError(f"saved format[{file_format}] is not supported")

    # 文件夹
    saved_dir = os.path.dirname(path)
    os.makedirs(saved_dir, exist_ok=True)

    if file_format in ["jpg", "jpeg"]:
        # 图片质量
        quality = kwargs.get("jpg_quality", 100)
        quality = min(max(0, quality), 100)
        params = [cv2.IMWRITE_JPEG_QUALITY, quality]
    elif file_format == "png":
        compression = kwargs.get("png_compression", 0)
        compression = min(max(0, compression), 9)
        params = [cv2.IMWRITE_PNG_COMPRESSION, compression]
    else:
        params = list()

    # 保存
    success = cv2.imwrite(path, image, params)
    if success:
        return True
    else:
        raise cv2.error(f"save image[{path}] by cv failed")
