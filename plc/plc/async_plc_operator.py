import logging
import asyncio
import typing
from concurrent.futures import ThreadPoolExecutor
from functools import partial

from .plc_operator import PLCOperator

_logger = logging.getLogger(__name__)


MAX_WORKERS = 10

class AsyncPLCOperator(PLCOperator):
    def __init__(self, ip, executor, **kwargs):
        # 执行器
        self.executor = executor or ThreadPoolExecutor(max_workers=MAX_WORKERS)
        # 标识是否是我们自己创建的 executor
        self._own_executor = executor is None

        super().__init__(ip=ip, **kwargs)

        self.loop = asyncio.get_running_loop()

    async def connect(self):
        func = partial(super().connect)
        await self.loop.run_in_executor(self.executor, func)

    async def disconnect(self):
        func = partial(super().disconnect)
        await self.loop.run_in_executor(self.executor, func)

    def cleanup(self):
        if self._own_executor:
            self.executor.shutdown()

    async def is_connected(self) -> bool:
        func = partial(super().is_connected)
        return await self.loop.run_in_executor(self.executor, func)

    async def __aenter__(self) -> typing.Self:
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.disconnect()
        self.cleanup()
        # 返回 False 以便异常继续抛出
        return False

    async def read_multi_vars(self, var_dict: dict) -> dict:
        """
        批量读取 PLC 中的多个变量
        参数:
        - var_dict: 字典，每个元素是 dict，例如：
            {
            name1:
                {
                    'area': 'DB',            # DB, MK, PE, PA
                    'db_number': 1,          # DB块编号，如果是DB才需要
                    'datatype': 'REAL',      # 支持 BOOL, BYTE, WORD, DWORD, INT, REAL
                    'start': 0,              # 起始地址（字节）
                    'bit': None,             # 如果是布尔类型，指定位偏移（0-7）
                    'amount': 4,             # 表示长度为4的数组
                }
            }
        返回:
        - 结果列表，变量值（按顺序）
        """
        '''
            区地址类型
               PE(I)   -> 0x81
               PA(Q)   -> 0x82
               MK(M)   -> 0x83
               DB      -> 0x84
               CT      -> 0x1C
               TM      -> 0x1D
        '''
        func = partial(super().read_multi_vars, var_dict)
        return await self.loop.run_in_executor(self.executor, func)

    async def read_multi(self) -> dict:
        return await self.read_multi_vars(self.multi_vars)
