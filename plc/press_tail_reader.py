import snap7
from .plc.async_plc_operator import AsyncPLCOperator, _logger


PLC_IP = "10.108.7.1"
PLC_MODEL = "S7-300"


class PressTailReader(AsyncPLCOperator):
    def __init__(self, executor):

        ip = PLC_IP
        model = PLC_MODEL

        super().__init__(ip=ip, executor=executor, model=model)

        self.multi_vars = {
            # "shuttle_sensors": {
            #     'area': 'PE',
            #     'db_number': 0,
            #     'start': 538,
            #     'bit': 1,
            #     'amount': 2,
            #     'datatype': 'BOOL',
            # },
            "part_counter": {
                'area': 'DB',
                'db_number': 160,
                'start': 54,
                'amount': 1,
                'datatype': 'DWORD',
            },
            # "right_conveyer_1": {
            #     'area': 'DB',
            #     'db_number': 630,
            #     'start': 6,
            #     'bit': 6,
            #     'amount': 1,
            #     'datatype': 'BOOL',
            # },
        }

    def _read_shuttle_sensors(self) -> tuple[bool, bool]:
        data = self.client.read_area(snap7.type.Area.PE, 0, 538, 1)
        s1 = snap7.util.get_bool(data, 0, 1)
        s2 = snap7.util.get_bool(data, 0, 2)
        _logger.debug(f"{self.identity} read_shuttle_sensors() = {(s1, s2)}")
        return s1, s2

    def _read_part_counter(self) -> int:
        data = self.client.read_area(snap7.type.Area.DB, 160, 54, 4)
        count = snap7.util.get_dword(data, 0)
        _logger.debug(f"{self.identity} read_part_counter() = {count}")
        return count

    def _read_right_conveyer_1(self) -> int:
        data = self.client.read_area(snap7.type.Area.DB, 630, 6, 1)
        running = snap7.util.get_bool(data, 0, 6)
        _logger.debug(f"{self.identity} read_right_conveyer_1() = {running}")
        return running

    async def read_shuttle_sensors(self) -> tuple:
        return await self.loop.run_in_executor(self.executor, self._read_shuttle_sensors)

    async def read_part_counter(self) -> int:
        return await self.loop.run_in_executor(self.executor, self._read_part_counter)

    async def read_right_conveyer_1(self) -> int:
        return await self.loop.run_in_executor(self.executor, self._read_right_conveyer_1)

    @property
    def identity(self):
        return f"PLC[{self.model_name}|PressTail]"
