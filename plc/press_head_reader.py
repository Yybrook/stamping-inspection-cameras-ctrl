import snap7
from .plc.async_plc_operator import AsyncPLCOperator, _logger
from utils import async_run_in_executor


PLC_IP = "10.108.9.1"
PLC_MODEL = "S7-300"


class PressHeadReader(AsyncPLCOperator):
    def __init__(self, executor):

        ip = PLC_IP
        model = PLC_MODEL

        super().__init__(ip=ip, executor=executor, model=model)

        self.multi_vars = {
            "program_id": {
                'area': 'DB',
                'db_number': 61,
                'start': 2,
                'amount': 1,
                'datatype': 'WORD',
            },
        }

    @async_run_in_executor
    def read_program_id(self) -> int:
        data = self.client.read_area(snap7.type.Area.DB, 61, 2, 2)
        program_id = snap7.util.get_word(data, 0)
        _logger.debug(f"{self.identity} read_program_id()={program_id}")
        return int(program_id)

    @property
    def identity(self):
        return f"PLC[{self.model_name}|PressHead]"
