import snap7
from .plc.plc_operator import PLCOperator, _logger


PLC_IP = "10.108.1.1"
PLC_MODEL = "S7-300"

class Press1stReader(PLCOperator):
    def __init__(self):

        ip = PLC_IP
        model = PLC_MODEL

        super().__init__(ip=ip, model=model)

        self.multi_vars = {
            "running": {
                'area': 'PA',
                'db_number': 0,
                'start': 255,
                'bit': 7,
                'amount': 1,
                'datatype': 'BOOL',
            },
        }

    def read_running_light(self) -> bool:
        data = self.client.read_area(snap7.type.Area.PA, 0, 255, 1)
        light = snap7.util.get_bool(data, 0, 7)
        _logger.debug(f"{self.identity} read_running_light()={light}")
        return light

    @property
    def identity(self):
        return f"PLC[{self.model_name}|Press1st]"
