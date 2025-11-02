import typing
import snap7
import ctypes
import logging

from .s7_result_map import parse_s7_result
from .s7_operation_map import PLC_AREA_MAP, PLC_WORDLEN_MAP, PLC_DATATYPE_MAP, PLC_PARSER_MAP, get_model_name, PLCModel

_logger = logging.getLogger(__name__)


class PLCOperator:
    def __init__(self, ip, **kwargs):
        """

        :param ip:
        :param rack:
        :param slot:
        """
        self.ip = ip
        if "model" in kwargs.keys():
            self.model = PLCModel(kwargs["model"])
            self.model_name = self.model.value
            self.rack, self.slot = self.model.get_location()
        elif "rack" in kwargs.keys() and "slot" in kwargs.keys():
            self.rack = kwargs["rack"]
            self.slot = kwargs["slot"]
            self.model_name = get_model_name(self.rack, self.slot)
        else:
            raise ValueError(f"illegal kwargs[{kwargs}]")

        # 客户端
        self.client = snap7.client.Client()

        # 需要读的地址列表(字典)
        '''
        {
            name1:
                {
                    'area': 'DB',            # DB, MK, PE, PA
                    'db_number': 1,          # DB块编号，如果是DB才需要
                    'datatype': 'REAL',      # 支持 BOOL, BYTE, WORD, DWORD, INT, REAL
                    'start': 0,              # 起始地址（字节）
                    'bit': None,             # 如果是布尔类型，指定位偏移（0-7）
                    'amount': 4,             # 表示长度为4的数组
                },
        }
        '''
        self.multi_vars = dict()

    def connect(self):
        self.client.connect(address=self.ip, rack=self.rack, slot=self.slot)
        _logger.debug(f"{self.identity} connect to {self.ip} successfully")

    def disconnect(self):
        # 断开客户端连接
        self.client.disconnect()
        # 销毁客户端
        self.client.destroy()
        _logger.debug(f"{self.identity} disconnect from {self.ip} successfully")

    def __enter__(self) -> typing.Self:
        self.connect()
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        self.disconnect()
        # 返回 False 以便异常继续抛出
        return False

    def is_connected(self) -> bool:
        return self.client.get_connected()

    def read_multi_vars(self, var_dict: typing.Optional[dict]=None) -> dict:
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

        if not var_dict:
            var_dict = self.multi_vars

        items = list()
        buffers = list()

        # --- 构造 S7DataItem 列表 ---
        for cfg in var_dict.values():

            datatype = cfg['datatype']
            wordlen = PLC_DATATYPE_MAP[datatype]["type"]
            size = PLC_DATATYPE_MAP[datatype]["size"]
            type_amount = PLC_DATATYPE_MAP[datatype]['amount']      # 一个变量的单位数量（比如 LREAL 占两个 REAL）
            value_amount = cfg.get('amount', 1)                     # 用户希望读取的变量数量（比如 LREAL[4]）
            amount = type_amount * value_amount

            item = snap7.type.S7DataItem()
            item.Area = PLC_AREA_MAP[cfg['area']]
            item.WordLen = wordlen
            item.DBNumber = cfg.get('db_number', 0)
            item.Start = cfg['start']
            item.Amount = amount

            # create the buffer
            buffer = ctypes.create_string_buffer(amount * size)
            buffers.append(buffer)
            # cast the pointer to the buffer to the required type
            p_buffer = ctypes.cast(buffer, ctypes.POINTER(ctypes.c_uint8))
            item.pData = p_buffer

            items.append(item)

        # --- 转为 ctypes 数组并调用 snap7 ---
        array_items = snap7.type.S7DataItem * len(items)
        c_items = array_items(*items)
        # 调用 read_multi_vars
        result, c_items = self.client.read_multi_vars(c_items)

        values = dict()
        for (name, cfg), item in zip(var_dict.items(), c_items):
            _logger.debug(f"{self.identity} item[{name}]={item}")
            if item.Result != 0:
                _logger.error(f"{self.identity} item[{name}] error: {parse_s7_result(item.Result)}")
                values[name] = None
            else:
                datatype = cfg['datatype']
                # wordlen = PLC_DATATYPE_MAP[datatype]["type"]
                size = PLC_DATATYPE_MAP[datatype]['size']
                type_amount = PLC_DATATYPE_MAP[datatype]['amount']
                value_amount = cfg.get('amount', 1)
                buf = bytearray(ctypes.string_at(item.pData, size * type_amount * value_amount))
                parser = PLC_PARSER_MAP[datatype]
                if value_amount == 1:
                    byte_index = 0
                    bit_index = cfg.get('bit', 0) if datatype == 'BOOL' else None
                    value = parser(buf, byte_index, bit_index)
                    values[name] = value
                else:
                    array = list()
                    for i in range(value_amount):
                        if datatype == "BOOL":
                            bit = cfg.get('bit', 0) + i
                            bit_index = bit % 8
                            byte_index = bit // 8
                            val = parser(buf, byte_index, bit_index)
                        else:
                            byte_index = i * size * type_amount
                            val = parser(buf, byte_index, None)
                        array.append(val)
                    values[name] = array

        _logger.debug(f"{self.identity} read_multi_vars()={values}")

        return values

    def set_multi_vars(self, multi_vars: dict):
        self.multi_vars = multi_vars

    @property
    def identity(self):
        return f"PLC[{self.model_name}]"

    def __repr__(self):
        return f"{self.model_name} in rack[{self.rack}] slot[{self.slot}]"
