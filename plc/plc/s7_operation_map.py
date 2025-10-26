import snap7
from enum import StrEnum


PLC_AREA_MAP = {
    'PE': snap7.type.Area.PE,     # I
    'PA': snap7.type.Area.PA,     # Q
    'MK': snap7.type.Area.MK,     # M
    'DB': snap7.type.Area.DB,     # DB
    'CT': snap7.type.Area.CT,     # 定时器 不推荐
    'TM': snap7.type.Area.TM,     # 计数器 不推荐
    }

PLC_WORDLEN_MAP = {
    'BOOL': {"type": snap7.type.WordLen.Bit, "size": 1},         # 1字节
    'BYTE': {"type": snap7.type.WordLen.Byte, "size": 1},        # 1字节
    'WORD': {"type": snap7.type.WordLen.Word, "size": 2},        # 2字节
    'DWORD': {"type": snap7.type.WordLen.DWord, "size": 4},      # 4字节
    'INT': {"type": snap7.type.WordLen.Int, "size": 2},          # 2字节
    'DINT': {"type": snap7.type.WordLen.DInt, "size": 4},        # 4字节
    'REAL': {"type": snap7.type.WordLen.Real, "size": 4},        # 4字节
    'CHAR': {"type": snap7.type.WordLen.Char, "size": 1},        # 1字节
    # 'COUNTER': {"type": snap7.type.WordLen.Counter, "size": 1},
    # 'TIMER': {"type": snap7.type.WordLen.Timer, "size": 1},
}

PLC_DATATYPE_MAP = {
    'BOOL': {**PLC_WORDLEN_MAP["BYTE"], "amount": 1},
    'BYTE': {**PLC_WORDLEN_MAP["BYTE"], "amount": 1},

    'INT': {**PLC_WORDLEN_MAP["INT"], "amount": 1},       # 有符号整型 -32768～32767
    'UNIT': {**PLC_WORDLEN_MAP["INT"], "amount": 1},      # 无符号整型 0～65535
    'DINT': {**PLC_WORDLEN_MAP["DINT"], "amount": 1},     # 有符号整型 -2147483648～2147483647
    'UDINT': {**PLC_WORDLEN_MAP["DINT"], "amount": 1},    # 无符号整型 0～4294967295

    'SINT': {**PLC_WORDLEN_MAP["BYTE"], "amount": 1},     # -128～127
    'USINT': {**PLC_WORDLEN_MAP["BYTE"], "amount": 1},    # 0～255

    'WORD': {**PLC_WORDLEN_MAP["WORD"], "amount": 1},     # 无符号整型 0～65536
    'DWORD': {**PLC_WORDLEN_MAP["DWORD"], "amount": 1},   # 无符号整型 0～4294967295

    'REAL': {**PLC_WORDLEN_MAP["REAL"], "amount": 1},     # 有符号浮点类型
    'LREAL': {**PLC_WORDLEN_MAP["REAL"], "amount": 2},    # 无符号浮点类型

    'CHAR': {**PLC_WORDLEN_MAP["CHAR"], "amount": 1},  # CHAR，ASCII字符集，占用1个字节内存，主要针对欧美国家（字符比较少）
    'WCHAR': {**PLC_WORDLEN_MAP["CHAR"], "amount": 2},  # WCHAR，Unicode字符集，占用2个字节内存，主要针对亚洲国家（字符比较多）

    'STRING': {**PLC_WORDLEN_MAP["BYTE"], "amount": 256},  # STRING，占用256个字节内存，ASCII字符串，由ASCII字符组成,字节 0：最大长度, 字节 1：当前长度
    'WSTRING': {**PLC_WORDLEN_MAP["BYTE"], "amount": 512},  # WSTRING，默认占用512个字节内存（可变），Unicode字符串，由Unicode字符构成

    'DATE': {**PLC_WORDLEN_MAP["BYTE"], "amount": 2},  # Date, 2字节
    'DT': {**PLC_WORDLEN_MAP["BYTE"], "amount": 8},  # DT, Date and Time, 8字节
    'DTL': {**PLC_WORDLEN_MAP["BYTE"], "amount": 12},  # DTL(日期和时间长型)数据类型使用 12 个字节的结构保存日期和时间信息。可以在块的临时存储器或者 DB 中定义 DTL 数据。
    'TIME': {**PLC_WORDLEN_MAP["BYTE"], "amount": 4},  # time, 4字节
    'TOD': {**PLC_WORDLEN_MAP["BYTE"], "amount": 4},  # TOD (TIME_OF_DAY)数据作为无符号双整数值存储，被解释为自指定日期的凌晨算起的毫秒数(凌晨 = 0ms)。必须指定小时(24 小时/天)、分钟和秒。可以选择指定小数秒格式。
    'LTOD': {**PLC_WORDLEN_MAP["BYTE"], "amount": 8},
}

PLC_PARSER_MAP = {
    'BOOL': lambda buf, byte_index, bit: snap7.util.get_bool(buf, byte_index, bit),
    'BYTE': lambda buf, byte_index, _: snap7.util.get_byte(buf, byte_index),

    'INT': lambda buf, byte_index, _: snap7.util.get_int(buf, byte_index),
    'UINT': lambda buf, byte_index, _: snap7.util.get_uint(buf, byte_index),
    'DINT': lambda buf, byte_index, _: snap7.util.get_dint(buf, byte_index),
    'UDINT': lambda buf, byte_index, _: snap7.util.get_udint(buf, byte_index),

    'SINT': lambda buf, byte_index, _: snap7.util.get_sint(buf, byte_index),
    'USINT': lambda buf, byte_index, _: snap7.util.get_usint(buf, byte_index),

    'WORD': lambda buf, byte_index, _: snap7.util.get_word(buf, byte_index),
    'DWORD': lambda buf, byte_index, _: snap7.util.get_dword(buf, byte_index),

    'REAL': lambda buf, byte_index, _: snap7.util.get_real(buf, byte_index),
    'LREAL': lambda buf, byte_index, _: snap7.util.get_lreal(buf, byte_index),

    'CHAR': lambda buf, byte_index, _: snap7.util.get_char(buf, byte_index),
    'WCHAR': lambda buf, byte_index, _: snap7.util.get_wchar(buf, byte_index),

    'STRING': lambda buf, byte_index, _: snap7.util.get_string(buf, byte_index),
    'WSTRING': lambda buf, byte_index, _: snap7.util.get_wstring(buf, byte_index),

    'DATE': lambda buf, byte_index, _: snap7.util.get_date(buf, byte_index),
    # 'DT': lambda buf, byte_index, _: snap7.util.get_date_time_object(buf, byte_index),
    'DT': lambda buf, byte_index, _: snap7.util.get_dt(buf, byte_index),
    'DTL': lambda buf, byte_index, _: snap7.util.get_dtl(buf, byte_index),
    'TIME': lambda buf, byte_index, _: snap7.util.get_time(buf, byte_index),
    'TOD': lambda buf, byte_index, _: snap7.util.get_tod(buf, byte_index),
    'LTOD': lambda buf, byte_index, _: snap7.util.get_ltod(buf, byte_index),
}


'''
    PLC             rack    solt
    S7-200 SMART    0       1
    S7-300          0       2
    S7-1200/1500    0       0/1
'''
PLC_LOCATION = {
    "S7-1200":      {"rack": 0, "slot": 0},
    "S7-1500":      {"rack": 0, "slot": 0},
    "S7-300":       {"rack": 0, "slot": 2},
    "S7-200 SMART": {"rack": 0, "slot": 1},
}


class PLCModel(StrEnum):
    S7_1200 = "S7-1200"
    S7_1500 = "S7-1500"
    S7_300 = "S7-300"
    S7_200_SMART = "S7-200 SMART"

    def get_location(self) -> tuple[int, int]:
        location = PLC_LOCATION[self.value]
        return location["rack"], location["slot"]

# 反向定义model
PLC_MODEL_MAP = {
    0:
        {
            0: "S7-1200/1500",
            1: PLCModel.S7_200_SMART,
            2: PLCModel.S7_300,
        }
}

def get_model_name(rack: int, slot: int) -> str:
    """
    根据 rock, slot 获取 model name
    :param rack:
    :param slot:
    :return:
    """
    if rack not in PLC_MODEL_MAP:
        raise ValueError(f"illegal rack[{rack}]")
    slots = PLC_MODEL_MAP[rack]
    if slot not in slots:
        raise ValueError(f"illegal slot[{slot}]")
    model_name =slots[slot]
    return model_name




