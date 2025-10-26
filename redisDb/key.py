import dataclasses
import typing
import json

FrameMetaT = typing.TypeVar("FrameMetaT", bound="MetaBase")


class CreateMixin:
    """混入类，提供通用的 create 方法"""
    @classmethod
    def create(cls, **kwargs) -> typing.Self:
        # 获取所有字段及其默认值信息
        fields = dataclasses.fields(cls)
        # 找出没有默认值的必需字段
        required_fields = [
            f.name for f in fields
            if f.default is dataclasses.MISSING and
               f.default_factory is dataclasses.MISSING
        ]
        # 检查必需字段是否都存在
        missing = [f for f in required_fields if f not in kwargs]
        if missing:
            raise ValueError(f"missing {cls.__name__} fields: {missing}")
        # 只提取类注解中定义的字段，忽略额外字段
        valid_fields = {f.name for f in fields}
        fields_to_use = {f: kwargs.get(f) for f in valid_fields if f in kwargs}
        return cls(**fields_to_use)

class MetaBase(CreateMixin):
    """元数据基类，提供通用的序列化和反序列化功能"""
    def __post_init__(self):
        for f in dataclasses.fields(self):
            value = getattr(self, f.name)
            anno = f.type

            # 解析 Optional / Union
            # Optional[T] 等价于 Union[T, NoneType]
            origin = typing.get_origin(anno)
            args = typing.get_args(anno)
            if origin is typing.Union:
                actual_types = [a for a in args if a is not type(None)]
            else:
                actual_types = [anno]

            # 'null' -> None
            if isinstance(value, str) and value.lower() == "null":
                setattr(self, f.name, None)
                continue

            # 遍历所有可能类型，优先匹配
            for t in actual_types:
                try:
                    # Any -> int
                    if t is int:
                        setattr(self, f.name, int(value))
                        break
                    # Any -> float
                    elif t is float:
                        setattr(self, f.name, float(value))
                        break
                    # Any -> bool
                    elif t is bool:
                        if isinstance(value, str):
                            setattr(self, f.name, value.lower() in ("true", "1", "yes"))
                        else:
                            setattr(self, f.name, bool(value))
                        break
                    # json -> tuple/list
                    elif t in (tuple, list) and isinstance(value, str):
                        parsed = json.loads(value)
                        setattr(self, f.name, tuple(parsed) if t is tuple else list(parsed))
                        break
                    # json -> dict
                    elif t is dict and isinstance(value, str):
                        parsed = json.loads(value)
                        setattr(self, f.name, dict(parsed))
                        break
                except Exception:
                    continue

    def to_dict(self) -> dict:
        """转换为字典，确保所有值都是 Redis 兼容的类型"""
        # return dataclasses.asdict(self)
        result = dict()
        for f in dataclasses.fields(self):
            value = getattr(self, f.name)
            # 如果值是元组或列表，序列化为 JSON 字符串
            if isinstance(value, (tuple, list, dict)):
                result[f.name] = json.dumps(value)
            elif isinstance(value, bool):
                result[f.name] = int(value)
            elif value is None:
                result[f.name] = "null"
            else:
                result[f.name] = value
        return result

class KeyBase(CreateMixin):
    """键基类，提供通用的键生成功能"""
    prefix: str = ''  # 子类需要定义

    def __post_init__(self):
        # 保证 prefix 结尾有冒号，除非为空字符串
        if self.prefix and not self.prefix.endswith(':'):
            self.prefix += ':'

    def _generate_key(self, key_type: str, *parts) -> str:
        """生成键的通用方法"""
        parts = [part for part in parts if part is not None]
        return f"{self.prefix}{key_type}:{':'.join(map(str, parts))}"


@dataclasses.dataclass
class PressKey(KeyBase):
    """压机信息"""
    press_line: str
    prefix: str = 'press'

    @property
    def program_id_key(self):
        return self._generate_key("programId", self.press_line)

    @property
    def running_status_key(self):
        return self._generate_key("runningStatus", self.press_line)

    @property
    def part_counter_key(self):
        return self._generate_key("partCounter", self.press_line)

@dataclasses.dataclass
class ShuttleKey(KeyBase):
    """穿梭小车相机"""
    press_line: str
    program_id: typing.Optional[int] = None
    part_counter: typing.Optional[int] = None
    camera_ip: typing.Optional[str] = None
    prefix: str = 'shuttle'

    @property
    def running_camera_key(self):
        return self._generate_key("runningCamera", self.press_line)

    @property
    def matrix_key(self):
        return self._generate_key("matrix", self.press_line, self.program_id, self.part_counter, self.camera_ip)

    @property
    def meta_key(self):
        return self._generate_key("meta", self.press_line, self.program_id, self.part_counter, self.camera_ip)

    @property
    def photographed_key(self):
        return self._generate_key("photographed", self.press_line, self.program_id, self.part_counter)

    @property
    def light_enable_key(self):
        return self._generate_key("lightEnable", self.press_line)

@dataclasses.dataclass
class ShuttleMeta(MetaBase):
    program_id: int
    part_counter: int
    camera_ip: str
    camera_user_id: str
    has_part_t: int
    frame_t: int
    frame_num: int
    frame_shape: tuple
    frame_size: int
    frame_dtype: str




@dataclasses.dataclass
class ThermalKey(KeyBase):
    press_line: str
    camera_serial: str
    frame_count: int
    prefix: str = 'thermal'

    @property
    def matrix_key(self):
        return self._generate_key("matrix", self.press_line, self.camera_serial, self.frame_count)

    @property
    def meta_key(self):
        return self._generate_key("meta", self.press_line, self.camera_serial, self.frame_count)

    @property
    def counter_key(self):
        return self._generate_key("counter", self.press_line, self.camera_serial)

    def __str__(self) -> str:
        return f"line[{self.press_line}], camera[{self.camera_serial}], frame[{self.frame_count}]"

@dataclasses.dataclass
class ImageKey(KeyBase):
    press_line: str
    camera_serial: str
    frame_count: int
    prefix: str = 'image'

    @property
    def matrix_key(self):
        return self._generate_key("matrix", self.press_line, self.camera_serial, self.frame_count)

    @property
    def meta_key(self):
        return self._generate_key("meta", self.press_line, self.camera_serial, self.frame_count)

    @property
    def counter_key(self):
        return self._generate_key("counter", self.press_line, self.camera_serial)

    def __str__(self) -> str:
        return f"line[{self.press_line}], camera[{self.camera_serial}], frame[{self.frame_count}]"



@dataclasses.dataclass
class ThermalMeta(MetaBase):
    shape: tuple    # height, width, channel(is needed)
    size: int
    dtype: str
    request_t: int      # 发送请求时间戳
    response_t: int     # 接收响应时间戳
    prase_t: int        # 完成解析时间戳

@dataclasses.dataclass
class ImageMeta(MetaBase):
    shape: tuple    # height, width, channel(is needed)
    size: int
    dtype: str

    request_t: int      # 发送请求时间戳
    response_t: int     # 接收响应时间戳
    prase_t: int        # 完成解析时间戳
    process_t: int      # 图像处理时间戳

    program_id: typing.Optional[int]
    program_id_t: int
    running_status: typing.Optional[bool]
    running_status_t: int

    monitor_window_max_temp: typing.Optional[float]
    monitor_window_min_temp: typing.Optional[float]
    to_collect: bool
    to_monit: bool

    counter: int

    to_draw_monitor_window: bool
    to_draw_collector_windows: bool
    to_draw_global_max_temp: bool

    color_map_min_temp: int
    color_map_max_temp: int
    auto_color_map: bool

    monitor_window_diff_temp_threshold: int

    collector_windows_realtime_max_temp: dict
    collector_windows_max_temp: dict

    monitor_window: list
    collector_windows: list


@dataclasses.dataclass
class MatrixProcessKey(KeyBase):
    press_line: str
    camera_serial: str
    program_id: int
    prefix: str = 'matrixProcess'

    @property
    def params_key(self):
        return self._generate_key("params", self.press_line, self.camera_serial, self.program_id)

@dataclasses.dataclass
class MaxTempKey(KeyBase):
    press_line: str
    camera_serial: str
    prefix: str = 'maxTemp'

    @property
    def new_key(self):
        return self._generate_key("new", self.press_line, self.camera_serial)
