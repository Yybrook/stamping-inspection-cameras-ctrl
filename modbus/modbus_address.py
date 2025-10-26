import os
import yaml
import typing


class ModbusAddressMeta(type):

    def __getitem__(cls, name):
        """
        支持类下标访问：ModbusAddress['light_enable']
        """
        return cls.get_address(name)


class ModbusAddress(metaclass=ModbusAddressMeta):

    @classmethod
    def load_address(cls, path: typing.Optional[str]=None) -> list:
        """
        加载地址配置文件，并将其注册为类属性。
        :param path: YAML 文件路径，如果为 None，则默认使用当前目录下 my_modbus_address.yml
        :return: 地址名称列表
        :raises FileNotFoundError, ValueError
        """
        if not hasattr(cls, "_address"):

            if getattr(cls, "_path", None) is None and path is None:
                basename = os.path.basename(__file__)
                cls._path = __file__.replace(basename, "modbus_address.yml")
            elif getattr(cls, "_path", None) is None:
                cls._path = path

            # 确保文件存在
            if not os.path.isfile(cls._path):
                cls._path = None
                raise FileNotFoundError(f"modbus address yaml file not found: {cls._path}")

            with open(cls._path, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f)

            cls._address = config.get("address")

            for i, name in enumerate(cls._address):
                # 避免无效名称和重复设置
                if not name.isidentifier():
                    raise ValueError(f"modbus address yaml file has illegal keyword[{name}]")
                setattr(cls, name, i)

        return cls._address

    @classmethod
    def get_address(cls, name: str) -> int:
        """
        根据名称获取地址值
        :param name: 地址名称
        :return: 整数地址
        :raises NameError
        """
        if not hasattr(cls, "_address"):
            cls.load_address(None)

        addr = getattr(cls, name, None)
        if addr is None:
            raise NameError(f"keyword[{name}] is not defined in modbus address")
        return addr

    @classmethod
    def show(cls) -> str:
        """
        获取地址 -> 名称 映射字符串
        :return: 文本表
        """
        if not hasattr(cls, "_address"):
            cls.load_address(None)

        lines = [f"\t{i}\t-> {name}" for i, name in enumerate(cls._address)]
        return "\n".join(lines)


if __name__ == "__main__":
    ModbusAddress.load_address(r"C:\Users\yy\Documents\yyProjects\StampingInspection\stamping-inspection\cameras-ctrl\config\modbus_address.yml")
    # print(ModbusAddress.show())
    # print(f"enable -> {ModbusAddress.get_address('enable')}")
    print(f"light_enable -> {ModbusAddress['light_enable']}")
