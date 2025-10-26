from . import config

TORTOISE_ORM = {
    'connections': {
        'default':{
            'engine': 'tortoise.backends.mssql',  # 使用 MSSQL 引擎
            'credentials': {
                'host': config.MSSQL_HOST,
                'port': config.MSSQL_PORT,
                'user': config.MSSQL_USER,
                'password': config.MSSQL_PWD,
                'database': config.MSSQL_DB,
                'driver': config.MSSQL_DRIVER,  # ODBC 驱动名称, 或根据系统改为 "ODBC Driver 11 for SQL Server", 可通过命令 odbcinst -q -d -n 查看
                'TrustServerCertificate': 'yes', # 忽略自签名证书验证
                'charset': 'utf8mb4',
                'echo': True,
                # 'timeout': 10,
            }
        },
    },
    'apps': {
        'models': {
            'models': ['saver.saverForShuttle.models', "aerich.models"],
            'default_connection': 'default',
        }
    },
    'use_tz': False,
    'timezone': 'Asia/Shanghai'
}

# aerich init -t config.mssql_setting.TORTOISE_ORM --location migrations

# aerich是一种ORM迁移工具，需要结合tortoise异步orm框架使
#
# 安装aerich: pip install aerich
#
# 初始化配置: aerich init -t mssql_setting.TORTOISE_ORM
# 初始化完会在当前目录生成一个文件[pyproject.toml] 和 一个文件夹[migrations]
# pyproject.toml：保存配置文件路径，低版本可能是aerich.ini
# migrations：存放迁移文件
#
# 初始化数据库: aerich init-db
#
# 更新模型并进行迁移: aerich migrate --name XXX
#
# 重新执行迁移, 写入数据库: aerich upgrade
#
# 回到上一个版本: aerich downgrade
#
# 查看历史迁移记录: aerich history
