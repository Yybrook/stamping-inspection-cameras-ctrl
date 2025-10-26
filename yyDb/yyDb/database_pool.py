import sys
from enum import StrEnum
from typing import Union
import sqlite3
import pymssql
import pyodbc
import pymysql
from dbutils.pooled_db import PooledDB


class DatabaseType(StrEnum):
    MSSQL = 'MSSQL'
    ACCESS = 'ACCESS'
    SQLITE = 'SQLITE'
    MYSQL = 'MYSQL'

    @property
    def injection_flag(self) -> str:
        return {
            self.MSSQL: "%s",
            self.ACCESS: "?",
            self.SQLITE: "?",
            self.MYSQL: "%s",
        }[self]


class DatabasePool:
    def __init__(self, db_type: Union[str, DatabaseType], **kwargs):
        if isinstance(db_type, str):
            db_type = db_type.upper()
            if db_type not in [item.value for item in DatabaseType]:
                raise NameError(f'Database[{db_type}] is illegal')
            self.db_type = DatabaseType(db_type)
        else:
            self.db_type = db_type

        if not sys.platform.startswith("win") and self.db_type == DatabaseType.ACCESS:
            raise EnvironmentError(f'Database[{self.db_type}] is not supported in platform[{sys.platform}]')

        self.injection_flag = self.db_type.injection_flag

        # 超时时间
        self.timeout = kwargs.pop("timeout", 2)
        # 最大连接数
        self.maxconnections = kwargs.pop("maxconnections", 20)

        for key, value in kwargs.items():
            self.__setattr__(key, value)

        # 创建连接池
        # todo 参数含义，以及可自定义
        self.pool_config = dict(
            timeout=self.timeout,
            mincached=0,  # 初始化连接池时创建的连接数。默认为0，即初始化时不创建连接。(建议默认0，假如非0的话，在某些数据库不可用时，整个项目会启动不了)
            maxcached=0,  # 池中空闲连接的最大数量。默认为0，即无最大数量限制。(建议默认)
            maxshared=0,  # 池中共享连接的最大数量。默认为0，即每个连接都是专用的，不可共享(不常用，建议默认)
            maxconnections=self.maxconnections,  # 连接池最大连接数
            blocking=True,      # 连接数达到最大时，新连接是否可阻塞。默认False，即达到最大连接数时，再取新连接将会报错。(建议True，达到最大连接数时，新连接阻塞，等待连接数减少再连接)
            maxusage=None,      # 控制单个连接的最大重用次数
            setsession=None,    # 用于初始化会话的 SQL 命令
            reset=True,         # 当连接返回到池中时，重置连接的方式。默认True，总是执行回滚。
            failures=None,      # 可选的异常类或异常类元组，用于指定连接故障转移机制应应用的异常类型
            ping=1,             # 确定何时使用ping()检查连接。默认1，即当连接被取走，做一次ping操作。0是从不ping，1是默认，2是当该连接创建游标时ping，4是执行sql语句时ping，7是总是ping
        )

        self.pool = self._create_pool()

    def _create_pool(self):
        """
        创建数据库连接池
        :return:
        """
        if self.db_type == DatabaseType.MSSQL:
            # SQL Server 连接配置
            return PooledDB(
                creator=pymssql,
                server=self.server,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
                charset='utf8',
                tds_version='7.0',
                autocommit=True,    # 自动提交
                **self.pool_config
            )

        elif self.db_type == DatabaseType.MYSQL:
            return PooledDB(
                creator=pymysql,
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
                charset='utf8mb4',
                autocommit=True,  # 自动提交
                **self.pool_config
            )

        elif self.db_type == DatabaseType.SQLITE:
            # SQLite 连接配置
            '''
            配置参数:
                detect_types, isolation_level, check_same_thread, factory, cached_statements, uri
            '''
            check_same_thread = self.__dict__.get('check_same_thread', None)
            if check_same_thread is None:
                check_same_thread = True
            return PooledDB(
                creator=sqlite3,
                database=self.path,                     # sqlite 数据库文件路径
                check_same_thread=check_same_thread,    # 允许跨线程访问, 重要! 默认 True
                isolation_level=None,
                **self.pool_config
            )

        elif self.db_type == DatabaseType.ACCESS:
            # Access 连接配置
            '''
            配置参数:
                autocommit, ansi, timeout
            '''
            return PooledDB(
                creator=pyodbc,
                driver='{Microsoft Access Driver (*.mdb, *.accdb)}',  # ODBC 驱动
                dbq=self.path,      # Access 数据库文件路径
                autocommit=True,    # 自动提交
                **self.pool_config
            )

    def connection(self):
        """
        从连接池获取连接
        :return: 数据库连接对象
        """
        return self.pool.connection()
