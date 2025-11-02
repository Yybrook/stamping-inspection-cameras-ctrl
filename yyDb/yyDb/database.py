from typing import Optional, Union
from .database_pool import DatabasePool, DatabaseType

import logging

_logger = logging.getLogger(__name__)


class Database:
    def __init__(self, db_pool: DatabasePool):
        self.db_pool = db_pool
        self.db_type = self.db_pool.db_type
        self.injection_flag = self.db_pool.injection_flag

        self.conn = None
        self.cursor = None

        # 表格列名缓存
        self._table_columns_cache = dict()

    def __enter__(self):
        """
        连接数据库, 创建游标
        :return:
        """
        self.conn = self.db_pool.connection()
        self.cursor = self.conn.cursor()
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        """
        关闭游标, 断开数据库连接
        :param exc_type:
        :param exc_value:
        :param exc_tb:
        :return:
        """
        if exc_type:
            self.conn.rollback()
        else:
            self.conn.commit()

        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        self.cursor = None
        self.conn = None

    def assign_where(self, filter_dict: Optional[dict] = None):
        """
        定义 sql 语句中的 where
        :param filter_dict:
        :return:
        """
        if not filter_dict:
            return "", tuple()

        conditions = []
        params = list()
        for key, val in filter_dict.items():
            if val is None:
                conditions.append(f"{key} IS NULL")
            elif isinstance(val, str) and (val.strip().upper() == "ALL" or val.strip().upper() == ""):
                continue
            elif isinstance(val, (list, tuple, set)):
                placeholders = ", ".join([self.injection_flag] * len(val))
                conditions.append(f"{key} IN ({placeholders})")
                params.extend(val)
            else:
                conditions.append(f"{key}={self.injection_flag}")
                params.append(val)

        if not conditions:
            return "", ()

        return " AND ".join(conditions), tuple(params)

    def get_table_columns_name(self, table_name) -> list:
        if table_name in self._table_columns_cache:
            return self._table_columns_cache[table_name]

        if self.db_type == DatabaseType.MSSQL:
            sql = f'SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = {self.injection_flag}'
            self.log_sql(sql, para=(table_name,))
            self.cursor.execute(sql, (table_name,))
            data = self.cursor.fetchall()
            columns = [d[0] for d in data]

        elif self.db_type == DatabaseType.ACCESS:
            sql = f'SELECT TOP 1 * FROM {table_name}'
            self.log_sql(sql)
            self.cursor.execute(sql)
            data = self.cursor.description
            columns = [d[0] for d in data]

        elif self.db_type == DatabaseType.SQLITE:
            sql = f"PRAGMA table_info('{table_name}')"
            self.log_sql(sql)
            self.cursor.execute(sql)
            data = self.cursor.fetchall()
            columns = [d[1] for d in data]  # 第二列是列名

        elif self.db_type == DatabaseType.MYSQL:
            # 需要数据库名 self.database
            sql = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = {self.injection_flag}"
            self.log_sql(sql, para=(table_name,))
            self.cursor.execute(sql, (table_name,))
            data = self.cursor.fetchall()
            columns = [d[0] for d in data]
        else:
            raise NotImplementedError(f"Unsupported database type: {self.db_type}")

        self.log_res(data=data)
        self._table_columns_cache[table_name] = columns
        return columns

    def get_table_rows(self, table_name: str, filter_dict: Optional[dict] = None) -> int:
        """
        获取行数
        :param table_name:
        :param filter_dict:
        :return:
        """
        where_clause, params = self.assign_where(filter_dict)
        if where_clause:
            sql = f"SELECT COUNT(*) FROM {table_name} WHERE {where_clause}"
            self.log_sql(sql, para=params)
            self.cursor.execute(sql, params)
        else:
            sql = f"SELECT COUNT(*) FROM {table_name}"
            self.log_sql(sql)
            self.cursor.execute(sql)

        data = self.cursor.fetchone()
        self.log_res(data=data)
        count = data[0] if data else 0
        return count

    def get_table_pages(self, table_name: str, page_rows: int, filter_dict: Optional[dict] = None):
        """
        获取分页信息
        :param table_name: 表名
        :param page_rows: 每页行数，必须 > 0
        :param filter_dict: 筛选条件
        :return: (总页数, 整页数, 最后一页行数)
        """
        if page_rows <= 0:
            raise ValueError("page_rows must be a positive integer")

        total_rows = self.get_table_rows(table_name=table_name, filter_dict=filter_dict)
        full_pages, residue_rows = divmod(total_rows, page_rows)  # 除
        total_pages = full_pages + bool(residue_rows)
        return total_pages, full_pages, residue_rows

    def select_from_table(self, table_name: str, demand_list: Optional[list], filter_dict: Optional[dict] = None,
                          is_fetchall: bool = True,
                          page: int = 0, page_rows: int = -1,
                          order_by: Optional[str] = None, is_desc: bool = True) -> Union[list, dict]:
        """
        从表格中筛选数据（支持分页、筛选、排序）
        :param table_name:
        :param demand_list:
        :param filter_dict:
        :param is_fetchall:
        :param page:        页码
        :param page_rows:   每页行数
        :param order_by:    排序
        :param is_desc:     降序
        :return:
        """

        '''
        page_rows <= 0 -> 不分页， 此时 page = 0
        page_rows >0   -> 分页,   此时 page == 0 -> 第一页
                                     page > 0  -> 其他页
        
        '''

        # 处理需求列
        if not demand_list:
            demand = "*"
            demand_list = self.get_table_columns_name(table_name)
        else:
            demand = ", ".join(demand_list)

        # 分页合法性
        if page_rows <= 0:
            page = 0  # 不分页
        elif page < 0:
            raise ValueError("page must be >= 0")

        # 处理排序字段（分页时必须排序）
        if not order_by:
            if page_rows > 0:
                order_by = demand_list[0]
        elif order_by not in self.get_table_columns_name(table_name):
            raise NameError(f"order by key[{order_by}] is illegal")
        elif order_by not in demand_list:
            demand_list.append(order_by)
            demand = f"{demand}, {order_by}"

        # 排序语句
        order_clause = ""
        if order_by:
            order_clause = f" ORDER BY {order_by} {'DESC' if is_desc else 'ASC'}"

        # 处理过滤条件
        where_clause, where_params = self.assign_where(filter_dict)
        if where_clause:
            where_clause = f" WHERE {where_clause}"

        # 分页
        if page_rows > 0:
            total_pages, full_pages, remaining_rows = self.get_table_pages(table_name, page_rows, filter_dict)
            if page >= total_pages:
                return list()

            limit_rows = page_rows if page < full_pages else remaining_rows
            offset_rows = page * page_rows

            is_fetchall = True

            if self.db_type in [DatabaseType.SQLITE, DatabaseType.MYSQL]:
                sql = f"SELECT {demand} FROM {table_name}{where_clause}{order_clause} LIMIT {limit_rows} OFFSET {offset_rows}"

            elif self.db_type == DatabaseType.MSSQL:
                if page == 0:
                    sql = f"SELECT TOP {page_rows} {demand} FROM {table_name}{where_clause}{order_clause}"
                else:
                    sql = f"SELECT {demand} FROM {table_name}{where_clause}{order_clause} OFFSET {offset_rows} ROWS FETCH NEXT {limit_rows} ROWS ONLY"

            elif self.db_type == DatabaseType.ACCESS:
                # 第一页
                if page == 0:
                    sql = f"SELECT TOP {page_rows} {demand} FROM {table_name}{where_clause}{order_clause}"
                # 其他页
                else:
                    inner_order = f" ORDER BY {order_by} {'ASC' if is_desc else 'DESC'}"
                    sql = (
                        f"SELECT TOP {limit_rows} {demand} FROM "
                        f"(SELECT TOP {(full_pages - page) * page_rows + remaining_rows} {demand} FROM {table_name}{where_clause}{inner_order}) "
                        f"AS inner_tbl1 {order_clause}"
                    )

            else:
                raise NotImplementedError(f"Unsupported database type: {self.db_type}")

        # 不分页
        else:
            sql = f"SELECT {demand} FROM {table_name}{where_clause}{order_clause}"

        self.log_sql(sql, para=where_params)
        self.cursor.execute(sql, where_params)

        # 获取结果
        if is_fetchall:
            data = self.cursor.fetchall()
            self.log_res(data=data)
            return [dict(zip(demand_list, row)) for row in data]
        else:
            data = self.cursor.fetchone()
            self.log_res(data=data)
            return dict(zip(demand_list, data)) if data else dict()

    def update_table(self, table_name: str, demand_dict: dict, filter_dict: dict):
        """
        更新表格
        :param table_name:
        :param demand_dict:
        :param filter_dict:
        :return:
        """
        if not demand_dict:
            raise ValueError("demand_dict should not be empty")

        if not filter_dict:
            raise ValueError("filter_dict should not be empty")

        set_clause = ", ".join(f"{key} = {self.injection_flag}" for key in demand_dict.keys())
        set_params = tuple(demand_dict.values())

        # where
        where_clause, where_params = self.assign_where(filter_dict)

        sql = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause}"
        params = set_params + where_params
        self.log_sql(sql, para=params)
        self.cursor.execute(sql, params)

    def assign_demand(self, demand_dict: dict):
        """
        组织 insert 需求
        :param demand_dict:
        :return:
        """
        keys = list(demand_dict.keys())
        demand_key = ", ".join(keys)
        demand_interrogation = ", ".join([self.injection_flag] * len(keys))
        params = tuple(demand_dict.values())
        return demand_key, demand_interrogation, params

    def insert_to_table(self, table_name: str, demand: Union[dict, list, tuple]):
        """
        向表格中插入单条或多条记录
        :param table_name:
        :param demand:
        :return:
        """
        # 字典
        if isinstance(demand, dict):
            demand_key, demand_interrogation, params = self.assign_demand(demand)
            sql = f'INSERT INTO {table_name} ({demand_key}) VALUES ({demand_interrogation})'
            self.log_sql(sql, para=params)
            self.cursor.execute(sql, params)
        # 列表
        else:
            demand_key, demand_interrogation, _ = self.assign_demand(demand[0])
            params = [tuple(d.values()) for d in demand]
            sql = f'INSERT INTO {table_name} ({demand_key}) VALUES ({demand_interrogation})'
            self.log_sql(sql, para=params)
            self.cursor.executemany(sql, params)

    def delete_from_table(self, table_name: str, filter_dict: Optional[dict]):
        """
        从表格中删除
        :param table_name:
        :param filter_dict:
        :return:
        """
        if filter_dict:
            where_clause, where_params = self.assign_where(filter_dict=filter_dict)
            sql = f"DELETE FROM {table_name} WHERE {where_clause}"
            self.log_sql(sql, para=where_params)
            self.cursor.execute(sql, where_params)
        # 删除所有
        else:
            # 删除所有记录，谨慎操作
            sql = f"DELETE FROM {table_name}"
            self.log_sql(sql)
            self.cursor.execute(sql)

    def get_version(self) -> int:
        if self.db_type == DatabaseType.MSSQL:
            '''
            SQL Server 版本	主版本号
            2012	        11
            2014	        12
            2016	        13
            2017	        14
            2019	        15
            2022	        16
            '''
            sql = "SELECT SERVERPROPERTY('ProductVersion')"
            self.log_sql(sql)
            self.cursor.execute(sql)
            # data -> ('16.0.4195.2',)
            data = self.cursor.fetchone()
            self.log_res(data=data)
            major_version = int(data[0].split('.')[0])
            return major_version
        else:
            raise NotImplementedError(f"Unsupported database type for get_version() function: {self.db_type}")

    def log_sql(self, sql: str, para=None):
        """
        log 输出 sql语句
        :param sql:
        :param para:
        :return:
        """
        if para:
            try:
                # 用 {} 占位符替换注入标识符，避免多重替换影响
                safety_sql = sql.replace(self.injection_flag, "{}")
                # 使用 format 进行格式化，异常捕获防止日志崩溃
                formatted_sql = safety_sql.format(*[repr(p) for p in para])
                _logger.debug(f"{self.identity} sql: {formatted_sql}")
            except Exception as err:
                _logger.exception(f"{self.identity} sql[{sql}], params[{para}] format error: {err}")
        else:
            _logger.debug(f"{self.identity} sql: {sql}")

    def log_res(self, data):
        _logger.debug(f"{self.identity} res: {data}")

    @property
    def identity(self):
        return f"Database[{self.db_type}]"
