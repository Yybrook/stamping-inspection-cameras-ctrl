import typing
from datetime import date

from .yyDb import Database, DatabasePool, DatabaseType

class ImageInfoDB(Database):
    def __init__(self, server: str, port: int, user: str, password: str, db: str):

        self.shuttle_image_table = "shuttle_image"

        super().__init__(
            db_pool=DatabasePool(
                db_type=DatabaseType.MSSQL,
                server=server,
                port=port,
                user=user,
                password=password,
                database=db,
            )
        )

        with self:
            self.major_version = self.get_version()

    @property
    def china_time_script(self):
        # 生成东8区时间
        # < 13 是低于 2016版本
        return "DATEADD(HOUR, 8, GETUTCDATE())" if self.major_version < 13 else "CAST(SYSDATETIMEOFFSET() AT TIME ZONE 'UTC' AT TIME ZONE 'China Standard Time' AS DATETIME)"

    def add_2_shuttle_image_table(self, **kwargs):
        self.insert_to_table(table_name=self.shuttle_image_table, demand=kwargs)

    def locate_image(self, date_only: date, part_id: int, part_count: int, image_name: str) -> typing.Optional[dict]:
        sql = f"""
            SELECT id, time, camera_ip, camera_user_id, frame_num, frame_t, shuttle_has_part_t, frame_width, frame_height
            FROM {self.shuttle_image_table}
            WHERE part_id = {self.injection_flag}
              AND part_count = {self.injection_flag}
              AND CONVERT(DATE, time) = {self.injection_flag}
              AND image_path LIKE {self.injection_flag}
        """
        params = (part_id, part_count, date_only, f'%{image_name}')
        self.log_sql(sql, para=params)
        self.cursor.execute(sql, params)
        data = self.cursor.fetchone()
        if data is None:
            return None
        return {
            "id": data[0],
            "saved_t": int(data[1].timestamp() * 1000),
            "camera_ip": data[2],
            "camera_user_id": data[3],
            "frame_num": int(data[4]),
            "frame_t": int(data[5]),
            "shuttle_has_part_t": int(data[6]),
            "frame_width": data[7],
            "frame_height": data[8],
        }

    def locate_images(self, date_only: date, part_id: int, part_counts: list, image_name: str) -> typing.Optional[dict]:
        placeholders = ", ".join([self.injection_flag] * len(part_counts))

        sql = f"""
            SELECT part_count, time, frame_t, shuttle_has_part_t
            FROM {self.shuttle_image_table}
            WHERE part_id = {self.injection_flag}
              AND part_count IN ({placeholders})
              AND CONVERT(DATE, time) = {self.injection_flag}
              AND image_path LIKE {self.injection_flag}
        """
        params = (part_id, *part_counts, date_only, f'%{image_name}')
        self.log_sql(sql, para=params)
        self.cursor.execute(sql, params)
        data = self.cursor.fetchall()
        if not data:
            return None
        res = dict()
        for part_count, time_obj, frame_t, shuttle_t in data:
            res[part_count] = {
                "saved_t": int(time_obj.timestamp() * 1000),
                "frame_t": int(frame_t),
                "shuttle_has_part_t": int(shuttle_t),
            }

        return dict(sorted(res.items()))

    def get_part_counts_range(self, date_only: date, part_id: int, part_count: int, image_name: str, range_len: int = 100):
        sql = f"""
            SELECT part_count
            FROM {self.shuttle_image_table}
            WHERE part_id = {self.injection_flag}
              AND CONVERT(DATE, time) = {self.injection_flag}
              AND image_path LIKE {self.injection_flag}
        """
        params = (part_id, date_only, f'%{image_name}')
        self.log_sql(sql, para=params)
        self.cursor.execute(sql, params)
        data = self.cursor.fetchall()
        if not data:
            return None

        all_part_counts = sorted([d[0] for d in data])
        if part_count not in all_part_counts:
            return None

        if len(all_part_counts) <= range_len:
            return all_part_counts

        # 获取中心索引
        idx = all_part_counts.index(part_count)
        half = (range_len - 1) // 2

        # 向前取 half 个，向后取补足的
        start = max(0, idx - half)
        end = min(len(all_part_counts), idx + (range_len - (idx - start)))

        return all_part_counts[start:end]

    def clean_image_table(self, keep_days: int, table: str):
        sql = f"""
            DELETE FROM {table}
            WHERE time < DATEADD(DAY, -{keep_days}, {self.china_time_script})
        """
        self.log_sql(sql)
        self.cursor.execute(sql)

    def clean_shuttle_image_table(self, keep_days: int):
        self.clean_image_table(keep_days, self.shuttle_image_table)

    def create_shuttle_image_table(self):
        sql = f"""
            IF OBJECT_ID('{self.shuttle_image_table}', 'U') IS NULL
            CREATE TABLE {self.shuttle_image_table} (
                id INT IDENTITY(1,1) PRIMARY KEY,
                -- time DATETIME NOT NULL DEFAULT GETDATE(),   -- 自动生成UTC时间
                time DATETIME NOT NULL DEFAULT {self.china_time_script},    -- -- 自动生成东8区时间
                part_id INT NOT NULL,
                part_count INT NOT NULL,
                camera_ip NVARCHAR(20) NOT NULL,
                camera_user_id NVARCHAR(20) NOT NULL,
                frame_num int NOT NULL,
                frame_t bigint NOT NULL,
                frame_width int NOT NULL,
                frame_height int NOT NULL,
                frame_size int NOT NULL,
                shuttle_has_part_t bigint NOT NULL,
                image_path NVARCHAR(255) NOT NULL,
                res_match BIT,
                res_prediction BIT
            )
        """
        self.log_sql(sql)
        self.cursor.execute(sql)

    def drop_shuttle_image_table(self):
        sql = f"""
            IF OBJECT_ID('{self.shuttle_image_table}', 'U') IS NOT NULL
                DROP TABLE {self.shuttle_image_table}
        """
        self.log_sql(sql)
        self.cursor.execute(sql)

    @property
    def identity(self):
        return f"Database[{self.db_type}|{self.db_pool.database}]"
