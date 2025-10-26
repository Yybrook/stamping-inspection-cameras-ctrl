import logging
from yyDb.yyDb import Database, DatabaseType, DatabasePool

if __name__ == '__main__':
    # 配置日志系统
    logging.basicConfig(
        level=logging.DEBUG,  # 设置全局日志级别
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),  # 输出到控制台
            # logging.FileHandler("app.log", mode="w", encoding="utf-8")  # 输出到文件
        ]
    )

    db_ms = Database(
        db_pool=DatabasePool(
            db_type=DatabaseType.MSSQL,
            server='127.0.0.1',
            port='1433',
            user='sa',
            password='yymy',
            database='pressSafety',
        )
    )
    with db_ms:
        print(db_ms.get_table_rows(table_name="stuffInfo"))
        print(db_ms.get_table_columns_name(table_name="stuffInfo"))
        print(db_ms.select_from_table(
            table_name="stuffInfo",
            demand_list=["id", ],
            filter_dict={"id": ['000001', '500268', '500444','500454', '500455',  '501631', '501632', "503841",  ]},
            is_fetchall=True,
            page=2,
            page_rows=3,
            order_by="id",
            is_desc=False
        ))

    db_sq = Database(
        db_pool=DatabasePool(
            db_type="SQLITE",
            path=r"CamerasCtrl.db",
        )
    )
    with db_sq:
        print(db_sq.get_table_rows(table_name="partsMap"))
        print(db_sq.get_table_columns_name(table_name="partsMap"))
        print(db_sq.select_from_table(
            table_name="partsMap",
            demand_list=['part_id',],
            filter_dict={"[192.168.4.105]": 1},
            is_fetchall=True,
            page=0,
            page_rows=2,
            order_by="part_id",
            is_desc=False
        ))

    db_as = Database(
        db_pool=DatabasePool(
            db_type="ACCESS",
            path=r".\myAccess.accdb",
        )
    )
    with db_as:
        print(db_as.get_table_rows(table_name="demo"))
        print(db_as.get_table_columns_name(table_name="demo"))
        print(db_as.select_from_table(
            table_name="demo",
            demand_list=["role", "location"],
            filter_dict={'location': 567},
            is_fetchall=True,
            page=0,
            page_rows=0,
            order_by="ID",
            is_desc=True
        ))