from tortoise import Tortoise
import traceback
from config.mssql_setting import TORTOISE_ORM
from config import config
from web.routers.pictures_viewer_for_shuttle import Part
from yyDb import ImageInfoDB
import logging

_logger = logging.getLogger(__name__)

async def test_db_connection():
    try:
        # 初始化 Tortoise
        await Tortoise.init(config=TORTOISE_ORM)

        # 执行简单查询测试连接
        result = await Tortoise.get_connection("default").execute_query("SELECT @@VERSION as version")
        print("数据库连接成功！")
        print(f"SQL Server 版本: {result[1][0]['version']}")

        database = TORTOISE_ORM['connections']['default']['credentials']['database']
        # 测试数据库是否存在
        db_result = await Tortoise.get_connection("default").execute_query(
            "SELECT name FROM sys.databases WHERE name = ?",
            [database,]
        )
        if db_result[1]:
            print(f"数据库[{database}]存在")

            # db = ImageInfoDB(
            #     server=config.MSSQL_HOST,
            #     port=config.MSSQL_PORT,
            #     user=config.MSSQL_USER,
            #     password=config.MSSQL_PWD,
            #     db=config.MSSQL_DB,
            # )
            # with db as db:
            #     import datetime
            #     data = datetime.date(2026, 1, 13)
            #     row = db.locate_image(date_only=data, part_id=77, part_count=1, image_name="00-01-00.jpg")
            #     print(row)

            p = Part(year=2026, month=1, day=13, part_id=77)
            # row = await p.query_image_info(count=1, image_name="00-01-00.jpg")
            # row = await p.query_part_counts_range(count=1, image_name="00-01-00.jpg", range_len=100)
            # row = await p.query_images_time_within_part_counts(counts=[1,2], image_name="00-01-00.jpg")
            row = await p.query_part_data(count=1, image_name="00-01-00.jpg", range_len=100)
            print(row)

        else:
            print(f"数据库[{database}]不存在")

    except Exception as err:
        print(f"数据库连接连接失败: {traceback.format_exc()}")
    finally:
        # 关闭连接
        await Tortoise.close_connections()


if __name__ == "__main__":
    # 创建logger对象
    logger = logging.getLogger()
    # 设置全局最低等级（让所有handler能接收到）
    logger.setLevel(logging.DEBUG)

    # === 控制台 Handler（只显示 WARNING 及以上） ===
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    console_formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    console_handler.setFormatter(console_formatter)
    # 添加 handler 到 logger
    logger.addHandler(console_handler)


    import asyncio
    asyncio.run(test_db_connection())