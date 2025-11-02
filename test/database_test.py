from tortoise import Tortoise
import traceback
from config.mssql_setting import TORTOISE_ORM

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
        else:
            print(f"数据库[{database}]不存在")

    except Exception as err:
        print(f"数据库连接连接失败: {traceback.format_exc()}")
    finally:
        # 关闭连接
        await Tortoise.close_connections()


if __name__ == "__main__":
    import asyncio
    asyncio.run(test_db_connection())