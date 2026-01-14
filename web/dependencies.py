import typing

from redisDb import AsyncRedisDB
from config import config


redis_params = {
    "host": config.REDIS_HOST,
    "port": config.REDIS_PORT,
    "db": config.REDIS_DB,
}

_redis: typing.Optional[AsyncRedisDB] = None

async def get_redis():
    global _redis

    if _redis is None:
        # 假设 Redis 配置在 app_config 中
        _redis = await AsyncRedisDB.create(**redis_params, ping=True)

    return _redis

async def close_redis():
    global _redis

    if _redis is not None:
        # 假设 Redis 配置在 app_config 中
        _redis.aclose()
        _redis = None
