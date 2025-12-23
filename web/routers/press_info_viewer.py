import os
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
import asyncio
import logging
import datetime
import json
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Depends
from fastapi.responses import FileResponse
import asyncio
import logging

from web import ws_manager, dependencies
from redisDb import AsyncRedisDB

TAG = "pressInfo"

# 定位到 web目录
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # app/ 的上一级目录
STATIC_DIR = os.path.join(BASE_DIR, "statics")

router = APIRouter()


@router.get("/")
async def press_info_viewer():
    file_path = os.path.join(STATIC_DIR, "press_info_viewer.html")
    return FileResponse(file_path, media_type="text/html")


@router.websocket("/{press_line}/ws")
async def websocket_endpoint(
        ws: WebSocket,
        press_line: str,
        redis: AsyncRedisDB = Depends(dependencies.get_redis)
):
    await ws_manager.connect(tag=TAG, ws=ws)

    try:
        while True:
            # 或者 receive_json()
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        ws_manager.disconnect(tag=TAG, ws=ws)



async def subscribe_program_id(
        press_line: str,
        redis: AsyncRedisDB = Depends(dependencies.get_redis)
):
    async for timestamp, program_id in redis.get_program_id(
            press_line=press_line,
            block=1000,             # 阻塞1秒等待新消息
            include_last=True       # 先返回最后一条历史消息
    ):
        # 检查是否有活跃客户端
        if not ws_manager.survival(tag=TAG):
            continue

        if program_id is None:
            data = {
                "program_id": {
                    "value": None,
                    "timestamp": None,
                }
            }
        else:
            data = {
                "program_id": {
                    "value": program_id,
                    "timestamp": timestamp,
                }
            }
        # 发送消息
        await ws_manager.broadcast(tag=TAG, message=data)


async def subscribe_running_status(
        press_line: str,
        redis: AsyncRedisDB = Depends(dependencies.get_redis)
):
    async for timestamp, running_status in redis.get_running_status(
            press_line=press_line,
            block=1000,             # 阻塞1秒等待新消息
            include_last=True       # 先返回最后一条历史消息
    ):
        # 检查是否有活跃客户端
        if not ws_manager.survival(tag=TAG):
            continue

        if running_status is None:
            data = {
                "running_status": {
                    "value": None,
                    "timestamp": None,
                }
            }
        else:
            data = {
                "running_status": {
                    "value": running_status,
                    "timestamp": timestamp,
                }
            }
        # 发送消息
        await ws_manager.broadcast(tag=TAG, message=data)


async def subscribe_part_counter(
        press_line: str,
        redis: AsyncRedisDB = Depends(dependencies.get_redis)
):
    async for timestamp, part_counter in redis.get_part_counter(
            press_line=press_line,
            block=1000,             # 阻塞1秒等待新消息
            include_last=True       # 先返回最后一条历史消息
    ):
        # 检查是否有活跃客户端
        if not ws_manager.survival(tag=TAG):
            continue

        if part_counter is None:
            data = {
                "part_counter": {
                    "value": None,
                    "timestamp": None,
                }
            }
        else:
            data = {
                "part_counter": {
                    "value": part_counter,
                    "timestamp": timestamp,
                }
            }
        # 发送消息
        await ws_manager.broadcast(tag=TAG, message=data)


async def get_light_enable(
        press_line: str,
        redis: AsyncRedisDB = Depends(dependencies.get_redis)
):
    while True:
        await asyncio.sleep(1)

        # 检查是否有活跃客户端
        if not ws_manager.survival(tag=TAG):
            continue

        enable = await redis.get_light_enable(press_line=press_line)
        ttl = await redis.get_light_enable_ttl(press_line=press_line)

        data = {
            "light_enable": {
                "value": enable,
                "ttl": ttl
            }
        }

        # 发送消息
        await ws_manager.broadcast(tag=TAG, message=data)