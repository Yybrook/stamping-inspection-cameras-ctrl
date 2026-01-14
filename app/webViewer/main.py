import asyncio
import os
from pathlib import Path
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from tortoise import Tortoise
from config.mssql_setting import TORTOISE_ORM
from config.config import IMAGE_SAVED_DIR_FOR_SHUTTLE

from web.routers import press_info_viewer, pictures_viewer_for_shuttle
from web.dependencies import get_redis, close_redis


async def cancel_tasks(tasks: list[asyncio.Task]):
    for task in tasks:
        task.cancel()
    for task in tasks:
        try:
            await task
        except asyncio.CancelledError:
            pass


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 连接 redis
    await get_redis()

    # 连接数据库连接
    await Tortoise.init(config=TORTOISE_ORM)

    tasks = list()
    tasks.append(asyncio.create_task(press_info_viewer.subscribe_program_id(press_line="5-100")))
    tasks.append(asyncio.create_task(press_info_viewer.subscribe_running_status(press_line="5-100")))
    tasks.append(asyncio.create_task(press_info_viewer.subscribe_part_counter(press_line="5-100")))
    tasks.append(asyncio.create_task(press_info_viewer.get_light_enable(press_line="5-100")))

    yield

    await cancel_tasks(tasks)
    await close_redis()

app = FastAPI(lifespan=lifespan)

# 添加CORS中间件
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


########################################################################
# 加载静态文件
########################################################################
BASE_DIR = Path(__file__).resolve().parent.parent.parent  # app/

app.mount("/static", StaticFiles(directory=BASE_DIR / "web/static", html=True), name="static")
# 在应用启动时挂载静态文件目录
app.mount("/pictures/forShuttle", StaticFiles(directory=IMAGE_SAVED_DIR_FOR_SHUTTLE), name="pictures_for_shuttle")


app.include_router(press_info_viewer.router, prefix=f"/{press_info_viewer.TAG}", tags=["压机实时信息", ])
app.include_router(pictures_viewer_for_shuttle.router, prefix=f"/{pictures_viewer_for_shuttle.TAG}", tags=["穿梭小车相机图片回放", ])

@app.get("/", tags=["导航",])
async def root():
    file_path = os.path.join(BASE_DIR, "web/static", "guidance.html")
    return FileResponse(file_path, media_type="text/html")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host='0.0.0.0', port=8002)