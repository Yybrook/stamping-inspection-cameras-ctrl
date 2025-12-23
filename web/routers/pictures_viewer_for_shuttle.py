import os
# from datetime import datetime
from fastapi import APIRouter
from fastapi import Query, HTTPException
from fastapi.responses import FileResponse
from concurrent.futures import ThreadPoolExecutor
import asyncio
import aiofiles
import base64
from typing import Union, List, Tuple
from pathlib import Path
import logging
from database import ImageInfoDB
from datetime import date

_logger = logging.getLogger(__name__)

TAG = "picturesForShuttle"

# 定位到 web目录
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # app/ 的上一级目录
STATIC_DIR = os.path.join(BASE_DIR, "statics")

router = APIRouter()

BASE_DIR = r"D:\CapturedPicFromShuttle"
MOUNT_DIR = r"/pictures/forShuttle"


# 控制协程并发数量，防止内存爆炸
sem = asyncio.Semaphore(50)
executor = ThreadPoolExecutor(max_workers=50)


# @router.get("/")
# async def root():
#     return FileResponse("./static/pictures_for_shuttle.html", media_type="text/html")


@router.get("/{year}/{month}/{day}/{part_id}/{position}/count")
def get_part_count(year: int, month: int, day: int, part_id: int, position: str):
    if position != "first" and position != "last":
        raise HTTPException(status_code=404, detail=f"part count position[{position}] is illegal")
    elif position == "first":
        pos = 0
    else:
        pos = -1
    # 组合路径
    part_id_dir = join_part_id_dir(year, month, day, part_id)
    # 获取所有 part_counts
    part_counts = get_sorted_part_counts(part_id_dir)
    return {"part_count": part_counts[pos]}


@router.get("/{year}/{month}/{day}/{part_id}")
async def get_part_images(year: int, month: int, day: int, part_id: int, part_count: int):
    # 组合路径
    part_id_dir = join_part_id_dir(year, month, day, part_id)
    # 获取所有 part_counts
    part_counts = get_sorted_part_counts(part_id_dir)
    if part_count not in part_counts:
        raise HTTPException(status_code=404, detail=f"part count[{part_count}] out of range")

    # 按 part_count 快速查找文件夹
    part_count_dirs = {pc: part_id_dir / str(pc) for pc in part_counts}

    part_count_dir = part_count_dirs[part_count]

    # 获取图片并按 "00-00-00" 分级排序
    image_files = sorted(part_count_dir.glob("*.jpg"), key=lambda x: split_picture_name(x.stem))
    # if not image_files:
    #     raise HTTPException(status_code=404, detail=f"get null images")

    images_url = [
        f"{MOUNT_DIR}/{year}/{month:02d}/{day:02d}/{part_id}/{part_count}/{img.name}"
        for img in image_files
    ]

    # images_b64 = await encode_images_parallel_async(image_files)

    previous_part_count, next_part_count = get_previous_and_next(part_count=part_count, library=list(part_count_dirs.keys()))

    date_only = date(year, month, day)
    images_info, ranges_info = await sacn_all_image_info(date_only=date_only, part_id=part_id, part_count=part_count, image_files=image_files)

    return {
        "year": year,
        "month": month,
        "day": day,
        "part_id": part_id,
        "part_count": part_count,
        "previous": previous_part_count,
        "next": next_part_count,
        "first": part_counts[0],
        "last": part_counts[-1],
        "image_urls": images_url,
        "images_info": images_info,
        "ranges_info": ranges_info,
        # "images": images_b64,
    }


def join_part_id_dir(year: int, month: int, day: int, part_id: int) -> Path:
    """返回对应 part_id 的目录路径"""
    # part_id_dir = BASE_DIR / str(year) / f"{month:02d}" / f"{day:02d}" / str(part_id)
    part_id_dir = Path(os.path.join(BASE_DIR, str(year), f"{month:02d}", f"{day:02d}", str(part_id)))
    if not part_id_dir.exists():
        raise HTTPException(status_code=404, detail=f"part folder[{part_id_dir}] not found")
    return part_id_dir


def get_sorted_part_counts(part_id_dir: Path) -> List[int]:
    """返回目录下所有 part_count，已排序"""
    part_counts = sorted([int(f.name) for f in part_id_dir.iterdir() if f.is_dir()])
    if not part_counts:
        raise HTTPException(status_code=404, detail=f"part folder[{part_id_dir}] have null parts")
    return part_counts


def split_picture_name(file_stem: str) -> List[int]:
    try:
        # name_part = os.path.splitext(filename)[0]
        # 按 "-" 分割并转成整数
        return [int(part) for part in file_stem.split('-')]
    except Exception as err:
        _logger.warning(f"Failed to parse picture name[{file_stem}]: {err}")
        return [9999, 9999, 9999]  # 排在最后


def encode_image_2_base64_sync(image_path: Path) -> str:
    with open(image_path, "rb") as f:
        img_bytes = f.read()
    b64 = base64.b64encode(img_bytes).decode()
    return f"data:image/jpeg;base64,{b64}"


async def encode_image_2_base64_async(image_path: Path) -> str:
    async with sem:
        async with aiofiles.open(image_path, "rb") as f:
            img_bytes = await f.read()
    b64 = base64.b64encode(img_bytes).decode()
    return f"data:image/jpeg;base64,{b64}"


async def encode_images_parallel_async(image_files):
    # 并发异步处理所有图片
    tasks = [encode_image_2_base64_async(f) for f in image_files]
    return await asyncio.gather(*tasks)
    #
    # loop = asyncio.get_running_loop()
    # tasks = [loop.run_in_executor(executor, encode_image_to_base64_sync, f) for f in image_files]
    # return await asyncio.gather(*tasks)


def get_previous_and_next(part_count: int, library: List[int]) -> Tuple[int, int]:
    library = sorted(library)
    try:
        index = library.index(part_count)
    except ValueError:
        raise ValueError(f"part count [{part_count}] not in library")

    previous_count = library[index - 1] if index > 0 else -1
    next_count = library[index + 1] if index < len(library) - 1 else -1
    return previous_count, next_count


def sacn_one_image_info(date_only: date, part_id: int, part_count: int, image_name: str) -> Tuple[dict, dict]:
    with ImageInfoDB() as db:
        self_info = db.locate_image(part_id=part_id, part_count=part_count, date_only=date_only, image_name=image_name)
        if self_info is None:
            # raise ValueError(f"scan none for part[id={part_id}, count={part_count}, date={date_only}]")
            return dict(), dict()
        part_counts = db.get_part_counts_range(part_id=part_id, part_count=part_count, date_only=date_only, image_name=image_name, range_len=50)
        range_info = db.locate_images(part_id=part_id, part_counts=part_counts, date_only=date_only, image_name=image_name)

    return self_info, range_info


async def sacn_one_image_info_async(date_only: date, part_id: int, part_count: int, image: Path):
    return await asyncio.to_thread(
        sacn_one_image_info,
        date_only=date_only,
        part_id=part_id,
        part_count=part_count,
        image_name=image.name
    )


async def sacn_all_image_info(date_only: date, part_id: int, part_count: int, image_files: list):
    results = await asyncio.gather(
        *(sacn_one_image_info_async(
            date_only=date_only,
            part_id=part_id,
            part_count=part_count,
            image=img
        ) for img in image_files)
    )
    images_info, ranges_info = zip(*results) if results else (list(), list())
    return list(images_info), list(ranges_info)


"""
{
  "year": 2025,
  "month": 7,
  "day": 12,
  "part_id": 13,
  "part_count": 555,
  "previous": -1,
  "next": 556,
  "first": 555,
  "last": 846,
  "image_urls": [
    "/pictures/forShuttle/2025/07/12/13/555/00-01-00.jpg",
    "/pictures/forShuttle/2025/07/12/13/555/00-02-00.jpg",
    "/pictures/forShuttle/2025/07/12/13/555/00-05-00.jpg"
  ],
  "images_info": [
    {
      "id": 1004,
      "saved_t": 1752287084450,
      "camera_ip": "192.168.4.101",
      "camera_user_id": "01",
      "frame_num": 8,
      "frame_t": 1752287082270,
      "shuttle_has_part_t": 1752287081957,
      "frame_width": 2448,
      "frame_height": 2048
    },
    {
      "id": 1003,
      "saved_t": 1752287084433,
      "camera_ip": "192.168.4.102",
      "camera_user_id": "02",
      "frame_num": 8,
      "frame_t": 1752287082271,
      "shuttle_has_part_t": 1752287081957,
      "frame_width": 2448,
      "frame_height": 2048
    },
    {
      "id": 1002,
      "saved_t": 1752287083347,
      "camera_ip": "192.168.4.105",
      "camera_user_id": "05",
      "frame_num": 8,
      "frame_t": 1752287082271,
      "shuttle_has_part_t": 1752287081957,
      "frame_width": 2448,
      "frame_height": 2048
    }
  ],
  "ranges_info": [
    {
      "555": {
        "saved_t": 1752287084450,
        "frame_t": 1752287082270,
        "shuttle_has_part_t": 1752287081957
      },
      "556": {
        "saved_t": 1752287088380,
        "frame_t": 1752287087276,
        "shuttle_has_part_t": 1752287086964
      },
      "557": {
        "saved_t": 1752287093380,
        "frame_t": 1752287092268,
        "shuttle_has_part_t": 1752287091955
      },
      "558": {
        "saved_t": 1752287098250,
        "frame_t": 1752287097271,
        "shuttle_has_part_t": 1752287096958
      },
      "559": {
        "saved_t": 1752287103383,
        "frame_t": 1752287102273,
        "shuttle_has_part_t": 1752287101962
      },
      "560": {
        "saved_t": 1752287108280,
        "frame_t": 1752287107273,
        "shuttle_has_part_t": 1752287106962
      },
      "561": {
        "saved_t": 1752287113313,
        "frame_t": 1752287112264,
        "shuttle_has_part_t": 1752287111953
      },
      "562": {
        "saved_t": 1752287118350,
        "frame_t": 1752287117271,
        "shuttle_has_part_t": 1752287116960
      },
      "563": {
        "saved_t": 1752287123333,
        "frame_t": 1752287122271,
        "shuttle_has_part_t": 1752287121957
      },
      "564": {
        "saved_t": 1752287128110,
        "frame_t": 1752287127270,
        "shuttle_has_part_t": 1752287126956
      },
      "565": {
        "saved_t": 1752287133077,
        "frame_t": 1752287132269,
        "shuttle_has_part_t": 1752287131957
      },
      "566": {
        "saved_t": 1752287138340,
        "frame_t": 1752287137264,
        "shuttle_has_part_t": 1752287136952
      },
      "567": {
        "saved_t": 1752287143443,
        "frame_t": 1752287142276,
        "shuttle_has_part_t": 1752287141964
      },
      "568": {
        "saved_t": 1752287148270,
        "frame_t": 1752287147270,
        "shuttle_has_part_t": 1752287146957
      },
      "569": {
        "saved_t": 1752287153327,
        "frame_t": 1752287152269,
        "shuttle_has_part_t": 1752287151956
      },
      "570": {
        "saved_t": 1752287158313,
        "frame_t": 1752287157279,
        "shuttle_has_part_t": 1752287156965
      },
      "571": {
        "saved_t": 1752287163250,
        "frame_t": 1752287162267,
        "shuttle_has_part_t": 1752287161956
      },
      "577": {
        "saved_t": 1752287195033,
        "frame_t": 1752287194120,
        "shuttle_has_part_t": 1752287193603
      },
      "578": {
        "saved_t": 1752287198573,
        "frame_t": 1752287197498,
        "shuttle_has_part_t": 1752287196969
      },
      "579": {
        "saved_t": 1752287203350,
        "frame_t": 1752287202494,
        "shuttle_has_part_t": 1752287201968
      },
      "580": {
        "saved_t": 1752287208427,
        "frame_t": 1752287207477,
        "shuttle_has_part_t": 1752287206965
      },
      "581": {
        "saved_t": 1752287213547,
        "frame_t": 1752287212490,
        "shuttle_has_part_t": 1752287211969
      },
      "582": {
        "saved_t": 1752287218653,
        "frame_t": 1752287217481,
        "shuttle_has_part_t": 1752287216964
      },
      "583": {
        "saved_t": 1752287223477,
        "frame_t": 1752287222473,
        "shuttle_has_part_t": 1752287221960
      },
      "584": {
        "saved_t": 1752287228540,
        "frame_t": 1752287227476,
        "shuttle_has_part_t": 1752287226965
      },
      "585": {
        "saved_t": 1752287233547,
        "frame_t": 1752287232469,
        "shuttle_has_part_t": 1752287231958
      },
      "586": {
        "saved_t": 1752287238343,
        "frame_t": 1752287237476,
        "shuttle_has_part_t": 1752287236963
      },
      "587": {
        "saved_t": 1752287243477,
        "frame_t": 1752287242472,
        "shuttle_has_part_t": 1752287241962
      },
      "588": {
        "saved_t": 1752287248490,
        "frame_t": 1752287247476,
        "shuttle_has_part_t": 1752287246961
      },
      "589": {
        "saved_t": 1752287253533,
        "frame_t": 1752287252470,
        "shuttle_has_part_t": 1752287251961
      },
      "599": {
        "saved_t": 1752287303393,
        "frame_t": 1752287302380,
        "shuttle_has_part_t": 1752287301958
      },
      "600": {
        "saved_t": 1752287308373,
        "frame_t": 1752287307384,
        "shuttle_has_part_t": 1752287306966
      },
      "601": {
        "saved_t": 1752287313443,
        "frame_t": 1752287312386,
        "shuttle_has_part_t": 1752287311971
      },
      "602": {
        "saved_t": 1752287318440,
        "frame_t": 1752287317394,
        "shuttle_has_part_t": 1752287316966
      },
      "603": {
        "saved_t": 1752287323380,
        "frame_t": 1752287322400,
        "shuttle_has_part_t": 1752287321977
      },
      "604": {
        "saved_t": 1752287328393,
        "frame_t": 1752287327404,
        "shuttle_has_part_t": 1752287326973
      },
      "605": {
        "saved_t": 1752287333397,
        "frame_t": 1752287332384,
        "shuttle_has_part_t": 1752287331962
      },
      "606": {
        "saved_t": 1752287338477,
        "frame_t": 1752287337401,
        "shuttle_has_part_t": 1752287336971
      },
      "607": {
        "saved_t": 1752287343467,
        "frame_t": 1752287342400,
        "shuttle_has_part_t": 1752287341966
      },
      "608": {
        "saved_t": 1752287348480,
        "frame_t": 1752287347409,
        "shuttle_has_part_t": 1752287346980
      },
      "609": {
        "saved_t": 1752287353477,
        "frame_t": 1752287352410,
        "shuttle_has_part_t": 1752287351976
      },
      "610": {
        "saved_t": 1752287358427,
        "frame_t": 1752287357391,
        "shuttle_has_part_t": 1752287356972
      },
      "611": {
        "saved_t": 1752287363387,
        "frame_t": 1752287362395,
        "shuttle_has_part_t": 1752287361965
      },
      "612": {
        "saved_t": 1752287368420,
        "frame_t": 1752287367401,
        "shuttle_has_part_t": 1752287366975
      },
      "613": {
        "saved_t": 1752287373470,
        "frame_t": 1752287372410,
        "shuttle_has_part_t": 1752287371971
      },
      "614": {
        "saved_t": 1752287378530,
        "frame_t": 1752287377397,
        "shuttle_has_part_t": 1752287376964
      },
      "615": {
        "saved_t": 1752287383450,
        "frame_t": 1752287382405,
        "shuttle_has_part_t": 1752287381980
      },
      "616": {
        "saved_t": 1752287388423,
        "frame_t": 1752287387400,
        "shuttle_has_part_t": 1752287386968
      },
      "617": {
        "saved_t": 1752287393517,
        "frame_t": 1752287392426,
        "shuttle_has_part_t": 1752287391991
      },
      "618": {
        "saved_t": 1752287398437,
        "frame_t": 1752287397424,
        "shuttle_has_part_t": 1752287396978
      },
      "619": {
        "saved_t": 1752287403527,
        "frame_t": 1752287402386,
        "shuttle_has_part_t": 1752287401968
      },
      "620": {
        "saved_t": 1752287408503,
        "frame_t": 1752287407428,
        "shuttle_has_part_t": 1752287406980
      },
      "621": {
        "saved_t": 1752287413380,
        "frame_t": 1752287412407,
        "shuttle_has_part_t": 1752287411973
      },
      "622": {
        "saved_t": 1752287418490,
        "frame_t": 1752287417417,
        "shuttle_has_part_t": 1752287416988
      },
      "623": {
        "saved_t": 1752287423483,
        "frame_t": 1752287422397,
        "shuttle_has_part_t": 1752287421971
      },
      "624": {
        "saved_t": 1752287428450,
        "frame_t": 1752287427410,
        "shuttle_has_part_t": 1752287426981
      },
      "625": {
        "saved_t": 1752287433560,
        "frame_t": 1752287432392,
        "shuttle_has_part_t": 1752287431973
      },
      "626": {
        "saved_t": 1752287438160,
        "frame_t": 1752287437395,
        "shuttle_has_part_t": 1752287436973
      },
      "627": {
        "saved_t": 1752287443463,
        "frame_t": 1752287442393,
        "shuttle_has_part_t": 1752287441974
      },
      "628": {
        "saved_t": 1752287448473,
        "frame_t": 1752287447390,
        "shuttle_has_part_t": 1752287446974
      },
      "629": {
        "saved_t": 1752287453493,
        "frame_t": 1752287452377,
        "shuttle_has_part_t": 1752287451963
      },
      "630": {
        "saved_t": 1752287458420,
        "frame_t": 1752287457377,
        "shuttle_has_part_t": 1752287456962
      },
      "631": {
        "saved_t": 1752287463447,
        "frame_t": 1752287462381,
        "shuttle_has_part_t": 1752287461967
      },
      "632": {
        "saved_t": 1752287468117,
        "frame_t": 1752287467384,
        "shuttle_has_part_t": 1752287466969
      },
      "633": {
        "saved_t": 1752287473423,
        "frame_t": 1752287472379,
        "shuttle_has_part_t": 1752287471962
      },
      "634": {
        "saved_t": 1752287478420,
        "frame_t": 1752287477381,
        "shuttle_has_part_t": 1752287476964
      },
      "635": {
        "saved_t": 1752287483497,
        "frame_t": 1752287482387,
        "shuttle_has_part_t": 1752287481972
      },
      "636": {
        "saved_t": 1752287488383,
        "frame_t": 1752287487379,
        "shuttle_has_part_t": 1752287486964
      },
      "637": {
        "saved_t": 1752287493503,
        "frame_t": 1752287492400,
        "shuttle_has_part_t": 1752287491973
      },
      "638": {
        "saved_t": 1752287498317,
        "frame_t": 1752287497389,
        "shuttle_has_part_t": 1752287496972
      },
      "639": {
        "saved_t": 1752287503470,
        "frame_t": 1752287502389,
        "shuttle_has_part_t": 1752287501974
      },
      "640": {
        "saved_t": 1752287508407,
        "frame_t": 1752287507391,
        "shuttle_has_part_t": 1752287506974
      },
      "641": {
        "saved_t": 1752287513167,
        "frame_t": 1752287512383,
        "shuttle_has_part_t": 1752287511968
      },
      "642": {
        "saved_t": 1752287518523,
        "frame_t": 1752287517385,
        "shuttle_has_part_t": 1752287516969
      },
      "643": {
        "saved_t": 1752287523373,
        "frame_t": 1752287522383,
        "shuttle_has_part_t": 1752287521969
      },
      "644": {
        "saved_t": 1752287528430,
        "frame_t": 1752287527378,
        "shuttle_has_part_t": 1752287526959
      },
      "645": {
        "saved_t": 1752287533360,
        "frame_t": 1752287532383,
        "shuttle_has_part_t": 1752287531968
      },
      "646": {
        "saved_t": 1752287538713,
        "frame_t": 1752287537380,
        "shuttle_has_part_t": 1752287536961
      },
      "647": {
        "saved_t": 1752287543403,
        "frame_t": 1752287542381,
        "shuttle_has_part_t": 1752287541967
      },
      "648": {
        "saved_t": 1752287548497,
        "frame_t": 1752287547399,
        "shuttle_has_part_t": 1752287546980
      },
      "649": {
        "saved_t": 1752287553400,
        "frame_t": 1752287552387,
        "shuttle_has_part_t": 1752287551969
      },
      "650": {
        "saved_t": 1752287558410,
        "frame_t": 1752287557383,
        "shuttle_has_part_t": 1752287556966
      },
      "651": {
        "saved_t": 1752287563430,
        "frame_t": 1752287562384,
        "shuttle_has_part_t": 1752287561968
      },
      "652": {
        "saved_t": 1752287568397,
        "frame_t": 1752287567382,
        "shuttle_has_part_t": 1752287566966
      },
      "653": {
        "saved_t": 1752287573763,
        "frame_t": 1752287572379,
        "shuttle_has_part_t": 1752287571963
      },
      "654": {
        "saved_t": 1752287578383,
        "frame_t": 1752287577383,
        "shuttle_has_part_t": 1752287576967
      },
      "655": {
        "saved_t": 1752287583437,
        "frame_t": 1752287582388,
        "shuttle_has_part_t": 1752287581973
      },
      "656": {
        "saved_t": 1752287588457,
        "frame_t": 1752287587388,
        "shuttle_has_part_t": 1752287586972
      },
      "657": {
        "saved_t": 1752287593273,
        "frame_t": 1752287592384,
        "shuttle_has_part_t": 1752287591969
      },
      "658": {
        "saved_t": 1752287598353,
        "frame_t": 1752287597388,
        "shuttle_has_part_t": 1752287596970
      },
      "659": {
        "saved_t": 1752287603453,
        "frame_t": 1752287602382,
        "shuttle_has_part_t": 1752287601968
      },
      "660": {
        "saved_t": 1752287608427,
        "frame_t": 1752287607392,
        "shuttle_has_part_t": 1752287606973
      },
      "661": {
        "saved_t": 1752287613377,
        "frame_t": 1752287612390,
        "shuttle_has_part_t": 1752287611973
      },
      "662": {
        "saved_t": 1752287618523,
        "frame_t": 1752287617389,
        "shuttle_has_part_t": 1752287616972
      },
      "663": {
        "saved_t": 1752287623187,
        "frame_t": 1752287622386,
        "shuttle_has_part_t": 1752287621970
      },
      "664": {
        "saved_t": 1752287628420,
        "frame_t": 1752287627407,
        "shuttle_has_part_t": 1752287626984
      },
      "665": {
        "saved_t": 1752287633467,
        "frame_t": 1752287632380,
        "shuttle_has_part_t": 1752287631964
      },
      "666": {
        "saved_t": 1752287638540,
        "frame_t": 1752287637393,
        "shuttle_has_part_t": 1752287636973
      },
      "667": {
        "saved_t": 1752287643120,
        "frame_t": 1752287642388,
        "shuttle_has_part_t": 1752287641975
      },
      "668": {
        "saved_t": 1752287648120,
        "frame_t": 1752287647390,
        "shuttle_has_part_t": 1752287646974
      }
    },
    {
      "555": {
        "saved_t": 1752287084433,
        "frame_t": 1752287082271,
        "shuttle_has_part_t": 1752287081957
      },
      "556": {
        "saved_t": 1752287088380,
        "frame_t": 1752287087276,
        "shuttle_has_part_t": 1752287086964
      },
      "557": {
        "saved_t": 1752287093363,
        "frame_t": 1752287092268,
        "shuttle_has_part_t": 1752287091955
      },
      "558": {
        "saved_t": 1752287098237,
        "frame_t": 1752287097272,
        "shuttle_has_part_t": 1752287096958
      },
      "559": {
        "saved_t": 1752287103367,
        "frame_t": 1752287102274,
        "shuttle_has_part_t": 1752287101962
      },
      "560": {
        "saved_t": 1752287108310,
        "frame_t": 1752287107273,
        "shuttle_has_part_t": 1752287106962
      },
      "561": {
        "saved_t": 1752287113313,
        "frame_t": 1752287112265,
        "shuttle_has_part_t": 1752287111953
      },
      "562": {
        "saved_t": 1752287118317,
        "frame_t": 1752287117272,
        "shuttle_has_part_t": 1752287116960
      },
      "563": {
        "saved_t": 1752287123317,
        "frame_t": 1752287122271,
        "shuttle_has_part_t": 1752287121957
      },
      "564": {
        "saved_t": 1752287128110,
        "frame_t": 1752287127270,
        "shuttle_has_part_t": 1752287126956
      },
      "565": {
        "saved_t": 1752287133077,
        "frame_t": 1752287132269,
        "shuttle_has_part_t": 1752287131957
      },
      "566": {
        "saved_t": 1752287138307,
        "frame_t": 1752287137264,
        "shuttle_has_part_t": 1752287136952
      },
      "567": {
        "saved_t": 1752287143493,
        "frame_t": 1752287142277,
        "shuttle_has_part_t": 1752287141964
      },
      "568": {
        "saved_t": 1752287148327,
        "frame_t": 1752287147270,
        "shuttle_has_part_t": 1752287146957
      },
      "569": {
        "saved_t": 1752287153353,
        "frame_t": 1752287152269,
        "shuttle_has_part_t": 1752287151956
      },
      "570": {
        "saved_t": 1752287158280,
        "frame_t": 1752287157281,
        "shuttle_has_part_t": 1752287156965
      },
      "571": {
        "saved_t": 1752287163330,
        "frame_t": 1752287162267,
        "shuttle_has_part_t": 1752287161956
      },
      "577": {
        "saved_t": 1752287195000,
        "frame_t": 1752287194120,
        "shuttle_has_part_t": 1752287193603
      },
      "578": {
        "saved_t": 1752287198567,
        "frame_t": 1752287197498,
        "shuttle_has_part_t": 1752287196969
      },
      "579": {
        "saved_t": 1752287203367,
        "frame_t": 1752287202496,
        "shuttle_has_part_t": 1752287201968
      },
      "580": {
        "saved_t": 1752287208443,
        "frame_t": 1752287207477,
        "shuttle_has_part_t": 1752287206965
      },
      "581": {
        "saved_t": 1752287213513,
        "frame_t": 1752287212490,
        "shuttle_has_part_t": 1752287211969
      },
      "582": {
        "saved_t": 1752287218620,
        "frame_t": 1752287217481,
        "shuttle_has_part_t": 1752287216964
      },
      "583": {
        "saved_t": 1752287223490,
        "frame_t": 1752287222473,
        "shuttle_has_part_t": 1752287221960
      },
      "584": {
        "saved_t": 1752287228517,
        "frame_t": 1752287227477,
        "shuttle_has_part_t": 1752287226965
      },
      "585": {
        "saved_t": 1752287233567,
        "frame_t": 1752287232470,
        "shuttle_has_part_t": 1752287231958
      },
      "586": {
        "saved_t": 1752287238327,
        "frame_t": 1752287237476,
        "shuttle_has_part_t": 1752287236963
      },
      "587": {
        "saved_t": 1752287243450,
        "frame_t": 1752287242472,
        "shuttle_has_part_t": 1752287241962
      },
      "588": {
        "saved_t": 1752287248487,
        "frame_t": 1752287247476,
        "shuttle_has_part_t": 1752287246961
      },
      "589": {
        "saved_t": 1752287253553,
        "frame_t": 1752287252470,
        "shuttle_has_part_t": 1752287251961
      },
      "599": {
        "saved_t": 1752287303423,
        "frame_t": 1752287302379,
        "shuttle_has_part_t": 1752287301958
      },
      "600": {
        "saved_t": 1752287308327,
        "frame_t": 1752287307384,
        "shuttle_has_part_t": 1752287306966
      },
      "601": {
        "saved_t": 1752287313443,
        "frame_t": 1752287312386,
        "shuttle_has_part_t": 1752287311971
      },
      "602": {
        "saved_t": 1752287318440,
        "frame_t": 1752287317394,
        "shuttle_has_part_t": 1752287316966
      },
      "603": {
        "saved_t": 1752287323407,
        "frame_t": 1752287322400,
        "shuttle_has_part_t": 1752287321977
      },
      "604": {
        "saved_t": 1752287328437,
        "frame_t": 1752287327404,
        "shuttle_has_part_t": 1752287326973
      },
      "605": {
        "saved_t": 1752287333410,
        "frame_t": 1752287332384,
        "shuttle_has_part_t": 1752287331962
      },
      "606": {
        "saved_t": 1752287338440,
        "frame_t": 1752287337401,
        "shuttle_has_part_t": 1752287336971
      },
      "607": {
        "saved_t": 1752287343417,
        "frame_t": 1752287342400,
        "shuttle_has_part_t": 1752287341966
      },
      "608": {
        "saved_t": 1752287348457,
        "frame_t": 1752287347409,
        "shuttle_has_part_t": 1752287346980
      },
      "609": {
        "saved_t": 1752287353460,
        "frame_t": 1752287352410,
        "shuttle_has_part_t": 1752287351976
      },
      "610": {
        "saved_t": 1752287358427,
        "frame_t": 1752287357391,
        "shuttle_has_part_t": 1752287356972
      },
      "611": {
        "saved_t": 1752287363333,
        "frame_t": 1752287362395,
        "shuttle_has_part_t": 1752287361965
      },
      "612": {
        "saved_t": 1752287368413,
        "frame_t": 1752287367401,
        "shuttle_has_part_t": 1752287366975
      },
      "613": {
        "saved_t": 1752287373487,
        "frame_t": 1752287372410,
        "shuttle_has_part_t": 1752287371971
      },
      "614": {
        "saved_t": 1752287378530,
        "frame_t": 1752287377397,
        "shuttle_has_part_t": 1752287376964
      },
      "615": {
        "saved_t": 1752287383420,
        "frame_t": 1752287382405,
        "shuttle_has_part_t": 1752287381980
      },
      "616": {
        "saved_t": 1752287388437,
        "frame_t": 1752287387400,
        "shuttle_has_part_t": 1752287386968
      },
      "617": {
        "saved_t": 1752287393517,
        "frame_t": 1752287392426,
        "shuttle_has_part_t": 1752287391991
      },
      "618": {
        "saved_t": 1752287398480,
        "frame_t": 1752287397424,
        "shuttle_has_part_t": 1752287396978
      },
      "619": {
        "saved_t": 1752287403490,
        "frame_t": 1752287402386,
        "shuttle_has_part_t": 1752287401968
      },
      "620": {
        "saved_t": 1752287408517,
        "frame_t": 1752287407428,
        "shuttle_has_part_t": 1752287406980
      },
      "621": {
        "saved_t": 1752287413407,
        "frame_t": 1752287412407,
        "shuttle_has_part_t": 1752287411973
      },
      "622": {
        "saved_t": 1752287418457,
        "frame_t": 1752287417417,
        "shuttle_has_part_t": 1752287416988
      },
      "623": {
        "saved_t": 1752287423483,
        "frame_t": 1752287422397,
        "shuttle_has_part_t": 1752287421971
      },
      "624": {
        "saved_t": 1752287428450,
        "frame_t": 1752287427410,
        "shuttle_has_part_t": 1752287426981
      },
      "625": {
        "saved_t": 1752287433567,
        "frame_t": 1752287432392,
        "shuttle_has_part_t": 1752287431973
      },
      "626": {
        "saved_t": 1752287438160,
        "frame_t": 1752287437395,
        "shuttle_has_part_t": 1752287436973
      },
      "627": {
        "saved_t": 1752287443463,
        "frame_t": 1752287442393,
        "shuttle_has_part_t": 1752287441974
      },
      "628": {
        "saved_t": 1752287448473,
        "frame_t": 1752287447390,
        "shuttle_has_part_t": 1752287446974
      },
      "629": {
        "saved_t": 1752287453473,
        "frame_t": 1752287452378,
        "shuttle_has_part_t": 1752287451963
      },
      "630": {
        "saved_t": 1752287458373,
        "frame_t": 1752287457377,
        "shuttle_has_part_t": 1752287456962
      },
      "631": {
        "saved_t": 1752287463447,
        "frame_t": 1752287462381,
        "shuttle_has_part_t": 1752287461967
      },
      "632": {
        "saved_t": 1752287468117,
        "frame_t": 1752287467384,
        "shuttle_has_part_t": 1752287466969
      },
      "633": {
        "saved_t": 1752287473440,
        "frame_t": 1752287472379,
        "shuttle_has_part_t": 1752287471962
      },
      "634": {
        "saved_t": 1752287478340,
        "frame_t": 1752287477381,
        "shuttle_has_part_t": 1752287476964
      },
      "635": {
        "saved_t": 1752287483467,
        "frame_t": 1752287482387,
        "shuttle_has_part_t": 1752287481972
      },
      "636": {
        "saved_t": 1752287488397,
        "frame_t": 1752287487379,
        "shuttle_has_part_t": 1752287486964
      },
      "637": {
        "saved_t": 1752287493503,
        "frame_t": 1752287492400,
        "shuttle_has_part_t": 1752287491973
      },
      "638": {
        "saved_t": 1752287498300,
        "frame_t": 1752287497389,
        "shuttle_has_part_t": 1752287496972
      },
      "639": {
        "saved_t": 1752287503443,
        "frame_t": 1752287502389,
        "shuttle_has_part_t": 1752287501974
      },
      "640": {
        "saved_t": 1752287508377,
        "frame_t": 1752287507391,
        "shuttle_has_part_t": 1752287506974
      },
      "641": {
        "saved_t": 1752287513167,
        "frame_t": 1752287512383,
        "shuttle_has_part_t": 1752287511968
      },
      "642": {
        "saved_t": 1752287518507,
        "frame_t": 1752287517385,
        "shuttle_has_part_t": 1752287516969
      },
      "643": {
        "saved_t": 1752287523343,
        "frame_t": 1752287522383,
        "shuttle_has_part_t": 1752287521969
      },
      "644": {
        "saved_t": 1752287528413,
        "frame_t": 1752287527378,
        "shuttle_has_part_t": 1752287526959
      },
      "645": {
        "saved_t": 1752287533393,
        "frame_t": 1752287532383,
        "shuttle_has_part_t": 1752287531968
      },
      "646": {
        "saved_t": 1752287538713,
        "frame_t": 1752287537380,
        "shuttle_has_part_t": 1752287536961
      },
      "647": {
        "saved_t": 1752287543403,
        "frame_t": 1752287542381,
        "shuttle_has_part_t": 1752287541967
      },
      "648": {
        "saved_t": 1752287548497,
        "frame_t": 1752287547399,
        "shuttle_has_part_t": 1752287546980
      },
      "649": {
        "saved_t": 1752287553400,
        "frame_t": 1752287552386,
        "shuttle_has_part_t": 1752287551969
      },
      "650": {
        "saved_t": 1752287558410,
        "frame_t": 1752287557383,
        "shuttle_has_part_t": 1752287556966
      },
      "651": {
        "saved_t": 1752287563430,
        "frame_t": 1752287562384,
        "shuttle_has_part_t": 1752287561968
      },
      "652": {
        "saved_t": 1752287568397,
        "frame_t": 1752287567382,
        "shuttle_has_part_t": 1752287566966
      },
      "653": {
        "saved_t": 1752287573747,
        "frame_t": 1752287572380,
        "shuttle_has_part_t": 1752287571963
      },
      "654": {
        "saved_t": 1752287578383,
        "frame_t": 1752287577383,
        "shuttle_has_part_t": 1752287576967
      },
      "655": {
        "saved_t": 1752287583420,
        "frame_t": 1752287582388,
        "shuttle_has_part_t": 1752287581973
      },
      "656": {
        "saved_t": 1752287588467,
        "frame_t": 1752287587388,
        "shuttle_has_part_t": 1752287586972
      },
      "657": {
        "saved_t": 1752287593317,
        "frame_t": 1752287592384,
        "shuttle_has_part_t": 1752287591969
      },
      "658": {
        "saved_t": 1752287598353,
        "frame_t": 1752287597389,
        "shuttle_has_part_t": 1752287596970
      },
      "659": {
        "saved_t": 1752287603453,
        "frame_t": 1752287602382,
        "shuttle_has_part_t": 1752287601968
      },
      "660": {
        "saved_t": 1752287608427,
        "frame_t": 1752287607392,
        "shuttle_has_part_t": 1752287606973
      },
      "661": {
        "saved_t": 1752287613360,
        "frame_t": 1752287612390,
        "shuttle_has_part_t": 1752287611973
      },
      "662": {
        "saved_t": 1752287618533,
        "frame_t": 1752287617389,
        "shuttle_has_part_t": 1752287616972
      },
      "663": {
        "saved_t": 1752287623213,
        "frame_t": 1752287622387,
        "shuttle_has_part_t": 1752287621970
      },
      "664": {
        "saved_t": 1752287628420,
        "frame_t": 1752287627407,
        "shuttle_has_part_t": 1752287626984
      },
      "665": {
        "saved_t": 1752287633450,
        "frame_t": 1752287632380,
        "shuttle_has_part_t": 1752287631964
      },
      "666": {
        "saved_t": 1752287638523,
        "frame_t": 1752287637393,
        "shuttle_has_part_t": 1752287636973
      },
      "667": {
        "saved_t": 1752287643120,
        "frame_t": 1752287642388,
        "shuttle_has_part_t": 1752287641975
      },
      "668": {
        "saved_t": 1752287648133,
        "frame_t": 1752287647390,
        "shuttle_has_part_t": 1752287646974
      }
    },
    {
      "555": {
        "saved_t": 1752287083347,
        "frame_t": 1752287082271,
        "shuttle_has_part_t": 1752287081957
      },
      "556": {
        "saved_t": 1752287088363,
        "frame_t": 1752287087276,
        "shuttle_has_part_t": 1752287086964
      },
      "557": {
        "saved_t": 1752287093333,
        "frame_t": 1752287092268,
        "shuttle_has_part_t": 1752287091955
      },
      "558": {
        "saved_t": 1752287098250,
        "frame_t": 1752287097272,
        "shuttle_has_part_t": 1752287096958
      },
      "559": {
        "saved_t": 1752287103337,
        "frame_t": 1752287102273,
        "shuttle_has_part_t": 1752287101962
      },
      "560": {
        "saved_t": 1752287108310,
        "frame_t": 1752287107273,
        "shuttle_has_part_t": 1752287106962
      },
      "561": {
        "saved_t": 1752287113297,
        "frame_t": 1752287112264,
        "shuttle_has_part_t": 1752287111953
      },
      "562": {
        "saved_t": 1752287118350,
        "frame_t": 1752287117272,
        "shuttle_has_part_t": 1752287116960
      },
      "563": {
        "saved_t": 1752287123300,
        "frame_t": 1752287122271,
        "shuttle_has_part_t": 1752287121957
      },
      "564": {
        "saved_t": 1752287128093,
        "frame_t": 1752287127270,
        "shuttle_has_part_t": 1752287126956
      },
      "565": {
        "saved_t": 1752287133077,
        "frame_t": 1752287132269,
        "shuttle_has_part_t": 1752287131957
      },
      "566": {
        "saved_t": 1752287138383,
        "frame_t": 1752287137264,
        "shuttle_has_part_t": 1752287136952
      },
      "567": {
        "saved_t": 1752287143410,
        "frame_t": 1752287142276,
        "shuttle_has_part_t": 1752287141964
      },
      "568": {
        "saved_t": 1752287148280,
        "frame_t": 1752287147270,
        "shuttle_has_part_t": 1752287146957
      },
      "569": {
        "saved_t": 1752287153303,
        "frame_t": 1752287152269,
        "shuttle_has_part_t": 1752287151956
      },
      "570": {
        "saved_t": 1752287158313,
        "frame_t": 1752287157281,
        "shuttle_has_part_t": 1752287156965
      },
      "571": {
        "saved_t": 1752287163220,
        "frame_t": 1752287162267,
        "shuttle_has_part_t": 1752287161956
      },
      "577": {
        "saved_t": 1752287195017,
        "frame_t": 1752287194120,
        "shuttle_has_part_t": 1752287193603
      },
      "578": {
        "saved_t": 1752287198530,
        "frame_t": 1752287197498,
        "shuttle_has_part_t": 1752287196969
      },
      "579": {
        "saved_t": 1752287203317,
        "frame_t": 1752287202494,
        "shuttle_has_part_t": 1752287201968
      },
      "580": {
        "saved_t": 1752287208443,
        "frame_t": 1752287207476,
        "shuttle_has_part_t": 1752287206965
      },
      "581": {
        "saved_t": 1752287213470,
        "frame_t": 1752287212490,
        "shuttle_has_part_t": 1752287211969
      },
      "582": {
        "saved_t": 1752287218590,
        "frame_t": 1752287217481,
        "shuttle_has_part_t": 1752287216964
      },
      "583": {
        "saved_t": 1752287223507,
        "frame_t": 1752287222473,
        "shuttle_has_part_t": 1752287221960
      },
      "584": {
        "saved_t": 1752287228523,
        "frame_t": 1752287227476,
        "shuttle_has_part_t": 1752287226965
      },
      "585": {
        "saved_t": 1752287233547,
        "frame_t": 1752287232469,
        "shuttle_has_part_t": 1752287231958
      },
      "586": {
        "saved_t": 1752287238297,
        "frame_t": 1752287237476,
        "shuttle_has_part_t": 1752287236963
      },
      "587": {
        "saved_t": 1752287243430,
        "frame_t": 1752287242472,
        "shuttle_has_part_t": 1752287241962
      },
      "588": {
        "saved_t": 1752287248497,
        "frame_t": 1752287247476,
        "shuttle_has_part_t": 1752287246961
      },
      "589": {
        "saved_t": 1752287253500,
        "frame_t": 1752287252470,
        "shuttle_has_part_t": 1752287251961
      },
      "599": {
        "saved_t": 1752287303413,
        "frame_t": 1752287302379,
        "shuttle_has_part_t": 1752287301958
      },
      "600": {
        "saved_t": 1752287308300,
        "frame_t": 1752287307384,
        "shuttle_has_part_t": 1752287306966
      },
      "601": {
        "saved_t": 1752287313417,
        "frame_t": 1752287312386,
        "shuttle_has_part_t": 1752287311971
      },
      "602": {
        "saved_t": 1752287318393,
        "frame_t": 1752287317397,
        "shuttle_has_part_t": 1752287316966
      },
      "603": {
        "saved_t": 1752287323340,
        "frame_t": 1752287322400,
        "shuttle_has_part_t": 1752287321977
      },
      "604": {
        "saved_t": 1752287328437,
        "frame_t": 1752287327404,
        "shuttle_has_part_t": 1752287326973
      },
      "605": {
        "saved_t": 1752287333410,
        "frame_t": 1752287332384,
        "shuttle_has_part_t": 1752287331962
      },
      "606": {
        "saved_t": 1752287338477,
        "frame_t": 1752287337401,
        "shuttle_has_part_t": 1752287336971
      },
      "607": {
        "saved_t": 1752287343450,
        "frame_t": 1752287342400,
        "shuttle_has_part_t": 1752287341966
      },
      "608": {
        "saved_t": 1752287348457,
        "frame_t": 1752287347409,
        "shuttle_has_part_t": 1752287346980
      },
      "609": {
        "saved_t": 1752287353470,
        "frame_t": 1752287352410,
        "shuttle_has_part_t": 1752287351976
      },
      "610": {
        "saved_t": 1752287358427,
        "frame_t": 1752287357391,
        "shuttle_has_part_t": 1752287356972
      },
      "611": {
        "saved_t": 1752287363357,
        "frame_t": 1752287362395,
        "shuttle_has_part_t": 1752287361965
      },
      "612": {
        "saved_t": 1752287368413,
        "frame_t": 1752287367401,
        "shuttle_has_part_t": 1752287366975
      },
      "613": {
        "saved_t": 1752287373487,
        "frame_t": 1752287372410,
        "shuttle_has_part_t": 1752287371971
      },
      "614": {
        "saved_t": 1752287378460,
        "frame_t": 1752287377397,
        "shuttle_has_part_t": 1752287376964
      },
      "615": {
        "saved_t": 1752287383447,
        "frame_t": 1752287382405,
        "shuttle_has_part_t": 1752287381980
      },
      "616": {
        "saved_t": 1752287388387,
        "frame_t": 1752287387400,
        "shuttle_has_part_t": 1752287386968
      },
      "617": {
        "saved_t": 1752287393503,
        "frame_t": 1752287392426,
        "shuttle_has_part_t": 1752287391991
      },
      "618": {
        "saved_t": 1752287398443,
        "frame_t": 1752287397424,
        "shuttle_has_part_t": 1752287396978
      },
      "619": {
        "saved_t": 1752287403527,
        "frame_t": 1752287402386,
        "shuttle_has_part_t": 1752287401968
      },
      "620": {
        "saved_t": 1752287408447,
        "frame_t": 1752287407428,
        "shuttle_has_part_t": 1752287406980
      },
      "621": {
        "saved_t": 1752287413373,
        "frame_t": 1752287412412,
        "shuttle_has_part_t": 1752287411973
      },
      "622": {
        "saved_t": 1752287418450,
        "frame_t": 1752287417417,
        "shuttle_has_part_t": 1752287416988
      },
      "623": {
        "saved_t": 1752287423483,
        "frame_t": 1752287422397,
        "shuttle_has_part_t": 1752287421971
      },
      "624": {
        "saved_t": 1752287428433,
        "frame_t": 1752287427410,
        "shuttle_has_part_t": 1752287426981
      },
      "625": {
        "saved_t": 1752287433517,
        "frame_t": 1752287432392,
        "shuttle_has_part_t": 1752287431973
      },
      "626": {
        "saved_t": 1752287438103,
        "frame_t": 1752287437396,
        "shuttle_has_part_t": 1752287436973
      },
      "627": {
        "saved_t": 1752287443447,
        "frame_t": 1752287442393,
        "shuttle_has_part_t": 1752287441974
      },
      "628": {
        "saved_t": 1752287448490,
        "frame_t": 1752287447390,
        "shuttle_has_part_t": 1752287446974
      },
      "629": {
        "saved_t": 1752287453480,
        "frame_t": 1752287452377,
        "shuttle_has_part_t": 1752287451963
      },
      "630": {
        "saved_t": 1752287458377,
        "frame_t": 1752287457377,
        "shuttle_has_part_t": 1752287456962
      },
      "631": {
        "saved_t": 1752287463430,
        "frame_t": 1752287462381,
        "shuttle_has_part_t": 1752287461967
      },
      "632": {
        "saved_t": 1752287468133,
        "frame_t": 1752287467385,
        "shuttle_has_part_t": 1752287466969
      },
      "633": {
        "saved_t": 1752287473423,
        "frame_t": 1752287472379,
        "shuttle_has_part_t": 1752287471962
      },
      "634": {
        "saved_t": 1752287478340,
        "frame_t": 1752287477382,
        "shuttle_has_part_t": 1752287476964
      },
      "635": {
        "saved_t": 1752287483497,
        "frame_t": 1752287482387,
        "shuttle_has_part_t": 1752287481972
      },
      "636": {
        "saved_t": 1752287488383,
        "frame_t": 1752287487379,
        "shuttle_has_part_t": 1752287486964
      },
      "637": {
        "saved_t": 1752287493503,
        "frame_t": 1752287492400,
        "shuttle_has_part_t": 1752287491973
      },
      "638": {
        "saved_t": 1752287498317,
        "frame_t": 1752287497389,
        "shuttle_has_part_t": 1752287496972
      },
      "639": {
        "saved_t": 1752287503443,
        "frame_t": 1752287502389,
        "shuttle_has_part_t": 1752287501974
      },
      "640": {
        "saved_t": 1752287508380,
        "frame_t": 1752287507391,
        "shuttle_has_part_t": 1752287506974
      },
      "641": {
        "saved_t": 1752287513193,
        "frame_t": 1752287512383,
        "shuttle_has_part_t": 1752287511968
      },
      "642": {
        "saved_t": 1752287518477,
        "frame_t": 1752287517385,
        "shuttle_has_part_t": 1752287516969
      },
      "643": {
        "saved_t": 1752287523367,
        "frame_t": 1752287522383,
        "shuttle_has_part_t": 1752287521969
      },
      "644": {
        "saved_t": 1752287528430,
        "frame_t": 1752287527378,
        "shuttle_has_part_t": 1752287526959
      },
      "645": {
        "saved_t": 1752287533333,
        "frame_t": 1752287532383,
        "shuttle_has_part_t": 1752287531968
      },
      "646": {
        "saved_t": 1752287538693,
        "frame_t": 1752287537380,
        "shuttle_has_part_t": 1752287536961
      },
      "647": {
        "saved_t": 1752287543370,
        "frame_t": 1752287542381,
        "shuttle_has_part_t": 1752287541967
      },
      "648": {
        "saved_t": 1752287548477,
        "frame_t": 1752287547400,
        "shuttle_has_part_t": 1752287546980
      },
      "649": {
        "saved_t": 1752287553413,
        "frame_t": 1752287552386,
        "shuttle_has_part_t": 1752287551969
      },
      "650": {
        "saved_t": 1752287558410,
        "frame_t": 1752287557384,
        "shuttle_has_part_t": 1752287556966
      },
      "651": {
        "saved_t": 1752287563437,
        "frame_t": 1752287562384,
        "shuttle_has_part_t": 1752287561968
      },
      "652": {
        "saved_t": 1752287568397,
        "frame_t": 1752287567383,
        "shuttle_has_part_t": 1752287566966
      },
      "653": {
        "saved_t": 1752287573763,
        "frame_t": 1752287572380,
        "shuttle_has_part_t": 1752287571963
      },
      "654": {
        "saved_t": 1752287578387,
        "frame_t": 1752287577384,
        "shuttle_has_part_t": 1752287576967
      },
      "655": {
        "saved_t": 1752287583420,
        "frame_t": 1752287582388,
        "shuttle_has_part_t": 1752287581973
      },
      "656": {
        "saved_t": 1752287588457,
        "frame_t": 1752287587389,
        "shuttle_has_part_t": 1752287586972
      },
      "657": {
        "saved_t": 1752287593303,
        "frame_t": 1752287592384,
        "shuttle_has_part_t": 1752287591969
      },
      "658": {
        "saved_t": 1752287598353,
        "frame_t": 1752287597388,
        "shuttle_has_part_t": 1752287596970
      },
      "659": {
        "saved_t": 1752287603437,
        "frame_t": 1752287602383,
        "shuttle_has_part_t": 1752287601968
      },
      "660": {
        "saved_t": 1752287608427,
        "frame_t": 1752287607392,
        "shuttle_has_part_t": 1752287606973
      },
      "661": {
        "saved_t": 1752287613360,
        "frame_t": 1752287612391,
        "shuttle_has_part_t": 1752287611973
      },
      "662": {
        "saved_t": 1752287618523,
        "frame_t": 1752287617390,
        "shuttle_has_part_t": 1752287616972
      },
      "663": {
        "saved_t": 1752287623187,
        "frame_t": 1752287622387,
        "shuttle_has_part_t": 1752287621970
      },
      "664": {
        "saved_t": 1752287628420,
        "frame_t": 1752287627407,
        "shuttle_has_part_t": 1752287626984
      },
      "665": {
        "saved_t": 1752287633467,
        "frame_t": 1752287632380,
        "shuttle_has_part_t": 1752287631964
      },
      "666": {
        "saved_t": 1752287638523,
        "frame_t": 1752287637393,
        "shuttle_has_part_t": 1752287636973
      },
      "667": {
        "saved_t": 1752287643120,
        "frame_t": 1752287642388,
        "shuttle_has_part_t": 1752287641975
      },
      "668": {
        "saved_t": 1752287648133,
        "frame_t": 1752287647391,
        "shuttle_has_part_t": 1752287646974
      }
    }
  ]
}

"""