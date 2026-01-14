import os
from functools import lru_cache
from fastapi import APIRouter
from fastapi import HTTPException
from fastapi.responses import FileResponse
import asyncio
import aiofiles
import base64
from typing import List, Tuple, Optional, Dict
from pathlib import Path
import logging
from datetime import datetime, timedelta
from imageSaver import ImageModelForShuttle
from config.config import IMAGE_SAVED_DIR_FOR_SHUTTLE

_logger = logging.getLogger(__name__)

TAG = "picturesForShuttle"

# 定位到 web目录
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # app/ 的上一级目录
STATIC_DIR = os.path.join(BASE_DIR, "static")

# 图片保存地址
IMAGE_SAVED_DIR = IMAGE_SAVED_DIR_FOR_SHUTTLE

MOUNT_DIR = r"/pictures/forShuttle"

router = APIRouter()

# 控制协程并发数量，防止内存爆炸
sem = asyncio.Semaphore(50)


def press_for_shuttle_tag(press_line: str) -> str:
    return f"{TAG}/{press_line}"


@router.get("/{press_line}")
async def pictures_for_shuttle_viewer():
    file_path = os.path.join(STATIC_DIR, "pictures_for_shuttle_viewer.html")
    return FileResponse(file_path, media_type="text/html")


class Part:
    def __init__(self, year: int, month: int, day: int, part_id: int):
        self.year = year
        self.month = month
        self.day = day
        self.part_id = part_id

        self._part_dir = None
        self._all_part_counts = list()
        self._part_count_dirs = dict()

        self._start = datetime(self.year, self.month, self.day, 0, 0, 0)
        self._end = self._start + timedelta(days=1)

    @property
    def part_dir(self) -> Path:
        if self._part_dir is None:
            part_dir = Path(os.path.join(
                IMAGE_SAVED_DIR,
                str(self.year),
                f"{self.month:02d}",
                f"{self.day:02d}",
                str(self.part_id)
            ))

            if not part_dir.exists():
                raise HTTPException(status_code=404, detail=f"part folder[{part_dir}] not existed")

            self._part_dir = part_dir
            return self._part_dir
        else:
            return self._part_dir

    # @classmethod
    # @lru_cache(maxsize=128)
    # def get_all_part_counts(cls, part_dir: Path) -> List[int]:
    #     return sorted([int(f.name) for f in part_dir.iterdir() if f.is_dir()])

    @property
    def all_part_counts(self) -> List[int]:
        # if not self._all_part_counts:
        #     all_part_counts = self.get_all_part_counts(part_dir=self.part_dir)
        #
        #     if not all_part_counts:
        #         raise HTTPException(status_code=404, detail=f"part folder[{self._part_dir}] have no parts")
        #
        #     self._all_part_counts = sorted(all_part_counts)
        #     return self._all_part_counts
        # else:
        #     return self._all_part_counts
        return sorted([int(f.name) for f in self.part_dir.iterdir() if f.is_dir()])

    # @classmethod
    # @lru_cache(maxsize=128)
    # def get_part_count_dirs(cls, part_dir: Path, all_part_counts: List[int]) -> Dict[int, Path]:
    #     return {pc: part_dir / str(pc) for pc in all_part_counts}

    @property
    def part_count_dirs(self) -> Dict[int, Path]:
        # if not self._part_count_dirs:
        #     part_count_dirs = self.get_part_count_dirs(part_dir=self.part_dir, all_part_counts=self.all_part_counts)
        #     self._part_count_dirs = part_count_dirs
        #     return self._part_count_dirs
        # else:
        #     return self._part_count_dirs
        return {pc: self.part_dir / str(pc) for pc in self.all_part_counts}

    def part_count_dir(self, count: int) -> Path:
        if count in self.part_count_dirs:
            part_count_dir = self.part_count_dirs[count]
            return part_count_dir
        else:
            raise HTTPException(status_code=404, detail=f"part count[{count}] out of range")

    def images_of_part_count(self, count: int, image_format: str = "jpg"):
        return self.part_count_dir(count=count).glob(f"*.{image_format}")

    @staticmethod
    def define_image_name(camera_user_id: str, prefix: str = "00", index: int = 0, pic_format: str = "jpg") -> str:
        """定义 picture_name"""
        return f"{prefix}-{camera_user_id}-{index:02d}.{pic_format}"

    @staticmethod
    def split_image_name(name: str) -> List[int]:
        # name = os.path.splitext(name)[0]
        # 按 "-" 分割并转成整数
        return [int(part) for part in name.split('-')]

    def get_previous_and_next(self, count: int) -> Tuple[int, int]:
        index = self.all_part_counts.index(count)
        _previous = self.all_part_counts[index - 1] if index > 0 else -1
        _next = self.all_part_counts[index + 1] if index < (len(self.all_part_counts) - 1) else -1
        return _previous, _next

    async def query_part_data(self, count: int, image_name: str, range_len: int = 100):
        sql = f"""
            SELECT
                id,
                [time],
                part_id,
                part_count,
                camera_ip,
                camera_user_id,
                frame_num,
                frame_t,
                frame_width,
                frame_height,
                shuttle_has_part_t
            FROM shuttle_image
            WHERE part_id = {self.part_id}
              AND [time] >= '{self._start:%Y-%m-%d %H:%M:%S}'
              AND [time] <  '{self._end:%Y-%m-%d %H:%M:%S}'
              AND image_path LIKE '%{image_name}'
            ORDER BY part_count ASC;
            """
        rows = await ImageModelForShuttle.raw(sql)

        if not rows:
            return None, None, None

        # 获取所有 part_count 并排序
        all_part_counts = sorted({row.part_count for row in rows})

        if count not in all_part_counts:
            return None, None, None

        # 计算中心范围
        if range_len > 0:
            idx = all_part_counts.index(count)
            half = (range_len - 1) // 2
            start_idx = max(0, idx - half)
            end_idx = min(len(all_part_counts), start_idx + range_len)
            part_counts = all_part_counts[start_idx:end_idx]
        else:
            part_counts = all_part_counts

        # 当前图片 info，构建字典并加 saved_t
        image_row = next(row for row in rows if row.part_count == count)
        image_info = {
            "id": image_row.id,
            "time": image_row.time,
            "part_id": image_row.part_id,
            "part_count": image_row.part_count,
            "camera_ip": image_row.camera_ip,
            "camera_user_id": image_row.camera_user_id,
            "frame_num": image_row.frame_num,
            "frame_t": int(image_row.frame_t),
            "frame_width": image_row.frame_width,
            "frame_height": image_row.frame_height,
            "shuttle_has_part_t": int(image_row.shuttle_has_part_t),
            "saved_t": int(image_row.time.timestamp() * 1000),
        }

        # images_time 只取 part_counts 对应的行
        images_time = {
            row.part_count: {
            "frame_t": int(row.frame_t),
            "saved_t": int(row.time.timestamp() * 1000),
            "shuttle_has_part_t": int(row.shuttle_has_part_t),
        } for row in rows if row.part_count in part_counts}

        return image_info, part_counts, images_time

    @staticmethod
    async def encode_image_2_base64(image_path: Path) -> str:
        async with sem:
            async with aiofiles.open(image_path, "rb") as f:
                img_bytes = await f.read()
        b64 = base64.b64encode(img_bytes).decode()
        return f"data:image/jpeg;base64,{b64}"

    @staticmethod
    async def encode_images_2_base64(image_files):
        # 并发异步处理所有图片
        tasks = [Part.encode_image_2_base64(f) for f in image_files]
        return await asyncio.gather(*tasks)

    async def query_image_info(self, count: int, image_name: str) -> Optional[Dict]:
        sql = f"""
            SELECT
                id, [time], 
                camera_ip, camera_user_id,
                frame_num, frame_t, frame_width, frame_height,
                shuttle_has_part_t
            FROM shuttle_image
            WHERE part_id = {self.part_id}
              AND part_count = {count}
              AND [time] >= '{self._start:%Y-%m-%d %H:%M:%S}'
              AND [time] <  '{self._end:%Y-%m-%d %H:%M:%S}'
              AND image_path LIKE '%{image_name}'
            ORDER BY [time] DESC
            """

        rows = await ImageModelForShuttle.raw(sql)

        if not rows:
            return None

        row = rows[0]

        return{
            "id": row.id,
            "time": row.time,
            "camera_ip": row.camera_ip,
            "camera_user_id": row.camera_user_id,
            "frame_num": row.frame_num,
            "frame_t": row.frame_t,
            "frame_width": row.frame_width,
            "frame_height": row.frame_height,
            "shuttle_has_part_t": row.shuttle_has_part_t,
            "saved_t": int(row.time.timestamp() * 1000),
        }

    async def query_part_counts_range(self, count: int, image_name: str, range_len: int = 100) -> Optional[List[int]]:
        sql = f"""
            SELECT part_count
                FROM shuttle_image
                WHERE part_id = {self.part_id}
                  AND [time] >= '{self._start:%Y-%m-%d %H:%M:%S}'
                  AND [time] < '{self._end:%Y-%m-%d %H:%M:%S}'
                  AND image_path LIKE '%{image_name}'
                ORDER BY part_count ASC;
        """

        rows = await ImageModelForShuttle.raw(sql)

        if not rows:
            return None

        all_part_counts = list(dict.fromkeys(row.part_count for row in rows))

        if count not in all_part_counts:
            return None

        if len(all_part_counts) <= range_len:
            return all_part_counts

        # 计算中心范围
        idx = all_part_counts.index(count)
        half = (range_len - 1) // 2

        start_idx = max(0, idx - half)
        end_idx = min(len(all_part_counts), start_idx + range_len)

        return all_part_counts[start_idx: end_idx]

    async def query_images_time_within_part_counts(self, counts: List[int], image_name: str) -> Optional[Dict[int, Dict[str, int]]]:
        if not counts:
            return None

        sql = f"""
            SELECT
                part_count,
                [time],
                frame_t,
                shuttle_has_part_t
            FROM shuttle_image
            WHERE part_id = {self.part_id}
              AND [time] >= '{self._start:%Y-%m-%d %H:%M:%S}'
              AND [time] < '{self._end:%Y-%m-%d %H:%M:%S}'
              AND image_path LIKE '%{image_name}'
              AND part_count IN ({",".join(str(c) for c in counts)})
            ORDER BY part_count ASC;
            """

        rows = await ImageModelForShuttle.raw(sql)

        if not rows:
            return None

        res = dict()
        for row in rows:
            res[row.part_count] = {
                "frame_t": int(row.frame_t),
                "saved_t": int(row.time.timestamp() * 1000),
                "shuttle_has_part_t": int(row.shuttle_has_part_t),
            }

        return res


@router.get("/{press_line}/{year}/{month}/{day}/{part_id}/{position}/count")
def get_part_count(year: int, month: int, day: int, part_id: int, position: str) -> dict[str, int]:
    if position != "first" and position != "last":
        raise HTTPException(status_code=404, detail=f"part count position[{position}] is illegal")
    elif position == "first":
        pos = 0
    else:
        pos = -1
    part = Part(year=year, month=month, day=day, part_id=part_id)
    return {"part_count": part.all_part_counts[pos]}


@router.get("/{press_line}/{year}/{month}/{day}/{part_id}/{part_count}/{camera_uid}")
async def get_part_image(year: int, month: int, day: int, part_id: int, part_count: int, camera_uid: str):
    part = Part(year=year, month=month, day=day, part_id=part_id)

    if part_count not in part.all_part_counts:
        raise HTTPException(status_code=404, detail=f"part count[{part_count}] out of range")

    image_name = part.define_image_name(camera_user_id=camera_uid)

    image_url = f"{MOUNT_DIR}/{year}/{month:02d}/{day:02d}/{part_id}/{part_count}/{image_name}"

    _previous, _next = part.get_previous_and_next(count=part_count)

    image_info, _, images_time = await part.query_part_data(count=part_count, image_name=image_name)

    return {
        "year": year,
        "month": month,
        "day": day,
        "part_id": part_id,
        "part_count": part_count,
        "previous": _previous,
        "next": _next,
        "first": part.all_part_counts[0],
        "last": part.all_part_counts[-1],
        "camera_uid": camera_uid,
        "image_url": image_url,
        "image_info": image_info,
        "images_time": images_time,
    }


