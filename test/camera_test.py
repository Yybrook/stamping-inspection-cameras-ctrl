import logging
import cv2
from camera import MyCameraForShuttle
import asyncio

_logger = logging.getLogger(__name__)


def init_logger():
    # 创建logger对象
    logger = logging.getLogger()
    # 设置全局最低等级（让所有handler能接收到）
    logger.setLevel(logging.DEBUG)
    # 控制台 Handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    console_formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    console_handler.setFormatter(console_formatter)
    # 添加 handler 到 logger
    logger.addHandler(console_handler)

    logging.getLogger('asyncio').setLevel(logging.INFO)
    logging.getLogger('aiormq.connection').setLevel(logging.INFO)
    logging.getLogger('aio_pika.robust_connection').setLevel(logging.INFO)
    logging.getLogger('aio_pika.connection').setLevel(logging.INFO)
    logging.getLogger('aio_pika.channel').setLevel(logging.INFO)
    logging.getLogger('aio_pika.queue').setLevel(logging.INFO)
    logging.getLogger('aio_pika.exchange').setLevel(logging.INFO)
    logging.getLogger('utils').setLevel(logging.INFO)


async def main():
    camera = await TestCamera.create(
        stop_event=None,
        redis_host="127.0.0.1",
        redis_port=6379,
        redis_db=0,
        rabbitmq_url="amqp://admin:123@localhost/",
        press_line="5-100",
        ip="192.168.31.230",
        camera_params_path="../config/camera_params.yml",
        TriggerMode="Off", grab_method=3
    )
    try:
        # 监听 rabbitmq 消息
        await camera.rabbitmq_worker()
    except (KeyboardInterrupt, asyncio.CancelledError):
        _logger.warning(f"main() cancelled")
    except Exception as err:
        _logger.exception(f"main() error: {err}")

class TestCamera(MyCameraForShuttle):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._flag = 0
        self.shuttle_has_part_t = 1000

    async def _output_frame(self, image_data):
        try:
            # todo 判断 program_id, part_counter 是否有效
            # 从 redis 获取数据
            # program_id_t, program_id = await self.redis.get_latest_program_id(press_line=self.press_line)
            # part_counter_t, part_counter = await self.redis.get_latest_part_counter(press_line=self.press_line)
            self._flag += 1

            program_id = self._flag // 10000
            part_counter = self._flag % 10000

            # 放入 redis
            await self.redis.set_shuttle_frame(
                press_line=self.press_line,
                program_id=program_id,
                part_counter=part_counter,
                camera_ip=self.ip,
                camera_user_id=self.DeviceUserID,
                matrix=image_data,
                frame_num=self.stFrameInfo.nFrameNum,
                frame_t=self.stFrameInfo.nHostTimeStamp,
                has_part_t=self.shuttle_has_part_t,
            )
        except Exception as err:
            _logger.exception(f"{self.identity} output framer to redis error: {err}")

    def get_one_frame_callback(self, pData, pFrameInfo, pUser):
        image_data = super().get_one_frame_callback(pData, pFrameInfo, pUser)
        # 显示图片
        self.show_in_cv(image_data)
        return image_data

    def get_one_frame(self):
        image_data = super().get_one_frame()
        # 将图片数据放入队列
        self.async_worker.run_async(self._output_frame(image_data))
        # 显示图片
        self.show_in_cv(image_data)
        return image_data

    def show_in_cv(self, image_data):
        _image = cv2.resize(image_data, None, None, fx=0.4, fy=0.4, interpolation=cv2.INTER_AREA)
        cv2.imshow(self.ip, _image)
        cv2.waitKey(1)

    def __exit__(self, exc_type, exc_value, traceback):
        super().__exit__(exc_type, exc_value, traceback)
        cv2.destroyAllWindows()
        return False

if __name__ == '__main__':

    init_logger()

    # 获取SDK版本
    _sdk_version = TestCamera.get_sdk_version()
    print(_sdk_version)

    # 枚举获得所有相机ip
    _cam_ips = TestCamera.enum_all_ips()
    print(_cam_ips)

    asyncio.run(main())
