import asyncio
import json
import logging
from rabbitmq import RabbitmqCameraProducer

_logger = logging.getLogger(__name__)


async def get_input():
    input_str = await asyncio.to_thread(
        input,
        "input cmds:\n"
    )
    input_str = input_str.strip()
    if input_str.lower() in ("exit", "quit", "-q"):
        return -1
    elif input_str.lower() in ("help", "-h"):
        return 0
    input_list = input_str.split()

    ips = input_list[0]
    if ips.lower() == "all":
        ips = None
    else:
        ips = ips.split(",")

    cmds = [cmd.split(",") for cmd in input_list[1:]]
    return ips, cmds


async def output_cmds(mq):
    while True:
        try:
            input_cmds = await get_input()
            if input_cmds == -1:
                break
            elif input_cmds == 0:
                print(f"ips = 192.168.1.1,192.168.1.2,... or all \ncmds:\n\topen -> ips open \n\tclose -> ips close \n\tset -> set,node,value\n\tget -> get,node\nexample:\n\tset TriggerSoftware -> set,TriggerSoftware,1000")
                continue
            else:
                camera_ips, cmds = input_cmds
                data = json.dumps(cmds)
                await mq.publish(camera_ip=camera_ips, data=data)
        except Exception as err:
            print(f"发送消息错误: {err}")
    print("结束输出监听")
    raise KeyboardInterrupt


async def handle_response(mq):
    async for data in mq.listener():
        pass


async def main(rabbit_url):
    mq = RabbitmqCameraProducer(rabbit_url)
    try:
        await mq.connect()
        tasks = [
            asyncio.create_task(handle_response(mq)),
            asyncio.create_task(output_cmds(mq)),
        ]
        await asyncio.gather(*tasks, return_exceptions=False)
    except (KeyboardInterrupt, asyncio.CancelledError):
        print("中断")
    except Exception as err:
        print(f"错误: {err}")
    finally:
        await mq.close()
        print("退出")


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


if __name__ == "__main__":

    url = "amqp://admin:123@localhost/"
    init_logger()
    asyncio.run(main(url))