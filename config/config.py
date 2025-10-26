import os

PRESS_LINE = "5-100"

# redis
REDIS_HOST = '127.0.0.1'
REDIS_PORT = 6379
REDIS_DB = 0

# rabbitmq
RABBITMQ_URL = "amqp://admin:123@localhost/"

# modbus
MODBUS_HOST = "192.168.4.23"
MODBUS_PORT = 5020
MODBUS_SLAVE = 0x01

# udp multicast
UDP_MULTICAST_IP = "224.0.0.1"
UDP_MULTICAST_PORT = 1000
UDP_MULTICAST_INTERFACE_IP = None

# mssql
MSSQL_HOST = "127.0.0.1"
MSSQL_PORT = 1433
MSSQL_USER = "sa"
MSSQL_PWD = "Abc12345"
MSSQL_DB = "imageInfo"
MSSQL_DRIVER = "ODBC Driver 18 for SQL Server"

# 根目录
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


PARTS_INFO_PATH = os.path.join(ROOT_DIR, "config/parts_info.yaml")
CAMERA_PARAMS_PATH = os.path.join(ROOT_DIR, "config/camera_params.yml")
MODBUS_ADDRESS_PATH = os.path.join(ROOT_DIR, "config/modbus_address.yml")

LOG_FILE_READER_FOR_PRESS = os.path.join(ROOT_DIR, "app/log/readerForPress.log")
LOG_FILE_CONTROLLER_FOR_SHUTTLE_CAMERAS = os.path.join(ROOT_DIR, "app/log/controllerForShuttleCameras.log")
LOG_FILE_SHUTTLE_CAMERAS = os.path.join(ROOT_DIR, "app/log/shuttleCameras.log")
LOG_FILE_IMAGE_SAVER_FOR_SHUTTLE = os.path.join(ROOT_DIR, "app/log/imageSaverForShuttle.log")


IMAGE_SAVED_DIR_FOR_SHUTTLE = r"D:\CapturedPicFromShuttle"
IMAGE_SAVER_FOR_SHUTTLE_GET_IMAGE_TIMEOUT_SEC = 5
IMAGE_SAVER_FOR_SHUTTLE_IMAGE_OVERWRITE = False
IMAGE_SAVER_FOR_SHUTTLE_IMAGE_FORMAT = "png"
IMAGE_SAVER_FOR_SHUTTLE_WORKERS_NUMBER = 5



