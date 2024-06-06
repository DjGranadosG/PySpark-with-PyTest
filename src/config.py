from dotenv import load_dotenv
import os

load_dotenv()


class Config:
    SPARK_APP_NAME = os.getenv('SPARK_APP_NAME')
    SPARK_URL = os.getenv('SPARK_URL')
    SPARK_EXECUTOR_MEMORY = os.getenv('SPARK_EXECUTOR_MEMORY')
    SPARK_EXECUTOR_CORES = os.getenv('SPARK_EXECUTOR_CORES')
    SPARK_CORES_MAX = os.getenv('SPARK_CORES_MAX')
    DATA_URL = os.getenv('DATA_URL')
    REMOTE_DATA_URL = os.getenv('REMOTE_DATA_URL')
    LOCAL_FILENAME = os.getenv('LOCAL_FILENAME')
