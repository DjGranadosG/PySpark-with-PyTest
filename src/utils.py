import os
import requests
from pyspark.sql import SparkSession
from .config import Config


def download_csv():
    if not os.path.exists(Config.DATA_URL):
        response = requests.get(Config.REMOTE_DATA_URL)
        with open(Config.LOCAL_FILENAME, 'wb') as file:
            file.write(response.content)
        print(f"Downloaded {Config.LOCAL_FILENAME} from {Config.REMOTE_DATA_URL}")
    else:
        print(f"{Config.LOCAL_FILENAME} already exists.")


def get_spark_session():
    spark = SparkSession.builder \
        .appName(Config.SPARK_APP_NAME) \
        .master(Config.SPARK_URL) \
        .config("spark.executor.memory", Config.SPARK_EXECUTOR_MEMORY) \
        .config("spark.executor.cores", Config.SPARK_EXECUTOR_CORES) \
        .config("spark.cores.max", Config.SPARK_CORES_MAX) \
        .getOrCreate()
    return spark


def load_csv_to_df(spark):
    try:
        df = spark.read.csv(Config.DATA_URL, header=True, inferSchema=True)
        return df
    except Exception as e:
        print(f"Error loading CSV: {e}")
        return None
