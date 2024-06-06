import pytest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col, year, month
from src.transformations import CrimeDataTransformations, filter_data

@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("pytest-pyspark-local-testing") \
        .getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture
def test_df(spark):
    data = [
        Row(id=1, value=10, Date="01/01/2020 12:00:00 AM", Primary_Type="THEFT", District=1),
        Row(id=2, value=20, Date="02/01/2020 12:00:00 AM", Primary_Type="ASSAULT", District=2),
        Row(id=3, value=30, Date="03/01/2020 12:00:00 AM", Primary_Type="THEFT", District=1),
        Row(id=4, value=40, Date="04/01/2021 12:00:00 AM", Primary_Type="BURGLARY", District=2)
    ]
    return spark.createDataFrame(data).withColumnRenamed("Primary_Type", "Primary Type")