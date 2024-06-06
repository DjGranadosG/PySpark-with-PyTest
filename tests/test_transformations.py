import pytest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col, year, month
from src.transformations import CrimeDataTransformations, filter_data
from unittest.mock import patch, MagicMock


def test_filter_data(test_df):
    result_df = filter_data(test_df, 20)
    assert result_df.count() == 2
    assert result_df.filter(col('value') <= 20).count() == 0


# Test for CrimeDataTransformations class
def test_prepare_df(test_df):
    transformer = CrimeDataTransformations(test_df)
    prepared_df = transformer.prepare_df(test_df)
    assert prepared_df is not None
    assert "Date" in prepared_df.columns
    assert "MyDate" in prepared_df.columns
    assert prepared_df.filter(col("Date").isNull()).count() == 0


def test_filter_by_year(test_df):
    transformer = CrimeDataTransformations(test_df)
    prepared_df = transformer.prepare_df(test_df)
    transformer.df = prepared_df
    result_df = transformer.filter_by_year(2021)
    assert result_df.count() == 1
    assert result_df.filter(year(col("MyDate")) < 2021).count() == 0


def test_filter_by_type(test_df):
    transformer = CrimeDataTransformations(test_df)
    result_df = transformer.filter_by_type("THEFT")
    assert result_df.count() == 2
    assert result_df.filter(col("Primary Type") != "THEFT").count() == 0


def test_crimes_per_month(test_df):
    transformer = CrimeDataTransformations(test_df)
    prepared_df = transformer.prepare_df(test_df)
    transformer.df = prepared_df
    result_df = transformer.crimes_per_month()
    assert result_df.count() == 4
    assert result_df.filter((col("year") == 2020) & (col("month") == 1)).count() == 1


def test_crimes_by_district(test_df):
    transformer = CrimeDataTransformations(test_df)
    result_df = transformer.crimes_by_district()
    assert result_df.count() == 2
    assert result_df.filter(col("District") == 1).count() == 1
    assert result_df.filter(col("District") == 2).count() == 1


# Mocking example: prepare_df method
@patch.object(CrimeDataTransformations, 'prepare_df')
def test_prepare_df_mock(mock_prepare_df, test_df):
    # Arrange
    mock_prepare_df.return_value = test_df.withColumn("MyDate", col("Date"))

    # Act
    transformer = CrimeDataTransformations(test_df)
    prepared_df = transformer.prepare_df(test_df)

    # Assert
    mock_prepare_df.assert_called_once_with(test_df)
    assert "MyDate" in prepared_df.columns
    assert prepared_df.filter(col("MyDate").isNull()).count() == 0
