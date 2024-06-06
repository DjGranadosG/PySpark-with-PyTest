from pyspark.sql import DataFrame
from pyspark.sql.functions import col, year, month, to_timestamp, to_date


def filter_data(df: object, threshold: object) -> object:
    """
    Filter the DataFrame to include only rows where 'value' is greater than the threshold.

    :param df: Input DataFrame
    :param threshold: Threshold for the filter
    :return: Filtered DataFrame
    """
    return df.filter(col('value') > threshold)


class CrimeDataTransformations:
    def __init__(self, df: DataFrame):
        self.df = df

    @staticmethod  # static method, you don't need an instance of the class. You can call him directly from class
    def prepare_df(df: DataFrame) -> DataFrame:
        return df.withColumn("Date", to_timestamp(col("Date"), "MM/dd/yyyy hh:mm:ss a"))\
                 .withColumn("MyDate", to_date(col("Date")))

    def filter_by_year(self, year_threshold: int) -> DataFrame:
        return self.df.filter(year(col("MyDate")) >= year_threshold)

    def filter_by_type(self, crime_type: str) -> DataFrame:
        return self.df.filter(col("Primary Type") == crime_type)

    def crimes_per_month(self) -> DataFrame:
        return self.df.groupBy(year(col("MyDate")).alias("year"), month(col("MyDate")).alias("month")).count()

    def crimes_by_district(self) -> DataFrame:
        return self.df.groupBy("District").count()
