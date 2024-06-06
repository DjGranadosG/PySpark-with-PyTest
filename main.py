from src.utils import download_csv, get_spark_session, load_csv_to_df
from src.transformations import filter_data, CrimeDataTransformations


def main():
    download_csv()

    spark = get_spark_session()

    # 1st test
    test_data = [(1, 10), (2, 20), (3, 30)]
    test_columns = ["id", "value"]
    df_test = spark.createDataFrame(test_data, test_columns)
    df_test_filtered = filter_data(df_test, 15)
    print("Filtered Test DataFrame:")
    df_test_filtered.show()

    # 2nd test
    df_csv = load_csv_to_df(spark)
    print("DataFrame imported from CSV:")
    df_csv.show(5)

    # 3rd test
    if df_csv is not None:
        df_prepared = CrimeDataTransformations.prepare_df(df_csv) # By using the staticmethod decorator we do not need to instantiate the class

        crime_transformer = CrimeDataTransformations(df_prepared) # Instantiate CrimeDataTransformations

        print("Filtering crimes from 2020:")
        df_filtered_year = crime_transformer.filter_by_year(2020)
        df_filtered_year.show(5)

        print("Filtering THEFT type crimes:")
        df_filtered_type = crime_transformer.filter_by_type("THEFT")
        df_filtered_type.show(5)

        print("Crimes per month:")
        df_crimes_per_month = crime_transformer.crimes_per_month()
        df_crimes_per_month.show(12)

        print("Crimes by district:")
        df_crimes_by_district = crime_transformer.crimes_by_district()
        df_crimes_by_district.show(5)
    else:
        print("Could not load the DataFrame from the CSV.")

    spark.stop()


if __name__ == "__main__":
    main()
