from src.utils import get_spark_session, load_csv_to_df, download_csv


def test_load_csv_to_df(spark):
    download_csv()
    df = load_csv_to_df(spark)

    assert df is not None, "DataFrame should not be None"
    assert df.count() > 0, "DataFrame should not be empty"
