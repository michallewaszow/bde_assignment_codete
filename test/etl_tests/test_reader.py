from pyspark.sql import SparkSession
from src.etl import read_csv_pyspark


class TestReader:
    def test_csv_pandas_reader(self) -> None:
        """
        test pandas csv reader
        """
        assert None

    def test_csv_spark_reader(self, spark: SparkSession) -> None:
        """
        test spark reader
        """
        fb = read_csv_pyspark(spark, r"data\facebook_dataset.csv")
        google = read_csv_pyspark(spark, r"data\google_dataset.csv")
        website = read_csv_pyspark(spark, r"data\website_dataset.csv", ";")
