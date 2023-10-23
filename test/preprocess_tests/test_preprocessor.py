from pyspark.sql import SparkSession
from chispa import assert_df_equality

from src.preprocessor import add_prefix_to_colnames


class TestPreprocessor:
    def test_prefixing_column_names(self, spark: SparkSession) -> None:
        given_schema = "id: int, age: int, phone: int"
        data = [(1, 2, 3),]
        prefix = "fb"
        given_df = spark.createDataFrame(data, given_schema)

        expected_schema = f"{prefix}_id: int, {prefix}_age: int, {prefix}_phone: int"
        expected = spark.createDataFrame(data, expected_schema)

        actual = add_prefix_to_colnames(given_df, prefix=prefix)
        assert_df_equality(actual, expected)
