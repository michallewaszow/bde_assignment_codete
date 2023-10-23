from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from chispa import assert_df_equality

from src.aggregate import join



class TestJoin:
    def test_outer_join_on_two_dataframes(self, spark: SparkSession) -> None:
        #given
        schema = "id: int, age: int"
        data = [(1, 25), (2, 43), (3, 54)]
        other_schema = "other_id: int, other_age: int"
        other_data = [(1, 25), (3, 56), (4, 78)]

        df = spark.createDataFrame(data, schema)
        other_df = spark.createDataFrame(other_data, other_schema)

        #actual
        actual = join(df, other_df, [f.col("id") == f.col("other_id")], "outer")

        #expected
        expected_schema = "id: int, age: int, other_id: int, other_age: int"
        expected_data = [(1, 25, 1, 25), (2, 43, None, None), (3, 54, 3, 56), (None, None, 4, 78)]
        expected = spark.createDataFrame(expected_data, expected_schema)

        assert_df_equality(actual, expected)