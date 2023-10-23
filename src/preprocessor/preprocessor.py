from pyspark.sql import DataFrame
import pyspark.sql.functions as f


def add_prefix_to_colnames(df: DataFrame, prefix: str) -> DataFrame:
    new_colnames = [f"{prefix}_{col}" for col in df.columns]
    return df.toDF(*new_colnames)

def lowercase_values(df: DataFrame, column_name: str) -> DataFrame:
    return df.withColumn(column_name, f.lower(f.col(column_name)))

def remove_special_chars(df: DataFrame, column_name: str) -> DataFrame:
    return df.withColumn(column_name, f.regexp_replace(column_name, "[@\+\#\$\%\^\!]+", ""))

def trim(df: DataFrame, column_name: str) -> DataFrame:
    return df.withColumn(column_name, f.trim(f.col(column_name)))
