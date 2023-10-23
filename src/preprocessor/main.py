from pyspark.sql import DataFrame

from .preprocessor import lowercase_values, add_prefix_to_colnames, remove_special_chars, trim


def preprocess(df: DataFrame, identity_column: str, dataset_prefix: str) -> DataFrame:
    lowercased = lowercase_values(df, identity_column)
    special_char_cleaned = remove_special_chars(lowercased, identity_column)
    trimmed = trim(special_char_cleaned, identity_column)
    return add_prefix_to_colnames(trimmed, dataset_prefix)
