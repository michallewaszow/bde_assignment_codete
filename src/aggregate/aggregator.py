from pyspark.sql import DataFrame


def join(
    df: DataFrame, other_df: DataFrame, condition: list, join_type: str
) -> DataFrame:
    """
    join function wrapped
    """
    return df.join(other=other_df, on=condition, how=join_type)
