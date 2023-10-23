import pandas as pd
from pyspark.sql import SparkSession, DataFrame


def read_csv_pd(path: str, delimiter: str = ",") -> pd.DataFrame:
    """
    Read csv with pandas
    """
    return pd.read_csv(filepath_or_buffer=path, delimiter=delimiter)


def read_csv_pyspark(spark: SparkSession, path: str, delimiter: str = ",") -> DataFrame:
    """
    Read csv with spark
    """
    return spark.read.csv(
        path=path,
        enforceSchema="false",
        header="true",
        quote='"',
        sep=delimiter,
        mode="PERMISSIVE",
        unescapedQuoteHandling="STOP_AT_CLOSING_QUOTE",
        columnNameOfCorruptRecord="_corrupt_record"
    )
