from pyspark.sql import SparkSession
import pyspark.sql.functions as f

from etl import read_csv_pyspark
from preprocessor import preprocess
from aggregate import join


def main(spark: SparkSession) -> None:
    google = read_csv_pyspark(spark, r"data\google_dataset.csv")
    fb = read_csv_pyspark(spark, r"data\facebook_dataset.csv")
    website = read_csv_pyspark(spark, r"data\website_dataset.csv", ";")

    google_preprocessed = preprocess(
        df=google, identity_column="name", dataset_prefix="gg"
    )
    fb_preprocessed = preprocess(df=fb, identity_column="name", dataset_prefix="fb")
    website_preprocessed = preprocess(
        df=website, identity_column="legal_name", dataset_prefix="ws"
    )

    fb_ws_joined = fb_preprocessed.join(website_preprocessed, [f.col("fb_domain") == f.col("ws_root_domain")], "inner")
    all_joined = fb_ws_joined.join(google_preprocessed, [f.col("fb_name") == f.col("gg_name")], "outer")

    all_joined.coalesce(1).write.csv("output", mode="overwrite", header=True)


if __name__ == "__main__":
    spark_session = (
        SparkSession.builder.master("local")
        .appName("company_datasets")
        .enableHiveSupport()
        .getOrCreate()
    )
    main(spark=spark_session)
