from pyspark.sql import SparkSession
import pytest


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """
    Create SparkSession for unit tests
    """
    return (
        SparkSession.builder.master("local")
        .appName("test")
        .enableHiveSupport()
        .getOrCreate()
    )
