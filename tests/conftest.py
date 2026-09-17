import os
import sys

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    # Make Spark use the same Python interpreter running pytest.
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    session = (
        SparkSession.builder
        .master(os.getenv("TEST_SPARK_MASTER", "local[2]"))
        .appName("OnlineRetailTests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .getOrCreate()
    )

    session.sparkContext.setLogLevel("ERROR")

    yield session

    session.stop()