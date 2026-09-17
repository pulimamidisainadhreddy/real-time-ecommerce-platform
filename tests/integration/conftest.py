import os
import sys

import pytest
from pyspark.sql import SparkSession


# Use test tables so integration tests do not touch production tables.
os.environ["FINAL_TABLE"] = "public.test_online_retail"
os.environ["STAGING_TABLE"] = "public.test_online_retail_staging"
os.environ["QUALITY_RESULTS_TABLE"] = "public.test_data_quality_results"
os.environ["REJECTED_RECORDS_TABLE"] = (
    "public.test_rejected_online_retail_records"
)


@pytest.fixture(scope="session")
def integration_spark():
    # Use the same Python interpreter that runs pytest.
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    # Windows Spark integration tests need Hadoop utilities.
    if os.name == "nt":
        hadoop_home = os.getenv("HADOOP_HOME")

        if not hadoop_home:
            pytest.fail(
                "HADOOP_HOME is not configured. "
                "Configure Hadoop/winutils before running "
                "Spark integration tests on Windows."
            )

        os.environ["hadoop.home.dir"] = hadoop_home

    spark = (
        SparkSession.builder
        .master(os.getenv("TEST_SPARK_MASTER", "local[2]"))
        .appName("OnlineRetailIntegrationTests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0,"
            "org.postgresql:postgresql:42.7.7",
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    yield spark

    spark.stop()