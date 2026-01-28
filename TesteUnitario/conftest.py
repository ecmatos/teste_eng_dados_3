"""
Fixture configuration for unit tests.
"""

import os
import sys
import pytest
from pyspark.sql import SparkSession


# Adjust the system path to include the parent directory for module imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


@pytest.fixture(scope="session")
def spark():

    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("unit_tests") \
        .config("spark.ui.enabled", "false") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    
    yield spark
    spark.stop()
