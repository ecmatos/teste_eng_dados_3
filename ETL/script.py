"""
Module for ETL process of client data from raw CSV to Bronze and Silver layers using PySpark.
"""

import os
import logging
from datetime import datetime
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, 
    IntegerType, TimestampType, DateType, 
    DecimalType)


def setup_logger() -> logging.Logger:
    """
    Set up and return a logger for the ETL process.
    :return: Configured logger
    """
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    return logging.getLogger("etl_clientes")


class EnvironmentResolver:


    @staticmethod
    def is_glue() -> bool:
        return os.getenv("GLUE_PYTHON_VERSION") is not None

    
    @staticmethod
    def build_s3_path(bucket: str, key: str) -> str:
        if EnvironmentResolver.is_glue():
            return "s3://{}/{}".format(bucket, key)
        
        return "s3a://{}/{}".format(bucket, key)


class ETLConfig:
    """
    Configuration class for ETL process.
    """

    APP_NAME = "etl_clientes"

    BUCKET_RAW = "bucket-dev-raw"
    BUCKET_BRONZE = "bucket-dev-bronze"
    BUCKET_SILVER = "bucket-dev-silver"

    CLIENTS_FILE_NAME = "clientes_sinteticos.csv"

    RAW_PATH = EnvironmentResolver.build_s3_path(BUCKET_RAW, CLIENTS_FILE_NAME)
    BRONZE_PATH = EnvironmentResolver.build_s3_path(BUCKET_BRONZE, "tabela_cliente_landing")
    SILVER_PATH = EnvironmentResolver.build_s3_path(BUCKET_SILVER, "tb_cliente")

    SHUFFLE_PARTITIONS = 8
    DEFAULT_PARALLELISM = 8

    PARTITION_COLUMN = "anomesdia"


class SparkSessionFactory:
    """
    Factory class for creating SparkSession instances.
    """

    @staticmethod
    def create() -> SparkSession:
        """
        Create and return a SparkSession configured for the ETL process.
        :return: SparkSession instance
        """

        builder = (
            SparkSession.builder \
            .appName(ETLConfig.APP_NAME) \
            .config("spark.sql.shuffle.partitions", ETLConfig.SHUFFLE_PARTITIONS) \
            .config("spark.default.parallelism", ETLConfig.DEFAULT_PARALLELISM) \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider", 
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
            )
        )

        if EnvironmentResolver.is_glue():
            return builder.getOrCreate()

        spark_host = os.getenv("SPARK_MASTER_HOST")
        spark_port = os.getenv("SPARK_MASTER_PORT")

        if spark_host and spark_port:
            builder = builder.master("spark://{}:{}".format(spark_host, spark_port))
        else:
            builder = builder.master("local[*]")

        return builder.getOrCreate()


class DataReader:
    """
    Class for reading data from different sources into Spark DataFrames.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark


    def read_csv(self, path: str) -> DataFrame:
        """
        Read a CSV file into a Spark DataFrame.
        :param path: Path to the CSV file
        :return: DataFrame with the CSV data
        """

        return self.spark.read.option("header", True).csv(path)


    def read_parquet(self, path: str) -> DataFrame:
        """
        Read a Parquet file into a Spark DataFrame.
        :param path: Path to the Parquet file
        :return: DataFrame with the Parquet data
        """

        return self.spark.read.option("header", True).parquet(path)


class DataWriter:
    """
    Class for writing Spark DataFrames to target formats and locations.
    """

    @staticmethod
    def write(df: DataFrame, path: str, partition_col: str):
        """
        Write a DataFrame to a specified path in a given format.
        :param df: DataFrame to write
        :param path: Path to write the DataFrame
        :param partition_col: Column to partition by
        """

        df.write.mode("append").partitionBy(partition_col).parquet(path)


class BronzeTransformations:
    """
    Class for Bronze layer transformations.
    """

    @staticmethod
    def apply(df: DataFrame) -> DataFrame:
        """
        Apply Bronze layer transformations to the DataFrame.
        :param df: DataFrame to transform
        :return: Transformed DataFrame
        """

        return (
            df
            .withColumn("nm_cliente", F.upper(F.col("nm_cliente")))
            .withColumnRenamed("telefone_cliente", "num_telefone_cliente")
        )


class SilverTransformations:
    """
    Class for Silver layer transformations.
    """

    PHONE_REGEX = r"^\(\d{2}\)\d{5}-\d{4}$"

    @staticmethod
    def apply(df: DataFrame) -> DataFrame:
        
        window_spec = (
            Window
            .partitionBy("cod_cliente")
            .orderBy(F.col("dt_atualizacao").desc())
        )

        df_deduplicated = (
            df
            .withColumn("row_num", F.row_number().over(window_spec))
            .filter(F.col("row_num") == 1)
            .drop("row_num")
        )

        return (
            df_deduplicated
            .withColumn(
                "num_telefone_cliente",
                F.when(
                    F.col("num_telefone_cliente").rlike(SilverTransformations.PHONE_REGEX),
                    F.col("num_telefone_cliente")
                ).otherwise(F.lit(None))
            )
            .withColumn("dt_nascimento_cliente", F.to_date(F.col("dt_nascimento_cliente"), "yyyy-MM-dd"))
            .withColumn("dt_atualizacao", F.to_timestamp(F.col("dt_atualizacao")))
            .withColumn("vl_renda", F.col("vl_renda").cast("decimal(15,2)"))
        )


class ETLOrchestrator:
    """
    Class to orchestrate the ETL process.
    """

    def __init__(self):
        self.logger = setup_logger()
        self.spark = SparkSessionFactory.create()
        self.reader = DataReader(spark=self.spark)

    def execute(self):
        """
        Execute the ETL process.
        """

        self.logger.info("Starting ETL process")

        try:
            processing_date = datetime.now().strftime("%Y-%m-%d")
            self.logger.info("Processing date: {}".format(processing_date))

            self.logger.info("Reading raw data from {}".format(ETLConfig.RAW_PATH))
            df_raw = self.reader.read_csv(ETLConfig.RAW_PATH)
            self.logger.info("Raw record count: {}".format(df_raw.count()))

            self.logger.info("Applying Bronze transformations")
            df_bronze = BronzeTransformations.apply(df_raw)
            df_bronze = df_bronze.withColumn(ETLConfig.PARTITION_COLUMN, F.lit(processing_date))

            self.logger.info("Writing Bronze data to {}".format(ETLConfig.BRONZE_PATH))
            DataWriter.write(df_bronze, ETLConfig.BRONZE_PATH, ETLConfig.PARTITION_COLUMN)

            self.logger.info("Reading Bronze data for Silver transformations")
            df_bronze_source = self.reader.read_parquet(ETLConfig.BRONZE_PATH)

            self.logger.info("Applying Silver transformations")
            df_silver = SilverTransformations.apply(df_bronze_source)
            df_silver = df_silver.withColumn(ETLConfig.PARTITION_COLUMN, F.lit(processing_date))
        
            self.logger.info("Writing Silver data to {}".format(ETLConfig.SILVER_PATH))
            DataWriter.write(df_silver, ETLConfig.SILVER_PATH, ETLConfig.PARTITION_COLUMN)

            self.logger.info("ETL process completed successfully")

        except Exception as e:
            self.logger.exception("ETL process failed: {}".format(e))
            raise

        finally:
            self.logger.info("Stopping Spark session")
            self.spark.stop()


if __name__ == "__main__":
    ETLOrchestrator().execute()
