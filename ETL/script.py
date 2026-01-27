"""
Module for ETL process of client data from raw CSV to Bronze and Silver layers using PySpark.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StructField, StringType, IntegerType, TimestampType, DateType, DecimalType)
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime
import logging
import os
from enum import Enum


class ETLConfig:
    """
    Configuration class for ETL process.
    """

    APP_NAME = "etl_clientes"

    BUCKET_RAW = "itau-de-case-dev-raw"
    BUCKET_BRONZE = "itau-de-case-dev-bronze"
    BUCKET_SILVER = "itau-de-case-dev-silver"

    CLIENTS_FILE_NAME = "clientes_sinteticos.csv"

    RAW_PATH = "s3a://{}/{}".format(BUCKET_RAW, CLIENTS_FILE_NAME)
    BRONZE_PATH = "s3a://{}/tabela_cliente_landing".format(BUCKET_BRONZE)
    SILVER_PATH = "s3a://{}/tb_cliente".format(BUCKET_SILVER)

    SHUFFLE_PARTITIONS = 8
    DEFAULT_PARALLELISM = 8



class TableSchemas(Enum):
    """
    Enum with table schemas for the ETL process.
    """

    BRONZE_CLIENTS_SCHEMA = StructType([
        StructField("cod_cliente", StringType(), False),
        StructField("nm_cliente", StringType(), True),
        StructField("nm_pais_cliente", StringType(), True),
        StructField("nm_cidade_cliente", StringType(), True),
        StructField("nm_rua_cliente", StringType(), True),
        StructField("num_casa_cliente", StringType(), True),
        StructField("num_telefone_cliente", StringType(), True),
        StructField("dt_nascimento_cliente", StringType(), True),
        StructField("dt_atualizacao", StringType(), True),
        StructField("tp_pessoa", StringType(), True),
        StructField("vl_renda", StringType(), True)
    ])

    SILVER_CLIENTES_SCHEMA = StructType([
        StructField("cod_cliente", StringType(), False),
        StructField("nm_cliente", StringType(), True),
        StructField("nm_pais_cliente", StringType(), True),
        StructField("nm_cidade_cliente", StringType(), True),
        StructField("nm_rua_cliente", StringType(), True),
        StructField("num_casa_cliente", StringType(), True),
        StructField("num_telefone_cliente", StringType(), True),
        StructField("dt_nascimento_cliente", DateType(), True),
        StructField("dt_atualizacao", TimestampType(), True),
        StructField("tp_pessoa", StringType(), True),
        StructField("vl_renda", DecimalType(15, 2), True)
    ])


def get_spark_session():
    """
    Create and return a SparkSession configured for the ETL process.
    :return: SparkSession instance
    """

    return (
        SparkSession.builder
        .appName(ETLConfig.APP_NAME)
        .master("spark://{}:{}".format(os.environ['SPARK_MASTER_HOST'], os.environ['SPARK_MASTER_PORT']))
        .config("spark.sql.shuffle.partitions", ETLConfig.SHUFFLE_PARTITIONS)
        .config("spark.default.parallelism", ETLConfig.DEFAULT_PARALLELISM)
        # Hadoop AWS
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
        .getOrCreate()
    )


def read_csv(spark, path):
    """
    Read a CSV file into a Spark DataFrame.
    :param spark: SparkSession instance
    :param path: Path to the CSV file
    :return: DataFrame with the CSV data
    """

    return (
        spark.read
        .option("header", True)
        .csv(path)
    )

def read_parquet(spark, path):
    """
    Read a Parquet file into a Spark DataFrame.
    :param spark: SparkSession instance
    :param path: Path to the Parquet file
    :return: DataFrame with the Parquet data
    """

    return (
        spark.read
        .option("header", True)
        .parquet(path)
    )


def read_source(spark, path, source_type):
    """
    Read data from a specified source type into a Spark DataFrame.
    :param spark: SparkSession instance
    :param path: Path to the data source
    :param source_type: Type of the data source ("parquet" or "csv")
    :return: DataFrame with the source data
    """

    if source_type == "parquet":
        df = read_parquet(spark, path)
    elif source_type == "csv":
        df = read_csv(spark, path)
    else:
        raise ValueError("Unsupported source type: {}".format(source_type))
    
    return df


def add_partition_column(df, processing_date):
    """
    Add a partition column 'anomesdia' to the DataFrame based on the processing date.
    :param df: DataFrame to add the partition column to
    :param processing_date: Processing date in 'YYYYMMDD' format
    :return: DataFrame with the added partition column
    """

    return df.withColumn("anomesdia", F.lit(processing_date))


def transform_bronze(df):
    return (
        df
        .withColumn("nm_cliente", F.upper(F.col("nm_cliente")))
        .withColumnRenamed("telefone_cliente", "num_telefone_cliente")
    )


def save_data(df, partition_col, path):
    """
    Save a DataFrame to a specified path in Parquet format, partitioned by a given column.
    :param df: DataFrame to save
    :param partition_col: Column to partition by
    :param path: Path to save the DataFrame
    """
    
    print("Saving data to {}".format(path))
    df.repartition(8).coalesce(4).write.mode("append").partitionBy(partition_col).parquet(path)


def transform_silver(df):
    """
    Apply Silver layer transformations:
    - Deduplicate by cod_cliente keeping the most recent record
    - Validate phone number format
    - Cast columns to semantic data types
    :param df: DataFrame to transform
    :return: Transformed DataFrame
    """

    window_spec = (
        Window
        .partitionBy("cod_cliente")
        .orderBy(F.col("dt_atualizacao").desc())
    )

    df_dedup = (
        df
        .withColumn("row_num", F.row_number().over(window_spec))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )

    phone_regex = r"^\(\d{2}\)\d{5}-\d{4}$"

    df_validated = (
        df_dedup
        .withColumn(
            "num_telefone_cliente",
            F.when(
                F.col("num_telefone_cliente").rlike(phone_regex),
                F.col("num_telefone_cliente")
            ).otherwise(F.lit(None))
        )
    )

    final_silver = (
        df_validated
        .withColumn(
            "dt_nascimento_cliente",
            F.to_date(F.col("dt_nascimento_cliente"), "yyyy-MM-dd")
        )
        .withColumn(
            "dt_atualizacao",
            F.to_timestamp(F.col("dt_atualizacao"))
        )
        .withColumn(
            "vl_renda",
            F.col("vl_renda").cast("decimal(15,2)")
        )
    )

    return final_silver


def main():
    print("Starting ETL process")
    spark = get_spark_session()

    processing_date = datetime.now().strftime("%Y-%m-%d")

    empty_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), TableSchemas.BRONZE_CLIENTS_SCHEMA.value)

    print("Reading raw data")
    df_raw = read_csv(spark, ETLConfig.RAW_PATH)

    df_bronze = transform_bronze(df_raw)
    df_bronze = empty_df.unionByName(df_bronze)
    df_bronze = add_partition_column(df_bronze, processing_date)
    save_data(df_bronze, "anomesdia", ETLConfig.BRONZE_PATH)

    df_silver = read_source(spark, ETLConfig.BRONZE_PATH, "parquet")

    df_silver = transform_silver(df_silver)
    df_silver = add_partition_column(df_silver, processing_date)
    save_data(df_silver, "anomesdia", ETLConfig.SILVER_PATH)

    spark.stop()


if __name__ == "__main__":
    main()