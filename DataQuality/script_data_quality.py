"""
Module for data quality checks for Client data in Silver layer.
"""

import os
import logging
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

def setup_logger() -> logging.Logger:
    """
    Set up and return a logger for the data quality process.
    :return: Configured logger
    """
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    return logging.getLogger("data_quality_clientes")


class DataQualityConfig:
    """
    Configuration class for ETL process.
    """

    APP_NAME = "data_quality_clientes"

    BUCKET_SILVER = "itau-de-case-dev-silver"
    SILVER_PATH = "s3a://{}/tb_cliente".format(BUCKET_SILVER)

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

        return (
            SparkSession.builder
            .appName(DataQualityConfig.APP_NAME)
            .master("spark://{}:{}".format(os.environ['SPARK_MASTER_HOST'], os.environ['SPARK_MASTER_PORT']))
            .config("spark.sql.shuffle.partitions", DataQualityConfig.SHUFFLE_PARTITIONS)
            .config("spark.default.parallelism", DataQualityConfig.DEFAULT_PARALLELISM)
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider", 
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
            )
            .getOrCreate()
        )


class ClientDataQualityChecks:

    PHONE_REGEX = r"^\(\d{2}\)\d{5}-\d{4}$"

    def __init__(self, logger: logging.Logger):
        self.logger = logger


    def check_unique_values(self, df: DataFrame, column_name: str) -> int:
        self.logger.info("Checking unique values for column {} and partition {}".format(column_name, DataQualityConfig.PARTITION_COLUMN))
        invalid = (
            df.groupBy(column_name, DataQualityConfig.PARTITION_COLUMN)
            .count()
            .filter(F.col("count") > 1)
        )
        return invalid.count()


    def check_empty_values(self, df: DataFrame, column_name: str) -> int:
        self.logger.info("Checking empty values for column {}".format(column_name))
        invalid = df.filter(
            F.col(column_name).isNull()
        )
        return invalid.count()


    def check_phone_format(self, df: DataFrame) -> int:
        self.logger.info("Checking phone format")
        invalid = df.filter(
            (~F.col("num_telefone_cliente").rlike(self.PHONE_REGEX)) &
            (F.col("num_telefone_cliente").isNotNull())
        )
        return invalid.count()


    def check_person_type(self, df: DataFrame) -> int:
        self.logger.info("Checking invalid values of tp_pessoa")
        invalid = df.filter(
            (~F.col("tp_pessoa").isin("PF", "PJ")) &
            (F.col("tp_pessoa").isNotNull())
        )
        return invalid.count()


    def check_income_values(self, df: DataFrame) -> int:
        self.logger.info("Checking invalid income values (negative vl_renda)")
        invalid = df.filter(F.col("vl_renda") < 0)
        return invalid.count()
        
    
    def check_temporal_consistency(self, df: DataFrame) -> int:
        self.logger.info("Checking temporal consistency (dt_nascimento_cliente < dt_atualizacao)")
        invalid = df.filter(
            (F.col("dt_nascimento_cliente").isNotNull()) &
            (F.col("dt_nascimento_cliente") >= F.col("dt_atualizacao"))
        )
        return invalid.count()


def main():
    logger = setup_logger()
    logger.info("Starting Data Quality process for Clients")

    spark = SparkSessionFactory.create()

    logger.info("Reading Silver data from {}".format(DataQualityConfig.SILVER_PATH))
    df_silver = spark.read.parquet(DataQualityConfig.SILVER_PATH)

    dq = ClientDataQualityChecks(logger)

    results = {
        "cod_cliente_unique_values": dq.check_unique_values(df_silver, 'cod_cliente'),
        "cod_cliente_empty_values": dq.check_empty_values(df_silver, 'cod_cliente'),
        "nm_cliente_empty_values": dq.check_empty_values(df_silver, 'nm_cliente'),
        "dt_atualizacao_empty_values": dq.check_empty_values(df_silver, 'dt_atualizacao'),
        "num_telefone_cliente_empty_values": dq.check_empty_values(df_silver, 'num_telefone_cliente'),
        "num_telefone_cliente_format": dq.check_phone_format(df_silver),
        "dt_nascimento_cliente_empty_values": dq.check_empty_values(df_silver, 'dt_nascimento_cliente'),
        "tp_pessoa_empty_values": dq.check_empty_values(df_silver, 'tp_pessoa'),
        "tp_pessoa_invalid_values": dq.check_person_type(df_silver),
        "vl_renda_empty_values": dq.check_empty_values(df_silver, 'vl_renda'),
        "vl_renda_invalid_values": dq.check_income_values(df_silver),
        "temporal_consistency": dq.check_temporal_consistency(df_silver)
    }

    logger.info("Data Quality Results:")
    for check, count in results.items():
        status = "PASS" if count == 0 else "FAILED ({} records)".format(count)
        logger.info("{}: {}".format(check, status))

    logger.info("Data Quality process finished")
    spark.stop()


if __name__ == "__main__":
    main()
