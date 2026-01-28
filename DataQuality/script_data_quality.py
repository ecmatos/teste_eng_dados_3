"""
Module for data quality checks for Client data in Silver layer.
"""

import os
import json
import logging
from datetime import datetime
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

    QUALITY_CHECKS = {
        "cod_cliente": ["unique_values", "empty_values"],
        "nm_cliente": ["empty_values"],
        "dt_atualizacao": ["empty_values"],
        "num_telefone_cliente": ["empty_values", "phone_format"],
        "dt_nascimento_cliente": ["empty_values", "temporal_consistency"],
        "tp_pessoa": ["empty_values", "person_type"],
        "vl_renda": ["empty_values", "income_values"]
    }


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



    def generate_data_quality_report(self, df_silver: DataFrame, results: dict) -> str:
        self.logger.info("Generating data quality report")

        dq_report = {}
        dq_report['timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        dq_report['processed_records'] = df_silver.count()
        dq_report['data_quality_results'] = results

        report = json.dumps(dq_report)
        return report


def main():
    logger = setup_logger()
    logger.info("Starting Data Quality process for Clients")

    spark = SparkSessionFactory.create()

    logger.info("Reading Silver data from {}".format(DataQualityConfig.SILVER_PATH))
    df_silver = spark.read.parquet(DataQualityConfig.SILVER_PATH)

    dq = ClientDataQualityChecks(logger)

    quality_checks = DataQualityConfig.QUALITY_CHECKS

    dq_results = {}

    for column, checks in quality_checks.items():
        dq_results[column] = {}
        for check in checks:
            logger.info("Performing check: {} on column: {}".format(check, column))
            if check == "unique_values":
                dq_results[column][check] = dq.check_unique_values(df_silver, column)
            elif check == "empty_values":
                dq_results[column][check] = dq.check_empty_values(df_silver, column)
            elif check == "phone_format" and column == "num_telefone_cliente":
                dq_results[column][check] = dq.check_phone_format(df_silver)
            elif check == "person_type" and column == "tp_pessoa":
                dq_results[column][check] = dq.check_person_type(df_silver)
            elif check == "income_values" and column == "vl_renda":
                dq_results[column][check] = dq.check_income_values(df_silver)
            elif check == "temporal_consistency" and column == "dt_nascimento_cliente":
                dq_results[column][check] = dq.check_temporal_consistency(df_silver)

    logger.info("Data Quality Check Results:")
    for column, checks in dq_results.items():
        for check, count in checks.items():
            dq_results[column][check] = "PASS" if count == 0 else "FAILED ({} records)".format(count)

    data_quality_report = dq.generate_data_quality_report(df_silver, dq_results)
    logger.info("Data Quality Report:\n{}".format(data_quality_report))

    logger.info("Data Quality process finished")
    spark.stop()


if __name__ == "__main__":
    main()
