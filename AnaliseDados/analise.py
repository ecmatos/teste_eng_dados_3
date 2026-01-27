"""
Module for client data analysis using PySpark
"""

import os
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DateType


INPUT_PATH = "file:///mnt/notebooks/clientes_sinteticos.csv"
APP_NAME = "analise_clientes"


def get_spark_session() -> SparkSession:
    """
    Create and return a SparkSession instance
    :return: SparkSession instance
    """

    return (
        SparkSession.builder
        .appName(APP_NAME)
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def read_csv(spark, path) -> DataFrame:
    """
    Read a CSV file into a DataFrame
    :param spark: SparkSession instance
    :param path: Path to the CSV file
    :return: DataFrame with the CSV data
    """

    return (
        spark.read
        .option("header", True)
        .csv(path)
    )


def top_most_updated_clients(df, qty=5) -> DataFrame:
    """
    Calculate the top N clients with the most updates
    :param df: DataFrame with client data
    :param qty: Number of top clients to return
    :return: DataFrame with top N clients
    """
    
    return (
        df
        .groupBy("cod_cliente")
        .agg(F.count("*").alias("update_qty"))
        .orderBy(F.col("update_qty").desc())
        .limit(qty)
    )


def remove_duplicates(df) -> DataFrame:
    """
    Remove duplicate records based on 'cod_cliente', keeping the latest update
    :param df: DataFrame with client data
    :return: DataFrame without duplicates
    """

    df = df.withColumn(
        "dt_atualizacao",
        F.to_timestamp("dt_atualizacao")
    )

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

    return df_dedup

    
def add_idade(df) -> DataFrame:
    """
    Age is calculated using floor to ensure semantic correctness (i.e., age increases only after birthday)
    :param df: DataFrame with client data
    :return: DataFrame with an additional 'idade' column
    """

    processing_date = F.lit(datetime.today().date())

    return (
        df
        .withColumn(
            "dt_nascimento_cliente",
            F.to_date("dt_nascimento_cliente", "yyyy-MM-dd")
        )
        .withColumn(
            "idade",
            F.floor(F.months_between(processing_date, F.col("dt_nascimento_cliente")) / 12).cast("int")
        )
    )



def calculate_avg_age(df) -> DataFrame:
    """
    Calculate the average age of clients
    :param df: DataFrame with client data including 'idade' column
    :return: DataFrame with the average age of clients
    """
    return (
        df
        .filter(F.col("dt_nascimento_cliente").isNotNull())
        .agg(F.round(F.avg("idade"), 2).alias("average_client_age"))
    )


def main():
    """
    Main function to execute the data analysis
    """

    start_time = datetime.now()

    spark = get_spark_session()

    print("Reading dataset")
    df = read_csv(spark, INPUT_PATH)

    cols_needed = [
        "cod_cliente",
        "dt_nascimento_cliente",
        "dt_atualizacao"
    ]

    df_filtered = df.select(*cols_needed)

    print("Top 5 clients with most updates:")
    top_updated_clients = top_most_updated_clients(df_filtered, qty=5)
    top_updated_clients.show(truncate=False)

    print("Removing duplicates")
    df_dedup = remove_duplicates(df_filtered).cache()
    df_dedup.count()  # Materialize cache

    print("Calculating average age of clients:")
    df_age = add_idade(df_dedup)

    print("Average age of clients:")
    df_avg_age = calculate_avg_age(df_age)
    df_avg_age.show(truncate=False)

    end_time = datetime.now()
    print("Total execution time: {}".format(end_time - start_time))

    spark.stop()


if __name__ == "__main__":
    main()
