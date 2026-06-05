from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
)
from pyspark.sql import functions as F
from typing import Tuple
import logging


def transform_1(spark: SparkSession, input_path: str) -> DataFrame:
    """
    Transformation 1 - Lecture silver avec schéma explicite + filtre
    """

    # Définir le schéma explicite basé sur votre CSV
    silver_schema = StructType(
        [
            StructField("tx_id", StringType(), True),
            StructField("category", StringType(), True),
            StructField("payment_method", StringType(), True),
            StructField("country", StringType(), True),
            StructField("amount_eur", DoubleType(), True),
            StructField("ts", TimestampType(), True),
        ]
    )

    # Lecture avec le schéma explicite
    df_silver = (
        spark.read.schema(silver_schema)
        .option("header", "true")
        .option("delimiter", ",")
        .csv(input_path)
    )

    # Filtre: garder uniquement les transactions valides
    df_filtered = df_silver.filter(
        (F.col("amount_eur") > 0)
        & (F.col("tx_id").isNotNull())
        & (F.col("category").isNotNull())
        & (F.col("country").isNotNull())
    )
    return df_filtered


def transform_2(spark: SparkSession, df: DataFrame, logical_date: str) -> DataFrame:
    df_enriched = (
        df.withColumn("transaction_hour", F.hour("ts"))
        .withColumn("transaction_date", F.to_date("ts"))
        .withColumn(
            "amount_category",
            F.when(F.col("amount_eur") < 50, "small")
            .when(F.col("amount_eur") < 150, "medium")
            .when(F.col("amount_eur") < 300, "large")
            .otherwise("very_large"),
        )
        .withColumn(
            "is_card_payment", F.when(F.col("payment_method") == "card", 1).otherwise(0)
        )
    )
    return df_enriched


def transform_3(df: DataFrame) -> DataFrame:
    """Transformation 3 - e.g. aggregate KPIs by category and country."""
    raise NotImplementedError("Implement transformation 3")


def run_daily(logical_date: str, *, with_reference: bool = False) -> dict:
    """Called from your Airflow @task. Wire transform_1 → transform_2 → transform_3, then write outputs."""
    raise NotImplementedError("Implement run_daily")
