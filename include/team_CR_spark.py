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
from paths import raw_parquet, reference_targets
import logging


def transform_1(spark: SparkSession, logical_date: str) -> DataFrame:

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

    df_silver = (
        spark.read.schema(silver_schema)
        .option("header", "true")
        .option("delimiter", ",")
        .csv(raw_parquet(logical_date))
    )

    df_filtered = df_silver.filter(
        (F.col("amount_eur") > 0)
        & (F.col("tx_id").isNotNull())
        & (F.col("category").isNotNull())
        & (F.col("country").isNotNull())
    )
    return df_filtered


def transform_2(df: DataFrame, spark: SparkSession, logical_date: str) -> DataFrame:
    ref_schema = StructType(
        [
            StructField("category", StringType(), True),
            StructField("target_revenue_eur", DoubleType(), True),
        ]
    )

    ref_path = reference_targets()

    df_targets = spark.read.schema(ref_schema).option("header", "true").csv(ref_path)

    df_enriched = df.join(df_targets, df.category == df_targets.category, "left")

    df_enriched = (
        df_enriched.withColumn("transaction_hour", F.hour("ts"))
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
        .withColumn(
            "target_achievement_pct",
            F.round((F.col("amount_eur") / F.col("target_revenue_eur")) * 100, 2),
        )
    )
    return df_enriched


def transform_3(df: DataFrame) -> DataFrame:
    kpi_category = (
        df.groupBy("category")
        .agg(
            F.count("tx_id").alias("transaction_count"),
            F.sum("amount_eur").alias("total_revenue_eur"),
            F.round(F.avg("amount_eur"), 2).alias("avg_transaction_eur"),
            F.max("amount_eur").alias("max_transaction_eur"),
            F.min("amount_eur").alias("min_transaction_eur"),
            F.sum("is_card_payment").alias("card_payments_count"),
            (F.sum("is_card_payment") / F.count("tx_id") * 100).alias(
                "card_payment_rate_pct"
            ),
        )
        .orderBy(F.col("total_revenue_eur").desc())
    )

    # KPI par pays
    kpi_country = (
        df.groupBy("country")
        .agg(
            F.count("tx_id").alias("transaction_count"),
            F.sum("amount_eur").alias("total_revenue_eur"),
            F.round(F.avg("amount_eur"), 2).alias("avg_transaction_eur"),
            F.countDistinct("category").alias("category_diversity"),
            F.countDistinct("payment_method").alias("payment_methods_count"),
        )
        .orderBy(F.col("total_revenue_eur").desc())
    )

    return kpi_category, kpi_country


def run_daily(logical_date: str, *, with_reference: bool = False) -> dict:
    """Called from your Airflow @task. Wire transform_1 → transform_2 → transform_3, then write outputs."""
    raise NotImplementedError("Implement run_daily")
    # run all transform and return kpi from trnasform 3
    # use reference for transform 2
