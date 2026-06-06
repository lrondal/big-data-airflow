from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
)
from pyspark.sql import functions as F
from typing import Tuple
from include.paths import raw_parquet, reference_targets, curated_kpis
import logging

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def transform_1(spark: SparkSession, logical_date: str) -> DataFrame:
    """
    Lecture du Parquet source et filtrage des données.
    """
    input_path = raw_parquet(logical_date)
    logging.info(f"Lecture du Parquet source: {input_path}")

    df_silver = spark.read.parquet(str(input_path))

    logging.info(f"Nombre de lignes initial: {df_silver.count()}")

    df_filtered = df_silver.filter(
        (F.col("amount_eur") > 0)
        & (F.col("tx_id").isNotNull())
        & (F.col("category").isNotNull())
        & (F.col("country").isNotNull())
    )

    logging.info(f"Nombre de lignes après filtrage: {df_filtered.count()}")

    return df_filtered


def transform_2(df: DataFrame, spark: SparkSession) -> DataFrame:
    """
    Enrichissement avec les données de référence CSV.
    """
    ref_path = reference_targets()
    logging.info(f"Lecture du fichier de référence CSV: {ref_path}")

    ref_schema = StructType(
        [
            StructField("category", StringType(), True),
            StructField("target_revenue_eur", DoubleType(), True),
        ]
    )

    df_targets = (
        spark.read.schema(ref_schema)
        .option("header", "true")
        .option("delimiter", ",")
        .csv(str(ref_path))
    )

    logging.info(f"Catégories de référence chargées: {df_targets.count()}")

    df_targets_renamed = df_targets.withColumnRenamed("category", "ref_category")

    df_enriched = df.join(
        df_targets_renamed, df.category == df_targets_renamed.ref_category, "left"
    ).drop("ref_category")

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


def transform_3(df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """
    Agrégation des KPIs par catégorie et par pays.
    """
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


def run_daily(logical_date: str) -> dict:
    """
    Pipeline ETL quotidien avec inputs Parquet et référence CSV.
    """
    spark = (
        SparkSession.builder.appName(f"Daily_ETL_{logical_date}")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )

    logging.info(f"Démarrage du pipeline ETL pour: {logical_date}")

    try:
        df_silver = transform_1(spark, logical_date)
        df_enriched = transform_2(df_silver, spark)
        kpi_category, kpi_country = transform_3(df_enriched)

        output_path = (
            curated_kpis(logical_date) if callable(curated_kpis) else curated_kpis
        )

        kpi_combined = kpi_category.withColumn(
            "kpi_type", F.lit("category")
        ).unionByName(
            kpi_country.withColumn("kpi_type", F.lit("country")),
            allowMissingColumns=True,
        )

        kpi_combined.coalesce(1).write.mode("overwrite").parquet(str(output_path))

        logging.info(f"KPIs écrits en Parquet: {output_path}")

    except Exception as e:
        logging.error(f"Erreur dans le pipeline: {str(e)}")
        raise
    finally:
        spark.stop()
        logging.info("Session Spark fermée")

    return {"output_path": output_path}
