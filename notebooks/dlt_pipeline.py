# Databricks notebook source
# MAGIC %md
# MAGIC # DLT パイプライン — 在庫管理デモ
# MAGIC
# MAGIC Medallion Architecture に基づく3段階パイプライン:
# MAGIC - **Bronze**: 生データの忠実な取り込み（CSV → Delta）
# MAGIC - **Silver**: 品質検証・型変換・重複排除
# MAGIC - **Gold**: ビジネス集約テーブル

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col, to_date, to_timestamp, round as spark_round,
    sum as spark_sum, avg as spark_avg, count, countDistinct,
    datediff, current_date, lit, when, max as spark_max,
    min as spark_min, abs as spark_abs
)
from pyspark.sql.types import *

# データソースパス（DLTのパイプライン設定で変更可能）
try:
    LANDING_PATH = spark.conf.get("dlt.landing_path")
except Exception:
    LANDING_PATH = "/Volumes/prod_manufacturing/default/landing"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze レイヤー — 生データの取り込み

# COMMAND ----------

@dlt.table(
    name="raw_orders",
    comment="発注データ（生）— ERP からのCSV取り込み",
    table_properties={"quality": "bronze"},
)
def bronze_raw_orders():
    """発注データの生取り込み（Auto Loader）"""
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .load(f"{LANDING_PATH}/orders/")
    )


@dlt.table(
    name="raw_inventory",
    comment="在庫スナップショット（生）— WMS からの日次取り込み",
    table_properties={"quality": "bronze"},
)
def bronze_raw_inventory():
    """在庫スナップショットの生取り込み"""
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .load(f"{LANDING_PATH}/inventory/")
    )


@dlt.table(
    name="raw_receipts",
    comment="入荷データ（生）— WMS からの取り込み",
    table_properties={"quality": "bronze"},
)
def bronze_raw_receipts():
    """入荷データの生取り込み"""
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .load(f"{LANDING_PATH}/receipts/")
    )


@dlt.table(
    name="raw_demand",
    comment="需要予測データ（生）— 需要予測システムからの取り込み",
    table_properties={"quality": "bronze"},
)
def bronze_raw_demand():
    """需要予測データの生取り込み"""
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .load(f"{LANDING_PATH}/demand/")
    )


@dlt.table(
    name="item_master",
    comment="品目マスタ — マスタデータの取り込み",
    table_properties={"quality": "bronze"},
)
def bronze_item_master():
    """品目マスタの取り込み"""
    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"{LANDING_PATH}/item_master/")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver レイヤー — 品質検証・型変換・重複排除

# COMMAND ----------

@dlt.table(
    name="orders",
    comment="発注マスタ（検証済み）— 品質チェック通過済み",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_fail("pk_not_null", "order_id IS NOT NULL")
@dlt.expect_or_drop("valid_qty", "order_qty > 0")
@dlt.expect_or_drop("valid_amount", "order_amount > 0")
@dlt.expect("valid_date", "order_date IS NOT NULL")
def silver_orders():
    """発注データの品質検証・型変換"""
    return (
        dlt.read_stream("raw_orders")
        .withColumn("order_date", to_date(col("order_date")))
        .withColumn("expected_delivery_date", to_date(col("expected_delivery_date")))
        .withColumn("order_qty", col("order_qty").cast("int"))
        .withColumn("unit_price", col("unit_price").cast("double"))
        .withColumn("order_amount", col("order_amount").cast("double"))
        .dropDuplicates(["order_id"])
    )


@dlt.table(
    name="inventory",
    comment="在庫マスタ（検証済み）— 品質チェック通過済み",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_qty", "inventory_qty >= 0")
@dlt.expect_or_drop("valid_value", "inventory_value >= 0")
@dlt.expect("has_item", "item_id IS NOT NULL")
def silver_inventory():
    """在庫データの品質検証・型変換"""
    return (
        dlt.read_stream("raw_inventory")
        .withColumn("snapshot_date", to_date(col("snapshot_date")))
        .withColumn("inventory_qty", col("inventory_qty").cast("int"))
        .withColumn("inventory_value", col("inventory_value").cast("double"))
        .dropDuplicates(["item_id", "location_id", "snapshot_date"])
    )


@dlt.table(
    name="receipts",
    comment="入荷マスタ（検証済み）— 品質チェック通過済み",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_fail("pk_not_null", "receipt_id IS NOT NULL")
@dlt.expect_or_drop("valid_qty", "receipt_qty > 0")
def silver_receipts():
    """入荷データの品質検証・型変換"""
    return (
        dlt.read_stream("raw_receipts")
        .withColumn("receipt_date", to_date(col("receipt_date")))
        .withColumn("receipt_qty", col("receipt_qty").cast("int"))
        .withColumn("lead_time_actual_days", col("lead_time_actual_days").cast("int"))
        .dropDuplicates(["receipt_id"])
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold レイヤー — ビジネス集約テーブル

# COMMAND ----------

@dlt.table(
    name="inventory_summary",
    comment="在庫サマリ（品目×月）— ダッシュボード・Genie の基盤テーブル",
    table_properties={
        "quality": "gold",
        "delta.enableDeletionVectors": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    },
)
def gold_inventory_summary():
    """在庫サマリ: 品目×月の在庫数量・金額・回転率を集約"""
    inventory = dlt.read("inventory")
    items = dlt.read("item_master")

    # 月次集約
    monthly = (
        inventory
        .withColumn("month", to_date(col("snapshot_date"), "yyyy-MM-01"))
        .groupBy("item_id", "month")
        .agg(
            spark_avg("inventory_qty").alias("avg_inventory_qty"),
            spark_avg("inventory_value").alias("avg_inventory_value"),
            spark_max("inventory_qty").alias("max_inventory_qty"),
            spark_min("inventory_qty").alias("min_inventory_qty"),
        )
    )

    # 品目マスタと結合
    result = (
        monthly
        .join(
            items.select("item_id", "item_name", "category", "category_name", "unit_price"),
            on="item_id",
            how="inner",
        )
        # 在庫回転率の計算（簡易版: 月間消費量 / 平均在庫量）
        # ※ 実際には COGS や出荷データが必要だが、デモ用に簡易計算
        .withColumn("turnover_rate", spark_round(lit(12.0) / (col("avg_inventory_qty") / lit(100) + lit(1)), 2))
        .withColumn("days_on_hand", (col("avg_inventory_qty") / lit(10)).cast("int"))
    )

    return result


@dlt.table(
    name="turnover_analysis",
    comment="在庫回転率分析 — カテゴリ×月のトレンド分析",
    table_properties={"quality": "gold"},
)
def gold_turnover_analysis():
    """カテゴリ別の在庫回転率トレンド"""
    summary = dlt.read("inventory_summary")

    return (
        summary
        .groupBy("category", "category_name", "month")
        .agg(
            spark_avg("turnover_rate").alias("avg_turnover_rate"),
            spark_sum("avg_inventory_value").alias("total_inventory_value"),
            countDistinct("item_id").alias("item_count"),
            spark_avg("days_on_hand").alias("avg_days_on_hand"),
        )
    )


@dlt.table(
    name="overstock_alert",
    comment="過剰在庫アラート — 回転率が閾値以下の品目を検出",
    table_properties={"quality": "gold"},
)
@dlt.expect("has_alert_items", "turnover_rate < 4.0")
def gold_overstock_alert():
    """過剰在庫品目の検出: 回転率 < 4.0 の品目"""
    summary = dlt.read("inventory_summary")

    return (
        summary
        .filter(col("turnover_rate") < 4.0)
        .select(
            "item_id", "item_name", "category", "category_name",
            "month", "avg_inventory_qty", "avg_inventory_value",
            "turnover_rate", "days_on_hand",
        )
    )
