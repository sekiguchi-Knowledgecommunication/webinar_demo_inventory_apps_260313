# Databricks notebook source
# MAGIC %md
# MAGIC # 【デモ用】過剰在庫サンプルデータ追加スクリプト
# MAGIC
# MAGIC このノートブックは、AIエージェントのデモシナリオをデモ映えさせるために
# MAGIC `inventory_summary` テーブルおよび `overstock_alert` テーブルに
# MAGIC デモ用の過剰在庫データを追加インサートします。
# MAGIC
# MAGIC **実行前に確認してください:**
# MAGIC - Catalog / Schema 名が実際の環境と一致しているか
# MAGIC - クラスタが起動済みであるか
# MAGIC
# MAGIC **実行後の確認:**
# MAGIC - Genie で「過剰在庫の品目を教えて」と質問し、追加品目が返ってくることを確認

# COMMAND ----------

# ========================================
# 設定: 環境に応じてカタログ/スキーマを変更
# ========================================
CATALOG = "prod_manufacturing"
SCHEMA = "default"

# テーブル名（DLT で生成されたテーブルをそのまま利用）
INVENTORY_SUMMARY_TABLE = f"`{CATALOG}`.`{SCHEMA}`.`inventory_summary`"
OVERSTOCK_ALERT_TABLE   = f"`{CATALOG}`.`{SCHEMA}`.`overstock_alert`"

print(f"ターゲットテーブル: {INVENTORY_SUMMARY_TABLE}")
print(f"ターゲットテーブル: {OVERSTOCK_ALERT_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: 既存テーブルの確認

# COMMAND ----------

# inventory_summary テーブルのスキーマ確認
print("=== inventory_summary スキーマ ===")
spark.sql(f"DESCRIBE TABLE {INVENTORY_SUMMARY_TABLE}").show(truncate=False)

# COMMAND ----------

# 現在の過剰在庫アラート件数を確認
current_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {OVERSTOCK_ALERT_TABLE}").collect()[0]["cnt"]
print(f"現在の overstock_alert 件数: {current_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: デモ用過剰在庫データの定義
# MAGIC
# MAGIC デモストーリーに沿ったデータ:
# MAGIC - 電子部品（カテゴリB）を中心に在庫金額が高い品目を選定
# MAGIC - 回転率が低く（< 4.0）、在庫日数が長い（> 90日）品目
# MAGIC - AI エージェントが「推奨アクション」を出しやすいよう特徴を明確化

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType, DateType
)
from datetime import date, datetime

# スキーマ定義（inventory_summary と overstock_alert の共通列）
# ※ DLT テーブルへの MERGE/INSERT には適宜カラムを合わせること
DEMO_SNAPSHOT_MONTH = date(2026, 2, 1)  # 最新月（2026年2月）

# デモ用過剰在庫品目（18件）:
# - 電子部品（B）15件: 長リードタイム × 大量発注 → 過剰在庫の典型
# - 機械部品（A）2件: 季節需要の読み違え
# - 素材・原料（C）1件: 一括調達による過剰
DEMO_OVERSTOCK_ROWS = [
    # --- 電子部品（カテゴリB） ---
    # item_id, item_name, category, category_name, month, avg_inventory_qty, avg_inventory_value, unit_price, turnover_rate, days_on_hand
    ("ITM-0201", "電子部品_DEMO_マイコンチップA",   "B", "電子部品", DEMO_SNAPSHOT_MONTH, 1850, 14_800_000, 8000, 1.2, 185),
    ("ITM-0202", "電子部品_DEMO_センサーモジュールB", "B", "電子部品", DEMO_SNAPSHOT_MONTH, 2200, 13_200_000, 6000, 0.9, 220),
    ("ITM-0203", "電子部品_DEMO_電源ICチップC",       "B", "電子部品", DEMO_SNAPSHOT_MONTH, 3100,  9_920_000, 3200, 0.8, 310),
    ("ITM-0204", "電子部品_DEMO_パワートランジスタD", "B", "電子部品", DEMO_SNAPSHOT_MONTH, 4500,  9_000_000, 2000, 1.1, 450),
    ("ITM-0205", "電子部品_DEMO_高精度コンデンサE",   "B", "電子部品", DEMO_SNAPSHOT_MONTH, 6200,  7_440_000, 1200, 0.7, 620),
    ("ITM-0206", "電子部品_DEMO_RF通信モジュールF",   "B", "電子部品", DEMO_SNAPSHOT_MONTH, 1100,  6_600_000, 6000, 1.5, 110),
    ("ITM-0207", "電子部品_DEMO_LEDドライバICG",      "B", "電子部品", DEMO_SNAPSHOT_MONTH, 2800,  5_600_000, 2000, 1.3, 280),
    ("ITM-0208", "電子部品_DEMO_メモリチップH",        "B", "電子部品", DEMO_SNAPSHOT_MONTH, 1600,  6_400_000, 4000, 2.1, 160),
    ("ITM-0209", "電子部品_DEMO_水晶発振子I",          "B", "電子部品", DEMO_SNAPSHOT_MONTH, 8000,  4_800_000,  600, 0.6,  800),
    ("ITM-0210", "電子部品_DEMO_フォトカプラJ",        "B", "電子部品", DEMO_SNAPSHOT_MONTH, 5500,  4_400_000,  800, 1.8, 550),
    ("ITM-0211", "電子部品_DEMO_マグネティックセンサK","B", "電子部品", DEMO_SNAPSHOT_MONTH, 2100,  6_300_000, 3000, 2.3, 210),
    ("ITM-0212", "電子部品_DEMO_熱電変換モジュールL", "B", "電子部品", DEMO_SNAPSHOT_MONTH,  900,  5_400_000, 6000, 1.9,  90),
    ("ITM-0213", "電子部品_DEMO_デジタルポテンショM", "B", "電子部品", DEMO_SNAPSHOT_MONTH, 3700,  3_700_000, 1000, 2.8, 370),
    ("ITM-0214", "電子部品_DEMO_フラッシュメモリN",   "B", "電子部品", DEMO_SNAPSHOT_MONTH, 1300,  5_200_000, 4000, 3.1, 130),
    ("ITM-0215", "電子部品_DEMO_MPU加速度センサO",    "B", "電子部品", DEMO_SNAPSHOT_MONTH, 2600,  5_200_000, 2000, 2.6, 260),
    # --- 機械部品（カテゴリA）---
    ("ITM-0101", "機械部品_DEMO_精密ベアリングP",      "A", "機械部品", DEMO_SNAPSHOT_MONTH, 1200,  3_600_000, 3000, 3.5, 120),
    ("ITM-0102", "機械部品_DEMO_ステッピングモータQ",  "A", "機械部品", DEMO_SNAPSHOT_MONTH,  750,  2_625_000, 3500, 3.2,  95),
    # --- 素材・原料（カテゴリC）---
    ("ITM-0301", "素材・原料_DEMO_特殊合金粉末R",      "C", "素材・原料", DEMO_SNAPSHOT_MONTH, 4200,  2_100_000,  500, 3.0, 420),
]

print(f"追加予定のデモ品目数: {len(DEMO_OVERSTOCK_ROWS)} 件")
for row in DEMO_OVERSTOCK_ROWS:
    print(f"  {row[0]}: {row[1]} ({row[2]}) — 在庫金額=¥{row[7]:,} 回転率={row[8]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: inventory_summary テーブルへの MERGE

# COMMAND ----------

# デモデータを一時ビューとして作成
demo_schema = StructType([
    StructField("item_id",              StringType(),  False),
    StructField("item_name",            StringType(),  True),
    StructField("category",             StringType(),  True),
    StructField("category_name",        StringType(),  True),
    StructField("month",                DateType(),    True),
    StructField("avg_inventory_qty",    DoubleType(),  True),
    StructField("avg_inventory_value",  DoubleType(),  True),
    StructField("unit_price",           DoubleType(),  True),
    StructField("turnover_rate",        DoubleType(),  True),
    StructField("days_on_hand",         IntegerType(), True),
])

demo_rows = [
    Row(
        item_id=r[0], item_name=r[1], category=r[2], category_name=r[3],
        month=r[4],
        avg_inventory_qty=float(r[5]),
        avg_inventory_value=float(r[6]),
        unit_price=float(r[7]),
        turnover_rate=float(r[8]),
        days_on_hand=int(r[9]),
    )
    for r in DEMO_OVERSTOCK_ROWS
]

# max_inventory_qty / min_inventory_qty も列として必要なため計算して追加
from pyspark.sql.functions import col, lit, round as spark_round

demo_df = spark.createDataFrame(demo_rows, schema=demo_schema) \
    .withColumn("max_inventory_qty", (col("avg_inventory_qty") * 1.2).cast("double")) \
    .withColumn("min_inventory_qty", (col("avg_inventory_qty") * 0.8).cast("double"))

# 一時ビューに登録
demo_df.createOrReplaceTempView("demo_inventory_summary_insert")

print("一時ビュー作成完了: demo_inventory_summary_insert")
spark.sql("SELECT * FROM demo_inventory_summary_insert").show(5, truncate=False)

# COMMAND ----------

# inventory_summary へ MERGE（既存品目の最新月データを上書き or 新規追加）
merge_summary_sql = f"""
MERGE INTO {INVENTORY_SUMMARY_TABLE} AS target
USING demo_inventory_summary_insert AS source
  ON target.item_id = source.item_id
  AND target.month  = source.month
WHEN MATCHED THEN
  UPDATE SET
    target.item_name           = source.item_name,
    target.category            = source.category,
    target.category_name       = source.category_name,
    target.avg_inventory_qty   = source.avg_inventory_qty,
    target.avg_inventory_value = source.avg_inventory_value,
    target.max_inventory_qty   = source.max_inventory_qty,
    target.min_inventory_qty   = source.min_inventory_qty,
    target.turnover_rate       = source.turnover_rate,
    target.days_on_hand        = source.days_on_hand
WHEN NOT MATCHED THEN
  INSERT (
    item_id, item_name, category, category_name,
    month, avg_inventory_qty, avg_inventory_value,
    max_inventory_qty, min_inventory_qty,
    turnover_rate, days_on_hand
  )
  VALUES (
    source.item_id, source.item_name, source.category, source.category_name,
    source.month, source.avg_inventory_qty, source.avg_inventory_value,
    source.max_inventory_qty, source.min_inventory_qty,
    source.turnover_rate, source.days_on_hand
  )
"""

result = spark.sql(merge_summary_sql)
result.show()
print("✅ inventory_summary MERGE 完了")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: overstock_alert テーブルへの MERGE（回転率 < 4.0 の品目）

# COMMAND ----------

# overstock_alert はデモ品目を全件対象（全て turnover_rate < 4.0 を設計済み）
demo_df.createOrReplaceTempView("demo_overstock_insert")

merge_alert_sql = f"""
MERGE INTO {OVERSTOCK_ALERT_TABLE} AS target
USING (
  SELECT
    item_id, item_name, category, category_name,
    month, avg_inventory_qty, avg_inventory_value,
    turnover_rate, days_on_hand
  FROM demo_overstock_insert
  WHERE turnover_rate < 4.0
) AS source
  ON target.item_id = source.item_id
  AND target.month  = source.month
WHEN MATCHED THEN
  UPDATE SET
    target.item_name           = source.item_name,
    target.category            = source.category,
    target.category_name       = source.category_name,
    target.avg_inventory_qty   = source.avg_inventory_qty,
    target.avg_inventory_value = source.avg_inventory_value,
    target.turnover_rate       = source.turnover_rate,
    target.days_on_hand        = source.days_on_hand
WHEN NOT MATCHED THEN
  INSERT (
    item_id, item_name, category, category_name,
    month, avg_inventory_qty, avg_inventory_value,
    turnover_rate, days_on_hand
  )
  VALUES (
    source.item_id, source.item_name, source.category, source.category_name,
    source.month, source.avg_inventory_qty, source.avg_inventory_value,
    source.turnover_rate, source.days_on_hand
  )
"""

result = spark.sql(merge_alert_sql)
result.show()
print("✅ overstock_alert MERGE 完了")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: 投入結果の確認

# COMMAND ----------

print("=== 投入後の overstock_alert 件数 ===")
after_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {OVERSTOCK_ALERT_TABLE}").collect()[0]["cnt"]
print(f"  合計: {after_count:,} 件（追加前: {current_count:,} 件 → 増加: {after_count - current_count} 件）")

print("\n=== 追加されたデモ品目（最新月）===")
spark.sql(f"""
    SELECT
        item_id,
        item_name,
        category_name,
        FORMAT_NUMBER(avg_inventory_value, 0) AS 在庫金額,
        turnover_rate                          AS 回転率,
        days_on_hand                           AS 在庫日数
    FROM {OVERSTOCK_ALERT_TABLE}
    WHERE item_id LIKE 'ITM-0%'
      AND month = '{DEMO_SNAPSHOT_MONTH}'
    ORDER BY avg_inventory_value DESC
""").show(20, truncate=False)

print("\n=== カテゴリ別集計 ===")
spark.sql(f"""
    SELECT
        category_name,
        COUNT(DISTINCT item_id)                AS 品目数,
        FORMAT_NUMBER(SUM(avg_inventory_value), 0) AS 合計在庫金額,
        ROUND(AVG(turnover_rate), 2)           AS 平均回転率,
        ROUND(AVG(days_on_hand), 0)            AS 平均在庫日数
    FROM {OVERSTOCK_ALERT_TABLE}
    WHERE month = '{DEMO_SNAPSHOT_MONTH}'
    GROUP BY category_name
    ORDER BY SUM(avg_inventory_value) DESC
""").show(truncate=False)

print("\n🎉 サンプルデータの追加が完了しました！")
print("Genie で「過剰在庫のレポートを作成して」と質問してデモを確認してください。")
