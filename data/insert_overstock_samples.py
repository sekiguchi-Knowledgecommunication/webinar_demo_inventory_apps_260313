# Databricks notebook source
# MAGIC %md
# MAGIC # 【デモ用】過剰在庫サンプルデータ追加スクリプト
# MAGIC
# MAGIC ## 方針
# MAGIC
# MAGIC DLT パイプラインが管理する `inventory_summary` / `overstock_alert` は
# MAGIC Unity Catalog 上では **View** として公開されており、`MERGE` の書き込み先にできません。
# MAGIC
# MAGIC そのため、このスクリプトでは以下の方針を採用します。
# MAGIC
# MAGIC **➡ デモ専用の Delta テーブル `overstock_demo` を新規作成して Genie に認識させる**
# MAGIC
# MAGIC | テーブル | 用途 |
# MAGIC |---|---|
# MAGIC | `overstock_alert`（DLT管理・View） | 既存の本番データ（書き込み不可） |
# MAGIC | `overstock_demo`（今回作成） | デモ用追加品目を格納した書き込み可能な Delta テーブル |
# MAGIC
# MAGIC Genie Space の設定で `overstock_demo` テーブルを参照先に追加することで、
# MAGIC エージェントが「過剰在庫のレポートを作成して」と質問した際にデモ品目も返ってきます。

# COMMAND ----------

# ========================================
# 設定: 実環境のカタログ・スキーマに合わせて変更
# ========================================
CATALOG = "apps_demo_catalog"   # ← エラーで確認できたカタログ名
SCHEMA  = "webinar_demo_0313"   # ← エラーで確認できたスキーマ名

DEMO_TABLE = f"`{CATALOG}`.`{SCHEMA}`.`overstock_demo`"

print(f"ターゲットテーブル: {DEMO_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: デモ用過剰在庫データの定義

# COMMAND ----------

from datetime import date

# スナップショット月（最新月）
DEMO_SNAPSHOT_MONTH = "2026-02-01"

# デモ用品目データ（18件）
# 列: item_id, item_name, category, category_name, month,
#      avg_inventory_qty, avg_inventory_value, unit_price,
#      turnover_rate, days_on_hand, recommended_action
DEMO_DATA = [
    # --- 電子部品（カテゴリB）15件 ---
    ("ITM-D0201", "電子部品_マイコンチップA",     "B", "電子部品", DEMO_SNAPSHOT_MONTH, 1850, 14_800_000, 8000, 1.2, 185, "販売促進または廃棄検討"),
    ("ITM-D0202", "電子部品_センサーモジュールB",  "B", "電子部品", DEMO_SNAPSHOT_MONTH, 2200, 13_200_000, 6000, 0.9, 220, "発注停止・在庫消化優先"),
    ("ITM-D0203", "電子部品_電源ICチップC",        "B", "電子部品", DEMO_SNAPSHOT_MONTH, 3100,  9_920_000, 3200, 0.8, 310, "発注停止・在庫消化優先"),
    ("ITM-D0204", "電子部品_パワートランジスタD",  "B", "電子部品", DEMO_SNAPSHOT_MONTH, 4500,  9_000_000, 2000, 1.1, 450, "販売促進または廃棄検討"),
    ("ITM-D0205", "電子部品_高精度コンデンサE",    "B", "電子部品", DEMO_SNAPSHOT_MONTH, 6200,  7_440_000, 1200, 0.7, 620, "発注停止・廃棄検討"),
    ("ITM-D0206", "電子部品_RF通信モジュールF",    "B", "電子部品", DEMO_SNAPSHOT_MONTH, 1100,  6_600_000, 6000, 1.5, 110, "需要予測見直し"),
    ("ITM-D0207", "電子部品_LEDドライバICG",       "B", "電子部品", DEMO_SNAPSHOT_MONTH, 2800,  5_600_000, 2000, 1.3, 280, "発注ロット縮小"),
    ("ITM-D0208", "電子部品_メモリチップH",         "B", "電子部品", DEMO_SNAPSHOT_MONTH, 1600,  6_400_000, 4000, 2.1, 160, "需要予測見直し"),
    ("ITM-D0209", "電子部品_水晶発振子I",           "B", "電子部品", DEMO_SNAPSHOT_MONTH, 8000,  4_800_000,  600, 0.6, 800, "発注停止・廃棄検討"),
    ("ITM-D0210", "電子部品_フォトカプラJ",         "B", "電子部品", DEMO_SNAPSHOT_MONTH, 5500,  4_400_000,  800, 1.8, 550, "発注ロット縮小"),
    ("ITM-D0211", "電子部品_マグネティックセンサK", "B", "電子部品", DEMO_SNAPSHOT_MONTH, 2100,  6_300_000, 3000, 2.3, 210, "需要予測見直し"),
    ("ITM-D0212", "電子部品_熱電変換モジュールL",  "B", "電子部品", DEMO_SNAPSHOT_MONTH,  900,  5_400_000, 6000, 1.9,  90, "需要予測見直し"),
    ("ITM-D0213", "電子部品_デジタルポテンショM",  "B", "電子部品", DEMO_SNAPSHOT_MONTH, 3700,  3_700_000, 1000, 2.8, 370, "発注ロット縮小"),
    ("ITM-D0214", "電子部品_フラッシュメモリN",    "B", "電子部品", DEMO_SNAPSHOT_MONTH, 1300,  5_200_000, 4000, 3.1, 130, "需要予測見直し"),
    ("ITM-D0215", "電子部品_MPU加速度センサO",     "B", "電子部品", DEMO_SNAPSHOT_MONTH, 2600,  5_200_000, 2000, 2.6, 260, "発注ロット縮小"),
    # --- 機械部品（カテゴリA）2件 ---
    ("ITM-D0101", "機械部品_精密ベアリングP",       "A", "機械部品", DEMO_SNAPSHOT_MONTH, 1200,  3_600_000, 3000, 3.5, 120, "季節需要見直し"),
    ("ITM-D0102", "機械部品_ステッピングモータQ",   "A", "機械部品", DEMO_SNAPSHOT_MONTH,  750,  2_625_000, 3500, 3.2,  95, "季節需要見直し"),
    # --- 素材・原料（カテゴリC）1件 ---
    ("ITM-D0301", "素材・原料_特殊合金粉末R",       "C", "素材・原料", DEMO_SNAPSHOT_MONTH, 4200, 2_100_000,  500, 3.0, 420, "一括調達を分割発注に見直し"),
]

print(f"追加予定のデモ品目数: {len(DEMO_DATA)} 件")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: overstock_demo テーブルの作成（CREATE OR REPLACE）

# COMMAND ----------

# VALUES 句用に文字列を組み立て
def fmt_val(v):
    """Python 値を SQL リテラルに変換"""
    if isinstance(v, str):
        # シングルクォートをエスケープ
        return f"'{v.replace(chr(39), chr(39)*2)}'"
    return str(v)

rows_sql = ",\n  ".join(
    f"({', '.join(fmt_val(c) for c in row)})"
    for row in DEMO_DATA
)

create_sql = f"""
CREATE OR REPLACE TABLE {DEMO_TABLE} (
    item_id             STRING  COMMENT '品目ID',
    item_name           STRING  COMMENT '品目名',
    category            STRING  COMMENT 'カテゴリコード',
    category_name       STRING  COMMENT 'カテゴリ名',
    month               DATE    COMMENT 'スナップショット月',
    avg_inventory_qty   DOUBLE  COMMENT '平均在庫数量',
    avg_inventory_value DOUBLE  COMMENT '平均在庫金額（円）',
    unit_price          DOUBLE  COMMENT '単価（円）',
    turnover_rate       DOUBLE  COMMENT '在庫回転率',
    days_on_hand        INT     COMMENT '平均在庫日数',
    recommended_action  STRING  COMMENT '推奨アクション'
)
COMMENT 'デモ用過剰在庫データ — AIエージェントのレポート生成シナリオ用'
TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')
"""

spark.sql(create_sql)
print(f"✅ テーブル作成完了: {DEMO_TABLE}")

# COMMAND ----------

# デモデータをインサート
insert_sql = f"""
INSERT INTO {DEMO_TABLE}
VALUES
  {rows_sql}
"""

spark.sql(insert_sql)
print(f"✅ {len(DEMO_DATA)} 件のデモデータを挿入完了")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: 投入結果の確認

# COMMAND ----------

print("=== 投入データ一覧（在庫金額降順）===")
spark.sql(f"""
    SELECT
        item_id,
        item_name,
        category_name                                    AS カテゴリ,
        FORMAT_NUMBER(avg_inventory_value, 0)            AS 在庫金額,
        turnover_rate                                    AS 回転率,
        days_on_hand                                     AS 在庫日数,
        recommended_action                               AS 推奨アクション
    FROM {DEMO_TABLE}
    ORDER BY avg_inventory_value DESC
""").show(20, truncate=False)

# COMMAND ----------

print("=== カテゴリ別サマリ ===")
spark.sql(f"""
    SELECT
        category_name                                    AS カテゴリ,
        COUNT(*)                                         AS 品目数,
        FORMAT_NUMBER(SUM(avg_inventory_value), 0)       AS 合計在庫金額,
        ROUND(AVG(turnover_rate), 2)                     AS 平均回転率,
        ROUND(AVG(days_on_hand), 0)                      AS 平均在庫日数
    FROM {DEMO_TABLE}
    GROUP BY category_name
    ORDER BY SUM(avg_inventory_value) DESC
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Genie Space への追加案内
# MAGIC
# MAGIC **テーブルの作成が完了しました。**  
# MAGIC 以下の手順で Genie Space にこのテーブルを追加してください。
# MAGIC
# MAGIC 1. Databricks の左メニューから **Genie** を開く
# MAGIC 2. 対象の Genie Space を選択して **「設定（Settings）」** を開く
# MAGIC 3. **「Data（データ）」タブ** → **「テーブルを追加」**
# MAGIC 4. 以下のテーブルを追加する:
# MAGIC    - `apps_demo_catalog`.`webinar_demo_0313`.`overstock_demo`
# MAGIC 5. **保存** して Genie を再起動
# MAGIC
# MAGIC 追加後、エージェントに「過剰在庫のレポートを作成して」と質問すると  
# MAGIC デモ品目（ITM-D0201 〜 ITM-D0301）が返ってくることを確認してください。

print("\n🎉 セットアップ完了！")
print(f"作成テーブル: {DEMO_TABLE}")
print("次のステップ: 上記の[Step 4]を参照して Genie Space にテーブルを追加してください。")
