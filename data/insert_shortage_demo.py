# Databricks notebook source
# MAGIC %md
# MAGIC # 【デモ用】欠品限界時・在庫転用デモ用データ追加スクリプト
# MAGIC
# MAGIC ## 方針
# MAGIC
# MAGIC ウェビナーデモの「追加発注 vs 在庫転用（横持ち）」シナリオを再現するためのデータを作成します。
# MAGIC 既存のDeltatableとは別に、デモ専用の `shortage_demo` テーブルを作成します。
# MAGIC
# MAGIC Genie Space にこのテーブルを追加することで、
# MAGIC エージェントが「特注モーターXが不足しそう」という質問に対して、
# MAGIC リードタイム判定と他倉庫の過剰在庫発見を行えるようになります。

# COMMAND ----------

# ========================================
# 設定: 実環境のカタログ・スキーマに合わせて変更
# ========================================
CATALOG = "apps_demo_catalog"   # ← 環境に合わせて変更
SCHEMA  = "webinar_demo_0313"   # ← 環境に合わせて変更

DEMO_TABLE = f"`{CATALOG}`.`{SCHEMA}`.`shortage_demo`"

print(f"ターゲットテーブル: {DEMO_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: デモ用データの定義

# COMMAND ----------

# デモ用品目データ（欠品シナリオ用）
# 問い合わせ対象：機械部品_特注モーターX (東京倉庫で不足、大阪倉庫で過剰)
DEMO_DATA = [
    # 問い合わせのメインターゲット
    ("ITM-S0001", "機械部品_特注モーターX", "東京倉庫", 100, 500, 45, "不足（来週分）", "A", "機械部品"),
    ("ITM-S0001", "機械部品_特注モーターX", "大阪倉庫", 600, 50,  45, "過剰（滞留中）", "A", "機械部品"),
    
    # ノイズデータ（他の品目）
    ("ITM-S0002", "電子部品_センサーY", "東京倉庫", 300, 200, 15, "適正", "B", "電子部品"),
    ("ITM-S0002", "電子部品_センサーY", "福岡倉庫", 50,  50,  15, "適正", "B", "電子部品"),
    ("ITM-S0003", "素材・原料_高強度樹脂Z", "名古屋倉庫", 1200, 1000, 30, "適正", "C", "素材・原料"),
    ("ITM-S0003", "素材・原料_高強度樹脂Z", "大阪倉庫", 1500, 1000, 30, "適正", "C", "素材・原料"),
]

print(f"追加予定のデモ品目数: {len(DEMO_DATA)} 件")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: shortage_demo テーブルの作成（CREATE OR REPLACE）

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
    location            STRING  COMMENT '倉庫・拠点名',
    current_stock       INT     COMMENT '現在庫数',
    required_qty        INT     COMMENT '直近の必要数量',
    lead_time_days      INT     COMMENT 'サプライヤーリードタイム（日）',
    status              STRING  COMMENT '在庫ステータス（不足/過剰/適正）',
    category            STRING  COMMENT 'カテゴリコード',
    category_name       STRING  COMMENT 'カテゴリ名'
)
COMMENT 'デモ用欠品・転用データ — AIエージェントの拠点間転用シナリオ用'
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

print("=== 投入データ一覧 ===")
spark.sql(f"""
    SELECT
        item_id,
        item_name,
        location                                         AS 拠点,
        current_stock                                    AS 現在庫,
        required_qty                                     AS 必要量,
        (current_stock - required_qty)                   AS 過不足,
        lead_time_days                                   AS リードタイム,
        status                                           AS ステータス
    FROM {DEMO_TABLE}
    ORDER BY item_id, location DESC
""").show(20, truncate=False)

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
# MAGIC    - `apps_demo_catalog`.`webinar_demo_0313`.`shortage_demo`
# MAGIC 5. **保存** して Genie を再起動
# MAGIC
# MAGIC 追加後、エージェントに「来週の生産で特注モーターXが不足しそう」と質問すると、デモシナリオが発動します。

print("\n🎉 セットアップ完了！")
print(f"作成テーブル: {DEMO_TABLE}")
print("次のステップ: 上記の[Step 4]を参照して Genie Space にテーブルを追加してください。")
