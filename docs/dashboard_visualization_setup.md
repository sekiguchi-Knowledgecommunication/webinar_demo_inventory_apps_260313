# ダッシュボード Visualization 手動設定手順

Lakeview Dashboard API でダッシュボードをインポート後、**Visualization（チャートの軸・色の紐付け）は UI 上で手動設定が必要**です。

> [!IMPORTANT]
> Lakeview API の現時点の仕様では、`serialized_dashboard` 内の `spec.encodings` はデータとして保存されますが、
> **UIのVisualization レンダリングには反映されません**。データセット（SQL）やレイアウトは正しく反映されます。

---

## 前提

- `bash scripts/03_import_dashboard.sh` でダッシュボードがインポート済み
- データセットの SQL エラーが解消済み（各データセットで「Run」を押してデータが返ること）

---

## 設定手順

ダッシュボードを開き、各ウィジェットの右上メニュー（⋮）→「Edit」でウィジェット設定パネルを開きます。

### 1. KPI カウンター（3個）

| # | ウィジェット位置 | Dataset 選択 | Visualization | Value フィールド |
|:---|:---|:---|:---|:---|
| 1 | 左上 | `在庫KPI` | Counter | `total_value` |
| 2 | 中上 | `回転率KPI` | Counter | `avg_turnover` |
| 3 | 右上 | `過剰在庫KPI` | Counter | `overstock_count` |

**操作手順:**
1. ウィジェットをクリック → 右パネルが表示される
2. **Dataset**: ドロップダウンから上記データセットを選択
3. **Visualization**: `Counter` を選択
4. **Value**: `+` ボタンをクリック → 上記フィールドを選択

---

### 2. 在庫総額推移チャート（左中）

| 項目 | 設定値 |
|:---|:---|
| Dataset | `在庫推移` |
| Visualization | **Area** |
| X axis | `month` |
| Y axis | `avg_inventory_value`（集約: **SUM**） |
| Color | `category` |

---

### 3. カテゴリ別在庫構成チャート（右中）

| 項目 | 設定値 |
|:---|:---|
| Dataset | `カテゴリ別構成` |
| Visualization | **Bar** |
| X axis | `category` |
| Y axis | `total_value`（集約: **SUM**） |
| Color | `category` |

---

### 4. 過剰在庫アラートテーブル（中央下）

| 項目 | 設定値 |
|:---|:---|
| Dataset | `過剰在庫アラート` |
| Visualization | **Table** |
| Columns | `item_id`, `item_name`, `category`, `turnover_rate`, `days_on_hand`, `avg_inventory_value` |

**操作:** Dataset 選択後、「Table」を選択。表示するカラムを選択する。

---

### 5. カテゴリ別回転率推移チャート（左下）

| 項目 | 設定値 |
|:---|:---|
| Dataset | `回転率推移` |
| Visualization | **Line** |
| X axis | `month` |
| Y axis | `avg_turnover_rate`（集約: **AVG**） |
| Color | `category` |

---

### 6. サプライヤー別リードタイムチャート（右下）

| 項目 | 設定値 |
|:---|:---|
| Dataset | `サプライヤーリードタイム` |
| Visualization | **Bar**（横棒グラフ） |
| X axis | `avg_lead_time`（集約: **AVG**） |
| Y axis | `supplier_id` |
| Color | （設定不要） |

**Tips:** X と Y を入れ替えるボタン（↕）を使って横棒グラフにする。

---

## 設定後の確認

1. 全ウィジェットで「Run」を押してデータが表示されることを確認
2. 右上の「Publish」ボタンでダッシュボードを公開

---

## データセット一覧（SQL リファレンス）

| # | データセット名 | 参照テーブル | 集約 |
|:---|:---|:---|:---|
| 1 | 在庫KPI | `inventory_summary` | SUM, COUNT DISTINCT（最新月のみ） |
| 2 | 回転率KPI | `turnover_analysis` | AVG |
| 3 | 過剰在庫KPI | `overstock_alert` | COUNT DISTINCT（days_on_hand ≥ 90） |
| 4 | 在庫推移 | `inventory_summary` | GROUP BY month, category |
| 5 | カテゴリ別構成 | `inventory_summary` | GROUP BY category（最新月のみ） |
| 6 | 過剰在庫アラート | `overstock_alert` | days_on_hand ≥ 60、LIMIT 20 |
| 7 | 回転率推移 | `turnover_analysis` | ORDER BY month, category |
| 8 | サプライヤーリードタイム | `receipts` | GROUP BY supplier_id、LIMIT 15 |

全テーブルは `apps_demo_catalog.webinar_demo_0313` スキーマ内に存在します。
