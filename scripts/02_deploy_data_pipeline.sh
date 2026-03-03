#!/bin/bash
set -e

# デモ用データアップロードとDLTパイプライン作成を自動化するスクリプト

echo "========================================================"
echo " デモ用データ基盤デプロイ"
echo "========================================================"

# パス設定
PROJECT_ROOT=$(realpath "$(dirname "$0")/..")
DATA_DIR="$PROJECT_ROOT/data/output"
NOTEBOOK_PATH="$PROJECT_ROOT/notebooks/dlt_pipeline.py"

# ターゲット設定 (固定値)
CATALOG_NAME="apps_demo_catalog"
SCHEMA_NAME="webinar_demo_0313"
VOLUME_NAME="landing_data"
BASE_VOLUME_PATH="/Volumes/$CATALOG_NAME/$SCHEMA_NAME/$VOLUME_NAME"

echo ""
echo "▶️ 実行プラン:"
echo "1. ターゲット: $CATALOG_NAME.$SCHEMA_NAME"
echo "2. Volumeパス: $BASE_VOLUME_PATH"
echo "3. データアップロード: $DATA_DIR -> $BASE_VOLUME_PATH"
echo "4. DLTパイプラインの作成"
echo ""

# ==========================================
# 1. Volume の存在確認
# ==========================================
echo "--------------------------------------------------------"
echo "1. 指定された Volume の確認..."

# Volume が存在するかだけ確認（エラー時はスキップして後続の databricks fs が失敗するのを待つか、ここでチェック）
# Free Edition 等の制限で CLI からの Volume 一覧取得が制限されている可能性も考慮し、
# 厳密なチェックはせずに単に事前通知のみ行います。

echo "💡 以下のパスにデータをアップロードします:"
echo "  $BASE_VOLUME_PATH"
echo "  ※事前にこの Volume が作成されている必要があります。"

# ==========================================
# 2. データのアップロード (Databricks fs)
# ==========================================
echo "--------------------------------------------------------"
echo "2. CSVデータのアップロード..."

# フォルダごとのアップロードマッピング
# ローカルファイル名 -> Volume上のディレクトリ名
declare -A FILE_MAP=(
    ["item_master.csv"]="item_master"
    ["raw_orders.csv"]="orders"
    ["raw_inventory.csv"]="inventory"
    ["raw_receipts.csv"]="receipts"
    ["raw_demand.csv"]="demand"
)

for file in "${!FILE_MAP[@]}"; do
    target_dir="${FILE_MAP[$file]}"
    local_file="$DATA_DIR/$file"
    remote_dir="$BASE_VOLUME_PATH/$target_dir"
    
    if [ -f "$local_file" ]; then
        echo "アップロード中: $file -> $remote_dir/"
        # フォルダ作成（存在しなくても --recursive などの仕様があるが、コマンドラインで直接ファイルを配置可能）
        # ※ dbfs cli ではなく databricks fs を使用
        databricks fs mkdir "dbfs:${remote_dir}" || true
        databricks fs cp "$local_file" "dbfs:${remote_dir}/$file" --overwrite
    else
        echo "⚠️ 警告: ローカルファイルが見つかりません ($local_file)"
    fi
done

# ==========================================
# 3. ノートブックのアップロード（パイプライン用）
# ==========================================
echo "--------------------------------------------------------"
echo "3. DLTノートブックのアップロード..."

WORKSPACE_USER=$(databricks current-user me | grep '"userName"' | cut -d '"' -f 4)
WORKSPACE_PATH="/Users/$WORKSPACE_USER/webinar_demo/dlt_pipeline"

# Workspace ディレクトリ作成とノートブックアップロード
databricks workspace mkdirs "/Users/$WORKSPACE_USER/webinar_demo" || true
databricks workspace import "$WORKSPACE_PATH" --file "$NOTEBOOK_PATH" --format SOURCE --language PYTHON --overwrite

# ==========================================
# 4. DLTパイプラインの作成
# ==========================================
echo "--------------------------------------------------------"
echo "4. DLTパイプラインの作成..."

PIPELINE_NAME="inventory_pipeline_demo_$(date +%s)"
PIPELINE_JSON="/tmp/pipeline_def.json"

cat << EOF > $PIPELINE_JSON
{
  "name": "$PIPELINE_NAME",
  "catalog": "$CATALOG_NAME",
  "target": "$SCHEMA_NAME",
  "continuous": false,
  "development": true,
  "serverless": true,
  "channel": "CURRENT",
  "libraries": [
    {
      "notebook": {
        "path": "$WORKSPACE_PATH"
      }
    }
  ],
  "configuration": {
    "dlt.landing_path": "$BASE_VOLUME_PATH"
  }
}
EOF

# パイプラインAPIのコール
PIPELINE_RESPONSE=$(databricks pipelines create --json @$PIPELINE_JSON)
PIPELINE_ID=$(echo $PIPELINE_RESPONSE | grep -o '"pipeline_id": *"[^"]*"' | grep -o '"[^"]*"$' | sed 's/"//g')

if [ -n "$PIPELINE_ID" ]; then
    echo "✅ DLTパイプラインが作成されました！"
    echo "Pipeline ID: $PIPELINE_ID"
    echo "Pipeline Name: $PIPELINE_NAME"
    echo ""
    echo "DatabricksのUI（Delta Live Tables）から '$PIPELINE_NAME' を探し、手動で[開始]を実行してください。"
else
    echo "❌ パイプライン作成でエラーが発生した可能性があります。"
    echo $PIPELINE_RESPONSE
fi

echo "========================================================"
echo "完了しました！"
echo "========================================================"
