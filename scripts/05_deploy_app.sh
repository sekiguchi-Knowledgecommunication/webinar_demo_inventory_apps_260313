#!/bin/bash
# ============================================================
# inventory-demo-app を Databricks Apps にデプロイ（v3）
# ============================================================
set -e
export PATH="$HOME/.local/bin:$PATH"

APP_NAME="inventory-demo"
SOURCE_DIR="$(cd "$(dirname "$0")/.." && pwd)"
WS_PATH="/Users/s.sekiguchi7056@gmail.com/apps/$APP_NAME"

echo "========================================================"
echo " Databricks Apps デプロイ: $APP_NAME"
echo "========================================================"

# ワークスペースにディレクトリ作成
echo "📁 ワークスペースディレクトリ作成..."
databricks workspace mkdirs "$WS_PATH" 2>/dev/null || true

# ソースファイルをアップロード（--format AUTO でテキストファイルとして）
echo "📤 ソースファイルをアップロード中..."
for f in app.py app.yaml requirements.txt; do
    if [ -f "$SOURCE_DIR/$f" ]; then
        echo "  ↑ $f"
        databricks workspace import "$WS_PATH/$f" \
          --file "$SOURCE_DIR/$f" \
          --format AUTO \
          --overwrite 2>&1 || echo "    ⚠️ $f のアップロードに失敗"
    fi
done

# assets ディレクトリ
if [ -d "$SOURCE_DIR/assets" ]; then
    echo "  ↑ assets/"
    databricks workspace mkdirs "$WS_PATH/assets" 2>/dev/null || true
    for f in "$SOURCE_DIR/assets"/*; do
        if [ -f "$f" ]; then
            fname=$(basename "$f")
            echo "    ↑ $fname"
            databricks workspace import "$WS_PATH/assets/$fname" \
              --file "$f" --format AUTO --overwrite 2>&1 || true
        fi
    done
fi

echo ""
echo "📊 アップロード確認:"
databricks workspace list "$WS_PATH" 2>/dev/null || echo "  (確認失敗)"

# デプロイ
echo ""
echo "🚀 デプロイ実行..."
databricks apps deploy $APP_NAME --source-code-path "$WS_PATH" --no-wait

echo ""
echo "========================================================"
echo "✅ デプロイ完了！"
echo "========================================================"
echo ""
databricks apps get $APP_NAME 2>/dev/null || true
