#!/bin/bash
set -e

# Databricks CLI インストールと認証をセットアップするスクリプト

echo "========================================================"
echo " Databricks CLI セットアップ"
echo "========================================================"

# 1. CLI のインストール確認と実行パスの設定
export PATH="$HOME/.local/bin:$PATH"

if ! command -v databricks &> /dev/null; then
    echo "⬇️ Databricks CLI をダウンロードしています..."
    
    if ! command -v unzip &> /dev/null; then
        echo "unzip コマンドが見つかりません。apt-get でインストールします。"
        sudo apt-get update && sudo apt-get install -y unzip curl
    fi

    # 最新リリースバージョンを取得
    VERSION=$(curl -s https://api.github.com/repos/databricks/cli/releases/latest | grep '"tag_name":' | sed -E 's/.*"v([^"]+)".*/\1/')
    
    if [ -z "$VERSION" ]; then
        echo "❌ バージョンの取得に失敗しました。時間をおいて再実行してください。"
        exit 1
    fi
    
    echo "⬇️ バージョン v${VERSION} をダウンロードしています..."
    curl -fsSL "https://github.com/databricks/cli/releases/download/v${VERSION}/databricks_cli_${VERSION}_linux_amd64.zip" -o databricks.zip
    
    unzip -o databricks.zip databricks
    mkdir -p ~/.local/bin
    mv databricks ~/.local/bin/
    rm databricks.zip
    
    # bashrc への追記
    if ! grep -q 'export PATH="$HOME/.local/bin:$PATH"' ~/.bashrc; then
        echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.bashrc
        echo "💡 ~/.bashrc に ~/.local/bin のパスを追記しました"
    fi
    
    echo "✅ Databricks CLI のインストールが完了しました"
else
    echo "✅ Databricks CLI は既にインストールされています"
    databricks --version
fi

echo ""
echo "🔑 認証プロファイル (DEFAULT) を設定します"
echo "Databricks のホストURLとトークン（PAT）を準備してください。"
echo ""

# 2. 認証設定
databricks configure

echo ""
echo "✅ セットアップ完了！"
echo "正しく認証されたか確認します..."
databricks current-user me

echo ""
echo "続いて 'bash 02_deploy_data_pipeline.sh' を実行してください。"
