# Databricks CLI によるデータ準備の自動化手順

Databricks CLI を使用して、WSLローカルから一括で「データのアップロード」と「DLTパイプラインの作成」を行う手順です。

## 前提条件

1. Databricks ワークスペースの URL（例: `https://adb-123456789.x.azuredatabricks.net/`）
2. 個人用アクセス トークン（PAT）

> トークンの取得方法:
> Databricks 右上のユーザー名 > 「設定 (ユーザー設定)」> 「開発者」>「アクセス トークン」>「管理」>「新しいトークンを生成」

## 手順

以下の手順に沿ってスクリプトを実行してください。

### 1. 初期セットアップと構成

まず、Databricks CLI のインストールと認証設定を行います。

```bash
# スクリプトがあるディレクトリに移動
cd /home/ubuntu/work/03.databricks/00.macnica_webinar/00.260313_webinar/projects/webinar_0313/inventory-demo-app/scripts

# 1. 認証設定の実行
bash 01_setup_cli.sh
```

実行中に以下のプロンプトが表示されるので入力してください:
- **Databricks Host**: `https://<your-workspace-url>`
- **Personal Access Token**: `dapi...`
（※トークン入力時は画面に文字が表示されませんが、貼り付けて Enter を押してください）

### 2. データアップロードとパイプライン作成の実行

次に、CSV データのアップロードとパイプラインの作成を一括で実行します。

```bash
# 2. セットアップの実行
bash 02_deploy_data_pipeline.sh
```

このスクリプトは以下の情報を対話的に聞いてきます:
- **カタログ名**: デモ用データを格納する Unity Catalog 名（例: `prod_manufacturing` または使っているカタログ名）
- **ターゲットスキーマ**: テーブルを作成するデータベース名（例: `webinar_demo`）

### 3. DLTパイプラインの手動実行

スクリプト完了後、Databricks ワークスペースの UI で以下の操作を行ってください:

1. 左メニュー **Delta Live Tables** をクリック
2. 作成された `inventory_pipeline_xxx` を開く
3. 右上の **開始 (Start)** をクリック
4. 処理が完了するまで待機（すべてのテーブルが作成されるのを確認）

※ パイプラインの初回実行により、先ほどアップロードした CSV から Gold テーブルが自動生成されます。
