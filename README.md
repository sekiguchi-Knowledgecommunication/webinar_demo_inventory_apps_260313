# 在庫管理デモアプリ — Databricks Apps

マクニカ×KC 共催ウェビナー（2026.03.13）のデモ用アプリケーション。

## 技術スタック

- **フレームワーク**: Dash + dash-mantine-components (DMC)
- **チャート**: Plotly
- **プラットフォーム**: Databricks Apps（サーバーレス）
- **データ基盤**: Unity Catalog + DLT パイプライン

## 構成

```
├── app.py                  # メインエントリポイント
├── app.yaml                # Databricks Apps 実行構成
├── requirements.txt        # 依存関係
├── pages/                  # マルチページ
│   ├── pipeline.py         # Scene 1: パイプライン概要
│   ├── dashboard.py        # Scene 2: 在庫ダッシュボード
│   └── summary.py          # Scene 5: まとめ + CTA
├── utils/                  # ユーティリティ
├── assets/                 # CSS / 静的ファイル
├── data/                   # サンプルデータ生成
└── notebooks/              # DLT パイプライン
```

## ローカル起動

```bash
pip install -r requirements.txt
python app.py
# → http://localhost:8050/
```

## Databricks Apps デプロイ

Git リポジトリソースから Databricks Apps にデプロイ。
詳細は [Databricks Apps ドキュメント](https://docs.databricks.com/aws/ja/dev-tools/databricks-apps/) を参照。
