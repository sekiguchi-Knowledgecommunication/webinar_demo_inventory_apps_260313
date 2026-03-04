"""
レポート自動生成ツール — 分析結果を CSV ファイルとして保存

エージェントが分析結果を構造化レポートとしてまとめ、
CSV ファイルに保存してダウンロード用パスを返す。
"""

import os
import csv
import datetime
import logging

logger = logging.getLogger(__name__)

# レポート保存ディレクトリ
REPORT_DIR = "/tmp/reports"


def generate_report(
    report_title: str,
    headers: list,
    rows: list,
    summary: str,
) -> str:
    """
    分析結果を CSV レポートとして保存する。

    エージェントが Genie で取得したデータを整理し、
    ユーザーがダウンロードできる CSV ファイルとして出力する。

    Args:
        report_title: レポートのタイトル（例: "過剰在庫分析レポート"）
        headers: CSV のヘッダー行（例: ["品目ID", "品目名", "在庫金額"]）
        rows: データ行のリスト（例: [["ITM-001", "電子部品A", "1200000"], ...]）
        summary: レポートのサマリテキスト

    Returns:
        レポートファイルのパスと概要を含むメッセージ
    """
    try:
        # ディレクトリ作成
        os.makedirs(REPORT_DIR, exist_ok=True)

        # ファイル名にタイムスタンプを付与
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_title = report_title.replace(" ", "_").replace("/", "_")[:30]
        filename = f"{safe_title}_{timestamp}.csv"
        filepath = os.path.join(REPORT_DIR, filename)

        # CSV 書き込み
        with open(filepath, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)

            # サマリをコメント行として追加
            writer.writerow([f"# レポート: {report_title}"])
            writer.writerow([f"# 作成日時: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"])
            writer.writerow([f"# 作成者: AI エージェント"])
            writer.writerow([])

            # ヘッダーとデータ
            if headers:
                writer.writerow(headers)
            for row in rows:
                writer.writerow(row)

        row_count = len(rows)
        logger.info(f"📊 レポート生成完了: {filepath} ({row_count} 行)")

        return (
            f"[REPORT:{filepath}]\n\n"
            f"📊 **レポートを生成しました**\n\n"
            f"- **タイトル**: {report_title}\n"
            f"- **データ件数**: {row_count} 件\n"
            f"- **ファイル**: {filename}\n\n"
            f"**サマリ**: {summary}"
        )

    except Exception as e:
        logger.error(f"レポート生成エラー: {e}")
        return f"⚠️ レポートの生成中にエラーが発生しました: {e}"
