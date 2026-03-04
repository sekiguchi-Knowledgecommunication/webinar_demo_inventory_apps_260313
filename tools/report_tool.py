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
REPORT_DIR = "/Workspace/Users/s.sekiguchi7056@gmail.com/10.webinar/00.260313/webinar_demo"


from agents import function_tool

@function_tool
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
        # ファイル名の生成
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_report_title = report_title.replace(" ", "_").replace("/", "_").replace("\\", "_")[:30]
        filename = f"{safe_report_title}_{timestamp}.csv"
        filepath = os.path.join(REPORT_DIR, filename)

        # 1. ローカルファイルには書き込まず、メモリ上に CSV を作成する
        import io
        output = io.StringIO()
        writer = csv.writer(output)

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
            
        csv_bytes = output.getvalue().encode("utf-8-sig")

        # 2. Databricks SDK を使って Workspace に直接アップロード
        from databricks.sdk import WorkspaceClient
        import base64
        
        try:
            w = WorkspaceClient()
            
            # ディレクトリを作成（存在していてもエラーにはならない・再帰作成可能）
            try:
                w.workspace.mkdirs(REPORT_DIR)
            except Exception as d_err:
                logger.warning(f"ディレクトリ作成で例外が発生しましたが続行します: {d_err}")

            # アップロード実行: w.files.upload を使用して Workspace File としてアップロード
            try:
                # content は BinaryIO が期待されるため io.BytesIO でラップ
                w.files.upload(filepath, io.BytesIO(csv_bytes), overwrite=True)
            except Exception as inner_err:
                # フォールバック: import_ を使う
                import base64
                b64_str = base64.b64encode(csv_bytes).decode("utf-8")
                w.workspace.import_(path=filepath, content=b64_str, format="AUTO", language="PYTHON", overwrite=True)
                
        except Exception as upload_err:
            logger.error(f"Workspace へのアップロードに失敗しました: {upload_err}")
            return f"⚠️ レポートの保存に失敗しました: {upload_err}"

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
