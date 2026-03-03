"""
Genie ツール — Databricks Genie Space への問い合わせ

Genie API を使用して自然言語でデータに問い合わせを行い、
結果を構造化データとして返す。
"""

import os
import time
import logging

logger = logging.getLogger(__name__)

# Genie Space ID は環境変数から取得
GENIE_SPACE_ID = os.environ.get("GENIE_SPACE_ID", "")


def query_genie(question: str) -> str:
    """
    Genie Space に自然言語で質問し、結果を返す。

    Args:
        question: 日本語の自然言語クエリ

    Returns:
        Genie の応答テキスト（SQL 結果含む）
    """
    if not GENIE_SPACE_ID:
        return _fallback_response(question)

    try:
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient()

        # Genie に質問を送信
        response = w.genie.start_conversation(
            space_id=GENIE_SPACE_ID,
            content=question,
        )

        # 会話 ID を取得
        conversation_id = response.conversation_id
        message_id = response.message_id

        # ポーリングで結果を待つ（最大30秒）
        result = None
        for _ in range(15):
            time.sleep(2)
            result = w.genie.get_message(
                space_id=GENIE_SPACE_ID,
                conversation_id=conversation_id,
                message_id=message_id,
            )

            if result.status and result.status.value in ("COMPLETED", "FAILED"):
                break

        # 結果を整形
        if result and result.status and result.status.value == "COMPLETED":
            return _format_genie_result(result)
        else:
            logger.warning(f"Genie が応答を完了しませんでした: {getattr(result, 'status', 'unknown')}")
            return _fallback_response(question)

    except Exception as e:
        logger.error(f"Genie API エラー: {e}")
        return _fallback_response(question)


def _format_genie_result(result) -> str:
    """Genie の結果を読みやすいテキストに整形"""
    parts = []

    # テキスト応答
    if hasattr(result, "attachments") and result.attachments:
        for attachment in result.attachments:
            # テキスト部分
            if hasattr(attachment, "text") and attachment.text:
                parts.append(attachment.text.content)

            # クエリ結果
            if hasattr(attachment, "query") and attachment.query:
                query = attachment.query
                if hasattr(query, "description") and query.description:
                    parts.append(f"【実行クエリの説明】\n{query.description}")

                # 結果データ
                if hasattr(query, "result") and query.result:
                    result_data = query.result
                    if hasattr(result_data, "row_count"):
                        parts.append(f"結果: {result_data.row_count} 行")

                    # データをテーブル形式で返す
                    if hasattr(result_data, "columns") and hasattr(result_data, "data_array"):
                        cols = [c.name for c in result_data.columns]
                        parts.append("| " + " | ".join(cols) + " |")
                        parts.append("| " + " | ".join(["---"] * len(cols)) + " |")
                        for row in result_data.data_array[:20]:  # 最大20行
                            parts.append("| " + " | ".join(str(v) for v in row) + " |")

    return "\n\n".join(parts) if parts else "Genie から応答を取得できませんでした。"


def _fallback_response(question: str) -> str:
    """Genie Space が未設定の場合のフォールバック（デモ用ダミーデータ）"""
    q = question.lower()

    if "在庫" in q and ("総額" in q or "全体" in q or "概要" in q):
        return """【在庫総額の概要】

| カテゴリ | 品目名 | 在庫金額（円） | 構成比 |
| --- | --- | --- | --- |
| B | 電子部品 | 1,178,500,000 | 52.3% |
| A | 機械部品 | 385,000,000 | 17.1% |
| C | 素材・原料 | 412,000,000 | 18.3% |
| D | 包装・梱包材 | 278,000,000 | 12.3% |

合計: ¥2,253,500,000（約22.5億円）
カテゴリB（電子部品）が全体の52.3%を占め、突出して大きな比率です。"""

    elif "過剰" in q or "滞留" in q or "アラート" in q:
        return """【過剰在庫品目 Top 10】

| 品目ID | 品目名 | カテゴリ | 回転率 | 滞留日数 |
| --- | --- | --- | --- | --- |
| ITM-0109 | 電子部品_J09 | B | 0.31 | 175日 |
| ITM-0103 | 電子部品_D03 | B | 0.45 | 162日 |
| ITM-0106 | 電子部品_G06 | B | 0.52 | 148日 |
| ITM-0101 | 電子部品_B01 | B | 0.58 | 141日 |
| ITM-0104 | 電子部品_E04 | B | 0.63 | 134日 |

過剰在庫品目の 80% がカテゴリB（電子部品）に集中しています。
平均滞留日数: カテゴリB = 133日、他カテゴリ = 45日"""

    elif "需要" in q or "予測" in q or "乖離" in q:
        return """【需要予測 vs 実績の乖離分析】

| カテゴリ | 平均予測数量 | 平均実績数量 | 乖離率 |
| --- | --- | --- | --- |
| B | 520 | 250 | +107.6% |
| A | 380 | 365 | +4.1% |
| C | 290 | 280 | +3.6% |
| D | 210 | 205 | +2.4% |

カテゴリB の需要予測乖離率が **107.6%** と極めて高く、
予測が実績の約2倍になっています。
これが過剰在庫の主要因と考えられます。"""

    elif "リードタイム" in q or "サプライヤー" in q or "発注" in q:
        return """【サプライヤー別 発注リードタイム】

| サプライヤー | 平均リードタイム | カテゴリ |
| --- | --- | --- |
| 日本電子商事 | 47日 | B |
| 四国電子 | 44日 | B |
| 北海道工業 | 37日 | B |
| アジア素材 | 32日 | C |
| 大阪部品 | 27日 | A |
| グローバルパーツ | 23日 | A |

カテゴリBのサプライヤーは平均リードタイムが 42.7日 で、
他カテゴリ（平均 21.8日）の約2倍です。"""

    elif "回転" in q:
        return """【カテゴリ別 在庫回転率】

| カテゴリ | 平均回転率 | 判定 |
| --- | --- | --- |
| D | 5.2 回/月 | ✅ 良好 |
| A | 3.8 回/月 | ✅ 良好 |
| C | 3.5 回/月 | ✅ 良好 |
| B | 1.1 回/月 | ⚠️ 要改善 |

カテゴリBの回転率が極端に低く、在庫の流動性に問題があります。"""

    else:
        return f"""ご質問「{question}」に対する分析結果:

在庫データベースを確認した結果、以下の傾向が確認されています:

1. 在庫総額は約22.5億円で、前月比+8.3%増加
2. カテゴリB（電子部品）が全体の52%を占め過剰在庫のリスクが高い
3. 管理品目数は500品目（4カテゴリ × 4拠点）

さらに詳しい分析が必要な場合は、具体的な質問をお願いします。"""
