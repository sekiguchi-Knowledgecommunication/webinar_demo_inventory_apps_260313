"""
発注提案ツール — Delta テーブルへの発注提案レコード書き込み

エージェントが在庫分析結果をもとに発注提案を生成し、
Databricks の Delta テーブルに INSERT する。
"""

import os
import datetime
import uuid
import logging
import sys

logger = logging.getLogger(__name__)


def create_order_proposal(
    item_id: str,
    item_name: str,
    category: str,
    current_stock: int,
    recommended_order_qty: int,
    reason: str,
    priority: str,
) -> str:
    """
    在庫分析に基づいて発注提案レコードを Delta テーブルに作成する。

    在庫不足リスクや回転率の低下が検出された品目に対して、
    具体的な発注数量と理由を記録する。

    Args:
        item_id: 品目ID（例: "ITM-0109"）
        item_name: 品目名（例: "電子部品_J09"）
        category: カテゴリ（A/B/C/D）
        current_stock: 現在の在庫数量
        recommended_order_qty: 推奨発注数量
        reason: 発注理由（例: "在庫回転率が0.31で基準値1.0を大幅に下回る"）
        priority: 優先度（HIGH / MEDIUM / LOW）

    Returns:
        書き込み結果のメッセージ
    """
    try:
        proposal_id = str(uuid.uuid4())[:8]
        created_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # SQL Warehouse 経由でテーブルに INSERT
        result = _insert_to_delta(
            proposal_id=proposal_id,
            created_at=created_at,
            item_id=item_id,
            item_name=item_name,
            category=category,
            current_stock=current_stock,
            recommended_order_qty=recommended_order_qty,
            reason=reason,
            priority=priority,
        )

        if result:
            print(f"📝 発注提案作成: {item_id} ({item_name}) - {recommended_order_qty}個")
            sys.stdout.flush()
            return (
                f"[ORDER_PROPOSAL:{proposal_id}]\n\n"
                f"📝 **発注提案を登録しました**\n\n"
                f"- **提案ID**: {proposal_id}\n"
                f"- **品目**: {item_id} ({item_name})\n"
                f"- **カテゴリ**: {category}\n"
                f"- **現在庫**: {current_stock:,} 個\n"
                f"- **推奨発注数**: {recommended_order_qty:,} 個\n"
                f"- **優先度**: {priority}\n"
                f"- **理由**: {reason}\n"
                f"- **ステータス**: PENDING（承認待ち）"
            )
        else:
            # フォールバック: テーブル書き込み失敗時もレスポンスは返す
            return _fallback_proposal(
                proposal_id, item_id, item_name, category,
                current_stock, recommended_order_qty, reason, priority
            )

    except Exception as e:
        logger.error(f"発注提案作成エラー: {e}")
        return _fallback_proposal(
            str(uuid.uuid4())[:8], item_id, item_name, category,
            current_stock, recommended_order_qty, reason, priority
        )


def _insert_to_delta(
    proposal_id, created_at, item_id, item_name, category,
    current_stock, recommended_order_qty, reason, priority
) -> bool:
    """SQL Warehouse 経由で Delta テーブルに INSERT する"""
    try:
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient()

        # テーブル名を取得
        catalog = os.environ.get("DATABRICKS_CATALOG", "apps_demo_catalog")
        schema = os.environ.get("DATABRICKS_SCHEMA", "webinar_demo_0313")
        table_name = f"{catalog}.{schema}.order_proposals"

        # INSERT SQL を構築
        sql = f"""
        INSERT INTO {table_name}
        (proposal_id, created_at, item_id, item_name, category,
         current_stock, recommended_order_qty, reason, priority, status, created_by)
        VALUES
        ('{proposal_id}', '{created_at}', '{item_id}', '{item_name}', '{category}',
         {current_stock}, {recommended_order_qty}, '{reason}', '{priority}', 'PENDING', 'ai-agent')
        """

        # SQL Warehouse で実行
        warehouse_id = os.environ.get("SQL_WAREHOUSE_ID", "")
        if not warehouse_id:
            logger.warning("SQL_WAREHOUSE_ID が未設定のため、フォールバックモードで動作")
            return False

        result = w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=sql,
        )

        logger.info(f"Delta テーブル INSERT 成功: proposal_id={proposal_id}")
        return True

    except Exception as e:
        logger.warning(f"Delta テーブル INSERT 失敗（フォールバック）: {e}")
        print(f"⚠️ Delta テーブル書き込み失敗（デモモードで動作）: {e}")
        sys.stdout.flush()
        return False


def _fallback_proposal(
    proposal_id, item_id, item_name, category,
    current_stock, recommended_order_qty, reason, priority
) -> str:
    """テーブル書き込み不可時のフォールバック応答"""
    return (
        f"📝 **発注提案を作成しました**（デモモード）\n\n"
        f"- **提案ID**: {proposal_id}\n"
        f"- **品目**: {item_id} ({item_name})\n"
        f"- **カテゴリ**: {category}\n"
        f"- **現在庫**: {current_stock:,} 個\n"
        f"- **推奨発注数**: {recommended_order_qty:,} 個\n"
        f"- **優先度**: {priority}\n"
        f"- **理由**: {reason}\n"
        f"- **ステータス**: PENDING（承認待ち）\n\n"
        f"_※ Delta テーブル未構成のためデモモードで動作しています_"
    )
