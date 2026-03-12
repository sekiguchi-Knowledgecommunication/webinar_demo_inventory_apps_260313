"""
専用レポートツール — 在庫転用依頼書・緊急追加発注依頼書

フォーマットが固定されたExcel帳票を生成するため、
LLMには単純なスカラー値（品目名、数量など）のみを引数として渡させる。
内部で確実に正しいヘッダー・行構造を組み立て、report_tool に委譲する。
"""

import logging
import datetime
from agents import function_tool

# 非デコレータ版のコアロジックをインポート（Pythonから直接呼び出し可能）
from tools.report_tool import generate_report_raw

logger = logging.getLogger(__name__)


def generate_transfer_request_impl(
    item_id: str,
    item_name: str,
    source_location: str,
    target_location: str,
    transfer_qty: int,
    desired_delivery_date: str,
    reason: str,
) -> str:
    """
    「拠点間在庫転用依頼書」をExcel形式で生成する（素のPython関数）。
    app.py のデモモードからも直接呼び出し可能。
    """
    report_title = "拠点間在庫転用依頼書"
    headers = [
        "依頼日", "品番", "品名", "出庫元拠点",
        "入庫先拠点", "転用数量", "希望納入日", "手配理由"
    ]

    today_str = datetime.datetime.now().strftime("%Y/%m/%d")

    rows = [
        [
            today_str,
            item_id,
            item_name,
            source_location,
            target_location,
            str(transfer_qty),
            desired_delivery_date,
            reason
        ]
    ]

    summary = (
        f"[{source_location}] から [{target_location}] へ "
        f"{item_name} を {transfer_qty}個 転用するための依頼書です。"
        f"理由: {reason}"
    )

    return generate_report_raw(
        report_title=report_title,
        headers=headers,
        rows=rows,
        summary=summary
    )


@function_tool
def generate_transfer_request(
    item_id: str,
    item_name: str,
    source_location: str,
    target_location: str,
    transfer_qty: int,
    desired_delivery_date: str,
    reason: str,
) -> str:
    """
    不足品目に対して、他拠点からの「拠点間在庫転用依頼書」をExcel形式で生成する。
    在庫転用を提案する際には必ずこのツールを呼び出すこと。

    Args:
        item_id: 品目ID（例: "ITM-001"）
        item_name: 品目名（例: "機械部品_特注モーターX"）
        source_location: 出庫元拠点（例: "大阪倉庫"）
        target_location: 入庫先拠点（例: "東京工場"）
        transfer_qty: 転用する数量
        desired_delivery_date: 希望納入日（例: "2026/03/20"）
        reason: 手配理由（例: "東京工場の次週生産ロス回避のため"）

    Returns:
        生成されたExcelレポートのダウンロードリンク
    """
    return generate_transfer_request_impl(
        item_id, item_name, source_location,
        target_location, transfer_qty,
        desired_delivery_date, reason
    )


def generate_emergency_order_request_impl(
    item_id: str,
    item_name: str,
    order_qty: int,
    desired_delivery_date: str,
    delivery_location: str,
    priority: str,
    reason: str,
) -> str:
    """
    「追加発注依頼書（緊急）」をExcel形式で生成する（素のPython関数）。
    app.py のデモモードからも直接呼び出し可能。
    """
    report_title = "追加発注依頼書（緊急）"
    headers = [
        "起票日", "品番", "品名", "発注数量",
        "希望納入日", "納入先拠点", "優先度区分", "発注理由"
    ]

    today_str = datetime.datetime.now().strftime("%Y/%m/%d")

    rows = [
        [
            today_str,
            item_id,
            item_name,
            str(order_qty),
            desired_delivery_date,
            delivery_location,
            priority,
            reason
        ]
    ]

    summary = (
        f"緊急手配: {item_name} を {order_qty}個発注するための依頼書です。"
        f"希望納期: {desired_delivery_date}。理由: {reason}"
    )

    return generate_report_raw(
        report_title=report_title,
        headers=headers,
        rows=rows,
        summary=summary
    )


@function_tool
def generate_emergency_order_request(
    item_id: str,
    item_name: str,
    order_qty: int,
    desired_delivery_date: str,
    delivery_location: str,
    priority: str,
    reason: str,
) -> str:
    """
    緊急で不足している品目に対して、「追加発注依頼書（緊急）」をExcel形式で生成する。
    緊急の追加発注を提案する際には必ずこのツールを呼び出すこと。

    Args:
        item_id: 品目ID（例: "ITM-002"）
        item_name: 品目名（例: "電子部品_センサーY"）
        order_qty: 発注数量
        desired_delivery_date: 希望納入日（例: "2026/03/25"）
        delivery_location: 納入先拠点（例: "東京工場"）
        priority: 優先度区分（例: "特急" または "通常"）
        reason: 発注理由（例: "需要急増に伴う安全在庫割れのため"）

    Returns:
        生成されたExcelレポートのダウンロードリンク
    """
    return generate_emergency_order_request_impl(
        item_id, item_name, order_qty,
        desired_delivery_date, delivery_location,
        priority, reason
    )
