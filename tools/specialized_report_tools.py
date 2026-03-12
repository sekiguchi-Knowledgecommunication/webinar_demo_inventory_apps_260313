import logging
import datetime
from agents import function_tool

# 既存の汎用レポートツールをインポートして内部で利用する
from tools.report_tool import generate_report

logger = logging.getLogger(__name__)


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
    
    summary = f"[{source_location}] から [{target_location}] へ {item_name} を {transfer_qty}個 転用するための依頼書です。理由: {reason}"
    
    # 既存の汎用レポートツールに処理を委譲
    return generate_report(
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
    
    summary = f"緊急手配: {item_name} を {order_qty}個発注するための依頼書です。希望納期: {desired_delivery_date}。理由: {reason}"
    
    # 既存の汎用レポートツールに処理を委譲
    return generate_report(
        report_title=report_title,
        headers=headers,
        rows=rows,
        summary=summary
    )
