"""
サンプルデータ生成スクリプト — ウェビナーデモ用在庫データ

デモシナリオに沿った現実的なダミーデータを生成する。
カテゴリB（電子部品）に過剰在庫を意図的に集中させ、
Scene 4（リサーチエージェント）の分析結果と整合させる。

使用方法:
    python generate_sample_data.py                 # CSV出力（./output/）
    python generate_sample_data.py --output ./data  # 出力先指定
    python generate_sample_data.py --dry-run        # 件数のみ表示
"""

import os
import random
import argparse
from datetime import datetime, timedelta
from typing import Any

import pandas as pd
import numpy as np

# --- 定数定義 ---
RANDOM_SEED = 42
OUTPUT_DIR = "./output"

# 期間設定: 過去12ヶ月
END_DATE = datetime(2026, 2, 28)
START_DATE = datetime(2025, 3, 1)

# カテゴリ定義（日本語）
CATEGORIES = {
    "A": {"name": "機械部品", "item_count": 150, "unit_price_range": (500, 5000)},
    "B": {"name": "電子部品", "item_count": 120, "unit_price_range": (200, 8000)},
    "C": {"name": "素材・原料", "item_count": 130, "unit_price_range": (100, 3000)},
    "D": {"name": "包装・梱包材", "item_count": 100, "unit_price_range": (50, 1500)},
}

# 拠点定義
LOCATIONS = ["東京倉庫", "大阪倉庫", "名古屋倉庫", "福岡倉庫"]

# サプライヤー定義
SUPPLIERS = [
    {"id": f"SUP-{i:03d}", "name": name, "lead_time_days": lt}
    for i, (name, lt) in enumerate([
        ("東洋精密工業", 14), ("グローバルパーツ", 21), ("日本電子商事", 45),
        ("アジア素材", 30), ("中部金属", 10), ("九州マテリアル", 18),
        ("関東パッケージ", 7),  ("大阪部品", 25), ("北海道工業", 35),
        ("四国電子", 42), ("信越精密", 12), ("東北素材", 20),
    ], start=1)
]

# カテゴリB のサプライヤー（リードタイム長め）
CATEGORY_B_SUPPLIERS = [s for s in SUPPLIERS if s["lead_time_days"] >= 30]


def set_seed(seed: int = RANDOM_SEED) -> None:
    """再現性のためにシードを設定"""
    random.seed(seed)
    np.random.seed(seed)


def generate_item_master() -> pd.DataFrame:
    """
    品目マスタを生成（500件）
    カテゴリBの品目には長リードタイムのサプライヤーを割り当て
    """
    items = []
    item_id_counter = 1

    for cat_code, cat_info in CATEGORIES.items():
        for i in range(cat_info["item_count"]):
            item_id = f"ITM-{item_id_counter:04d}"
            item_name = f"{cat_info['name']}_{chr(65 + i // 26)}{i % 26 + 1:02d}"
            unit_price = round(
                random.uniform(*cat_info["unit_price_range"]), 0
            )

            # カテゴリBにはリードタイムの長いサプライヤーを優先割当
            if cat_code == "B":
                supplier = random.choice(CATEGORY_B_SUPPLIERS)
            else:
                supplier = random.choice(SUPPLIERS)

            items.append({
                "item_id": item_id,
                "item_name": item_name,
                "category": cat_code,
                "category_name": cat_info["name"],
                "unit_price": int(unit_price),
                "primary_supplier_id": supplier["id"],
                "lead_time_days": supplier["lead_time_days"],
                "safety_stock_days": random.choice([7, 14, 21, 30]),
                "reorder_point_qty": random.randint(50, 500),
                "created_at": (START_DATE - timedelta(days=random.randint(30, 365))).strftime("%Y-%m-%d"),
            })
            item_id_counter += 1

    return pd.DataFrame(items)


def generate_raw_orders(item_master: pd.DataFrame) -> pd.DataFrame:
    """
    発注データを生成（10,000件）
    カテゴリBは発注ロットが大きめ＋頻度が低い = 過剰在庫の要因
    """
    orders = []
    order_id_counter = 1
    date_range = pd.date_range(START_DATE, END_DATE, freq="D")

    for _ in range(10000):
        item = item_master.sample(1).iloc[0]
        order_date = random.choice(date_range)

        # カテゴリBは発注ロットが大きい（過剰の原因）
        if item["category"] == "B":
            qty = random.randint(200, 1000)  # 大ロット
        elif item["category"] == "A":
            qty = random.randint(50, 300)
        elif item["category"] == "C":
            qty = random.randint(100, 500)
        else:
            qty = random.randint(30, 200)

        orders.append({
            "order_id": f"ORD-{order_id_counter:06d}",
            "item_id": item["item_id"],
            "supplier_id": item["primary_supplier_id"],
            "order_qty": qty,
            "unit_price": int(item["unit_price"]),
            "order_amount": qty * int(item["unit_price"]),
            "order_date": order_date.strftime("%Y-%m-%d"),
            "expected_delivery_date": (order_date + timedelta(days=int(item["lead_time_days"]))).strftime("%Y-%m-%d"),
            "status": random.choice(["delivered", "delivered", "delivered", "in_transit", "pending"]),
        })
        order_id_counter += 1

    return pd.DataFrame(orders)


def generate_raw_inventory(item_master: pd.DataFrame) -> pd.DataFrame:
    """
    在庫スナップショットを生成（50,000件）
    月末スナップショット × 品目 × 拠点
    カテゴリBは在庫量が多く、トレンドが増加
    """
    inventory = []
    # 各月末日のリストを生成
    month_ends = pd.date_range(START_DATE, END_DATE, freq="ME")

    for snapshot_date in month_ends:
        month_idx = (snapshot_date.year - START_DATE.year) * 12 + (snapshot_date.month - START_DATE.month)

        # 各品目×拠点についてサンプリング
        sampled_items = item_master.sample(min(len(item_master), 350))

        for _, item in sampled_items.iterrows():
            for location in random.sample(LOCATIONS, k=random.randint(1, len(LOCATIONS))):
                # カテゴリBは在庫が増加傾向（過剰在庫の表現）
                if item["category"] == "B":
                    base_qty = random.randint(300, 1200)
                    # 月が進むにつれ在庫増加（トレンド）
                    trend_factor = 1.0 + (month_idx * 0.05)
                    seasonal_noise = random.uniform(0.8, 1.3)
                    qty = int(base_qty * trend_factor * seasonal_noise)
                elif item["category"] == "A":
                    base_qty = random.randint(100, 500)
                    qty = int(base_qty * random.uniform(0.7, 1.3))
                elif item["category"] == "C":
                    base_qty = random.randint(150, 600)
                    qty = int(base_qty * random.uniform(0.8, 1.2))
                else:
                    base_qty = random.randint(50, 300)
                    qty = int(base_qty * random.uniform(0.9, 1.1))

                inventory.append({
                    "item_id": item["item_id"],
                    "location_id": location,
                    "inventory_qty": qty,
                    "inventory_value": qty * item["unit_price"],
                    "snapshot_date": snapshot_date.strftime("%Y-%m-%d"),
                })

    df = pd.DataFrame(inventory)
    # 目標件数に近づけるためサンプリング
    if len(df) > 50000:
        df = df.sample(50000, random_state=RANDOM_SEED)
    return df.reset_index(drop=True)


def generate_raw_receipts(item_master: pd.DataFrame, orders: pd.DataFrame) -> pd.DataFrame:
    """
    入荷データを生成（8,000件）
    発注データから入荷実績を生成
    """
    delivered = orders[orders["status"] == "delivered"].copy()
    if len(delivered) > 8000:
        delivered = delivered.sample(8000, random_state=RANDOM_SEED)

    receipts = []
    for i, (_, order) in enumerate(delivered.iterrows()):
        # 入荷日は予定日 ± 数日のばらつき
        expected = datetime.strptime(order["expected_delivery_date"], "%Y-%m-%d")
        delay_days = random.randint(-2, 7)  # 最大7日遅れ
        receipt_date = expected + timedelta(days=delay_days)

        # 入荷数量は発注数量の 95〜105%（一部過不足）
        receipt_qty = int(order["order_qty"] * random.uniform(0.95, 1.05))

        receipts.append({
            "receipt_id": f"RCV-{i+1:06d}",
            "order_id": order["order_id"],
            "item_id": order["item_id"],
            "supplier_id": order["supplier_id"],
            "receipt_qty": receipt_qty,
            "receipt_date": receipt_date.strftime("%Y-%m-%d"),
            "lead_time_actual_days": (receipt_date - datetime.strptime(order["order_date"], "%Y-%m-%d")).days,
            "quality_status": random.choice(["passed", "passed", "passed", "passed", "inspection_required"]),
        })

    return pd.DataFrame(receipts)


def generate_raw_demand(item_master: pd.DataFrame) -> pd.DataFrame:
    """
    需要予測 vs 実績データを生成（5,000件）
    カテゴリBは予測精度が低い（乖離率が大きい）
    """
    demand = []
    months = pd.date_range(START_DATE, END_DATE, freq="MS")

    sampled_items = item_master.sample(min(len(item_master), 420), random_state=RANDOM_SEED)

    for month in months:
        month_items = sampled_items.sample(min(len(sampled_items), 420))
        for _, item in month_items.iterrows():
            base_demand = random.randint(50, 500)

            # カテゴリBは需要予測の精度が低い（乖離率が大きい）
            if item["category"] == "B":
                forecast_qty = int(base_demand * random.uniform(1.2, 1.8))  # 過大予測
                actual_qty = int(base_demand * random.uniform(0.5, 1.0))    # 実績は少ない
            elif item["category"] == "A":
                forecast_qty = int(base_demand * random.uniform(0.9, 1.15))
                actual_qty = int(base_demand * random.uniform(0.85, 1.1))
            elif item["category"] == "C":
                forecast_qty = int(base_demand * random.uniform(0.85, 1.2))
                actual_qty = int(base_demand * random.uniform(0.8, 1.15))
            else:
                forecast_qty = int(base_demand * random.uniform(0.9, 1.1))
                actual_qty = int(base_demand * random.uniform(0.88, 1.08))

            gap_rate = round((forecast_qty - actual_qty) / max(actual_qty, 1) * 100, 1)

            demand.append({
                "item_id": item["item_id"],
                "month": month.strftime("%Y-%m-%d"),
                "forecast_qty": forecast_qty,
                "actual_qty": actual_qty,
                "gap_qty": forecast_qty - actual_qty,
                "gap_rate_pct": gap_rate,
            })

    df = pd.DataFrame(demand)
    if len(df) > 5000:
        df = df.sample(5000, random_state=RANDOM_SEED)
    return df.reset_index(drop=True)


def main():
    parser = argparse.ArgumentParser(description="ウェビナーデモ用サンプルデータ生成")
    parser.add_argument("--output", default=OUTPUT_DIR, help="CSV出力ディレクトリ")
    parser.add_argument("--dry-run", action="store_true", help="件数のみ表示（ファイル出力なし）")
    args = parser.parse_args()

    set_seed()
    print("🔧 サンプルデータ生成を開始...")

    # 1. 品目マスタ（他テーブルの基盤）
    print("  📋 品目マスタ生成中...")
    item_master = generate_item_master()

    # 2. 発注データ
    print("  📦 発注データ生成中...")
    raw_orders = generate_raw_orders(item_master)

    # 3. 在庫スナップショット
    print("  🏭 在庫スナップショット生成中...")
    raw_inventory = generate_raw_inventory(item_master)

    # 4. 入荷データ
    print("  🚚 入荷データ生成中...")
    raw_receipts = generate_raw_receipts(item_master, raw_orders)

    # 5. 需要予測 vs 実績
    print("  📊 需要予測データ生成中...")
    raw_demand = generate_raw_demand(item_master)

    # 結果サマリ表示
    datasets = {
        "item_master": item_master,
        "raw_orders": raw_orders,
        "raw_inventory": raw_inventory,
        "raw_receipts": raw_receipts,
        "raw_demand": raw_demand,
    }

    print("\n✅ 生成完了:")
    print(f"  {'テーブル':<20} {'件数':>8}  {'カラム':>6}")
    print("  " + "-" * 40)
    for name, df in datasets.items():
        print(f"  {name:<20} {len(df):>8,}  {len(df.columns):>6}")

    # カテゴリ別の在庫サマリ（データ品質確認用）
    print("\n📈 カテゴリ別在庫サマリ（最新月）:")
    latest = raw_inventory[raw_inventory["snapshot_date"] == raw_inventory["snapshot_date"].max()]
    latest_with_cat = latest.merge(item_master[["item_id", "category", "category_name"]], on="item_id")
    cat_summary = latest_with_cat.groupby(["category", "category_name"]).agg(
        total_qty=("inventory_qty", "sum"),
        total_value=("inventory_value", "sum"),
        item_count=("item_id", "nunique"),
    ).reset_index()
    for _, row in cat_summary.iterrows():
        print(f"  {row['category']} ({row['category_name']}): "
              f"品目数={row['item_count']}, 数量={row['total_qty']:,}, "
              f"金額=¥{row['total_value']:,.0f}")

    # 需要予測精度サマリ
    print("\n🎯 カテゴリ別需要予測乖離率（平均）:")
    demand_with_cat = raw_demand.merge(item_master[["item_id", "category", "category_name"]], on="item_id")
    gap_summary = demand_with_cat.groupby(["category", "category_name"])["gap_rate_pct"].mean().reset_index()
    for _, row in gap_summary.iterrows():
        print(f"  {row['category']} ({row['category_name']}): 乖離率={row['gap_rate_pct']:.1f}%")

    if args.dry_run:
        print("\n⚠️  --dry-run モード: ファイル出力はスキップしました")
        return

    # CSV 出力
    os.makedirs(args.output, exist_ok=True)
    for name, df in datasets.items():
        filepath = os.path.join(args.output, f"{name}.csv")
        df.to_csv(filepath, index=False, encoding="utf-8")
        print(f"  💾 {filepath} ({len(df):,} 件)")

    print(f"\n🎉 全データの出力が完了しました → {args.output}/")


if __name__ == "__main__":
    main()
