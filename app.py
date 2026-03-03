"""
在庫管理デモアプリ — Databricks Apps

シングルファイル構成（Dash 2.x 互換）
dcc.Tabs でページ切替を実装。
"""

import os
import random

import dash
from dash import Dash, html, dcc, callback, Input, Output, State, callback_context, no_update
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np

# ====================
# 定数・テーマ
# ====================
PLOTLY_THEME = {
    "paper_bgcolor": "rgba(0,0,0,0)",
    "plot_bgcolor": "rgba(0,0,0,0)",
    "font": {"color": "#e5e7eb", "family": "Space Grotesk, Noto Sans JP, sans-serif", "size": 12},
    "margin": {"l": 40, "r": 20, "t": 30, "b": 40},
    "xaxis": {"gridcolor": "rgba(255,255,255,0.05)", "zeroline": False},
    "yaxis": {"gridcolor": "rgba(255,255,255,0.05)", "zeroline": False},
}
CATEGORY_COLORS = {"A": "#0ea5e9", "B": "#ef4444", "C": "#10b981", "D": "#f59e0b"}
CATEGORY_NAMES = {"A": "機械部品", "B": "電子部品", "C": "素材・原料", "D": "包装・梱包材"}


# ====================
# ダミーデータ（ローカル/デモ用）
# ====================
def get_kpi_summary():
    return {"total_value": 151_000_000, "delta_pct": 8.3, "avg_turnover": 3.2,
            "overstock_count": 47, "total_items": 500}

def get_medallion_stats():
    return {"bronze": {"tables": 5, "total_rows": 73_500},
            "silver": {"tables": 5, "total_rows": 73_200},
            "gold": {"tables": 3, "total_rows": 12_800}}

def get_inventory_trend():
    np.random.seed(42)
    months = pd.date_range("2025-03-01", "2026-02-01", freq="MS")
    data = []
    for m in months:
        idx = (m.year - 2025) * 12 + m.month - 3
        for cat in ["A", "B", "C", "D"]:
            if cat == "B":
                val = int(45_000_000 * (1 + idx * 0.04) * np.random.uniform(0.9, 1.1))
            elif cat == "A":
                val = int(30_000_000 * np.random.uniform(0.95, 1.05))
            elif cat == "C":
                val = int(25_000_000 * np.random.uniform(0.92, 1.08))
            else:
                val = int(15_000_000 * np.random.uniform(0.98, 1.02))
            data.append({"month": m.strftime("%Y-%m-%d"), "category": cat, "total_value": val})
    return pd.DataFrame(data)

def get_category_breakdown():
    return pd.DataFrame([
        {"category": "B", "category_name": "電子部品", "total_value": 78_500_000, "item_count": 120},
        {"category": "A", "category_name": "機械部品", "total_value": 32_000_000, "item_count": 150},
        {"category": "C", "category_name": "素材・原料", "total_value": 26_000_000, "item_count": 130},
        {"category": "D", "category_name": "包装・梱包材", "total_value": 14_500_000, "item_count": 100},
    ])

def get_overstock_alerts():
    np.random.seed(42)
    alerts = []
    for i in range(10):
        cat = "B" if i < 7 else random.choice(["A", "C", "D"])
        alerts.append({"item_id": f"ITM-{100+i:04d}",
                       "item_name": f"{'電子' if cat == 'B' else '機械'}部品_{chr(65+i)}{i:02d}",
                       "category": cat,
                       "turnover_rate": round(np.random.uniform(0.3, 1.8), 2),
                       "days_on_hand": int(np.random.uniform(60, 180))})
    return pd.DataFrame(alerts).sort_values("days_on_hand", ascending=False)

def get_order_demand_gap():
    np.random.seed(42)
    data = []
    for i in range(40):
        cat = "B" if i < 20 else np.random.choice(["A", "C", "D"])
        forecast = int(np.random.uniform(100, 800))
        actual = int(forecast * (np.random.uniform(0.4, 0.7) if cat == "B" else np.random.uniform(0.85, 1.1)))
        data.append({"item_id": f"ITM-{200+i:04d}", "category": cat,
                     "forecast_qty": forecast, "actual_qty": actual,
                     "gap_rate_pct": round((forecast - actual) / max(actual, 1) * 100, 1),
                     "inventory_value": int(np.random.uniform(200_000, 4_000_000))})
    return pd.DataFrame(data)

def get_supplier_leadtime():
    suppliers = [("SUP-003", "日本電子商事", 47), ("SUP-010", "四国電子", 44),
                 ("SUP-009", "北海道工業", 37), ("SUP-004", "アジア素材", 32),
                 ("SUP-008", "大阪部品", 27), ("SUP-002", "グローバルパーツ", 23),
                 ("SUP-012", "東北素材", 21), ("SUP-006", "九州マテリアル", 19),
                 ("SUP-001", "東洋精密工業", 15), ("SUP-011", "信越精密", 13),
                 ("SUP-005", "中部金属", 11), ("SUP-007", "関東パッケージ", 8)]
    return pd.DataFrame([{"supplier_id": s[0], "supplier_name": s[1], "avg_lead_time": s[2]} for s in suppliers])


# ====================
# アプリ初期化
# ====================
app = Dash(__name__, suppress_callback_exceptions=True,
           title="在庫管理統合プラットフォーム | Databricks Lakehouse")

GOOGLE_FONTS = "https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;600;700;800&family=Noto+Sans+JP:wght@400;500;600;700;800&display=swap"


# ====================
# Scene 1: パイプライン概要
# ====================
def build_pipeline_page():
    stats = get_medallion_stats()
    kpi = get_kpi_summary()

    stages = [
        {"name": "Raw Sources", "cls": "", "icon": "📥", "stats": "CSV / API"},
        {"name": "Bronze", "cls": "bronze", "icon": "🥉",
         "stats": f'{stats["bronze"]["tables"]} テーブル / {stats["bronze"]["total_rows"]:,} 行'},
        {"name": "Silver", "cls": "silver", "icon": "🥈",
         "stats": f'{stats["silver"]["tables"]} テーブル / {stats["silver"]["total_rows"]:,} 行'},
        {"name": "Gold", "cls": "gold", "icon": "🥇",
         "stats": f'{stats["gold"]["tables"]} テーブル / {stats["gold"]["total_rows"]:,} 行'},
    ]

    flow = []
    for i, s in enumerate(stages):
        if i > 0:
            flow.append(html.Span("→", className="medallion-arrow"))
        flow.append(html.Div(className=f"medallion-stage {s['cls']}", children=[
            html.Div(s["icon"], style={"fontSize": "2rem", "marginBottom": "8px"}),
            html.Div(s["name"], style={"fontWeight": "700", "color": "white"}),
            html.Div(s["stats"], style={"fontSize": "0.75rem", "color": "#60a5fa", "fontWeight": "600", "marginTop": "8px"}),
        ]))

    return html.Div([
        html.Div(className="hero-section", children=[
            html.H1("在庫管理統合プラットフォーム", className="hero-title"),
            html.P("Databricks Lakehouse で在庫データを統合し、AI で課題の真因に切り込む", className="hero-subtitle"),
        ]),
        html.Div(style={"height": "24px"}),
        html.Div(className="chart-panel", children=[
            html.Div(className="chart-title", children=["🏗️ Medallion Architecture"]),
            html.Div(className="medallion-flow", children=flow),
        ]),
        html.Div(style={"height": "16px"}),
        html.Div(className="chart-panel", children=[
            html.Div(className="chart-title", children=["⚡ パイプラインステータス"]),
            html.Div(style={"display": "grid", "gridTemplateColumns": "repeat(3, 1fr)", "gap": "16px"}, children=[
                html.Div(className="kpi-card", children=[
                    html.Div("パイプライン", className="kpi-label"),
                    html.Div("正常稼働", className="kpi-value", style={"fontSize": "1.3rem", "color": "#10b981"}),
                ]),
                html.Div(className="kpi-card", children=[
                    html.Div("品質チェック合格率", className="kpi-label"),
                    html.Div("98.7%", className="kpi-value", style={"fontSize": "1.5rem"}),
                ]),
                html.Div(className="kpi-card", children=[
                    html.Div("管理品目数", className="kpi-label"),
                    html.Div(f'{kpi["total_items"]:,}', className="kpi-value", style={"fontSize": "1.5rem"}),
                ]),
            ]),
        ]),
    ])


# ====================
# Scene 2: ダッシュボード（Databricks AI/BI ダッシュボード iframe 埋め込み）
# ====================
# ダッシュボード設定
# ⚠️ DATABRICKS_HOST は Databricks Apps が自動設定するため使用不可
#    （アプリ自身の URL になり iframe が入れ子ループになる）
#    代わりに DATABRICKS_WORKSPACE_URL を使用する
WORKSPACE_URL = os.environ.get(
    "DATABRICKS_WORKSPACE_URL",
    "https://dbc-e852234c-8d1e.cloud.databricks.com"
)
DASHBOARD_ID = os.environ.get(
    "DASHBOARD_ID",
    "01f116d5e20611fb9cb1f97f9d1aa817"
)
# 公開ダッシュボードの埋め込み URL
EMBED_URL = f"{WORKSPACE_URL}/embed/dashboardsv3/{DASHBOARD_ID}?o=0"

def build_dashboard_page():
    return html.Div([
        html.H2("📊 在庫管理統合ビュー", style={"fontWeight": "800", "color": "white", "margin": "0 0 8px 0"}),
        html.P("Databricks AI/BI ダッシュボードを Dash App 内にリアルタイム表示",
               style={"color": "#9ca3af", "fontSize": "0.85rem", "margin": "0 0 16px 0"}),
        # iframe 埋め込み
        html.Div(className="chart-panel", style={"padding": "0", "overflow": "hidden"}, children=[
            html.Iframe(
                src=EMBED_URL,
                style={
                    "width": "100%",
                    "height": "85vh",
                    "border": "none",
                    "borderRadius": "12px",
                },
            ),
        ]),
    ])


# ====================
# Scene 5: まとめ
# ====================
def build_summary_page():
    return html.Div([
        html.H2("🎯 まとめ", style={"fontWeight": "800", "color": "white", "margin": "0 0 16px 0"}),
        html.Div(className="chart-panel", children=[
            html.Div(className="chart-title", children=["📋 本日のデモ"]),
            *[html.Div(style={"display": "flex", "gap": "16px", "padding": "12px", "borderRadius": "12px",
                               "background": "rgba(59,130,246,0.05)", "border": "1px solid rgba(59,130,246,0.1)",
                               "marginBottom": "8px"}, children=[
                html.Span("✅", style={"fontSize": "1.3rem"}),
                html.Div([html.Div(t, style={"fontWeight": "700", "color": "white"}),
                          html.Div(d, style={"fontSize": "0.85rem", "color": "#9ca3af"})]),
            ]) for t, d in [
                ("データ統合", "Medallion Architecture で Single Source of Truth を確立"),
                ("統合可視化", "部門横断ダッシュボードで在庫を一画面で俯瞰"),
                ("Genie: 自然言語探索", "SQL不要で日本語でデータを探索"),
                ("リサーチエージェント", "要因分析を自動化、アクション提案まで生成"),
            ]],
        ]),
        html.Div(style={"height": "16px"}),
        html.Div(className="cta-section", children=[
            html.Div("🎯", style={"fontSize": "3rem", "marginBottom": "16px"}),
            html.Div("ワークショップに参加しませんか？", style={"fontSize": "1.3rem", "fontWeight": "800", "color": "white"}),
            html.Div("貴社のデータで Databricks Lakehouse を半日で体験できます",
                     style={"color": "#9ca3af", "marginTop": "8px", "marginBottom": "24px"}),
            html.A("📝 ワークショップに申し込む", href="#", className="cta-button"),
        ]),
    ])


# ====================
# メインレイアウト（タブ切替）
# ====================
app.layout = html.Div([
    html.Link(rel="stylesheet", href=GOOGLE_FONTS),
    # ヘッダー
    html.Div(className="app-header", children=[
        html.Div(style={"display": "flex", "justifyContent": "space-between", "alignItems": "center",
                         "height": "60px", "padding": "0 24px"}, children=[
            html.Div(style={"display": "flex", "alignItems": "center", "gap": "12px"}, children=[
                html.Span("📦", style={"fontSize": "1.5rem"}),
                html.Span("在庫管理統合プラットフォーム", style={"fontSize": "1.1rem", "fontWeight": "700", "color": "white"}),
                html.Span("DEMO", style={"fontSize": "0.7rem", "fontWeight": "600", "color": "#60a5fa",
                                         "background": "rgba(59,130,246,0.15)", "padding": "2px 8px", "borderRadius": "4px"}),
            ]),
        ]),
    ]),
    # タブナビゲーション
    html.Div(style={"padding": "0 24px", "maxWidth": "1400px", "margin": "0 auto"}, children=[
        dcc.Tabs(id="page-tabs", value="pipeline", style={"marginTop": "16px"}, children=[
            dcc.Tab(label="🏠 パイプライン概要", value="pipeline",
                    style={"color": "#9ca3af", "backgroundColor": "transparent", "border": "none", "padding": "10px 20px"},
                    selected_style={"color": "#60a5fa", "backgroundColor": "rgba(59,130,246,0.1)",
                                    "border": "none", "borderBottom": "2px solid #3b82f6", "padding": "10px 20px"}),
            dcc.Tab(label="📊 ダッシュボード", value="dashboard",
                    style={"color": "#9ca3af", "backgroundColor": "transparent", "border": "none", "padding": "10px 20px"},
                    selected_style={"color": "#60a5fa", "backgroundColor": "rgba(59,130,246,0.1)",
                                    "border": "none", "borderBottom": "2px solid #3b82f6", "padding": "10px 20px"}),
            dcc.Tab(label="🤖 AI エージェント", value="agent",
                    style={"color": "#9ca3af", "backgroundColor": "transparent", "border": "none", "padding": "10px 20px"},
                    selected_style={"color": "#60a5fa", "backgroundColor": "rgba(59,130,246,0.1)",
                                    "border": "none", "borderBottom": "2px solid #3b82f6", "padding": "10px 20px"}),
            dcc.Tab(label="🎯 まとめ", value="summary",
                    style={"color": "#9ca3af", "backgroundColor": "transparent", "border": "none", "padding": "10px 20px"},
                    selected_style={"color": "#60a5fa", "backgroundColor": "rgba(59,130,246,0.1)",
                                    "border": "none", "borderBottom": "2px solid #3b82f6", "padding": "10px 20px"}),
        ]),
        html.Div(id="page-content", style={"paddingTop": "24px"}),
    ]),
    # フッター
    html.Div(style={"padding": "24px", "textAlign": "center", "borderTop": "1px solid rgba(255,255,255,0.05)", "marginTop": "48px"},
             children=[html.Span("Powered by Databricks Lakehouse Platform", style={"fontSize": "0.85rem", "color": "#6b7280"})]),
])


@callback(Output("page-content", "children"), Input("page-tabs", "value"))
def render_page(tab):
    if tab == "pipeline":
        return build_pipeline_page()
    elif tab == "dashboard":
        return build_dashboard_page()
    elif tab == "agent":
        return build_agent_page()
    elif tab == "summary":
        return build_summary_page()
    return html.Div("ページが見つかりません")


# ====================
# Scene 3: AI エージェントチャット
# ====================
def build_agent_page():
    """AI エージェントチャット UI"""
    initial_msg = html.Div(className="chat-message assistant-msg", children=[
        html.Div("🤖", style={"fontSize": "1.5rem", "marginRight": "12px"}),
        html.Div(children=[
            html.Strong("在庫分析アシスタント"),
            html.P("こんにちは！在庫データについて何でもお聞きください。", style={"margin": "4px 0 8px"}),
            html.Div(style={"display": "flex", "gap": "8px", "flexWrap": "wrap"}, children=[
                html.Button(q, id={"type": "suggestion-btn", "index": i},
                            className="suggestion-chip",
                            style={"padding": "6px 14px", "borderRadius": "20px",
                                   "border": "1px solid rgba(96,165,250,0.3)",
                                   "background": "rgba(59,130,246,0.08)", "color": "#93c5fd",
                                   "fontSize": "0.8rem", "cursor": "pointer"})
                for i, q in enumerate([
                    "在庫総額の概要を教えて",
                    "過剰在庫品目を見せて",
                    "需要予測との乖離を分析",
                    "カテゴリ別の回転率は？",
                ])
            ]),
        ]),
    ])

    return html.Div([
        html.H2("🤖 在庫分析 AI エージェント", style={"fontWeight": "800", "color": "white", "margin": "0 0 4px"}),
        html.P("Genie API を活用し、自然言語で在庫データを分析",
               style={"color": "#9ca3af", "fontSize": "0.85rem", "margin": "0 0 16px"}),
        # チャットエリア
        html.Div(className="chart-panel", style={"padding": "0", "display": "flex", "flexDirection": "column", "height": "70vh"}, children=[
            # メッセージ表示エリア
            html.Div(id="chat-messages", style={
                "flex": "1", "overflowY": "auto", "padding": "20px",
                "display": "flex", "flexDirection": "column", "gap": "16px",
            }, children=[initial_msg]),
            # 入力エリア
            html.Div(style={
                "padding": "16px 20px",
                "borderTop": "1px solid rgba(255,255,255,0.08)",
                "display": "flex", "gap": "12px", "alignItems": "center",
            }, children=[
                dcc.Input(id="chat-input", type="text",
                          placeholder="在庫について質問してください...",
                          style={"flex": "1", "padding": "12px 16px",
                                 "background": "rgba(255,255,255,0.05)",
                                 "border": "1px solid rgba(255,255,255,0.1)",
                                 "borderRadius": "12px", "color": "#e5e7eb",
                                 "fontSize": "0.95rem", "outline": "none"},
                          debounce=True),
                html.Button("送信", id="chat-send-btn",
                            style={"padding": "12px 24px",
                                   "background": "linear-gradient(135deg, #3b82f6, #8b5cf6)",
                                   "color": "white", "border": "none", "borderRadius": "12px",
                                   "fontWeight": "600", "cursor": "pointer", "fontSize": "0.95rem"}),
            ]),
        ]),
        # チャット履歴保存用
        dcc.Store(id="chat-history", data=[]),
    ])


@callback(
    Output("chat-messages", "children"),
    Output("chat-history", "data"),
    Output("chat-input", "value"),
    Input("chat-send-btn", "n_clicks"),
    Input("chat-input", "n_submit"),
    State("chat-input", "value"),
    State("chat-messages", "children"),
    State("chat-history", "data"),
    prevent_initial_call=True,
)
def handle_chat(n_clicks, n_submit, user_input, current_messages, history):
    """AI エージェントチャットのコールバック"""
    if not user_input or not user_input.strip():
        return no_update, no_update, no_update

    question = user_input.strip()

    # ユーザーメッセージを追加
    user_msg = html.Div(className="chat-message user-msg", style={
        "alignSelf": "flex-end", "maxWidth": "75%",
    }, children=[
        html.Div(question, style={
            "background": "linear-gradient(135deg, #3b82f6, #8b5cf6)",
            "padding": "12px 16px", "borderRadius": "16px 16px 4px 16px",
            "color": "white", "lineHeight": "1.6",
        }),
    ])

    # Genie API でデータ取得
    from tools.genie_tool import query_genie
    genie_result = query_genie(question)

    # アシスタントメッセージを作成
    assistant_msg = html.Div(className="chat-message assistant-msg", children=[
        html.Div("🤖", style={"fontSize": "1.5rem", "marginRight": "12px", "flexShrink": "0"}),
        html.Div(children=[
            html.Pre(genie_result, style={
                "whiteSpace": "pre-wrap", "wordWrap": "break-word",
                "fontFamily": "'Inter', 'Noto Sans JP', monospace",
                "fontSize": "0.9rem", "lineHeight": "1.7",
                "margin": "0", "color": "#e5e7eb",
            }),
        ]),
    ])

    # 履歴更新
    new_history = history + [{"role": "user", "content": question},
                             {"role": "assistant", "content": genie_result}]
    new_messages = current_messages + [user_msg, assistant_msg]

    return new_messages, new_history, ""


server = app.server

if __name__ == "__main__":
    port = int(os.environ.get("DATABRICKS_APP_PORT", 8050))
    # Dash 4.x は app.run、Dash 2.x は app.run_server
    try:
        app.run(host="0.0.0.0", port=port, debug=False)
    except Exception:
        app.run_server(host="0.0.0.0", port=port, debug=False)
