"""
在庫管理デモアプリ — Databricks Apps

シングルファイル構成（Dash 2.x 互換）
dcc.Tabs でページ切替を実装。
"""

import os
import json
import random
import logging
import re
import sys
import dash
from dash import Dash, html, dcc, callback, Input, Output, State, ALL, callback_context, no_update
from flask import send_file, request, ALL
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np

# ====================
# 定数・テーマ
# ====================
PLOTLY_THEME = {
    "paper_bgcolor": "rgba(255,255,255,0)",
    "plot_bgcolor": "rgba(255,255,255,0)",
    "font": {"color": "#374151", "family": "Noto Sans JP, sans-serif", "size": 12},
    "margin": {"l": 40, "r": 20, "t": 30, "b": 40},
    "xaxis": {"gridcolor": "rgba(0,0,0,0.06)", "zeroline": False},
    "yaxis": {"gridcolor": "rgba(0,0,0,0.06)", "zeroline": False},
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

GOOGLE_FONTS = "https://fonts.googleapis.com/css2?family=Noto+Sans+JP:wght@400;500;600;700;800&display=swap"


# ====================
# Scene 1: パイプライン概要
# ====================
def build_pipeline_page():
    """Scene 1: パイプライン概要ページ"""
    stats = get_medallion_stats()
    kpi = get_kpi_summary()

    stages = [
        {"name": "Raw Sources", "cls": "", "stats": "CSV / API"},
        {"name": "Bronze", "cls": "bronze",
         "stats": f'{stats["bronze"]["tables"]} テーブル / {stats["bronze"]["total_rows"]:,} 行'},
        {"name": "Silver", "cls": "silver",
         "stats": f'{stats["silver"]["tables"]} テーブル / {stats["silver"]["total_rows"]:,} 行'},
        {"name": "Gold", "cls": "gold",
         "stats": f'{stats["gold"]["tables"]} テーブル / {stats["gold"]["total_rows"]:,} 行'},
    ]

    flow = []
    for i, s in enumerate(stages):
        if i > 0:
            flow.append(html.Span("→", className="medallion-arrow"))
        flow.append(html.Div(className=f"medallion-stage {s['cls']}", children=[
            html.Div(s["name"], style={"fontWeight": "700", "color": "#1f2937", "fontSize": "1.05rem"}),
            html.Div(s["stats"], style={"fontSize": "0.75rem", "color": "#6b7280", "fontWeight": "600", "marginTop": "8px"}),
        ]))

    return html.Div([
        # ヒーローセクション（animate-reveal）
        html.Div(className="hero-section animate-reveal", children=[
            html.H1("在庫管理統合プラットフォーム", className="hero-title"),
            html.P("Databricks Lakehouse で在庫データを統合し、AI で課題の真因に切り込む", className="hero-subtitle"),
        ]),
        html.Div(style={"height": "24px"}),
        # Medallion Architecture パネル（animate-reveal）
        html.Div(className="chart-panel animate-reveal", children=[
            html.Div(className="chart-title", children=["Medallion Architecture"]),
            html.Div(className="medallion-flow", children=flow),
        ]),
        html.Div(style={"height": "16px"}),
        # パイプラインステータス（animate-reveal + KPI 非対称グリッド）
        html.Div(className="chart-panel animate-reveal", children=[
            html.Div(className="chart-title", children=["パイプラインステータス"]),
            html.Div(style={"display": "grid", "gridTemplateColumns": "repeat(4, 1fr)", "gap": "16px"}, children=[
                # メイン KPI: primary（2列幅で強調）
                html.Div(className="kpi-card primary", children=[
                    html.Div("パイプライン", className="kpi-label"),
                    html.Div("正常稼働", className="kpi-value", style={"color": "var(--color-accent-green)"}),
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
    "DATABRICKS_WORKSPACE_URL", ""
) or "https://dbc-e852234c-8d1e.cloud.databricks.com"
DASHBOARD_ID = os.environ.get(
    "DASHBOARD_ID", ""
) or "01f116d5e20611fb9cb1f97f9d1aa817"
# 公開ダッシュボードの埋め込み URL
EMBED_URL = f"{WORKSPACE_URL}/embed/dashboardsv3/{DASHBOARD_ID}?o=0"

# AI エージェント設定
logger = logging.getLogger(__name__)
try:
    from agent import inventory_agent
    import asyncio
    from agents import Runner
    AGENT_AVAILABLE = True
    print("\u2705 AI \u30a8\u30fc\u30b8\u30a7\u30f3\u30c8\u521d\u671f\u5316\u6210\u529f")
except Exception as e:
    AGENT_AVAILABLE = False
    print(f"\u26a0\ufe0f AI \u30a8\u30fc\u30b8\u30a7\u30f3\u30c8\u521d\u671f\u5316\u5931\u6557: {e}")
    import traceback
    traceback.print_exc()

def build_dashboard_page():
    """Scene 2: ダッシュボードページ（AI/BI ダッシュボード iframe）"""
    return html.Div([
        html.H2("在庫管理統合ビュー", className="animate-reveal",
                style={"fontWeight": "800", "color": "var(--color-text-primary)", "margin": "0 0 8px 0"}),
        html.P("Databricks AI/BI ダッシュボードを Dash App 内にリアルタイム表示",
               className="animate-reveal",
               style={"color": "var(--color-text-secondary)", "fontSize": "0.85rem", "margin": "0 0 16px 0"}),
        # iframe 埋め込み
        html.Div(className="chart-panel animate-reveal", style={"padding": "0", "overflow": "hidden"}, children=[
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
# メインレイアウト（タブ切替）
# ====================
app.layout = html.Div([
    html.Link(rel="stylesheet", href=GOOGLE_FONTS),
    # ヘッダー
    # ヘッダー（アイコン削除済み）
    html.Div(className="app-header", children=[
        html.Div(style={"display": "flex", "justifyContent": "space-between", "alignItems": "center",
                         "height": "60px", "padding": "0 24px"}, children=[
            html.Div(style={"display": "flex", "alignItems": "center", "gap": "12px"}, children=[
                html.Span("在庫管理統合プラットフォーム",
                          style={"fontSize": "1.1rem", "fontWeight": "700", "color": "var(--color-text-primary)"}),
                html.Span("DEMO", style={"fontSize": "0.7rem", "fontWeight": "600", "color": "var(--color-accent-primary)",
                                         "background": "#e0f2fe", "padding": "2px 8px", "borderRadius": "4px"}),
            ]),
        ]),
    ]),
    # タブナビゲーション（アイコン削除済み）
    html.Div(style={"padding": "0 24px", "maxWidth": "1400px", "margin": "0 auto"}, children=[
        dcc.Tabs(id="page-tabs", value="pipeline", style={"marginTop": "16px"}, children=[
            dcc.Tab(label="パイプライン概要", value="pipeline",
                    style={"color": "#6b7280", "backgroundColor": "transparent", "border": "none", "padding": "10px 20px"},
                    selected_style={"color": "var(--color-accent-primary)", "backgroundColor": "#f0f9ff",
                                    "border": "none", "borderBottom": "2px solid var(--color-accent-primary)", "padding": "10px 20px"}),
            dcc.Tab(label="ダッシュボード", value="dashboard",
                    style={"color": "#6b7280", "backgroundColor": "transparent", "border": "none", "padding": "10px 20px"},
                    selected_style={"color": "var(--color-accent-primary)", "backgroundColor": "#f0f9ff",
                                    "border": "none", "borderBottom": "2px solid var(--color-accent-primary)", "padding": "10px 20px"}),
            dcc.Tab(label="AI エージェント", value="agent",
                    style={"color": "#6b7280", "backgroundColor": "transparent", "border": "none", "padding": "10px 20px"},
                    selected_style={"color": "var(--color-accent-primary)", "backgroundColor": "#f0f9ff",
                                    "border": "none", "borderBottom": "2px solid var(--color-accent-primary)", "padding": "10px 20px"}),
        ]),
        html.Div(id="page-content", style={"paddingTop": "24px"}),
    ]),
    # フッター
    html.Div(style={"padding": "24px", "textAlign": "center", "borderTop": "1px solid #e5e7eb", "marginTop": "48px"},
             children=[html.Span("Powered by Databricks Lakehouse Platform", style={"fontSize": "0.85rem", "color": "#9ca3af"})]),
])


@callback(Output("page-content", "children"), Input("page-tabs", "value"))
def render_page(tab):
    if tab == "pipeline":
        return build_pipeline_page()
    elif tab == "dashboard":
        return build_dashboard_page()
    elif tab == "agent":
        return build_agent_page()
    return html.Div("ページが見つかりません")


# ====================
# Scene 3: AI エージェントチャット
# ====================
def build_agent_page():
    """Scene 3: AI エージェントチャット UI"""
    # ウェルカムカード（m-4: 空の状態デザイン改善）
    welcome_card = html.Div(className="chat-message assistant-msg", children=[
        html.Div(children=[
            # ウェルカムヘッダー
            html.Div(style={"marginBottom": "12px"}, children=[
                html.Div("在庫分析アシスタント",
                         style={"fontFamily": "var(--font-display)", "fontWeight": "700",
                                "fontSize": "1.1rem", "color": "var(--color-text-primary)", "marginBottom": "4px"}),
                html.Div("Genie API を活用し、在庫データの分析とインサイトの提供を行います。",
                         style={"fontSize": "0.85rem", "color": "var(--color-text-secondary)", "lineHeight": "1.6"}),
            ]),
            # サジェスチョンチップ
            html.Div("質問の例:", style={"fontSize": "0.75rem", "color": "#6b7280",
                                        "textTransform": "uppercase", "letterSpacing": "0.05em",
                                        "marginBottom": "8px", "fontWeight": "600"}),
            html.Div(style={"display": "flex", "gap": "8px", "flexWrap": "wrap"}, children=[
                html.Button(q, id={"type": "suggestion-btn", "index": i},
                            className="suggestion-chip",
                            style={"padding": "8px 16px", "borderRadius": "20px",
                                   "border": "1px solid #bae6fd",
                                   "background": "#f0f9ff", "color": "#0369a1",
                                   "fontSize": "0.8rem", "cursor": "pointer"})
                for i, q in enumerate([
                    "在庫の全体状況を分析して",
                    "過剰在庫のレポートを作成して",
                    "不足品目の発注提案を作成して",
                    "カテゴリ別の回転率は？",
                ])
            ]),
        ]),
    ])

    return html.Div([
        html.H2("在庫分析 AI エージェント", className="animate-reveal",
                style={"fontWeight": "800", "color": "var(--color-text-primary)", "margin": "0 0 4px"}),
        html.P("Genie API を活用し、自然言語で在庫データを分析",
               className="animate-reveal",
               style={"color": "var(--color-text-secondary)", "fontSize": "0.85rem", "margin": "0 0 16px"}),
        # チャットエリア（animate-reveal）
        html.Div(className="chart-panel animate-reveal",
                 style={"padding": "0", "display": "flex", "flexDirection": "column", "height": "70vh",
                        "borderLeft": "3px solid var(--color-accent-primary)"}, children=[
            # メッセージ表示エリア
            html.Div(id="chat-scroll-area", style={
                "flex": "1", "overflowY": "auto", "padding": "20px",
                "display": "flex", "flexDirection": "column", "gap": "16px",
            }, children=[
                html.Div(id="chat-history-ui", style={"display": "flex", "flexDirection": "column", "gap": "16px"}, children=[welcome_card]),
                html.Div(id="chat-temporary-ui", style={"display": "flex", "flexDirection": "column", "gap": "16px"}, children=[]),
            ]),
            # 入力エリア
            html.Div(style={
                "padding": "16px 20px",
                "borderTop": "1px solid rgba(255,255,255,0.08)",
                "display": "flex", "gap": "12px", "alignItems": "center",
            }, children=[
                dcc.Input(id="chat-input", type="text",
                          placeholder="在庫について質問してください...",
                          style={"flex": "1", "padding": "12px 16px",
                                 "background": "#f9fafb",
                                 "border": "1px solid #e5e7eb",
                                 "borderRadius": "12px", "color": "#1f2937",
                                 "fontSize": "0.95rem", "outline": "none"},
                          debounce=True),
                # 送信ボタン
                html.Button("送信", id="chat-send-btn",
                            style={"padding": "12px 24px",
                                   "color": "white", "border": "none", "borderRadius": "8px",
                                   "fontWeight": "600", "cursor": "pointer", "fontSize": "0.95rem"}),
            ]),
        ]),
        # チャット履歴保存用と非同期トリガー用
        dcc.Store(id="chat-history", data=[]),
        dcc.Store(id="trigger-agent", data=None),
    ])


@callback(
    Output("chat-temporary-ui", "children"),
    Output("trigger-agent", "data"),
    Output("chat-input", "value"),
    Input("chat-send-btn", "n_clicks"),
    Input("chat-input", "n_submit"),
    Input({"type": "suggestion-btn", "index": ALL}, "n_clicks"),
    State("chat-input", "value"),
    prevent_initial_call=True,
)
def handle_user_input(n_clicks, n_submit, suggestion_clicks, user_input):
    """ユーザー入力を受け取り、即座にUIに反映してLLMをトリガー"""
    ctx = callback_context
    if not ctx.triggered:
        return no_update, no_update, no_update
        
    trigger_id_str = ctx.triggered[0]["prop_id"].split(".")[0]
    question = ""
    
    # サジェスチョンボタンがクリックされた場合
    if "suggestion-btn" in trigger_id_str:
        try:
            import ast
            btn_id_dict = ast.literal_eval(trigger_id_str)
            idx = btn_id_dict["index"]
            suggestions = [
                "在庫の全体状況を分析して",
                "過剰在庫のレポートを作成して",
                "不足品目の発注提案を作成して",
                "カテゴリ別の回転率は？",
            ]
            question = suggestions[idx]
        except Exception as e:
            pass
    # 通常の送信ボタンまたはEnterキーの場合
    elif user_input and user_input.strip():
        question = user_input.strip()

    if not question:
        return no_update, no_update, no_update

    # ユーザーメッセージ
    user_msg = html.Div(className="chat-message user-msg", style={
        "alignSelf": "flex-end", "maxWidth": "75%",
    }, children=[
        html.Div(question, style={
            "background": "var(--color-accent-primary)",
            "padding": "12px 16px", "borderRadius": "16px 16px 4px 16px",
            "color": "white", "lineHeight": "1.6",
        }),
    ])

    # タイピングインジケーターを作成
    typing_indicator = html.Div(className="chat-message assistant-msg", style={
        "alignSelf": "flex-start", "maxWidth": "75%",
    }, children=[
        html.Div(className="typing-indicator", style={
            "background": "white", "border": "1px solid #e5e7eb",
            "padding": "12px 16px", "borderRadius": "16px 16px 16px 4px",
        }, children=[
            html.Span(""), html.Span(""), html.Span("")
        ])
    ])

    # 一時UIにメッセージを表示し、エージェントをトリガー
    return [user_msg, typing_indicator], {"question": question}, ""


@callback(
    Output("chat-history-ui", "children"),
    Output("chat-history", "data"),
    Output("chat-temporary-ui", "children", allow_duplicate=True),
    Input("trigger-agent", "data"),
    State("chat-history-ui", "children"),
    State("chat-history", "data"),
    prevent_initial_call=True,
)
def handle_agent_response(trigger_data, current_messages, history):
    """AI エージェントと通信して応答を得る"""
    if not trigger_data or "question" not in trigger_data:
        return no_update, no_update, no_update
        
    question = trigger_data["question"]

    # エージェントからの応答を取得
    agent_result = _call_agent(question, history)
    print(f"\U0001f916 エージェント応答: {agent_result[:100] if agent_result else 'None'}")
    sys.stdout.flush()

    # ユーザーメッセージ（履歴用に再作成）
    user_msg = html.Div(className="chat-message user-msg", style={
        "alignSelf": "flex-end", "maxWidth": "75%",
    }, children=[
        html.Div(question, style={
            "background": "var(--color-accent-primary)",
            "padding": "12px 16px", "borderRadius": "16px 16px 4px 16px",
            "color": "white", "lineHeight": "1.6",
        }),
    ])

    # レポートや発注提案のプレフィックスを解釈して特別なUIを描画
    children = []
    
    report_match = re.search(r'\[REPORT:(.*?)\]', agent_result)
    order_match = re.search(r'\[ORDER_PROPOSAL:(.*?)\]', agent_result)
    
    if report_match:
        filepath = report_match.group(1)
        filename = os.path.basename(filepath)
        clean_text = agent_result.replace(report_match.group(0), "").strip()
        
        children.append(html.Div(children=[
            html.Pre(clean_text, style={
                "whiteSpace": "pre-wrap", "wordWrap": "break-word",
                "fontFamily": "var(--font-body)", "fontSize": "0.9rem", "lineHeight": "1.7",
                "margin": "0 0 16px", "color": "var(--color-text-primary)",
            }),
            html.A(
                html.Div([
                    html.Span("📊 ", style={"fontSize": "1.2rem"}),
                    html.Span(f"{filename} をダウンロード", style={"fontWeight": "600"})
                ]),
                href=f"/download?file={filepath}",
                target="_blank",
                style={
                    "display": "inline-block", "padding": "12px 20px",
                    "background": "#f0fdf4", "border": "1px solid #bbf7d0",
                    "borderRadius": "8px", "color": "#166534", "textDecoration": "none",
                }
            )
        ]))
    elif order_match:
        proposal_id = order_match.group(1)
        clean_text = agent_result.replace(order_match.group(0), "").strip()
        
        children.append(html.Div(children=[
            html.Pre(clean_text, style={
                "whiteSpace": "pre-wrap", "wordWrap": "break-word",
                "fontFamily": "var(--font-body)", "fontSize": "0.9rem", "lineHeight": "1.7",
                "margin": "0 0 16px", "color": "var(--color-text-primary)",
            }),
            html.Div([
                html.Span("📝 発注提案登録完了", style={"fontWeight": "700", "color": "#075985"}),
                html.Br(),
                html.Span(f"提案ID: {proposal_id} - Delta テーブルに保存されました", style={"fontSize": "0.85rem", "color": "#0369a1"})
            ], style={
                "padding": "12px 16px", "background": "#f0f9ff",
                "borderLeft": "4px solid #0ea5e9", "borderRadius": "4px",
            })
        ]))
    else:
        children.append(html.Pre(agent_result, style={
            "whiteSpace": "pre-wrap", "wordWrap": "break-word",
            "fontFamily": "var(--font-body)", "fontSize": "0.9rem", "lineHeight": "1.7",
            "margin": "0", "color": "var(--color-text-primary)",
        }))

    # アシスタントメッセージ
    assistant_msg = html.Div(className="chat-message assistant-msg", children=[
        html.Div(children=children),
    ])

    # 履歴更新
    new_history = history + [{"role": "user", "content": question},
                             {"role": "assistant", "content": agent_result}]
    new_messages = current_messages + [user_msg, assistant_msg]

    # 履歴UIに追加し、一時UIはクリアする
    return new_messages, new_history, []


def _call_agent(question: str, history: list) -> str:
    """
    AI エージェントを直接呼び出す（公式パターン: AsyncDatabricksOpenAI）。
    Serving Endpoint を経由せず、アプリ内で Runner.run() を実行。
    エージェント未初期化時はフォールバック（ダミーデータ）。
    """
    if not AGENT_AVAILABLE:
        print("\u26a0\ufe0f AGENT_AVAILABLE=False, Genie フォールバック")
        from tools.genie_tool import query_genie
        return query_genie(question)

    try:
        import nest_asyncio
        nest_asyncio.apply()
        messages = history + [{"role": "user", "content": question}]
        print(f"\U0001f680 Runner.run 開始: {question[:50]}")
        result = asyncio.run(
            Runner.run(inventory_agent, input=messages)
        )
        print(f"\u2705 Runner.run 完了: final_output={result.final_output[:100] if result.final_output else 'None'}")
        return result.final_output or "エージェントからの応答が空です。もう一度お試しください。"
    except Exception as e:
        print(f"\u274c Runner.run エラー: {e}")
        import traceback
        traceback.print_exc()
        # フォールバック: Genie ツールを直接呼び出し
        logger.info("フォールバックモードで Genie を直接呼び出し")
        from tools.genie_tool import query_genie
        return query_genie(question)


server = app.server

@server.route("/download")
def download_file():
    filepath = request.args.get("file")
    if filepath and os.path.exists(filepath):
        from flask import send_file
        return send_file(filepath, as_attachment=True)
    return "File not found", 404

if __name__ == "__main__":
    port = int(os.environ.get("DATABRICKS_APP_PORT", 8050))
    # Dash 4.x は app.run、Dash 2.x は app.run_server
    try:
        app.run(host="0.0.0.0", port=port, debug=False)
    except Exception:
        app.run_server(host="0.0.0.0", port=port, debug=False)
