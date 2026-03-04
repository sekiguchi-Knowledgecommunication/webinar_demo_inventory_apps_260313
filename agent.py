"""
在庫分析 AI エージェント — Databricks 公式パターン準拠

AsyncDatabricksOpenAI + OpenAI Agents SDK で構成。
Databricks Apps 内で直接実行（Serving Endpoint 不要）。
"""

import os
import logging
import pathlib

from databricks_openai import AsyncDatabricksOpenAI
from agents import Agent, ModelSettings, Runner, function_tool
from agents import set_default_openai_client, set_default_openai_api

logger = logging.getLogger(__name__)


# ====================
# Databricks AI Gateway 設定（公式パターン）
# ====================

# Databricks 専用 OpenAI クライアント（認証自動管理）
set_default_openai_client(AsyncDatabricksOpenAI())
set_default_openai_api("chat_completions")


# ====================
# システムプロンプト
# ====================

PROMPT_DIR = pathlib.Path(__file__).parent / "prompts"
PROMPT_PATH = PROMPT_DIR / "system_prompt.md"

try:
    SYSTEM_PROMPT = PROMPT_PATH.read_text(encoding="utf-8")
except FileNotFoundError:
    SYSTEM_PROMPT = "あなたは製造業の在庫管理に特化した分析エージェントです。日本語で応答してください。"


# ====================
# ツール定義
# ====================

# Genie ツール読み込み
from tools.genie_tool import query_genie


@function_tool
def query_inventory_data(question: str) -> str:
    """
    在庫データに自然言語で問い合わせる汎用ツール。
    Genie が SQL を自動生成・実行し、結果をテーブル形式で返す。
    在庫サマリ、過剰在庫アラート、回転率分析、リードタイム、
    需要予測と実績の比較など、あらゆる在庫データの問い合わせに使用する。
    """
    return query_genie(question)


@function_tool
def report_step(step_number: int, step_title: str, step_detail: str) -> str:
    """
    分析の各ステップをユーザーに通知するためのツール。
    データ取得や分析を開始する前に必ずこのツールを呼び出して、
    今から何をするかをユーザーに伝えること。

    Args:
        step_number: ステップ番号（1〜5）
        step_title: ステップのタイトル（例: "在庫サマリを取得"）
        step_detail: ステップの詳細説明（例: "カテゴリ別の在庫金額を確認します"）
    """
    logger.info(f"📍 Step {step_number}: {step_title} — {step_detail}")
    return f"✅ Step {step_number}: {step_title}"


# ====================
# モデル設定
# ====================

MODEL_NAME = os.environ.get(
    "AGENT_MODEL",
    "databricks-meta-llama-3-3-70b-instruct"
)


# ====================
# エージェント定義
# ====================

inventory_agent = Agent(
    name="在庫分析アシスタント",
    instructions=SYSTEM_PROMPT,
    tools=[
        query_inventory_data,
        report_step,
    ],
    model=MODEL_NAME,
    model_settings=ModelSettings(
        max_tokens=4096,
    ),
)
