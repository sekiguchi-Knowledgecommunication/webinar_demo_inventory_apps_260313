#!/bin/bash
export PATH="$HOME/.local/bin:$PATH"

DASHBOARD_JSON="/home/ubuntu/work/03.databricks/00.macnica_webinar/00.260313_webinar/projects/webinar_0313/inventory-demo-app/dashboards/inventory_dashboard.lvdash.json"

echo "========================================================"
echo " ダッシュボード新規作成（UI内部形式準拠版）"
echo "========================================================"

# ユーザー情報取得
echo "👤 ユーザー情報取得中..."
USER_INFO=$(databricks current-user me 2>&1)
echo "$USER_INFO"
USER_NAME=$(echo "$USER_INFO" | python3 -c "import sys,json; print(json.load(sys.stdin).get('userName',''))" 2>/dev/null)
echo "User: $USER_NAME"

# .lvdash.json を内部形式に変換
python3 << PYEOF
import json

with open("$DASHBOARD_JSON") as f:
    dashboard = json.load(f)

# query → queryLines 変換
for ds in dashboard.get("datasets", []):
    if "query" in ds:
        lines = ds["query"].split("\n")
        ds["queryLines"] = [line + "\n" for line in lines[:-1]] + [lines[-1]]
        del ds["query"]

for page in dashboard.get("pages", []):
    page["pageType"] = "PAGE_TYPE_CANVAS"

dashboard["uiSettings"] = {
    "theme": {"widgetHeaderAlignment": "ALIGNMENT_UNSPECIFIED"},
    "applyModeEnabled": False
}

payload = {
    "display_name": "在庫管理ダッシュボード_デモ",
    "serialized_dashboard": json.dumps(dashboard, ensure_ascii=False),
    "warehouse_id": ""
}

with open("/tmp/dashboard_payload.json", "w") as out:
    json.dump(payload, out, ensure_ascii=False)

print("OK: payload saved (without parent_path)")
PYEOF

echo ""
echo "📊 ダッシュボードを新規作成中 (POST)..."
RESULT=$(databricks api post /api/2.0/lakeview/dashboards --json @/tmp/dashboard_payload.json 2>&1)
echo "Response: $RESULT"

echo ""
echo "========================================================"
echo "完了！ダッシュボードをブラウザで確認してください。"
echo "========================================================"
