#!/bin/bash
export PATH="$HOME/.local/bin:$PATH"

# ペイロードからwarehouse_idとparent_pathを除去
python3 << 'PYEOF'
import json
with open("/tmp/dashboard_payload.json") as f:
    d = json.load(f)
d.pop("warehouse_id", None)
d.pop("parent_path", None)
with open("/tmp/dashboard_payload.json", "w") as out:
    json.dump(d, out, ensure_ascii=False)
print("Fixed: keys =", list(d.keys()))
PYEOF

echo "📊 ダッシュボードを新規作成中 (POST)..."
databricks api post /api/2.0/lakeview/dashboards --json @/tmp/dashboard_payload.json
