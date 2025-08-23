#!/usr/bin/env python3
# upsert.py – 2025-08-26 header-auto / array-safe

"""
期待する環境変数
────────────────
GS_CREDS_JSON : サービスアカウント JSON 文字列
GS_SHEET_ID   : 対象スプレッドシート ID
"""

import os, json, gspread
from google.oauth2.service_account import Credentials

# ───── Google 認証 ─────────────────────────
CREDS_PATH = "secrets/gs_creds.json"
os.makedirs("secrets", exist_ok=True)
if os.getenv("GS_CREDS_JSON") and not os.path.exists(CREDS_PATH):
    with open(CREDS_PATH, "w", encoding="utf-8") as f:
        f.write(os.getenv("GS_CREDS_JSON"))

SCOPE  = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
creds  = Credentials.from_service_account_file(CREDS_PATH, scopes=SCOPE)
gc     = gspread.authorize(creds)
ws     = gc.open_by_key(os.getenv("GS_SHEET_ID")).sheet1

# ───── ヘッダー確認 & 自動追加 ───────────────
NEEDED = [
    "id","slug","make_en","model_en","make_ja","model_ja",
    "body_type","body_type_ja","fuel","price_min_gbp","price_max_gbp",
    "price_min_jpy","price_max_jpy","overview_en","overview_ja","spec_json",
    "media_urls","catalog_url","doors","seats","dimensions_mm","drive_type",
    "grades","engines","full_model_ja","updated_at"
]

def _ensure_header() -> dict:
    header = {v: i + 1 for i, v in enumerate(ws.row_values(1))}
    missing = [c for c in NEEDED if c not in header]
    if missing:
        ws.insert_cols([missing], col=len(header) + 1)
        header = {v: i + 1 for i, v in enumerate(ws.row_values(1))}
    return header

HEADER = _ensure_header()

# ───── 値の整形 ────────────────────────────
def _ser(v):
    """配列 / dict は JSON 文字列化、それ以外はそのまま"""
    if isinstance(v, (list, dict)):
        return json.dumps(v, ensure_ascii=False)
    return "" if v is None else v

# ───── メイン関数 ──────────────────────────
def upsert(row: dict):
    slug_col = HEADER.get("slug")
    cell = ws.find(row["slug"]) if slug_col else None

    if cell:  # 更新
        for k, v in row.items():
            ws.update_cell(cell.row, HEADER[k], _ser(v))
    else:     # 追加
        ws.append_row(
            [_ser(row.get(h)) for h in HEADER.keys()],
            value_input_option="USER_ENTERED"
        )

# ───── CLI テスト用 ────────────────────────
if __name__ == "__main__":
    import sys
    with open(sys.argv[1], encoding="utf-8") as f:
        for line in f:
            upsert(json.loads(line))
    print("DONE → Google Sheet:", os.getenv("GS_SHEET_ID"))
