#!/usr/bin/env python3
# upsert.py – 2025-08-26 full

import os, json, gspread
from google.oauth2.service_account import Credentials

# ───── 認証 ─────
CREDS_PATH = "secrets/gs_creds.json"
os.makedirs("secrets", exist_ok=True)
if os.getenv("GS_CREDS_JSON") and not os.path.exists(CREDS_PATH):
    with open(CREDS_PATH, "w", encoding="utf-8") as f:
        f.write(os.getenv("GS_CREDS_JSON"))

SCOPE = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
gc = gspread.authorize(
    Credentials.from_service_account_file(CREDS_PATH, scopes=SCOPE)
)
ws = gc.open_by_key(os.getenv("GS_SHEET_ID")).sheet1

# ───── ヘッダー確保 ─────
NEEDED = [
    "id","slug","make_en","model_en","make_ja","model_ja",
    "body_type","body_type_ja","fuel","price_min_gbp","price_max_gbp",
    "price_min_jpy","price_max_jpy","overview_en","overview_ja","spec_json",
    "media_urls","catalog_url","doors","seats","dimensions_mm","drive_type",
    "grades","engines","colors","full_model_ja","updated_at"
]

def _ensure_header() -> dict:
    header = {v: i + 1 for i, v in enumerate(ws.row_values(1))}
    miss = [c for c in NEEDED if c not in header]
    if miss:
        ws.insert_cols([miss], col=len(header) + 1)
        header = {v: i + 1 for i, v in enumerate(ws.row_values(1))}
    return header

HEADER = _ensure_header()

# ───── シリアライズ ─────
def _cell(v):
    if isinstance(v, list):
        return ", ".join(map(str, v))
    if isinstance(v, dict):
        return json.dumps(v, ensure_ascii=False)
    return "" if v is None else v

# ───── upsert  ─────
def upsert(row: dict):
    cell = ws.find(row["slug"]) if HEADER.get("slug") else None
    if cell:  # update
        for k, v in row.items():
            ws.update_cell(cell.row, HEADER[k], _cell(v))
    else:     # insert
        ws.append_row([_cell(row.get(h)) for h in HEADER.keys()],
                      value_input_option="USER_ENTERED")

# CLI 使い捨て
if __name__ == "__main__":
    import sys
    with open(sys.argv[1], encoding="utf-8") as f:
        for line in f:
            upsert(json.loads(line))
