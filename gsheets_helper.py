#!/usr/bin/env python3
# gsheets_helper.py – 2025-08-27 fixed A1 range & header sync

"""
Google Sheets 連携ユーティリティ
────────────────────────────────────
必須環境変数
  GS_CREDS_JSON : サービスアカウント JSON（一行）
  GS_SHEET_ID   : スプレッドシート ID
────────────────────────────────────
"""

import os, json, gspread, traceback
from gspread.utils import rowcol_to_a1

CREDS_PATH = "secrets/gs_creds.json"
SHEET_ID   = os.getenv("GS_SHEET_ID")      # Workflow env
WS_NAME    = "system_cars"                 # ← 固定タブ名

# ───────── スキーマ定義（Supabase と一致させる） ─────────
_HEADERS = [
    "id","slug",
    "make_en","model_en","make_ja","model_ja",
    "body_type","body_type_ja","fuel",
    "price_min_gbp","price_max_gbp",
    "price_min_jpy","price_max_jpy",
    "overview_en","overview_ja",
    "spec_json","media_urls",
    "catalog_url","doors","seats","dimensions_mm",
    "drive_type","grades","engines","colors",
    "full_model_ja","updated_at"
]

# ───────── util ─────────
def _dump(v):
    if isinstance(v, list):
        # media_urls だけは改行、それ以外カンマ
        sep = "\n" if all(u.startswith("http") for u in v) else ", "
        return sep.join(map(str, v))
    if isinstance(v, dict):
        return json.dumps(v, ensure_ascii=False)
    return "" if v is None else str(v)

def _row(item: dict):
    """payload → シート 1 行（配列）"""
    return [_dump(item.get(h)) for h in _HEADERS]

# ───────── Sheet 接続 ─────────
def _noop(*a, **kw): ...

try:
    if not (os.path.exists(CREDS_PATH) and SHEET_ID):
        raise RuntimeError("GS_CREDS_JSON または GS_SHEET_ID が未設定")

    gc     = gspread.service_account(filename=CREDS_PATH)
    ws     = gc.open_by_key(SHEET_ID).worksheet(WS_NAME)

    # ―― ヘッダを自動整合 ――――――――――――――――――――――――――
    def _ensure_header():
        cur = ws.row_values(1)
        if cur == _HEADERS:
            return
        if cur == []:
            ws.insert_row(_HEADERS, 1)
        else:
            # 差分があれば 1 行目を総入れ替え
            ws.update(rowcol_to_a1(1, 1) + ':' + rowcol_to_a1(1, len(_HEADERS)),
                      [_HEADERS], value_input_option="RAW")
        print("GSHEETS HEADER SYNCED")

    _ensure_header()

    # ───────── public: UPSERT ─────────
    def upsert(item: dict):
        try:
            # 1. slug を検索
            matches = ws.findall(item["slug"])
            if matches:
                r = matches[0].row
                # 2. 行更新
                rng = f"{rowcol_to_a1(r,1)}:{rowcol_to_a1(r,len(_HEADERS))}"
                ws.update(rng, [_row(item)], value_input_option="USER_ENTERED")
                print(f"GSHEETS UPDATED row {r}: {item['slug']}")
            else:
                # 3. 末尾追加
                ws.append_row(_row(item), value_input_option="USER_ENTERED")
                print(f"GSHEETS ADDED: {item['slug']}")
        except Exception as e:
            print(f"GSHEETS ERROR {item['slug']}: {e}")
            traceback.print_exc()

except Exception as e:
    print("⚠️  Google Sheets 連携をスキップ:", e)
    traceback.print_exc()
    upsert = _noop
