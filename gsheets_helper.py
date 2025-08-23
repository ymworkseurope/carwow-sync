#!/usr/bin/env python3
# gsheets_helper.py – 2025-08-30 full
"""
Google Sheets 連携ユーティリティ
────────────────────────────────────────
必要な環境変数
  GS_SHEET_ID      : スプレッドシートの ID
  GS_CREDS_JSON    : サービスアカウント JSON （workflow 側で secrets に渡す）

備考
  • シート（タブ）名は WS_NAME で固定
  • 1 行目がヘッダー。足りない列は自動で挿入
  • slug が既存行にあれば UPDATE、無ければ APPEND
  • 26 列超え（AA, AB…）も自前で A1 参照を生成
"""
from __future__ import annotations
import os, json, gspread, traceback
from typing import Any

# ───────── 設定 ─────────
CREDS_PATH = "secrets/gs_creds.json"
SHEET_ID   = os.getenv("GS_SHEET_ID")
WS_NAME    = "system_cars"

HEADERS = [                    # Supabase と同じ順
    "id","slug",
    "make_en","model_en","make_ja","model_ja",
    "body_type","body_type_ja","fuel",
    "price_min_gbp","price_max_gbp","price_min_jpy","price_max_jpy",
    "overview_en","overview_ja",
    "spec_json","media_urls","catalog_url",
    "doors","seats","dimensions_mm","drive_type",
    "grades","engines","colors",
    "full_model_ja","updated_at",
]

# ──────────── util ────────────
def _col_letter(idx: int) -> str:
    """1-index → A,B,…,Z,AA,AB…"""
    s = ""
    while idx:
        idx, rem = divmod(idx-1, 26)
        s = chr(65+rem) + s
    return s

def _serialize(v: Any, col_name:str) -> str:
    """セル用文字列へ変換"""
    if v is None:       return ""
    if isinstance(v, (int,float)): return str(v)
    if isinstance(v, list):
        if col_name=="media_urls":
            return "\n".join(map(str,v))          # 改行区切り
        return ", ".join(map(str,v))              # カンマ区切り
    if isinstance(v, dict):
        return json.dumps(v, ensure_ascii=False)  # JSON 文字列
    return str(v)

def _row_dict_to_list(item:dict) -> list[str]:
    return [_serialize(item.get(h), h) for h in HEADERS]

def _ensure_header(ws):
    """ヘッダー列が不足していれば追加"""
    cur = ws.row_values(1)
    if cur==HEADERS: return
    if not cur:  # 空シート
        ws.insert_row(HEADERS, 1)
        return
    # 既存 + 追加
    need = [h for h in HEADERS if h not in cur]
    ws.update(f"A1:{_col_letter(len(cur)+len(need))}1", [cur+need])

# ──────────── main ────────────
def _noop(*a, **kw): ...

try:
    if not (os.path.exists(CREDS_PATH) and SHEET_ID):
        raise RuntimeError("GS_CREDS_JSON または GS_SHEET_ID が未設定")

    gc     = gspread.service_account(filename=CREDS_PATH)
    sheet  = gc.open_by_key(SHEET_ID)
    try:
        ws = sheet.worksheet(WS_NAME)
    except gspread.WorksheetNotFound:
        ws = sheet.add_worksheet(title=WS_NAME, rows="100", cols="50")

    _ensure_header(ws)

    def upsert(item: dict):
        try:
            slug = str(item["slug"])
            # B列（slug）全体を一度だけ取得
            slug_col = 2
            slugs = ws.col_values(slug_col)[1:]  # 2行目から
            try:
                idx = slugs.index(slug) + 2      # 行番号 (1-based)
            except ValueError:
                idx = None

            if idx:          # UPDATE
                start = f"A{idx}"
                end   = f"{_col_letter(len(HEADERS))}{idx}"
                ws.update(f"{start}:{end}", [_row_dict_to_list(item)],
                          value_input_option="USER_ENTERED")
                print(f"GSHEETS UPDATED: {slug} (row {idx})")
            else:            # APPEND
                ws.append_row(_row_dict_to_list(item),
                              value_input_option="USER_ENTERED")
                print(f"GSHEETS ADDED: {slug} (new row)")
        except Exception as e:
            print(f"GSHEETS ERROR for {item.get('slug')}: {e}")
            traceback.print_exc()
            raise e

except Exception as e:
    print("⚠️  Google Sheets 連携をスキップ:", repr(e))
    traceback.print_exc()
    upsert = _noop
