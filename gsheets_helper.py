#!/usr/bin/env python3
# gsheets_helper.py – 2025-08-27 full

"""
Google Sheets 連携ユーティリティ
────────────────────────────────────────
必要な環境変数
  GS_SHEET_ID      : スプレッドシートの ID
  （URL の /d/ と /edit の間にある長い文字列）
"""

import os, json, traceback, gspread
from typing import List

CREDS_PATH = "secrets/gs_creds.json"
SHEET_ID   = os.getenv("GS_SHEET_ID")          # GitHub Actions から渡す
WS_NAME    = "system_cars"                     # ← タブ名（固定して良い）

# ─────────────── シートの論理カラム定義 ───────────────
HEADERS: List[str] = [
    "id","slug",
    "make_en","model_en","make_ja","model_ja",
    "body_type","body_type_ja","fuel",
    "price_min_gbp","price_max_gbp",
    "price_min_jpy","price_max_jpy",
    "overview_en","overview_ja",
    "spec_json","media_urls",
    "catalog_url",
    "doors","seats","dimensions_mm","drive_type",
    "grades","engines","colors",
    "full_model_ja","updated_at"
]

# ─────────────── 共通ユーティリティ ───────────────
def _row(payload: dict) -> List[str]:
    """Supabase 用 dict → Sheets 1 行（文字列リスト）"""
    out = []
    for h in HEADERS:
        v = payload.get(h, "")

        # list → 改行区切り
        if isinstance(v, list):
            out.append("\n".join(map(str, v)))
        # dict → JSON 文字列
        elif isinstance(v, dict):
            out.append(json.dumps(v, ensure_ascii=False))
        # それ以外
        else:
            out.append("" if v is None else str(v))
    return out


def _ensure_header(ws):
    """ヘッダ行が無ければ作成、列不足があれば追記"""
    cur = ws.row_values(1)
    if cur == HEADERS:
        return

    if not cur:        # 1 行目が空
        ws.insert_row(HEADERS, 1)
        print("GSHEETS: ヘッダ行を新規作成しました")
    else:              # 既存ヘッダに不足列があれば右側へ追加
        diff = [c for c in HEADERS if c not in cur]
        if diff:
            ws.update(
                f"{chr(65+len(cur))}1:{chr(64+len(cur)+len(diff))}1",
                [diff]
            )
            print(f"GSHEETS: ヘッダに列を追加 → {diff}")


def _noop(*args, **kwargs): ...
upsert = _noop  # デフォルトは無効化しておく

# ─────────────── 認証 & シート準備 ───────────────
try:
    if not (os.path.exists(CREDS_PATH) and SHEET_ID):
        raise RuntimeError("GS_CREDS_JSON または GS_SHEET_ID が未設定")

    gc  = gspread.service_account(filename=CREDS_PATH)
    ws  = gc.open_by_key(SHEET_ID).worksheet(WS_NAME)
    _ensure_header(ws)

    # ────── 公開 API ──────
    def upsert(item: dict):
        """slug で検索 → あれば更新、無ければ append"""
        try:
            cells = ws.findall(item["slug"])
            if cells:                       # 既存行を更新
                row_idx = cells[0].row
                ws.update(
                    f"A{row_idx}:{chr(64+len(HEADERS))}{row_idx}",
                    [_row(item)],
                    value_input_option="USER_ENTERED"
                )
                print(f"GSHEETS UPDATED: {item['slug']} (row {row_idx})")
            else:                           # 末尾に追加
                ws.append_row(
                    _row(item),
                    value_input_option="USER_ENTERED"
                )
                print(f"GSHEETS ADDED: {item['slug']} (new row)")
        except Exception as e:
            print(f"GSHEETS ERROR for {item['slug']}: {e}")
            traceback.print_exc()

except Exception as e:
    print("⚠️  Google Sheets 連携をスキップ:", repr(e))
    traceback.print_exc()
