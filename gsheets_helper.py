
# rev: 2025-08-24 T01:10Z (修正版)
"""
Google Sheets 連携ユーティリティ
────────────────────────────────────────
必要な環境変数
  GS_SHEET_ID      : スプレッドシートの ID（URL の /d/ と /edit の間の長い文字列）
  ※シート（タブ）名が固定なら下の WS_NAME を書き換えるだけでも可
"""
import os, gspread, traceback
CREDS_PATH = "secrets/gs_creds.json"
SHEET_ID   = os.getenv("GS_SHEET_ID")          # Workflow から渡す
WS_NAME    = "system_cars"                     # ← タブ名をここで固定
_HEADERS = [
    "id","slug",
    "make_en","model_en","make_ja","model_ja",
    "body_type","fuel",
    "price_min_gbp","price_max_gbp",
    "price_min_jpy","price_max_jpy",
    "overview_en","overview_ja",
    "spec_json","media_urls","updated_at"
]

def _row(item: dict):              # DB 形式 → スプレッドシート 1 行
    return [item.get(h, "") for h in _HEADERS]

def _noop(*args, **kwargs): pass          # フォールバック用ダミー

try:
    # ── 認証 & シート取得 ─────────────────────────────
    if not (os.path.exists(CREDS_PATH) and SHEET_ID):
        raise RuntimeError("GS_CREDS_JSON または GS_SHEET_ID が未設定")
    
    gc = gspread.service_account(filename=CREDS_PATH)
    sheet = gc.open_by_key(SHEET_ID).worksheet(WS_NAME)
    
    # ── UPSERT ───────────────────────────────────────
    def upsert(item: dict):
        try:
            # slugでセルを検索
            cell = sheet.find(item["slug"])
            # 見つかった場合は行全体を更新
            sheet.update(
                f"A{cell.row}:{chr(64+len(_HEADERS))}{cell.row}",
                [_row(item)],
                value_input_option="USER_ENTERED"
            )
            print(f"GSHEETS UPDATED: {item['slug']} (row {cell.row})")
        except gspread.exceptions.APIError as e:
            # セルが見つからない場合（APIError として処理）
            if "Unable to parse range" in str(e) or "not found" in str(e).lower():
                # 新規行として追加
                sheet.append_row(_row(item), value_input_option="USER_ENTERED")
                print(f"GSHEETS ADDED: {item['slug']} (new row)")
            else:
                raise e
        except Exception as e:
            # その他のエラーの場合も新規追加を試行
            print(f"GSHEETS FIND ERROR for {item['slug']}: {e}")
            try:
                sheet.append_row(_row(item), value_input_option="USER_ENTERED")
                print(f"GSHEETS ADDED: {item['slug']} (fallback)")
            except Exception as append_error:
                print(f"GSHEETS APPEND ERROR for {item['slug']}: {append_error}")
                raise append_error
        
except Exception as e:
    print("⚠️  Google Sheets 連携をスキップ:", repr(e))
    traceback.print_exc()
    upsert = _noop
