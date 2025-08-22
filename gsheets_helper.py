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
    row = []
    for h in _HEADERS:
        value = item.get(h, "")
        
        # リスト型のデータを文字列に変換
        if isinstance(value, list):
            if h == "media_urls":
                # media_urlsは改行区切りで結合
                value = "\n".join(str(url) for url in value)
            else:
                # その他のリストはカンマ区切り
                value = ", ".join(str(v) for v in value)
        
        # 辞書型のデータをJSON文字列に変換
        elif isinstance(value, dict):
            import json
            value = json.dumps(value, ensure_ascii=False)
        
        # その他の型は文字列に変換
        else:
            value = str(value) if value is not None else ""
        
        row.append(value)
    
    return row

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
            # slugでセルを検索（B列のslugを検索）
            try:
                # slugが格納されているB列から検索
                slug_cells = sheet.findall(item["slug"])
                if slug_cells:
                    cell = slug_cells[0]  # 最初に見つかったセルを使用
                    # 見つかった場合は行全体を更新
                    sheet.update(
                        f"A{cell.row}:{chr(64+len(_HEADERS))}{cell.row}",
                        [_row(item)],
                        value_input_option="USER_ENTERED"
                    )
                    print(f"GSHEETS UPDATED: {item['slug']} (row {cell.row})")
                else:
                    # 見つからない場合は新規追加
                    sheet.append_row(_row(item), value_input_option="USER_ENTERED")
                    print(f"GSHEETS ADDED: {item['slug']} (new row)")
            except Exception as find_error:
                print(f"GSHEETS FIND ERROR for {item['slug']}: {find_error}")
                # 検索でエラーが発生した場合も新規追加を試行
                sheet.append_row(_row(item), value_input_option="USER_ENTERED")
                print(f"GSHEETS ADDED: {item['slug']} (fallback after find error)")
                
        except Exception as e:
            print(f"GSHEETS ERROR for {item['slug']}: {e}")
            raise e
        
except Exception as e:
    print("⚠️  Google Sheets 連携をスキップ:", repr(e))
    traceback.print_exc()
    upsert = _noop
