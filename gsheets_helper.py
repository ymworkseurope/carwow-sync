#!/usr/bin/env python3
# gsheets_helper.py – 2025-08-27 fixed (proper upsert logic)
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
from google.oauth2.service_account import Credentials

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
    "drive_type","drive_type_ja","grades","engines","colors",
    "full_model_ja","updated_at"
]

# ───────── util ─────────
def _dump(v):
    """値をスプレッドシート用に変換"""
    if v is None:
        return ""
    if isinstance(v, list):
        # media_urls（HTTP URLのリスト）は改行区切り
        if v and all(str(item).startswith("http") for item in v):
            return "\n".join(map(str, v))
        # それ以外のリストはカンマ区切り
        return ", ".join(map(str, v))
    if isinstance(v, dict):
        return json.dumps(v, ensure_ascii=False)
    # 改行をエスケープ
    return str(v).replace("\n", "\\n").replace("\r", "")

def _row(item: dict):
    """payload → シート 1 行（配列）"""
    return [_dump(item.get(h)) for h in _HEADERS]

# ───────── Sheet 接続 ─────────
def _noop(*a, **kw): 
    """ダミー関数"""
    pass

try:
    # 認証情報の準備
    if os.getenv("GS_CREDS_JSON") and not os.path.exists(CREDS_PATH):
        os.makedirs("secrets", exist_ok=True)
        with open(CREDS_PATH, "w", encoding="utf-8") as f:
            f.write(os.getenv("GS_CREDS_JSON"))
    
    if not (os.path.exists(CREDS_PATH) and SHEET_ID):
        raise RuntimeError("GS_CREDS_JSON または GS_SHEET_ID が未設定")
    
    # Google Sheets接続
    SCOPES = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = Credentials.from_service_account_file(CREDS_PATH, scopes=SCOPES)
    gc = gspread.authorize(creds)
    
    # スプレッドシート取得
    spreadsheet = gc.open_by_key(SHEET_ID)
    
    # ワークシート取得（なければ作成）
    try:
        ws = spreadsheet.worksheet(WS_NAME)
    except gspread.exceptions.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=WS_NAME, rows=1000, cols=len(_HEADERS))
        print(f"Created new worksheet: {WS_NAME}")
    
    # ―― ヘッダを自動整合 ――――――――――――――――――――――――――
    def _ensure_header():
        """ヘッダ行を確認・更新"""
        try:
            cur = ws.row_values(1)
            if cur == _HEADERS:
                return  # 既に正しい
            
            if not cur:  # 空の場合
                ws.insert_row(_HEADERS, 1)
                print("GSHEETS: Added headers")
            else:  # 異なる場合は更新
                end_col = rowcol_to_a1(1, len(_HEADERS))
                ws.update(f"A1:{end_col}", [_HEADERS], value_input_option="RAW")
                print("GSHEETS: Updated headers")
        except Exception as e:
            print(f"Header update error: {e}")
    
    _ensure_header()
    
    # ───────── public: UPSERT ─────────
    def upsert(item: dict):
        """データをアップサート（更新または追加）"""
        try:
            slug = item.get("slug")
            if not slug:
                print("GSHEETS ERROR: No slug in item")
                return
            
            # slugで既存行を検索
            try:
                cell = ws.find(slug, in_column=2)  # slugは2列目
                if cell:
                    # 既存行を更新
                    row_num = cell.row
                    end_col = rowcol_to_a1(row_num, len(_HEADERS))
                    range_name = f"A{row_num}:{end_col}"
                    ws.update(range_name, [_row(item)], value_input_option="USER_ENTERED")
                    print(f"GSHEETS UPDATED row {row_num}: {slug}")
                else:
                    # 新規追加
                    ws.append_row(_row(item), value_input_option="USER_ENTERED")
                    print(f"GSHEETS ADDED: {slug}")
            except gspread.exceptions.CellNotFound:
                # 新規追加
                ws.append_row(_row(item), value_input_option="USER_ENTERED")
                print(f"GSHEETS ADDED: {slug}")
                
        except Exception as e:
            print(f"GSHEETS ERROR {item.get('slug', 'unknown')}: {e}")
            traceback.print_exc()

except Exception as e:
    print(f"⚠️  Google Sheets 連携をスキップ: {e}")
    traceback.print_exc()
    upsert = _noop
