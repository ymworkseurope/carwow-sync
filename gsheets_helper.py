#!/usr/bin/env python3
# gsheets_helper.py – 2025-08-27 fixed (cell limit + error handling)
"""
Google Sheets 連携ユーティリティ
────────────────────────────────────
必須環境変数
  GS_CREDS_JSON : サービスアカウント JSON（一行）
  GS_SHEET_ID   : スプレッドシート ID
────────────────────────────────────
セル数制限対策:
  - 最大行数: 50,000行
  - 古いデータの自動アーカイブ
  - セル数監視
────────────────────────────────────
"""
import os, json, gspread, traceback, time
from datetime import datetime
from gspread.utils import rowcol_to_a1
from google.oauth2.service_account import Credentials
from typing import Dict, List, Optional

CREDS_PATH = "secrets/gs_creds.json"
SHEET_ID   = os.getenv("GS_SHEET_ID")
WS_NAME    = "system_cars"
ARCHIVE_WS_NAME = "archive_cars"
MAX_ROWS   = 50000  # 最大行数制限
MAX_CELLS  = 9000000  # セル数上限（10Mの90%）

# ───────── スキーマ定義 ─────────
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
        # URLリストは改行区切り（セル内改行）
        if v and all(str(item).startswith("http") for item in v):
            # 最大5個まで
            return "\n".join(map(str, v[:5]))
        # それ以外のリストはカンマ区切り
        return ", ".join(map(str, v))
    if isinstance(v, dict):
        return json.dumps(v, ensure_ascii=False)
    # 改行をエスケープ
    return str(v).replace("\n", " ").replace("\r", "")

def _row(item: dict) -> List[str]:
    """payload → シート 1 行（配列）"""
    return [_dump(item.get(h)) for h in _HEADERS]

# ───────── 初期化とシート管理 ─────────
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
    
    # メインワークシート取得（なければ作成）
    try:
        ws = spreadsheet.worksheet(WS_NAME)
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=WS_NAME, rows=MAX_ROWS, cols=len(_HEADERS))
        print(f"Created new worksheet: {WS_NAME}")
    
    # アーカイブワークシート（必要時に作成）
    archive_ws: Optional[gspread.Worksheet] = None
    
    # ───────── セル数管理 ─────────
    def _check_cell_limit() -> tuple[int, int, bool]:
        """
        現在のセル数をチェック
        Returns: (current_rows, total_cells, needs_cleanup)
        """
        try:
            all_values = ws.get_all_values()
            current_rows = len(all_values)
            total_cells = current_rows * len(_HEADERS)
            needs_cleanup = total_cells > MAX_CELLS or current_rows > MAX_ROWS
            return current_rows, total_cells, needs_cleanup
        except Exception as e:
            print(f"Cell count check error: {e}")
            return 0, 0, False
    
    def _archive_old_data(keep_rows: int = 10000):
        """
        古いデータをアーカイブシートに移動
        """
        global archive_ws
        try:
            # アーカイブシート作成
            if archive_ws is None:
                try:
                    archive_ws = spreadsheet.worksheet(ARCHIVE_WS_NAME)
                except gspread.WorksheetNotFound:
                    archive_ws = spreadsheet.add_worksheet(
                        title=ARCHIVE_WS_NAME, 
                        rows=MAX_ROWS, 
                        cols=len(_HEADERS)
                    )
                    archive_ws.append_row(_HEADERS)
            
            # 古いデータを取得
            all_data = ws.get_all_values()
            if len(all_data) <= keep_rows:
                return
            
            # アーカイブするデータ
            to_archive = all_data[1:len(all_data)-keep_rows]  # ヘッダーを除く古いデータ
            
            # アーカイブシートに追加
            for row in to_archive:
                archive_ws.append_row(row)
            
            # メインシートをクリア&再構築
            keep_data = [all_data[0]] + all_data[len(all_data)-keep_rows:]  # ヘッダー + 新しいデータ
            ws.clear()
            ws.update(f"A1:{rowcol_to_a1(len(keep_data), len(_HEADERS))}", 
                     keep_data, 
                     value_input_option="RAW")
            
            print(f"Archived {len(to_archive)} rows to {ARCHIVE_WS_NAME}")
            
        except Exception as e:
            print(f"Archive error: {e}")
    
    # ───────── ヘッダー管理 ─────────
    def _ensure_header():
        """ヘッダ行を確認・更新"""
        try:
            cur = ws.row_values(1)
            if cur == _HEADERS:
                return
            
            if not cur:
                ws.insert_row(_HEADERS, 1)
                print("GSHEETS: Added headers")
            else:
                end_col = rowcol_to_a1(1, len(_HEADERS))
                ws.update(f"A1:{end_col}", [_HEADERS], value_input_option="RAW")
                print("GSHEETS: Updated headers")
        except Exception as e:
            print(f"Header update error: {e}")
    
    _ensure_header()
    
    # ───────── public: UPSERT ─────────
    def upsert(item: dict):
        """データをアップサート（セル数制限対応）"""
        try:
            slug = item.get("slug")
            if not slug:
                print("GSHEETS ERROR: No slug in item")
                return
            
            # セル数チェック
            current_rows, total_cells, needs_cleanup = _check_cell_limit()
            if needs_cleanup:
                print(f"Cell limit approaching ({total_cells:,} cells, {current_rows:,} rows)")
                _archive_old_data()
            
            # slugで既存行を検索（高速化のため列指定）
            try:
                slug_column = ws.col_values(2)  # slug列は2番目
                if slug in slug_column:
                    row_num = slug_column.index(slug) + 1
                    # 既存行を更新
                    end_col = rowcol_to_a1(row_num, len(_HEADERS))
                    range_name = f"A{row_num}:{end_col}"
                    ws.update(range_name, [_row(item)], value_input_option="USER_ENTERED")
                    print(f"GSHEETS UPDATED row {row_num}: {slug}")
                else:
                    # 新規追加
                    ws.append_row(_row(item), value_input_option="USER_ENTERED")
                    print(f"GSHEETS ADDED: {slug}")
                    
            except Exception as e:
                # エラー時も新規追加を試みる
                print(f"Search error, trying append: {e}")
                ws.append_row(_row(item), value_input_option="USER_ENTERED")
                print(f"GSHEETS ADDED (fallback): {slug}")
            
            # レート制限対策
            time.sleep(0.5)
            
        except gspread.exceptions.APIError as e:
            if "cells in the workbook" in str(e):
                print(f"GSHEETS CELL LIMIT ERROR: Cannot add {slug}")
                # 強制アーカイブ
                _archive_old_data(keep_rows=5000)
            else:
                print(f"GSHEETS API ERROR {item.get('slug', 'unknown')}: {e}")
        except Exception as e:
            print(f"GSHEETS ERROR {item.get('slug', 'unknown')}: {e}")
            traceback.print_exc()

except Exception as e:
    print(f"⚠️  Google Sheets 連携をスキップ: {e}")
    traceback.print_exc()
    upsert = _noop
