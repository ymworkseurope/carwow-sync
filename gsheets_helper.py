# gsheets_helper.py
# rev: 2025-08-24 T00:30Z  ←★タイムスタンプ更新
import os, gspread, traceback

CREDS_PATH  = "secrets/gs_creds.json"
SHEET_ID    = os.getenv("GS_SHEET_ID")       # ← ここを統一
WS_NAME     = "Carwow"                       # シートタブ名（変えるならここ）

def _stub(*_, **__): pass   # 失敗時フォールバック

try:
    if not (os.path.exists(CREDS_PATH) and SHEET_ID):
        raise FileNotFoundError("GS creds もしくは GS_SHEET_ID がありません")

    gc     = gspread.service_account(filename=CREDS_PATH)
    _sheet = gc.open_by_key(SHEET_ID).worksheet(WS_NAME)

    _HEADERS = [
        "slug","make_en","model_en",
        "price_min_gbp","price_max_gbp",
        "body_type","fuel","updated_at"
    ]

    def _row(item): return [item.get(h, "") for h in _HEADERS]

    def upsert(item: dict):
        try:
            cell = _sheet.find(item["slug"])
            _sheet.update(
                f"A{cell.row}:{chr(64+len(_HEADERS))}{cell.row}",
                [_row(item)],
                value_input_option="USER_ENTERED"
            )
        except gspread.exceptions.CellNotFound:
            _sheet.append_row(_row(item), value_input_option="USER_ENTERED")
        print("GSHEETS", item["slug"])

except Exception as e:
    print("⚠️  Google Sheets 連携をスキップ:", repr(e))
    traceback.print_exc()
    upsert = _stub
