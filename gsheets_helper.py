import os, gspread

CREDS_PATH = "secrets/gs_creds.json"        # Actions が作る場所
SHEET_ID   = os.getenv("GS_SHEET_ID")       # ← ★ENV 名を統一
WS_NAME    = "Carwow"                       # タブ名

if not (os.path.exists(CREDS_PATH) and SHEET_ID):
    raise RuntimeError("Google Sheets の認証情報が不足しています")

gc     = gspread.service_account(filename=CREDS_PATH)
_sheet = gc.open_by_key(SHEET_ID).worksheet(WS_NAME)

_HEADERS = [
    "slug", "make_en", "model_en",
    "price_min_gbp", "price_max_gbp",
    "body_type", "fuel", "updated_at"
]

def _row_from_item(item: dict) -> list[str]:
    return [item.get(h, "") for h in _HEADERS]

def upsert(item: dict) -> None:
    """slug があれば UPDATE、無ければ INSERT"""
    try:
        cell = _sheet.find(item["slug"])
        _sheet.update(
            f"A{cell.row}:{chr(64+len(_HEADERS))}{cell.row}",
            [_row_from_item(item)],
            value_input_option="USER_ENTERED"
        )
    except gspread.exceptions.CellNotFound:
        _sheet.append_row(
            _row_from_item(item),
            value_input_option="USER_ENTERED"
        )
    print("GSHEETS", item["slug"])

