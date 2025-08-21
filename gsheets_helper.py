# gsheets_helper.py
import os, gspread, datetime as dt

CREDS_PATH = "secrets/gs_creds.json"            # Actions が書き出す場所
SHEET_KEY  = os.getenv("GS_SHEET_KEY")          # ★後で GitHub Secrets に追加する
WS_NAME    = "Carwow"                           # ワークシート名

_gc    = gspread.service_account(filename=CREDS_PATH)
_sheet = _gc.open_by_key(SHEET_KEY).worksheet(WS_NAME)

_HEADERS = ["slug","make_en","model_en","price_min_gbp",
            "price_max_gbp","body_type","fuel","updated_at"]

def _row_from_item(item: dict):
    return [item.get(h, "") for h in _HEADERS]

def upsert(item: dict):
    """slug が一致する行があれば UPDATE、無ければ append"""
    try:
        cell = _sheet.find(item["slug"])
        # --- UPDATE ---
        _sheet.update(
            f"A{cell.row}:{chr(64+len(_HEADERS))}{cell.row}",
            [_row_from_item(item)],
            value_input_option="USER_ENTERED"
        )
    except gspread.exceptions.CellNotFound:
        # --- INSERT ---
        _sheet.append_row(
            _row_from_item(item),
            value_input_option="USER_ENTERED"
        )
    print("GSHEETS", item["slug"])
