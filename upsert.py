# upsert.py
# rev: 2025-08-23 T10:30Z
"""
環境変数
---------
GS_CREDS      = secrets/gs_creds.json のパス
GS_SHEET_NAME = 1ZQjMIcIK7xIqra9Uu1ZAR170ekeMQdPsZW46Ts8UExE
"""
import os, csv, gspread
from google.oauth2.service_account import Credentials

CREDS_FILE = os.getenv("GS_CREDS","secrets/gs_creds.json")
SHEET_NAME = os.getenv("GS_SHEET_NAME","carwow_specs")

scope=[
  "https://www.googleapis.com/auth/spreadsheets",
  "https://www.googleapis.com/auth/drive"
]
creds = Credentials.from_service_account_file(CREDS_FILE,scopes=scope)
gc = gspread.authorize(creds)
ws = gc.open(SHEET_NAME).sheet1   # 1枚目のシート

def _header_map()->dict:
    """列名→列番号 dict（1-origin）"""
    return {v:i+1 for i,v in enumerate(ws.row_values(1))}

def upsert(row: dict):
    header=_header_map()
    slug_col=header.get("slug")
    if not slug_col:               # 初回だけヘッダを作る
        ws.insert_row(list(row.keys()),1)
        header=_header_map(); slug_col=header["slug"]

    cell=ws.find(row["slug"]) if slug_col else None
    if cell:                       # 更新
        for k,v in row.items():
            ws.update_cell(cell.row, header[k], v if v is not None else "")
    else:                          # 追加
        ws.append_row([row.get(h,"") for h in header.keys()], value_input_option="USER_ENTERED")

if __name__=="__main__":
    import sys, json
    with open(sys.argv[1],"r",encoding="utf-8") as f:
        for j in f: upsert(json.loads(j))
    print("DONE → Google Sheet:", SHEET_NAME)
