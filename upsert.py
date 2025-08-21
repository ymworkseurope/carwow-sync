# upsert.py
# rev: 2025-08-23 T23:40Z  ←★時刻だけ更新

"""
期待する環境変数
----------------
GS_CREDS_JSON   : サービスアカウントの JSON そのまま（長い 1 行）
GS_SHEET_ID     : 1aBcDeFGhiJkLmnO-pQRsTuVWXYZ_1234567890  ← スプレッドシート ID
"""

import os, json, io, gspread
from google.oauth2.service_account import Credentials

# ── ① JSON を secrets/gs_creds.json に書き出す ───────────────────
CREDS_PATH = "secrets/gs_creds.json"
os.makedirs("secrets", exist_ok=True)

# GitHub Actions では env に載ってくるのでファイルに落とす
if os.getenv("GS_CREDS_JSON") and not os.path.exists(CREDS_PATH):
    with open(CREDS_PATH, "w", encoding="utf-8") as f:
        f.write(os.getenv("GS_CREDS_JSON"))

# ── ② 認証 ────────────────────────────────────────────────
SCOPE = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
creds = Credentials.from_service_account_file(CREDS_PATH, scopes=SCOPE)
gc    = gspread.authorize(creds)

# ── ③ シートを ID で開く（※ open_by_key!）────────────────────
SHEET_ID = os.getenv("GS_SHEET_ID")
ws       = gc.open_by_key(SHEET_ID).sheet1      # 1 枚目のタブ

# -----------------------------------------------------------------
def _header_map() -> dict:
    """列名→列番号 dict（1-origin）"""
    return {v: i + 1 for i, v in enumerate(ws.row_values(1))}

def upsert(row: dict):
    header = _header_map()
    slug_col = header.get("slug")

    # 1 行目（ヘッダ）が無ければ作成
    if not slug_col:
        ws.insert_row(list(row.keys()), 1)
        header = _header_map()
        slug_col = header["slug"]

    # 既存行を検索
    cell = ws.find(row["slug"]) if slug_col else None
    if cell:
        # 更新
        for k, v in row.items():
            ws.update_cell(cell.row, header[k], v if v is not None else "")
    else:
        # 追加
        ws.append_row(
            [row.get(h, "") for h in header.keys()],
            value_input_option="USER_ENTERED"
        )

if __name__ == "__main__":
    import sys
    with open(sys.argv[1], "r", encoding="utf-8") as f:
        for j in f:
            upsert(json.loads(j))
    print("DONE → Google Sheet:", SHEET_ID)
