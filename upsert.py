#!/usr/bin/env python3
# upsert.py – 2025-08-29 keep json/array as-is & newline→\n

import os, json, gspread
from google.oauth2.service_account import Credentials

CREDS_PATH="secrets/gs_creds.json"
os.makedirs("secrets",exist_ok=True)
if os.getenv("GS_CREDS_JSON") and not os.path.exists(CREDS_PATH):
    with open(CREDS_PATH,"w",encoding="utf-8") as f: f.write(os.getenv("GS_CREDS_JSON"))

SCOPE=["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
ws=gspread.authorize(Credentials.from_service_account_file(CREDS_PATH,scopes=SCOPE))\
        .open_by_key(os.getenv("GS_SHEET_ID")).sheet1

def _header(): return {v:i+1 for i,v in enumerate(ws.row_values(1))}

def _norm(v):
    if isinstance(v,(list,dict)):          # 配列・JSON はそのまま
        return json.dumps(v,ensure_ascii=False)
    if v is None: return ""
    return str(v).replace("\r","").replace("\n","\\n")  # 改行はセル内 \n

def upsert(row:dict):
    head=_header()
    if "slug" not in head:
        ws.insert_row(list(row.keys()),1); head=_header()
    slug_col=head["slug"]
    cell=ws.find(row["slug"]) if slug_col else None
    if cell:
        for k,v in row.items():
            ws.update_cell(cell.row, head[k], _norm(v))
    else:
        ws.append_row([_norm(row.get(h,"")) for h in head.keys()],
                      value_input_option="USER_ENTERED")
