# scrape.py  ― Carwow 全モデル → Supabase ＆ Google Sheets 連携
# rev: 2025-08-23 T14:20Z  ★このファイルが “完全版” です
# --------------------------------------------------------------------
import os, re, json, sys, time, random
from typing import List, Dict
from urllib.parse import urlparse

import requests, bs4, backoff
from tqdm import tqdm

# --- プロジェクト内モジュール --------------------------------------
from model_scraper import scrape as scrape_one          # 個別モデル取得
from transform      import to_payload                   # 正規化
# Google Sheets（無い環境でも動くようにトライ）
try:
    from gsheets_helper import upsert as gsheets_upsert
except Exception:
    gsheets_upsert = None

# --- 定数 -----------------------------------------------------------
UA   = ("Mozilla/5.0 (+https://github.com/ymworkseurope/"
        "carwow-sync 2025-08-23)")
HEAD = {"User-Agent": UA}

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# --- HTTP helper ----------------------------------------------------
@backoff.on_exception(backoff.expo,
                      (requests.RequestException,), max_tries=5, jitter=None)
def _get(url: str) -> requests.Response:
    r = requests.get(url, headers=HEAD, timeout=30)
    r.raise_for_status()
    return r

def _bs(url: str):
    return bs4.BeautifulSoup(_get(url).text, "lxml")

def _sleep():
    time.sleep(random.uniform(0.6, 1.1))

# --------------------------------------------------------------------
# (1) Carwow の全モデル URL 収集
# --------------------------------------------------------------------
def iter_model_urls() -> List[str]:
    """sitemap から /make/model 形式で Blog・UsedCar・News を除外した URL 一覧取得"""
    top_xml = _get("https://www.carwow.co.uk/sitemap.xml").text
    locs    = re.findall(r"<loc>(https://[^<]+)</loc>", top_xml)

    models: List[str] = []
    for loc in locs:
        if not loc.endswith(".xml"):          # 直リンクは skip
            continue
        sub_xml = _get(loc).text
        models += re.findall(r"<loc>(https://www\.carwow\.co\.uk/[^<]+)</loc>", sub_xml)

    good = [
        u for u in models
        if re.match(r"https://www\.carwow\.co\.uk/[a-z0-9-]+/[a-z0-9-]+/?$", u)
        and not re.search(r"(blog|used|news)", u)
    ]
    return sorted(set(good))

# --------------------------------------------------------------------
# (2) Supabase へ UPSERT
# --------------------------------------------------------------------
@backoff.on_exception(backoff.expo, requests.HTTPError, max_time=60)
def db_upsert(item: Dict):
    if not (SUPABASE_URL and SUPABASE_KEY):
        print("SKIP Supabase (環境変数未設定)", item.get("slug"))
        return
    r = requests.post(
        f"{SUPABASE_URL}/rest/v1/cars?on_conflict=slug",
        headers={
            "apikey":        SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Prefer":        "resolution=merge-duplicates",
            "Content-Type":  "application/json",
        },
        json=item,
        timeout=30,
    )
    r.raise_for_status()
    print("SUPABASE", item["slug"])

# --------------------------------------------------------------------
# (3) メイン処理
# --------------------------------------------------------------------
if __name__ == "__main__":
    urls = iter_model_urls()
    print("Total target models:", len(urls))
    print("sample 5:", ["/".join(urlparse(u).path.strip("/").split("/")[-2:]) for u in urls[:5]])
    _sleep()  # API 呼び出し前に少し待機（念のため）

    raw_dump   = open("raw.jsonl", "w", encoding="utf-8")  # 生データ保存用
    upsert_ok, skip = 0, 0

    for u in tqdm(urls, desc="scrape"):
        _sleep()
        try:
            raw       = scrape_one(u)        # model_scraper が dict を返す
            payload   = to_payload(raw)      # DB/Sheets 用に整形

            # --- Supabase ---
            db_upsert(payload)

            # --- Google Sheets ---
            if gsheets_upsert:
                gsheets_upsert(payload)

            # --- ローカル保存 ---
            raw_dump.write(json.dumps(raw, ensure_ascii=False) + "\n")
            upsert_ok += 1

        except Exception as e:
            print("[ERR]", u, e, file=sys.stderr)
            skip += 1

    raw_dump.close()
    print(f"\nFinished: {upsert_ok} OK / {skip} Error → raw.jsonl")

