#!/usr/bin/env python3
# scrape.py – 2025-09-xx simple-make-crawler

import os, re, json, sys, time, random, traceback, requests, backoff, bs4
from urllib.parse import urlparse
from typing import List, Dict, Any, Tuple
from tqdm import tqdm

# ───────────── 自作モジュール ─────────────
from urls import iter_model_urls          # ★ 新実装 (メーカー階層から抽出)
from model_scraper import scrape as scrape_one
from transform     import to_payload
try:
    from gsheets_helper import upsert as gsheets_upsert
except ImportError:
    gsheets_upsert = None                 # Sheets 無効時は noop

# ───────────── 定数 ─────────────
UA   = "Mozilla/5.0 (+https://github.com/ymworkseurope/carwow-sync 2025-09)"
HEAD = {"User-Agent": UA}

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# ───────────── low-level helpers ─────────────
@backoff.on_exception(backoff.expo, requests.RequestException, max_tries=5, jitter=None)
def _get(url: str) -> requests.Response:
    r = requests.get(url, headers=HEAD, timeout=30)
    r.raise_for_status()
    return r

def _bs(url: str) -> bs4.BeautifulSoup:
    return bs4.BeautifulSoup(_get(url).text, "lxml")

def _sleep():
    time.sleep(random.uniform(1.2, 2.2))

# ───────────── Supabase payload validator ─────────────
def validate_supabase_payload(payload: Dict[str, Any]) -> Tuple[bool, str]:
    errs: List[str] = []

    # 必須フィールド
    for f in ("id", "slug", "make_en", "model_en"):
        if not payload.get(f):
            errs.append(f"Missing {f}")

    # 数値フィールド
    for f in ("price_min_gbp", "price_max_gbp", "price_min_jpy", "price_max_jpy"):
        v = payload.get(f)
        if v is not None and not isinstance(v, (int, float)):
            errs.append(f"{f} not number: {v}")

    # JSON 文字列チェック
    sj = payload.get("spec_json")
    if sj is not None:
        if isinstance(sj, str):
            try:
                json.loads(sj)
            except json.JSONDecodeError as e:
                errs.append(f"spec_json bad JSON: {e}")
        elif not isinstance(sj, dict):
            errs.append(f"spec_json type {type(sj)} invalid")

    return (not errs, "; ".join(errs))

# ───────────── Supabase UPSERT ─────────────
def db_upsert(item: Dict[str, Any]):
    if not (SUPABASE_URL and SUPABASE_KEY):
        print("SKIP Supabase:", item.get("slug"))
        return

    ok, msg = validate_supabase_payload(item)
    if not ok:
        print("VALIDATION ERROR:", msg)
        print(json.dumps(item, indent=2, ensure_ascii=False))
        return

    r = requests.post(
        f"{SUPABASE_URL}/rest/v1/cars",
        headers={
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Prefer": "resolution=merge-duplicates",
            "Content-Type": "application/json",
        },
        json=item,
        timeout=30
    )

    if r.ok:
        print("SUPABASE OK", item["slug"])
    else:
        print(f"SUPABASE ERROR [{r.status_code}] {item['slug']}\n{r.text}")
        r.raise_for_status()

# ───────────── Main loop ─────────────
if __name__ == "__main__":
    urls = list(iter_model_urls())
    print(f"Total target models (from make pages): {len(urls)}")

    DEBUG = os.getenv("DEBUG_MODE", "false").lower() == "true"
    if DEBUG:
        urls = urls[:10]
        print("DEBUG MODE → first 10 only")

    success = failed = 0
    processed_slugs = set()

    for url in tqdm(urls, desc="scrape"):
        _sleep()

        slug = "-".join(urlparse(url).path.strip("/").split("/"))
        if slug in processed_slugs:
            print(f"SKIP duplicate slug: {slug}")
            continue

        try:
            raw     = scrape_one(url)
            payload = to_payload(raw)

            # Supabase
            db_upsert(payload)

            # Google Sheets
            if gsheets_upsert:
                try:
                    gsheets_upsert(payload)
                except Exception as e:
                    print(f"Google Sheets error for {slug}: {e}")

            processed_slugs.add(slug)
            success += 1

        except Exception as e:
            failed += 1
            print(f"[ERR] {url}: {repr(e)}")
            traceback.print_exc()
            if DEBUG and failed >= 3:
                break

    print(f"\nFinished: {success} success / {failed} error")
    print(f"Processed slugs: {len(processed_slugs)}")
