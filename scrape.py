#!/usr/bin/env python3
"""Carwow full‑scraper – production edition (2025‑09‑final‑r6)
────────────────────────────────────────────────────────────────
* **1 model ⇒ 最大 3 HTTP** (main /specifications /colours)
* Supabase / Google Sheets への **自動アップサート** が再び正常動作。
  - `id` カラム (NOT‑NULL) を slug 派生のユニークキーで復活。
  - r5 の SyntaxError を修正し、JSON ボディ送信時の `Content-Type` を追加。
  - CLI: `--make`/`--slugs` が無指定の場合は body_map_* を総当たり。

使い方
------
```bash
$ python scrape.py --slugs abarth/500e alfa-romeo/tonale
$ python scrape.py --make abarth              # body_map_abarth.json 全車
$ python scrape.py                            # body_map_*.json を総当たり
```
実行ごとに **1 行 = 1 JSON** を `stdout` に出力しつつ、Supabase と
（資格情報があれば）Google Sheets へ同じデータを upsert します。
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple

import requests
from bs4 import BeautifulSoup

# ─────────────────────────────────────────────────────────────
# settings & creds
# ─────────────────────────────────────────────────────────────
BASE = "https://www.carwow.co.uk"
HEADERS = {"User-Agent": "Mozilla/5.0 (carwow-prodbot/1.0)"}
TIMEOUT = 20
MAX_IMAGES = 20  # cap images per model
SKIP_SLUGS = {
    "lease",
    "news",
    "mpv",
    "automatic",
    "manual",
    "black",
    "white",
    "grey",
    "green",
    "red",
    "blue",
    "yellow",
    "orange",
    "purple",
    "brown",
    "silver",
}

# Supabase creds (expect GitHub Actions secrets)
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Google Sheets helper is optional
try:
    from gsheets_helper import upsert as gsheets_upsert  # type: ignore
except ImportError:
    gsheets_upsert = None

# ─────────────────────────────────────────────────────────────
# HTTP helpers
# ─────────────────────────────────────────────────────────────

def fetch(url: str) -> BeautifulSoup:
    """GET & parse with small back‑off retry."""
    for i in range(3):
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        if r.ok:
            return BeautifulSoup(r.text, "lxml")
        if r.status_code == 404:
            raise requests.HTTPError(response=r)
        time.sleep(1 + i)
    r.raise_for_status()


def to_int(txt: str | None) -> Optional[int]:
    if not txt:
        return None
    try:
        return int(txt.replace("£", "").replace(",", "").strip())
    except Exception:
        return None


def split_make_model(title_core: str) -> Tuple[str, str]:
    parts = title_core.split()
    return parts[0], " ".join(parts[1:])


# primary key generator --------------------------------------------------

def make_id(path: str) -> str:
    """Unique PK for Supabase: make/model slug joined with hyphen."""
    return path.strip("/").replace("/", "-")


# ─────────────────────────────────────────────────────────────
# body‑map cache  (make → { slug: [body_types] })
# ─────────────────────────────────────────────────────────────
_body_map: Dict[str, Dict[str, List[str]]] = {}


def load_body_map(make: str) -> Dict[str, List[str]]:
    make = make.lower()
    if make in _body_map:
        return _body_map[make]
    fn = Path(f"body_map_{make}.json")
    _body_map[make] = json.loads(fn.read_text()) if fn.exists() else {}
    return _body_map[make]


# ─────────────────────────────────────────────────────────────
# spec / colours sub‑pages
# ─────────────────────────────────────────────────────────────

def scrape_specifications(slug: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    url = f"{BASE}/{slug}/specifications"
    try:
        soup = fetch(url)
    except requests.HTTPError as e:
        if e.response.status_code == 404:
            return {}, {}
        raise

    specs: Dict[str, Any] = {}
    extra: Dict[str, Any] = {}

    # summary list -------------------------------------------------------
    for dt in soup.select("div.model-hub__summary-list dt"):
        key = dt.get_text(strip=True)
        val = dt.find_next("dd").get_text(" ", strip=True)
        specs[key] = val

    # dimensions ---------------------------------------------------------
    dim_pat = re.compile(r"\b\d{3,4}\s?mm\b")
    dims: List[str] = [t.get_text(" ", strip=True) for t in soup.select("h4, li") if dim_pat.search(t.get_text())]
    if dims:
        extra["dimensions_mm"] = " | ".join(dims)

    # engines / grades ---------------------------------------------------
    for h3 in soup.select("h3"):
        h = h3.get_text(strip=True).lower()
        if "engine" in h:
            extra["engines"] = [li.get_text(" ", strip=True) for li in h3.find_all_next("li")][:10]
        if any(k in h for k in ("trim", "grade")):
            extra["grades"] = [li.get_text(" ", strip=True) for li in h3.find_all_next("li")][:10]

    return specs, extra


def scrape_colours(slug: str) -> List[str]:
    url = f"{BASE}/{slug}/colours"
    try:
        soup = fetch(url)
    except requests.HTTPError as e:
        if e.response.status_code == 404:
            return []
        raise
    return [h4.contents[0].strip() for h4 in soup.select("h4.model-hub__colour-details-title")]


# ─────────────────────────────────────────────────────────────
# main model page parser
# ─────────────────────────────────────────────────────────────

def try_fetch_model(slug: str, make_hint: Optional[str] = None) -> Tuple[BeautifulSoup, str]:
    try:
        return fetch(f"{BASE}/{slug}"), slug
    except requests.HTTPError as e:
        if e.response.status_code == 404 and make_hint and "/" not in slug:
            path = f"{make_hint}/{slug}"
            return fetch(f"{BASE}/{path}"), path
        raise


def parse_model_page(slug: str, make_hint: Optional[str] = None) -> Dict[str, Any]:
    soup, path = try_fetch_model(slug, make_hint)

    h1 = soup.select_one("h1.header__title")
    if not h1:
        raise ValueError(f"no title for {slug}")
    title_core = re.sub(r" Review.*", "", h1.get_text(strip=True))
    make_en, model_en = split_make_model(title_core)

    body_map = load_body_map(make_en)

    glance = {
        dt.get_text(strip=True): dt.find_next("dd").get_text(strip=True)
        for dt in soup.select("div.model-hub__summary-list dt")
    }

    body_type = glance.get("Body type") or body_map.get(path) or body_map.get(path.split("/")[-1], [])
    fuel = glance.get("Available fuel types")
    doors = glance.get("Number of doors")
    seats = glance.get("Number of seats")
    drive_tp = glance.get("Transmission") or glance.get("Drive type")

    price_min_gbp = price_max_gbp = None
    rrp = soup.select_one("span.deals-cta-list__rrp-price")
    if rrp and "£" in rrp.text:
        pounds = re.findall(r"£[\d,]+", rrp.text)
        if pounds:
            price_min_gbp = to_int(pounds[0])
            if len(pounds) > 1:
                price_max_gbp = to_int(pounds[1])
    else:
        used_dd = soup.find("dt", string=re.compile("Used", re.I))
        if used_dd:
            price_min_gbp = to_int(used_dd.find_next("dd").text)

    lead = soup.select_one("div#main p")
    overview_en = lead.get_text(strip=True) if lead else ""

    imgs: List[str] = []
    for im in soup.select("img.media-slider__image, img.thumbnail-carousel-vertical__img"):
        src = im.get("data-src") or im.get("src")
        if src:
            url_clean = src.split("?")[0]
            if url_clean not in imgs:
                imgs.append(url_clean)
            if len(imgs) == MAX_IMAGES:
                break

    spec_tbl, extra = scrape_specifications(path)
    colours = scrape_colours(path)

    doors = doors or spec_tbl.get("Number of doors")
    seats = seats or spec_tbl.get("Number of seats")
    fuel = fuel or spec_tbl.get("Fuel type") or spec_tbl.get("Fuel types")
    drive_tp = drive_tp or spec_tbl.get("Transmission")
    dimensions_mm = extra.get("dimensions_mm") if extra else None

    return {
        "id": make_id(path),
        "slug": path,
        "make_en": make_en,
        "model_en": model_en,
        "body_type": body_type if isinstance(body_type, list) else ([body_type] if body_type else []),
        "fuel": fuel,
        "price_min_gbp": price_min_gbp,
        "price_max_gbp": price_max_gbp,
        "doors": doors,
        "seats": seats,
        "dimensions_mm": dimensions_mm,
        "drive_type": drive_tp,
        "overview_en": overview_en,
        "media_urls": imgs,
        "colors": colours,
        "grades": extra.get("grades") if extra else None,
        "engines": extra.get("engines") if extra else None,
        "spec_json": json.dumps(spec_tbl, ensure_ascii=False),
        "catalog_url": f"{BASE}/{path}",
    }


# ─────────────────────────────────────────────────────────────
# Supabase / Sheets helpers
# ─────────────────────────────────────────────────────────────

def validate_supabase_payload(item: Dict[str, Any]) -> Tuple[bool, str]:
    errs: List[str] = []
    for f in ("id", "slug", "make_en", "model_en"):
        if not item.get(f):
            errs.append(f"missing {f}")
    for f in ("price_min_gbp", "price_max_gbp"):
        v = item.get(f)
        if v is not None and not isinstance(v, (int, float)):
            errs.append(f"{f} not number")
    return (not errs, "; ".join(errs))


def db_upsert(item: Dict[str, Any]):
    """Insert / merge into Supabase if creds present."""
    if not (SUPABASE_URL and SUPABASE_KEY):
        return
    ok, msg = validate_supabase_payload(item)
    if not ok:
        print(f"validation error:{item['slug']}: {msg}", file=sys.stderr)
        return
    r = requests.post(
        f"{SUPABASE_URL}/rest/v1/cars",
        headers={
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Prefer": "return=representation,resolution=merge-duplicates",
            "Content-Type": "application/json",
        },
        json=item,
        timeout=30,
    )
    if not r.ok:
        print(f"supabase error:{item['slug']}: [{r.status_code}] {r.text}", file=sys.stderr)


# ─────────────────────────────────────────────────────────────
# crawler loop
# ─────────────────────────────────────────────────────────────

def process(slugs: List[str]):
    for raw in slugs:
        if any(tok in SKIP_SLUGS for tok in raw.split("/")):
            continue
        make_hint = raw.split("/")[0] if "/" in raw else None
        try:
            data = parse_model_page(raw, make_hint)
            print(json.dumps(data, ensure_ascii=False))
            db_upsert(data)
            if gsheets_upsert:
                try:
                    gsheets_upsert(data)
                except Exception as e:
                    print(f"gsheets error:{raw}:{e}", file=sys.stderr)
        except Exception as e:
            print(f"error:{raw}:{e}", file=sys.stderr)


# ─────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────

def collect_all_slugs() -> List[str]:
    slugs: List[str] = []
    for body_map_file in Path.cwd().glob("body_map_*.json"):
        slugs += list(json.loads(body_map_file.read_text()).keys())
    return sorted(set(slugs))


def cli():
    ap = argparse.ArgumentParser()
    ap.add_argument("--slugs", nargs="*", help="model slugs a/b c/d …")
    ap.add_argument("--make", help="scrape every slug in body_map_<make>.json")
    args = ap.parse_args()

    if not args.slugs and not args.make:
        slugs = collect_all_slugs()
    else:
        slugs = []
        if args.slugs:
            slugs += args.slugs
        if args.make:
            slugs += list(load_body_map(args.make).keys())

    if not slugs:
        sys.exit("no target slugs found")

    process(slugs)


if __name__ == "__main__":
    cli()
