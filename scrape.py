#!/usr/bin/env python3
"""Carwow full‑scraper  –  production edition (2025‑09)
------------------------------------------------------
* **1 model → up to 3 HTTP requests**  (main /specifications /colours)  
* Grabs **all columns** required by Supabase schema.
  - slug, make_en, model_en, body_type, fuel, price_min/max, doors,
    seats, dimensions_mm, drive_type, overview, media_urls (max 20),
    colours, grades, engines, spec_json  … etc.
* Missing `body_type` は事前に作った `body_map_<make>.json` で補完。

Usage
-----
```
$ python scrape.py --slugs abarth/500e alfa-romeo/tonale
$ python scrape.py --make abarth               # メーカー丸ごと
```
Each run prints **one JSON line per car** to stdout – pipe it into `jq`
などで確認可能です。
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# settings
# ---------------------------------------------------------------------------
BASE       = "https://www.carwow.co.uk"
HEADERS    = {"User-Agent": "Mozilla/5.0 (carwow-prodbot/1.0)"}
TIMEOUT    = 20
MAX_IMAGES = 20      # production：サムネイル含めて 20 枚まで

# ---------------------------------------------------------------------------
# small helpers
# ---------------------------------------------------------------------------

def fetch(url: str) -> BeautifulSoup:
    """GET and parse – retry ×3 with 1 s back‑off"""
    for i in range(3):
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        if r.ok:
            return BeautifulSoup(r.text, "lxml")
        time.sleep(1 + i)
    r.raise_for_status()  # 最後のレスポンスで例外


def to_int(txt: str | None) -> Optional[int]:
    if not txt:
        return None
    try:
        return int(txt.replace("£", "").replace(",", "").strip())
    except Exception:
        return None


def split_make_model(title_core: str) -> Tuple[str, str]:
    """Simple split: first token = make, rest = model."""
    parts = title_core.split()
    return parts[0], " ".join(parts[1:])

# ---------------------------------------------------------------------------
# body‑map cache  (make → { slug: [body_types] })
# ---------------------------------------------------------------------------
_body_map: Dict[str, Dict[str, List[str]]] = {}

def load_body_map(make: str) -> Dict[str, List[str]]:
    make = make.lower()
    if make in _body_map:
        return _body_map[make]
    fn = Path(f"body_map_{make}.json")
    if fn.exists():
        _body_map[make] = json.loads(fn.read_text())
    else:
        _body_map[make] = {}
    return _body_map[make]

# ---------------------------------------------------------------------------
# scrape spec / colours sub‑pages
# ---------------------------------------------------------------------------

def scrape_specifications(slug: str) -> Tuple[Dict[str, Any], Optional[Dict[str, str]]]:
    url = f"{BASE}/{slug}/specifications"
    soup = fetch(url)

    specs: Dict[str, Any] = {}
    dimensions: Optional[str] = None
    engines: Dict[str, str] = {}
    grades: Dict[str, str]  = {}

    # dt/dd table patterns ---------------------------------------------------
    for dt in soup.select("div.summary-list__item dt"):
        key = dt.get_text(strip=True)
        val = dt.find_next("dd").get_text(" ", strip=True)
        specs[key] = val

    # dimensions block – numbers with mm inside h4 or li --------------------
    dim_texts: List[str] = []
    for t in soup.select("h4, li"):
        txt = t.get_text(" ", strip=True)
        if "mm" in txt and re.search(r"\d", txt):
            dim_texts.append(txt)
    if dim_texts:
        dimensions = " | ".join(dim_texts)

    # simple engines / grades: look for <h3>Engines / Trims etc -------------
    for section in soup.select("h3"):
        h = section.get_text(strip=True).lower()
        if "engine" in h:
            for li in section.find_all_next("li"):
                engines[len(engines)] = li.get_text(" ", strip=True)
                if len(engines) >= 10:
                    break
        if "trim" in h or "grade" in h:
            for li in section.find_all_next("li"):
                grades[len(grades)] = li.get_text(" ", strip=True)
                if len(grades) >= 10:
                    break

    extra = {}
    if dimensions:
        extra["dimensions_mm"] = dimensions
    if engines:
        extra["engines"] = list(engines.values())
    if grades:
        extra["grades"] = list(grades.values())

    return specs, extra or None


def scrape_colours(slug: str) -> List[str]:
    url = f"{BASE}/{slug}/colours"
    try:
        soup = fetch(url)
    except requests.HTTPError as e:
        if e.response.status_code == 404:
            return []
        raise
    colours: List[str] = []
    for h4 in soup.select("h4.model-hub__colour-details-title"):
        name = h4.contents[0].strip()  # first text node before <span>
        colours.append(name)
    return colours

# ---------------------------------------------------------------------------
# main model page parser
# ---------------------------------------------------------------------------

def parse_model_page(slug: str) -> Dict[str, Any]:
    url  = f"{BASE}/{slug}"
    soup = fetch(url)

    # -- title → make / model ----------------------------------------------
    h1 = soup.select_one("h1.header__title")
    if not h1:
        raise ValueError(f"cannot find title for {slug}")
    title_core = re.sub(r" Review.*", "", h1.get_text(strip=True))
    make_en, model_en = split_make_model(title_core)

    body_map = load_body_map(make_en)

    # -- At a glance --------------------------------------------------------
    glance = {
        dt.get_text(strip=True): dt.find_next("dd").get_text(strip=True)
        for dt in soup.select("div.summary-list__item dt")
    }

    body_type = glance.get("Body type") or body_map.get(slug, [])
    fuel      = glance.get("Available fuel types")
    doors     = glance.get("Number of doors")
    seats     = glance.get("Number of seats")
    drive_tp  = glance.get("Transmission") or glance.get("Drive type")

    # -- price --------------------------------------------------------------
    price_min_gbp = price_max_gbp = None
    rrp_span = soup.select_one("span.deals-cta-list__rrp-price")
    if rrp_span and "£" in rrp_span.text:
        prices = re.findall(r"£[\d,]+", rrp_span.text)
        if prices:
            price_min_gbp = to_int(prices[0])
            if len(prices) > 1:
                price_max_gbp = to_int(prices[1])
    else:  # fallback: Used price
        used_dd = soup.find("dt", string=re.compile("Used", re.I))
        if used_dd:
            price_min_gbp = to_int(used_dd.find_next("dd").text)

    # -- overview paragraph -------------------------------------------------
    overview_en = ""
    lead = soup.select_one("div#main p")
    if lead:
        overview_en = lead.get_text(strip=True)

    # -- images -------------------------------------------------------------
    imgs: List[str] = []
    for im in soup.select("img.media-slider__image, img.thumbnail-carousel-vertical__img"):
        src = im.get("data-src") or im.get("src")
        if src:
            clean = src.split("?")[0]
            if clean not in imgs:
                imgs.append(clean)
        if len(imgs) == MAX_IMAGES:
            break

    # ----------------------------------------------------------------------
    # specifications / colours pages
    # ----------------------------------------------------------------------
    specs_tbl, extra = scrape_specifications(slug)
    colours = scrape_colours(slug)

    # prefer spec‑page values if glance missing ----------------------------
    doors  = doors  or specs_tbl.get("Number of doors")
    seats  = seats  or specs_tbl.get("Number of seats")
    fuel   = fuel   or specs_tbl.get("Fuel type") or specs_tbl.get("Fuel types")
    drive_tp = drive_tp or specs_tbl.get("Transmission")

    dimensions_mm = extra.get("dimensions_mm") if extra else None

    # spec_json  – store raw spec table ------------------------------------
    spec_json = json.dumps(specs_tbl, ensure_ascii=False)

    return {
        "slug": slug,
        "make_en": make_en,
        "model_en": model_en,
        "body_type": body_type if isinstance(body_type, list) else [body_type] if body_type else [],
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
        "spec_json": spec_json,
        "catalog_url": url,
    }

# ---------------------------------------------------------------------------
# CLI wrapper
# ---------------------------------------------------------------------------

def cli():
    ap = argparse.ArgumentParser()
    ap.add_argument("--slugs", nargs="*", help="model slugs a/b c/d …")
    ap.add_argument("--make", help="scrape every slug in body_map_<make>.json")
    args = ap.parse_args()

    if not args.slugs and not args.make:
        ap.error("--slugs または --make を指定してください")

    slugs: List[str] = []
    if args.slugs:
        slugs += args.slugs
    if args.make:
        slugs += list(load_body_map(args.make).keys())

    for slug in slugs:
        try:
            data = parse_model_page(slug)
            print(json.dumps(data, ensure_ascii=False))
        except Exception as e:
            print(f"error:{slug}:{e}", file=sys.stderr)

if __name__ == "__main__":
    cli()
