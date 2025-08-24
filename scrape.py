#!/usr/bin/env python3
"""Carwow full‑scraper  – production edition (2025‑09‑final‑r2)
──────────────────────────────────────────────────────────────
* **1 model ⇒ 最大 3 HTTP** (main /specifications /colours)
* Supabase 用の全カラムを収集。
* `body_map_<make>.json` がある場合は body_type を補完。
* **引数ゼロなら**: カレントディレクトリに存在するすべての
  `body_map_*.json` を巡回して *全車* を取得します。

主な修正 (r2)
--------------
* **“/make/slug” 形式で URL を組み立てる** ─ 旧版は body_map に
  ベアスラッグ (例 `q5`) が入っていると 404 になっていた。
* CLI で body_map を展開する際、必ず `make/slug` を付与。
* `parse_model_page()` 側でもセーフティ: もし 404 が返ったら
  `/make/slug` を再試行（将来の手書きスラッグでも落ちない）。
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
MAX_IMAGES = 20      # cap images at 20 / model

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def fetch(url: str) -> BeautifulSoup:
    """GET + parse with small retry back‑off"""
    for i in range(3):
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        if r.ok:
            return BeautifulSoup(r.text, "lxml")
        if r.status_code == 404:
            raise requests.HTTPError(response=r)
        time.sleep(1 + i)  # 1s, 2s
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

    # dt/dd pairs → raw spec table ----------------------------------------
    for dt in soup.select("div.summary-list__item dt"):
        key = dt.get_text(strip=True)
        val = dt.find_next("dd").get_text(" ", strip=True)
        specs[key] = val

    # Dimensions (mm) ------------------------------------------------------
    dims: List[str] = []
    for t in soup.select("h4, li"):
        txt = t.get_text(" ", strip=True)
        if "mm" in txt and re.search(r"\d", txt):
            dims.append(txt)
    if dims:
        extra["dimensions_mm"] = " | ".join(dims)

    # Engines / Trims ------------------------------------------------------
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

# ---------------------------------------------------------------------------
# main model page parser
# ---------------------------------------------------------------------------

def try_fetch_model(slug: str, make_hint: Optional[str] = None) -> BeautifulSoup:
    """Fetch /slug ; on 404 retry /make/slug (if make provided)."""
    try:
        return fetch(f"{BASE}/{slug}")
    except requests.HTTPError as e:
        if e.response.status_code == 404 and make_hint and "/" not in slug:
            # retry with make prefix
            return fetch(f"{BASE}/{make_hint}/{slug}")
        raise


def parse_model_page(slug: str, make_hint: Optional[str] = None) -> Dict[str, Any]:
    soup = try_fetch_model(slug, make_hint)
    final_url_path = soup.find("link", rel="canonical").get("href", "").replace(BASE + "/", "") or slug

    # title → make / model -------------------------------------------------
    h1 = soup.select_one("h1.header__title")
    if not h1:
        raise ValueError(f"no title for {slug}")
    title_core = re.sub(r" Review.*", "", h1.get_text(strip=True))
    make_en, model_en = split_make_model(title_core)

    body_map = load_body_map(make_en)

    # At a glance ----------------------------------------------------------
    glance = {dt.get_text(strip=True): dt.find_next("dd").get_text(strip=True)
              for dt in soup.select("div.summary-list__item dt")}

    body_type = glance.get("Body type") or body_map.get(final_url_path, [])
    fuel      = glance.get("Available fuel types")
    doors     = glance.get("Number of doors")
    seats     = glance.get("Number of seats")
    drive_tp  = glance.get("Transmission") or glance.get("Drive type")

    # price ----------------------------------------------------------------
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

    # overview -------------------------------------------------------------
    lead = soup.select_one("div#main p")
    overview_en = lead.get_text(strip=True) if lead else ""

    # images ---------------------------------------------------------------
    imgs: List[str] = []
    for im in soup.select("img.media-slider__image, img.thumbnail-carousel-vertical__img"):
        src = im.get("data-src") or im.get("src")
        if src:
            url_clean = src.split("?")[0]
            if url_clean not in imgs:
                imgs.append(url_clean)
        if len(imgs) == MAX_IMAGES:
            break

    # specs / colours ------------------------------------------------------
    spec_tbl, extra = scrape_specifications(final_url_path)
    colours = scrape_colours(final_url_path)

    # fallback enrich ------------------------------------------------------
    doors  = doors  or spec_tbl.get("Number of doors")
    seats  = seats  or spec_tbl.get("Number of seats")
    fuel   = fuel   or spec_tbl.get("Fuel type") or spec_tbl.get("Fuel types")
    drive_tp = drive_tp or spec_tbl.get("Transmission")
    dimensions_mm = extra.get("dimensions_mm") if extra else None

    # serialise raw spec table --------------------------------------------
    spec_json = json.dumps(spec_tbl, ensure_ascii=False)

    return {
        "slug": final_url_path,
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
        "spec_json": spec_json,
        "catalog_url": f"{BASE}/{final_url_path}",
    }

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def discover_all_body_maps() -> List[str]:
    return [p.stem.replace("body_map_", "") for p in Path(".").glob("body_map_*.json")]


def expand_slugs(make: str, inner_slugs: List[str]) -> List[str]:
    """Prefix bare slugs with make/ for correct URL path."""
    out: List[str] = []
    for s in inner_slugs:
        if "/" in s:
            out.append(s)
        else:
            out.append(f"{make}/{s}")
    return out


def cli():
    ap = argparse.ArgumentParser()
    ap.add_argument("--slugs", nargs="*", help="model slugs a/b c/d …")
    ap.add_argument("--make", help="scrape all slugs in body_map_<make>.json")
    args = ap.parse_args()

    slugs: List[str] = []

    if args.slugs:
        slugs += args.slugs
    if args.make:
        bm = load_body_map(args.make)
        slugs += expand_slugs(args.make, list(bm.keys()))

    if not slugs:
        for make in discover_all_body_maps():
            bm = load_body_map(make)
            slugs.extend(expand_slugs(make, list(bm.keys())))
        if not slugs:
            ap.error("No body_map_*.json found and no arguments supplied")

    seen = set()
    for slug in slugs:
        if slug in seen:
            continue
        seen.add(slug)
        try:
            make_hint = slug.split("/")[0] if "/" in slug else None
            data = parse_model_page(slug, make_hint)
            print(json.dumps(data, ensure_ascii=False))
        except Exception as e:
            print(f"error:{slug}:{e}", file=sys.stderr)

if __name__ == "__main__":
    cli()
