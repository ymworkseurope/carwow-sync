#!/usr/bin/env python3
"""carwow-sync scraper — **r10 (2025-09-06)**
────────────────────────────────────────────────────────────────────
● **今回のパッチ内容**
  1. **Supabase 400** ─ `id` を UUIDv5(slug) に統一（継続）。
  2. **overview_en & media_urls 完全取得**
     * `<script id="__NEXT_DATA__">` の JSON を解析し、
       * `product.review.intro` → overview
       * `product.galleryImages[*].url` → 画像
       * これで **JS 後挿入の本文 / 高解像度画像** も取得可能。
  3. **静的 HTML フォールバック**
     * 従来の `<em>` や `<meta name="description">` も残し多重フォールバック。
  4. **画像収集**
     * `<img|source>` の `src | data-src | srcset | data-srcset` を全て走査。
     * JSON 由来分と合わせ **重複排除して 20 枚上限**。
  5. **スペック抽出**
     * `__NEXT_DATA__ → product.specifications` から doors / seats / transmission 等を直接取得。
     * HTML 上の summary-list も併用。
  6. **skip リスト拡充** — 色 / lease / news など 43 語。
  7. **CLI** ― `--slugs` & `--make` 両対応はそのまま。

*Python >= 3.9 / requests / beautifulsoup4 / lxml が前提*
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

# ────────────────────────── Global settings
BASE = "https://www.carwow.co.uk"
HEADERS = {"User-Agent": "Mozilla/5.0 (carwow-sync/2025-r10)"}
TIMEOUT = 20
MAX_IMAGES = 20

# スキップする slug トークン（43 語）
SKIP_SLUG_PARTS = {
    # サービス/雑
    "lease", "news", "mpv",
    # 色
    "black", "white", "grey", "gray", "silver", "red", "blue", "green", "yellow", "orange",
    "purple", "brown", "beige", "gold", "bronze", "pink", "multi-colour", "multi-color", "colour", "color",
    # ミッション
    "automatic", "manual",
}

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

try:
    from gsheets_helper import upsert as gsheets_upsert  # type: ignore
except ImportError:
    gsheets_upsert = None

# ────────────────────────── UUID helper
UUID_NS = uuid.UUID("12345678-1234-5678-1234-123456789012")  # 固定 namespace

def slug_uuid(slug: str) -> str:
    return str(uuid.uuid5(UUID_NS, slug))

# ────────────────────────── HTTP helpers

def fetch(url: str) -> BeautifulSoup:
    for i in range(3):
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        if r.ok:
            return BeautifulSoup(r.text, "lxml")
        if r.status_code == 404:
            raise requests.HTTPError(response=r)
        time.sleep(1 + i)
    r.raise_for_status()


def to_int(val: str | None) -> Optional[int]:
    if not val:
        return None
    digits = re.sub(r"[^0-9]", "", val)
    return int(digits) if digits.isdigit() else None

# ────────────────────────── make / model split
COMPOUND_MAKES = sorted(
    [
        "Rolls-Royce",
        "Mercedes-Benz",
        "Aston Martin",
        "Land Rover",
        "Alfa Romeo",
    ],
    key=len,
    reverse=True,
)

def split_make_model(title: str) -> Tuple[str, str]:
    for mk in COMPOUND_MAKES:
        if title.lower().startswith(mk.lower() + " "):
            return mk, title[len(mk) :].strip()
    parts = title.split(" ", 1)
    return parts[0], parts[1] if len(parts) > 1 else parts[0]

# ────────────────────────── body_map cache
_body_map: Dict[str, Dict[str, List[str]]] = {}

def load_body_map(make: str) -> Dict[str, List[str]]:
    make_l = make.lower()
    if make_l in _body_map:
        return _body_map[make_l]
    fp = Path(f"body_map_{make_l}.json")
    if not fp.exists():
        _body_map[make_l] = {}
        return {}
    raw: Dict[str, List[str]] = json.loads(fp.read_text())
    fixed: Dict[str, List[str]] = {}
    for k, v in raw.items():
        slug = k if "/" in k else f"{make_l}/{k}"
        fixed[slug] = v
    _body_map[make_l] = fixed
    return fixed

# ────────────────────────── __NEXT_DATA__ helper

def parse_next_data(soup: BeautifulSoup) -> Dict[str, Any]:
    script = soup.find("script", id="__NEXT_DATA__")
    if not script or not script.string:
        return {}
    try:
        return json.loads(script.string)
    except Exception:
        return {}

# ────────────────────────── overview & images
_ALLOWED_IMG_DOMS = ("images.prismic.io", "carwow", "imgix.net")

def collect_images(soup: BeautifulSoup, j: Dict[str, Any]) -> List[str]:
    urls: List[str] = []

    def add(u: str):
        if not u or not u.startswith("http"):
            return
        u = u.split("?")[0]
        if any(dom in u for dom in _ALLOWED_IMG_DOMS) and u not in urls:
            urls.append(u)

    # 1) JSON ギャラリー
    gallery = (
        j.get("props", {})
        .get("pageProps", {})
        .get("product", {})
        .get("galleryImages", [])
    )
    for g in gallery:
        add(g.get("url", ""))
        if len(urls) >= MAX_IMAGES:
            return urls[:MAX_IMAGES]

    # 2) <img> & <source>
    for tag in soup.select("img, source"):
        for attr in ("data-srcset", "srcset", "data-src", "src"):
            src = tag.get(attr)
            if not src:
                continue
            src = src.split(",")[-1].split()[0]
            if src.startswith("//"):
                src = "https:" + src
            add(src)
            if len(urls) >= MAX_IMAGES:
                return urls[:MAX_IMAGES]
    return urls[:MAX_IMAGES]


def extract_overview(soup: BeautifulSoup, j: Dict[str, Any]) -> str:
    # 1) NEXT_DATA JSON
    intro = (
        j.get("props", {})
        .get("pageProps", {})
        .get("product", {})
        .get("review", {})
        .get("intro")
    )
    if intro and len(intro) >= 30:
        return intro.strip()

    # 2) 静的 HTML
    css_try = [
        "div.review-overview__content em",
        "div.review-overview__intro p",
        "article p",
        "div#main p",
        "p",
    ]
    for sel in css_try:
        tag = soup.select_one(sel)
        if tag and len(tag.get_text(strip=True)) >= 30:
            return tag.get_text(" ", strip=True)

    # 3) meta description
    meta = soup.find("meta", attrs={"name": "description"})
    if meta and meta.get("content"):
        return meta["content"].strip()
    return ""

# ────────────────────────── spec helpers

def scrape_specifications(path: str, j: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    # 1) JSON から
    prod = (
        j.get("props", {})
        .get("pageProps", {})
        .get("product", {})
    )
    spec: Dict[str, Any] = prod.get("specifications", {}) if prod else {}

    extra: Dict[str, Any] = {}
    if prod:
        if "dimensions" in prod:
            d = prod["dimensions"]
            extra["dimensions_mm"] = (
                f"{d.get('length')} x {d.get('width')} x {d.get('height')} mm"
                if d.get("length") else None
            )
    # 2) fallback HTML table
    url = f"{BASE}/{path}/specifications"
    try:
        soup = fetch(url)
    except requests.HTTPError as e:
        if e.response.status_code == 404:
            return spec, extra
        raise

    for dt in soup.select("dt"):
        dd = dt.find_next("dd")
        if dd:
            spec[dt.get_text(strip=True)] = dd.get_text(" ", strip=True)

    return spec, extra


def scrape_colours(path: str) -> List[str]:
    url = f"{BASE}/{path}/colours"
    try:
        soup = fetch(url)
    except requests.HTTPError as e:
        if e.response.status_code == 404:
            return []
        raise
    return [h4.get_text(strip=True) for h4 in soup.select("h4.model-hub__colour-details-title")]

# ────────────────────────── model parser

def parse_model_page(path: str) -> Dict[str, Any]:
    soup = fetch(f"{BASE}/{path}")
    next_json = parse_next_data(soup)

    h1 = soup.find("h1")
    if not h1:
        raise ValueError(f"<h1> missing for {path}")
    title_core = re.sub(r" review.*", "", h1.get_text(strip=True), flags=re.I)

    make_en, model_en = split_make_model(title_core)
    body_type = load_body_map(make_en).get(path, [])
    overview_en = extract_overview(soup, next_json)

    # 価格 (JSON 優先)
    price_min_gbp = price_max_gbp = None
    prod_json = (
        next_json.get("props", {})
        .get("pageProps", {})
        .get("product", {})
    )
    if prod_json:
        price_min_gbp = prod_json.get("priceMin") or prod_json.get("rrpMin")
        price_max_gbp = prod_json.get("priceMax") or prod_json.get("rrpMax")

    if not price_min_gbp:
        rrp = soup.select_one("span.deals-cta-list__rrp-price")
        if rrp and "£" in rrp.text:
            pounds = re.findall(r"£[\d,]+", rrp.text)
            if pounds:
                price_min_gbp = to_int(pounds[0])
                price_max_gbp = to_int(pounds[-1])

    imgs = collect_images(soup, next_json)

    # glance table (HTML)
    glance = {
        dt.get_text(strip=True): dt.find_next("dd").get_text(strip=True)
        for dt in soup.select("div.model-hub__summary-list dt")
    }

    spec_tbl, extra = scrape_specifications(path, next_json)
    colours = scrape_colours(path)

    def pick(*keys):
        for k in keys:
            if k in glance and glance[k]:
                return glance[k]
            if k in spec_tbl and spec_tbl[k]:
                return spec_tbl[k]
            if prod_json and k in prod_json and prod_json[k]:
                return prod_json[k]
        return None

    return {
        "id": slug_uuid(path),
        "slug": path,
        "make_en": make_en,
        "model_en": model_en,
        "body_type": body_type,
        "fuel": pick("Available fuel", "Fuel type", "fuelType"),
        "price_min_gbp": price_min_gbp,
        "price_max_gbp": price_max_gbp,
        "doors": pick("Number of doors", "doors"),
        "seats": pick("Number of seats", "seats"),
        "dimensions_mm": extra.get("dimensions_mm"),
        "drive_type": pick("Transmission", "Drive type", "transmission"),
        "overview_en": overview_en,
        "media_urls": imgs,
        "colors": colours or prod_json.get("colours", []) if prod_json else [],
        "grades": extra.get("grades"),
        "engines": extra.get("engines"),
        "spec_json": json.dumps(spec_tbl, ensure_ascii=False),
        "catalog_url": f"{BASE}/{path}",
    }

# ────────────────────────── Supabase & Sheets

def validate(item: Dict[str, Any]) -> Tuple[bool, str]:
    miss = [k for k in ("id", "slug", "make_en", "model_en") if not item.get(k)]
    return not miss, ", ".join(miss)


def db_upsert(item: Dict[str, Any]):
    if not (SUPABASE_URL and SUPABASE_KEY):
        return
    ok, msg = validate(item)
    if not ok:
        print(f"validate:{item['slug']}: {msg}", file=sys.stderr)
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
        print(f"supabase:{item['slug']}: [{r.status_code}] {r.text}", file=sys.stderr)

# ────────────────────────── crawl loop

def process(paths: List[str]):
    seen: set[str] = set()
    for path in paths:
        if path in seen or any(tok in SKIP_SLUG_PARTS for tok in path.split("/")):
            continue
        seen.add(path)
        try:
            data = parse_model_page(path)
            print(json.dumps(data, ensure_ascii=False))
            db_upsert(data)
            if gsheets_upsert:
                try:
                    gsheets_upsert(data)
                except Exception as e:
                    print(f"gsheets:{path}:{e}", file=sys.stderr)
        except Exception as e:
            print(f"error:{path}:{e}", file=sys.stderr)

# ────────────────────────── utilities

def collect_all_slugs() -> List[str]:
    slugs: List[str] = []
    for bf in Path.cwd().glob("body_map_*.json"):
        make = bf.stem.replace("body_map_", "")
        for k in json.loads(bf.read_text()).keys():
            full = k if "/" in k else f"{make}/{k}"
            if not any(tok in SKIP_SLUG_PARTS for tok in full.split("/")):
                slugs.append(full)
    return sorted(set(slugs))

# ────────────────────────── CLI

def cli():
    ap = argparse.ArgumentParser()
    ap.add_argument("--slugs", nargs="*", help="model slugs make/slug …")
    ap.add_argument("--make", help="target make (loads body_map_<make>.json)")
    args = ap.parse_args()

    targets: List[str] = []
    if not args.slugs and not args.make:
        targets = collect_all_slugs()
    else:
        if args.slugs:
            for s in args.slugs:
                if "/" in s:
                    targets.append(s)
                else:
                    print(f"warn: ambiguous slug '{s}' — needs make/slug", file=sys.stderr)
        if args.make:
            targets.extend(load_body_map(args.make).keys())

    if not targets:
        sys.exit("no valid targets")

    process(sorted(targets))

if __name__ == "__main__":
    cli()
