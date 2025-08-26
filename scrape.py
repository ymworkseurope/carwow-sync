#!/usr/bin/env python3
"""carwow‑sync scraper — **r13 (2025‑09‑09)**
────────────────────────────────────────────────────────────────────
* **主な変更点**
  1. `/specifications` ページを <dt>/<dd> だけでなく **table th/td** もパース
  2. `/colours` ページで `<li>`・汎用 colour クラスも走査
  3. **リダイレクト検知** — 目的 URL と最終 URL が異なる場合は `RedirectError`
  4. **中古車価格 (used) を追加**  
     * `price_used_gbp`, `price_used_jpy` を算出（GBP→JPY は環境変数 `GBP_TO_JPY`）
  5. 画像取得上限を 40 枚へ拡張

* **CLI 例**
  ```bash
  python scrape.py                       # body_map_* 全モデル
  python scrape.py --make volvo          # volvo のみ
  python scrape.py --paths audi/q4-e-tron volkswagen/golf-r
  ```
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
HEADERS = {"User-Agent": "Mozilla/5.0 (carwow-sync/2025-r13)"}
TIMEOUT = 25
MAX_IMAGES = 40
GBP_TO_JPY = float(os.getenv("GBP_TO_JPY", "190"))  # fallback 190

SKIP_SLUG_PARTS = {
    "lease", "news", "mpv", "van", "camper", "commercial",
    "black", "white", "grey", "gray", "silver", "red", "blue", "green", "yellow", "orange",
    "purple", "brown", "beige", "gold", "bronze", "pink", "multi-colour", "multicolour",
    "multi-color", "multicolor", "colour", "color", "automatic", "manual",
}

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

try:
    from gsheets_helper import upsert as gsheets_upsert  # type: ignore
except ImportError:
    gsheets_upsert = None

# ────────────────────────── UUID helper
UUID_NS = uuid.UUID("12345678-1234-5678-1234-123456789012")

def slug_uuid(slug: str) -> str:
    return str(uuid.uuid5(UUID_NS, slug))

# ────────────────────────── HTTP helpers
class RedirectError(Exception):
    pass

def fetch(url: str) -> BeautifulSoup:
    for i in range(4):
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT, allow_redirects=True)
        if r.ok:
            if r.url.rstrip("/") != url.rstrip("/") and len(r.history) > 0:
                raise RedirectError(f"redirect→ {r.url}")
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
COMPOUND_MAKES = [
    "Rolls-Royce",
    "Mercedes-Benz",
    "Aston Martin",
    "Land Rover",
    "Alfa Romeo",
]
COMPOUND_MAKES.sort(key=len, reverse=True)

def split_make_model(title: str) -> Tuple[str, str]:
    for mk in COMPOUND_MAKES:
        if title.lower().startswith(mk.lower() + " "):
            return mk, title[len(mk) :].strip()
    parts = title.split(" ", 1)
    return parts[0], parts[1] if len(parts) > 1 else parts[0]

# ────────────────────────── body_map cache
_body_map: Dict[str, Dict[str, List[str]]] = {}

def load_body_map(make: str) -> Dict[str, List[str]]:
    mk = make.lower()
    if mk in _body_map:
        return _body_map[mk]
    fp = Path(f"body_map_{mk}.json")
    if not fp.exists():
        _body_map[mk] = {}
        return {}
    raw: Dict[str, List[str]] = json.loads(fp.read_text())
    fixed = { (slug if "/" in slug else f"{mk}/{slug}"): v for slug, v in raw.items() }
    _body_map[mk] = fixed
    return fixed

# ────────────────────────── __NEXT_DATA__ helper

def parse_next_data(soup: BeautifulSoup) -> Dict[str, Any]:
    script = soup.find("script", id="__NEXT_DATA__")
    return json.loads(script.string) if script and script.string else {}

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

    prod = j.get("props", {}).get("pageProps", {}).get("product", {})
    add(prod.get("heroImage", ""))
    for g in prod.get("galleryImages", []):
        add(g.get("url", ""))
        if len(urls) >= MAX_IMAGES:
            return urls[:MAX_IMAGES]
    for g in prod.get("mediaGallery", []):
        add(g.get("url", ""))
        if len(urls) >= MAX_IMAGES:
            return urls[:MAX_IMAGES]
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
    prod = j.get("props", {}).get("pageProps", {}).get("product", {})
    intro = prod.get("review", {}).get("intro")
    if intro and len(intro) >= 30:
        return intro.strip()
    for sel in ("div.review-overview__intro p", "article p", "div#main p", "p"):
        tag = soup.select_one(sel)
        if tag and len(tag.get_text(strip=True)) >= 30:
            return tag.get_text(" ", strip=True)
    meta = soup.find("meta", attrs={"name": "description"})
    return meta.get("content", "").strip() if meta else ""

# ────────────────────────── spec helpers

def scrape_specifications(path: str, j: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    prod = j.get("props", {}).get("pageProps", {}).get("product", {})
    spec: Dict[str, Any] = prod.get("specifications", {}).copy()
    extra: Dict[str, Any] = {}
    dims = prod.get("dimensions", {})
    if dims:
        l = dims.get("length") or dims.get("lengthMm")
        w = dims.get("width") or dims.get("widthMm")
        h = dims.get("height") or dims.get("heightMm")
        if l and w and h:
            extra["dimensions_mm"] = f"{l} x {w} x {h} mm"

    url = f"{BASE}/{path}/specifications"
    try:
        soup = fetch(url)
    except requests.HTTPError as e:
        if e.response.status_code == 404:
            return spec, extra
        raise
    except RedirectError:
        return spec, extra

    # dt/dd pairs
    for dt in soup.select("dt"):
        dd = dt.find_next("dd")
        if dd:
            spec[dt.get_text(strip=True)] = dd.get_text(" ", strip=True)
    # th/td table pairs
    for row in soup.select("table tr"):
        th = row.find(["th", "td"])
        td = th.find_next("td") if th else None
        if th and td:
            spec[th.get_text(strip=True)] = td.get_text(" ", strip=True)
    return spec, extra


def scrape_colours(path: str, prod_json: Dict[str, Any]) -> List[str]:
    colours = prod_json.get("colours") or prod_json.get("colors") or []
    if colours:
        return [c if isinstance(c, str) else c.get("name", "") for c in colours]
    url = f"{BASE}/{path}/colours"
    try:
        soup = fetch(url)
    except (requests.HTTPError, RedirectError):
        return []
    names: List[str] = []
    for h4 in soup.select("h4.model-hub__colour-details-title"):
        names.append(h4.get_text(strip=True))
    for li in soup.select("li"):
        if li.get("class") and any("colour" in c for c in li["class"]):
            names.append(li.get_text(" ", strip=True))
    return list(dict.fromkeys(names))  # uniq + preserve order

# ────────────────────────── model parser

def parse_model_page(path: str) -> Dict[str, Any]:
    try:
        soup = fetch(f"{BASE}/{path}")
    except RedirectError as e:
        raise ValueError(f"{path}: redirected ({e})")

    next_json = parse_next_data(soup)
    h1 = soup.find("h1")
    if not h1:
        raise ValueError(f"<h1> missing for {path}")
    title_core = re.sub(r" review.*", "", h1.get_text(strip=True), flags=re.I)
    make_en, model_en = split_make_model(title_core)

    body_type = load_body_map(make_en).get(path, [])
    overview_en = extract_overview(soup, next_json)
    prod_json = next_json.get("props", {}).get("pageProps", {}).get("product", {})

    price_min_gbp = prod_json.get("priceMin") or prod_json.get("rrpMin")
    price_max_gbp = prod_json.get("priceMax") or prod_json.get("rrpMax")
    price_used_gbp = None

    if not price_min_gbp:
        rrp = soup.select_one("span.deals-cta-list__rrp-price")
        if rrp and "£" in rrp.text:
            pounds = re.findall(r"£[\d,]+", rrp.text)
            if pounds:
                price_min_gbp = to_int(pounds[0])
                price_max_gbp = to_int(pounds[-1])

    imgs = collect_images(soup, next_json)

    glance = {
        dt.get_text(strip=True): dt.find_next("dd").get_text(strip=True)
        for dt in soup.select("div.model-hub__summary-list dt")
    }

    spec_tbl, extra = scrape_specifications(path, next_json)

    # 中古車価格 (Used)
    for key in ("Used", "Used price", "Used Price"):
        if key in spec_tbl and spec_tbl[key]:
            price_used_gbp = to_int(spec_tbl[key])
            break

    colours = scrape_colours(path, prod_json)

    fuel_set = {prod_json.get("fuelType")}
    for var in prod_json.get("variants", []):
        if var.get("fuelType"):
            fuel_set.add(var["fuelType"])
    fuel_set.discard(None)
    fuel = ", ".join(sorted(fuel_set)) if fuel_set else None

    grades = [t.get("name") for t in prod_json.get("trims", []) if t.get("name")]

    def pick(*keys):
        for k in keys:
            if k in glance and glance[k]:
                return glance[k]
            if k in spec_tbl and spec_tbl[k]:
                return spec_tbl[k]
            if k in prod_json and prod_json[k]:
                return prod_json[k]
        return None

    price_used_jpy = int(price_used_gbp * GBP_TO_JPY) if price_used_gbp else None

    return {
        "id": slug_uuid(path),
        "slug": path,
        "make_en": make_en,
        "model_en": model_en,
        "body_type": body_type,
        "fuel": fuel or pick("Available fuel", "Fuel type"),
        "price_min_gbp": price_min_gbp,
        "price_max_gbp": price_max_gbp,
        "price_used_gbp": price_used_gbp,
        "price_min_jpy": int(price_min_gbp * GBP_TO_JPY) if price_min_gbp else None,
        "price_max_jpy": int(price_max_gbp * GBP_TO_JPY) if price_max_gbp else None,
        "price_used_jpy": price_used_jpy,
        "doors": pick("Number of doors", "doors"),
        "seats": pick("Number of seats", "seats"),
        "dimensions_mm": extra.get("dimensions_mm"),
        "drive_type": pick("Transmission", "Drive type", "transmission"),
        "overview_en": overview_en,
        "media_urls": imgs,
        "colors": colours,
        "grades": grades or None,
        "engines": prod_json.get("engines"),
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
        timeout=35,
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
            slugs.append(full)
    return sorted(set(slugs))

# ──────────────── CLI

def cli():
    p = argparse.ArgumentParser()
    p.add_argument("--make", help="メーカー単位で絞る (ex: volvo)")
    p.add_argument("--paths", nargs="*", metavar="SLUG", help="モデルslugを列挙 (ex: audi/q4-e-tron)")
    args = p.parse_args()

    if args.paths:
        process(args.paths)
        return
    if args.make:
        bm = load_body_map(args.make)
        if not bm:
            print(f"body_map for '{args.make}' not found", file=sys.stderr)
            return
        process(sorted(bm.keys()))
        return
    process(collect_all_slugs())


if __name__ == "__main__":
    cli()
