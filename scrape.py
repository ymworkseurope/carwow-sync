#!/usr/bin/env python3
"""carwow-sync scraper — 2025-09 (fixed-r7)
────────────────────────────────────────────────────────────────────
* 404 大量発生／2 語メーカー名バグ／body_map スラグ不一致を修正。
* body_map_<make>.json を「make/slug」形式に正規化して参照。
* `split_make_model()` は複語メーカーを完全サポート。
* CLI は --make / --slugs 併用 OK。未指定なら全 body_map を総当たり。
* Supabase / Google Sheets アップサートは従来通り。
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

# ────────────────────────── Global settings
BASE = "https://www.carwow.co.uk"
HEADERS = {"User-Agent": "Mozilla/5.0 (carwow-sync/2025-r7)"}
TIMEOUT = 20
MAX_IMAGES = 20

SKIP_SLUG_PARTS = {
    # 色／特殊パスなど — ここに含まれるトークンが slug に現れたらスキップ
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

# Supabase creds (GitHub Actions Secrets 想定)
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Google Sheets (任意)
try:
    from gsheets_helper import upsert as gsheets_upsert  # type: ignore
except ImportError:
    gsheets_upsert = None

# ────────────────────────── HTTP helpers

def fetch(url: str) -> BeautifulSoup:
    """GET + lxml parse（シンプルな指数バックオフ）。"""
    for i in range(3):
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        if r.ok:
            return BeautifulSoup(r.text, "lxml")
        if r.status_code == 404:
            raise requests.HTTPError(response=r)
        time.sleep(1 + i)
    r.raise_for_status()


def to_int(pounds: str | None) -> Optional[int]:
    if not pounds:
        return None
    pounds = pounds.replace("£", "").replace(",", "").strip()
    return int(pounds) if pounds.isdigit() else None


# ────────────────────────── make / model 分割ユーティリティ
# 2 語以上のメーカー名リストは長い順に並べて前方一致で判定
COMPOUND_MAKES = [
    "Rolls-Royce",
    "Mercedes-Benz",
    "Aston Martin",
    "Land Rover",
    "Alfa Romeo",
]
COMPOUND_MAKES.sort(key=len, reverse=True)


def split_make_model(title: str) -> Tuple[str, str]:
    """ページタイトルから make / model を推定。"""
    for mk in COMPOUND_MAKES:
        if title.lower().startswith(mk.lower() + " "):
            return mk, title[len(mk) :].strip()
    first, *rest = title.split(" ", 1)
    return first, rest[0] if rest else first


# ────────────────────────── body_map キャッシュ
_body_map: Dict[str, Dict[str, List[str]]] = {}


def load_body_map(make: str) -> Dict[str, List[str]]:
    make = make.lower()
    if make in _body_map:
        return _body_map[make]
    fn = Path(f"body_map_{make}.json")
    if not fn.exists():
        _body_map[make] = {}
        return {}
    raw: Dict[str, List[str]] = json.loads(fn.read_text())
    # フォーマット揺れ対策: キーが単独 slug の場合 → make/slug に正規化
    fixed: Dict[str, List[str]] = {}
    for k, v in raw.items():
        fixed_key = k if "/" in k else f"{make}/{k}"
        fixed[fixed_key] = v
    _body_map[make] = fixed
    return fixed


# ────────────────────────── 補助スクレイパ

def scrape_specifications(path: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    url = f"{BASE}/{path}/specifications"
    try:
        soup = fetch(url)
    except requests.HTTPError as e:
        if e.response.status_code == 404:
            return {}, {}
        raise

    specs: Dict[str, Any] = {}
    extra: Dict[str, Any] = {}

    for dt in soup.select("div.model-hub__summary-list dt"):
        specs[dt.get_text(strip=True)] = dt.find_next("dd").get_text(" ", strip=True)

    dim_pat = re.compile(r"\b\d{3,4}\s?mm\b")
    dims = [t.get_text(" ", strip=True) for t in soup.select("h4, li") if dim_pat.search(t.get_text())]
    if dims:
        extra["dimensions_mm"] = " | ".join(dims)

    def _harvest(section_kw: str, key: str):
        for h in soup.select("h3"):
            if section_kw in h.get_text(strip=True).lower():
                extra[key] = [li.get_text(" ", strip=True) for li in h.find_all_next("li")][:10]
                break

    _harvest("engine", "engines")
    _harvest("trim", "grades")
    _harvest("grade", "grades")

    return specs, extra


def scrape_colours(path: str) -> List[str]:
    url = f"{BASE}/{path}/colours"
    try:
        soup = fetch(url)
    except requests.HTTPError as e:
        if e.response.status_code == 404:
            return []
        raise
    return [h4.get_text(strip=True) for h4 in soup.select("h4.model-hub__colour-details-title")]


# ────────────────────────── モデルページ解析

def parse_model_page(path: str) -> Dict[str, Any]:
    soup = fetch(f"{BASE}/{path}")

    h1 = soup.find("h1")
    if not h1:
        raise ValueError(f"No <h1> for {path}")
    title_core = re.sub(r" review.*", "", h1.get_text(strip=True), flags=re.I)

    make_en, model_en = split_make_model(title_core)

    body_map = load_body_map(make_en)
    body_type = body_map.get(path, [])  # prefer body_map 1st

    # 概要
    lead = soup.select_one("div#main p")
    overview_en = lead.get_text(strip=True) if lead else ""

    # 価格
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

    # スライダー画像
    imgs: List[str] = []
    for im in soup.select("img.media-slider__image, img.thumbnail-carousel-vertical__img"):
        src = im.get("data-src") or im.get("src")
        if src:
            clean = src.split("?")[0]
            if clean not in imgs:
                imgs.append(clean)
            if len(imgs) == MAX_IMAGES:
                break

    # サマリー表
    glance = {
        dt.get_text(strip=True): dt.find_next("dd").get_text(strip=True)
        for dt in soup.select("div.model-hub__summary-list dt")
    }

    fuel = glance.get("Available fuel types")
    doors = glance.get("Number of doors")
    seats = glance.get("Number of seats")
    drive_tp = glance.get("Transmission") or glance.get("Drive type")

    spec_tbl, extra = scrape_specifications(path)
    colours = scrape_colours(path)

    # fallback / merge
    doors = doors or spec_tbl.get("Number of doors")
    seats = seats or spec_tbl.get("Number of seats")
    fuel = fuel or spec_tbl.get("Fuel type") or spec_tbl.get("Fuel types")
    drive_tp = drive_tp or spec_tbl.get("Transmission")

    return {
        "id": path.replace("/", "-"),  # Supabase PK
        "slug": path,
        "make_en": make_en,
        "model_en": model_en,
        "body_type": body_type,
        "fuel": fuel,
        "price_min_gbp": price_min_gbp,
        "price_max_gbp": price_max_gbp,
        "doors": doors,
        "seats": seats,
        "dimensions_mm": extra.get("dimensions_mm"),
        "drive_type": drive_tp,
        "overview_en": overview_en,
        "media_urls": imgs,
        "colors": colours,
        "grades": extra.get("grades"),
        "engines": extra.get("engines"),
        "spec_json": json.dumps(spec_tbl, ensure_ascii=False),
        "catalog_url": f"{BASE}/{path}",
    }


# ────────────────────────── Supabase / Sheets

def validate(item: Dict[str, Any]) -> Tuple[bool, str]:
    errs = []
    for f in ("id", "slug", "make_en", "model_en"):
        if not item.get(f):
            errs.append(f"missing {f}")
    return (not errs, "; ".join(errs))


def db_upsert(item: Dict[str, Any]):
    if not (SUPABASE_URL and SUPABASE_KEY):
        return
    ok, msg = validate(item)
    if not ok:
        print(f"validation:{item['slug']}: {msg}", file=sys.stderr)
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


# ────────────────────────── crawler loop

def process(paths: List[str]):
    for path in paths:
        if any(tok in SKIP_SLUG_PARTS for tok in path.split("/")):
            continue
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


# ────────────────────────── collect targets

def collect_all_slugs() -> List[str]:
    """body_map_*.json 全てを読み込み、make/slug 形式で返す"""
    targets: List[str] = []
    for bf in Path.cwd().glob("body_map_*.json"):
        make = bf.stem.replace("body_map_", "")
        for s in json.loads(bf.read_text()).keys():
            targets.append(s if "/" in s else f"{make}/{s}")
    return sorted(set(targets))


# ────────────────────────── CLI

def cli():
    ap = argparse.ArgumentParser()
    ap.add_argument("--slugs", nargs="*", help="model slugs (make/slug or slug)")
    ap.add_argument("--make", help="target make (use body_map_make.json contents)")
    args = ap.parse_args()

    paths: List[str] = []
    if not args.slugs and not args.make:
        paths = collect_all_slugs()
    else:
        if args.slugs:
            for raw in args.slugs:
                if "/" in raw:
                    paths.append(raw)
                else:
                    print(f"warn: skipping ambiguous slug '{raw}' (needs make/)", file=sys.stderr)
        if args.make:
            paths.extend(load_body_map(args.make).keys())

    if not paths:
        sys.exit("no target models")

    process(sorted(set(paths)))


if __name__ == "__main__":
    cli()
