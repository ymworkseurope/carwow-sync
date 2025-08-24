#!/usr/bin/env python3
# model_scraper.py – 2025-09-xx json-ld + slug-fix

"""
単一モデルページを解析し:
  * slug / make / model
  * 価格・ボディタイプ・燃料・画像
を dict で返す。
"""

import re, json, time, random, requests, bs4
from urllib.parse import urljoin, urlparse
from typing import Dict, List, Optional

UA   = "Mozilla/5.0 (+https://github.com/ymworkseurope/carwow-sync 2025-09)"
HEAD = {"User-Agent": UA}

# ───────────── HTTP helpers
def _get(url: str) -> requests.Response:
    r = requests.get(url, headers=HEAD, timeout=30)
    r.raise_for_status()
    return r

def _bs(url: str) -> bs4.BeautifulSoup:
    return bs4.BeautifulSoup(_get(url).text, "lxml")

def _sleep(): time.sleep(random.uniform(0.6, 1.2))

# ───────────── JSON-LD 抽出
def _jsonld(doc: bs4.BeautifulSoup) -> dict:
    for s in doc.select('script[type="application/ld+json"]'):
        try:
            data = json.loads(s.string)
            if isinstance(data, dict) and data.get("@type") == "Product":
                return data
        except Exception:
            pass
    return {}

# ───────────── 画像
_GALLERY_DOMAINS = ("images.prismic.io", "car-data.carwow.co.uk")

def _gallery_imgs(doc: bs4.BeautifulSoup, base: str, limit: int = 20) -> List[str]:
    imgs = doc.select("img[srcset], img[src]")
    out: List[str] = []
    for img in imgs:
        src = img.get("srcset") or img.get("src") or ""
        if "," in src:
            src = src.split(",")[-1].split()[0]
        full = urljoin(base, src)
        if any(dom in full for dom in _GALLERY_DOMAINS) and full not in out:
            out.append(full)
            if len(out) >= limit:
                break
    return out

def _safe_int(val: str | None) -> Optional[int]:
    if val and val.isdigit():
        return int(val)
    return None

# ───────────── main
def scrape(url: str) -> Dict:
    doc  = _bs(url)
    data = _jsonld(doc)
    _sleep()

    make_slug, model_slug = urlparse(url).path.strip("/").split("/")[:2]

    # slug は make が重複していれば除去
    slug = (model_slug if model_slug.startswith(make_slug + "-")
            else f"{make_slug}-{model_slug}")

    make_en  = data.get("brand", {}).get("name") or make_slug.title()
    model_en = data.get("name") or model_slug.replace("-", " ").title()

    # ボディタイプ・燃料
    body_type = data.get("bodyType") or []
    if isinstance(body_type, str):
        body_type = [body_type]
    fuel = data.get("fuelType")

    # 価格
    pmin = pmax = None
    if "offers" in data:
        price_spec = data["offers"].get("priceSpecification", {})
        pmin = price_spec.get("minPrice") or price_spec.get("price")
        pmax = price_spec.get("maxPrice")
        if isinstance(pmin, str): pmin = int(pmin)
        if isinstance(pmax, str): pmax = int(pmax)

    # ドア・シート・駆動
    doors = data.get("numberOfDoors")
    seats = data.get("numberOfSeats")
    drive = data.get("vehicleTransmission")

    # 寸法（mm）
    dims = None
    if "height" in data and "width" in data and "length" in data:
        dims = f"{data['length']} / {data['width']} / {data['height']}"

    # 概要文（旧 <em> タグフォールバック）
    overview = data.get("description") or (doc.select_one("em") or bs4.Tag()).get_text(strip=True)

    return {
        "slug": slug,
        "url": url,
        "title": f"{make_en} {model_en}",
        "make_en": make_en,
        "model_en": model_en,
        "overview_en": overview,
        "body_type": body_type,
        "fuel": fuel,
        "price_min_gbp": pmin,
        "price_max_gbp": pmax,
        "spec_json": json.dumps(
            {
                "doors": doors,
                "seats": seats,
                "drive_type": drive,
                "dimensions_mm": dims,
            },
            ensure_ascii=False,
        ),
        "media_urls": _gallery_imgs(doc, url),
        "doors": _safe_int(str(doors) if doors else None),
        "seats": _safe_int(str(seats) if seats else None),
        "dimensions_mm": dims,
        "drive_type": drive,
        "grades": None,
        "engines": None,
        "colors": None,
        "catalog_url": url,
    }
