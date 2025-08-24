#!/usr/bin/env python3
# coding: utf-8
"""
Carwow model scraper – 2025-09 update
"""

from __future__ import annotations
import re, json, requests, bs4, time, random
from urllib.parse import urljoin, urlparse
from typing import Dict, List, Optional

UA   = "Mozilla/5.0 (+https://github.com/ymworkseurope/carwow-sync)"
HEAD = {"User-Agent": UA}
_DOMAINS = ("images.prismic.io", "car-data.carwow.co.uk")

# ─────────── helpers ───────────
def _get(url: str) -> bs4.BeautifulSoup:
    r = requests.get(url, headers=HEAD, timeout=30)
    r.raise_for_status()
    return bs4.BeautifulSoup(r.text, "lxml")

def _sleep(): time.sleep(random.uniform(.4, .8))

def _safe_int(txt: str | None) -> Optional[int]:
    s = re.sub(r"[^\d]", "", txt or "")
    return int(s) if s else None

# ─────────── JSON-LD ───────────
def _jsonld(doc: bs4.BeautifulSoup) -> dict:
    for s in doc.select('script[type="application/ld+json"]'):
        try:
            data = json.loads(s.string)
            if isinstance(data, dict) and data.get("@type") == "Product":
                return data
        except Exception:
            pass
    return {}

# ─────────── key/value maps ───────────
def _dtdd_pairs(doc: bs4.BeautifulSoup) -> dict:
    out = {}
    for dt in doc.select("dt"):
        dd = dt.find_next("dd")
        if dd:
            out[dt.get_text(" ", strip=True).lower()] = dd.get_text(" ", strip=True)
    return out

def _at_a_glance(doc: bs4.BeautifulSoup) -> dict:
    out = {}
    for blk in doc.select(".review-overview__at-a-glance-model"):
        cells = [c.get_text(" ", strip=True) for c in blk.select("div")]
        for k, v in zip(cells[::2], cells[1::2]):
            out[k.lower()] = v
    return out

def _pick(keys: list[str], *sources: dict) -> Optional[str]:
    for key in keys:
        for src in sources:
            if key in src and src[key]:
                return src[key]
    return None

# ─────────── price ───────────
def _prices(doc: bs4.BeautifulSoup, kv: dict) -> tuple[Optional[int], Optional[int]]:
    # 1) RRP
    rrp = doc.select_one(".deals-cta-list__rrp-price")
    if rrp:
        nums = [int(x.replace(",", "")) for x in re.findall(r"£([\d,]+)", rrp.text)]
        if nums:
            return (nums[0], nums[-1] if len(nums) > 1 else None)

    # 2) Used only
    used = kv.get("used")
    if used:
        return (_safe_int(used), None)

    # fallback = None
    return (None, None)

# ─────────── images ───────────
def _hires(url: str) -> str:
    base = url.split("?")[0]
    return f"{base}?auto=format&fit=max&q=80"

def _gallery(doc: bs4.BeautifulSoup, base: str, limit: int = 20) -> List[str]:
    urls: List[str] = []
    # main slider / generic imgs
    for tag in doc.select("img[data-srcset], source[data-srcset], img[srcset], img[src]"):
        src = (
            tag.get("data-srcset") or tag.get("srcset") or
            tag.get("data-src") or tag.get("src") or ""
        )
        if "," in src:
            src = src.split(",")[-1].split()[0]
        full = urljoin(base, src)
        if any(dom in full for dom in _DOMAINS):
            h = _hires(full)
            if h not in urls:
                urls.append(h)
        if len(urls) >= limit:
            return urls
    # thumbnail carousel
    for img in doc.select(".thumbnail-carousel-vertical__img"):
        src = img.get("data-src") or img.get("src") or ""
        full = urljoin(base, src)
        if any(dom in full for dom in _DOMAINS):
            h = _hires(full)
            if h not in urls:
                urls.append(h)
        if len(urls) >= limit:
            break
    return urls[:limit]

# ─────────── make / model from H1 ───────────
def _split_make_model(title: str, make_slug: str) -> tuple[str, str]:
    title = re.sub(r"(?i)review.*$", "", title).strip()
    title = re.sub(r"(?i)& prices.*$", "", title).strip()
    tk = make_slug.split("-")
    parts = title.split()
    make = " ".join(parts[:len(tk)])
    model = " ".join(parts[len(tk):]).strip() or make
    return make, model

# ─────────── /colours page ───────────
def _colors(base_url: str) -> List[str]:
    try:
        doc = _get(f"{base_url}/colours")
    except requests.HTTPError as e:
        if e.response.status_code == 404:
            return []
        raise
    out = []
    for h4 in doc.select("h4.model-hub__colour-details-title"):
        txt = h4.get_text(" ", strip=True)
        name = txt.split(" - ")[0].strip()
        if name and name not in out:
            out.append(name)
    return out

# ─────────── /specifications page ───────────
_DIMS_RE = re.compile(r"(\d{3,4})\s?mm", re.I)

def _spec_page(base_url: str) -> dict:
    try:
        doc = _get(f"{base_url}/specifications")
    except requests.HTTPError as e:
        if e.response.status_code == 404:
            return {}
        raise
    kv = _dtdd_pairs(doc)
    # external dimensions: 最初の 3 つ mm を length/width/height と仮定
    dims = _DIMS_RE.findall(doc.text)
    if len(dims) >= 3:
        kv["length_mm"], kv["width_mm"], kv["height_mm"] = dims[:3]
    if m := re.search(r"wheelbase\s+([\d\.]+)\s?m", doc.text, re.I):
        kv["wheelbase_m"] = m.group(1)
    if m := re.search(r"turning circle\s+([\d\.]+)\s?m", doc.text, re.I):
        kv["turning_circle_m"] = m.group(1)
    if m := re.search(r"boot\s+\(seats up\)\s+(\d+)\s?l", doc.text, re.I):
        kv["boot_up_l"] = m.group(1)
    if m := re.search(r"boot\s+\(seats down\)\s+(\d+)\s?l", doc.text, re.I):
        kv["boot_down_l"] = m.group(1)
    return kv

# ─────────── main ───────────
def scrape(url: str) -> Dict:
    doc  = _get(url)
    ld   = _jsonld(doc)
    _sleep()

    make_slug, model_slug = urlparse(url).path.strip("/").split("/")[:2]
    slug = f"{make_slug}-{model_slug}"

    title = (doc.select_one("h1") or bs4.Tag()).get_text(" ", strip=True)
    make_en, model_en = _split_make_model(title, make_slug)

    kv_glance  = _at_a_glance(doc)
    kv_summary = _dtdd_pairs(doc)
    kv_all     = {**kv_glance, **kv_summary}

    price_min, price_max = _prices(doc, kv_all)

    body_type = ld.get("bodyType") or _pick(["body type", "body"], kv_all)
    body_list = [b.strip() for b in (body_type if isinstance(body_type, list)
                                     else [body_type] if body_type else []) if b]

    fuel  = ld.get("fuelType") or _pick(["fuel type", "available fuel"], kv_all)
    doors = ld.get("numberofdoors") or _pick(["number of doors", "doors"], kv_all)
    seats = ld.get("numberofseats") or _pick(["number of seats", "seats"], kv_all)
    drive = ld.get("vehicleTransmission") or _pick(["transmission", "drive type"], kv_all)

    dims = None
    if all(k in ld for k in ("length", "width", "height")):
        dims = f"{ld['length']} / {ld['width']} / {ld['height']}"
    else:
        mm = re.findall(r"(\d{3,4}\s?mm)", doc.text)
        if len(mm) >= 3:
            dims = " / ".join(mm[:3])

    overview = ld.get("description") or (doc.select_one("em") or bs4.Tag()).get_text(" ", strip=True)

    base_url = f"https://{urlparse(url).netloc}/{make_slug}/{model_slug}"
    colours  = _colors(base_url)
    spec_ext = _spec_page(base_url)

    spec_json = {
        "doors": doors,
        "seats": seats,
        "drive_type": drive,
        "dimensions_mm": dims,
        **spec_ext,
    }

    return {
        # ─── primary
        "slug": slug,
        "catalog_url": url,
        "make_en": make_en,
        "model_en": model_en,
        "overview_en": overview,
        "body_type": body_list,
        "fuel": fuel,
        "price_min_gbp": price_min,
        "price_max_gbp": price_max,
        # ─── detailed specs
        "spec_json": json.dumps(spec_json, ensure_ascii=False),
        "media_urls": _gallery(doc, url, 20),
        "doors": _safe_int(str(doors) if doors else None),
        "seats": _safe_int(str(seats) if seats else None),
        "dimensions_mm": dims,
        "drive_type": drive,
        # ─── extra
        "colors": colours,
        "grades": None,
        "engines": None,
        # transform.py で *_ja 系 / JPY 換算 / updated_at などを付与
    }
