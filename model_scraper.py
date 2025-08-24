#!/usr/bin/env python3
# model_scraper.py ― 2025-09-02 full (Review 除外・正式名称抽出・gallery only)

import re, json, time, random, requests, bs4
from urllib.parse import urljoin, urlparse
from typing import Dict, List, Optional

UA   = "Mozilla/5.0 (+https://github.com/ymworkseurope/carwow-sync 2025-09-02)"
HEAD = {"User-Agent": UA}

# ───────────────────────── helpers
def _get(url: str) -> requests.Response:
    r = requests.get(url, headers=HEAD, timeout=30)
    r.raise_for_status()
    return r

def _bs(url: str) -> bs4.BeautifulSoup:
    return bs4.BeautifulSoup(_get(url).text, "lxml")

def _sleep(): time.sleep(random.uniform(0.6, 1.1))

_GALLERY_DOMAINS = ("images.prismic.io", "car-data.carwow.co.uk")

def _gallery_imgs(doc: bs4.BeautifulSoup, base: str, limit: int = 20) -> List[str]:
    imgs = (
        doc.select("img.media-slider__image[srcset]") +
        doc.select("img.media-slider__image[src]") +
        doc.select("img.thumbnail-carousel-vertical__img[data-src]")
    )
    out: List[str] = []
    for img in imgs:
        src = img.get("srcset") or img.get("src") or img.get("data-src") or ""
        if "," in src:                                   # srcset highest-res
            src = src.split(",")[-1].split()[0]
        else:
            src = src.split()[0]
        if not src: continue
        full = urljoin(base, src)
        if (full.startswith("http")
            and any(dom in full for dom in _GALLERY_DOMAINS)
            and full not in out):
            out.append(full)
            if len(out) >= limit: break
    return out

def _safe_int(val: str) -> Optional[int]:
    d = re.sub(r"[^\d]", "", str(val or ""))
    return int(d) if d.isdigit() else None

def _parse_make_model(title: str, fallback_make: str) -> tuple[str,str]:
    t = re.sub(r"review.*$", "", title, flags=re.I).strip()
    if t.lower().startswith(fallback_make.lower()):
        model = t[len(fallback_make):].strip()
        return fallback_make, model or fallback_make
    return fallback_make, t

# ───────────────────────── scrape main
def scrape(url: str) -> Dict:
    doc = _bs(url); _sleep()

    make_slug, model_slug = urlparse(url).path.strip("/").split("/")[:2]
    fallback_make = make_slug.replace("-", " ").title()

    h1 = doc.select_one("h1")
    title_txt = h1.get_text(" ", strip=True) if h1 else f"{fallback_make} {model_slug}"
    make_en, model_en = _parse_make_model(title_txt, fallback_make)

    slug = f"{make_slug}-{model_slug}"

    price_m = re.search(r"£([\d,]+)\D+£([\d,]+)", doc.text)
    pmin, pmax = (int(price_m[1].replace(",", "")), int(price_m[2].replace(",", ""))) if price_m else (None, None)

    overview = doc.select_one("em")
    overview = overview.get_text(strip=True) if overview else ""

    body_type = fuel = None
    glance = doc.select_one(".review-overview__at-a-glance-model")
    if glance:
        cells = [c.get_text(strip=True) for c in glance.select("div")]
        for k, v in zip(cells[::2], cells[1::2]):
            if "Body type" in k: body_type = [s.strip() for s in re.split(r",|/|&", v) if s.strip()]
            elif "Available fuel" in k and v.lower() != "chooser": fuel = v

    def _summary(label):                                       # summary list
        n = doc.select_one(f".summary-list__item:has(dt:-soup-contains('{label}')) dd")
        return n.get_text(strip=True) if n else None

    doors = _summary("Number of doors")
    seats = _summary("Number of seats")
    trans = _summary("Transmission")

    dim_m = re.search(r"(\d{1,3}[,\d]*\s*mm)[^m]{0,30}(\d{1,3}[,\d]*\s*mm)[^m]{0,30}(\d{1,3}[,\d]*\s*mm)", doc.text)
    dims = " / ".join(dim_m.groups()) if dim_m else None

    grades, engines = [], []
    try:
        spec = _bs(url.rstrip("/") + "/specifications"); _sleep()
        grades = [s.get_text(strip=True) for s in spec.select("span.trim-article__title-part-2")]
        for tr in spec.select("table tr"):
            tds = [td.get_text(" ", strip=True) for td in tr.select("td")]
            if len(tds) == 2 and re.search(r"(PS|hp|kW|kWh)", tds[1]): engines.append(" ".join(tds))
    except Exception: pass

    colors = []
    try:
        col = _bs(url.rstrip("/") + "/colours"); _sleep()
        for h4 in col.select(".model-hub__colour-details-title"):
            t = " ".join(h4.get_text(" ", strip=True).split())
            if t: colors.append(t)
    except Exception: pass

    return {
        "slug": slug,
        "url": url,
        "title": title_txt,
        "make_en": make_en,
        "model_en": model_en,
        "overview_en": overview,
        "body_type": body_type,
        "fuel": fuel,
        "price_min_gbp": pmin,
        "price_max_gbp": pmax,
        "spec_json": json.dumps(
            {"doors": doors, "seats": seats, "drive_type": trans, "dimensions_mm": dims},
            ensure_ascii=False,
        ),
        "media_urls": _gallery_imgs(doc, url),
        "doors": _safe_int(doors),
        "seats": _safe_int(seats),
        "dimensions_mm": dims,
        "drive_type": trans,
        "grades": grades or None,
        "engines": engines or None,
        "colors": colors or None,
        "catalog_url": url,
    }
