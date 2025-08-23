#!/usr/bin/env python3
# model_scraper.py – 2025-08-27 full (spec 強化 + colours 対応)

import re, json, time, random, requests, bs4
from urllib.parse import urljoin, urlparse
from typing import Dict, List, Optional

UA   = "Mozilla/5.0 (+https://github.com/ymworkseurope/carwow-sync 2025-08-27)"
HEAD = {"User-Agent": UA}

# ───────── helpers ─────────
def _get(url: str) -> requests.Response:
    r = requests.get(url, headers=HEAD, timeout=30)
    r.raise_for_status()
    return r


def _bs(url: str):
    return bs4.BeautifulSoup(_get(url).text, "lxml")


def _maybe_bs(url: str) -> Optional[bs4.BeautifulSoup]:
    try:
        return _bs(url)
    except requests.HTTPError as e:
        if e.response.status_code == 404:
            return None
        raise


def _sleep():
    time.sleep(random.uniform(0.6, 1.1))


EXCLUDE_IMG = ("logo", "icon", "badge", "sprite", "favicon")


def _clean_imgs(doc: bs4.BeautifulSoup, base: str, limit: int = 12) -> List[str]:
    out: List[str] = []
    for img in doc.select("img[src]"):
        src = img["src"]
        if any(k in src.lower() for k in EXCLUDE_IMG):
            continue
        full = urljoin(base, src)
        if full.startswith("http") and full not in out:
            out.append(full)
        if len(out) >= limit:
            break
    return out


# ───────── main ─────────
def scrape(url: str) -> Dict:
    """モデルページ + /specifications + /colours を統合して dict を返す"""
    doc = _bs(url)
    _sleep()

    make_raw, model_raw = urlparse(url).path.strip("/").split("/")[:2]
    make_en  = make_raw.replace("-", " ").title()
    model_en = model_raw.replace("-", " ").title()
    slug     = f"{make_raw}-{model_raw}"

    # ---- タイトル & 価格 -------------------------------------------------
    h1 = doc.select_one("h1")
    title = h1.get_text(" ", strip=True) if h1 else f"{make_en} {model_en}"

    price_m = re.search(r"£([\d,]+)\s*[–-]\s*£([\d,]+)", doc.text)
    if price_m:
        pmin, pmax = (int(price_m[1].replace(",", "")),
                      int(price_m[2].replace(",", "")))
    else:  # “From £19,995” 形式
        one = re.search(r"From\s+£([\d,]+)", doc.text)
        pmin = pmax = int(one[1].replace(",", "")) if one else None

    # ---- 概要 -----------------------------------------------------------
    overview = doc.select_one("em")
    overview = overview.get_text(strip=True) if overview else ""

    # ---- Body type / Fuel ----------------------------------------------
    body_type = fuel = None
    glance = doc.select_one(".review-overview__at-a-glance-model")
    if glance:
        kv = [b.get_text(strip=True) for b in glance.select("div")]
        for k, v in zip(kv[::2], kv[1::2]):
            if k.startswith("Body type"):
                body_type = [t.strip() for t in re.split(r",|/&", v) if t.strip()]
            if k.startswith("Available fuel"):
                fuel = "要確認" if v.lower() == "chooser" else v

    # ---- サマリ値 --------------------------------------------------------
    def _summary(label: str) -> Optional[str]:
        n = doc.select_one(
            f".summary-list__item:has(dt:-soup-contains('{label}')) dd"
        )
        return n.get_text(strip=True) if n else None

    doors = _summary("Number of doors")
    seats = _summary("Number of seats")
    trans = _summary("Transmission")

    dim_m = re.search(r"([\d,]+\s*mm)\s*/\s*([\d,]+\s*mm)\s*/\s*([\d,]+\s*mm)",
                      doc.text)
    dims = " / ".join(dim_m.groups()) if dim_m else None

    # ---- 下層 /specifications ------------------------------------------
    grades, engines = [], []
    spec = _maybe_bs(url.rstrip("/") + "/specifications")
    if spec:
        _sleep()
        grades = [s.get_text(strip=True)
                  for s in spec.select("span.trim-article__title-part-2")]

        for row in spec.select("table tr, .summary-list__item"):
            cells = [c.get_text(" ", strip=True)
                     for c in row.select("td, dd")]
            if len(cells) < 2:
                continue

            label = cells[0].lower()
            value = cells[1]

            # エンジン
            if re.search(r"(ps|hp|kw)", value.lower()):
                engines.append(" ".join(cells))

            # Doors / Seats
            if re.search(r"\bdoor\b", label):
                m = re.search(r"(\d+)", value)
                if m:
                    doors = m.group(1)
            if re.search(r"\bseat\b", label):
                m = re.search(r"(\d+)", value)
                if m:
                    seats = m.group(1)

            # Dimensions
            if ("dimension" in label or "length" in label) and "mm" in value:
                dims = value

            # Drive type / Transmission
            if re.search(r"\bdrive\b|\btransmission\b", label):
                trans = value

    # ---- 下層 /colours --------------------------------------------------
    colours = []
    col = _maybe_bs(url.rstrip("/") + "/colours")
    if col:
        _sleep()
        colours = sorted({
            c.get_text(strip=True)
            for c in col.select("figcaption, .colour-picker__name")
        })

    # ---- Assemble dict --------------------------------------------------
    return {
        "slug": slug,
        "url": url,
        "title": title,
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
                "dimensions_mm": dims,
                "drive_type": trans,
            },
            ensure_ascii=False,
        ),
        "media_urls": _clean_imgs(doc, url),
        "doors": int(doors) if doors and doors.isdigit() else None,
        "seats": int(seats) if seats and seats.isdigit() else None,
        "dimensions_mm": dims,
        "drive_type": trans,
        "grades": grades or None,
        "engines": engines or None,
        "colors": colours or None,
        "catalog_url": url,
    }
