#!/usr/bin/env python3
# model_scraper.py – 2025-08-25 indent-fixed
import os, re, json, time, random, requests, bs4
from urllib.parse import urljoin, urlparse
from typing import Dict, List

UA   = "Mozilla/5.0 (+https://github.com/ymworkseurope/carwow-sync 2025-08-25)"
HEAD = {"User-Agent": UA}

# ---------- helpers ----------
def _get(url: str, **kw) -> requests.Response:
    r = requests.get(url, headers=HEAD, timeout=30, **kw)
    r.raise_for_status()
    return r


def _bs(url: str):
    return bs4.BeautifulSoup(_get(url).text, "lxml")


def _sleep():
    time.sleep(random.uniform(0.6, 1.1))


# 画像抽出
EXCLUDE_IMG = ("logo", "icon", "badge", "sprite", "favicon")


def _clean_imgs(soup: bs4.BeautifulSoup, base: str, limit: int = 12) -> List[str]:
    out: List[str] = []
    for img in soup.select("img[src]"):
        src = img["src"]
        if any(k in src.lower() for k in EXCLUDE_IMG):
            continue
        full = urljoin(base, src)
        if full.startswith("http") and full not in out:
            out.append(full)
        if len(out) >= limit:
            break
    return out


# body_type マップ読み込み
def _load_body_map(make_slug: str) -> dict[str, list[str]]:
    path = f"body_map_{make_slug}.json"
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    return {}


# body_type を文字列→配列化
def _split_body_types(raw: str | None) -> list[str] | None:
    if not raw:
        return None
    parts = re.split(r"[,&/]+", raw)
    return [p.strip() for p in parts if p.strip()]


# ---------- main ----------
def scrape(url: str) -> Dict:
    """スクレイピングして dict を返す"""
    soup = _bs(url)
    _sleep()

    path_parts = urlparse(url).path.strip("/").split("/")
    if len(path_parts) < 2:
        raise ValueError("invalid url")
    make_raw, model_raw = path_parts[:2]

    make_en = make_raw.replace("-", " ").title()
    model_en = model_raw.replace("-", " ").title()
    slug = f"{make_raw}-{model_raw}"

    title_el = soup.select_one("h1")
    title = title_el.get_text(" ", strip=True) if title_el else f"{make_en} {model_en}"

    txt = soup.get_text(" ", strip=True)
    m = re.search(r"£([\d,]+)\s*[–-]\s*£([\d,]+)", txt)
    pmin, pmax = (None, None)
    if m:
        pmin = int(m[1].replace(",", ""))
        pmax = int(m[2].replace(",", ""))

    # overview
    ov = soup.select_one("em")
    overview = ov.get_text(strip=True) if ov else ""

    # body_type
    body_map = _load_body_map(make_raw)
    body_types = body_map.get(model_raw)

    # fuel / body_type fallback
    fuel = None
    glance = soup.select_one(".review-overview__at-a-glance-model")
    if glance:
        blocks = [b.get_text(strip=True) for b in glance.select("div")]
        for k, v in zip(blocks[::2], blocks[1::2]):
            if k.startswith("Available fuel"):
                fuel = "要確認" if v.lower() == "chooser" else v
            elif not body_types and k.startswith("Body type"):
                body_types = _split_body_types(v)

    # summary items
    def _summary(label: str) -> str | None:
        sel = soup.select_one(f".summary-list__item:has(dt:-soup-contains('{label}')) dd")
        return sel.get_text(strip=True) if sel else None

    doors = _summary("Number of doors")
    seats = _summary("Number of seats")
    drive_type = _summary("Transmission")

    dim_m = re.search(
        r"(\d{1,4},\d{3}\s*mm\s*/\s*\d{1,4},\d{3}\s*mm\s*/\s*\d{1,4},\d{3}\s*mm)",
        soup.text,
    )
    dimensions = dim_m.group(1).replace(" ", "") if dim_m else None

    # grades
    grades = [s.get_text(strip=True) for s in soup.select("span.trim-article__title-part-2")] or None

    # engines
    engines: List[str] = []
    spec_url = url.rstrip("/") + "/specifications"
    try:
        spec_bs = _bs(spec_url)
        _sleep()
        for tr in spec_bs.select("table tr"):
            tds = [td.get_text(" ", strip=True) for td in tr.select("td")]
            if len(tds) == 2 and re.search(r"(PS|hp|kW)", tds[1]):
                engines.append(" ".join(tds))
    except Exception:
        pass
    engines = engines or None

    spec_json = json.dumps(
        {
            "doors": doors,
            "seats": seats,
            "dimensions_mm": dimensions,
            "drive_type": drive_type,
        },
        ensure_ascii=False,
    )

    media_urls = _clean_imgs(soup, url)

    return {
        "slug": slug,
        "url": url,
        "title": title,
        "make_en": make_en,
        "model_en": model_en,
        "overview_en": overview,
        "body_type": body_types,
        "fuel": fuel,
        "price_min_gbp": pmin,
        "price_max_gbp": pmax,
        "spec_json": spec_json,
        "media_urls": media_urls,
        "doors": int(doors) if doors and doors.isdigit() else None,
        "seats": int(seats) if seats and seats.isdigit() else None,
        "dimensions_mm": dimensions,
        "drive_type": drive_type,
        "grades": grades,
        "engines": engines,
        "catalog_url": url,
    }
