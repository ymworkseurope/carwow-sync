#!/usr/bin/env python3
# model_scraper.py ― 2025-09-01 gallery-only / 正式版
import re, json, time, random, requests, bs4
from urllib.parse import urljoin, urlparse
from typing import Dict, List, Optional

UA   = "Mozilla/5.0 (+https://github.com/ymworkseurope/carwow-sync 2025-09-01)"
HEAD = {"User-Agent": UA}

def _get(url: str) -> requests.Response:
    r = requests.get(url, headers=HEAD, timeout=30)
    r.raise_for_status()
    return r

def _bs(url: str) -> bs4.BeautifulSoup:
    """URL → BeautifulSoup"""
    return bs4.BeautifulSoup(_get(url).text, "lxml")

def _sleep():
    time.sleep(random.uniform(0.6, 1.1))

def is_review_page(url: str) -> bool:
    """URLがレビューページかどうか判定"""
    try:
        doc = _bs(url)
        _sleep()
        
        # グローバルメニューの「Reviews」が選択状態（is-activeクラス）かチェック
        reviews_menu = doc.select_one('a[data-main-menu-section="car-reviews"]')
        if reviews_menu and 'is-active' in reviews_menu.get('class', []):
            return True
            
        return False
    except Exception:
        return False

# ギャラリードメイン許可リスト
_GALLERY_DOMAINS = (
    "images.prismic.io",        # 新 CMS
    "car-data.carwow.co.uk",    # 旧静止画 API
)

def _gallery_imgs(doc: bs4.BeautifulSoup, base: str, limit: int = 20) -> List[str]:
    """
    1️⃣ ページ内の『ギャラリー / スライダー』画像だけを抽出  
       ・class='media-slider__image'（srcset or src）  
       ・縦サムネ : .thumbnail-carousel-vertical__img[data-src]  
    2️⃣ 許可ドメインかどうかでホワイトリスト  
    3️⃣ limit 枚で打ち切り
    """
    out: List[str] = []

    candidates = (
        doc.select("img.media-slider__image[srcset]") +
        doc.select("img.media-slider__image[src]") +
        doc.select("img.thumbnail-carousel-vertical__img[data-src]")
    )

    for img in candidates:
        src = img.get("srcset") or img.get("src") or img.get("data-src") or ""
        # `srcset` は「URL 800w, URL 1600w …」の形式なので最高解像度を取る
        if src and "," in src:
            # 最高解像度の画像を選択（最後のエントリ）
            src_parts = [part.strip().split()[0] for part in src.split(",")]
            src = src_parts[-1] if src_parts else ""
        else:
            src = src.split()[0] if src else ""
        
        if not src:
            continue
            
        full = urljoin(base, src)

        # ホワイトリスト & 重複チェック & 有効URL確認
        if (full.startswith(("http://", "https://")) and 
            any(dom in full for dom in _GALLERY_DOMAINS) and 
            full not in out):
            out.append(full)
            if len(out) >= limit:
                break
    return out

def _safe_int(value: str) -> Optional[int]:
    """文字列を安全にintに変換"""
    if not value:
        return None
    # 数字のみ抽出
    digits = re.sub(r'[^\d]', '', str(value))
    return int(digits) if digits.isdigit() else None

def scrape(url: str) -> Dict:
    """車両モデルページ（例: …/bmw/5-series）→ dict"""
    doc = _bs(url)
    _sleep()

    # ── 基本 name / slug ──────────────────────────────
    path_parts = urlparse(url).path.strip("/").split("/")
    if len(path_parts) < 2:
        raise ValueError(f"Invalid URL format: {url}")
    
    make_raw, model_raw = path_parts[:2]
    make_en  = make_raw.replace("-", " ").title()
    model_en = model_raw.replace("-", " ").title()
    slug     = f"{make_raw}-{model_raw}"

    # ── タイトル & 価格帯 ─────────────────────────────
    title_node = doc.select_one("h1")
    title   = title_node.get_text(" ", strip=True) if title_node else f"{make_en} {model_en}"

    price_m = re.search(r"£([\d,]+)\D+£([\d,]+)", doc.text)
    pmin, pmax = (
        (int(price_m[1].replace(",", "")), int(price_m[2].replace(",", "")))
        if price_m else (None, None)
    )

    # ── 概要 ───────────────────────────────────────────
    overview_node = doc.select_one("em")
    overview = overview_node.get_text(strip=True) if overview_node else ""

    # ── Body type / Fuel ──────────────────────────────
    body_type, fuel = None, None
    glance = doc.select_one(".review-overview__at-a-glance-model")
    if glance:
        cells = [c.get_text(strip=True) for c in glance.select("div")]
        for k, v in zip(cells[::2], cells[1::2]):
            if "Body type" in k:
                body_type = [t.strip() for t in re.split(r",|/|&", v) if t.strip()]
            elif "Available fuel" in k:
                fuel = v if v.lower() != "chooser" else None

    # ── Summary list ──────────────────────────────────
    def _summary(label: str) -> Optional[str]:
        node = doc.select_one(f".summary-list__item:has(dt:-soup-contains('{label}')) dd")
        return node.get_text(strip=True) if node else None

    doors = _summary("Number of doors")
    seats = _summary("Number of seats")
    trans = _summary("Transmission")

    # Dimensions: SVG に embed されている mm 値 3 つを取得
    dim_m = re.search(r"(\d{1,3}[,\d]*\s*mm)[^m]{0,30}(\d{1,3}[,\d]*\s*mm)[^m]{0,30}(\d{1,3}[,\d]*\s*mm)", doc.text)
    dims  = " / ".join(dim_m.groups()) if dim_m else None

    # ── Grades / Engines ─────────────────────────────
    grades, engines = [], []
    try:
        spec_url = url.rstrip("/") + "/specifications"
        spec = _bs(spec_url)
        _sleep()
        grades = [s.get_text(strip=True) for s in spec.select("span.trim-article__title-part-2")] or []
        for tr in spec.select("table tr"):
            tds = [td.get_text(" ", strip=True) for td in tr.select("td")]
            if len(tds) == 2 and re.search(r"(PS|hp|kW|kWh)", tds[1]):
                engines.append(" ".join(tds))
    except Exception:
        pass

    # ── Colours ──────────────────────────────────────
    colors = []
    try:
        col_url = url.rstrip("/") + "/colours"
        col = _bs(col_url)
        _sleep()
        for h4 in col.select(".model-hub__colour-details-title"):
            color_text = h4.get_text(" ", strip=True)
            if color_text:
                colors.append(" ".join(color_text.split()))
    except Exception:
        pass

    # ── 返却 dict ────────────────────────────────────
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
