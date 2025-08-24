#!/usr/bin/env python3
# model_scraper.py ― 2025-09-02 full (Review page detection + price fix + robust spec extraction)

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

# レビューページ判定は scrape.py に移動

# ───────────────────────── 価格抽出の修正
def _extract_prices(doc: bs4.BeautifulSoup) -> tuple[Optional[int], Optional[int]]:
    """
    価格抽出ロジック:
    1. USEDのみの場合: Used価格をprice_minに入れる
    2. 新車価格の場合: RRP範囲から取得
    """
    # パターン1: USED価格
    used_item = doc.select_one('.summary-list__item dt:-soup-contains("Used") + dd')
    if used_item:
        price_text = used_item.get_text(strip=True)
        price_match = re.search(r'£([\d,]+)', price_text)
        if price_match:
            used_price = int(price_match.group(1).replace(',', ''))
            return (used_price, None)  # USEDはmin_priceのみ
    
    # パターン2: RRP価格範囲
    rrp_elem = doc.select_one('.deals-cta-list__rrp-price')
    if rrp_elem:
        rrp_text = rrp_elem.get_text(' ', strip=True)
        # £110,960 - £166,425 形式
        price_match = re.search(r'£([\d,]+)\s*-\s*£([\d,]+)', rrp_text)
        if price_match:
            pmin = int(price_match.group(1).replace(',', ''))
            pmax = int(price_match.group(2).replace(',', ''))
            return (pmin, pmax)
        # 単一価格の場合
        single_match = re.search(r'£([\d,]+)', rrp_text)
        if single_match:
            price = int(single_match.group(1).replace(',', ''))
            return (price, price)
    
    # 旧パターン（フォールバック）
    price_m = re.search(r"£([\d,]+)\D+£([\d,]+)", doc.text)
    if price_m:
        return (int(price_m[1].replace(",", "")), int(price_m[2].replace(",", "")))
    
    return (None, None)

# ───────────────────────── スペック抽出の強化
def _extract_spec(doc: bs4.BeautifulSoup, label: str) -> Optional[str]:
    """
    スペック情報の抽出（doors, seats, transmission）
    複数のUIパターンに対応
    """
    # パターン1: 旧UI - summary-list形式
    old_ui = doc.select_one(f'.summary-list__item dt:-soup-contains("{label}") + dd')
    if old_ui:
        return old_ui.get_text(strip=True)
    
    # パターン2: 新UI - car-specs形式
    for item in doc.select('.car-specs__item'):
        text = item.get_text(' ', strip=True).lower()
        if label.lower() in text:
            # 数値を抽出（doors, seats用）
            if 'doors' in label.lower() or 'seats' in label.lower():
                nums = re.findall(r'\d+', text)
                if nums:
                    return nums[0]
            # Transmissionは文字列全体
            elif 'transmission' in label.lower():
                if 'automatic' in text:
                    return 'Automatic'
                elif 'manual' in text:
                    return 'Manual'
                elif 'cvt' in text:
                    return 'CVT'
    
    # パターン3: specs-list形式
    for spec in doc.select('.specs-list__item'):
        text = spec.get_text(' ', strip=True)
        if label.lower() in text.lower():
            # ddタグから値を取得
            dd = spec.select_one('dd')
            if dd:
                return dd.get_text(strip=True)
    
    # パターン4: 正規表現フォールバック
    patterns = {
        'doors': r'(\d+)\s+doors?',
        'seats': r'(\d+)\s+seats?',
        'transmission': r'(Automatic|Manual|CVT|Semi-automatic)',
    }
    
    key = label.lower().split()[-1] if ' ' in label else label.lower()
    if key in patterns:
        match = re.search(patterns[key], doc.text, re.IGNORECASE)
        if match:
            return match.group(1)
    
    return None

# ───────────────────────── Overview取得（将来のGemini API用コメント付き）
def _get_overview(doc: bs4.BeautifulSoup) -> str:
    """
    概要文の取得
    現在: <em>タグから取得
    将来: Gemini APIで全体的な要約生成
    """
    # 現在の実装
    overview = doc.select_one("em")
    current_overview = overview.get_text(strip=True) if overview else ""
    
    # --- 将来のGemini API実装例（コメントアウト） ---
    # import google.generativeai as genai
    # 
    # def get_gemini_overview(page_content: str, make: str, model: str) -> str:
    #     """
    #     Gemini APIを使用してページ全体から車両の概要を生成
    #     """
    #     genai.configure(api_key=os.getenv('GEMINI_API_KEY'))
    #     model = genai.GenerativeModel('gemini-pro')
    #     
    #     prompt = f"""
    #     以下の{make} {model}のWebページ内容から、
    #     車両の主な特徴、性能、価格帯を含む200文字程度の概要を英語で作成してください:
    #     
    #     {page_content[:3000]}  # 最初の3000文字のみ送信
    #     """
    #     
    #     response = model.generate_content(prompt)
    #     return response.text
    # 
    # # Gemini APIが設定されている場合は使用
    # if os.getenv('GEMINI_API_KEY'):
    #     try:
    #         return get_gemini_overview(doc.text, make_en, model_en)
    #     except Exception as e:
    #         print(f"Gemini API error: {e}, falling back to default")
    #         return current_overview
    
    return current_overview

# ───────────────────────── scrape main
def scrape(url: str) -> Dict:
    doc = _bs(url)
    _sleep()

    make_slug, model_slug = urlparse(url).path.strip("/").split("/")[:2]
    fallback_make = make_slug.replace("-", " ").title()

    h1 = doc.select_one("h1")
    title_txt = h1.get_text(" ", strip=True) if h1 else f"{fallback_make} {model_slug}"
    make_en, model_en = _parse_make_model(title_txt, fallback_make)

    slug = f"{make_slug}-{model_slug}"

    # 価格取得（修正版）
    pmin, pmax = _extract_prices(doc)

    # 概要取得（Gemini API対応準備）
    overview = _get_overview(doc)

    # ボディタイプと燃料
    body_type = fuel = None
    glance = doc.select_one(".review-overview__at-a-glance-model")
    if glance:
        cells = [c.get_text(strip=True) for c in glance.select("div")]
        for k, v in zip(cells[::2], cells[1::2]):
            if "Body type" in k: 
                body_type = [s.strip() for s in re.split(r",|/|&", v) if s.strip()]
            elif "Available fuel" in k and v.lower() != "chooser": 
                fuel = v

    # スペック情報（強化版）
    doors = _extract_spec(doc, "Number of doors") or _extract_spec(doc, "doors")
    seats = _extract_spec(doc, "Number of seats") or _extract_spec(doc, "seats")
    trans = _extract_spec(doc, "Transmission") or _extract_spec(doc, "transmission")

    # 寸法
    dim_m = re.search(r"(\d{1,3}[,\d]*\s*mm)[^m]{0,30}(\d{1,3}[,\d]*\s*mm)[^m]{0,30}(\d{1,3}[,\d]*\s*mm)", doc.text)
    dims = " / ".join(dim_m.groups()) if dim_m else None

    # グレードとエンジン
    grades, engines = [], []
    try:
        spec = _bs(url.rstrip("/") + "/specifications")
        _sleep()
        grades = [s.get_text(strip=True) for s in spec.select("span.trim-article__title-part-2")]
        for tr in spec.select("table tr"):
            tds = [td.get_text(" ", strip=True) for td in tr.select("td")]
            if len(tds) == 2 and re.search(r"(PS|hp|kW|kWh)", tds[1]): 
                engines.append(" ".join(tds))
    except Exception:
        pass

    # カラー
    colors = []
    try:
        col = _bs(url.rstrip("/") + "/colours")
        _sleep()
        for h4 in col.select(".model-hub__colour-details-title"):
            t = " ".join(h4.get_text(" ", strip=True).split())
            if t:
                colors.append(t)
    except Exception:
        pass

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
