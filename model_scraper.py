# model_scraper.py
# rev: 2025-08-24 修正版（slug生成改良）
import re, json, time, random, requests, bs4
from urllib.parse import urljoin, urlparse
from slugify import slugify
from typing import Dict, List

UA = ("Mozilla/5.0 (+https://github.com/ymworkseurope/"
      "carwow-sync 2025-08-24)")
HEAD = {"User-Agent": UA}

def _get(url: str, **kw) -> requests.Response:
    r = requests.get(url, headers=HEAD, timeout=30, **kw)
    r.raise_for_status()
    return r

def _bs(url: str):
    return bs4.BeautifulSoup(_get(url).text, "lxml")

def _sleep():
    time.sleep(random.uniform(0.6, 1.1))

def _clean_imgs(s, base: str, limit=12) -> List[str]:
    out = []
    for img in s.select("img[src]"):
        src = img["src"]
        if any(k in src.lower() for k in ("logo","icon","badge","sprite","favicon")):
            continue
        full = urljoin(base, src)
        if full.startswith("http") and full not in out:
            out.append(full)
        if len(out) >= limit:
            break
    return out

def scrape(url: str) -> Dict:
    """https://www.carwow.co.uk/abarth/500e → 仕様・画像 etc."""
    top = _bs(url)
    _sleep()
    
    # URLからメーカー名とモデル名を抽出
    path_parts = urlparse(url).path.strip('/').split('/')
    if len(path_parts) < 2:
        raise ValueError(f"Invalid URL format: {url}")
    
    make_raw = path_parts[0]
    model_raw = path_parts[1]
    
    # メーカー名とモデル名の正規化
    make_en = make_raw.replace('-', ' ').title().strip()
    model_en = model_raw.replace('-', ' ').title().strip()
    
    # slug生成（メーカー名-モデル名の形式）
    slug = f"{make_raw}-{model_raw}"
    
    # タイトル
    title_elem = top.select_one("h1")
    title = title_elem.get_text(" ", strip=True) if title_elem else f"{make_en} {model_en}"
    
    # 価格
    price_txt = top.get_text(" ", strip=True)
    price_m = re.search(r"£([\d,]+)\s*[–-]\s*£([\d,]+)", price_txt)
    pmin, pmax = (int(price_m[1].replace(",","")), int(price_m[2].replace(",",""))) if price_m else (None, None)
    
    # Overview（概要）の抽出
    overview = ""
    overview_section = top.select_one("section[data-testid='overview']")
    if overview_section:
        overview_p = overview_section.select("p")
        if overview_p:
            overview = " ".join(p.get_text(strip=True) for p in overview_p[:2])  # 最初の2段落
    
    # もしoverview sectionが見つからない場合、他の方法を試す
    if not overview:
        # レビューセクションやイントロダクション部分を探す
        intro_sections = top.select(".review-intro p, .car-overview p, .intro p")
        if intro_sections:
            overview = " ".join(p.get_text(strip=True) for p in intro_sections[:2])
    
    # まだ見つからない場合、車種説明を探す
    if not overview:
        desc_sections = top.select("[data-testid*='description'] p, .description p, .content p")
        if desc_sections:
            overview = " ".join(p.get_text(strip=True) for p in desc_sections[:2])
    
    ## ======= Model, Body type, Fuel =======
    glance = top.select_one(".review-overview__at-a-glance-model")
    body_type = fuel = None
    
    if glance:
        blocks = [b.get_text(strip=True) for b in glance.select("div")]
        for k, v in zip(blocks[::2], blocks[1::2]):
            if k.startswith("Body type"):      
                body_type = v
            elif k.startswith("Available fuel"): 
                fuel = v
    
    # 別の方法でbody_typeとfuelを探す
    if not body_type or not fuel:
        spec_text = top.get_text()
        
        # Body typeパターン
        body_match = re.search(r'Body type[:\s]+([^\n]+)', spec_text, re.IGNORECASE)
        if body_match and not body_type:
            body_type = body_match.group(1).strip()
        
        # Fuel typeパターン  
        fuel_match = re.search(r'(?:Fuel|Engine)[:\s]+([^\n]+)', spec_text, re.IGNORECASE)
        if fuel_match and not fuel:
            fuel = fuel_match.group(1).strip()
    
    # 仕様情報の抽出（JSON形式）
    spec_data = {}
    spec_sections = top.select(".specifications table, .specs table, table")
    for table in spec_sections:
        for row in table.select("tr"):
            cells = row.select("td, th")
            if len(cells) >= 2:
                key = cells[0].get_text(strip=True)
                value = cells[1].get_text(strip=True)
                if key and value and len(key) < 100:  # 長すぎるキーは除外
                    spec_data[key] = value
    
    ## ======= 画像 =======
    media_urls = _clean_imgs(top, url)
    
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
        "spec_json": json.dumps(spec_data) if spec_data else "{}",
        "media_urls": media_urls,
    }
