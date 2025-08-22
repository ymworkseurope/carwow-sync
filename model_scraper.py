# model_scraper.py
# rev: 2025-08-24 修正版
import re, json, time, random, requests, bs4
from urllib.parse import urljoin, urlparse
from slugify import slugify
from typing import Dict, List

UA = ("Mozilla/5.0 (+https://github.com/ymworkseurope/"
      "carwow-sync 2025-08-23)")
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
    make_en = path_parts[0] if len(path_parts) > 0 else ""
    model_en = path_parts[1] if len(path_parts) > 1 else ""
    
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
    
    # 仕様情報の抽出（JSON形式）
    spec_data = {}
    spec_sections = top.select(".specifications table, .specs table")
    for table in spec_sections:
        for row in table.select("tr"):
            cells = row.select("td, th")
            if len(cells) >= 2:
                key = cells[0].get_text(strip=True)
                value = cells[1].get_text(strip=True)
                if key and value:
                    spec_data[key] = value
    
    ## ======= 画像 =======
    media_urls = _clean_imgs(top, url)
    
    return {
        "slug": slugify(f"{make_en}-{model_en}"),
        "url": url,
        "title": title,
        "make_en": make_en.title(),  # 最初の文字を大文字に
        "model_en": model_en.replace('-', ' ').title(),  # ハイフンをスペースに、タイトルケース
        "overview_en": overview,
        "body_type": body_type,
        "fuel": fuel,
        "price_min_gbp": pmin,
        "price_max_gbp": pmax,
        "spec_json": json.dumps(spec_data) if spec_data else "{}",
        "media_urls": media_urls,
    }
