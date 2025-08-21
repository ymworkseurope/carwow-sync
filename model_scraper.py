# model_scraper.py
# rev: 2025-08-23 T10:30Z
import re, json, time, random, requests, bs4
from urllib.parse import urljoin
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
    out=[]
    for img in s.select("img[src]"):
        src=img["src"]
        if any(k in src.lower() for k in ("logo","icon","badge","sprite","favicon")):
            continue
        full=urljoin(base,src)
        if full.startswith("http") and full not in out:
            out.append(full)
        if len(out)>=limit:
            break
    return out

def scrape(url: str) -> Dict:
    """https://www.carwow.co.uk/skoda/octavia-estate → 仕様・画像 etc."""
    top=_bs(url)
    _sleep()
    # -- 「Overview」の見出しブロックが必ず 1 個だけあるのでそこを anchor にする
    title = top.select_one("h1").get_text(" ", strip=True)
    price_txt = top.get_text(" ", strip=True)
    price_m = re.search(r"£([\d,]+)\s*[–-]\s*£([\d,]+)", price_txt)
    pmin,pmax = (int(price_m[1].replace(",","")), int(price_m[2].replace(",",""))) if price_m else (None,None)

    ## ======= Model, Body type, Fuel =======
    glance = top.select_one(".review-overview__at-a-glance-model")
    model = body_type = fuel = None
    if glance:
        blocks = [b.get_text(strip=True) for b in glance.select("div")]
        for k,v in zip(blocks[::2],blocks[1::2]):
            if   k.startswith("Model"):          model = v
            elif k.startswith("Body type"):      body_type = v
            elif k.startswith("Available fuel"): fuel = v

    ## ======= 画像 =======
    media_urls = _clean_imgs(top, url)

    return {
        "slug"          : slugify(url.split("/")[-1]),
        "url"           : url,
        "title"         : title,
        "model_en"      : model or title.split()[-1],
        "body_type"     : body_type,
        "fuel"          : fuel,
        "price_min_gbp" : pmin,
        "price_max_gbp" : pmax,
        "media_urls"    : media_urls,
    }
