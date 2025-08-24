#!/usr/bin/env python3
"""
make_scraper.py
────────────────────────────────────────
メーカー直下ページ (例: /kia) に並ぶカードから
<make>/<model> 形式 URL を抜き出すだけの超軽量版。
Selenium 不要、requests + BeautifulSoup で完結。
"""

import re
import requests, bs4
from urllib.parse import urljoin

HEAD = {
    "User-Agent": "Mozilla/5.0 (+https://github.com/ymworkseurope/carwow-sync)"
}

# 「モデルではない部分」を除外
NON_MODEL_SEGMENTS = {
    "electric","hybrid","suv","suvs","estate","hatchback","convertible",
    "coupe","saloon","people-carriers","mpv","colours","colors",
    "two-tone","used","lease","deals","prices","finance","reviews"
}

_RE_MODEL = re.compile(r"^/(?P<make>[a-z0-9-]+)/(?P<model>[a-z0-9-]+)$")

def get_model_urls(make: str) -> set[str]:
    page = f"https://www.carwow.co.uk/{make}"
    doc  = bs4.BeautifulSoup(requests.get(page, headers=HEAD, timeout=30).text, "lxml")

    out:set[str] = set()
    for a in doc.select(f'a[href^="/{make}/"]'):
        href = a.get("href","").split("#")[0].rstrip("/")
        m = _RE_MODEL.match(href)
        if not m:
            continue
        seg = m.group("model")
        if seg in NON_MODEL_SEGMENTS:
            continue
        out.add(urljoin("https://www.carwow.co.uk", href))
    return out
