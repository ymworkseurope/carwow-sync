#!/usr/bin/env python3
"""
make_scraper.py – 2025-09-xx fixed-review
メーカー直下ページ (例: /kia) から
<make>/<model> 形式の “レビュー元 URL” を抽出する軽量モジュール
"""

import re, requests, bs4
from urllib.parse import urljoin, urlparse

HEAD = {
    "User-Agent": "Mozilla/5.0 (+https://github.com/ymworkseurope/carwow-sync)"
}

# モデルではないセグメントを除外
NON_MODEL_SEGMENTS = {
    "electric","hybrid","suv","suvs","estate","hatchback","convertible",
    "coupe","saloon","people-carriers","mpv","colours","colors",
    "two-tone","used","lease","deals","prices","finance","reviews",
    # ↑ “reviews” 自体も除外。/review は後で削るので OK
}

SEG_RE = re.compile(r"^[a-z0-9-]+$")      # 英数字とハイフンのみ

def _normalise(href: str) -> str|None:
    """
    /audi/q4-e-tron/review → /audi/q4-e-tron に変換。
    モデル階層以外なら None を返す。
    """
    href = href.split("#")[0].split("?")[0].rstrip("/")
    if not href.startswith("/"):          # フル URL もあり得る
        href = urlparse(href).path
    if href.endswith("/review"):
        href = href[:-7]                  # /review を削除
    parts = href.strip("/").split("/")
    if len(parts) != 2:
        return None
    make, model = parts
    if model in NON_MODEL_SEGMENTS:
        return None
    if not (SEG_RE.fullmatch(make) and SEG_RE.fullmatch(model)):
        return None
    return "/" + "/".join(parts)          # 例: /audi/q4-e-tron

def get_model_urls(make: str) -> set[str]:
    page = f"https://www.carwow.co.uk/{make}"
    html = requests.get(page, headers=HEAD, timeout=30).text
    doc  = bs4.BeautifulSoup(html, "lxml")

    out: set[str] = set()
    for a in doc.select(f'a[href*="/{make}/"]'):
        href = a.get("href") or ""
        norm = _normalise(href)
        if norm:
            out.add(urljoin("https://www.carwow.co.uk", norm))
    return out

