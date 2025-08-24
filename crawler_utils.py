#!/usr/bin/env python3
# crawler_utils.py – 2025-09-02

"""
モデル URL 一覧を作り、レビュー / リスト / 色別ページ等を除外する共通ユーティリティ
"""

import re, time, requests, bs4
from urllib.parse import urljoin

UA   = "Mozilla/5.0 (+https://github.com/ymworkseurope/carwow-sync 2025-09-02)"
HEAD = {"User-Agent": UA}

_EXCLUDE_KEYWORDS = (
    "/automatic", "/lease", "/used", "/deals",
    "/blue", "/green", "/red", "/black", "/grey", "/orange",
    # など色名や特殊 URL
)

def _get(url: str) -> requests.Response:
    r = requests.get(url, headers=HEAD, timeout=30)
    r.raise_for_status(); return r

def _bs(url:str): return bs4.BeautifulSoup(_get(url).text, "lxml")

def is_review_page(url:str)->bool:
    try:
        doc = _bs(url)
        tab = doc.select_one('a[data-main-menu-section="car-reviews"]')
        return bool(tab and 'is-active' in tab.get('class', []))
    except Exception: return False

def collect_model_urls(make_url: str) -> list[str]:
    """
    メーカーのトップページ（例 https://www.carwow.co.uk/bmw）から
    モデルページだけを収集
    """
    base = make_url.rstrip("/") + "/"
    doc  = _bs(base)
    links = [
        urljoin(base, a["href"].split("#")[0])
        for a in doc.select('a.card-compact-review[href]')
    ]
    cleaned = []
    for u in links:
        if any(k in u for k in _EXCLUDE_KEYWORDS):      # 文字列除外
            continue
        if is_review_page(u):                           # レビューページなら skip
            continue
        cleaned.append(u.rstrip("/"))                   # 正規化
    return sorted(set(cleaned))
