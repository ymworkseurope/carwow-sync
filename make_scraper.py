#!/usr/bin/env python3
# make_scraper.py – 2025-09-xx fixed-review

"""
メーカー直下ページ（例: /kia）から
  /<make>/<model> 形式のレビュー URL だけを静的に抽出する軽量モジュール
Selenium 不要・requests + BeautifulSoup で完結
"""

import re, requests, bs4
from urllib.parse import urljoin, urlparse

HEAD = {
    "User-Agent": "Mozilla/5.0 (+https://github.com/ymworkseurope/carwow-sync)"
}

# モデルではないまとめ／色／カテゴリの末尾セグメントを除外
NON_MODEL_SEGMENTS = {
    # カテゴリ
    "electric","hybrid","suv","suvs","estate","hatchback","convertible",
    "coupe","saloon","people-carriers","mpv",
    # まとめ
    "automatic","manual","lease","used","deals","prices","finance","reviews",
    # 色
    "white","black","silver","grey","gray","red","blue","green","yellow","orange",
    "brown","purple","pink","gold","bronze","beige","cream",
    # その他
    "news","two-tone","multi-colour","multi-color"
}

_RE_SEG = re.compile(r"^[a-z0-9-]+$")         # 英数字＋ハイフンのみ

# ───────────────────────────────────────── helpers
def _normalise_href(href: str) -> str | None:
    """
    a[href] の値を /<make>/<model> の形に正規化。
    ・末尾 /review は削除
    ・クエリ # アンカーは削除
    ・セグメントが 2 つ以外は除外
    ・NON_MODEL_SEGMENTS に該当すれば除外
    """
    href = href.split("#")[0].split("?")[0].rstrip("/")
    if not href.startswith("/"):
        href = urlparse(href).path          # フル URL → path 部分

    if href.endswith("/review"):
        href = href[:-7]                    # /review を除去

    parts = href.strip("/").split("/")
    if len(parts) != 2:
        return None

    make, model = parts
    if model in NON_MODEL_SEGMENTS:
        return None
    if not (_RE_SEG.fullmatch(make) and _RE_SEG.fullmatch(model)):
        return None

    return "/" + "/".join(parts)

# ───────────────────────────────────────── public
def get_model_urls(make: str) -> set[str]:
    """指定メーカーのモデル URL セットを返す"""
    page = f"https://www.carwow.co.uk/{make}"
    html = requests.get(page, headers=HEAD, timeout=30).text
    doc  = bs4.BeautifulSoup(html, "lxml")

    out: set[str] = set()
    for a in doc.select(f'a[href*="/{make}/"]'):
        href  = a.get("href") or ""
        fixed = _normalise_href(href)
        if fixed:
            out.add(urljoin("https://www.carwow.co.uk", fixed))

    return out
