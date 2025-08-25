#!/usr/bin/env python3
"""
auto_maker_scraper.py – 2025-09-08
────────────────────────────────────────────────────────────
Carwow から “全メーカー” の英語 slug を抽出し、スペース区切りで
標準出力へ返すシンプルなツール。

・Python 3.9 以降 / requests / beautifulsoup4 / lxml が必要
・--short オプションを付けると slug 群だけを 1 行で出力
  （GitHub Actions の環境変数用途）
"""

from __future__ import annotations

import argparse
import re
import sys
from typing import List, Set
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

BASE      = "https://www.carwow.co.uk"
HEADERS   = {"User-Agent": "Mozilla/5.0 (carwow-auto-scraper/2025)"}
TIMEOUT   = 25
EXCLUDE   = {
    # 汎用ページ
    "news","review","reviews","blog","help","about","finance","lease","used",
    "sell","deals","search","compare","tools","electric","hybrid","suv","mpv",
    "hatchback","saloon","coupe","estate",
}

class MakerScraper:
    def __init__(self) -> None:
        self.session          = requests.Session()
        self.session.headers  = HEADERS.copy()
        self.makers: Set[str] = set()

    # ────────────────────────────── public
    def get_all(self) -> List[str]:
        self._from_brands()
        self._from_home()
        self._from_sitemap()
        return sorted(self._filter(self.makers))

    # ────────────────────────────── helpers
    def _from_brands(self) -> None:
        print("📋  /brands ページを走査…")
        try:
            soup = self._soup(f"{BASE}/brands")
            for a in soup.select('a[href*="/brands/"]'):
                self._add(a["href"])
        except Exception as e:
            print(f"  ✗ brands error: {e}")

    def _from_home(self) -> None:
        print("🏠  トップページを走査…")
        try:
            soup = self._soup(BASE)
            for a in soup.find_all("a", href=True):
                self._add(a["href"])
        except Exception as e:
            print(f"  ✗ home error: {e}")

    def _from_sitemap(self) -> None:
        print("🗺️  サイトマップを走査…")
        for sm in ("/sitemap.xml", "/sitemap_index.xml", "/robots.txt"):
            url = BASE + sm
            try:
                txt = self.session.get(url, timeout=TIMEOUT).text
            except Exception:
                continue
            for u in re.findall(r"https?://[^\s\"'<>]+", txt):
                self._add(u)

    def _add(self, href: str) -> None:
        slug = self._extract(href)
        if slug:
            self.makers.add(slug)

    def _extract(self, url: str) -> str:
        if url.startswith("/"):
            url = urljoin(BASE, url)
        try:
            part = urlparse(url).path.strip("/").split("/")[0].lower()
        except Exception:
            return ""
        return part if self._valid(part) else ""

    # ───────── utility
    def _soup(self, url: str) -> BeautifulSoup:
        r = self.session.get(url, timeout=TIMEOUT)
        r.raise_for_status()
        return BeautifulSoup(r.text, "lxml")

    def _valid(self, name: str) -> bool:
        return (
            1 < len(name) < 20
            and name.isascii()
            and re.fullmatch(r"[a-z-]+", name)
            and name not in EXCLUDE
        )

    def _filter(self, items: Set[str]) -> List[str]:
        print(f"✅  抽出メーカー数: {len(items)}")
        return list(items)


# ───────────────────────────────────────── CLI
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--short", action="store_true",
                    help="スペース区切りで 1 行出力 (CI 用)")
    args = ap.parse_args()

    makers = MakerScraper().get_all()

    if args.short:
        print(" ".join(makers))
    else:
        print("\n📊  全メーカー一覧")
        for i, m in enumerate(makers, 1):
            print(f"{i:2d}. {m}")
        print("\n🔧  環境変数用:")
        print(f'MAKES_FOR_BODYMAP: "{" ".join(makers)}"')


if __name__ == "__main__":
    main()
