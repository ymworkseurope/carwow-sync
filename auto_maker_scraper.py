#!/usr/bin/env python3
"""
auto_maker_scraper.py — 2025‑09‑08
────────────────────────────────────────────────────────────────────
Carwow から『登録されている全メーカー』を抽出して **MAKES_FOR_BODYMAP**
環境変数の値に使える形で出力するユーティリティ。

• 依存: requests, beautifulsoup4, lxml (同じ venv で動作)
• 使い方 (ローカル):  python auto_maker_scraper.py > makers.txt
• 使い方 (GitHub Actions 内):
    - run: |
        MAKES=$(python scripts/auto_maker_scraper.py --short)
        echo "MAKES_FOR_BODYMAP=$MAKES" >> "$GITHUB_ENV"

生成されるメーカー名は carwow の URL で使われる slug そのまま
(例: "alfa-romeo", "mercedes", "rolls-royce")。

メインロジックは以下の 3 つのソースを順番に試して統合します。
 1. https://www.carwow.co.uk/brands
 2. https://www.carwow.co.uk (トップページ内リンク)
 3. robots.txt / sitemap.xml 群 (フォールバック)
"""

from __future__ import annotations

import re
import sys
import textwrap
from typing import List, Set
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

#─────────────────────────────────────────────────────────────────────
BASE_URL = "https://www.carwow.co.uk"
UA = "Mozilla/5.0 (carwow-auto-maker-scraper/2025)"
TIMEOUT = 30

# carwow 以外の URL が混入するケースを防ぐ — host 名が BASE_URL と同じか判定
def _same_host(url: str) -> bool:
    return urlparse(url).netloc.replace("www.", "") == urlparse(BASE_URL).netloc.replace("www.", "")

class CarwowMakerScraper:
    """Carwow から全メーカー slug を抽出する"""

    #: メーカーではあり得ない語句 (除外フィルター)
    _EXCLUDED = {
        "news",
        "blog",
        "deals",
        "finance",
        "insurance",
        "sell",
        "electric",
        "hybrid",
        "suv",
        "hatchback",
        "saloon",
        "used",
        "new",
        "lease",
        "pcp",
        "reviews",
        "advice",
        "about",
        "contact",
        "help",
        "terms",
        "privacy",
        "brands",
        "cars",
        "search",
        "compare",
        "tools",
    }

    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": UA})

    #───────────────── public API ─────────────────
    def get_all_makers(self) -> List[str]:
        makers: Set[str] = set()

        makers.update(self._from_brands_page())
        makers.update(self._from_homepage())
        makers.update(self._from_sitemaps())

        return sorted(self._filter_normalize(makers))

    #───────────────── private helpers ────────────
    def _from_brands_page(self) -> Set[str]:
        makers: Set[str] = set()
        try:
            print("📋 /brands ページを解析中…", file=sys.stderr)
            soup = self._get_soup(f"{BASE_URL}/brands")
            links = soup.select('a[href^="/brands/"]')
            for a in links:
                href = a.get("href", "")
                maker = self._extract_maker(href)
                if maker:
                    makers.add(maker)
            print(f"  ↳ {len(makers)} found on /brands", file=sys.stderr)
        except Exception as e:
            print(f"  ✗ brands page: {e}", file=sys.stderr)
        return makers

    def _from_homepage(self) -> Set[str]:
        makers: Set[str] = set()
        try:
            print("🏠 ホームページを解析中…", file=sys.stderr)
            soup = self._get_soup(BASE_URL)
            for a in soup.find_all("a", href=True):
                href = a["href"]
                maker = self._extract_maker(href)
                if maker:
                    makers.add(maker)
            print(f"  ↳ {len(makers)} found on homepage", file=sys.stderr)
        except Exception as e:
            print(f"  ✗ homepage: {e}", file=sys.stderr)
        return makers

    def _from_sitemaps(self) -> Set[str]:
        makers: Set[str] = set()
        print("🗺️  サイトマップを解析中…", file=sys.stderr)
        cand = [
            f"{BASE_URL}/sitemap.xml",
            f"{BASE_URL}/sitemap_index.xml",
            f"{BASE_URL}/robots.txt",
        ]
        for url in cand:
            try:
                r = self.session.get(url, timeout=TIMEOUT)
                if not r.ok:
                    continue
                for loc in re.findall(r"https?://[^\s<>\"]+", r.text):
                    maker = self._extract_maker(loc)
                    if maker:
                        makers.add(maker)
            except Exception:
                continue
        print(f"  ↳ {len(makers)} found via sitemaps", file=sys.stderr)
        return makers

    #──────────── HTML fetch & parse ────────────
    def _get_soup(self, url: str) -> BeautifulSoup:
        r = self.session.get(url, timeout=TIMEOUT)
        r.raise_for_status()
        return BeautifulSoup(r.text, "lxml")

    #──────────── extract & validate ────────────
    def _extract_maker(self, href: str) -> str | None:
        if not href:
            return None
        # 絶対 URL に変換
        if href.startswith("/"):
            href = urljoin(BASE_URL, href)
        if not _same_host(href):
            return None
        path = urlparse(href).path.strip("/")
        if not path:
            return None
        maker = path.split("/")[0].lower()
        return maker if self._is_valid_maker(maker) else None

    def _is_valid_maker(self, name: str) -> bool:
        if name in self._EXCLUDED:
            return False
        if len(name) < 2 or len(name) > 20:
            return False
        # 英小文字とハイフンのみ許可
        return bool(re.match(r"^[a-z-]+$", name))

    def _filter_normalize(self, makers: Set[str]) -> List[str]:
        # 追加の品質チェックをここで実施可
        return [m.lower().strip() for m in makers if self._is_valid_maker(m)]

#─────────────────────────────────────────────────────────────────────
# CLI 入口
#   • --short   : 空白区切りのみ (GitHub Actions 用)
#   • それ以外 : 人間向けリスト & Actions 用の export 行 を出力
#─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    short = "--short" in sys.argv
    scraper = CarwowMakerScraper()
    makers = scraper.get_all_makers()

    if short:
        print(" ".join(makers))
        sys.exit(0)

    print("\n🎉 取得完了 — 全メーカー一覧 ({} 件)".format(len(makers)))
    for i, mk in enumerate(makers, 1):
        print(f"{i:>2}. {mk}")

    print("\n📝 GitHub Actions 用 (環境変数にコピペ) ──────────────────")
    print(f'MAKES_FOR_BODYMAP: "{" ".join(makers)}"')
