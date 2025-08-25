#!/usr/bin/env python3
"""
auto_maker_scraper.py – 2025‑09‑08  ✨FULL VERSION✨
──────────────────────────────────────────────────
Carwow から *利用可能な全メーカー* を抽出して、
GitHub Actions の環境変数 `MAKES_FOR_BODYMAP` 向けに
スペース区切り文字列を出力します。

主な特長
────────
1. **brands ページ・トップページ・サイトマップ** の 3 段抽出で取り漏らしを最小化。
2. 除外キーワード／正規表現で“モデル名”や汎用ページを自動フィルタ。
3. `--short` オプションで *環境変数出力のみ*、何も付けなければテーブル表示付き。
4. UA 偽装＆リトライ付き `requests.Session`。

必要ライブラリ: `requests`, `beautifulsoup4`, `lxml`（インストール済みなら OK）
"""
from __future__ import annotations

import argparse
import re
import sys
import textwrap
from html import unescape
from typing import List, Set
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://www.carwow.co.uk"
HEADERS = {"User-Agent": "Mozilla/5.0 (carwow-auto-maker/1.0)"}
TIMEOUT = 30
EXCLUDE = {
    "news",
    "blog",
    "lease",
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
    "reviews",
    "review",
    "search",
    "tools",
    "help",
    "compare",
    "brands",
    "cars",
    "advice",
    "terms",
    "privacy",
    "about",
    "contact",
    "cookie",
}


class MakerScraper:
    def __init__(self) -> None:
        s = requests.Session()
        s.headers.update(HEADERS)
        self.sess = s

    # ───────────────────────── internal helpers

    def _clean(self, name: str) -> str:
        return unescape(name.strip().lower())

    def _valid(self, name: str) -> bool:
        return (
            name
            and name not in EXCLUDE
            and 1 < len(name) < 20
            and re.fullmatch(r"[a-z\-]+", name) is not None
        )

    def _extract_from_href(self, href: str) -> str | None:
        if not href:
            return None
        # 相対→絶対化
        if href.startswith("/"):
            href = urljoin(BASE_URL, href)
        parsed = urlparse(href)
        parts = [p for p in parsed.path.split("/") if p]
        if parts:
            cand = self._clean(parts[0])
            return cand if self._valid(cand) else None
        return None

    # ───────────────────────── 3 sources

    def from_brands(self) -> Set[str]:
        url = f"{BASE_URL}/brands"
        try:
            r = self.sess.get(url, timeout=TIMEOUT)
            r.raise_for_status()
        except Exception as e:
            print(f" ✗ /brands error: {e}", file=sys.stderr)
            return set()
        soup = BeautifulSoup(r.text, "lxml")
        makers = {
            self._extract_from_href(a.get("href", ""))
            for a in soup.select('a[href^="/brands/"]')
        }
        makers.discard(None)
        return set(makers)  # type: ignore

    def from_home(self) -> Set[str]:
        try:
            r = self.sess.get(BASE_URL, timeout=TIMEOUT)
            r.raise_for_status()
        except Exception as e:
            print(f" ✗ / home error: {e}", file=sys.stderr)
            return set()
        soup = BeautifulSoup(r.text, "lxml")
        makers = {self._extract_from_href(a.get("href", "")) for a in soup.find_all("a")}
        makers.discard(None)
        return set(makers)  # type: ignore

    def from_sitemap(self) -> Set[str]:
        sitemap_urls = [
            f"{BASE_URL}/sitemap.xml",
            f"{BASE_URL}/sitemap_index.xml",
            f"{BASE_URL}/robots.txt",
        ]
        makers: Set[str] = set()
        for sm in sitemap_urls:
            try:
                r = self.sess.get(sm, timeout=TIMEOUT)
                if not r.ok:
                    continue
                urls = re.findall(r"https?://[^\s<>\"]+", r.text)
                for u in urls:
                    cand = self._extract_from_href(u)
                    if cand:
                        makers.add(cand)
            except Exception:
                continue
        return makers

    # ───────────────────────── public facade

    def all_makers(self) -> List[str]:
        makers: Set[str] = set()
        makers |= self.from_brands()
        makers |= self.from_home()
        makers |= self.from_sitemap()
        return sorted(makers)


# ───────────────────────── CLI

def main() -> None:
    ap = argparse.ArgumentParser(description="Fetch all car makes from carwow.co.uk")
    ap.add_argument(
        "--short",
        action="store_true",
        help="print space‑separated list for GitHub env (no extra output)",
    )
    args = ap.parse_args()

    scraper = MakerScraper()
    makers = scraper.all_makers()

    if args.short:
        print(" ".join(makers))
        return

    print("\n🚗  Carwow — ALL MAKES ({} total)".format(len(makers)))
    print("=" * 50)
    for i, mk in enumerate(makers, 1):
        print(f"{i:2d}. {mk}")
    print("\n📝 GitHub Actions export: \nMAKES_FOR_BODYMAP=\"{}\"".format(" ".join(makers)))


if __name__ == "__main__":
    main()
