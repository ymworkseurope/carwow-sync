#!/usr/bin/env python3
"""
auto_maker_scraper.py – 2025-09-09 r2
────────────────────────────────────────────────────────────────────
Carwow から “現存する全メーカー” を抽出 → スペース区切りで返すユーティリティ
  * /brands, トップページ, robots.txt / sitemap の３段構え
  * 生成結果を GitHub Actions でそのまま環境変数へ流用しやすい形式で出力
依存: requests, beautifulsoup4 (＋lxml)
"""

from __future__ import annotations
import re, sys, time
from typing import List, Set
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

BASE = "https://www.carwow.co.uk"
HEADERS = {"User-Agent": "Mozilla/5.0 (carwow-auto-maker-scraper/2025)"}
TIMEOUT = 20

# ────────────────────────── helper
def _extract_maker(url: str) -> str | None:
    """URL 文字列から maker 名を推定して返す（不適切なら None）"""
    if not url:
        return None
    if url.startswith("/"):
        url = urljoin(BASE, url)
    try:
        path = urlparse(url).path.strip("/").lower()
    except Exception:
        return None
    part = path.split("/", 1)[0]
    if len(part) < 2 or len(part) > 20:
        return None
    # 除外ワード
    if part in {
        "news", "blog", "help", "sell", "compare", "tools", "brands",
        "finance", "insurance", "lease", "deals", "search", "about",
        "reviews", "electric", "hybrid", "suv", "hatchback", "saloon",
        "estate", "coupe", "convertible", "used", "new",
    }:
        return None
    if not re.fullmatch(r"[a-z\-]+", part):
        return None
    return part

# ────────────────────────── main scraper
class MakerScraper:
    def __init__(self):
        self.s = requests.Session()
        self.s.headers.update(HEADERS)

    # 1) /brands ページ
    def from_brands(self) -> Set[str]:
        makers: Set[str] = set()
        try:
            r = self.s.get(f"{BASE}/brands", timeout=TIMEOUT)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "lxml")
            for a in soup.select('a[href*="/brands/"]'):
                m = _extract_maker(a.get("href", ""))
                if m:
                    makers.add(m)
        except Exception as e:
            print(f"[warn] /brands fetch failed: {e}", file=sys.stderr)
        return makers

    # 2) トップページ
    def from_home(self) -> Set[str]:
        makers: Set[str] = set()
        try:
            r = self.s.get(BASE, timeout=TIMEOUT)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "lxml")
            for a in soup.find_all("a", href=True):
                m = _extract_maker(a["href"])
                if m:
                    makers.add(m)
        except Exception as e:
            print(f"[warn] / homepage fetch failed: {e}", file=sys.stderr)
        return makers

    # 3) robots.txt / sitemap
    def from_sitemap(self) -> Set[str]:
        makers: Set[str] = set()
        for url in (f"{BASE}/robots.txt",
                    f"{BASE}/sitemap.xml",
                    f"{BASE}/sitemap_index.xml"):
            try:
                r = self.s.get(url, timeout=TIMEOUT)
                if not r.ok:
                    continue
                urls = re.findall(r"https?://[^\s<>\"']+", r.text)
                for u in urls:
                    m = _extract_maker(u)
                    if m:
                        makers.add(m)
                if makers:
                    break
            except Exception:
                continue
        return makers

    def all_makers(self) -> List[str]:
        makers: Set[str] = set()
        makers.update(self.from_brands())
        makers.update(self.from_home())
        makers.update(self.from_sitemap())
        # 正規化 & ソート
        return sorted(makers)


def main() -> None:
    print("🚗  Discovering all makers from carwow…", file=sys.stderr)
    start = time.time()
    scraper = MakerScraper()
    makers = scraper.all_makers()
    elapsed = time.time() - start
    # ── human readable
    print(f"\n✅  Found {len(makers)} makers in {elapsed:.1f}s")
    for i, m in enumerate(makers, 1):
        print(f"{i:>2}  {m}")
    # ── GitHub Actions 用
    joined = " ".join(makers)
    print(f'\nMAKES_FOR_BODYMAP: "{joined}"')
    # stdout へ makers だけ吐き、呼び出し側が拾う
    print(joined)


if __name__ == "__main__":
    main()
