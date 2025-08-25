#!/usr/bin/env python3
# body_type_mapper.py – 2025-09-06 r3
"""
make 単位で slug → [body_type,…] を作成。
改良点
  1. 無限スクロール対応（scrollTo + 判定）
  2. make のトップページも走査
  3. _NEXT_DATA_ の JSON があれば body_type を直接読む
  4. カテゴリーに無い slug を最後に 'Unknown' として残さない
"""

from __future__ import annotations
import json, re, sys, time
from pathlib import Path
from typing import Dict, List, Set

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup

BASE = "https://www.carwow.co.uk"
CATEGORIES = {
    "SUVs": "suv",
    "Electric": "electric",
    "Hybrid": "hybrid",
    "Convertible": "convertible",
    "Estate": "estate",
    "Hatchback": "hatchback",
    "Saloon": "saloon",
    "Coupe": "coupe",
    "Sports": "sports",
}
EXCLUDE = {
    "used", "deals", *CATEGORIES.values()
}

# ────────────────────────── helpers

def page_slugs(html: str, make: str) -> Set[str]:
    """a タグと __NEXT_DATA__ の両方から slug を抽出"""
    soup = BeautifulSoup(html, "lxml")
    out: Set[str] = set()

    # 1) a タグ
    for a in soup.select(f"a[href*='/{make}/']"):
        href = a.get("href") or ""
        m = re.search(rf"/{make}/([^/?#]+)", href)
        if m:
            slug = m.group(1)
            if slug not in EXCLUDE and len(slug) > 1:
                out.add(slug)

    # 2) __NEXT_DATA__ JSON
    script = soup.find("script", id="__NEXT_DATA__")
    if script and script.string:
        try:
            j = json.loads(script.string)
            nodes = (
                j["props"]["pageProps"]
                .get("collection", {})
                .get("productCardList", [])
            )
            for node in nodes:
                slug = (node.get("url") or "").split("/")[-1]
                if slug and slug not in EXCLUDE:
                    out.add(slug)
        except Exception:
            pass

    return out


def scroll_to_bottom(driver) -> None:
    last = driver.execute_script("return document.body.scrollHeight")
    while True:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(1.2)
        new = driver.execute_script("return document.body.scrollHeight")
        if new == last:
            break
        last = new


def grab_html(driver, url: str) -> str:
    driver.get(url)
    scroll_to_bottom(driver)
    return driver.page_source


# ────────────────────────── main mapping

def build_map(make: str) -> Dict[str, List[str]]:
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")

    slug2types: Dict[str, List[str]] = {}

    with webdriver.Chrome(options=opts) as d:
        # ① カテゴリーページ
        for bt, suf in CATEGORIES.items():
            url = f"{BASE}/{make}/{suf}"
            print(f"▶ {bt:<11} {url}")
            html = grab_html(d, url)
            for slug in page_slugs(html, make):
                slug2types.setdefault(slug, []).append(bt)

        # ② make 直下（拾い残しケア）
        root_html = grab_html(d, f"{BASE}/{make}")
        for slug in page_slugs(root_html, make):
            slug2types.setdefault(slug, [])

    return slug2types


def main():
    make = (sys.argv[1] if len(sys.argv) > 1 else "audi").lower()
    mapping = build_map(make)
    fp = Path(f"body_map_{make}.json")
    fp.write_text(json.dumps(mapping, ensure_ascii=False, indent=2))
    print(f"✓ saved {fp} ({len(mapping)} models)")

if __name__ == "__main__":
    main()
