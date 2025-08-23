#!/usr/bin/env python3
# body_type_mapper.py – 2025-08-25 link-selector-fixed
"""
使い方: python body_type_mapper.py abarth
メーカー一覧ページのフィルタを順に ON/OFF して
slug → [body_type,…] の対応表を body_map_<make>.json として保存
"""

from __future__ import annotations
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import json, time, sys

FILTERS = {
    "Small cars":             "small-cars",
    "Sports cars":            "sports-cars",
    "Hybrid & electric cars": "hybrid-electric-cars",
    "Convertibles":           "convertibles",
    "SUVs":                   "suvs",
    "Hot hatches":            "hot-hatches",
    "Hatchbacks":             "hatchbacks",
}


def build_map(make: str) -> dict[str, list[str]]:
    url = f"https://www.carwow.co.uk/{make}"

    opt = Options()
    opt.add_argument("--headless=new")
    opt.add_argument("--no-sandbox")
    opt.add_argument("--disable-dev-shm-usage")

    slug2types: dict[str, list[str]] = {}

    with webdriver.Chrome(options=opt) as drv:
        drv.get(url)
        time.sleep(2)  # 初期ロード待ち

        # ▼ カード内リンクの CSS セレクタを 1 箇所にまとめる（環境差対策）
        css_link = "article.card-compact a.card-compact-review"

        base = {
            a.get_attribute("href").rstrip("/").split("/")[-1]
            for a in drv.find_elements("css selector", css_link)
        }

        for bt_en, key in FILTERS.items():
            sel = f"[data-filter*='{key}'], label[for*='{key}']"
            try:
                drv.find_element("css selector", sel).click()
            except Exception:
                continue
            time.sleep(1)

            current = {
                a.get_attribute("href").rstrip("/").split("/")[-1]
                for a in drv.find_elements("css selector", css_link)
            }
            diff = current - base
            for slug in diff:
                slug2types.setdefault(slug, []).append(bt_en)

            drv.find_element("css selector", sel).click()
            time.sleep(0.5)

    return slug2types


if __name__ == "__main__":
    make = sys.argv[1] if len(sys.argv) > 1 else "abarth"
    dst = f"body_map_{make}.json"
    mapping = build_map(make)
    with open(dst, "w", encoding="utf-8") as f:
        json.dump(mapping, f, ensure_ascii=False, indent=2)
    print(f"saved ⇒ {dst} ({len(mapping)} models)")
