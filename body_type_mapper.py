"""Build slug → body_type list mapping & cache as body_map_<make>.json
例: python body_type_mapper.py abarth
"""
from __future__ import annotations
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import json, time, sys


# Car‑type フィルタ名（UI 表示ラベル ⇒ キーに含まれる英語）
FILTERS = {
"Small cars": "small-cars",
"Sports cars": "sports-cars",
"Hybrid & electric cars": "hybrid-electric-cars",
"Convertibles": "convertibles",
"SUVs": "suvs",
"Hot hatches": "hot-hatches",
"Hatchbacks": "hatchbacks",
}




def build_map(make: str) -> dict[str, list[str]]:
"""メーカーごとに slug→[body_type,…] を生成"""
url = f"https://www.carwow.co.uk/{make}"


opt = Options()
opt.add_argument("--headless=new")
opt.add_argument("--no-sandbox")
opt.add_argument("--disable-dev-shm-usage")


slug2types: dict[str, list[str]] = {}


with webdriver.Chrome(options=opt) as drv:
drv.get(url)
time.sleep(2) # 初期ロード待ち


# --- 何も選択していない状態の slug = 全モデル ---
base = {
a.get_attribute("href").rstrip("/").split("/")[-1]
for a in drv.find_elements("css selector", "article.card-compact a[href*='://']")
}


# --- フィルタを順にクリックして差分取得 ---
for bt_en, css_key in FILTERS.items():
sel = f"[data-filter*='{css_key}'], label[for*='{css_key}']"
try:
drv.find_element("css selector", sel).click()
except Exception:
continue # フィルタ自体が無い
time.sleep(1)


current = {
a.get_attribute("href").rstrip("/").split("/")[-1]
for a in drv.find_elements("css selector", "article.card-compact a[href*='://']")
}
diff = current - base
for slug in diff:
slug2types.setdefault(slug, []).append(bt_en)


# トグル解除
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
