#!/usr/bin/env python3
# body_type_mapper.py – 2025-08-25 fixed-version
"""
使い方: python body_type_mapper.py audi
各カテゴリー専用ページから直接車両データを取得して
slug → [body_type,…] の対応表を body_map_<make>.json として保存
"""
from __future__ import annotations
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import json
import time
import sys
import re

# カテゴリーとそのURL suffixの対応
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

def extract_car_links(driver, make: str, category_url: str) -> set[str]:
    """指定されたカテゴリーページから車のslugを抽出"""
    try:
        driver.get(category_url)
        wait = WebDriverWait(driver, 10)
        
        # ページが読み込まれるまで待機
        time.sleep(3)
        
        # 車のリンクを取得（複数のセレクタを試す）
        possible_selectors = [
            "a[href*='/" + make + "/'][href$='review']",  # レビューページへのリンク
            f"a[href*='/{make}/']:not([href*='/used']):not([href*='/deals'])",  # 一般的なモデルページ
            "article a[href*='/" + make + "/']",  # articleタグ内のリンク
            ".car-card a, .model-card a, .vehicle-card a",  # 車カード内のリンク
        ]
        
        slugs = set()
        for selector in possible_selectors:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                for element in elements:
                    href = element.get_attribute("href")
                    if href and f"/{make}/" in href:
                        # URLからslugを抽出 (例: /audi/q3 -> q3)
                        match = re.search(rf"/{make}/([^/?#]+)", href)
                        if match:
                            slug = match.group(1)
                            # 不要なパスを除外
                            if slug not in ['used', 'deals', 'suv', 'electric', 'hybrid', 'convertible', 'estate', 'hatchback', 'saloon', 'coupe', 'sports']:
                                slugs.add(slug)
                                
            except Exception as e:
                print(f"セレクタ {selector} でエラー: {e}")
                continue
                
        return slugs
        
    except Exception as e:
        print(f"エラー: {category_url} の取得に失敗 - {e}")
        return set()

def build_map(make: str) -> dict[str, list[str]]:
    """各カテゴリーページを巡回してbody typeのマッピングを作成"""
    
    opt = Options()
    opt.add_argument("--headless=new")
    opt.add_argument("--no-sandbox")  
    opt.add_argument("--disable-dev-shm-usage")
    opt.add_argument("--disable-blink-features=AutomationControlled")
    opt.add_experimental_option("excludeSwitches", ["enable-automation"])
    opt.add_experimental_option('useAutomationExtension', False)
    
    slug2types: dict[str, list[str]] = {}
    
    with webdriver.Chrome(options=opt) as driver:
        # User-Agentを設定
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        for body_type, url_suffix in CATEGORIES.items():
            category_url = f"https://www.carwow.co.uk/{make}/{url_suffix}"
            print(f"▶ {body_type}カテゴリーを取得中: {category_url}")
            
            slugs = extract_car_links(driver, make, category_url)
            
            if slugs:
                print(f"  見つかった車両: {len(slugs)}台 - {list(slugs)}")
                for slug in slugs:
                    if slug not in slug2types:
                        slug2types[slug] = []
                    if body_type not in slug2types[slug]:
                        slug2types[slug].append(body_type)
            else:
                print(f"  {body_type}カテゴリーで車両が見つかりませんでした")
            
            # リクエスト間隔を空ける
            time.sleep(2)
    
    return slug2types

def main():
    make = sys.argv[1] if len(sys.argv) > 1 else "audi"
    dst = f"body_map_{make}.json"
    
    print(f"▶ {make}のbody mapを構築中...")
    mapping = build_map(make)
    
    with open(dst, "w", encoding="utf-8") as f:
        json.dump(mapping, f, ensure_ascii=False, indent=2)
    
    print(f"saved ⇒ {dst} ({len(mapping)} models)")
    
    # 結果の詳細表示
    if mapping:
        print("\n取得されたマッピング:")
        for slug, types in sorted(mapping.items()):
            print(f"  {slug}: {types}")
    else:
        print("⚠️ データが取得できませんでした。以下を確認してください:")
        print("  1. メーカー名が正しいか")
        print("  2. carwow.co.ukのサイト構造が変更されていないか")
        print("  3. ネットワーク接続に問題がないか")

if __name__ == "__main__":
    main()
