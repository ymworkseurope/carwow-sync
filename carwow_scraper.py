#!/usr/bin/env python3
"""
carwow_scraper.py - 完全に書き直したシンプル版
"""
import re
import json
import time
from typing import Dict, List, Optional
from bs4 import BeautifulSoup
import requests

BASE_URL = "https://www.carwow.co.uk"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

class CarwowScraper:
    
    def scrape_vehicle(self, slug: str) -> Optional[Dict]:
        """車両データを取得"""
        
        # メインページ取得
        main_url = f"{BASE_URL}/{slug}"
        main_resp = requests.get(main_url, headers=HEADERS, timeout=30)
        if main_resp.status_code != 200:
            return None
        
        main_soup = BeautifulSoup(main_resp.text, 'lxml')
        
        # 基本情報
        make_en = slug.split('/')[0].replace('-', ' ').title()
        # 特殊なメーカー名の処理
        make_map = {
            'Mercedes Benz': 'Mercedes-Benz',
            'Alfa Romeo': 'Alfa Romeo',
            'Land Rover': 'Land Rover',
            'Aston Martin': 'Aston Martin'
        }
        make_en = make_map.get(make_en, make_en)
        
        # タイトルからモデル名取得
        title = main_soup.find('title')
        model_en = ''
        if title:
            title_text = title.text
            # "Abarth 500e Review 2025" のような形式から抽出
            model_en = title_text.split('Review')[0].split('|')[0].replace(make_en, '').strip()
        
        # overview
        meta = main_soup.find('meta', {'name': 'description'})
        overview_en = meta.get('content', '') if meta else ''
        
        # 価格
        prices = {}
        text = main_soup.get_text()
        
        # Cash価格
        cash_match = re.search(r'Cash\s*£([\d,]+)', text)
        if cash_match:
            prices['price_min_gbp'] = int(cash_match.group(1).replace(',', ''))
        
        # Used価格  
        used_match = re.search(r'Used\s*£([\d,]+)', text)
        if used_match:
            prices['price_used_gbp'] = int(used_match.group(1).replace(',', ''))
        
        # RRP範囲
        rrp_match = re.search(r'RRP.*?£([\d,]+)\s*to\s*£([\d,]+)', text)
        if rrp_match:
            if not prices.get('price_min_gbp'):
                prices['price_min_gbp'] = int(rrp_match.group(1).replace(',', ''))
            prices['price_max_gbp'] = int(rrp_match.group(2).replace(',', ''))
        
        # メディアURL（最初の20個のみ）
        media_urls = []
        for img in main_soup.find_all('img', src=True)[:20]:
            src = img['src']
            if 'carwow' in src or 'prismic' in src:
                media_urls.append(src)
        
        # Specificationsページ確認
        grades = []
        specs = {}
        colors = []
        body_types = []
        
        specs_url = f"{BASE_URL}/{slug}/specifications"
        try:
            specs_resp = requests.get(specs_url, headers=HEADERS, timeout=30, allow_redirects=False)
            
            if specs_resp.status_code == 200:
                # パターン①: 詳細ページあり
                specs_soup = BeautifulSoup(specs_resp.text, 'lxml')
                specs_text = specs_soup.get_text()
                
                # グレード情報（簡単なパターンマッチング）
                # "500e Standard RRP £29,985" のような形式
                trim_matches = re.findall(r'([A-Za-z]+(?:\s+[A-Za-z]+)?)\s+RRP\s*£([\d,]+)', specs_text)
                for trim_name, price in trim_matches:
                    # 明らかにトリム名と思われるものだけ
                    if trim_name in ['Standard', 'Turismo', 'Scorpionissima', 'Sport', 'Premium', 'Base']:
                        grades.append({
                            'grade': trim_name,
                            'engine': '',
                            'price_min_gbp': int(price.replace(',', ''))
                        })
                
                # スペック情報（簡単な抽出）
                if 'Number of doors' in specs_text:
                    doors_match = re.search(r'Number of doors\s*(\d+)', specs_text)
                    if doors_match:
                        specs['doors'] = doors_match.group(1)
                
                if 'Number of seats' in specs_text:
                    seats_match = re.search(r'Number of seats\s*(\d+)', specs_text)
                    if seats_match:
                        specs['seats'] = seats_match.group(1)
                
                if 'Transmission' in specs_text:
                    if 'Automatic' in specs_text:
                        specs['transmission'] = 'Automatic'
                    elif 'Manual' in specs_text:
                        specs['transmission'] = 'Manual'
                
                # ボディタイプ（キーワードベース）
                full_text = text.lower() + specs_text.lower()
                if 'electric' in full_text:
                    body_types.append('Electric')
                if 'hatchback' in full_text:
                    body_types.append('Hatchback')
                if 'suv' in full_text:
                    body_types.append('SUV')
                if 'convertible' in full_text or 'cabrio' in full_text:
                    body_types.append('Convertible')
                
        except:
            pass
        
        # グレードがない場合はデフォルト
        if not grades:
            grades = [{'grade': '-', 'engine': '-', 'price_min_gbp': prices.get('price_min_gbp')}]
        
        return {
            'slug': slug,
            'make_en': make_en,
            'model_en': model_en,
            'overview_en': overview_en,
            'prices': prices,
            'grades': grades,
            'specifications': specs,
            'colors': colors,
            'media_urls': media_urls,
            'body_types': body_types,
            'catalog_url': main_url
        }
    
    def get_all_makers(self) -> List[str]:
        """brandsページからメーカー一覧を取得"""
        makers = []
        
        try:
            # brandsページから取得
            resp = requests.get(f"{BASE_URL}/brands", headers=HEADERS, timeout=30)
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, 'lxml')
                
                # メーカーへのリンクを探す
                for link in soup.find_all('a', href=True):
                    href = link['href']
                    # /audi, /bmw のような単純なパスのリンク
                    if href.startswith('/') and href.count('/') == 1 and len(href) > 2:
                        maker = href[1:]
                        # 除外リスト
                        if not any(x in maker for x in ['brands', 'news', 'reviews', 'deals', 'finance', 'lease']):
                            if maker not in makers:
                                makers.append(maker)
        except:
            pass
        
        # 取得できなかった場合は既知のリスト
        if not makers:
            makers = [
                'abarth', 'alfa-romeo', 'alpine', 'aston-martin', 'audi',
                'bentley', 'bmw', 'byd', 'citroen', 'cupra', 'dacia', 'ds',
                'fiat', 'ford', 'genesis', 'honda', 'hyundai', 'jaguar',
                'jeep', 'kia', 'land-rover', 'lexus', 'lotus', 'mazda',
                'mercedes-benz', 'mg', 'mini', 'nissan', 'peugeot', 'polestar',
                'porsche', 'renault', 'seat', 'skoda', 'smart', 'subaru',
                'suzuki', 'tesla', 'toyota', 'vauxhall', 'volkswagen', 'volvo'
            ]
        
        return sorted(makers)
    
    def get_models_for_maker(self, maker: str) -> List[str]:
        """メーカーのモデル一覧を取得"""
        models = []
        
        try:
            resp = requests.get(f"{BASE_URL}/{maker}", headers=HEADERS, timeout=30)
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, 'lxml')
                
                # モデルへのリンクを探す
                # パターン: /maker/model
                pattern = re.compile(f'^/{re.escape(maker)}/([^/]+)$')
                
                for link in soup.find_all('a', href=True):
                    href = link['href']
                    match = pattern.match(href)
                    if match:
                        model = match.group(1)
                        # 除外パターン
                        exclude = ['review', 'reviews', 'price', 'prices', 'spec', 'specs', 
                                 'deals', 'used', 'lease', 'finance', 'colours', 'dimensions',
                                 'specifications', 'electric', 'hybrid', 'automatic', 'manual',
                                 'suv', 'estate', 'hatchback', 'saloon', 'convertible']
                        
                        if not any(ex in model.lower() for ex in exclude):
                            full_slug = f"{maker}/{model}"
                            if full_slug not in models:
                                models.append(full_slug)
        except Exception as e:
            print(f"    Error getting models for {maker}: {e}")
        
        return models
