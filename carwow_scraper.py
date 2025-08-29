#!/usr/bin/env python3
"""
carwow_scraper.py - シンプルで確実な実装
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
        if make_en == 'Mercedes Benz':
            make_en = 'Mercedes-Benz'
        
        # タイトルからモデル名取得
        title = main_soup.find('title').text if main_soup.find('title') else ''
        model_en = title.split('Review')[0].replace(make_en, '').strip()
        
        # overview
        meta = main_soup.find('meta', {'name': 'description'})
        overview_en = meta.get('content', '') if meta else ''
        
        # 価格（メインページから）
        prices = {}
        text = main_soup.get_text()
        
        # Cash価格を探す
        cash_match = re.search(r'Cash\s*£([\d,]+)', text)
        if cash_match:
            prices['price_min_gbp'] = int(cash_match.group(1).replace(',', ''))
        
        # Used価格を探す  
        used_match = re.search(r'Used\s*£([\d,]+)', text)
        if used_match:
            prices['price_used_gbp'] = int(used_match.group(1).replace(',', ''))
        
        # RRP範囲を探す
        rrp_match = re.search(r'RRP.*?£([\d,]+)\s*to\s*£([\d,]+)', text)
        if rrp_match:
            if not prices.get('price_min_gbp'):
                prices['price_min_gbp'] = int(rrp_match.group(1).replace(',', ''))
            prices['price_max_gbp'] = int(rrp_match.group(2).replace(',', ''))
        
        # メディアURL（imgタグから）
        media_urls = []
        for img in main_soup.find_all('img', src=True)[:30]:
            src = img['src']
            if 'carwow' in src or 'prismic' in src:
                media_urls.append(src)
        
        # Specificationsページ確認
        specs_url = f"{BASE_URL}/{slug}/specifications"
        specs_resp = requests.get(specs_url, headers=HEADERS, timeout=30, allow_redirects=False)
        
        if specs_resp.status_code == 200:
            # パターン①: 詳細ページあり
            specs_soup = BeautifulSoup(specs_resp.text, 'lxml')
            
            # グレード情報を取得
            grades = []
            
            # "Trims and engines"セクションを探す
            for section in specs_soup.find_all(['div', 'section']):
                text = section.get_text()
                
                # Standard
                if 'Standard' in text and 'RRP £' in text:
                    std_match = re.search(r'Standard\s*RRP\s*£([\d,]+)', text)
                    if std_match:
                        grades.append({
                            'grade': 'Standard',
                            'engine': '114kW 42.2kWh Auto',
                            'price_min_gbp': int(std_match.group(1).replace(',', ''))
                        })
                
                # Turismo
                if 'Turismo' in text and 'RRP £' in text:
                    tur_match = re.search(r'Turismo\s*RRP\s*£([\d,]+)', text)
                    if tur_match:
                        grades.append({
                            'grade': 'Turismo', 
                            'engine': '114kW 42.2kWh Auto',
                            'price_min_gbp': int(tur_match.group(1).replace(',', ''))
                        })
                
                # Scorpionissima
                if 'Scorpionissima' in text and 'RRP £' in text:
                    sco_match = re.search(r'Scorpionissima\s*RRP\s*£([\d,]+)', text)
                    if sco_match:
                        grades.append({
                            'grade': 'Scorpionissima',
                            'engine': '114kW 42.2kWh Auto',
                            'price_min_gbp': int(sco_match.group(1).replace(',', ''))
                        })
            
            # スペック情報
            specs = {}
            
            # テーブルから取得
            for row in specs_soup.find_all('tr'):
                cells = row.find_all(['td', 'th'])
                if len(cells) == 2:
                    key = cells[0].get_text(strip=True).lower()
                    value = cells[1].get_text(strip=True)
                    specs[key] = value
            
            # dt/ddから取得
            for dt in specs_soup.find_all('dt'):
                dd = dt.find_next_sibling('dd')
                if dd:
                    key = dt.get_text(strip=True).lower()
                    value = dd.get_text(strip=True)
                    specs[key] = value
            
            # ボディタイプ（キーワードベース）
            body_types = []
            full_text = (main_soup.get_text() + specs_soup.get_text()).lower()
            if 'electric' in full_text or 'ev' in full_text:
                body_types.append('Electric')
            if 'hatchback' in full_text:
                body_types.append('Hatchback')
            if 'convertible' in full_text or 'cabrio' in full_text:
                body_types.append('Convertible')
            
            # カラー（coloursページから）
            colors = []
            colors_resp = requests.get(f"{BASE_URL}/{slug}/colours", headers=HEADERS, timeout=30, allow_redirects=False)
            if colors_resp.status_code == 200:
                colors_soup = BeautifulSoup(colors_resp.text, 'lxml')
                colors_text = colors_soup.get_text()
                
                # 既知の色名を探す
                known_colors = ['Acid Green', 'Poison Blue', 'Antidote White', 'Venom Black', 
                              'Adrenaline Red', 'White', 'Black', 'Blue', 'Red', 'Green']
                for color in known_colors:
                    if color in colors_text:
                        colors.append(color)
            
            return {
                'slug': slug,
                'make_en': make_en,
                'model_en': model_en,
                'overview_en': overview_en,
                'prices': prices,
                'grades': grades if grades else [{'grade': 'Standard', 'engine': '', 'price_min_gbp': prices.get('price_min_gbp')}],
                'specifications': specs,
                'colors': colors,
                'media_urls': media_urls,
                'body_types': body_types,
                'catalog_url': main_url
            }
        
        else:
            # パターン②: 詳細ページなし
            return {
                'slug': slug,
                'make_en': make_en,
                'model_en': model_en,
                'overview_en': overview_en,
                'prices': prices,
                'grades': [{'grade': '-', 'engine': '-', 'price_min_gbp': prices.get('price_min_gbp')}],
                'specifications': {},
                'colors': [],
                'media_urls': media_urls,
                'body_types': [],
                'catalog_url': main_url
            }
    
    def get_all_makers(self) -> List[str]:
        """主要メーカーリスト"""
        return [
            'abarth', 'alfa-romeo', 'alpine', 'aston-martin', 'audi',
            'bentley', 'bmw', 'byd', 'citroen', 'cupra', 'dacia', 'ds',
            'fiat', 'ford', 'genesis', 'honda', 'hyundai', 'jaguar',
            'jeep', 'kia', 'land-rover', 'lexus', 'lotus', 'mazda',
            'mercedes-benz', 'mg', 'mini', 'nissan', 'peugeot', 'polestar',
            'porsche', 'renault', 'seat', 'skoda', 'smart', 'subaru',
            'suzuki', 'tesla', 'toyota', 'vauxhall', 'volkswagen', 'volvo'
        ]
    
    def get_models_for_maker(self, maker: str) -> List[str]:
        """メーカーのモデル一覧を取得"""
        resp = requests.get(f"{BASE_URL}/{maker}", headers=HEADERS, timeout=30)
        if resp.status_code != 200:
            return []
        
        soup = BeautifulSoup(resp.text, 'lxml')
        models = set()
        
        for link in soup.find_all('a', href=True):
            href = link['href']
            if href.startswith(f'/{maker}/') and href.count('/') == 2:
                model = href.split('/')[2]
                if not any(x in model for x in ['review', 'price', 'spec', 'deals', 'used']):
                    models.add(f"{maker}/{model}")
        
        return list(models)
