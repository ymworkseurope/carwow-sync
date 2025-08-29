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
        
        main_url = f"{BASE_URL}/{slug}"
        main_resp = requests.get(main_url, headers=HEADERS, timeout=30)
        if main_resp.status_code != 200:
            return None
        
        main_soup = BeautifulSoup(main_resp.text, 'lxml')
        
        # 基本情報
        make_en = slug.split('/')[0].replace('-', ' ').title()
        make_map = {
            'Mercedes Benz': 'Mercedes-Benz',
            'Alfa Romeo': 'Alfa Romeo',
            'Land Rover': 'Land Rover',
            'Aston Martin': 'Aston Martin'
        }
        make_en = make_map.get(make_en, make_en)
        
        # モデル名
        title = main_soup.find('title')
        model_en = ''
        if title:
            title_text = title.text
            model_en = title_text.split('Review')[0].split('|')[0].replace(make_en, '').strip()
        
        # overview
        meta = main_soup.find('meta', {'name': 'description'})
        overview_en = meta.get('content', '') if meta else ''
        
        # 価格
        prices = {}
        text = main_soup.get_text()
        
        cash_match = re.search(r'Cash\s*£([\d,]+)', text)
        if cash_match:
            prices['price_min_gbp'] = int(cash_match.group(1).replace(',', ''))
        
        used_match = re.search(r'Used\s*£([\d,]+)', text)
        if used_match:
            prices['price_used_gbp'] = int(used_match.group(1).replace(',', ''))
        
        rrp_match = re.search(r'RRP.*?£([\d,]+)\s*to\s*£([\d,]+)', text)
        if rrp_match:
            if not prices.get('price_min_gbp'):
                prices['price_min_gbp'] = int(rrp_match.group(1).replace(',', ''))
            prices['price_max_gbp'] = int(rrp_match.group(2).replace(',', ''))
        
        # メディアURL
        media_urls = []
        for img in main_soup.find_all('img', src=True)[:20]:
            src = img['src']
            if 'carwow' in src or 'prismic' in src:
                media_urls.append(src)
        
        # Specificationsページ
        grades = []
        specs = {}
        colors = []
        body_types = []
        
        specs_url = f"{BASE_URL}/{slug}/specifications"
        try:
            specs_resp = requests.get(specs_url, headers=HEADERS, timeout=30, allow_redirects=False)
            
            if specs_resp.status_code == 200:
                specs_soup = BeautifulSoup(specs_resp.text, 'lxml')
                specs_text = specs_soup.get_text()
                
                # グレード情報
                trim_matches = re.findall(r'([A-Za-z]+(?:\s+[A-Za-z]+)?)\s+RRP\s*£([\d,]+)', specs_text)
                for trim_name, price in trim_matches:
                    if trim_name in ['Standard', 'Turismo', 'Scorpionissima', 'Sport', 'Premium', 'Base']:
                        grades.append({
                            'grade': trim_name,
                            'engine': '',
                            'price_min_gbp': int(price.replace(',', ''))
                        })
                
                # スペック
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
                
                # ボディタイプ
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
            resp = requests.get(f"{BASE_URL}/brands", headers=HEADERS, timeout=30)
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, 'lxml')
                
                # brands-list__group-item-title-name クラスからブランド名を取得
                for brand_div in soup.find_all('div', class_='brands-list__group-item-title-name'):
                    brand_name = brand_div.get_text(strip=True).lower()
                    brand_slug = brand_name.replace(' ', '-')
                    if brand_slug and brand_slug not in makers:
                        makers.append(brand_slug)
                
                # フォールバック: リンクから取得
                if not makers:
                    for link in soup.find_all('a', href=True):
                        href = link['href']
                        if href.startswith('/') and href.count('/') == 1:
                            maker = href[1:]
                            if maker and not any(x in maker for x in ['brands', 'news', 'reviews']):
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
        """メーカーページからモデル一覧を取得"""
        models = []
        seen = set()
        
        try:
            url = f"{BASE_URL}/{maker}"
            resp = requests.get(url, headers=HEADERS, timeout=30)
            
            # デバッグ: ステータスコード確認
            if resp.status_code != 200:
                print(f"    Error: Got status code {resp.status_code} for {url}")
                return models
            
            soup = BeautifulSoup(resp.text, 'lxml')
            
            # デバッグ: ページタイトル確認
            title = soup.find('title')
            if title:
                print(f"    Page title: {title.text[:50]}...")
            
            # 方法1: article.card-compactから取得
            articles = soup.find_all('article', class_='card-compact')
            print(f"    Found {len(articles)} article.card-compact elements")
            
            for article in articles:
                # h3タグから車種名を取得
                h3 = article.find('h3', class_='card-compact__title')
                if h3:
                    model_name = h3.get_text(strip=True)
                    print(f"      Found model name: {model_name}")
                
                # リンクを探す
                for link in article.find_all('a', href=True):
                    href = link['href']
                    # URLからモデルを抽出
                    if f'/{maker}/' in href:
                        # URLパースしてモデル部分を取得
                        if 'carwow.co.uk' in href:
                            # https://www.carwow.co.uk/abarth/500e から abarth/500e を抽出
                            parts = href.split('carwow.co.uk/')[-1].split('?')[0].split('#')[0].split('/')
                        else:
                            # 相対URL
                            parts = href.strip('/').split('?')[0].split('#')[0].split('/')
                        
                        if len(parts) >= 2 and parts[0] == maker:
                            model_slug = f"{parts[0]}/{parts[1]}"
                            if model_slug not in seen:
                                print(f"      Added: {model_slug}")
                                models.append(model_slug)
                                seen.add(model_slug)
                                break  # 同じarticle内の重複リンクを避ける
            
            # 方法2: articleで見つからない場合、すべてのaタグから取得
            if not models:
                print("    No models found in articles, checking all links...")
                all_links = soup.find_all('a', href=True)
                print(f"    Found {len(all_links)} total links")
                
                for link in all_links:
                    href = link['href']
                    # review リンクを探す
                    if f'/{maker}/' in href and 'review' in link.get_text('').lower():
                        if 'carwow.co.uk' in href:
                            parts = href.split('carwow.co.uk/')[-1].split('?')[0].split('#')[0].split('/')
                        else:
                            parts = href.strip('/').split('?')[0].split('#')[0].split('/')
                        
                        if len(parts) >= 2 and parts[0] == maker:
                            model_slug = f"{parts[0]}/{parts[1]}"
                            if model_slug not in seen:
                                print(f"      Added from link: {model_slug}")
                                models.append(model_slug)
                                seen.add(model_slug)
                                
        except Exception as e:
            print(f"    Exception in get_models_for_maker: {e}")
            import traceback
            traceback.print_exc()
        
        print(f"    Total models found: {len(models)}")
        return models
