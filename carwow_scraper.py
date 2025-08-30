#!/usr/bin/env python3
"""
carwow_scraper.py - body_type取得機能実装版
"""
import re
import json
import time
import random
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse, quote

import requests
from bs4 import BeautifulSoup

# ======================== Configuration ========================
BASE_URL = "https://www.carwow.co.uk"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
HEADERS = {"User-Agent": USER_AGENT}
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3
RETRY_DELAY = 2

# ボディタイプのマッピング
BODY_TYPE_MAPPING = {
    'suvs': 'SUV',
    'hatchbacks': 'Hatchback',
    'saloons': 'Saloon',
    'coupes': 'Coupe',
    'estate-cars': 'Estate',
    'people-carriers': 'MPV',
    'sports-cars': 'Sports',
    'convertibles': 'Convertible'
}

# 除外するURLセグメント
EXCLUDE_SEGMENTS = {
    "automatic", "manual", "lease", "used", "deals", "finance", 
    "reviews", "prices", "news", "hybrid", "electric", "suv", 
    "estate", "hatchback", "saloon", "coupe", "convertible", 
    "sports", "mpv", "people-carriers",
    "white", "black", "silver", "grey", "gray", "red", "blue", 
    "green", "yellow", "orange", "brown", "purple", "pink", 
    "gold", "bronze", "beige", "cream", "multi-colour", "two-tone"
}

# ======================== Body Type Cache Manager ========================
class BodyTypeCache:
    """ボディタイプ情報のキャッシュ管理"""
    
    def __init__(self, cache_file: str = "body_type_cache.json"):
        self.cache_file = cache_file
        self.cache = self._load_cache()
        self.updated = False
    
    def _load_cache(self) -> Dict:
        """キャッシュファイルを読み込み"""
        try:
            with open(self.cache_file, 'r') as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {}
    
    def save(self):
        """キャッシュをファイルに保存"""
        if self.updated:
            try:
                with open(self.cache_file, 'w') as f:
                    json.dump(self.cache, f, indent=2)
            except Exception as e:
                print(f"Warning: Failed to save cache: {e}")
    
    def get(self, model_key: str) -> Optional[List[str]]:
        """モデルのボディタイプを取得"""
        return self.cache.get(model_key)
    
    def set(self, model_key: str, body_types: List[str]):
        """モデルのボディタイプを設定"""
        self.cache[model_key] = body_types
        self.updated = True

# ======================== HTTP Utilities ========================
class HTTPClient:
    """HTTP通信を管理するクライアント"""
    
    @staticmethod
    def get(url: str, allow_redirects: bool = True) -> requests.Response:
        """リトライ機能付きHTTPリクエスト"""
        for attempt in range(MAX_RETRIES):
            try:
                response = requests.get(
                    url, 
                    headers=HEADERS, 
                    timeout=REQUEST_TIMEOUT,
                    allow_redirects=allow_redirects
                )
                if response.status_code == 200:
                    return response
                elif response.status_code == 404:
                    raise requests.HTTPError(f"404 Not Found: {url}")
                else:
                    time.sleep(RETRY_DELAY * (attempt + 1))
            except requests.RequestException as e:
                if attempt == MAX_RETRIES - 1:
                    raise
                time.sleep(RETRY_DELAY * (attempt + 1))
        
        raise requests.HTTPError(f"Failed after {MAX_RETRIES} attempts: {url}")
    
    @staticmethod
    def get_soup(url: str) -> BeautifulSoup:
        """HTMLを取得してBeautifulSoupオブジェクトを返す"""
        response = HTTPClient.get(url)
        return BeautifulSoup(response.text, 'lxml')

# ======================== Body Type Discovery ========================
class BodyTypeDiscovery:
    """ボディタイプの自動発見"""
    
    def __init__(self):
        self.client = HTTPClient()
        self.cache = BodyTypeCache()
    
    def get_body_types_for_model(self, make: str, model: str) -> List[str]:
        """特定モデルのボディタイプを取得"""
        # キャッシュキー生成
        cache_key = f"{make}/{model}".lower()
        
        # キャッシュチェック
        cached = self.cache.get(cache_key)
        if cached is not None:
            return cached
        
        body_types = []
        
        # 各ボディタイプページをチェック
        for body_type_param, body_type_name in BODY_TYPE_MAPPING.items():
            try:
                if self._check_model_in_body_type(make, model, body_type_param):
                    body_types.append(body_type_name)
                    print(f"    Found {model} in {body_type_name}")
                
                # レート制限対策
                time.sleep(0.3)
                
            except Exception as e:
                print(f"    Warning: Failed to check {body_type_name}: {e}")
        
        # キャッシュに保存
        self.cache.set(cache_key, body_types)
        
        return body_types if body_types else []
    
    def _check_model_in_body_type(self, make: str, model: str, body_type_param: str) -> bool:
        """特定のボディタイプページにモデルが存在するかチェック"""
        page = 1
        max_pages = 10  # 最大ページ数の制限
        
        while page <= max_pages:
            url = f"{BASE_URL}/car-chooser?vehicle_body_type%5B%5D={body_type_param}"
            if page > 1:
                url += f"&page={page}"
            
            try:
                soup = self.client.get_soup(url)
                
                # モデル名を探す
                model_cards = soup.select('h3.card-compact__title')
                
                for card in model_cards:
                    card_text = card.get_text(strip=True).lower()
                    
                    # メーカー名とモデル名の組み合わせでチェック
                    full_name = f"{make} {model}".lower()
                    if full_name in card_text or model.lower() in card_text:
                        return True
                
                # 次のページへのリンクがあるかチェック
                next_page_links = soup.select(f'a[href*="page={page+1}"][href*="{body_type_param}"]')
                if not next_page_links:
                    break
                
                page += 1
                
            except Exception:
                break
        
        return False
    
    def save_cache(self):
        """キャッシュを保存"""
        self.cache.save()

# ======================== Vehicle Data Scraper ========================
class VehicleScraper:
    """個別車両ページのデータを取得"""
    
    def __init__(self):
        self.client = HTTPClient()
        self.body_type_discovery = BodyTypeDiscovery()
    
    def scrape_vehicle(self, slug: str) -> Dict:
        """車両データを取得（改善版）"""
        url = f"{BASE_URL}/{slug}"
        
        try:
            # メインページ取得
            soup = self.client.get_soup(url)
            
            # リダイレクトチェック
            if self._is_redirect_or_list_page(soup):
                raise ValueError(f"Not a valid model page: {slug}")
            
            # 基本データ取得
            make = slug.split('/')[0]
            model_slug = slug.split('/')[1] if '/' in slug else ''
            
            # __NEXT_DATA__から取得
            next_data = self._extract_next_data(soup)
            product = next_data.get('props', {}).get('pageProps', {}).get('product', {})
            
            # タイトルとモデル名
            title = self._extract_title(soup, product)
            model = self._extract_model_name(title, make)
            
            data = {
                'slug': slug,
                'url': url,
                'make': make,
                'model': model,
                'title': title,
                'overview': self._extract_overview(soup, product)
            }
            
            # 価格情報
            data.update(self._extract_prices(soup, product))
            
            # スペック情報
            specs = self._extract_specs(soup, product)
            data.update(specs)
            
            # 追加のspecificationsページから取得
            detailed_specs = self._scrape_specifications(slug)
            data.update(detailed_specs)
            
            # doors/seatsの確実な取得
            if not data.get('doors') and 'specifications' in detailed_specs:
                spec_data = detailed_specs['specifications']
                for key in ['number of doors', 'doors', 'no. of doors']:
                    if key in spec_data:
                        data['doors'] = self._extract_number(spec_data[key])
                        if data['doors']:
                            break
            
            if not data.get('seats') and 'specifications' in detailed_specs:
                spec_data = detailed_specs['specifications']
                for key in ['number of seats', 'seats', 'no. of seats', 'seating capacity']:
                    if key in spec_data:
                        data['seats'] = self._extract_number(spec_data[key])
                        if data['seats']:
                            break
            
            # メディア
            data['images'] = self._extract_images(soup, product)
            
            # カラー
            data['colors'] = self._scrape_colors(slug)
            
            # ボディタイプ（改善版）
            print(f"  Checking body types for {model}...")
            data['body_types'] = self.body_type_discovery.get_body_types_for_model(make, model_slug)
            
            # トリム情報を取得
            data['trims'] = self._scrape_trims_improved(slug)
            
            return data
            
        except Exception as e:
            raise Exception(f"Failed to scrape {slug}: {str(e)}")
        finally:
            # キャッシュを保存
            self.body_type_discovery.save_cache()
    
    def _is_redirect_or_list_page(self, soup: BeautifulSoup) -> bool:
        """リストページや無効なページかチェック"""
        title = soup.find('title')
        if title and 'review' not in title.text.lower():
            if not soup.select_one('div.review-overview, div.model-hub'):
                return True
        
        if soup.select_one('div.filter-panel, div.listing-grid'):
            return True
        
        return False
    
    def _extract_next_data(self, soup: BeautifulSoup) -> Dict:
        """__NEXT_DATA__を抽出"""
        script = soup.find('script', id='__NEXT_DATA__')
        if script and script.string:
            try:
                return json.loads(script.string)
            except json.JSONDecodeError:
                pass
        return {}
    
    def _extract_title(self, soup: BeautifulSoup, product: Dict) -> str:
        """タイトルを取得"""
        if h1 := soup.find('h1', class_='header__title'):
            title = h1.get_text(strip=True)
            title = re.sub(r'\s*(Review|Prices?|&).*$', '', title, flags=re.IGNORECASE)
            return title.strip()
        
        if h1 := soup.find('h1'):
            title = h1.get_text(strip=True)
            title = re.sub(r'\s*(review).*$', '', title, flags=re.IGNORECASE)
            return title.strip()
        
        if title := product.get('name'):
            return title
        
        return ""
    
    def _extract_model_name(self, title: str, make: str) -> str:
        """タイトルからモデル名を抽出"""
        model = title
        
        make_variations = [
            make,
            make.replace('-', ' '),
            make.replace('-', ''),
            make.title(),
            make.upper()
        ]
        
        for variation in make_variations:
            if model.lower().startswith(variation.lower()):
                model = model[len(variation):].strip()
                break
        
        return model or title
    
    def _extract_overview(self, soup: BeautifulSoup, product: Dict) -> str:
        """概要文を取得"""
        if review := product.get('review', {}).get('intro'):
            if len(review) >= 30:
                return review.strip()
        
        if meta := soup.find('meta', {'name': 'description'}):
            content = meta.get('content', '').strip()
            if content and not content.startswith('Your account') and len(content) >= 50:
                return content
        
        return ""
    
    def _extract_prices(self, soup: BeautifulSoup, product: Dict) -> Dict:
        """価格情報を取得"""
        prices = {
            'price_min_gbp': None,
            'price_max_gbp': None,
            'price_used_gbp': None
        }
        
        prices['price_min_gbp'] = product.get('priceMin') or product.get('rrpMin')
        prices['price_max_gbp'] = product.get('priceMax') or product.get('rrpMax')
        
        if not prices['price_min_gbp']:
            patterns = [
                r'RRP\s*£([\d,]+)\s*-\s*£([\d,]+)',
                r'£([\d,]+)\s*-\s*£([\d,]+)',
                r'From\s*£([\d,]+)',
            ]
            
            page_text = soup.get_text()
            for pattern in patterns:
                if match := re.search(pattern, page_text):
                    if '-' in pattern:
                        prices['price_min_gbp'] = int(match.group(1).replace(',', ''))
                        prices['price_max_gbp'] = int(match.group(2).replace(',', ''))
                    else:
                        prices['price_min_gbp'] = int(match.group(1).replace(',', ''))
                    if prices['price_min_gbp']:
                        break
        
        return prices
    
    def _extract_specs(self, soup: BeautifulSoup, product: Dict) -> Dict:
        """基本スペックを取得"""
        specs = {
            'fuel_type': None,
            'doors': None,
            'seats': None,
            'transmission': None,
            'dimensions': None
        }
        
        specs['fuel_type'] = product.get('fuelType')
        specs['doors'] = product.get('numberOfDoors')
        specs['seats'] = product.get('numberOfSeats')
        specs['transmission'] = product.get('transmission')
        
        return specs
    
    def _scrape_trims_improved(self, slug: str) -> List[Dict]:
        """トリム情報を取得"""
        trims = []
        
        try:
            spec_url = f"{BASE_URL}/{slug}/specifications"
            response = self.client.get(spec_url, allow_redirects=True)
            
            # リダイレクトチェック
            final_url = response.url
            if f"/{slug.split('/')[0]}#" in final_url:
                return []  # トリム情報なし
            
            soup = BeautifulSoup(response.text, 'lxml')
            
            # トリム情報を抽出する処理（簡略版）
            # 実際のページ構造に合わせて調整が必要
            
        except Exception as e:
            print(f"  Warning: Failed to get trims for {slug}: {e}")
        
        return trims
    
    def _extract_images(self, soup: BeautifulSoup, product: Dict) -> List[str]:
        """画像URLを取得"""
        images = []
        seen = set()
        
        if hero := product.get('heroImage'):
            images.append(hero)
            seen.add(hero)
        
        for gallery in ['galleryImages', 'mediaGallery', 'images']:
            if items := product.get(gallery, []):
                for item in items:
                    if isinstance(item, str):
                        url = item
                    else:
                        url = item.get('url', item.get('src', ''))
                    
                    if url and url not in seen:
                        images.append(url)
                        seen.add(url)
                        if len(images) >= 40:
                            break
        
        return images[:40]
    
    def _scrape_colors(self, slug: str) -> List[str]:
        """カラーバリエーションを取得"""
        colors = []
        
        try:
            response = self.client.get(f"{BASE_URL}/{slug}/colours", allow_redirects=True)
            
            if f"/{slug.split('/')[0]}#" in response.url:
                return []
            
            soup = BeautifulSoup(response.text, 'lxml')
            
            for element in soup.select('h4.model-hub__colour-details-title, .colour-name, .color-option'):
                color = element.get_text(strip=True)
                color = re.sub(r'(Free|£[\d,]+).*$', '', color).strip()
                
                if color and color not in colors and len(color) < 50:
                    colors.append(color)
        
        except Exception:
            pass
        
        return colors
    
    def _scrape_specifications(self, slug: str) -> Dict:
        """詳細スペックを取得（dimensions改善版）"""
        spec_data = {}
        
        try:
            url = f"{BASE_URL}/{slug}/specifications"
            response = self.client.get(url, allow_redirects=True)
            
            final_url = response.url
            if f"/{slug.split('/')[0]}#" in final_url:
                return {'specifications': {}}
            
            soup = BeautifulSoup(response.text, 'lxml')
            
            # テーブルから取得
            for table in soup.select('table'):
                for row in table.select('tr'):
                    cells = row.select('th, td')
                    if len(cells) >= 2:
                        key = cells[0].get_text(strip=True).lower()
                        value = cells[1].get_text(strip=True)
                        spec_data[key] = value
            
            # 寸法情報を構造化
            dimensions = self._extract_dimensions_from_spec(spec_data, soup.get_text())
            if dimensions:
                spec_data['dimensions_structured'] = dimensions
            
        except Exception as e:
            print(f"  Warning: Failed to get specifications for {slug}: {e}")
        
        return {'specifications': spec_data}
    
    def _extract_dimensions_from_spec(self, spec_data: Dict, page_text: str) -> Optional[str]:
        """寸法情報を抽出"""
        dimensions = {'length': None, 'width': None, 'height': None}
        
        LENGTH_KEYS = [
            'length', 'overall length', 'length (mm)', 'overall length (mm)',
            'length mm', 'exterior length', 'body length', 'total length'
        ]
        WIDTH_KEYS = [
            'width', 'overall width', 'width (mm)', 'overall width (mm)',
            'width mm', 'exterior width', 'body width', 'total width'
        ]
        HEIGHT_KEYS = [
            'height', 'overall height', 'height (mm)', 'overall height (mm)',
            'height mm', 'exterior height', 'body height', 'total height'
        ]
        
        for key, value in spec_data.items():
            key_lower = key.lower().strip()
            
            if not dimensions['length']:
                for lk in LENGTH_KEYS:
                    if lk in key_lower:
                        if match := re.search(r'(\d{3,4})', str(value).replace(',', '')):
                            num = int(match.group(1))
                            if 2000 <= num <= 6000:
                                dimensions['length'] = str(num)
                                break
            
            if not dimensions['width']:
                for wk in WIDTH_KEYS:
                    if wk in key_lower:
                        if match := re.search(r'(\d{3,4})', str(value).replace(',', '')):
                            num = int(match.group(1))
                            if 1500 <= num <= 2500:
                                dimensions['width'] = str(num)
                                break
            
            if not dimensions['height']:
                for hk in HEIGHT_KEYS:
                    if hk in key_lower:
                        if match := re.search(r'(\d{3,4})', str(value).replace(',', '')):
                            num = int(match.group(1))
                            if 1000 <= num <= 2500:
                                dimensions['height'] = str(num)
                                break
        
        if all(dimensions.values()):
            return f"{dimensions['length']} x {dimensions['width']} x {dimensions['height']} mm"
        elif dimensions['length'] and dimensions['width']:
            return f"{dimensions['length']} x {dimensions['width']} mm"
        
        return None
    
    def _extract_number(self, text: str) -> Optional[int]:
        """テキストから数値を抽出"""
        if text is None:
            return None
        if match := re.search(r'\d+', str(text)):
            return int(match.group())
        return None

# 残りのクラス（MakerDiscovery, ModelDiscovery, CarwowScraper）は元のコードと同じ
