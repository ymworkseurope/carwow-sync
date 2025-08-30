#!/usr/bin/env python3
"""
carwow_scraper.py
データ取得に特化したメインスクレイピングモジュール（body_type取得機能実装版）
"""
import re
import json
import time
import random
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse

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

# 除外するURLセグメント（モデルページではない）
EXCLUDE_SEGMENTS = {
    # カテゴリ・フィルタ
    "automatic", "manual", "lease", "used", "deals", "finance", 
    "reviews", "prices", "news", "hybrid", "electric", "suv", 
    "estate", "hatchback", "saloon", "coupe", "convertible", 
    "sports", "mpv", "people-carriers",
    
    # カラー名
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
                    full_name = f"{make} {model}".lower().replace('-', ' ')
                    model_lower = model.lower().replace('-', ' ')
                    
                    if full_name in card_text or model_lower in card_text:
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

# ======================== Makers Discovery ========================
class MakerDiscovery:
    """メーカー一覧を自動発見"""
    
    @staticmethod
    def get_all_makers() -> List[str]:
        """全メーカーのスラッグを取得"""
        makers = set()
        
        # /brands ページから取得
        try:
            soup = HTTPClient.get_soup(f"{BASE_URL}/brands")
            for link in soup.select('a[href*="/brands/"]'):
                href = link.get('href', '')
                if match := re.search(r'/brands/([a-z-]+)', href):
                    maker = match.group(1)
                    if maker not in EXCLUDE_SEGMENTS:
                        makers.add(maker)
        except Exception as e:
            print(f"Warning: Failed to fetch brands page: {e}")
        
        # トップページからも補完
        try:
            soup = HTTPClient.get_soup(BASE_URL)
            for link in soup.select('a[href^="/"][href*="/"]'):
                href = link.get('href', '')
                parts = href.strip('/').split('/')
                if len(parts) >= 1:
                    maker = parts[0]
                    if (maker not in EXCLUDE_SEGMENTS and 
                        re.match(r'^[a-z-]+$', maker) and 
                        len(maker) > 2):
                        makers.add(maker)
        except Exception:
            pass
        
        # 既知の主要メーカーを確実に含める
        known_makers = {
            "abarth", "alfa-romeo", "alpine", "aston-martin", "audi",
            "bentley", "bmw", "byd", "citroen", "cupra", "dacia", "ds",
            "fiat", "ford", "genesis", "honda", "hyundai", "jaguar",
            "jeep", "kia", "land-rover", "lexus", "lotus", "mazda",
            "mercedes-benz", "mg", "mini", "nissan", "peugeot", "polestar",
            "porsche", "renault", "seat", "skoda", "smart", "subaru",
            "suzuki", "tesla", "toyota", "vauxhall", "volkswagen", "volvo"
        }
        makers.update(known_makers)
        
        return sorted(list(makers))

# ======================== Model Discovery ========================
class ModelDiscovery:
    """メーカーごとのモデル一覧を発見"""
    
    @staticmethod
    def get_models_for_maker(maker: str) -> List[str]:
        """指定メーカーの全モデルスラッグを取得"""
        models = set()
        
        try:
            soup = HTTPClient.get_soup(f"{BASE_URL}/{maker}")
            
            # __NEXT_DATA__から取得
            models.update(ModelDiscovery._extract_from_next_data(soup, maker))
            
            # リンクから取得
            models.update(ModelDiscovery._extract_from_links(soup, maker))
            
        except Exception as e:
            print(f"Warning: Failed to get models for {maker}: {e}")
        
        # 除外処理
        valid_models = []
        for model in models:
            if not any(exc in model for exc in EXCLUDE_SEGMENTS):
                valid_models.append(f"{maker}/{model}")
        
        return sorted(valid_models)
    
    @staticmethod
    def _extract_from_next_data(soup: BeautifulSoup, maker: str) -> Set[str]:
        """__NEXT_DATA__からモデルを抽出"""
        models = set()
        script = soup.find('script', id='__NEXT_DATA__')
        
        if script and script.string:
            try:
                data = json.loads(script.string)
                
                # productCardListから取得
                product_list = (data.get('props', {})
                              .get('pageProps', {})
                              .get('collection', {})
                              .get('productCardList', []))
                
                for product in product_list:
                    if url := product.get('url'):
                        if match := re.search(f'/{maker}/([^/?]+)', url):
                            models.add(match.group(1))
                
                # 他の可能な場所も探索
                page_props = data.get('props', {}).get('pageProps', {})
                if 'models' in page_props:
                    for model in page_props['models']:
                        if slug := model.get('slug'):
                            models.add(slug)
                            
            except (json.JSONDecodeError, KeyError):
                pass
        
        return models
    
    @staticmethod
    def _extract_from_links(soup: BeautifulSoup, maker: str) -> Set[str]:
        """<a>タグからモデルを抽出"""
        models = set()
        
        for link in soup.select(f'a[href*="/{maker}/"]'):
            href = link.get('href', '')
            if match := re.search(f'/{maker}/([^/?#]+)', href):
                model = match.group(1)
                # /review を除去
                model = model.replace('/review', '')
                if model and len(model) > 1:
                    models.add(model)
        
        return models

# ======================== Vehicle Data Scraper ========================
class VehicleScraper:
    """個別車両ページのデータを取得"""
    
    def __init__(self):
        self.client = HTTPClient()
        self.body_type_discovery = BodyTypeDiscovery()
    
    def scrape_vehicle(self, slug: str) -> Dict:
        """車両データを取得（トリム情報を含む）"""
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
            
            # スペック情報（doors/seats含む）
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
            
            # トランスミッション情報を確実に取得
            if not data.get('transmission'):
                if spec_json := detailed_specs.get('specifications', {}):
                    for key in ['transmission', 'gearbox', 'transmission type']:
                        if key in spec_json:
                            data['transmission'] = spec_json[key]
                            break
            
            # メディア
            data['images'] = self._extract_images(soup, product)
            
            # カラー
            data['colors'] = self._scrape_colors(slug)
            
            # ボディタイプ（改善版）
            print(f"  Checking body types for {model_slug}...")
            data['body_types'] = self.body_type_discovery.get_body_types_for_model(make, model_slug)
            
            # トリム情報を取得（改善版）
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
        """タイトルを取得（h1から正確に）"""
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
        
        if og_meta := soup.find('meta', {'property': 'og:description'}):
            content = og_meta.get('content', '').strip()
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
                r'Cash\s*£([\d,]+)',
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
        
        if not prices['price_used_gbp']:
            used_patterns = [
                r'Used\s*£([\d,]+)',
                r'Used\s*from\s*£([\d,]+)',
                r'Pre-owned\s*£([\d,]+)',
            ]
            
            page_text = soup.get_text()
            for pattern in used_patterns:
                if match := re.search(pattern, page_text):
                    prices['price_used_gbp'] = int(match.group(1).replace(',', ''))
                    break
        
        return prices
    
    def _extract_specs(self, soup: BeautifulSoup, product: Dict) -> Dict:
        """基本スペックを取得（doors/seats含む）"""
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
        
        all_data = {}
        
        for dt in soup.select('dt'):
            if dd := dt.find_next_sibling('dd'):
                key = dt.get_text(strip=True).lower()
                value = dd.get_text(strip=True)
                all_data[key] = value
        
        for row in soup.select('table tr'):
            cells = row.select('th, td')
            if len(cells) >= 2:
                key = cells[0].get_text(strip=True).lower()
                value = cells[1].get_text(strip=True)
                all_data[key] = value
        
        if not specs['fuel_type']:
            for key in ['fuel', 'fuel type', 'fuel types', 'engine type']:
                if key in all_data:
                    specs['fuel_type'] = all_data[key]
                    break
        
        if not specs['doors']:
            for key in ['doors', 'number of doors', 'no. of doors']:
                if key in all_data:
                    specs['doors'] = self._extract_number(all_data[key])
                    break
        
        if not specs['seats']:
            for key in ['seats', 'number of seats', 'no. of seats']:
                if key in all_data:
                    specs['seats'] = self._extract_number(all_data[key])
                    break
        
        if not specs['transmission']:
            for key in ['transmission', 'gearbox', 'drivetrain']:
                if key in all_data:
                    specs['transmission'] = all_data[key]
                    break
        
        return specs
    
    def _scrape_trims_improved(self, slug: str) -> List[Dict]:
        """トリム情報を正確に取得"""
        trims = []
        
        try:
            spec_url = f"{BASE_URL}/{slug}/specifications"
            response = self.client.get(spec_url, allow_redirects=True)
            
            # リダイレクトチェック
            final_url = response.url
            if f"/{slug.split('/')[0]}#" in final_url:
                return []  # トリム情報なし
            
            soup = BeautifulSoup(response.text, 'lxml')
            
            # テキスト全体から正確なトリム名を抽出
            page_text = soup.get_text()
            
            # トリムセクションを探す
            trim_sections = []
            for elem in soup.find_all(['h2', 'h3', 'h4']):
                if 'Trims and engines' in elem.get_text():
                    # このセクション以降のトリム情報を取得
                    next_elem = elem.find_next_sibling()
                    while next_elem:
                        text = next_elem.get_text()
                        # トリム名のパターンマッチング
                        if any(keyword in text for keyword in ['RRP', '£', 'Standard', 'Sport', 'Premium', 'Executive']):
                            trim_sections.append(next_elem)
                        next_elem = next_elem.find_next_sibling()
                        if next_elem and next_elem.name in ['h2', 'h3']:
                            break
            
            # パターンマッチングでトリム情報を抽出
            trim_patterns = [
                r'([A-Za-z0-9\s\-\.]+)\s+RRP\s*£([\d,]+)',
                r'([A-Za-z0-9\s\-\.]+)\s+£([\d,]+)',
            ]
            
            for pattern in trim_patterns:
                matches = re.finditer(pattern, page_text)
                for match in matches:
                    trim_name = match.group(1).strip()
                    price = int(match.group(2).replace(',', ''))
                    
                    # 無効なトリム名をフィルタ
                    if len(trim_name) > 1 and not trim_name.isdigit():
                        trim_data = {
                            'trim_name': trim_name,
                            'engine': '',
                            'fuel_type': '',
                            'power_bhp': None,
                            'transmission': '',
                            'drive_type': '',
                            'price_rrp': price
                        }
                        trims.append(trim_data)
            
            print(f"  Found {len(trims)} trims")
            
        except Exception as e:
            print(f"  Warning: Failed to get trims for {slug}: {e}")
        
        return trims
    
    def _extract_images(self, soup: BeautifulSoup, product: Dict) -> List[str]:
        """画像URLを取得"""
        images = []
        seen = set()
        
        # productデータから
        if hero := product.get('heroImage'):
            images.append(hero)
            seen.add(hero)
        
        # ギャラリー画像
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
        
        # HTMLから補完
        for img in soup.select('img[src], img[data-src]'):
            src = img.get('src') or img.get('data-src', '')
            if self._is_valid_image_url(src) and src not in seen:
                images.append(src)
                seen.add(src)
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
                # 価格部分を除去
                color = re.sub(r'(Free|£[\d,]+).*$', '', color).strip()
                
                if color and color not in colors and len(color) < 50:
                    colors.append(color)
        
        except Exception as e:
            print(f"  Warning: Failed to get colors for {slug}: {e}")
        
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
            
            # dt/ddペアから取得
            for dt in soup.select('dt'):
                if dd := dt.find_next('dd'):
                    key = dt.get_text(strip=True).lower()
                    value = dd.get_text(strip=True)
                    spec_data[key] = value
            
            # セクションごとの処理（Dimensions and capacitiesなど）
            sections = soup.find_all(['h2', 'h3', 'h4'])
            for section in sections:
                section_text = section.get_text(strip=True).lower()
                if 'dimension' in section_text or 'capacity' in section_text or 'exterior' in section_text:
                    # このセクションの次の要素から寸法情報を取得
                    next_elem = section.find_next_sibling()
                    while next_elem and next_elem.name not in ['h2', 'h3', 'h4']:
                        if next_elem.name == 'table':
                            for row in next_elem.select('tr'):
                                cells = row.select('th, td')
                                if len(cells) >= 2:
                                    key = cells[0].get_text(strip=True).lower()
                                    value = cells[1].get_text(strip=True)
                                    spec_data[key] = value
                        elif next_elem.name == 'dl':
                            for dt in next_elem.select('dt'):
                                if dd := dt.find_next_sibling('dd'):
                                    key = dt.get_text(strip=True).lower()
                                    value = dd.get_text(strip=True)
                                    spec_data[key] = value
                        next_elem = next_elem.find_next_sibling()
            
            # テキスト全体から寸法をパターンマッチで取得（フォールバック）
            page_text = soup.get_text()
            
            # Length/Width/Height パターン
            dimensions_patterns = [
                (r'Length[:\s]+(\d{1,2}[,.]?\d{3,4})\s*mm', 'length'),
                (r'Width[:\s]+(\d{1,2}[,.]?\d{3,4})\s*mm', 'width'),
                (r'Height[:\s]+(\d{1,2}[,.]?\d{3,4})\s*mm', 'height'),
                (r'Wheelbase[:\s]+(\d{1,2}[,.]?\d{3,4})\s*mm', 'wheelbase'),
                (r'Overall length[:\s]+(\d{1,2}[,.]?\d{3,4})\s*mm', 'overall length'),
                (r'Overall width[:\s]+(\d{1,2}[,.]?\d{3,4})\s*mm', 'overall width'),
                (r'Overall height[:\s]+(\d{1,2}[,.]?\d{3,4})\s*mm', 'overall height'),
            ]
            
            for pattern, key in dimensions_patterns:
                match = re.search(pattern, page_text, re.IGNORECASE)
                if match:
                    value = match.group(1).replace(',', '').replace('.', '')
                    spec_data[key] = f"{value} mm"
            
            # 寸法情報を構造化
            dimensions = self._extract_dimensions_from_spec(spec_data, soup.get_text())
            if dimensions:
                spec_data['dimensions_structured'] = dimensions
            
        except Exception as e:
            print(f"  Warning: Failed to get specifications for {slug}: {e}")
        
        return {'specifications': spec_data}
    
    def _extract_dimensions_from_spec(self, spec_data: Dict, page_text: str) -> Optional[str]:
        """寸法情報を抽出（改善版）"""
        dimensions = {'length': None, 'width': None, 'height': None}
        
        # 様々なキー名で寸法を探す
        LENGTH_KEYS = [
            'length', 'overall length', 'length (mm)', 'overall length (mm)',
            'length mm', 'exterior length', 'body length', 'total length'
        ]
        WIDTH_KEYS = [
            'width', 'overall width', 'width (mm)', 'overall width (mm)',
            'width mm', 'exterior width', 'body width', 'total width',
            'width (without mirrors)', 'width without mirrors'
        ]
        HEIGHT_KEYS = [
            'height', 'overall height', 'height (mm)', 'overall height (mm)',
            'height mm', 'exterior height', 'body height', 'total height'
        ]
        
        for key, value in spec_data.items():
            key_lower = key.lower().strip()
            
            # 長さの取得
            if not dimensions['length']:
                for lk in LENGTH_KEYS:
                    if lk in key_lower or key_lower == lk:
                        if match := re.search(r'(\d{3,4})', str(value).replace(',', '')):
                            num = int(match.group(1))
                            if 2000 <= num <= 6000:  # 妥当な範囲チェック
                                dimensions['length'] = str(num)
                                break
            
            # 幅の取得
            if not dimensions['width']:
                for wk in WIDTH_KEYS:
                    if wk in key_lower or key_lower == wk:
                        if match := re.search(r'(\d{3,4})', str(value).replace(',', '')):
                            num = int(match.group(1))
                            if 1500 <= num <= 2500:  # 妥当な範囲チェック
                                dimensions['width'] = str(num)
                                break
            
            # 高さの取得
            if not dimensions['height']:
                for hk in HEIGHT_KEYS:
                    if hk in key_lower or key_lower == hk:
                        if match := re.search(r'(\d{3,4})', str(value).replace(',', '')):
                            num = int(match.group(1))
                            if 1000 <= num <= 2500:  # 妥当な範囲チェック
                                dimensions['height'] = str(num)
                                break
        
        # ページテキストから直接探す（フォールバック）
        if not all(dimensions.values()):
            # "Dimensions: 4000 x 1800 x 1500 mm" のようなパターン
            dim_pattern = r'(\d{3,4})\s*[xX×]\s*(\d{3,4})\s*[xX×]\s*(\d{3,4})'
            if match := re.search(dim_pattern, page_text):
                l, w, h = match.groups()
                l, w, h = int(l), int(w), int(h)
                
                # 通常は長さ > 幅 > 高さの順
                if not dimensions['length'] and 2000 <= l <= 6000:
                    dimensions['length'] = str(l)
                if not dimensions['width'] and 1500 <= w <= 2500:
                    dimensions['width'] = str(w)
                if not dimensions['height'] and 1000 <= h <= 2500:
                    dimensions['height'] = str(h)
        
        if all(dimensions.values()):
            return f"{dimensions['length']} x {dimensions['width']} x {dimensions['height']} mm"
        elif dimensions['length'] and dimensions['width']:
            return f"{dimensions['length']} x {dimensions['width']} mm"
        elif dimensions['length']:
            return f"Length: {dimensions['length']} mm"
        
        return None
    
    def _extract_number(self, text: str) -> Optional[int]:
        """テキストから数値を抽出"""
        if text is None:
            return None
        if match := re.search(r'\d+', str(text)):
            return int(match.group())
        return None
    
    def _is_valid_image_url(self, url: str) -> bool:
        """有効な画像URLかチェック"""
        if not url or not url.startswith('http'):
            return False
        
        valid_domains = ['prismic.io', 'carwow', 'imgix.net', 'cloudinary.com']
        return any(domain in url for domain in valid_domains)

# ======================== Main Scraper Class ========================
class CarwowScraper:
    """Carwowスクレイパーのメインクラス"""
    
    def __init__(self):
        self.maker_discovery = MakerDiscovery()
        self.model_discovery = ModelDiscovery()
        self.vehicle_scraper = VehicleScraper()
    
    def get_all_makers(self) -> List[str]:
        """全メーカーを取得"""
        return self.maker_discovery.get_all_makers()
    
    def get_models_for_maker(self, maker: str) -> List[str]:
        """指定メーカーの全モデルを取得"""
        return self.model_discovery.get_models_for_maker(maker)
    
    def scrape_vehicle(self, slug: str) -> Dict:
        """車両データを取得"""
        return self.vehicle_scraper.scrape_vehicle(slug)
    
    def scrape_all_vehicles(self, makers: Optional[List[str]] = None) -> List[Dict]:
        """全車両データを取得"""
        if makers is None:
            makers = self.get_all_makers()
        
        all_vehicles = []
        
        for maker in makers:
            print(f"Processing maker: {maker}")
            models = self.get_models_for_maker(maker)
            
            for model_slug in models:
                try:
                    print(f"  Scraping: {model_slug}")
                    vehicle_data = self.scrape_vehicle(model_slug)
                    all_vehicles.append(vehicle_data)
                    
                    time.sleep(random.uniform(0.5, 1.5))
                    
                except Exception as e:
                    print(f"  Error scraping {model_slug}: {e}")
                    continue
        
        return all_vehicles

# ======================== Utility Functions ========================
def test_scraper():
    """スクレイパーのテスト"""
    scraper = CarwowScraper()
    
    # メーカー取得テスト
    print("Testing maker discovery...")
    makers = scraper.get_all_makers()
    print(f"Found {len(makers)} makers: {makers[:5]}...")
    
    # モデル取得テスト
    print("\nTesting model discovery for 'audi'...")
    models = scraper.get_models_for_maker('audi')
    print(f"Found {len(models)} models: {models[:5]}...")
    
    # 車両データ取得テスト
    if models:
        print(f"\nTesting vehicle scrape for '{models[0]}'...")
        vehicle = scraper.scrape_vehicle(models[0])
        print(f"Title: {vehicle.get('title')}")
        print(f"Price: £{vehicle.get('price_min_gbp')} - £{vehicle.get('price_max_gbp')}")
        print(f"Body types: {vehicle.get('body_types')}")
        print(f"Dimensions: {vehicle.get('specifications', {}).get('dimensions_structured')}")
        print(f"Trims: {len(vehicle.get('trims', []))} found")
        for trim in vehicle.get('trims', []):
            print(f"  - {trim.get('trim_name')}: {trim.get('engine')}")


if __name__ == "__main__":
    test_scraper()
