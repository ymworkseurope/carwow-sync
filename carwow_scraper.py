#!/usr/bin/env python3
"""
carwow_scraper.py
データ取得に特化したメインスクレイピングモジュール
"""
import re
import json
import time
import random
from typing import Dict, List, Optional, Set
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# ======================== Configuration ========================
BASE_URL = "https://www.carwow.co.uk"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
HEADERS = {"User-Agent": USER_AGENT}
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3
RETRY_DELAY = 2

# 除外するURLセグメント（モデルページではない）
EXCLUDE_SEGMENTS = {
    "automatic", "manual", "lease", "used", "deals", "finance",
    "reviews", "prices", "news", "hybrid", "electric", "suv",
    "estate", "hatchback", "saloon", "coupe", "convertible",
    "sports", "mpv", "people-carriers"
}

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
                model = model.replace('/review', '')
                if model and len(model) > 1:
                    models.add(model)
        
        return models

# ======================== Vehicle Data Scraper ========================
class VehicleScraper:
    """個別車両ページのデータを取得"""
    
    def __init__(self):
        self.client = HTTPClient()
    
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
            
            # トランスミッション情報
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
            
            # ボディタイプ
            data['body_types'] = product.get('bodyType', []) or []
            
            # トリム情報（シンプル版）
            data['trims'] = self._scrape_trims(slug)
            
            return data
            
        except Exception as e:
            raise Exception(f"Failed to scrape {slug}: {str(e)}")
    
    def _is_redirect_or_list_page(self, soup: BeautifulSoup) -> bool:
        """リストページや無効なページかチェック"""
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
            'transmission': None
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
    
    def _scrape_trims(self, slug: str) -> List[Dict]:
        """トリム情報を取得（汎用版）"""
        trims = []
        try:
            spec_url = f"{BASE_URL}/{slug}/specifications"
            soup = self.client.get_soup(spec_url)
            
            # __NEXT_DATA__からトリム取得
            next_data = self._extract_next_data(soup)
            product_list = (next_data.get('props', {})
                          .get('pageProps', {})
                          .get('trims', []) or
                          next_data.get('props', {})
                          .get('pageProps', {})
                          .get('productCardList', []))
            
            for product in product_list:
                trim_name = product.get('name') or product.get('trim_name')
                if trim_name:
                    trim_data = {
                        'trim_name': trim_name,
                        'engine': product.get('engine', ''),
                        'fuel_type': product.get('fuelType', ''),
                        'power_bhp': self._extract_number(product.get('power')),
                        'transmission': product.get('transmission', ''),
                        'drive_type': product.get('driveType', ''),
                        'price_rrp': self._extract_number(product.get('price') or product.get('rrp'))
                    }
                    trims.append(trim_data)
            
            # HTMLから補完
            for elem in soup.select('h4, .trim-name, .variant-title'):
                trim_name = elem.get_text(strip=True)
                if trim_name and len(trim_name) >= 2:
                    trims.append({
                        'trim_name': trim_name,
                        'engine': '',
                        'fuel_type': '',
                        'power_bhp': None,
                        'transmission': '',
                        'drive_type': ''
                    })
            
            if not trims:
                trims = [{'trim_name': 'Standard', 'engine': '', 'fuel_type': '', 'power_bhp': None, 'transmission': '', 'drive_type': ''}]
            
            print(f"  Found {len(trims)} trims: {[t['trim_name'] for t in trims]}")
        
        except Exception as e:
            print(f"  Warning: Failed to get trims for {slug}: {e}")
            trims = [{'trim_name': 'Standard', 'engine': '', 'fuel_type': '', 'power_bhp': None, 'transmission': '', 'drive_type': ''}]
        
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
                color = re.sub(r'(Free|£[\d,]+).*$', '', color).strip()
                
                if color and color not in colors and len(color) < 50:
                    colors.append(color)
        
        except Exception as e:
            print(f"  Warning: Failed to get colors for {slug}: {e}")
        
        return colors
    
    def _scrape_specifications(self, slug: str) -> Dict:
        """詳細スペックを取得"""
        spec_data = {}
        
        try:
            url = f"{BASE_URL}/{slug}/specifications"
            response = self.client.get(url, allow_redirects=True)
            
            final_url = response.url
            if f"/{slug.split('/')[0]}#" in final_url:
                return {'specifications': {}}
            
            soup = BeautifulSoup(response.text, 'lxml')
            
            # テーブルから取得
            for row in soup.select('table tr'):
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
            
            # 寸法情報を構造化
            dimensions = self._extract_dimensions(spec_data)
            if dimensions:
                spec_data['dimensions'] = dimensions
            
        except Exception as e:
            print(f"  Warning: Failed to get specifications for {slug}: {e}")
        
        return {'specifications': spec_data}
    
    def _extract_dimensions(self, spec_data: Dict) -> Optional[str]:
        """寸法情報を抽出"""
        dimensions = {'length': None, 'width': None, 'height': None}
        
        LENGTH_KEYS = {'length', 'overall length', 'length (mm)', 'overall length (mm)'}
        WIDTH_KEYS = {'width', 'overall width', 'width (mm)', 'overall width (mm)'}
        HEIGHT_KEYS = {'height', 'overall height', 'height (mm)', 'overall height (mm)'}
        
        for key, value in spec_data.items():
            key_lower = key.lower()
            
            if any(lk in key_lower for lk in LENGTH_KEYS):
                if match := re.search(r'(\d{3,4})', str(value)):
                    dimensions['length'] = match.group(1)
            elif any(wk in key_lower for wk in WIDTH_KEYS):
                if match := re.search(r'(\d{3,4})', str(value)):
                    dimensions['width'] = match.group(1)
            elif any(hk in key_lower for hk in HEIGHT_KEYS):
                if match := re.search(r'(\d{3,4})', str(value)):
                    dimensions['height'] = match.group(1)
        
        if all(dimensions.values()):
            return f"{dimensions['length']} x {dimensions['width']} x {dimensions['height']} mm"
        
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
