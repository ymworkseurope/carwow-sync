#!/usr/bin/env python3
"""
carwow_scraper.py
データ取得に特化したメインスクレイピングモジュール - 完全修正版
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
            
            # ボディタイプ
            data['body_types'] = self._determine_body_types(slug)
            
            # トリム情報を取得（改善版）
            data['trims'] = self._scrape_trims_improved(slug)
            
            return data
            
        except Exception as e:
            raise Exception(f"Failed to scrape {slug}: {str(e)}")
    
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
                return []
            
            soup = BeautifulSoup(response.text, 'lxml')
            
            # "Trims and engines"セクション後のコンテンツを解析
            trim_sections = []
            found_trim_section = False
            
            for elem in soup.find_all(['h2', 'h3', 'h4', 'div']):
                if 'Trims and engines' in elem.get_text():
                    found_trim_section = True
                    continue
                
                if found_trim_section:
                    text = elem.get_text()
                    if any(word in text for word in ['Standard', 'Turismo', 'Scorpionissima', 'Sport', 'S line']):
                        trim_sections.append(elem)
            
            # テキスト全体から正確なトリム名を抽出
            page_text = soup.get_text()
            
            # 一般的なトリム名パターンを検索
            known_trim_patterns = [
                r'(\w+(?:\s+\w+)?)\s+RRP\s*£([\d,]+)',
                r'(\w+(?:\s+\w+)?)\s+.*?£([\d,]+)',
            ]
            
            found_trims = {}
            
            for pattern in known_trim_patterns:
                matches = re.finditer(pattern, page_text, re.IGNORECASE)
                for match in matches:
                    trim_name = match.group(1).strip()
                    try:
                        price = int(match.group(2).replace(',', ''))
                    except:
                        continue
                    
                    # 有効なトリム名かチェック
                    if (trim_name and len(trim_name) > 1 and 
                        trim_name not in ['RRP', 'Used', 'From', 'Price'] and
                        not trim_name.isdigit()):
                        if trim_name not in found_trims or found_trims[trim_name]['price_rrp'] < price:
                            found_trims[trim_name] = {
                                'trim_name': trim_name,
                                'engine': '',
                                'fuel_type': 'Electric' if 'e' in slug else 'Petrol',
                                'power_bhp': None,
                                'transmission': 'Automatic',
                                'drive_type': '',
                                'price_rrp': price
                            }
            
            # 見つかったトリムをリストに変換
            for trim_name, trim_data in found_trims.items():
                trims.append(trim_data)
            
            if trims:
                print(f"  Found {len(trims)} valid trims: {[t['trim_name'] for t in trims]}")
            
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
                # 価格部分を除去（修正済み）
                color = re.sub(r'(Free|£[\d,]+).*$', '', color).strip()
                
                if color and color not in colors and len(color) < 50:
                    colors.append(color)
        
        except Exception as e:
            print(f"  Warning: Failed to get colors for {slug}: {e}")
        
        return colors
    
    def _scrape_specifications(self, slug: str) -> Dict:
        """詳細スペックを取得（改善版 - 寸法情報を強化）"""
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
            
            # External dimensions セクションを特別に処理
            external_dims = {}
            internal_dims = {}
            
            # ページテキストから寸法を抽出
            page_text = soup.get_text()
            
            # 寸法パターンを検索
            dimension_patterns = {
                'length': [r'Length[:\s]*(\d{3,5}(?:\.\d+)?)\s*(?:mm|m)', r'(\d{4,5})\s*mm.*length'],
                'width': [r'Width[:\s]*(\d{3,5}(?:\.\d+)?)\s*(?:mm|m)', r'(\d{4,5})\s*mm.*width'],
                'height': [r'Height[:\s]*(\d{3,5}(?:\.\d+)?)\s*(?:mm|m)', r'(\d{4,5})\s*mm.*height'],
                'wheelbase': [r'Wheelbase[:\s]*(\d{1,3}(?:\.\d+)?)\s*(?:mm|m)', r'(\d{2,4})\s*mm.*wheelbase'],
                'turning_circle': [r'Turning circle[:\s]*(\d{1,3}(?:\.\d+)?)\s*(?:mm|m)'],
                'boot_seats_up': [r'Boot.*seats up.*?(\d{2,4})\s*L', r'(\d{2,4})\s*L.*boot'],
                'boot_seats_down': [r'Boot.*seats down.*?(\d{3,4})\s*L', r'(\d{3,4})\s*L.*seats down']
            }
            
            for dim_name, patterns in dimension_patterns.items():
                for pattern in patterns:
                    match = re.search(pattern, page_text, re.IGNORECASE)
                    if match:
                        try:
                            value = float(match.group(1))
                            # 単位変換（mからmm）
                            if 'm' in pattern and value < 100:
                                value = int(value * 1000)
                            else:
                                value = int(value)
                            spec_data[dim_name] = str(value)
                            break
                        except:
                            continue
            
            # HTMLの特定セクションから寸法を抽出
            for section in soup.find_all(['section', 'div'], class_=re.compile('dimension|spec')):
                for item in section.find_all(['dt', 'th', 'td']):
                    text = item.get_text().lower()
                    if any(dim in text for dim in ['length', 'width', 'height', 'wheelbase', 'boot', 'turning']):
                        # 次の要素から値を取得
                        next_elem = item.find_next(['dd', 'td'])
                        if next_elem:
                            value = next_elem.get_text().strip()
                            spec_data[text] = value
            
            # 寸法情報を構造化
            dimensions = self._extract_dimensions_from_spec(spec_data, soup.text)
            if dimensions:
                spec_data['dimensions_structured'] = dimensions
            
        except Exception as e:
            print(f"  Warning: Failed to get specifications for {slug}: {e}")
        
        return {'specifications': spec_data}
    
    def _extract_dimensions_from_spec(self, spec_data: Dict, page_text: str) -> Optional[str]:
        """寸法情報を抽出（改善版）"""
        dimensions = {'length': None, 'width': None, 'height': None, 'wheelbase': None}
        
        # 標準的な寸法キーをチェック
        dimension_mappings = {
            'length': ['length', 'overall length', 'length (mm)', 'overall length (mm)', 'length mm'],
            'width': ['width', 'overall width', 'width (mm)', 'overall width (mm)', 'width mm'],
            'height': ['height', 'overall height', 'height (mm)', 'overall height (mm)', 'height mm'],
            'wheelbase': ['wheelbase', 'wheelbase (mm)', 'wheelbase mm']
        }
        
        for dim_type, keys in dimension_mappings.items():
            for key in keys:
                if key in spec_data:
                    value = self._extract_dimension_value(spec_data[key])
                    if value:
                        dimensions[dim_type] = value
                        break
        
        # ページテキストから直接パターンマッチング
        if not any(dimensions.values()):
            patterns = [
                r'(\d{4})\s*×\s*(\d{4})\s*×\s*(\d{4})\s*mm',  # 4973 × 1931 × 1498 mm
                r'(\d{4})\s*x\s*(\d{4})\s*x\s*(\d{4})\s*mm',   # 4973 x 1931 x 1498 mm
                r'L(\d{4})\s*×\s*W(\d{4})\s*×\s*H(\d{4})',     # L4973 × W1931 × H1498
            ]
            
            for pattern in patterns:
                match = re.search(pattern, page_text)
                if match:
                    dimensions['length'] = int(match.group(1))
                    dimensions['width'] = int(match.group(2))
                    dimensions['height'] = int(match.group(3))
                    break
        
        # フォーマット
        result_parts = []
        
        if dimensions['length'] and dimensions['width'] and dimensions['height']:
            result_parts.append(f"L{dimensions['length']} × W{dimensions['width']} × H{dimensions['height']} mm")
        elif dimensions['length'] and dimensions['width']:
            result_parts.append(f"L{dimensions['length']} × W{dimensions['width']} mm")
        elif dimensions['length']:
            result_parts.append(f"Length: {dimensions['length']} mm")
        
        if dimensions['wheelbase']:
            result_parts.append(f"Wheelbase: {dimensions['wheelbase']} mm")
        
        # その他の寸法情報
        other_dims = []
        for key, value in spec_data.items():
            if any(word in key for word in ['boot', 'turning', 'ground clearance', 'cargo']):
                if isinstance(value, str) and value.strip():
                    other_dims.append(f"{key.title()}: {value}")
        
        if other_dims:
            result_parts.extend(other_dims[:3])  # 最大3つまで
        
        return "; ".join(result_parts) if result_parts else None
    
    def _determine_body_types(self, slug: str) -> List[str]:
        """ボディタイプを推定"""
        body_types = []
        maker = slug.split('/')[0]
        
        categories = {
            'SUV': 'suv',
            'Electric': 'electric',
            'Hybrid': 'hybrid',
            'Convertible': 'convertible',
            'Estate': 'estate',
            'Hatchback': 'hatchback',
            'Saloon': 'saloon',
            'Coupe': 'coupe',
            'Sports': 'sports'
        }
        
        for body_type, category in categories.items():
            try:
                soup = self.client.get_soup(f"{BASE_URL}/{maker}/{category}")
                model_part = slug.split('/')[-1]
                
                for link in soup.select('a[href]'):
                    href = link.get('href', '')
                    if f"/{slug}" in href or f"/{model_part}" in href:
                        body_types.append(body_type)
                        break
                    
            except Exception:
                continue
        
        return body_types
    
    def _extract_number(self, text: str) -> Optional[int]:
        """テキストから数値を抽出"""
        if text is None:
            return None
        if match := re.search(r'\d+', str(text)):
            return int(match.group())
        return None
    
    def _extract_dimension_value(self, value: Any) -> Optional[int]:
        """寸法値の抽出（文字列から数値を抽出）"""
        if isinstance(value, (int, float)):
            return int(value)
        
        if isinstance(value, str):
            # mm単位の場合
            mm_match = re.search(r'(\d{3,5})\s*mm', value)
            if mm_match:
                return int(mm_match.group(1))
            
            # m単位の場合（mmに変換）
            m_match = re.search(r'(\d+(?:\.\d+)?)\s*m', value)
            if m_match:
                return int(float(m_match.group(1)) * 1000)
            
            # 単位なしの数値（3-5桁）
            num_match = re.search(r'(\d{3,5})', value)
            if num_match:
                return int(num_match.group(1))
        
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
        print(f"Trims: {len(vehicle.get('trims', []))} found")
        for trim in vehicle.get('trims', []):
            print(f"  - {trim.get('trim_name')}: {trim.get('engine')}")


if __name__ == "__main__":
    test_scraper()
