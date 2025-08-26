#!/usr/bin/env python3
"""
carwow_scraper.py
データ取得に特化したメインスクレイピングモジュール
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
        """車両データを取得"""
        url = f"{BASE_URL}/{slug}"
        
        try:
            # メインページ取得
            soup = self.client.get_soup(url)
            
            # リダイレクトチェック
            if self._is_redirect_or_list_page(soup):
                raise ValueError(f"Not a valid model page: {slug}")
            
            # 基本データ取得
            data = {
                'slug': slug,
                'url': url,
                'make': slug.split('/')[0],
                'model': slug.split('/')[1] if '/' in slug else slug
            }
            
            # __NEXT_DATA__から取得
            next_data = self._extract_next_data(soup)
            product = next_data.get('props', {}).get('pageProps', {}).get('product', {})
            
            # タイトル・概要
            data['title'] = self._extract_title(soup, product)
            data['overview'] = self._extract_overview(soup, product)
            
            # 価格情報
            data.update(self._extract_prices(soup, product))
            
            # スペック情報
            data.update(self._extract_specs(soup, product))
            
            # メディア
            data['images'] = self._extract_images(soup, product)
            
            # 追加ページから取得
            data['colors'] = self._scrape_colors(slug)
            data.update(self._scrape_specifications(slug))
            
            # ボディタイプ（カテゴリページから推定）
            data['body_types'] = self._determine_body_types(slug)
            
            return data
            
        except Exception as e:
            raise Exception(f"Failed to scrape {slug}: {str(e)}")
    
    def _is_redirect_or_list_page(self, soup: BeautifulSoup) -> bool:
        """リストページや無効なページかチェック"""
        # タイトルにReviewがない場合は疑わしい
        title = soup.find('title')
        if title and 'review' not in title.text.lower():
            # ただし明確に車両ページの要素がある場合はOK
            if not soup.select_one('div.review-overview, div.model-hub'):
                return True
        
        # リストページ特有の要素をチェック
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
        # productデータから
        if title := product.get('name'):
            return title
        
        # h1タグから
        if h1 := soup.find('h1'):
            title = h1.get_text(strip=True)
            # " review"部分を除去
            return re.sub(r'\s+review.*$', '', title, flags=re.IGNORECASE)
        
        return ""
    
    def _extract_overview(self, soup: BeautifulSoup, product: Dict) -> str:
        """概要文を取得（実際のページ構造に対応）"""
        # productデータから
        if review := product.get('review', {}).get('intro'):
            if len(review) >= 30:
                return review.strip()
        
        # "What's good" / "What's not so good" の前のテキストを探す
        for elem in soup.find_all(['p', 'div']):
            text = elem.get_text(strip=True)
            if len(text) >= 100 and not text.startswith('What'):
                # 価格情報や技術仕様を含まない段落を選ぶ
                if not any(skip in text for skip in ['£', 'mph', 'kWh', 'CO2']):
                    return text[:500]  # 最大500文字
        
        # メタディスクリプション
        if meta := soup.find('meta', {'name': 'description'}):
            content = meta.get('content', '').strip()
            if content:
                return content
        
        # フォールバック：最初の長い段落
        for p in soup.select('article p, div.content p, p'):
            text = p.get_text(strip=True)
            if len(text) >= 50:
                return text[:500]
        
        return ""
    
    def _extract_prices(self, soup: BeautifulSoup, product: Dict) -> Dict:
        """価格情報を取得（実際のCarwowページ構造に対応）"""
        prices = {
            'price_min_gbp': None,
            'price_max_gbp': None,
            'price_used_gbp': None
        }
        
        # productデータから
        prices['price_min_gbp'] = product.get('priceMin') or product.get('rrpMin')
        prices['price_max_gbp'] = product.get('priceMax') or product.get('rrpMax')
        
        # RRP範囲を探す（例: "RRP £29,985 - £34,485"）
        if not prices['price_min_gbp']:
            # 複数のパターンを試す
            patterns = [
                r'RRP\s*£([\d,]+)\s*-\s*£([\d,]+)',  # RRP範囲
                r'£([\d,]+)\s*-\s*£([\d,]+)',         # 単純な価格範囲
                r'From\s*£([\d,]+)',                   # From価格
                r'Cash\s*£([\d,]+)',                   # Cash価格
            ]
            
            page_text = soup.get_text()
            for pattern in patterns:
                if match := re.search(pattern, page_text):
                    if '-' in pattern:  # 範囲の場合
                        prices['price_min_gbp'] = int(match.group(1).replace(',', ''))
                        prices['price_max_gbp'] = int(match.group(2).replace(',', ''))
                    else:  # 単一価格の場合
                        prices['price_min_gbp'] = int(match.group(1).replace(',', ''))
                    if prices['price_min_gbp']:
                        break
        
        # 中古価格を探す（例: "Used £20,720"）
        if not prices['price_used_gbp']:
            used_patterns = [
                r'Used\s*£([\d,]+)',
                r'Used\s*from\s*£([\d,]+)',
                r'Pre-owned\s*£([\d,]+)',
            ]
            
            for pattern in used_patterns:
                if match := re.search(pattern, page_text):
                    prices['price_used_gbp'] = int(match.group(1).replace(',', ''))
                    break
        
        # 価格セクションを特定して解析
        for element in soup.select('div, span, p'):
            text = element.get_text(strip=True)
            
            # Cash価格
            if 'Cash' in text and not prices['price_min_gbp']:
                if match := re.search(r'£([\d,]+)', text):
                    prices['price_min_gbp'] = int(match.group(1).replace(',', ''))
            
            # Monthly価格（参考）
            elif 'Monthly' in text:
                continue  # 月額は今回は取得しない
            
            # Used価格
            elif 'Used' in text and not prices['price_used_gbp']:
                if match := re.search(r'£([\d,]+)', text):
                    prices['price_used_gbp'] = int(match.group(1).replace(',', ''))
        
        return prices
    
    def _extract_specs(self, soup: BeautifulSoup, product: Dict) -> Dict:
        """基本スペックを取得（At a glanceセクション対応）"""
        specs = {
            'fuel_type': None,
            'doors': None,
            'seats': None,
            'transmission': None,
            'dimensions': None
        }
        
        # productデータから
        specs['fuel_type'] = product.get('fuelType')
        specs['doors'] = product.get('numberOfDoors')
        specs['seats'] = product.get('numberOfSeats')
        specs['transmission'] = product.get('transmission')
        
        # すべてのkey-valueペアを収集
        all_data = {}
        
        # At a glanceセクション（新レイアウト）
        for section in soup.select('div.review-overview__at-a-glance, div.at-a-glance, section.specs'):
            for dt in section.select('dt'):
                if dd := dt.find_next('dd'):
                    key = dt.get_text(strip=True).lower()
                    value = dd.get_text(strip=True)
                    all_data[key] = value
        
        # 汎用的なdt/ddペア（At a glanceがない場合）
        if not all_data:
            for dt in soup.select('dt'):
                if dd := dt.find_next_sibling('dd'):
                    key = dt.get_text(strip=True).lower()
                    value = dd.get_text(strip=True)
                    all_data[key] = value
        
        # テーブルからも取得
        for row in soup.select('table tr'):
            cells = row.select('th, td')
            if len(cells) >= 2:
                key = cells[0].get_text(strip=True).lower()
                value = cells[1].get_text(strip=True)
                all_data[key] = value
        
        # 見出しバリエーション定義（より広範囲）
        FUEL_KEYS = {
            'fuel', 'fuel type', 'fuel types', 'fuel type(s)', 
            'available fuel', 'available fuel types', 'engine type',
            'engine', 'power source', 'energy'
        }
        DOOR_KEYS = {
            'doors', 'number of doors', 'no. of doors', 'no of doors',
            'door count', 'door configuration'
        }
        SEAT_KEYS = {
            'seats', 'number of seats', 'no. of seats', 'no of seats',
            'seating capacity', 'seating', 'passenger capacity'
        }
        TRANS_KEYS = {
            'transmission', 'drive type', 'drivetrain', 'gearbox', 
            'transmission type', 'drive', 'gear type', 'drive system',
            'transmission and drive'
        }
        
        # データ補完
        if not specs['fuel_type']:
            for key in FUEL_KEYS:
                if key in all_data:
                    specs['fuel_type'] = all_data[key]
                    break
        
        if not specs['doors']:
            for key in DOOR_KEYS:
                if key in all_data:
                    specs['doors'] = self._extract_number(all_data[key])
                    break
        
        if not specs['seats']:
            for key in SEAT_KEYS:
                if key in all_data:
                    specs['seats'] = self._extract_number(all_data[key])
                    break
        
        if not specs['transmission']:
            for key in TRANS_KEYS:
                if key in all_data:
                    specs['transmission'] = all_data[key]
                    break
        
        return specs
    
    def _extract_images(self, soup: BeautifulSoup, product: Dict) -> List[str]:
        """画像URLを取得"""
        images = []
        seen = set()
        
        # productデータから
        if hero := product.get('heroImage'):
            images.append(hero)
            seen.add(hero)
        
        for gallery in ['galleryImages', 'mediaGallery']:
            if items := product.get(gallery, []):
                for item in items:
                    if url := item.get('url'):
                        if url not in seen:
                            images.append(url)
                            seen.add(url)
        
        # HTMLから補完
        for img in soup.select('img[src], img[data-src]'):
            src = img.get('src') or img.get('data-src', '')
            if self._is_valid_image_url(src) and src not in seen:
                images.append(src)
                seen.add(src)
        
        return images[:40]  # 最大40枚
    
    def _scrape_colors(self, slug: str) -> List[str]:
        """カラーバリエーションを取得"""
        colors = []
        
        try:
            soup = self.client.get_soup(f"{BASE_URL}/{slug}/colours")
            
            # カラー名を抽出
            for element in soup.select('h4.colour-title, .colour-name, li.colour-option'):
                if color := element.get_text(strip=True):
                    # 価格部分を除去
                    color = re.sub(r'[£€][\d,]+.*$', '', color).strip()
                    if color and color not in colors:
                        colors.append(color)
        
        except Exception:
            pass
        
        return colors
    
    def _scrape_specifications(self, slug: str) -> Dict:
        """詳細スペックを取得（リダイレクト対応強化）"""
        spec_data = {}
        
        try:
            url = f"{BASE_URL}/{slug}/specifications"
            response = self.client.get(url, allow_redirects=True)
            
            # リダイレクト先がメーカートップページの場合は無視
            final_url = response.url
            if f"/{slug.split('/')[0]}#" in final_url or final_url.rstrip('/') == f"{BASE_URL}/{slug.split('/')[0]}":
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
            
            # 寸法情報を構造化（多様なフォーマットに対応）
            dimensions = self._extract_dimensions_from_spec(spec_data, soup.text)
            if dimensions:
                spec_data['dimensions_structured'] = dimensions
            
        except Exception as e:
            print(f"  Warning: Failed to get specifications for {slug}: {e}")
        
        return {'specifications': spec_data}
    
    def _extract_dimensions_from_spec(self, spec_data: Dict, page_text: str) -> Optional[str]:
        """寸法情報を抽出（多様なフォーマットに対応）"""
        # キーのバリエーション
        LENGTH_KEYS = {'length', 'overall length', 'length (mm)', 'overall length (mm)', 'total length'}
        WIDTH_KEYS = {'width', 'overall width', 'width (mm)', 'overall width (mm)', 'width inc mirrors', 'width including mirrors'}
        HEIGHT_KEYS = {'height', 'overall height', 'height (mm)', 'overall height (mm)', 'total height'}
        
        dimensions = {'length': None, 'width': None, 'height': None}
        
        # spec_dataから取得
        for key, value in spec_data.items():
            key_lower = key.lower()
            
            # 長さ
            if any(lk in key_lower for lk in LENGTH_KEYS):
                if match := re.search(r'(\d{3,4})', value):
                    dimensions['length'] = match.group(1)
            
            # 幅
            elif any(wk in key_lower for wk in WIDTH_KEYS):
                if match := re.search(r'(\d{3,4})', value):
                    dimensions['width'] = match.group(1)
            
            # 高さ
            elif any(hk in key_lower for hk in HEIGHT_KEYS):
                if match := re.search(r'(\d{3,4})', value):
                    dimensions['height'] = match.group(1)
        
        # ページテキストから補完（mm単位の数値を探す）
        if not all(dimensions.values()):
            mm_values = re.findall(r'(\d{3,4})\s*mm', page_text)
            if len(mm_values) >= 3:
                if not dimensions['length']: dimensions['length'] = mm_values[0]
                if not dimensions['width']: dimensions['width'] = mm_values[1]
                if not dimensions['height']: dimensions['height'] = mm_values[2]
        
        # 結果を構成
        if all(dimensions.values()):
            return f"{dimensions['length']} x {dimensions['width']} x {dimensions['height']} mm"
        
        return None
    
    def _determine_body_types(self, slug: str) -> List[str]:
        """ボディタイプを推定（2段slug対応）"""
        body_types = []
        maker = slug.split('/')[0]
        
        # カテゴリページをチェック
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
                
                # スラッグの完全一致と部分一致の両方をチェック
                # 例: mercedes/amg-cla-45-s と amg-cla-45-s の両方でマッチ
                model_part = slug.split('/')[-1] if '/' in slug else slug
                
                # このカテゴリページに該当モデルへのリンクがあるか確認
                found = False
                for link in soup.select('a[href]'):
                    href = link.get('href', '')
                    # 完全一致チェック
                    if f"/{slug}" in href:
                        found = True
                        break
                    # 部分一致チェック（AMGなど2段slugの対応）
                    if f"/{model_part}" in href:
                        found = True
                        break
                
                if found:
                    body_types.append(body_type)
                    
            except Exception:
                continue
        
        return body_types
    
    def _extract_number(self, text: str) -> Optional[int]:
        """テキストから数値を抽出"""
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
                    
                    # レート制限対策
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


if __name__ == "__main__":
    test_scraper()
