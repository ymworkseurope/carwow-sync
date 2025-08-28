#!/usr/bin/env python3
"""
carwow_scraper.py
データ取得に特化したメインスクレイピングモジュール（修正版）
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
                        break
            
            if not data.get('seats') and 'specifications' in detailed_specs:
                spec_data = detailed_specs['specifications']
                for key in ['number of seats', 'seats', 'no. of seats', 'seating capacity']:
                    if key in spec_data:
                        data['seats'] = self._extract_number(spec_data[key])
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
            
            # トリム情報を取得（修正版）
            data['trims'] = self._scrape_trims_from_specifications(slug)
            
            return data
            
        except Exception as e:
            raise Exception(f"Failed to scrape {slug}: {str(e)}")
    
    def _scrape_trims_from_specifications(self, slug: str) -> List[Dict]:
        """specificationsページから正確なトリム情報を取得"""
        trims = []
        
        try:
            # specificationsページを取得
            spec_url = f"{BASE_URL}/{slug}/specifications"
            response = self.client.get(spec_url, allow_redirects=True)
            
            # リダイレクト先がメーカートップページの場合は基本トリムのみ返す
            final_url = response.url
            if f"/{slug.split('/')[0]}#" in final_url or final_url.rstrip('/') == f"{BASE_URL}/{slug.split('/')[0]}":
                return [{
                    'trim_name': 'Standard',
                    'engine': '',
                    'fuel_type': '',
                    'power_bhp': None,
                    'transmission': '',
                    'drive_type': ''
                }]
            
            soup = BeautifulSoup(response.text, 'lxml')
            
            # RRPテーブルから正確なトリム情報を抽出
            trim_data = self._extract_trims_from_rrp_table(soup)
            
            # RRPテーブルがない場合は、別の方法で取得
            if not trim_data:
                trim_data = self._extract_trims_from_page_structure(soup, slug)
            
            # 各トリムに共通のエンジン情報を追加
            common_engine_info = self._extract_common_engine_info(soup)
            
            for trim in trim_data:
                trim.update(common_engine_info)
                trims.append(trim)
                
        except Exception as e:
            print(f"  Warning: Failed to get trims from specifications for {slug}: {e}")
        
        # デフォルトトリム
        if not trims:
            trims = [{
                'trim_name': 'Standard',
                'engine': '',
                'fuel_type': '',
                'power_bhp': None,
                'transmission': '',
                'drive_type': ''
            }]
        
        return trims
    
    def _extract_trims_from_rrp_table(self, soup: BeautifulSoup) -> List[Dict]:
        """RRPテーブルからトリム情報を抽出"""
        trims = []
        
        # RRPテーブルを探す
        for table in soup.select('table'):
            table_text = table.get_text()
            if 'RRP' in table_text:
                rows = table.select('tr')
                for row in rows[1:]:  # ヘッダー行をスキップ
                    cells = row.select('th, td')
                    if len(cells) >= 2:
                        trim_name = cells[0].get_text(strip=True)
                        price_text = cells[1].get_text(strip=True)
                        
                        # 有効なトリム名かチェック
                        if (trim_name and 
                            not trim_name.lower().startswith('model') and
                            not trim_name.lower().startswith('price') and
                            not trim_name.lower().startswith('rrp') and
                            '£' in price_text):
                            
                            trims.append({
                                'trim_name': trim_name,
                                'price': price_text
                            })
        
        return trims
    
    def _extract_trims_from_page_structure(self, soup: BeautifulSoup, slug: str) -> List[Dict]:
        """ページ構造からトリム情報を抽出"""
        trims = []
        page_text = soup.get_text()
        
        # 特定のモデルの既知トリム
        model_trims = {
            'abarth/500e': ['Standard', 'Turismo', 'Scorpionissima'],
            'abarth/500e-cabrio': ['Standard', 'Turismo', 'Scorpionissima'],
            'abarth/abarth-600e': ['Standard', 'Turismo', 'Scorpionissima']
        }
        
        if slug in model_trims:
            for trim_name in model_trims[slug]:
                # そのトリムが実際にページに記載されているかチェック
                if trim_name.lower() in page_text.lower():
                    trims.append({'trim_name': trim_name})
        else:
            # 一般的なトリム名パターンを探す
            common_trims = ['Standard', 'Sport', 'Premium', 'Luxury', 'Performance', 'GT', 'S', 'RS']
            for trim_name in common_trims:
                # RRP価格と組み合わせて存在確認
                pattern = rf'{trim_name}.*?RRP.*?£[\d,]+'
                if re.search(pattern, page_text, re.IGNORECASE | re.DOTALL):
                    trims.append({'trim_name': trim_name})
        
        return trims
    
    def _extract_common_engine_info(self, soup: BeautifulSoup) -> Dict:
        """共通のエンジン情報を抽出"""
        page_text = soup.get_text()
        engine_info = {
            'engine': '',
            'fuel_type': '',
            'power_bhp': None,
            'transmission': '',
            'drive_type': ''
        }
        
        # エンジンパターンを探す
        engine_patterns = [
            r'(\d+)\s*kW\s+(\d+(?:\.\d+)?)\s*kWh\s+(\w+)',  # 114kW 42.2kWh Auto
            r'(\d+)\s*hp\s+(\d+(?:\.\d+)?L?)\s*(\w+)',     # 155hp 1.4L Petrol
        ]
        
        for pattern in engine_patterns:
            if match := re.search(pattern, page_text):
                if 'kW' in match.group(0):
                    # 電気自動車
                    kw = int(match.group(1))
                    kwh = match.group(2)
                    trans = match.group(3)
                    
                    engine_info['engine'] = f"{kw}kW {kwh}kWh {trans}"
                    engine_info['fuel_type'] = 'Electric'
                    engine_info['power_bhp'] = int(kw * 1.341)  # kWをBHPに変換
                    engine_info['transmission'] = 'Automatic' if 'auto' in trans.lower() else trans
                    engine_info['drive_type'] = 'Front wheel drive'  # デフォルト
                else:
                    # ガソリン車
                    hp = int(match.group(1))
                    engine_size = match.group(2)
                    trans = match.group(3)
                    
                    engine_info['engine'] = f"{hp}hp {engine_size} {trans}"
                    engine_info['fuel_type'] = 'Petrol'
                    engine_info['power_bhp'] = hp
                    engine_info['transmission'] = trans
                    engine_info['drive_type'] = 'Front wheel drive'  # デフォルト
                
                break
        
        # トランスミッション情報を別途探す
        if not engine_info['transmission']:
            trans_patterns = ['Automatic', 'Manual', 'CVT', 'DSG']
            for trans in trans_patterns:
                if trans.lower() in page_text.lower():
                    engine_info['transmission'] = trans
                    break
        
        return engine_info
    
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
        """タイトルを取得（h1から正確に）"""
        # h1タグから取得（最も信頼性が高い）
        if h1 := soup.find('h1', class_='header__title'):
            title = h1.get_text(strip=True)
            # "Review & Prices"などを除去
            title = re.sub(r'\s*(Review|Prices?|&).*$', '', title, flags=re.IGNORECASE)
            return title.strip()
        
        # 通常のh1
        if h1 := soup.find('h1'):
            title = h1.get_text(strip=True)
            title = re.sub(r'\s*(review).*$', '', title, flags=re.IGNORECASE)
            return title.strip()
        
        # productデータから
        if title := product.get('name'):
            return title
        
        return ""
    
    def _extract_model_name(self, title: str, make: str) -> str:
        """タイトルからモデル名を抽出"""
        # メーカー名を除去してモデル名を取得
        model = title
        
        # メーカー名のバリエーションを除去
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
        # productデータから
        if review := product.get('review', {}).get('intro'):
            if len(review) >= 30:
                return review.strip()
        
        # メタディスクリプションから（最も確実）
        if meta := soup.find('meta', {'name': 'description'}):
            content = meta.get('content', '').strip()
            # ナビゲーションメニューでないことを確認
            if content and not content.startswith('Your account') and len(content) >= 50:
                return content
        
        # og:descriptionから
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
        
        # productデータから
        prices['price_min_gbp'] = product.get('priceMin') or product.get('rrpMin')
        prices['price_max_gbp'] = product.get('priceMax') or product.get('rrpMax')
        
        # RRP範囲を探す
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
        
        # 中古価格を探す
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
        
        # productデータから
        specs['fuel_type'] = product.get('fuelType')
        specs['doors'] = product.get('numberOfDoors')
        specs['seats'] = product.get('numberOfSeats')
        specs['transmission'] = product.get('transmission')
        
        # すべてのkey-valueペアを収集
        all_data = {}
        
        # At a glanceセクション、テーブル、dt/dd要素から収集
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
        
        # データ補完
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
        
        return images[:40]
    
    def _scrape_colors(self, slug: str) -> List[str]:
        """カラーバリエーションを取得"""
        colors = []
        
        try:
            response = self.client.get(f"{BASE_URL}/{slug}/colours", allow_redirects=True)
            
            # リダイレクトチェック
            if f"/{slug.split('/')[0]}#" in response.url or response.url.rstrip('/') == f"{BASE_URL}/{slug.split('/')[0]}":
                return []
            
            soup = BeautifulSoup(response.text, 'lxml')
            
            # model-hub__colour-details-title クラスから取得
            for element in soup.select('h4.model-hub__colour-details-title'):
                # 価格情報を含むspanを除去
                for span in element.select('span.model-hub__colour_details_title_float_right'):
                    span.decompose()
                
                color = element.get_text(strip=True)
                # 価格部分を除去
                color = re.sub(r'(Free|£[\d,]+).*, '', color).strip()
                
                if color and color not in colors:
                    colors.append(color)
            
            # 他のカラー要素も探す
            for element in soup.select('div.colour-option, li.colour-item, span.colour-name'):
                color = element.get_text(strip=True)
                color = re.sub(r'(Free|£[\d,]+).*, '', color).strip()
                if color and color not in colors:
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
            
            # 寸法情報を構造化
            dimensions = self._extract_dimensions_from_spec(spec_data, soup.text)
            if dimensions:
                spec_data['dimensions_structured'] = dimensions
            
        except Exception as e:
            print(f"  Warning: Failed to get specifications for {slug}: {e}")
        
        return {'specifications': spec_data}
    
    def _extract_dimensions_from_spec(self, spec_data: Dict, page_text: str) -> Optional[str]:
        """寸法情報を抽出"""
        LENGTH_KEYS = {'length', 'overall length', 'length (mm)', 'overall length (mm)', 'total length'}
        WIDTH_KEYS = {'width', 'overall width', 'width (mm)', 'overall width (mm)', 'width inc mirrors', 'width including mirrors'}
        HEIGHT_KEYS = {'height', 'overall height', 'height (mm)', 'overall height (mm)', 'total height'}
        
        dimensions = {'length': None, 'width': None, 'height': None}
        
        # spec_dataから取得
        for key, value in spec_data.items():
            key_lower = key.lower()
            
            if any(lk in key_lower for lk in LENGTH_KEYS):
                if match := re.search(r'(\d{3,4})', value):
                    dimensions['length'] = match.group(1)
            elif any(wk in key_lower for wk in WIDTH_KEYS):
                if match := re.search(r'(\d{3,4})', value):
                    dimensions['width'] = match.group(1)
            elif any(hk in key_lower for hk in HEIGHT_KEYS):
                if match := re.search(r'(\d{3,4})', value):
                    dimensions['height'] = match.group(1)
        
        # ページテキストから補完
        if not all(dimensions.values()):
            mm_values = re.findall(r'(\d{3,4})\s*mm', page_text)
            if len(mm_values) >= 3:
                if not dimensions['length']: dimensions['length'] = mm_values[0]
                if not dimensions['width']: dimensions['width'] = mm_values[1]
                if not dimensions['height']: dimensions['height'] = mm_values[2]
        
        if all(dimensions.values()):
            return f"{dimensions['length']} x {dimensions['width']} x {dimensions['height']} mm"
        
        return None
    
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
                model_part = slug.split('/')[-1] if '/' in slug else slug
                
                found = False
                for link in soup.select('a[href]'):
                    href = link.get('href', '')
                    if f"/{slug}" in href or f"/{model_part}" in href:
                        found = True
                        break
                
                if found:
                    body_types.append(body_type)
                    
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
