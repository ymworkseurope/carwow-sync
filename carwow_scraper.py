#!/usr/bin/env python3
"""
carwow_scraper.py - Fixed Production Version

"""
import re
import json
import time
from typing import Dict, List, Optional, Tuple, Set
from bs4 import BeautifulSoup
import requests
from pathlib import Path

BASE_URL = "https://www.carwow.co.uk"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

# Body type URLs mapping
BODY_TYPE_URLS = {
    'SUV': 'https://www.carwow.co.uk/car-chooser?vehicle_body_type%5B%5D=SUVs',
    'Hatchback': 'https://www.carwow.co.uk/car-chooser?vehicle_body_type%5B%5D=hatchbacks',
    'Saloon': 'https://www.carwow.co.uk/car-chooser?vehicle_body_type%5B%5D=saloons',
    'Coupe': 'https://www.carwow.co.uk/car-chooser?vehicle_body_type%5B%5D=coupes',
    'Estate': 'https://www.carwow.co.uk/car-chooser?vehicle_body_type%5B%5D=estate-cars',
    'People Carrier': 'https://www.carwow.co.uk/car-chooser?vehicle_body_type%5B%5D=people-carriers',
    'Sports Car': 'https://www.carwow.co.uk/car-chooser?vehicle_body_type%5B%5D=sports-cars',
    'Convertible': 'https://www.carwow.co.uk/car-chooser?vehicle_body_type%5B%5D=convertibles'
}

class CarwowScraper:
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.body_type_cache = {}
        self._load_body_type_cache()
    
    def _load_body_type_cache(self):
        """ボディタイプキャッシュを読み込み"""
        cache_file = Path('body_type_cache.json')
        if cache_file.exists():
            try:
                with open(cache_file, 'r') as f:
                    self.body_type_cache = json.load(f)
                print(f"Loaded body type cache with {len(self.body_type_cache)} entries")
            except Exception as e:
                print(f"Error loading body type cache: {e}")
                self.body_type_cache = {}
    
    def _save_body_type_cache(self):
        """ボディタイプキャッシュを保存"""
        try:
            with open('body_type_cache.json', 'w') as f:
                json.dump(self.body_type_cache, f, indent=2)
        except Exception as e:
            print(f"Error saving body type cache: {e}")
    
    def _build_body_type_cache(self):
        """全ボディタイプページをスクレイピングしてキャッシュを構築"""
        print("Building body type cache...")
        
        for body_type, url in BODY_TYPE_URLS.items():
            print(f"  Fetching {body_type} models...")
            models = self._scrape_body_type_page(url, body_type)
            
            for model_name in models:
                if model_name not in self.body_type_cache:
                    self.body_type_cache[model_name] = []
                if body_type not in self.body_type_cache[model_name]:
                    self.body_type_cache[model_name].append(body_type)
            
            time.sleep(1)
        
        self._save_body_type_cache()
        print(f"Body type cache built with {len(self.body_type_cache)} models")
    
    def _scrape_body_type_page(self, url: str, body_type: str) -> List[str]:
        """特定のボディタイプページから車種名を取得"""
        models = []
        page = 1
        max_pages = 10
        
        while page <= max_pages:
            try:
                page_url = url if page == 1 else f"{url}&page={page}"
                resp = self.session.get(page_url, timeout=30)
                
                if resp.status_code != 200:
                    break
                
                soup = BeautifulSoup(resp.text, 'lxml')
                model_cards = soup.find_all('h3', class_='card-compact__title')
                
                if not model_cards:
                    break
                
                for card in model_cards:
                    model_name = card.get_text(strip=True)
                    if model_name:
                        models.append(model_name)
                
                next_link = soup.find('a', {'data-car-chooser--filters-page-url-value': re.compile(f'page={page+1}')})
                if not next_link:
                    break
                
                page += 1
                time.sleep(0.5)
                
            except Exception as e:
                print(f"    Error scraping body type page {page}: {e}")
                break
        
        return models
    
    def _get_body_types_for_model(self, model_name: str, slug: str) -> List[str]:
        """モデル名からボディタイプを取得"""
        if not self.body_type_cache:
            self._build_body_type_cache()
        
        if model_name in self.body_type_cache:
            return self.body_type_cache[model_name]
        
        model_words = model_name.lower().split()
        for cached_model, body_types in self.body_type_cache.items():
            cached_words = cached_model.lower().split()
            if len(set(model_words) & set(cached_words)) >= min(len(model_words), len(cached_words)) - 1:
                return body_types
        
        if '/' in slug:
            maker_slug = slug.split('/')[0]
            maker_name = maker_slug.replace('-', ' ').title()
            short_model = model_name.replace(maker_name, '').strip()
            
            if short_model and short_model in self.body_type_cache:
                return self.body_type_cache[short_model]
        
        return []
    
    def scrape_vehicle(self, slug: str) -> Optional[Dict]:
        """車両データを取得"""
        
        main_url = f"{BASE_URL}/{slug}"
        main_resp = self.session.get(main_url, timeout=30)
        if main_resp.status_code != 200:
            return None
        
        main_soup = BeautifulSoup(main_resp.text, 'lxml')
        
        # 基本情報の取得
        make_en, model_en = self._extract_make_model(slug, main_soup)
        
        # overview_enの取得
        overview_en = self._extract_overview(main_soup)
        
        # 価格情報の取得
        prices = self._extract_prices_from_elements(main_soup)
        
        # メディアURLの取得（修正版）
        media_urls = self._extract_media_urls(main_soup)
        
        # Specificationsページから詳細データ取得
        specs_data = self._scrape_specifications(slug)
        
        # カラー情報の取得
        colors = self._scrape_colors(slug)
        
        # ボディタイプの取得（キャッシュから）
        body_types = self._get_body_types_for_model(model_en, slug)
        
        # フォールバック：メインページからボディタイプと燃料タイプを取得
        if not body_types or any(grade.get('fuel') == 'Information not available' for grade in specs_data.get('grades_engines', [])):
            fallback_body_types, fallback_fuel = self._extract_body_type_and_fuel_from_main(main_soup)
            
            if not body_types and fallback_body_types:
                body_types = fallback_body_types
            
            if fallback_fuel and fallback_fuel != 'Information not available':
                for grade in specs_data.get('grades_engines', []):
                    if grade.get('fuel') == 'Information not available':
                        grade['fuel'] = fallback_fuel
        
        return {
            'slug': slug,
            'make_en': make_en,
            'model_en': model_en,
            'overview_en': overview_en,
            'prices': prices,
            'grades_engines': specs_data.get('grades_engines', []),
            'specifications': specs_data.get('specifications', {}),
            'colors': colors,
            'media_urls': media_urls,
            'body_types': body_types if body_types else [],
            'catalog_url': main_url
        }
    
    def _extract_make_model(self, slug: str, soup: BeautifulSoup) -> Tuple[str, str]:
        """メーカーとモデル名を抽出"""
        make_slug = slug.split('/')[0]
        make_en = make_slug.replace('-', ' ').title()
        
        make_map = {
            'Mercedes Benz': 'Mercedes-Benz',
            'Alfa Romeo': 'Alfa Romeo',
            'Land Rover': 'Land Rover',
            'Aston Martin': 'Aston Martin'
        }
        make_en = make_map.get(make_en, make_en)
        
        model_en = ''
        title = soup.find('title')
        if title:
            title_text = title.text
            if 'Review' in title_text:
                model_part = title_text.split('Review')[0].strip()
                model_en = model_part.replace(make_en, '').strip()
            elif '|' in title_text:
                model_part = title_text.split('|')[0].strip()
                model_en = model_part.replace(make_en, '').strip()
        
        return make_en, model_en
    
    def _extract_overview(self, soup: BeautifulSoup) -> str:
        """overview_enをemタグから取得"""
        em_tag = soup.find('em')
        if em_tag:
            text = em_tag.get_text(strip=True)
            if len(text) > 50:
                return text
        
        meta = soup.find('meta', {'name': 'description'})
        if meta:
            return meta.get('content', '')
        
        return ''
    
    def _extract_prices_from_elements(self, soup: BeautifulSoup) -> Dict:
        """価格情報を抽出"""
        prices = {}
        
        rrp_span = soup.find('span', class_='deals-cta-list__rrp-price')
        if rrp_span:
            price_wraps = rrp_span.find_all('span', class_='price--no-wrap')
            if len(price_wraps) >= 2:
                min_price_text = price_wraps[0].get_text(strip=True)
                min_price_match = re.search(r'£([\d,]+)', min_price_text)
                if min_price_match:
                    prices['price_min_gbp'] = int(min_price_match.group(1).replace(',', ''))
                
                max_price_text = price_wraps[1].get_text(strip=True)
                max_price_match = re.search(r'£([\d,]+)', max_price_text)
                if max_price_match:
                    prices['price_max_gbp'] = int(max_price_match.group(1).replace(',', ''))
        
        summary_items = soup.find_all('div', class_='summary-list__item')
        for item in summary_items:
            dt = item.find('dt')
            dd = item.find('dd')
            if dt and dd and 'Used' in dt.get_text():
                used_price_text = dd.get_text(strip=True)
                used_match = re.search(r'£([\d,]+)', used_price_text)
                if used_match:
                    prices['price_used_gbp'] = int(used_match.group(1).replace(',', ''))
                break
        
        if not prices:
            text = soup.get_text()
            
            cash_match = re.search(r'Cash\s*£([\d,]+)', text)
            if cash_match:
                prices['price_min_gbp'] = int(cash_match.group(1).replace(',', ''))
            
            rrp_match = re.search(r'RRP.*?£([\d,]+)\s*(?:to|-)\s*£([\d,]+)', text)
            if rrp_match:
                if not prices.get('price_min_gbp'):
                    prices['price_min_gbp'] = int(rrp_match.group(1).replace(',', ''))
                prices['price_max_gbp'] = int(rrp_match.group(2).replace(',', ''))
        
        return prices
    
    def _extract_media_urls(self, soup: BeautifulSoup) -> List[str]:
        """画像URLの取得（修正版）- media-slider__imageのsrcsetから優先取得"""
        media_urls = []
        seen_urls = set()
        
        # 優先: media-slider__image クラスのsrcsetから取得
        slider_images = soup.find_all('img', class_='media-slider__image')
        for img in slider_images:
            srcset = img.get('srcset', '')
            src = img.get('src', '')
            
            if srcset:
                # srcsetから最大解像度のURLを抽出
                srcset_entries = [entry.strip() for entry in srcset.split(',')]
                highest_res_url = None
                highest_width = 0
                
                for entry in srcset_entries:
                    if ' ' in entry:
                        url_part = entry.rsplit(' ', 1)[0]  # 最後のスペースで分割
                        width_part = entry.rsplit(' ', 1)[1]
                        try:
                            width = int(width_part.replace('w', ''))
                            if width > highest_width:
                                highest_width = width
                                highest_res_url = url_part
                        except ValueError:
                            continue
                
                if highest_res_url and highest_res_url not in seen_urls:
                    # HTMLエンティティをデコード
                    highest_res_url = highest_res_url.replace('&amp;', '&')
                    media_urls.append(highest_res_url)
                    seen_urls.add(highest_res_url)
            
            elif src and src not in seen_urls:
                src = src.replace('&amp;', '&')
                media_urls.append(src)
                seen_urls.add(src)
        
        # フォールバック: thumbnail-carousel-vertical__img クラスから取得
        if len(media_urls) < 5:
            thumbnails = soup.find_all('img', class_='thumbnail-carousel-vertical__img')
            for img in thumbnails:
                url = img.get('data-src') or img.get('src')
                if url and 'images.prismic.io' in url:
                    base_url = url.split('?')[0]
                    high_res_url = f"{base_url}?auto=format&cs=tinysrgb&fit=max&q=90"
                    
                    if high_res_url not in seen_urls:
                        media_urls.append(high_res_url)
                        seen_urls.add(high_res_url)
        
        return media_urls[:10]
    
    def _extract_body_type_and_fuel_from_main(self, soup: BeautifulSoup) -> Tuple[List[str], str]:
        """フォールバック：メインページからボディタイプと燃料タイプを取得"""
        body_types = []
        fuel_type = 'Information not available'
        
        at_glance = soup.find('div', class_='review-overview__at-a-glance-model')
        
        if at_glance:
            headings = at_glance.find_all('div', class_='review-overview__at-a-glance-model-spec-heading')
            values = at_glance.find_all('div', class_='review-overview__at-a-glance-model-spec-value')
            
            for i, heading in enumerate(headings):
                heading_text = heading.get_text(strip=True).lower()
                
                if i < len(values):
                    value_elem = values[i].find('span')
                    if value_elem:
                        value_text = value_elem.get_text(strip=True)
                        
                        if 'body type' in heading_text and value_text:
                            body_types = [bt.strip() for bt in value_text.split(',')]
                        
                        elif 'fuel type' in heading_text and value_text:
                            fuel_type = value_text
        
        return body_types, fuel_type
    
    def _scrape_specifications(self, slug: str) -> Dict:
        """Specificationsページから詳細データ取得"""
        specs_url = f"{BASE_URL}/{slug}/specifications"
        
        try:
            specs_resp = self.session.get(specs_url, timeout=30, allow_redirects=False)
            
            if specs_resp.status_code != 200:
                return self._extract_specs_from_main(slug)
            
            specs_soup = BeautifulSoup(specs_resp.text, 'lxml')
            
            grades_engines = self._extract_grades_engines(specs_soup)
            specifications = self._extract_basic_specs(specs_soup)
            
            return {
                'grades_engines': grades_engines,
                'specifications': specifications
            }
            
        except Exception as e:
            print(f"    Error getting specifications: {e}")
            return self._extract_specs_from_main(slug)
    
    def _extract_grades_engines(self, soup: BeautifulSoup) -> List[Dict]:
        """グレードとエンジン情報を抽出（価格取得修正版）"""
        grades_engines = []
        processed_combinations = {}
        
        sections = soup.find_all('article', class_=lambda x: x and 'trim' in str(x) if x else False)
        
        if not sections:
            sections = [soup]
        
        for section in sections:
            grade_name = 'Information not available'
            grade_elem = section.find('span', class_='trim-article__title-part-2')
            if grade_elem:
                grade_name = grade_elem.get_text(strip=True)
            
            engine_divs = section.find_all('div', class_='specification-breakdown__title')
            
            if not engine_divs:
                section_text = section.get_text()
                if 'RRP' in section_text or grade_name != 'Information not available':
                    combo_key = f"{grade_name}_NO_ENGINE"
                    if combo_key not in processed_combinations:
                        grade_info = self._create_grade_info(section, grade_name, 'Information not available')
                        processed_combinations[combo_key] = grade_info
                continue
            
            for engine_div in engine_divs:
                engine_text = engine_div.get_text(strip=True)
                if not engine_text:
                    engine_text = 'Information not available'
                
                combo_key = f"{grade_name}_{engine_text}"
                if combo_key in processed_combinations:
                    existing = processed_combinations[combo_key]
                    new_info = self._create_grade_info(section, grade_name, engine_text)
                    for key, value in new_info.items():
                        if (not existing.get(key) or existing.get(key) == 'Information not available') and value and value != 'Information not available':
                            existing[key] = value
                else:
                    grade_info = self._create_grade_info(section, grade_name, engine_text)
                    processed_combinations[combo_key] = grade_info
        
        grades_engines = list(processed_combinations.values())
        
        if not grades_engines:
            default_grade = {
                'grade': 'Information not available',
                'engine': 'Information not available',
                'engine_price_gbp': None,
                'fuel': 'Information not available',
                'transmission': 'Information not available',
                'drive_type': 'Information not available',
                'power_bhp': None
            }
            grades_engines.append(default_grade)
        
        return grades_engines
    
    def _create_grade_info(self, section, grade_name: str, engine_text: str) -> Dict:
        """グレード情報を作成（価格取得修正版）"""
        grade_info = {
            'grade': grade_name if grade_name else 'Information not available',
            'engine': engine_text if engine_text else 'Information not available',
            'engine_price_gbp': None,
            'fuel': 'Information not available',
            'transmission': 'Information not available',
            'drive_type': 'Information not available',
            'power_bhp': None
        }
        
        # 価格取得の修正: より広範囲の価格パターンに対応
        section_text = section.get_text()
        
        # 価格パターンを複数試行（優先順位あり）
        price_patterns = [
            rf'{re.escape(engine_text)}.*?£([\d,]+)',  # エンジン名に続く価格
            r'RRP.*?£([\d,]+)',  # RRP価格
            r'From.*?£([\d,]+)',  # From価格
            r'Price.*?£([\d,]+)',  # Price価格
            r'£([\d,]+)(?!\s*(?:finance|deposit|month))',  # 一般的な価格（ファイナンス等を除外）
        ]
        
        for pattern in price_patterns:
            price_matches = re.findall(pattern, section_text, re.IGNORECASE)
            if price_matches:
                for match in price_matches:
                    price_value = int(match.replace(',', ''))
                    # 車の価格として妥当な範囲をチェック
                    if 10000 <= price_value <= 300000:
                        grade_info['engine_price_gbp'] = price_value
                        break
                if grade_info['engine_price_gbp']:
                    break
        
        # エンジン固有の価格取得（特定のエンジンdivに関連する価格）
        if not grade_info['engine_price_gbp'] and engine_text != 'Information not available':
            engine_div = section.find('div', class_='specification-breakdown__title', string=lambda x: x and engine_text in x)
            if engine_div:
                # 親要素または兄弟要素から価格を探す
                parent = engine_div.parent
                siblings = engine_div.find_next_siblings()
                
                for element in [parent] + siblings:
                    if element:
                        elem_text = element.get_text()
                        price_match = re.search(r'£([\d,]+)', elem_text)
                        if price_match:
                            price_value = int(price_match.group(1).replace(',', ''))
                            if 10000 <= price_value <= 300000:
                                grade_info['engine_price_gbp'] = price_value
                                break
        
        # 仕様詳細を取得
        category_lists = section.find_all('ul', class_='specification-breakdown__category-list')
        
        for category_list in category_lists:
            list_items = category_list.find_all('li', class_='specification-breakdown__category-list-item')
            
            for item in list_items:
                item_text = item.get_text(strip=True)
                
                if grade_info['transmission'] == 'Information not available':
                    if 'Automatic' in item_text:
                        grade_info['transmission'] = 'Automatic'
                    elif 'Manual' in item_text:
                        grade_info['transmission'] = 'Manual'
                    elif 'CVT' in item_text:
                        grade_info['transmission'] = 'CVT'
                    elif 'DCT' in item_text:
                        grade_info['transmission'] = 'DCT'
                
                if 'wheel drive' in item_text.lower() and grade_info['drive_type'] == 'Information not available':
                    grade_info['drive_type'] = item_text
                
                if 'bhp' in item_text.lower() and not grade_info['power_bhp']:
                    bhp_match = re.search(r'(\d+)\s*bhp', item_text, re.IGNORECASE)
                    if bhp_match:
                        grade_info['power_bhp'] = int(bhp_match.group(1))
        
        # 燃料タイプをエンジン情報から推測
        if engine_text and engine_text != 'Information not available':
            engine_lower = engine_text.lower()
            if 'kwh' in engine_lower or 'electric' in engine_lower:
                grade_info['fuel'] = 'Electric'
                if grade_info['transmission'] == 'Information not available':
                    grade_info['transmission'] = 'Automatic'
            elif 'diesel' in engine_lower:
                grade_info['fuel'] = 'Diesel'
            elif 'hybrid' in engine_lower:
                if 'plug-in' in engine_lower:
                    grade_info['fuel'] = 'Plug-in Hybrid'
                else:
                    grade_info['fuel'] = 'Hybrid'
            elif 'petrol' in engine_lower or 'tsi' in engine_lower or 'tfsi' in engine_lower:
                grade_info['fuel'] = 'Petrol'
        
        for category_list in category_lists:
            list_items = category_list.find_all('li', class_='specification-breakdown__category-list-item')
            for item in list_items:
                item_text = item.get_text(strip=True).lower()
                if 'petrol' in item_text and grade_info['fuel'] == 'Information not available':
                    grade_info['fuel'] = 'Petrol'
                elif 'diesel' in item_text and grade_info['fuel'] == 'Information not available':
                    grade_info['fuel'] = 'Diesel'
                elif 'electric' in item_text and grade_info['fuel'] == 'Information not available':
                    grade_info['fuel'] = 'Electric'
        
        return grade_info
    
    def _extract_basic_specs(self, soup: BeautifulSoup) -> Dict:
        """基本スペックを抽出"""
        specs = {}
        text = soup.get_text()
        
        doors_match = re.search(r'Number of doors\s*(\d+)', text)
        if doors_match:
            specs['doors'] = int(doors_match.group(1))
        
        seats_match = re.search(r'Number of seats\s*(\d+)', text)
        if seats_match:
            specs['seats'] = int(seats_match.group(1))
        
        dimensions = []
        for tspan in soup.find_all('tspan'):
            tspan_text = tspan.get_text(strip=True)
            if 'mm' in tspan_text and re.search(r'\d+,?\d*\s*mm', tspan_text):
                dimensions.append(tspan_text)
        
        if len(dimensions) >= 3:
            specs['dimensions_mm'] = f"{dimensions[0]} x {dimensions[1]} x {dimensions[2]}"
        
        if 'Boot (seats up)' in text:
            boot_match = re.search(r'Boot \(seats up\)\s*(\d+)\s*L', text)
            if boot_match:
                specs['boot_capacity_l'] = int(boot_match.group(1))
        
        if 'Battery capacity' in text:
            battery_match = re.search(r'Battery capacity\s*([\d.]+)\s*kWh', text)
            if battery_match:
                specs['battery_capacity_kwh'] = float(battery_match.group(1))
        
        return specs
    
    def _scrape_colors(self, slug: str) -> List[str]:
        """カラー情報を取得"""
        colors = []
        colors_url = f"{BASE_URL}/{slug}/colours"
        
        try:
            colors_resp = self.session.get(colors_url, timeout=30, allow_redirects=False)
            
            if colors_resp.status_code == 200:
                colors_soup = BeautifulSoup(colors_resp.text, 'lxml')
                
                for h4 in colors_soup.find_all('h4', class_='model-hub__colour-details-title'):
                    color_text = h4.get_text(strip=True)
                    color_name = re.sub(r'(Free|£[\d,]+).*$', '', color_text).strip()
                    if color_name and color_name not in colors:
                        colors.append(color_name)
        except:
            pass
        
        return colors
    
    def _extract_specs_from_main(self, slug: str) -> Dict:
        """メインページから仕様を抽出（specificationsページがない場合）"""
        try:
            main_url = f"{BASE_URL}/{slug}"
            main_resp = self.session.get(main_url, timeout=30)
            
            if main_resp.status_code != 200:
                return {'grades_engines': [], 'specifications': {}}
            
            soup = BeautifulSoup(main_resp.text, 'lxml')
            text = soup.get_text()
            
            grade_info = {
                'grade': 'Information not available',
                'engine': 'Information not available',
                'engine_price_gbp': None,
                'fuel': 'Information not available',
                'transmission': 'Information not available',
                'drive_type': 'Information not available',
                'power_bhp': None
            }
            
            if 'electric' in text.lower():
                grade_info['fuel'] = 'Electric'
                grade_info['transmission'] = 'Automatic'
            
            return {
                'grades_engines': [grade_info],
                'specifications': {}
            }
            
        except:
            return {'grades_engines': [], 'specifications': {}}
    
    def get_all_makers(self) -> List[str]:
        """brandsページからメーカー一覧を取得"""
        makers = []
        
        try:
            resp = self.session.get(f"{BASE_URL}/brands", timeout=30)
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, 'lxml')
                
                for brand_div in soup.find_all('div', class_='brands-list__group-item-title-name'):
                    brand_name = brand_div.get_text(strip=True).lower()
                    brand_slug = brand_name.replace(' ', '-')
                    if brand_slug and brand_slug not in makers:
                        makers.append(brand_slug)
                
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
            resp = self.session.get(url, timeout=30)
            
            if resp.status_code != 200:
                return models
            
            soup = BeautifulSoup(resp.text, 'lxml')
            
            articles = soup.find_all('article', class_='card-compact')
            
            for article in articles:
                for link in article.find_all('a', href=True):
                    href = link['href']
                    if f'/{maker}/' in href:
                        if 'carwow.co.uk' in href:
                            parts = href.split('carwow.co.uk/')[-1].split('?')[0].split('#')[0].split('/')
                        else:
                            parts = href.strip('/').split('?')[0].split('#')[0].split('/')
                        
                        if len(parts) >= 2 and parts[0] == maker:
                            model_slug = f"{parts[0]}/{parts[1]}"
                            if model_slug not in seen:
                                models.append(model_slug)
                                seen.add(model_slug)
                                break
            
            if not models:
                all_links = soup.find_all('a', href=True)
                
                for link in all_links:
                    href = link['href']
                    if f'/{maker}/' in href:
                        if any(skip in href for skip in ['/news/', '/reviews/', '/colours', '/specifications']):
                            continue
                        
                        if 'carwow.co.uk' in href:
                            parts = href.split('carwow.co.uk/')[-1].split('?')[0].split('#')[0].split('/')
                        else:
                            parts = href.strip('/').split('?')[0].split('#')[0].split('/')
                        
                        if len(parts) >= 2 and parts[0] == maker:
                            model_slug = f"{parts[0]}/{parts[1]}"
                            if model_slug not in seen:
                                models.append(model_slug)
                                seen.add(model_slug)
                                
        except Exception as e:
            print(f"    Error getting models for {maker}: {e}")
        
        return models
    
    def cleanup(self):
        """リソースのクリーンアップ"""
        if self.body_type_cache:
            self._save_body_type_cache()
        
        self.session.close()
