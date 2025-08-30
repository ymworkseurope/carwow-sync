#!/usr/bin/env python3
"""
carwow_scraper.py - 完全修正版
正確な要素から情報を取得
"""
import re
import json
import time
from typing import Dict, List, Optional, Tuple, Set
from bs4 import BeautifulSoup
import requests

BASE_URL = "https://www.carwow.co.uk"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

class CarwowScraper:
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
    
    def scrape_vehicle(self, slug: str) -> Optional[Dict]:
        """車両データを取得"""
        
        main_url = f"{BASE_URL}/{slug}"
        main_resp = self.session.get(main_url, timeout=30)
        if main_resp.status_code != 200:
            return None
        
        main_soup = BeautifulSoup(main_resp.text, 'lxml')
        
        # 基本情報の取得
        make_en, model_en = self._extract_make_model(slug, main_soup)
        
        # overview_enの取得（emタグから）
        overview_en = self._extract_overview(main_soup)
        
        # 価格情報の取得
        prices = self._extract_prices_from_elements(main_soup)
        
        # メディアURLの取得（正しいクラスと属性から）
        media_urls = self._extract_media_urls_correctly(main_soup)
        
        # Specificationsページから詳細データ取得
        specs_data = self._scrape_specifications(slug)
        
        # カラー情報の取得
        colors = self._scrape_colors(slug)
        
        # ボディタイプの取得（TODO: Seleniumで実装予定）
        body_types = self._get_fallback_body_types(model_en)
        
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
            'body_types': body_types,
            'catalog_url': main_url
        }
    
    def _extract_make_model(self, slug: str, soup: BeautifulSoup) -> Tuple[str, str]:
        """メーカーとモデル名を抽出"""
        make_slug = slug.split('/')[0]
        make_en = make_slug.replace('-', ' ').title()
        
        # 特殊なメーカー名のマッピング
        make_map = {
            'Mercedes Benz': 'Mercedes-Benz',
            'Alfa Romeo': 'Alfa Romeo',
            'Land Rover': 'Land Rover',
            'Aston Martin': 'Aston Martin'
        }
        make_en = make_map.get(make_en, make_en)
        
        # titleタグからモデル名を取得
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
        
        # フォールバック：meta descriptionから
        meta = soup.find('meta', {'name': 'description'})
        if meta:
            return meta.get('content', '')
        
        return ''
    
    def _extract_prices_from_elements(self, soup: BeautifulSoup) -> Dict:
        """価格情報を抽出"""
        prices = {}
        
        # RRP価格範囲から取得
        rrp_span = soup.find('span', class_='deals-cta-list__rrp-price')
        if rrp_span:
            price_wraps = rrp_span.find_all('span', class_='price--no-wrap')
            if len(price_wraps) >= 2:
                # 最小価格
                min_price_text = price_wraps[0].get_text(strip=True)
                min_price_match = re.search(r'£([\d,]+)', min_price_text)
                if min_price_match:
                    prices['price_min_gbp'] = int(min_price_match.group(1).replace(',', ''))
                
                # 最大価格
                max_price_text = price_wraps[1].get_text(strip=True)
                max_price_match = re.search(r'£([\d,]+)', max_price_text)
                if max_price_match:
                    prices['price_max_gbp'] = int(max_price_match.group(2).replace(',', ''))
        
        # Used価格
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
        
        # フォールバック：テキスト全体から価格を探す
        if not prices:
            text = soup.get_text()
            
            # Cash価格
            cash_match = re.search(r'Cash\s*£([\d,]+)', text)
            if cash_match:
                prices['price_min_gbp'] = int(cash_match.group(1).replace(',', ''))
            
            # RRP範囲
            rrp_match = re.search(r'RRP.*?£([\d,]+)\s*(?:to|-)\s*£([\d,]+)', text)
            if rrp_match:
                if not prices.get('price_min_gbp'):
                    prices['price_min_gbp'] = int(rrp_match.group(1).replace(',', ''))
                prices['price_max_gbp'] = int(rrp_match.group(2).replace(',', ''))
        
        return prices
    
    def _extract_media_urls_correctly(self, soup: BeautifulSoup) -> List[str]:
        """正しいクラスと属性から画像URLを取得"""
        media_urls = []
        seen_urls = set()
        
        # パターン1: thumbnail-carousel-vertical__img クラス
        thumbnails = soup.find_all('img', class_='thumbnail-carousel-vertical__img')
        for img in thumbnails:
            # data-srcを優先、なければsrc
            url = img.get('data-src') or img.get('src')
            if url and 'images.prismic.io' in url:
                # 高解像度版のURLを作成
                base_url = url.split('?')[0]
                # 正しいパラメータで高解像度版
                high_res_url = f"{base_url}?auto=format&cs=tinysrgb&fit=max&q=90"
                
                if high_res_url not in seen_urls:
                    media_urls.append(high_res_url)
                    seen_urls.add(high_res_url)
        
        # パターン2: media-slider系のクラス（フォールバック）
        if len(media_urls) < 3:
            slider_images = soup.find_all('img', class_=lambda x: x and ('media' in str(x) or 'slider' in str(x)))
            for img in slider_images:
                url = img.get('data-src') or img.get('src')
                if url and 'images.prismic.io' in url and url not in seen_urls:
                    base_url = url.split('?')[0]
                    high_res_url = f"{base_url}?auto=format&cs=tinysrgb&fit=max&q=90"
                    media_urls.append(high_res_url)
                    seen_urls.add(high_res_url)
                    if len(media_urls) >= 10:
                        break
        
        # パターン3: 一般的なimgタグから（最終フォールバック）
        if len(media_urls) < 3:
            for img in soup.find_all('img'):
                src = img.get('src', '')
                data_src = img.get('data-src', '')
                url = data_src or src
                
                if url and 'prismic' in url and url not in seen_urls:
                    # ロゴやアイコンを除外
                    if not any(skip in url.lower() for skip in ['logo', 'icon', 'badge', 'brand']):
                        base_url = url.split('?')[0]
                        high_res_url = f"{base_url}?auto=format&cs=tinysrgb&fit=max&q=90"
                        media_urls.append(high_res_url)
                        seen_urls.add(high_res_url)
                        if len(media_urls) >= 10:
                            break
        
        return media_urls[:10]  # 最大10枚
    
    def _scrape_specifications(self, slug: str) -> Dict:
        """Specificationsページから詳細データ取得"""
        specs_url = f"{BASE_URL}/{slug}/specifications"
        
        try:
            specs_resp = self.session.get(specs_url, timeout=30, allow_redirects=False)
            
            if specs_resp.status_code != 200:
                return self._extract_specs_from_main(slug)
            
            specs_soup = BeautifulSoup(specs_resp.text, 'lxml')
            
            # グレードとエンジン情報の取得（重複排除改善版）
            grades_engines = self._extract_grades_engines_without_duplicates(specs_soup)
            
            # 基本スペックの取得
            specifications = self._extract_basic_specs(specs_soup)
            
            return {
                'grades_engines': grades_engines,
                'specifications': specifications
            }
            
        except Exception as e:
            print(f"    Error getting specifications: {e}")
            return self._extract_specs_from_main(slug)
    
    def _extract_grades_engines_without_duplicates(self, soup: BeautifulSoup) -> List[Dict]:
        """重複なしでグレードとエンジン情報を抽出"""
        grades_engines = []
        processed_combinations = {}  # キー: "grade_engine", 値: grade_info辞書
        
        # すべてのトリムセクションを探す
        sections = soup.find_all('article', class_=lambda x: x and 'trim' in str(x) if x else False)
        
        # セクションが見つからない場合は全体を1つのセクションとして扱う
        if not sections:
            sections = [soup]
        
        for section in sections:
            # グレード名を取得
            grade_name = 'Standard'
            grade_elem = section.find('span', class_='trim-article__title-part-2')
            if grade_elem:
                grade_name = grade_elem.get_text(strip=True)
            
            # このセクション内のエンジン情報を全て取得
            engine_divs = section.find_all('div', class_='specification-breakdown__title')
            
            # エンジン情報が見つからない場合はスキップ（N/Aレコードを作らない）
            if not engine_divs:
                # ただし、セクション内に価格やその他の情報がある場合は1つだけ作成
                section_text = section.get_text()
                if 'RRP' in section_text and grade_name != 'Standard':
                    # エンジン情報なしでも価格がある場合は1レコード作成
                    combo_key = f"{grade_name}_NO_ENGINE"
                    if combo_key not in processed_combinations:
                        grade_info = self._create_grade_info(section, grade_name, 'Information not available')
                        processed_combinations[combo_key] = grade_info
                continue
            
            # 各エンジンごとに処理
            for engine_div in engine_divs:
                engine_text = engine_div.get_text(strip=True)
                if not engine_text:
                    continue
                
                # 重複チェック
                combo_key = f"{grade_name}_{engine_text}"
                if combo_key in processed_combinations:
                    # 既存のレコードに情報を追加（マージ）
                    existing = processed_combinations[combo_key]
                    new_info = self._create_grade_info(section, grade_name, engine_text)
                    # 空の値を新しい値で更新
                    for key, value in new_info.items():
                        if not existing.get(key) and value:
                            existing[key] = value
                else:
                    # 新規作成
                    grade_info = self._create_grade_info(section, grade_name, engine_text)
                    processed_combinations[combo_key] = grade_info
        
        # 辞書から値のリストに変換
        grades_engines = list(processed_combinations.values())
        
        # データが全く見つからない場合のフォールバック（1レコードのみ）
        if not grades_engines:
            default_grade = {
                'grade': 'Standard',
                'engine': 'Information not available',
                'engine_price_gbp': None,
                'fuel': '',
                'transmission': '',
                'drive_type': '',
                'power_bhp': None
            }
            grades_engines.append(default_grade)
        
        return grades_engines
    
    def _create_grade_info(self, section, grade_name: str, engine_text: str) -> Dict:
        """グレード情報を作成"""
        grade_info = {
            'grade': grade_name,
            'engine': engine_text,
            'engine_price_gbp': None,
            'fuel': '',
            'transmission': '',
            'drive_type': '',
            'power_bhp': None
        }
        
        # エンジンごとの価格を取得
        section_text = section.get_text()
        
        # RRP価格を探す（複数パターン）
        rrp_patterns = [
            r'RRP\s*£([\d,]+)',
            r'from\s*£([\d,]+)',
            r'Price\s*£([\d,]+)'
        ]
        
        for pattern in rrp_patterns:
            rrp_match = re.search(pattern, section_text)
            if rrp_match:
                grade_info['engine_price_gbp'] = int(rrp_match.group(1).replace(',', ''))
                break
        
        # 仕様詳細を取得
        category_lists = section.find_all('ul', class_='specification-breakdown__category-list')
        
        for category_list in category_lists:
            list_items = category_list.find_all('li', class_='specification-breakdown__category-list-item')
            
            for item in list_items:
                item_text = item.get_text(strip=True)
                
                # トランスミッション
                if not grade_info['transmission']:
                    if 'Automatic' in item_text:
                        grade_info['transmission'] = 'Automatic'
                    elif 'Manual' in item_text:
                        grade_info['transmission'] = 'Manual'
                    elif 'CVT' in item_text:
                        grade_info['transmission'] = 'CVT'
                    elif 'DCT' in item_text:
                        grade_info['transmission'] = 'DCT'
                
                # 駆動方式
                if 'wheel drive' in item_text.lower() and not grade_info['drive_type']:
                    grade_info['drive_type'] = item_text
                
                # パワー
                if 'bhp' in item_text.lower() and not grade_info['power_bhp']:
                    bhp_match = re.search(r'(\d+)\s*bhp', item_text, re.IGNORECASE)
                    if bhp_match:
                        grade_info['power_bhp'] = int(bhp_match.group(1))
        
        # 燃料タイプをエンジン情報から推測
        if engine_text:
            engine_lower = engine_text.lower()
            if 'kwh' in engine_lower or 'electric' in engine_lower:
                grade_info['fuel'] = 'Electric'
                # 電気自動車は通常オートマチック
                if not grade_info['transmission']:
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
        
        # カテゴリリスト内で燃料タイプを探す（上書き）
        for category_list in category_lists:
            list_items = category_list.find_all('li', class_='specification-breakdown__category-list-item')
            for item in list_items:
                item_text = item.get_text(strip=True).lower()
                if 'petrol' in item_text and not grade_info['fuel']:
                    grade_info['fuel'] = 'Petrol'
                elif 'diesel' in item_text and not grade_info['fuel']:
                    grade_info['fuel'] = 'Diesel'
                elif 'electric' in item_text and not grade_info['fuel']:
                    grade_info['fuel'] = 'Electric'
        
        return grade_info
    
    def _extract_basic_specs(self, soup: BeautifulSoup) -> Dict:
        """基本スペックを抽出"""
        specs = {}
        text = soup.get_text()
        
        # ドア数
        doors_match = re.search(r'Number of doors\s*(\d+)', text)
        if doors_match:
            specs['doors'] = int(doors_match.group(1))
        
        # シート数
        seats_match = re.search(r'Number of seats\s*(\d+)', text)
        if seats_match:
            specs['seats'] = int(seats_match.group(1))
        
        # 寸法
        dimensions = []
        for tspan in soup.find_all('tspan'):
            tspan_text = tspan.get_text(strip=True)
            if 'mm' in tspan_text and re.search(r'\d+,?\d*\s*mm', tspan_text):
                dimensions.append(tspan_text)
        
        if len(dimensions) >= 3:
            specs['dimensions_mm'] = f"{dimensions[0]} x {dimensions[1]} x {dimensions[2]}"
        
        # その他の仕様
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
    
    def _get_fallback_body_types(self, model_name: str) -> List[str]:
        """
        フォールバック：モデル名から推測
        TODO: Seleniumでcar-chooserから正確に取得する実装に置き換え
        """
        # 現時点ではフォールバックとして簡易的な推測のみ
        # 実際の実装ではSeleniumを使用してcar-chooserページから取得
        return ['Information pending']  # 明示的に未取得であることを示す
    
    def _extract_specs_from_main(self, slug: str) -> Dict:
        """メインページから仕様を抽出（specificationsページがない場合）"""
        try:
            main_url = f"{BASE_URL}/{slug}"
            main_resp = self.session.get(main_url, timeout=30)
            
            if main_resp.status_code != 200:
                return {'grades_engines': [], 'specifications': {}}
            
            soup = BeautifulSoup(main_resp.text, 'lxml')
            text = soup.get_text()
            
            # デフォルトグレードを作成（最小限の情報のみ）
            grade_info = {
                'grade': 'Standard',
                'engine': 'Information not available',
                'engine_price_gbp': None,
                'fuel': '',
                'transmission': '',
                'drive_type': '',
                'power_bhp': None
            }
            
            # 基本的な情報のみ抽出
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
