#!/usr/bin/env python3
"""
carwow_scraper.py - 完全版
正確な要素から情報を取得する完全なスクレイパー
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
        
        # 価格情報の取得（指定要素から）
        prices = self._extract_prices_from_elements(main_soup)
        
        # メディアURLの取得（media-slider__imageクラスから）
        media_urls = self._extract_media_urls_from_slider(main_soup)
        
        # Specificationsページから詳細データ取得
        specs_data = self._scrape_specifications(slug)
        
        # カラー情報の取得
        colors = self._scrape_colors(slug)
        
        # ボディタイプの取得（モデル名から推測）
        body_types = self._detect_body_types(model_en)
        
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
        """指定された要素から価格情報を抽出"""
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
                    prices['price_max_gbp'] = int(max_price_match.group(1).replace(',', ''))
        
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
    
    def _extract_media_urls_from_slider(self, soup: BeautifulSoup) -> List[str]:
        """media-slider__imageクラスから画像URLを取得"""
        media_urls = []
        seen_urls = set()
        
        # media-slider__imageクラスの画像
        slider_images = soup.find_all('img', class_='media-slider__image')
        
        for img in slider_images:
            url = None
            if img.get('srcset'):
                url = img['srcset'].split(' ')[0]
            elif img.get('src'):
                url = img['src']
            
            if url and 'images.prismic.io' in url:
                base_url = url.split('?')[0]
                high_res_url = f"{base_url}?auto=format&fit=max&q=90&w=1920"
                
                if high_res_url not in seen_urls:
                    media_urls.append(high_res_url)
                    seen_urls.add(high_res_url)
        
        # 追加の画像を探す
        if len(media_urls) < 5:
            for img in soup.find_all('img'):
                src = img.get('src', '')
                if 'prismic' in src and src not in seen_urls:
                    base_url = src.split('?')[0]
                    if 'logo' not in base_url.lower() and 'icon' not in base_url.lower():
                        high_res_url = f"{base_url}?auto=format&fit=max&q=90&w=1920"
                        media_urls.append(high_res_url)
                        seen_urls.add(high_res_url)
                        if len(media_urls) >= 10:
                            break
        
        return media_urls[:10]
    
    def _scrape_specifications(self, slug: str) -> Dict:
        """Specificationsページから詳細データ取得"""
        specs_url = f"{BASE_URL}/{slug}/specifications"
        
        try:
            specs_resp = self.session.get(specs_url, timeout=30, allow_redirects=False)
            
            if specs_resp.status_code != 200:
                return self._extract_specs_from_main(slug)
            
            specs_soup = BeautifulSoup(specs_resp.text, 'lxml')
            
            # グレードとエンジン情報の取得
            grades_engines = self._extract_grades_engines_complete(specs_soup)
            
            # 基本スペックの取得
            specifications = self._extract_basic_specs(specs_soup)
            
            return {
                'grades_engines': grades_engines,
                'specifications': specifications
            }
            
        except Exception as e:
            print(f"    Error getting specifications: {e}")
            return self._extract_specs_from_main(slug)
    
    def _extract_grades_engines_complete(self, soup: BeautifulSoup) -> List[Dict]:
        """完全なグレードとエンジン情報を抽出"""
        grades_engines = []
        processed_combinations = set()
        
        # すべてのグレード/トリムセクションを探す
        sections = []
        
        # パターン1: article.trim-article
        sections.extend(soup.find_all('article', class_=lambda x: x and 'trim-article' in str(x)))
        
        # パターン2: div with trim class
        sections.extend(soup.find_all('div', class_=lambda x: x and 'trim' in str(x) if x else False))
        
        # セクションが見つからない場合は全体を1つのセクションとして扱う
        if not sections:
            sections = [soup]
        
        for section in sections:
            # グレード名を取得
            grade_name = 'Standard'
            grade_elem = section.find('span', class_='trim-article__title-part-2')
            if grade_elem:
                grade_name = grade_elem.get_text(strip=True)
            
            # エンジン情報を取得
            engines = []
            
            # specification-breakdown__titleから取得
            engine_divs = section.find_all('div', class_='specification-breakdown__title')
            for engine_div in engine_divs:
                engine_text = engine_div.get_text(strip=True)
                if engine_text:
                    engines.append(engine_text)
            
            # エンジンが見つからない場合はデフォルト
            if not engines:
                engines = ['N/A']
            
            # 各エンジンごとにレコードを作成
            for engine in engines:
                # 重複チェック
                combo_key = f"{grade_name}_{engine}"
                if combo_key in processed_combinations:
                    continue
                processed_combinations.add(combo_key)
                
                grade_info = {
                    'grade': grade_name,
                    'engine': engine,
                    'engine_price_gbp': None,
                    'fuel': '',
                    'transmission': '',
                    'drive_type': '',
                    'power_bhp': None
                }
                
                # エンジンごとの価格を取得
                rrp_elem = section.find('span', class_='trim-article__rrp')
                if rrp_elem:
                    rrp_text = rrp_elem.get_text(strip=True)
                    rrp_match = re.search(r'£([\d,]+)', rrp_text)
                    if rrp_match:
                        grade_info['engine_price_gbp'] = int(rrp_match.group(1).replace(',', ''))
                
                # 仕様詳細を取得
                category_list = section.find('ul', class_='specification-breakdown__category-list')
                if category_list:
                    list_items = category_list.find_all('li', class_='specification-breakdown__category-list-item')
                    
                    for item in list_items:
                        item_text = item.get_text(strip=True)
                        
                        # トランスミッション
                        if any(trans in item_text for trans in ['Automatic', 'Manual', 'CVT', 'DCT']):
                            grade_info['transmission'] = item_text
                        
                        # 駆動方式
                        elif 'wheel drive' in item_text.lower():
                            grade_info['drive_type'] = item_text
                        
                        # 燃料タイプ
                        elif any(fuel in item_text.lower() for fuel in ['petrol', 'diesel', 'electric', 'hybrid']):
                            if 'petrol' in item_text.lower():
                                grade_info['fuel'] = 'Petrol'
                            elif 'diesel' in item_text.lower():
                                grade_info['fuel'] = 'Diesel'
                            elif 'electric' in item_text.lower():
                                grade_info['fuel'] = 'Electric'
                            elif 'plug-in hybrid' in item_text.lower():
                                grade_info['fuel'] = 'Plug-in Hybrid'
                            elif 'hybrid' in item_text.lower():
                                grade_info['fuel'] = 'Hybrid'
                        
                        # パワー（bhp）
                        bhp_match = re.search(r'(\d+)\s*bhp', item_text, re.IGNORECASE)
                        if bhp_match:
                            grade_info['power_bhp'] = int(bhp_match.group(1))
                
                # 燃料タイプをエンジン情報から推測
                if not grade_info['fuel'] and engine != 'N/A':
                    if 'kWh' in engine:
                        grade_info['fuel'] = 'Electric'
                    elif 'diesel' in engine.lower():
                        grade_info['fuel'] = 'Diesel'
                    elif 'hybrid' in engine.lower():
                        grade_info['fuel'] = 'Hybrid'
                    else:
                        grade_info['fuel'] = 'Petrol'
                
                grades_engines.append(grade_info)
        
        # データが見つからない場合のフォールバック
        if not grades_engines:
            default_grade = {
                'grade': 'Standard',
                'engine': 'N/A',
                'engine_price_gbp': None,
                'fuel': 'N/A',
                'transmission': 'N/A',
                'drive_type': 'N/A',
                'power_bhp': None
            }
            grades_engines.append(default_grade)
        
        return grades_engines
    
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
        
        # 寸法（複数のパターンを試す）
        dimensions = []
        
        # SVG内のtspanから
        for tspan in soup.find_all('tspan'):
            tspan_text = tspan.get_text(strip=True)
            if 'mm' in tspan_text and re.search(r'\d+,?\d*\s*mm', tspan_text):
                dimensions.append(tspan_text)
        
        # 通常のテキストから
        if not dimensions:
            dim_matches = re.findall(r'(\d+,?\d*)\s*mm', text)
            if len(dim_matches) >= 3:
                dimensions = [f"{d} mm" for d in dim_matches[:3]]
        
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
        
        if 'Wheelbase' in text:
            wheelbase_match = re.search(r'Wheelbase\s*([\d.]+)\s*m', text)
            if wheelbase_match:
                specs['wheelbase_m'] = float(wheelbase_match.group(1))
        
        if 'Turning circle' in text:
            turning_match = re.search(r'Turning circle\s*([\d.]+)\s*m', text)
            if turning_match:
                specs['turning_circle_m'] = float(turning_match.group(1))
        
        return specs
    
    def _scrape_colors(self, slug: str) -> List[str]:
        """カラー情報を取得"""
        colors = []
        colors_url = f"{BASE_URL}/{slug}/colours"
        
        try:
            colors_resp = self.session.get(colors_url, timeout=30, allow_redirects=False)
            
            if colors_resp.status_code == 200:
                colors_soup = BeautifulSoup(colors_resp.text, 'lxml')
                
                # model-hub__colour-details-titleクラスから色名を取得
                for h4 in colors_soup.find_all('h4', class_='model-hub__colour-details-title'):
                    color_text = h4.get_text(strip=True)
                    # 価格部分を除去
                    color_name = re.sub(r'(Free|£[\d,]+).*$', '', color_text).strip()
                    if color_name and color_name not in colors:
                        colors.append(color_name)
                
                # 他のパターンも試す
                if not colors:
                    for elem in colors_soup.find_all(class_=lambda x: x and 'color' in str(x).lower()):
                        color_text = elem.get_text(strip=True)
                        if color_text and len(color_text) < 50:
                            color_name = re.sub(r'(Free|£[\d,]+).*$', '', color_text).strip()
                            if color_name and color_name not in colors:
                                colors.append(color_name)
        except Exception as e:
            print(f"    Error getting colors: {e}")
        
        return colors
    
    def _detect_body_types(self, model_name: str) -> List[str]:
        """モデル名からボディタイプを推測"""
        body_types = []
        model_lower = model_name.lower()
        
        # キーワードマッピング
        body_type_keywords = {
            'Convertibles': ['convertible', 'cabrio', 'roadster', 'spider'],
            'SUVs': ['suv', 'cross', '4x4'],
            'Estate cars': ['estate', 'touring', 'avant', 'wagon'],
            'Coupes': ['coupe', 'coupé'],
            'Saloons': ['saloon', 'sedan'],
            'Hatchbacks': ['hatchback', 'hatch'],
            'Sports Cars': ['sport', 'gti', 'gt', 'rs', 'amg', 'm3', 'm5'],
            'People Carriers': ['mpv', 'carrier', 'van']
        }
        
        for body_type, keywords in body_type_keywords.items():
            for keyword in keywords:
                if keyword in model_lower:
                    if body_type not in body_types:
                        body_types.append(body_type)
                    break
        
        # デフォルト
        if not body_types:
            body_types.append('Hatchbacks')
        
        return body_types
    
    def _extract_specs_from_main(self, slug: str) -> Dict:
        """メインページから仕様を抽出（specificationsページがない場合）"""
        try:
            main_url = f"{BASE_URL}/{slug}"
            main_resp = self.session.get(main_url, timeout=30)
            
            if main_resp.status_code != 200:
                return {'grades_engines': [], 'specifications': {}}
            
            soup = BeautifulSoup(main_resp.text, 'lxml')
            text = soup.get_text()
            
            # デフォルトグレードを作成
            grade_info = {
                'grade': 'Standard',
                'engine': 'N/A',
                'engine_price_gbp': None,
                'fuel': 'N/A',
                'transmission': 'N/A',
                'drive_type': 'N/A',
                'power_bhp': None
            }
            
            # テキストから情報を抽出
            if 'electric' in text.lower():
                grade_info['fuel'] = 'Electric'
                grade_info['transmission'] = 'Automatic'
            elif 'hybrid' in text.lower():
                grade_info['fuel'] = 'Hybrid'
            elif 'diesel' in text.lower():
                grade_info['fuel'] = 'Diesel'
            else:
                grade_info['fuel'] = 'Petrol'
            
            # エンジン情報を探す
            engine_patterns = [
                r'(\d+kW\s+[\d.]+kWh)',
                r'(\d+\.?\d*)\s*litre',
                r'(\d+\.?\d*)L\s+\w+',
                r'(\d+)cc'
            ]
            
            for pattern in engine_patterns:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    grade_info['engine'] = match.group(0)
                    break
            
            # パワーを探す
            hp_match = re.search(r'(\d+)\s*(?:bhp|hp)', text, re.IGNORECASE)
            if hp_match:
                grade_info['power_bhp'] = int(hp_match.group(1))
            
            # 基本スペック
            specs = {}
            
            # ドア数を推定
            if 'coupe' in text.lower() or 'coupé' in text.lower():
                specs['doors'] = 2
            elif 'five-door' in text.lower() or '5-door' in text.lower():
                specs['doors'] = 5
            elif 'three-door' in text.lower() or '3-door' in text.lower():
                specs['doors'] = 3
            else:
                specs['doors'] = 4
            
            # シート数を推定
            seats_match = re.search(r'(\d+)[\s-]?seat', text, re.IGNORECASE)
            if seats_match:
                specs['seats'] = int(seats_match.group(1))
            else:
                specs['seats'] = 5
            
            return {
                'grades_engines': [grade_info],
                'specifications': specs
            }
            
        except Exception as e:
            print(f"    Error extracting from main: {e}")
            return {
                'grades_engines': [{
                    'grade': 'Standard',
                    'engine': 'N/A',
                    'engine_price_gbp': None,
                    'fuel': 'N/A',
                    'transmission': 'N/A',
                    'drive_type': 'N/A',
                    'power_bhp': None
                }],
                'specifications': {}
            }
    
    def get_all_makers(self) -> List[str]:
        """brandsページからメーカー一覧を取得"""
        makers = []
        
        try:
            resp = self.session.get(f"{BASE_URL}/brands", timeout=30)
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, 'lxml')
                
                # brands-list__group-item-title-nameクラスから取得
                for brand_div in soup.find_all('div', class_='brands-list__group-item-title-name'):
                    brand_name = brand_div.get_text(strip=True).lower()
                    brand_slug = brand_name.replace(' ', '-')
                    if brand_slug and brand_slug not in makers:
                        makers.append(brand_slug)
                
                # フォールバック：リンクから取得
                if not makers:
                    for link in soup.find_all('a', href=True):
                        href = link['href']
                        if href.startswith('/') and href.count('/') == 1:
                            maker = href[1:]
                            if maker and not any(x in maker for x in ['brands', 'news', 'reviews', 'editorial', 'deals']):
                                if maker not in makers:
                                    makers.append(maker)
        except Exception as e:
            print(f"Error getting makers: {e}")
        
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
            resp = self.session.get(url, timeout=30)
            
            if resp.status_code != 200:
                return models
            
            soup = BeautifulSoup(resp.text, 'lxml')
            
            # article.card-compactから取得
            articles = soup.find_all('article', class_='card-compact')
            
            for article in articles:
                # リンクを探す
                for link in article.find_all('a', href=True):
                    href = link['href']
                    # URLからモデルを抽出
                    if f'/{maker}/' in href:
                        # URLパース
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
            
            # articleで見つからない場合、すべてのaタグから取得
            if not models:
                all_links = soup.find_all('a', href=True)
                
                for link in all_links:
                    href = link['href']
                    if f'/{maker}/' in href:
                        # 除外パターン
                        if any(skip in href for skip in ['/news/', '/reviews/', '/deals/', '/colours', '/specifications']):
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
