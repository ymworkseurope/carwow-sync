#!/usr/bin/env python3
"""
carwow_scraper.py - 改良版
正確なHTML要素から情報を取得
"""
import re
import json
import time
from typing import Dict, List, Optional, Tuple
from bs4 import BeautifulSoup
import requests

BASE_URL = "https://www.carwow.co.uk"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

class CarwowScraper:
    
    def scrape_vehicle(self, slug: str) -> Optional[Dict]:
        """車両データを取得（改良版）"""
        
        main_url = f"{BASE_URL}/{slug}"
        main_resp = requests.get(main_url, headers=HEADERS, timeout=30)
        if main_resp.status_code != 200:
            return None
        
        main_soup = BeautifulSoup(main_resp.text, 'lxml')
        
        # 基本情報の取得
        make_en, model_en = self._extract_make_model(slug, main_soup)
        
        # overview_enの取得（emタグから）
        overview_en = self._extract_overview(main_soup)
        
        # 価格情報の取得
        prices = self._extract_prices(main_soup)
        
        # メディアURLの取得（高解像度版）
        media_urls = self._extract_media_urls(main_soup)
        
        # Specificationsページから詳細データ取得
        specs_data = self._scrape_specifications(slug)
        
        # カラー情報の取得
        colors = self._scrape_colors(slug)
        
        # ボディタイプの検出（car-chooserページから）
        body_types = self._detect_body_types(make_en, model_en)
        
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
        # slugから基本的なメーカー名を取得
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
            # "Abarth 500e Convertible Review" のようなタイトルからモデル名を抽出
            if 'Review' in title_text:
                model_part = title_text.split('Review')[0].strip()
                # メーカー名を除去
                model_en = model_part.replace(make_en, '').strip()
            elif '|' in title_text:
                model_part = title_text.split('|')[0].strip()
                model_en = model_part.replace(make_en, '').strip()
        
        return make_en, model_en
    
    def _extract_overview(self, soup: BeautifulSoup) -> str:
        """overview_enをemタグから取得"""
        # emタグを探す（最初の長い説明文）
        em_tag = soup.find('em')
        if em_tag:
            text = em_tag.get_text(strip=True)
            # 長さチェック（短すぎる場合は説明文ではない可能性）
            if len(text) > 50:
                return text
        
        # フォールバック：meta descriptionから
        meta = soup.find('meta', {'name': 'description'})
        if meta:
            return meta.get('content', '')
        
        return ''
    
    def _extract_prices(self, soup: BeautifulSoup) -> Dict:
        """価格情報を抽出"""
        prices = {}
        text = soup.get_text()
        
        # Cash価格
        cash_match = re.search(r'Cash\s*£([\d,]+)', text)
        if cash_match:
            prices['price_min_gbp'] = int(cash_match.group(1).replace(',', ''))
        
        # Used価格  
        used_match = re.search(r'Used\s*£([\d,]+)', text)
        if used_match:
            prices['price_used_gbp'] = int(used_match.group(1).replace(',', ''))
        
        # RRP範囲
        rrp_match = re.search(r'RRP.*?£([\d,]+)\s*to\s*£([\d,]+)', text)
        if rrp_match:
            if not prices.get('price_min_gbp'):
                prices['price_min_gbp'] = int(rrp_match.group(1).replace(',', ''))
            prices['price_max_gbp'] = int(rrp_match.group(2).replace(',', ''))
        
        return prices
    
    def _extract_media_urls(self, soup: BeautifulSoup) -> List[str]:
        """高解像度のメディアURLを取得"""
        media_urls = []
        seen_urls = set()
        
        # media-slider__imageクラスの画像を探す
        for img in soup.find_all('img', class_='media-slider__image'):
            # srcsetがある場合は最高解像度を取得
            if img.get('srcset'):
                src = img['srcset']
                # クエリパラメータを除去して高解像度版のURLを作成
                if 'images.prismic.io' in src:
                    base_url = src.split('?')[0]
                    high_res_url = f"{base_url}?auto=format&fit=max&q=90"
                    if high_res_url not in seen_urls:
                        media_urls.append(high_res_url)
                        seen_urls.add(high_res_url)
            elif img.get('src'):
                src = img['src']
                if 'images.prismic.io' in src:
                    base_url = src.split('?')[0]
                    high_res_url = f"{base_url}?auto=format&fit=max&q=90"
                    if high_res_url not in seen_urls:
                        media_urls.append(high_res_url)
                        seen_urls.add(high_res_url)
        
        # 追加の画像を探す（通常のimgタグ）
        if len(media_urls) < 5:
            for img in soup.find_all('img', src=True):
                src = img['src']
                if ('carwow' in src or 'prismic' in src) and src not in seen_urls:
                    if 'logo' not in src.lower() and 'icon' not in src.lower():
                        media_urls.append(src)
                        seen_urls.add(src)
                        if len(media_urls) >= 10:
                            break
        
        return media_urls[:10]  # 最大10枚
    
    def _scrape_specifications(self, slug: str) -> Dict:
        """Specificationsページから詳細データ取得"""
        specs_url = f"{BASE_URL}/{slug}/specifications"
        
        try:
            specs_resp = requests.get(specs_url, headers=HEADERS, timeout=30, allow_redirects=False)
            
            if specs_resp.status_code != 200:
                # specificationsページがない場合
                return self._extract_specs_from_main(slug)
            
            specs_soup = BeautifulSoup(specs_resp.text, 'lxml')
            
            # グレードとエンジン情報の取得
            grades_engines = self._extract_grades_engines(specs_soup)
            
            # 基本スペックの取得
            specifications = self._extract_basic_specs(specs_soup)
            
            return {
                'grades_engines': grades_engines,
                'specifications': specifications
            }
            
        except Exception as e:
            print(f"    Error getting specifications: {e}")
            return self._extract_specs_from_main(slug)
    
    def _extract_grades_engines(self, soup: BeautifulSoup) -> List[Dict]:
        """グレードとエンジン情報を抽出"""
        grades_engines = []
        
        # trim-article__title-part-2からグレード名を取得
        grade_elements = soup.find_all('span', class_='trim-article__title-part-2')
        
        for grade_elem in grade_elements:
            grade_name = grade_elem.get_text(strip=True)
            
            # このグレードに関連する情報を収集
            grade_info = {
                'grade': grade_name,
                'engine': '',
                'price_min_gbp': None,
                'fuel': '',
                'transmission': '',
                'drive_type': '',
                'power_bhp': None
            }
            
            # 親要素から詳細情報を探す
            parent = grade_elem.find_parent('div', recursive=True)
            if parent:
                parent_text = parent.get_text()
                
                # RRP価格を探す
                rrp_match = re.search(r'RRP\s*£([\d,]+)', parent_text)
                if rrp_match:
                    grade_info['price_min_gbp'] = int(rrp_match.group(1).replace(',', ''))
                
                # エンジン情報を探す（114kW 42.2kWhのような形式）
                engine_match = re.search(r'(\d+kW\s+[\d.]+kWh)', parent_text)
                if engine_match:
                    grade_info['engine'] = engine_match.group(1)
            
            # specification-breakdown__category-list-itemから詳細情報を取得
            breakdown_items = soup.find_all('li', class_='specification-breakdown__category-list-item')
            
            for item in breakdown_items:
                item_text = item.get_text(strip=True)
                
                # 燃料タイプ
                if any(fuel in item_text.lower() for fuel in ['petrol', 'diesel', 'electric', 'hybrid']):
                    if 'petrol' in item_text.lower():
                        grade_info['fuel'] = 'Petrol'
                    elif 'diesel' in item_text.lower():
                        grade_info['fuel'] = 'Diesel'
                    elif 'electric' in item_text.lower():
                        grade_info['fuel'] = 'Electric'
                    elif 'hybrid' in item_text.lower():
                        grade_info['fuel'] = 'Hybrid'
                    
                    # エンジンサイズも含まれることがある（1.6 L Petrol）
                    if 'L' in item_text:
                        if not grade_info['engine']:
                            grade_info['engine'] = item_text
                
                # トランスミッション
                if any(trans in item_text.lower() for trans in ['automatic', 'manual', 'cvt', 'dct']):
                    grade_info['transmission'] = item_text
                
                # 駆動方式
                if 'wheel drive' in item_text.lower():
                    grade_info['drive_type'] = item_text
                
                # パワー（bhp）
                bhp_match = re.search(r'(\d+)\s*bhp', item_text, re.IGNORECASE)
                if bhp_match:
                    grade_info['power_bhp'] = int(bhp_match.group(1))
            
            grades_engines.append(grade_info)
        
        # グレード情報が見つからない場合のフォールバック
        if not grades_engines:
            # デフォルトグレードを作成
            default_grade = {
                'grade': 'Standard',
                'engine': '',
                'price_min_gbp': None,
                'fuel': self._detect_fuel_from_text(soup.get_text()),
                'transmission': self._detect_transmission_from_text(soup.get_text()),
                'drive_type': self._detect_drive_type_from_text(soup.get_text()),
                'power_bhp': self._extract_power_from_text(soup.get_text())
            }
            
            # エンジン情報を探す
            text = soup.get_text()
            engine_match = re.search(r'(\d+kW\s+[\d.]+kWh|\d+\.?\d*\s*L\s+\w+)', text)
            if engine_match:
                default_grade['engine'] = engine_match.group(1)
            
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
        
        # 寸法（SVG内のtspanから）
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
            colors_resp = requests.get(colors_url, headers=HEADERS, timeout=30, allow_redirects=False)
            
            if colors_resp.status_code == 200:
                colors_soup = BeautifulSoup(colors_resp.text, 'lxml')
                
                # model-hub__colour-details-titleクラスから色名を取得
                for h4 in colors_soup.find_all('h4', class_='model-hub__colour-details-title'):
                    color_text = h4.get_text(strip=True)
                    # 価格部分を除去
                    color_name = re.sub(r'(Free|£[\d,]+).*$', '', color_text).strip()
                    if color_name and color_name not in colors:
                        colors.append(color_name)
        except:
            pass
        
        return colors
    
    def _detect_body_types(self, make: str, model: str) -> List[str]:
        """車両のボディタイプを検出"""
        body_types = []
        
        # モデル名からヒントを得る
        model_lower = model.lower()
        
        if 'convertible' in model_lower or 'cabrio' in model_lower or 'roadster' in model_lower:
            body_types.append('Convertible')
        if 'suv' in model_lower or 'cross' in model_lower:
            body_types.append('SUV')
        if 'estate' in model_lower or 'touring' in model_lower or 'avant' in model_lower:
            body_types.append('Estate')
        if 'coupe' in model_lower or 'coupé' in model_lower:
            body_types.append('Coupe')
        if 'saloon' in model_lower or 'sedan' in model_lower:
            body_types.append('Saloon')
        
        # ElectricやHybridは別途判定
        if 'electric' in model_lower or 'e-' in model_lower or '-e' in model_lower:
            if 'Electric' not in body_types:
                body_types.append('Electric')
        
        # デフォルトでHatchbackの可能性を考慮
        if not body_types:
            # 小型車の場合はHatchbackの可能性が高い
            small_car_makes = ['fiat', 'mini', 'smart', 'toyota', 'honda', 'mazda']
            if make.lower() in small_car_makes:
                body_types.append('Hatchback')
        
        return body_types
    
    def _extract_specs_from_main(self, slug: str) -> Dict:
        """メインページから仕様を抽出（specificationsページがない場合）"""
        try:
            main_url = f"{BASE_URL}/{slug}"
            main_resp = requests.get(main_url, headers=HEADERS, timeout=30)
            
            if main_resp.status_code != 200:
                return {'grades_engines': [], 'specifications': {}}
            
            soup = BeautifulSoup(main_resp.text, 'lxml')
            text = soup.get_text()
            
            # デフォルトグレードを作成
            grade_info = {
                'grade': 'Standard',
                'engine': '',
                'price_min_gbp': None,
                'fuel': self._detect_fuel_from_text(text),
                'transmission': self._detect_transmission_from_text(text),
                'drive_type': self._detect_drive_type_from_text(text),
                'power_bhp': self._extract_power_from_text(text)
            }
            
            # エンジン情報を探す
            engine_patterns = [
                r'(\d+\.?\d*)\s*litre',
                r'(\d+\.?\d*)L\s+\w+',
                r'(\d+)cc',
                r'(\d+kW\s+[\d.]+kWh)'
            ]
            
            for pattern in engine_patterns:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    grade_info['engine'] = match.group(0)
                    break
            
            # 基本スペック
            specs = {}
            
            # ドア数を推定
            if 'coupe' in text.lower() or 'coupé' in text.lower():
                specs['doors'] = 2
            elif 'five-door' in text.lower() or '5-door' in text.lower():
                specs['doors'] = 5
            elif 'three-door' in text.lower() or '3-door' in text.lower():
                specs['doors'] = 3
            
            # シート数を推定
            seats_match = re.search(r'(\d+)[\s-]?seat', text, re.IGNORECASE)
            if seats_match:
                specs['seats'] = int(seats_match.group(1))
            
            return {
                'grades_engines': [grade_info],
                'specifications': specs
            }
            
        except:
            return {'grades_engines': [], 'specifications': {}}
    
    def _detect_fuel_from_text(self, text: str) -> str:
        """テキストから燃料タイプを検出"""
        text_lower = text.lower()
        
        if 'electric' in text_lower or 'ev' in text_lower or 'kwh' in text_lower:
            return 'Electric'
        elif 'plug-in hybrid' in text_lower or 'phev' in text_lower:
            return 'Plug-in Hybrid'
        elif 'hybrid' in text_lower:
            return 'Hybrid'
        elif 'diesel' in text_lower:
            return 'Diesel'
        elif 'petrol' in text_lower or 'gasoline' in text_lower:
            return 'Petrol'
        
        return 'Petrol'  # デフォルト
    
    def _detect_transmission_from_text(self, text: str) -> str:
        """テキストからトランスミッションを検出"""
        text_lower = text.lower()
        
        if 'automatic' in text_lower or 'auto' in text_lower:
            return 'Automatic'
        elif 'manual' in text_lower:
            return 'Manual'
        elif 'cvt' in text_lower:
            return 'CVT'
        elif 'dct' in text_lower or 'dual-clutch' in text_lower:
            return 'DCT'
        
        # 電気自動車はデフォルトで自動
        if 'electric' in text_lower:
            return 'Automatic'
        
        return ''
    
    def _detect_drive_type_from_text(self, text: str) -> str:
        """テキストから駆動方式を検出"""
        text_lower = text.lower()
        
        if 'all-wheel drive' in text_lower or 'awd' in text_lower:
            return 'All-wheel drive'
        elif 'four-wheel drive' in text_lower or '4wd' in text_lower:
            return 'Four-wheel drive'
        elif 'rear-wheel drive' in text_lower or 'rwd' in text_lower:
            return 'Rear-wheel drive'
        elif 'front-wheel drive' in text_lower or 'fwd' in text_lower:
            return 'Front-wheel drive'
        
        return 'Front-wheel drive'  # デフォルト
    
    def _extract_power_from_text(self, text: str) -> Optional[int]:
        """テキストからパワー（馬力）を抽出"""
        # bhp/hp形式
        hp_match = re.search(r'(\d+)\s*(?:bhp|hp)', text, re.IGNORECASE)
        if hp_match:
            return int(hp_match.group(1))
        
        # kW形式（変換が必要）
        kw_match = re.search(r'(\d+)\s*kW', text)
        if kw_match:
            kw = int(kw_match.group(1))
            return int(kw * 1.341)  # kW to hp
        
        return None
    
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
                        # URLパースしてモデル部分を取得
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
                    if f'/{maker}/' in href and 'review' in link.get_text('').lower():
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
