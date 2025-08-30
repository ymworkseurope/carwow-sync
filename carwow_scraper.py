#!/usr/bin/env python3
"""
carwow_scraper.py - 正確な要素指定版
指定された要素から正確に情報を取得
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
        # body_typeマッピング用のキャッシュ
        self.body_type_cache = {}
    
    def scrape_vehicle(self, slug: str) -> Optional[Dict]:
        """車両データを取得"""
        
        main_url = f"{BASE_URL}/{slug}"
        main_resp = requests.get(main_url, headers=HEADERS, timeout=30)
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
        
        # ボディタイプの取得（car-chooserページから）
        # 注: 現時点ではSeleniumが必要なため、暫定的な実装
        body_types = self._get_body_types_for_model(make_en, model_en)
        
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
        
        # Cash価格（フォールバック）
        if 'price_min_gbp' not in prices:
            text = soup.get_text()
            cash_match = re.search(r'Cash\s*£([\d,]+)', text)
            if cash_match:
                prices['price_min_gbp'] = int(cash_match.group(1).replace(',', ''))
        
        return prices
    
    def _extract_media_urls_from_slider(self, soup: BeautifulSoup) -> List[str]:
        """media-slider__imageクラスから画像URLを取得"""
        media_urls = []
        seen_urls = set()
        
        # media-slider__imageクラスの画像のみを取得
        slider_images = soup.find_all('img', class_='media-slider__image')
        
        for img in slider_images:
            # srcsetまたはsrcから取得
            url = None
            if img.get('srcset'):
                url = img['srcset'].split(' ')[0]  # 最初のURLを取得
            elif img.get('src'):
                url = img['src']
            
            if url and 'images.prismic.io' in url:
                # 高解像度版のURLを作成（クエリパラメータを調整）
                base_url = url.split('?')[0]
                # 大きなサイズ、高品質で取得
                high_res_url = f"{base_url}?auto=format&fit=max&q=90&w=1920"
                
                if high_res_url not in seen_urls:
                    media_urls.append(high_res_url)
                    seen_urls.add(high_res_url)
        
        # 注: Seleniumが必要な場合は、すべてのスライダー画像を取得できない可能性がある
        
        return media_urls
    
    def _scrape_specifications(self, slug: str) -> Dict:
        """Specificationsページから詳細データ取得"""
        specs_url = f"{BASE_URL}/{slug}/specifications"
        
        try:
            specs_resp = requests.get(specs_url, headers=HEADERS, timeout=30, allow_redirects=False)
            
            if specs_resp.status_code != 200:
                return self._extract_specs_from_main(slug)
            
            specs_soup = BeautifulSoup(specs_resp.text, 'lxml')
            
            # グレードとエンジン情報の取得
            grades_engines = self._extract_grades_engines_from_elements(specs_soup)
            
            # 基本スペックの取得
            specifications = self._extract_basic_specs(specs_soup)
            
            return {
                'grades_engines': grades_engines,
                'specifications': specifications
            }
            
        except Exception as e:
            print(f"    Error getting specifications: {e}")
            return self._extract_specs_from_main(slug)
    
    def _extract_grades_engines_from_elements(self, soup: BeautifulSoup) -> List[Dict]:
        """指定された要素からグレードとエンジン情報を抽出"""
        grades_engines = []
        
        # trim-article要素を探す（各グレードのコンテナ）
        trim_articles = soup.find_all('article', class_=lambda x: x and 'trim-article' in x)
        
        if not trim_articles:
            # articleタグがない場合は別の方法で探す
            trim_sections = soup.find_all('div', class_=lambda x: x and 'trim' in str(x))
        else:
            trim_sections = trim_articles
        
        for section in trim_sections:
            grade_info = {
                'grade': 'Standard',
                'engine': '',
                'engine_price_gbp': None,  # 新しい価格列
                'fuel': '',
                'transmission': '',
                'drive_type': '',
                'power_bhp': None
            }
            
            # グレード名を取得（trim-article__title-part-2から）
            grade_elem = section.find('span', class_='trim-article__title-part-2')
            if grade_elem:
                grade_info['grade'] = grade_elem.get_text(strip=True)
            
            # エンジン情報を取得（specification-breakdown__titleから）
            engine_div = section.find('div', class_='specification-breakdown__title')
            if engine_div:
                # エンジン情報をそのまま使用（編集しない）
                grade_info['engine'] = engine_div.get_text(strip=True)
            
            # エンジンごとの価格を取得（trim-article__rrpから）
            rrp_elem = section.find('span', class_='trim-article__rrp')
            if rrp_elem:
                rrp_text = rrp_elem.get_text(strip=True)
                rrp_match = re.search(r'£([\d,]+)', rrp_text)
                if rrp_match:
                    grade_info['engine_price_gbp'] = int(rrp_match.group(1).replace(',', ''))
            
            # specification-breakdown__category-listから詳細を取得
            category_list = section.find('ul', class_='specification-breakdown__category-list')
            if category_list:
                list_items = category_list.find_all('li', class_='specification-breakdown__category-list-item')
                
                for item in list_items:
                    item_text = item.get_text(strip=True)
                    
                    # トランスミッション（Automatic/Manual等をそのまま）
                    if any(trans in item_text for trans in ['Automatic', 'Manual', 'CVT', 'DCT']):
                        grade_info['transmission'] = item_text
                    
                    # 駆動方式
                    elif 'wheel drive' in item_text.lower():
                        grade_info['drive_type'] = item_text
                    
                    # 燃料タイプ（エンジンサイズと一緒の場合も）
                    elif any(fuel in item_text.lower() for fuel in ['petrol', 'diesel', 'electric', 'hybrid']):
                        if 'petrol' in item_text.lower():
                            grade_info['fuel'] = 'Petrol'
                        elif 'diesel' in item_text.lower():
                            grade_info['fuel'] = 'Diesel'
                        elif 'electric' in item_text.lower():
                            grade_info['fuel'] = 'Electric'
                        elif 'hybrid' in item_text.lower():
                            if 'plug-in' in item_text.lower():
                                grade_info['fuel'] = 'Plug-in Hybrid'
                            else:
                                grade_info['fuel'] = 'Hybrid'
                    
                    # パワー（bhp）
                    bhp_match = re.search(r'(\d+)\s*bhp', item_text, re.IGNORECASE)
                    if bhp_match:
                        grade_info['power_bhp'] = int(bhp_match.group(1))
            
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
            colors_resp = requests.get(colors_url, headers=HEADERS, timeout=30, allow_redirects=False)
            
            if colors_resp.status_code == 200:
                colors_soup = BeautifulSoup(colors_resp.text, 'lxml')
                
                for h4 in colors_soup.find_all('h4', class_='model-hub__colour-details-title'):
                    color_text = h4.get_text(strip=True)
                    # 価格部分を除去
                    color_name = re.sub(r'(Free|£[\d,]+).*$', '', color_text).strip()
                    if color_name and color_name not in colors:
                        colors.append(color_name)
        except:
            pass
        
        return colors
    
    def _get_body_types_for_model(self, make: str, model: str) -> List[str]:
        """
        car-chooserページから車両のボディタイプを取得
        注: 現時点ではSeleniumが必要なため、暫定的な実装
        """
        # TODO: Selenium実装後、以下のロジックを実装
        # 1. 各ボディタイプのcar-chooserページをSeleniumで開く
        # 2. <h3 class="card-compact__title">要素から車両名を取得
        # 3. make + modelが一致する車両のボディタイプを記録
        
        # 暫定的な実装：モデル名から推測
        body_types = []
        model_lower = model.lower()
        
        if 'convertible' in model_lower or 'cabrio' in model_lower:
            body_types.append('Convertibles')
        if 'suv' in model_lower:
            body_types.append('SUVs')
        if 'estate' in model_lower or 'touring' in model_lower:
            body_types.append('Estate cars')
        if 'coupe' in model_lower or 'coupé' in model_lower:
            body_types.append('Coupes')
        
        # Electricは燃料タイプなので、body_typeとしては扱わない
        
        if not body_types:
            body_types.append('Hatchbacks')  # デフォルト
        
        return body_types
    
    def _extract_specs_from_main(self, slug: str) -> Dict:
        """メインページから仕様を抽出（specificationsページがない場合）"""
        try:
            main_url = f"{BASE_URL}/{slug}"
            main_resp = requests.get(main_url, headers=HEADERS, timeout=30)
            
            if main_resp.status_code != 200:
                return {'grades_engines': [], 'specifications': {}}
            
            soup = BeautifulSoup(main_resp.text, 'lxml')
            
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
            resp = requests.get(f"{BASE_URL}/brands", headers=HEADERS, timeout=30)
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
            resp = requests.get(url, headers=HEADERS, timeout=30)
            
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
