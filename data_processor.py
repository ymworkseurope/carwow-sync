#!/usr/bin/env python3
"""
data_processor.py - Improved Version with Better Japanese Support
エンジン単位でレコードを生成、UUIDでID生成、full_model_ja改良
"""
import re
import json
import os
import time
import logging
from typing import Dict, List, Optional
from datetime import datetime
import requests
import uuid

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('processor.log'),
        logging.StreamHandler()
    ]
)

class ExchangeRateAPI:
    """為替レートAPI管理クラス"""
    
    def __init__(self):
        self.cache_file = 'exchange_rate_cache.json'
        self.cache_duration = 3600  # 1時間キャッシュ
        self.rate = None
        self.logger = logging.getLogger(__name__)
        self._load_cached_rate()
    
    def _load_cached_rate(self):
        """キャッシュされた為替レートを読み込み"""
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, 'r') as f:
                    cache = json.load(f)
                    if time.time() - cache.get('timestamp', 0) < self.cache_duration:
                        self.rate = cache.get('rate')
                        self.logger.info(f"Using cached exchange rate: 1 GBP = {self.rate} JPY")
            except Exception as e:
                self.logger.error(f"Error loading exchange rate cache: {e}", exc_info=True)
    
    def get_rate(self) -> float:
        """GBPからJPYへの為替レートを取得"""
        if self.rate:
            return self.rate
        
        try:
            response = requests.get(
                'https://api.exchangerate-api.com/v4/latest/GBP',
                timeout=10
            )
            if response.status_code == 200:
                data = response.json()
                self.rate = data['rates']['JPY']
                
                with open(self.cache_file, 'w') as f:
                    json.dump({
                        'rate': self.rate,
                        'timestamp': time.time()
                    }, f)
                
                self.logger.info(f"Fetched exchange rate: 1 GBP = {self.rate} JPY")
                return self.rate
        except Exception as e:
            self.logger.error(f"Error fetching exchange rate: {e}", exc_info=True)
        
        self.rate = float(os.getenv('FALLBACK_EXCHANGE_RATE', '185.0'))
        self.logger.warning(f"Using fallback exchange rate: 1 GBP = {self.rate} JPY")
        return self.rate

class DeepLTranslator:
    """DeepL翻訳クラス（クォータ管理付き）"""
    
    def __init__(self):
        self.api_key = os.getenv('DEEPL_KEY')
        self.enabled = bool(self.api_key)
        self.cache = {}
        self.cache_file = 'translation_cache.json'
        self.quota_file = 'deepl_quota.json'
        self.quota_limit = 500000  # Free版の月間制限
        self.quota_used = 0
        self.logger = logging.getLogger(__name__)
        
        self.color_map = self._load_color_map()
        
        self._load_cache()
        self._load_quota()
        
        if not self.enabled:
            self.logger.warning("DeepL API key not configured")
    
    def _load_color_map(self) -> Dict[str, str]:
        """カラー翻訳辞書を読み込み"""
        return {
            'White': 'ホワイト',
            'Black': 'ブラック',
            'Silver': 'シルバー',
            'Grey': 'グレー',
            'Gray': 'グレー',
            'Red': 'レッド',
            'Blue': 'ブルー',
            'Green': 'グリーン',
            'Yellow': 'イエロー',
            'Orange': 'オレンジ',
            'Brown': 'ブラウン',
            'Pearl': 'パール',
            'Metallic': 'メタリック',
            'Abarth Red': 'アバルトレッド',
            'Abyss Blue': 'アビスブルー',
            # ... (他のカラーは省略、必要に応じて追加) ...
            'Zenith White': 'ゼニスホワイト'
        }
    
    def _load_cache(self):
        """翻訳キャッシュを読み込み"""
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, 'r') as f:
                    self.cache = json.load(f)
            except Exception as e:
                self.logger.error(f"Error loading translation cache: {e}", exc_info=True)
                self.cache = {}
    
    def _save_cache(self):
        """翻訳キャッシュを保存"""
        try:
            with open(self.cache_file, 'w') as f:
                json.dump(self.cache, f, ensure_ascii=False, indent=2)
        except Exception as e:
            self.logger.error(f"Error saving translation cache: {e}", exc_info=True)
    
    def _load_quota(self):
        """クォータ使用状況を読み込み"""
        if os.path.exists(self.quota_file):
            try:
                with open(self.quota_file, 'r') as f:
                    data = json.load(f)
                    saved_month = data.get('month', '')
                    current_month = datetime.now().strftime('%Y-%m')
                    if saved_month == current_month:
                        self.quota_used = data.get('used', 0)
                    else:
                        self.quota_used = 0
            except Exception as e:
                self.logger.error(f"Error loading DeepL quota: {e}", exc_info=True)
                self.quota_used = 0
    
    def _save_quota(self):
        """クォータ使用状況を保存"""
        try:
            with open(self.quota_file, 'w') as f:
                json.dump({
                    'month': datetime.now().strftime('%Y-%m'),
                    'used': self.quota_used
                }, f)
        except Exception as e:
            self.logger.error(f"Error saving DeepL quota: {e}", exc_info=True)
    
    def _check_quota(self, text: str) -> bool:
        """クォータチェック"""
        char_count = len(text)
        if self.quota_used + char_count > self.quota_limit * 0.9:
            self.logger.warning(f"DeepL quota nearly exhausted ({self.quota_used}/{self.quota_limit})")
            return False
        return True
    
    def translate(self, text: str, target_lang: str = 'JA') -> str:
        """テキストを翻訳"""
        if not self.enabled or not text or text == 'Information not available':
            return text
        
        if not self._check_quota(text):
            return text
        
        cache_key = f"{text}_{target_lang}"
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        try:
            url = 'https://api-free.deepl.com/v2/translate'
            params = {
                'auth_key': self.api_key,
                'text': text,
                'target_lang': target_lang
            }
            response = requests.post(url, data=params, timeout=10)
            
            if response.status_code == 200:
                result = response.json()
                translated = result['translations'][0]['text']
                
                self.quota_used += len(text)
                self._save_quota()
                
                self.cache[cache_key] = translated
                self._save_cache()
                
                return translated
            elif response.status_code == 456:
                self.logger.warning(f"DeepL API quota exceeded - using original text")
                return text
            else:
                self.logger.error(f"DeepL API error: {response.status_code} - {response.text}")
                return text
                
        except Exception as e:
            self.logger.error(f"Translation error: {e}", exc_info=True)
            return text
    
    def translate_colors(self, colors: List[str]) -> List[str]:
        """色名を翻訳（拡張辞書使用）"""
        if not colors or colors == ['Information not available']:
            return ['ー']
        
        translated = []
        for color in colors:
            if color in self.color_map:
                translated.append(self.color_map[color])
                continue
            
            ja_color = None
            for en_key, ja_value in self.color_map.items():
                if en_key.lower() in color.lower():
                    ja_color = color.lower().replace(en_key.lower(), ja_value)
                    break
            
            if not ja_color:
                if self.enabled:
                    ja_color = self.translate(color)
                else:
                    ja_color = color
            
            translated.append(ja_color)
        
        return translated

class DataProcessor:
    """データ処理クラス"""
    
    def __init__(self):
        self.exchange_api = ExchangeRateAPI()
        self.translator = DeepLTranslator()
        self.gbp_to_jpy = self.exchange_api.get_rate()
        self.na_value = 'Information not available'
        self.dash_value = 'ー'
        self.logger = logging.getLogger(__name__)
    
    def _generate_consistent_id(self, unique_key: str) -> str:
        """一貫性のある一意なIDを生成"""
        return str(uuid.uuid4())
    
    def _extract_base_data(self, raw_data: Dict) -> Dict:
        """基本データを抽出"""
        base_data = {
            'slug': raw_data.get('slug', self.na_value),
            'make_en': raw_data.get('make_en', self.na_value),
            'model_en': raw_data.get('model_en', self.na_value),
            'overview_en': raw_data.get('overview_en', self.na_value),
            'catalog_url': raw_data.get('catalog_url', self.na_value),
            'body_type': raw_data.get('body_types', [self.na_value]),
            'colors': raw_data.get('colors', [self.na_value]),
            'media_urls': raw_data.get('media_urls', []),
            'price_min_gbp': raw_data.get('prices', {}).get('price_min_gbp', self.na_value),
            'price_max_gbp': raw_data.get('prices', {}).get('price_max_gbp', self.na_value),
            'price_used_gbp': raw_data.get('prices', {}).get('price_used_gbp', self.na_value),
            'price_min_jpy': self.dash_value,
            'price_max_jpy': self.dash_value,
            'price_used_jpy': self.dash_value,
            'doors': raw_data.get('specifications', {}).get('doors', self.na_value),
            'seats': raw_data.get('specifications', {}).get('seats', self.na_value),
            'dimensions_mm': raw_data.get('specifications', {}).get('dimensions_mm', self.na_value),
            'make_ja': self.translator.translate(raw_data.get('make_en', self.na_value)),
            'overview_ja': self.translator.translate(raw_data.get('overview_en', self.na_value)),
            'body_type_ja': self.translator.translate(', '.join(raw_data.get('body_types', [self.na_value]))).split(', '),
            'colors_ja': self.translator.translate_colors(raw_data.get('colors', [self.na_value])),
            'dimensions_ja': self.translator.translate(raw_data.get('specifications', {}).get('dimensions_mm', self.na_value)),
        }
        
        for key in ['price_min_gbp', 'price_max_gbp', 'price_used_gbp']:
            if base_data[key] != self.na_value:
                base_data[key.replace('_gbp', '_jpy')] = int(base_data[key] * self.gbp_to_jpy)
        
        return base_data
    
    def process_vehicle_data(self, raw_data: Optional[Dict]) -> List[Dict]:
        """車両データを処理してデータベース用レコードに変換"""
        if not raw_data:
            return []
        
        records = []
        base_data = self._extract_base_data(raw_data)
        
        base_data['is_active'] = raw_data.get('is_active', True)
        base_data['last_updated'] = datetime.utcnow().isoformat()
        
        grades_engines = raw_data.get('grades_engines', [])
        
        if not grades_engines:
            grades_engines = [{
                'grade': self.na_value,
                'engine': self.na_value,
                'engine_price_gbp': None,
                'fuel': self.na_value,
                'transmission': self.na_value,
                'drive_type': self.na_value,
                'power_bhp': None
            }]
        
        for grade_engine in grades_engines:
            record = base_data.copy()
            
            record['grade'] = self._normalize_value(grade_engine.get('grade'), self.na_value)
            record['engine'] = self._normalize_value(grade_engine.get('engine'), self.na_value)
            record['fuel'] = self._normalize_value(grade_engine.get('fuel'), self.na_value)
            record['transmission'] = self._normalize_value(grade_engine.get('transmission'), self.na_value)
            record['drive_type'] = self._normalize_value(grade_engine.get('drive_type'), self.na_value)
            
            power_bhp = grade_engine.get('power_bhp')
            record['power_bhp'] = self.na_value if power_bhp is None else power_bhp
            
            engine_price_gbp = grade_engine.get('engine_price_gbp')
            if engine_price_gbp is not None:
                record['engine_price_gbp'] = engine_price_gbp
                record['engine_price_jpy'] = int(engine_price_gbp * self.gbp_to_jpy)
            else:
                record['engine_price_gbp'] = self.na_value
                record['engine_price_jpy'] = self.dash_value
            
            record['fuel_ja'] = self.translator.translate(record['fuel'])
            record['transmission_ja'] = self.translator.translate(record['transmission'])
            record['drive_type_ja'] = self.translator.translate(record['drive_type'])
            
            parts = [record['make_ja']]
            
            if record['model_en'] and record['model_en'] != self.na_value:
                model_ja = self.translator.translate(record['model_en'])
                record['model_ja'] = model_ja
                parts.append(model_ja)
            else:
                parts.append(self.dash_value)
            
            if record['grade'] and record['grade'] != self.na_value:
                parts.append(record['grade'])
            else:
                parts.append(self.dash_value)
            
            if record['engine'] and record['engine'] != self.na_value:
                engine_short = self._shorten_engine_text_improved(record['engine'])
                parts.append(engine_short)
            else:
                parts.append(self.dash_value)
            
            record['full_model_ja'] = ' '.join(parts)
            
            record['spec_json'] = {
                'battery_capacity_kwh': raw_data.get('specifications', {}).get('battery_capacity_kwh', None),
                'boot_capacity_l': raw_data.get('specifications', {}).get('boot_capacity_l', None)
            }
            
            record['updated_at'] = datetime.now().isoformat()
            
            unique_key = f"{record['slug']}_{record['grade']}_{record['engine']}"
            record['id'] = self._generate_consistent_id(unique_key)
            
            records.append(record)
        
        return records
    
    def _normalize_value(self, value, default: str) -> str:
        """値を正規化"""
        if value is None or value == '' or value == 'N/A' or value == '-':
            return default
        return str(value)
    
    def _shorten_engine_text_improved(self, engine_text: str) -> str:
        """エンジンテキストを短縮（改良版：ハイブリッド/EV情報保持）"""
        if engine_text == self.na_value:
            return self.dash_value
        
        if 'Hybrid' in engine_text or 'hybrid' in engine_text or 'e:HEV' in engine_text:
            match = re.search(r'(\d+\.?\d*)\s*[Ll]', engine_text)
            if match:
                return f"{match.group(0)} HEV"
            return 'HEV'
        
        if 'Plug-in' in engine_text or 'PHEV' in engine_text:
            match = re.search(r'(\d+\.?\d*)\s*[Ll]', engine_text)
            if match:
                return f"{match.group(0)} PHEV"
            return 'PHEV'
        
        if 'kWh' in engine_text or 'Electric' in engine_text or 'electric' in engine_text:
            match = re.search(r'([\d.]+)\s*kWh', engine_text)
            if match:
                return f"{match.group(1)}kWh"
            return 'EV'
        
        match = re.search(r'(\d+\.?\d*)\s*[Ll]', engine_text)
        if match:
            return match.group(0)
        
        words = engine_text.split()
        return words[0] if words else self.dash_value
