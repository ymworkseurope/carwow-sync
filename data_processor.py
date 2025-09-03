#!/usr/bin/env python3
"""
data_processor.py
"""
import re
import json
import os
import time
import hashlib
import logging
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
import requests

# 翻訳辞書・ヘルパーの読み込み
from translation_mappings import (
    MAKE_JA_MAP,
    BODY_TYPE_JA_MAP,
    FUEL_JA_MAP,
    TRANSMISSION_JA_MAP,
    DRIVE_TYPE_JA_MAP,
    COLOR_JA_MAP,
    DEFAULT_VALUES,
    translate_body_types,
    get_transmission_ja,
    get_drive_type_ja,
    get_translation,
)

# ロギング設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ExchangeRateAPI:
    """為替レートAPI管理クラス"""
    
    def __init__(self):
        self.cache_file = 'exchange_rate_cache.json'
        self.cache_duration = 3600  # 1時間キャッシュ
        self.rate = None
        self._load_cached_rate()
    
    def _load_cached_rate(self):
        """キャッシュされた為替レートを読み込み"""
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, 'r') as f:
                    content = f.read()
                    if content.strip():
                        cache = json.loads(content)
                        if time.time() - cache.get('timestamp', 0) < self.cache_duration:
                            self.rate = cache.get('rate')
                            logger.info(f"Using cached exchange rate: 1 GBP = {self.rate} JPY")
            except Exception as e:
                logger.error(f"Error loading exchange rate cache: {e}")
    
    def get_rate(self) -> float:
        """GBPからJPYへの為替レートを取得"""
        if self.rate:
            return self.rate
        
        try:
            # 無料の為替APIを使用
            response = requests.get(
                'https://api.exchangerate-api.com/v4/latest/GBP',
                timeout=10
            )
            if response.status_code == 200:
                data = response.json()
                self.rate = data['rates']['JPY']
                
                # キャッシュに保存
                with open(self.cache_file, 'w') as f:
                    json.dump({
                        'rate': self.rate,
                        'timestamp': time.time()
                    }, f)
                
                logger.info(f"Fetched exchange rate: 1 GBP = {self.rate} JPY")
                return self.rate
        except Exception as e:
            logger.error(f"Error fetching exchange rate: {e}")
        
        # フォールバック値
        self.rate = float(os.getenv('FALLBACK_EXCHANGE_RATE', '185.0'))
        logger.warning(f"Using fallback exchange rate: 1 GBP = {self.rate} JPY")
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
        self._load_cache()
        self._load_quota()
        
        if not self.enabled:
            logger.warning("DeepL API key not configured")
    
    def _load_cache(self):
        """翻訳キャッシュを読み込み"""
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, 'r') as f:
                    content = f.read()
                    if content.strip():
                        self.cache = json.loads(content)
                        logger.info(f"Loaded translation cache with {len(self.cache)} entries")
            except Exception as e:
                logger.error(f"Error loading translation cache: {e}")
                self.cache = {}
    
    def _save_cache(self):
        """翻訳キャッシュを保存"""
        try:
            with open(self.cache_file, 'w') as f:
                json.dump(self.cache, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Error saving translation cache: {e}")
    
    def _load_quota(self):
        """クォータ使用状況を読み込み"""
        if os.path.exists(self.quota_file):
            try:
                with open(self.quota_file, 'r') as f:
                    content = f.read()
                    if content.strip():
                        data = json.loads(content)
                        # 月が変わったらリセット
                        saved_month = data.get('month', '')
                        current_month = datetime.now().strftime('%Y-%m')
                        if saved_month == current_month:
                            self.quota_used = data.get('used', 0)
                        else:
                            self.quota_used = 0
            except Exception as e:
                logger.error(f"Error loading quota: {e}")
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
            logger.error(f"Error saving quota: {e}")
    
    def _check_quota(self, text: str) -> bool:
        """クォータチェック"""
        char_count = len(text)
        if self.quota_used + char_count > self.quota_limit * 0.9:  # 90%で警告
            logger.warning(f"DeepL quota nearly exhausted ({self.quota_used}/{self.quota_limit})")
            return False
        return True
    
    def translate(self, text: str, target_lang: str = 'JA') -> str:
        """テキストを翻訳"""
        if not self.enabled or not text or text == DEFAULT_VALUES['na_value']:
            return text
        
        # クォータチェック
        if not self._check_quota(text):
            return text
        
        # キャッシュチェック
        cache_key = f"{text}_{target_lang}"
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        try:
            # DeepL API Free版のエンドポイント
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
                
                # クォータ更新
                self.quota_used += len(text)
                self._save_quota()
                
                # キャッシュに保存
                self.cache[cache_key] = translated
                self._save_cache()
                
                return translated
            elif response.status_code == 456:
                logger.warning("DeepL API quota exceeded")
                return text
            else:
                logger.error(f"DeepL API error: {response.status_code}")
                return text
                
        except Exception as e:
            logger.error(f"Translation error: {e}")
            return text
    
    def translate_colors(self, colors: List[str], existing_map: Dict[str, str]) -> List[str]:
        """色名を翻訳（既存のマッピングを優先）"""
        if not colors or colors == [DEFAULT_VALUES['na_value']]:
            return [DEFAULT_VALUES['dash_value']]
        
        translated = []
        for color in colors:
            ja_color = color
            for en_word, ja_word in existing_map.items():
                if en_word.lower() in color.lower():
                    ja_color = color.lower().replace(en_word.lower(), ja_word)
                    break
            if ja_color == color and self.enabled:
                ja_color = self.translate(color)
            translated.append(ja_color)
        
        return translated

class DataProcessor:
    """データ処理クラス"""
    
    def __init__(self):
        self.exchange_api = ExchangeRateAPI()
        self.translator = DeepLTranslator()
        self.gbp_to_jpy = self.exchange_api.get_rate()
        self.na_value = DEFAULT_VALUES['na_value']
        self.dash_value = DEFAULT_VALUES['dash_value']
    
    # ------------------------
    # IDユーティリティ
    # ------------------------
    def _normalize_for_id(self, value: Any) -> str:
        """ID生成用の値正規化"""
        if value is None or value == '' or value in [self.na_value, self.dash_value, 'N/A', '-']:
            return 'NONE'
        
        if isinstance(value, str):
            normalized = re.sub(r'[^\w\s]', '', value.lower().strip())
            normalized = re.sub(r'\s+', '_', normalized)
            return normalized if normalized else 'NONE'
        
        return str(value)
    
    def _generate_consistent_id(self, unique_key: str) -> int:
        """一貫性のある整数IDを生成（修正版）"""
        parts = unique_key.split('_')
        if len(parts) >= 3:
            slug = parts[0]
            grade = '_'.join(parts[1:-1]) if len(parts) > 3 else parts[1]
            engine = parts[-1]
            normalized_slug = self._normalize_for_id(slug)
            normalized_grade = self._normalize_for_id(grade)
            normalized_engine = self._normalize_for_id(engine)
            stable_key = f"{normalized_slug}__{normalized_grade}__{normalized_engine}"
        else:
            stable_key = self._normalize_for_id(unique_key)
        
        hash_obj = hashlib.md5(stable_key.encode('utf-8'))
        return int(hash_obj.hexdigest()[:8], 16) % 2147483647

    # ------------------------
    # メイン処理
    # ------------------------
    def process_vehicle_data(self, raw_data: Optional[Dict]) -> List[Dict]:
        """
        車両データを処理してデータベース用レコードに変換
        エンジン単位で別レコードとして返す
        """
        if not raw_data:
            return []
        
        records = []
        base_data = self._extract_base_data(raw_data)
        
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
            
            if grade_engine.get('price_min_gbp'):
                record['price_min_gbp'] = grade_engine['price_min_gbp']
                record['price_min_jpy'] = int(grade_engine['price_min_gbp'] * self.gbp_to_jpy)
            
            # 日本語関連を付与
            record = self._add_japanese_fields(record, raw_data)

            # ===== シンプル版 “末尾テキスト” ロジック =====
            fuel_class = self._classify_fuel(
                engine_text=record.get('engine', ''),
                model_ja=record.get('model_ja', ''),
                explicit_fuel=record.get('fuel', self.na_value)
            )

            tail_text = self._build_tail_text(
                fuel_class=fuel_class,
                engine_text=record.get('engine', ''),
                raw_data=raw_data,
                grade_engine=grade_engine
            )

            parts = []
            if record['make_ja']:
                parts.append(record['make_ja'])
            if record.get('model_ja') and record['model_ja'] != self.dash_value:
                parts.append(record['model_ja'])
            if record['grade'] and record['grade'] not in [self.na_value, self.dash_value, '']:
                parts.append(record['grade'])
            if tail_text:
                parts.append(tail_text)
            record['full_model_ja'] = ' '.join(parts).strip()
            # ===== ここまで =====
            
            # spec_jsonを生成
            record['spec_json'] = self._create_spec_json(raw_data, grade_engine)
            # 保存スキーマは触らず、補助情報として spec_json にだけ fuel_class を残す
            record['spec_json']['fuel_class'] = fuel_class

            record['updated_at'] = datetime.now().isoformat()
            record['is_active'] = raw_data.get('is_active', True)
            
            unique_key = f"{record['slug']}_{record['grade']}_{record['engine']}"
            record['id'] = self._generate_consistent_id(unique_key)
            records.append(record)
        
        return records

    # ------------------------
    # 末尾テキスト・判定ユーティリティ
    # ------------------------
    def _classify_fuel(self, engine_text: str, model_ja: str, explicit_fuel: str) -> str:
        """
        BEV/PHEV/HEV/MHEV/Bi-Fuel/Diesel/Petrol を返す。
        engine / model のテキストでまず判定し、explicit_fuel は補助のみ。
        """
        txt = (engine_text or '')
        txt_l = txt.lower()

        if re.search(r'\bmhev\b', txt_l) or 'mild' in txt_l:
            return 'MHEV'
        if re.search(r'plug[-\s]?in', txt_l) or re.search(r'\bphev\b', txt_l):
            return 'PHEV'
        if 'hybrid' in txt_l or 'e:hev' in txt_l:
            return 'HEV'
        if 'bi-fuel' in txt_l or 'bifuel' in txt_l:
            return 'Bi-Fuel'

        has_disp = bool(re.search(r'\b\d\.\d\s*l\b', txt_l))
        is_ev_keyword = any(k in txt_l for k in ['electric', 'elettrica', 'e-tense']) or ('kwh' in txt_l and not has_disp)
        if is_ev_keyword:
            return 'Electric'

        if any(d in txt_l for d in ['diesel', 'tdi', 'bluehdi', 'cdi']) or re.search(r'\b\d\.\d\s*d\b', txt_l):
            return 'Diesel'

        if any(p in txt_l for p in ['petrol', 'tsi', 'tfsi', 't-gdi', 'tgi']):
            return 'Petrol'

        model_l = (model_ja or '').lower()
        if any(k in model_l for k in ['electric', 'ev']):
            return 'Electric'

        fuel_map = {
            'electric': 'Electric',
            'petrol': 'Petrol',
            'diesel': 'Diesel',
            'hybrid': 'HEV',
            'plug-in hybrid': 'PHEV',
            'mhev': 'MHEV',
            'bi-fuel': 'Bi-Fuel'
        }
        ef = (explicit_fuel or '').strip().lower()
        return fuel_map.get(ef, 'Petrol')

    def _extract_displacement_l(self, engine_text: str) -> Optional[str]:
        """エンジン排気量（x.xL）を抽出して 'x.xL' を返す"""
        if not engine_text or engine_text == self.na_value:
            return None
        m = re.search(r'(\d+(?:\.\d+)?)\s*l\b', engine_text.lower())
        if m:
            return f"{m.group(1)}L"
        return None

    def _get_battery_kwh(self, raw_data: Dict, grade_engine: Dict) -> Optional[float]:
        """
        可能な限り battery kWh を取得（specifications / engine_text から）
        """
        specs = (raw_data or {}).get('specifications', {})
        val = specs.get('battery_capacity_kwh')
        if isinstance(val, (int, float)):
            return float(val)

        eng = (grade_engine or {}).get('engine') or ''
        m = re.search(r'([\d.]+)\s*kwh', eng.lower())
        if m:
            try:
                return float(m.group(1))
            except:
                pass

        raw_txt = json.dumps(specs, ensure_ascii=False).lower()
        m2 = re.search(r'battery\s*capacity[^0-9]*([\d.]+)\s*kwh', raw_txt)
        if m2:
            try:
                return float(m2.group(1))
            except:
                pass

        return None

    def _format_kwh_tail(self, kwh: float) -> str:
        """xx.xkWh（.0は落とす）"""
        if kwh is None:
            return ''
        s = f"{kwh:.1f}"
        if s.endswith('.0'):
            s = s[:-2]
        return f"{s}kWh"

    def _build_tail_text(self, fuel_class: str, engine_text: str, raw_data: Dict, grade_engine: Dict) -> str:
        """
        末尾テキストを構築
        - Electric: kWhがあれば 'xx.xkWh'、なければ 'EV'
        - PHEV/HEV/MHEV/Bi-Fuel: 'x.xL {LABEL}'（排気量なければ {LABEL}のみ）
        - Petrol/Diesel: 'x.xL'（排気量なければ空）
        """
        label_map = {
            'PHEV': 'PHEV',
            'HEV': 'HEV',
            'MHEV': 'MHEV',
            'Bi-Fuel': 'Bi-Fuel'
        }

        if fuel_class == 'Electric':
            kwh = self._get_battery_kwh(raw_data, grade_engine)
            return self._format_kwh_tail(kwh) if kwh is not None else 'EV'

        if fuel_class in ('PHEV', 'HEV', 'MHEV', 'Bi-Fuel'):
            disp = self._extract_displacement_l(engine_text)
            if disp:
                return f"{disp} {label_map[fuel_class]}"
            return label_map[fuel_class]

        disp = self._extract_displacement_l(engine_text)
        return disp or ''

    # ------------------------
    # 既存の補助関数
    # ------------------------
    def _normalize_value(self, value: Any, default: str) -> str:
        """値を正規化"""
        if value is None or value == '' or value == 'N/A' or value == '-':
            return default
        return str(value)
    
    def _shorten_engine_text(self, engine_text: str) -> str:
        """（旧）エンジンテキスト簡易短縮（互換用）"""
        if engine_text == self.na_value or not engine_text:
            return ''
        if 'Hybrid' in engine_text or 'e:HEV' in engine_text or 'PHEV' in engine_text:
            match = re.search(r'(\d+\.?\d*)\s*L', engine_text)
            if 'Plug-in' in engine_text or 'PHEV' in engine_text:
                return f"{match.group(0)} PHEV" if match else 'PHEV'
            else:
                return f"{match.group(0)} HEV" if match else 'HEV'
        if 'kWh' in engine_text or 'Electric' in engine_text:
            match = re.search(r'([\d.]+)\s*kWh', engine_text)
            if match:
                return f"{match.group(1)}kWh"
            else:
                return 'EV'
        match = re.search(r'(\d+\.?\d*)\s*L', engine_text)
        if match:
            return match.group(0)
        words = engine_text.split()
        return words[0] if words else engine_text
    
    def _extract_base_data(self, raw_data: Dict) -> Dict:
        """基本データを抽出"""
        specs = raw_data.get('specifications', {})
        prices = raw_data.get('prices', {})
        
        body_types = raw_data.get('body_types', [])
        if not body_types or body_types == [self.na_value]:
            body_types = [self.na_value]
        
        base = {
            'slug': raw_data.get('slug', ''),
            'make_en': raw_data.get('make_en', ''),
            'model_en': raw_data.get('model_en', ''),
            'overview_en': raw_data.get('overview_en', ''),
            'body_type': body_types,
            'colors': self.na_value if not raw_data.get('colors') else raw_data.get('colors', []),
            'media_urls': raw_data.get('media_urls', []),
            'catalog_url': raw_data.get('catalog_url', ''),
            'doors': self.na_value if specs.get('doors') is None else specs.get('doors'),
            'seats': self.na_value if specs.get('seats') is None else specs.get('seats'),
            'dimensions_mm': self.na_value if not specs.get('dimensions_mm') else specs.get('dimensions_mm'),
            'price_min_gbp': self.na_value if prices.get('price_min_gbp') is None else prices.get('price_min_gbp'),
            'price_max_gbp': self.na_value if prices.get('price_max_gbp') is None else prices.get('price_max_gbp'),
            'price_used_gbp': self.na_value if prices.get('price_used_gbp') is None else prices.get('price_used_gbp'),
        }
        
        if base['price_min_gbp'] != self.na_value:
            base['price_min_jpy'] = int(base['price_min_gbp'] * self.gbp_to_jpy)
        else:
            base['price_min_jpy'] = self.dash_value
            
        if base['price_max_gbp'] != self.na_value:
            base['price_max_jpy'] = int(base['price_max_gbp'] * self.gbp_to_jpy)
        else:
            base['price_max_jpy'] = self.dash_value
            
        if base['price_used_gbp'] != self.na_value:
            base['price_used_jpy'] = int(base['price_used_gbp'] * self.gbp_to_jpy)
        else:
            base['price_used_jpy'] = self.dash_value
        
        return base
    
    def _add_japanese_fields(self, record: Dict, raw_data: Dict) -> Dict:
        """日本語フィールドを追加（辞書分離版）"""
        # メーカー名
        record['make_ja'] = MAKE_JA_MAP.get(record['make_en'], record['make_en'])
        
        # モデル名
        if record['model_en'] and record['model_en'] != self.na_value:
            record['model_ja'] = self.translator.translate(record['model_en'])
        else:
            record['model_ja'] = self.dash_value
        
        # ボディタイプ
        record['body_type_ja'] = translate_body_types(record.get('body_type', []))
        
        # 燃料タイプ
        record['fuel_ja'] = get_translation(FUEL_JA_MAP, record.get('fuel', self.na_value), self.dash_value)
        
        # トランスミッション
        record['transmission_ja'] = get_transmission_ja(record.get('transmission', ''))
        
        # 駆動方式
        record['drive_type_ja'] = get_drive_type_ja(record.get('drive_type', ''))
        
        # カラー
        colors = record.get('colors', [])
        if colors == self.na_value or not colors:
            record['colors_ja'] = self.dash_value
        else:
            record['colors_ja'] = self.translator.translate_colors(colors, COLOR_JA_MAP)
        
        # 寸法
        if record.get('dimensions_mm') and record['dimensions_mm'] != self.na_value:
            record['dimensions_ja'] = self._format_dimensions_ja(record['dimensions_mm'])
        else:
            record['dimensions_ja'] = self.dash_value
        
        # 概要
        overview_en = record.get('overview_en', '')
        if overview_en and overview_en != self.na_value:
            record['overview_ja'] = self.translator.translate(overview_en)
        else:
            record['overview_ja'] = self.dash_value
        
        return record
    
    def _create_spec_json(self, raw_data: Dict, grade_engine: Dict) -> Dict:
        """詳細仕様のJSONを作成"""
        spec_json = {
            'raw_specifications': raw_data.get('specifications', {}),
            'grade_engine_details': grade_engine,
            'body_types': raw_data.get('body_types', []),
            'available_colors': raw_data.get('colors', []),
            'media_count': len(raw_data.get('media_urls', [])),
            'scrape_date': datetime.now().isoformat(),
            'exchange_rate_gbp_to_jpy': self.gbp_to_jpy
        }
        
        engine_text = grade_engine.get('engine', '')
        if engine_text and engine_text != self.na_value:
            spec_json['engine_parsed'] = self._parse_engine_details(engine_text)
        
        specs = raw_data.get('specifications', {})
        if specs:
            spec_json['additional_specs'] = {
                'boot_capacity_l': specs.get('boot_capacity_l'),
                'wheelbase_m': specs.get('wheelbase_m'),
                'turning_circle_m': specs.get('turning_circle_m'),
                'battery_capacity_kwh': specs.get('battery_capacity_kwh')
            }
        
        return spec_json
    
    def _parse_engine_details(self, engine_text: str) -> Dict:
        """エンジン情報を詳細にパース"""
        details = {}
        
        if engine_text == self.na_value:
            return details
        
        electric_match = re.search(r'(\d+)\s*kW\s+([\d.]+)\s*kWh', engine_text)
        if electric_match:
            details['type'] = 'Electric'
            details['power_kw'] = int(electric_match.group(1))
            details['battery_kwh'] = float(electric_match.group(2))
            details['power_hp'] = int(details['power_kw'] * 1.341)
            return details
        
        size_match = re.search(r'(\d+\.?\d*)\s*L', engine_text)
        if size_match:
            details['engine_size_l'] = float(size_match.group(1))
        
        hp_match = re.search(r'(\d+)\s*(?:hp|bhp)', engine_text, re.IGNORECASE)
        if hp_match:
            details['power_hp'] = int(hp_match.group(1))
        
        kw_match = re.search(r'(\d+)\s*kW', engine_text)
        if kw_match:
            details['power_kw'] = int(kw_match.group(1))
            if 'power_hp' not in details:
                details['power_hp'] = int(details['power_kw'] * 1.341)
        
        torque_match = re.search(r'(\d+)\s*(?:Nm|lb-ft)', engine_text)
        if torque_match:
            details['torque'] = torque_match.group(0)
        
        if 'petrol' in engine_text.lower():
            details['type'] = 'Petrol'
        elif 'diesel' in engine_text.lower():
            details['type'] = 'Diesel'
        elif 'hybrid' in engine_text.lower() or 'mhev' in engine_text.lower():
            details['type'] = 'Hybrid'
        
        return details
    
    def _format_dimensions_ja(self, dimensions_mm: str) -> str:
        """寸法を日本語形式にフォーマット"""
        if not dimensions_mm or dimensions_mm == self.na_value:
            return self.dash_value
        
        numbers = re.findall(r'[\d,]+', dimensions_mm)
        if len(numbers) >= 3:
            length = int(numbers[0].replace(',', ''))
            width = int(numbers[1].replace(',', ''))
            height = int(numbers[2].replace(',', ''))
            return f"全長{length:,} mm x 全幅{width:,} mm x 全高{height:,} mm"
        
        return dimensions_mm
