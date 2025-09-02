#!/usr/bin/env python3
"""
data_processor.py - 完全版（重複防止機能付き）
整数ID使用、full_model_ja改良、DeepL翻訳統合
"""
import re
import json
import os
import time
import hashlib
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime
import requests

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
        if not self.enabled or not text or text == 'Information not available':
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
        if not colors or colors == ['Information not available']:
            return ['ー']
        
        translated = []
        for color in colors:
            # 既存のマッピングをチェック
            ja_color = color
            for en_word, ja_word in existing_map.items():
                if en_word.lower() in color.lower():
                    ja_color = color.lower().replace(en_word.lower(), ja_word)
                    break
            
            # マッピングで翻訳されなかった場合はDeepL使用
            if ja_color == color and self.enabled:
                ja_color = self.translate(color)
            
            translated.append(ja_color)
        
        return translated

class DataProcessor:
    """データ処理クラス（重複防止機能付き）"""
    
    def __init__(self):
        self.exchange_api = ExchangeRateAPI()
        self.translator = DeepLTranslator()
        self.gbp_to_jpy = self.exchange_api.get_rate()
        self.na_value = 'Information not available'
        self.dash_value = 'ー'
    
    def _normalize_for_id(self, value: str) -> str:
        """ID生成用の値を正規化"""
        if not value or value in [self.na_value, self.dash_value, '', 'N/A', '-', None]:
            return 'UNKNOWN'
        
        # 文字列を正規化
        normalized = str(value).strip().lower()
        # 余分なスペースを削除してアンダースコアに置換
        normalized = re.sub(r'\s+', '_', normalized)
        # 特殊文字を削除（英数字、ハイフン、ドット、アンダースコアのみ保持）
        normalized = re.sub(r'[^\w\-\.]', '', normalized)
        
        return normalized
    
    def _generate_consistent_id(self, slug: str, grade: str, engine: str) -> int:
        """重複を防ぐ一貫性のある整数IDを生成"""
        # 各値を正規化
        norm_slug = self._normalize_for_id(slug)
        norm_grade = self._normalize_for_id(grade)
        norm_engine = self._normalize_for_id(engine)
        
        # gradeとengineが両方ともUNKNOWNでない場合のみ追加
        if norm_grade != 'UNKNOWN' or norm_engine != 'UNKNOWN':
            unique_key = f"{norm_slug}_{norm_grade}_{norm_engine}"
        else:
            # グレード・エンジン情報がない場合は、slugのみでID生成
            unique_key = norm_slug
        
        # MD5ハッシュで一貫性のある整数IDを生成
        hash_obj = hashlib.md5(unique_key.encode('utf-8'))
        generated_id = int(hash_obj.hexdigest()[:8], 16) % 2147483647
        
        # IDが0になるのを防ぐ
        if generated_id == 0:
            generated_id = 1
            
        return generated_id
    
    def process_vehicle_data(self, raw_data: Optional[Dict]) -> List[Dict]:
        """
        車両データを処理してデータベース用レコードに変換
        エンジン単位で別レコードとして返す（重複防止機能付き）
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
        
        # 重複チェック用のセット
        seen_combinations = set()
        
        for grade_engine in grades_engines:
            record = base_data.copy()
            
            record['grade'] = self._normalize_value(grade_engine.get('grade'), self.na_value)
            record['engine'] = self._normalize_value(grade_engine.get('engine'), self.na_value)
            record['fuel'] = self._normalize_value(grade_engine.get('fuel'), self.na_value)
            record['transmission'] = self._normalize_value(grade_engine.get('transmission'), self.na_value)
            record['drive_type'] = self._normalize_value(grade_engine.get('drive_type'), self.na_value)
            
            # 組み合わせの重複チェック
            combination_key = f"{record['grade']}_{record['engine']}_{record['fuel']}"
            if combination_key in seen_combinations:
                # 重複する組み合わせの場合は、価格や出力で区別を試みる
                power_bhp = grade_engine.get('power_bhp')
                engine_price = grade_engine.get('engine_price_gbp')
                
                if power_bhp:
                    combination_key += f"_{power_bhp}bhp"
                elif engine_price:
                    combination_key += f"_{engine_price}gbp"
                else:
                    # それでも重複する場合はスキップ
                    continue
            
            seen_combinations.add(combination_key)
            
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
            
            record = self._add_japanese_fields(record, raw_data)
            
            # full_model_ja の構築
            parts = []
            
            # make_ja と model_ja は必須
            if record['make_ja']:
                parts.append(record['make_ja'])
            if record['model_ja'] and record['model_ja'] != self.dash_value:
                parts.append(record['model_ja'])
            
            # grade を追加（有効な値の場合のみ）
            if record['grade'] and record['grade'] not in [self.na_value, self.dash_value, '']:
                parts.append(record['grade'])
            
            # engine を短縮して追加（有効な値の場合のみ）
            if record['engine'] and record['engine'] not in [self.na_value, self.dash_value, '']:
                engine_short = self._shorten_engine_text(record['engine'])
                if engine_short and engine_short != self.dash_value:
                    parts.append(engine_short)
            
            # 半角スペースで結合
            record['full_model_ja'] = ' '.join(parts)
            
            record['spec_json'] = self._create_spec_json(raw_data, grade_engine)
            record['updated_at'] = datetime.now().isoformat()
            record['is_active'] = raw_data.get('is_active', True)
            
            # 整数IDを生成
            record['id'] = self._generate_consistent_id(
                raw_data.get('slug', ''),
                record['grade'],
                record['engine']
            )
            
            records.append(record)
        
        return records
    
    def _normalize_value(self, value: Any, default: str) -> str:
        """値を正規化"""
        if value is None or value == '' or value == 'N/A' or value == '-':
            return default
        return str(value)
    
    def _shorten_engine_text(self, engine_text: str) -> str:
        """エンジンテキストを短縮（ハイブリッド/EV情報を保持）"""
        if engine_text == self.na_value or not engine_text:
            return ''
        
        # ハイブリッドエンジンの処理
        if 'Hybrid' in engine_text or 'e:HEV' in engine_text or 'PHEV' in engine_text:
            match = re.search(r'(\d+\.?\d*)\s*L', engine_text)
            if 'Plug-in' in engine_text or 'PHEV' in engine_text:
                return f"{match.group(0)} PHEV" if match else 'PHEV'
            else:
                return f"{match.group(0)} HEV" if match else 'HEV'
        
        # 電気自動車の処理
        if 'kWh' in engine_text or 'Electric' in engine_text:
            match = re.search(r'([\d.]+)\s*kWh', engine_text)
            if match:
                return f"{match.group(1)}kWh"
            else:
                return 'EV'
        
        # 通常のエンジンの処理
        match = re.search(r'(\d+\.?\d*)\s*L', engine_text)
        if match:
            return match.group(0)
        
        # その他の場合は最初の単語を返す
        words = engine_text.split()
        return words[0] if words else engine_text
    
    def _extract_base_data(self, raw_data: Dict) -> Dict:
        """基本データを抽出"""
        specs = raw_data.get('specifications', {})
        prices = raw_data.get('prices', {})
        
        # body_typeの処理
        body_types = raw_data.get('body_types', [])
        if not body_types or body_types == ['Information not available']:
            body_types = ['Information not available']
        
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
        
        # 為替レートを使用して日本円価格を計算
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
        """日本語フィールドを追加（DeepL統合版）"""
        
        # メーカー名のマッピング
        make_ja_map = {
            'Abarth': 'アバルト',
            'Alfa Romeo': 'アルファロメオ',
            'Alpine': 'アルピーヌ',
            'Aston Martin': 'アストンマーティン',
            'Audi': 'アウディ',
            'Bentley': 'ベントレー',
            'BMW': 'BMW',
            'Bmw': 'BMW',
            'BYD': 'BYD',
            'Byd': 'BYD',
            'Citroen': 'シトロエン',
            'Cupra': 'クプラ',
            'Dacia': 'ダチア',
            'DS': 'DS',
            'Ds': 'DS',
            'Ferrari': 'フェラーリ',
            'Fiat': 'フィアット',
            'Ford': 'フォード',
            'Genesis': 'ジェネシス',
            'Gwm': 'GWM',
            'Honda': 'ホンダ',
            'Hyundai': 'ヒュンダイ',
            'Ineos': 'イネオス',
            'Infiniti': 'インフィニティ',
            'Jaecoo': 'ジャエクー',
            'Jaguar': 'ジャガー',
            'Jeep': 'ジープ',
            'Kgm Motors': 'KGMモーターズ',
            'Kia': 'キア',
            'Lamborghini': 'ランボルギーニ',
            'Land Rover': 'ランドローバー',
            'Lexus': 'レクサス',
            'Lotus': 'ロータス',
            'Maserati': 'マセラティ',
            'Mazda': 'マツダ',
            'Mclaren': 'マクラーレン',
            'Mercedes-Benz': 'メルセデス・ベンツ',
            'MG': 'MG',
            'Mg': 'MG',
            'MINI': 'ミニ',
            'Mini': 'ミニ',
            'Mitsubishi': '三菱',
            'Nissan': '日産',
            'Omoda': 'オモダ',
            'Peugeot': 'プジョー',
            'Polestar': 'ポールスター',
            'Porsche': 'ポルシェ',
            'Renault': 'ルノー',
            'Rolls Royce': 'ロールスロイス',
            'SEAT': 'セアト',
            'Seat': 'セアト',
            'Skoda': 'シュコダ',
            'Smart': 'スマート',
            'Subaru': 'スバル',
            'Suzuki': 'スズキ',
            'Tesla': 'テスラ',
            'Toyota': 'トヨタ',
            'Vauxhall': 'ボクスホール',
            'Volkswagen': 'フォルクスワーゲン',
            'Volvo': 'ボルボ'
        }
        
        record['make_ja'] = make_ja_map.get(record['make_en'], record['make_en'])
        
        # model_jaをDeepLで翻訳
        if record['model_en'] and record['model_en'] != self.na_value:
            record['model_ja'] = self.translator.translate(record['model_en'])
        else:
            record['model_ja'] = self.dash_value
        
        # body_type_jaのマッピング
        body_type_ja_map = {
            'SUV': 'SUV',
            'SUVs': 'SUV',
            'Hatchback': 'ハッチバック',
            'Hatchbacks': 'ハッチバック',
            'Saloon': 'セダン',
            'Saloons': 'セダン',
            'Estate': 'ステーションワゴン',
            'Estate cars': 'ステーションワゴン',
            'Coupe': 'クーペ',
            'Coupes': 'クーペ',
            'Sports Car': 'スポーツカー',
            'Sports cars': 'スポーツカー',
            'People Carrier': 'ミニバン',
            'People carriers': 'ミニバン',
            'Convertible': 'カブリオレ',
            'Convertibles': 'カブリオレ',
            'Electric': '電気自動車',
            'Information not available': 'ー'
        }
        
        body_types = record.get('body_type', [])
        if not body_types or body_types == ['Information not available']:
            record['body_type_ja'] = ['ー']
        else:
            record['body_type_ja'] = [
                body_type_ja_map.get(bt, body_type_ja_map.get(bt + 's', bt)) 
                for bt in body_types
            ]
        
        # 燃料タイプの翻訳
        fuel_ja_map = {
            'Petrol': 'ガソリン',
            'Diesel': 'ディーゼル',
            'Electric': '電気',
            'Hybrid': 'ハイブリッド',
            'Plug-in Hybrid': 'プラグインハイブリッド',
            self.na_value: self.dash_value
        }
        record['fuel_ja'] = fuel_ja_map.get(record.get('fuel', self.na_value), self.dash_value)
        
        # トランスミッションの翻訳
        trans_ja_map = {
            'Automatic': 'オートマチック',
            'Manual': 'マニュアル',
            'CVT': 'CVT',
            'DCT': 'DCT',
            self.na_value: self.dash_value
        }
        
        trans = record.get('transmission', '')
        if trans == self.na_value:
            record['transmission_ja'] = self.dash_value
        elif 'Automatic' in trans:
            record['transmission_ja'] = 'オートマチック'
        elif 'Manual' in trans:
            record['transmission_ja'] = 'マニュアル'
        else:
            record['transmission_ja'] = trans_ja_map.get(trans, self.dash_value)
        
        # ドライブタイプの翻訳
        drive_ja_map = {
            'Front-wheel drive': 'FF',
            'Rear-wheel drive': 'FR',
            'All-wheel drive': 'AWD',
            'Four-wheel drive': '4WD',
            self.na_value: self.dash_value
        }
        
        drive = record.get('drive_type', '')
        if 'Front' in drive or 'front' in drive:
            record['drive_type_ja'] = 'FF'
        elif 'Rear' in drive or 'rear' in drive:
            record['drive_type_ja'] = 'FR'
        elif 'All' in drive or 'all' in drive:
            record['drive_type_ja'] = 'AWD'
        elif 'Four' in drive or '4' in drive:
            record['drive_type_ja'] = '4WD'
        else:
            record['drive_type_ja'] = drive_ja_map.get(drive, self.dash_value)
        
        # カラーの翻訳
        color_ja_map = {
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
            'Metallic': 'メタリック'
        }
        
        colors = record.get('colors', [])
        if colors == self.na_value or not colors:
            record['colors_ja'] = self.dash_value
        else:
            record['colors_ja'] = self.translator.translate_colors(colors, color_ja_map)
        
        # 寸法の日本語フォーマット
        if record.get('dimensions_mm') and record['dimensions_mm'] != self.na_value:
            record['dimensions_ja'] = self._format_dimensions_ja(record['dimensions_mm'])
        else:
            record['dimensions_ja'] = self.dash_value
        
        # overview_jaをDeepLで翻訳
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
        elif 'hybrid' in engine_text.lower():
            details['type'] = 'Hybrid'
        
        return details
    
    def _format_dimensions_ja(self, dimensions_mm: str) -> str:
        """寸法を日本語形式にフォーマット"""
        if not dimensions_mm or dimensions_mm == self.na_value:
            return self.dash_value
        
        # カンマ区切りの数字を含む場合に対応
        numbers = re.findall(r'[\d,]+', dimensions_mm)
        
        if len(numbers) >= 3:
            # カンマを除去して数値に変換
            length = int(numbers[0].replace(',', ''))
            width = int(numbers[1].replace(',', ''))
            height = int(numbers[2].replace(',', ''))
            
            return f"全長{length:,} mm x 全幅{width:,} mm x 全高{height:,} mm"
        
        return dimensions_mm
