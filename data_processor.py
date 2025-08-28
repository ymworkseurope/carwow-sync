#!/usr/bin/env python3
"""
data_processor.py
データ変換、翻訳、価格換算などの処理モジュール
"""
import json
import os
import uuid
import requests
from datetime import datetime
from typing import Dict, List, Optional, Any  # Added Any import

# ======================== Configuration ========================
GBP_TO_JPY = 199  # 2025年8月28日時点のレート
DEEPL_API_KEY = os.getenv("DEEPL_KEY", "")
UUID_NAMESPACE = uuid.UUID("12345678-1234-5678-1234-123456789012")

# 日本語翻訳マッピング
MAKE_JA = {
    "abarth": "アバルト",
    "alfa romeo": "アルファロメオ",
    "alpine": "アルピーヌ",
    "aston martin": "アストンマーチン",
    "audi": "アウディ",
    "bentley": "ベントレー",
    "bmw": "BMW",
    "byd": "BYD",
    "citroen": "シトロエン",
    "cupra": "クプラ",
    "dacia": "ダチア",
    "ds": "DS",
    "ferrari": "フェラーリ",
    "fiat": "フィアット",
    "ford": "フォード",
    "genesis": "ジェネシス",
    "honda": "ホンダ",
    "hyundai": "ヒュンダイ",
    "infiniti": "インフィニティ",
    "isuzu": "いすゞ",
    "jaguar": "ジャガー",
    "jeep": "ジープ",
    "kia": "キア",
    "lamborghini": "ランボルギーニ",
    "land rover": "ランドローバー",
    "lexus": "レクサス",
    "lotus": "ロータス",
    "maserati": "マセラティ",
    "mazda": "マツダ",
    "mclaren": "マクラーレン",
    "mercedes-benz": "メルセデス・ベンツ",
    "mg": "MG",
    "mini": "MINI",
    "mitsubishi": "三菱",
    "nissan": "日産",
    "peugeot": "プジョー",
    "polestar": "ポールスター",
    "porsche": "ポルシェ",
    "renault": "ルノー",
    "rolls royce": "ロールスロイス",
    "seat": "セアト",
    "skoda": "シュコダ",
    "smart": "スマート",
    "subaru": "スバル",
    "suzuki": "スズキ",
    "tesla": "テスラ",
    "toyota": "トヨタ",
    "vauxhall": "ボクスホール",
    "volkswagen": "フォルクスワーゲン",
    "volvo": "ボルボ",
    "xpeng": "エクスペン"
}

BODY_TYPE_JA = {
    "SUV": "SUV",
    "Electric": "電気自動車",
    "Hybrid": "ハイブリッド",
    "Convertible": "カブリオレ",
    "Estate": "ステーションワゴン",
    "Hatchback": "ハッチバック",
    "Saloon": "セダン",
    "Coupe": "クーペ",
    "Sports": "スポーツカー"
}

# ======================== Translation Service ========================
class TranslationService:
    """翻訳サービス"""
    
    @staticmethod
    def translate_with_deepl(text: str, target_lang: str = "JA") -> str:
        """DeepL APIで翻訳"""
        if not text or not DEEPL_API_KEY:
            return text
        
        try:
            response = requests.post(
                "https://api-free.deepl.com/v2/translate",
                data={
                    "auth_key": DEEPL_API_KEY,
                    "text": text,
                    "source_lang": "EN",
                    "target_lang": target_lang
                },
                timeout=10
            )
            
            if response.status_code == 200:
                result = response.json()
                translations = result.get("translations", [])
                if translations:
                    return translations[0].get("text", text)
        except Exception as e:
            print(f"DeepL translation error: {e}")
        
        return text
    
    @staticmethod
    def translate_make(make_en: str) -> str:
        """メーカー名を日本語に変換"""
        make_lower = make_en.lower()
        return MAKE_JA.get(make_lower, make_en)
    
    @staticmethod
    def translate_body_type(body_type_en: str) -> str:
        """ボディタイプを日本語に変換"""
        return BODY_TYPE_JA.get(body_type_en, body_type_en)
    
    @staticmethod
    def translate_transmission(trans_en: str) -> str:
        """トランスミッションを日本語に変換"""
        return TranslationService.translate_with_deepl(trans_en) if trans_en else ""
    
    @staticmethod
    def translate_fuel(fuel_en: str) -> str:
        """燃料タイプを日本語に変換"""
        return TranslationService.translate_with_deepl(fuel_en) if fuel_en else ""
    
    @staticmethod
    def translate_drive_type(drive_en: str) -> str:
        """ドライブタイプを日本語に変換"""
        return TranslationService.translate_with_deepl(drive_en) if drive_en else ""

# ======================== Data Processor ========================
class DataProcessor:
    """データ変換・処理クラス"""
    
    def __init__(self):
        self.translator = TranslationService()
    
    def process_vehicle_data(self, raw_data: Dict) -> List[Dict]:
        """生データをデータベース形式に変換（トリムごとに複数行生成）"""
        # 基本情報の処理
        make_en = self._clean_make_name(raw_data.get('make', ''))
        model_en = raw_data.get('model', '')
        
        # 日本語翻訳
        make_ja = self.translator.translate_make(make_en)
        model_ja = model_en
        overview_ja = self.translator.translate_with_deepl(raw_data.get('overview', ''))
        
        # ボディタイプ処理
        body_types_en = raw_data.get('body_types', [])
        body_types_ja = [self.translator.translate_body_type(bt) for bt in body_types_en]
        
        # 基本価格処理
        price_min_gbp = self._safe_numeric(raw_data.get('price_min_gbp'))
        price_max_gbp = self._safe_numeric(raw_data.get('price_max_gbp'))
        price_used_gbp = self._safe_numeric(raw_data.get('price_used_gbp'))
        
        price_min_jpy = self._convert_to_jpy(price_min_gbp)
        price_max_jpy = self._convert_to_jpy(price_max_gbp)
        price_used_jpy = self._convert_to_jpy(price_used_gbp)
        
        # doors/seatsを取得
        doors = self._safe_smallint(raw_data.get('doors'))
        seats = self._safe_smallint(raw_data.get('seats'))
        
        # カラー処理
        colors = self._process_array_field(raw_data.get('colors', []))
        
        # メディアURL処理
        media_urls = self._process_media_urls(raw_data.get('images', []))
        
        # 寸法情報の処理
        dimensions_processed = self._extract_and_format_dimensions(raw_data)
        
        # トリム情報を取得
        trims = raw_data.get('trims', [])
        
        # トリムデータの検証
        valid_trims = [trim for trim in trims if self._is_valid_trim_name_for_processing(trim.get('trim_name', ''))]
        
        # 有効なトリムがない場合はStandard
        if not valid_trims:
            valid_trims = [{'trim_name': 'Standard'}]
        
        # グレード情報の収集
        all_grades = self._collect_grades(valid_trims)
        all_engines = self._collect_engines(valid_trims)
        
        print(f"  Found {len(valid_trims)} valid trims: {[t.get('trim_name', 'Unknown') for t in valid_trims]}")
        
        # 各トリムごとにペイロードを作成
        payloads = []
        for trim_index, trim in enumerate(valid_trims):
            trim_name = self._clean_trim_name(trim.get('trim_name', 'Standard'))
            
            if not self._is_valid_trim_name_for_processing(trim_name):
                print(f"    Skipping invalid trim: {trim_name}")
                continue
            
            trim_id = f"{raw_data['slug']}_{trim_name}_{trim_index}"
            vehicle_id = str(uuid.uuid5(UUID_NAMESPACE, trim_id))
            
            # トリム固有情報
            engine = trim.get('engine', '')
            transmission_en = trim.get('transmission') or raw_data.get('transmission', '')
            transmission_ja = self.translator.translate_transmission(transmission_en)
            fuel_en = trim.get('fuel_type') or raw_data.get('fuel_type', '')
            fuel_ja = self.translator.translate_fuel(fuel_en)
            drive_en = trim.get('drive_type', '')
            drive_ja = self.translator.translate_drive_type(drive_en)
            
            full_model_parts = [make_ja, model_en]
            if trim_name != 'Standard':
                full_model_parts.append(trim_name)
            full_model_ja = ' '.join(full_model_parts)
            
            trim_price_min = self._safe_numeric(trim.get('price_rrp')) or price_min_gbp
            trim_price_max = trim_price_min or price_max_gbp
            trim_price_min_jpy = self._convert_to_jpy(trim_price_min)
            trim_price_max_jpy = self._convert_to_jpy(trim_price_max)
            power_bhp = self._safe_int(trim.get('power_bhp'))
            
            spec_json = raw_data.get('specifications', {})
            spec_json['trim_info'] = {
                'trim_name': trim_name,
                'engine': engine,
                'fuel_type': fuel_en,
                'power_bhp': power_bhp,
                'transmission': transmission_en,
                'drive_type': drive_en
            }
            
            payload = {
                'id': vehicle_id,
                'slug': raw_data['slug'],
                'make_en': make_en,
                'model_en': model_en,
                'make_ja': make_ja,
                'model_ja': model_ja,
                'body_type': body_types_en,
                'fuel': fuel_en,
                'price_min_gbp': trim_price_min,
                'price_max_gbp': trim_price_max,
                'price_min_jpy': trim_price_min_jpy,
                'price_max_jpy': trim_price_max_jpy,
                'overview_en': raw_data.get('overview', ''),
                'overview_ja': overview_ja,
                'spec_json': spec_json,
                'media_urls': media_urls,
                'updated_at': datetime.utcnow(),
                'body_type_ja': body_types_ja,
                'catalog_url': raw_data.get('url'),
                'grades': all_grades,
                'engines': all_engines,
                'doors': doors,
                'seats': seats,
                'dimensions_mm': dimensions_processed,
                'drive_type': drive_en,
                'full_model_ja': full_model_ja,
                'colors': colors,
                'drive_type_ja': drive_ja,
                'price_used_gbp': price_used_gbp,
                'price_used_jpy': price_used_jpy,
                'transmission': transmission_en,
                'transmission_ja': transmission_ja,
                'fuel_ja': fuel_ja,
                'power_bhp': power_bhp,
                'trim_name': trim_name,
                'engine': engine
            }
            
            payloads.append(payload)
        
        if not payloads:
            print(f"  No valid trims found, creating default Standard trim")
            payloads = self._create_default_payload(raw_data, make_en, make_ja, model_en, model_ja,
                                                  overview_ja, body_types_en, body_types_ja,
                                                  price_min_gbp, price_max_gbp, price_used_gbp,
                                                  price_min_jpy, price_max_jpy, price_used_jpy,
                                                  doors, seats, colors, media_urls)
        
        return payloads
    
    def _collect_grades(self, trims: List[Dict]) -> Dict:
        """全トリムからグレード情報を収集してJSONB形式で構造化"""
        grades = {}
        
        for trim in trims:
            trim_name = trim.get('trim_name', 'Standard')
            grade_info = {
                'name': trim_name,
                'engine': trim.get('engine', ''),
                'power_bhp': self._safe_int(trim.get('power_bhp')),
                'transmission': trim.get('transmission', ''),
                'fuel_type': trim.get('fuel_type', ''),
                'drive_type': trim.get('drive_type', ''),
                'price_rrp': self._safe_numeric(trim.get('price_rrp'))
            }
            
            grade_info = {k: v for k, v in grade_info.items() if v is not None and v != ''}
            grades[trim_name] = grade_info
        
        return grades
    
    def _collect_engines(self, trims: List[Dict]) -> Dict:
        """全トリムからエンジン情報を収集してJSONB形式で構造化"""
        engines = {}
        
        for trim in trims:
            engine = trim.get('engine', '')
            if engine and engine not in engines:
                engines[engine] = {
                    'name': engine,
                    'power_bhp': self._safe_int(trim.get('power_bhp')),
                    'fuel_type': trim.get('fuel_type', '')
                }
        
        return engines
    
    def _extract_and_format_dimensions(self, raw_data: Dict) -> Optional[str]:
        """寸法データの抽出とフォーマット"""
        dimensions_sources = [
            raw_data.get('dimensions'),
            raw_data.get('specifications', {}).get('dimensions')
        ]
        
        length = None
        width = None
        height = None
        wheelbase = None
        other_dimensions = []
        
        for source in dimensions_sources:
            if not source:
                continue
                
            if isinstance(source, dict):
                for key in ['length', 'length_mm', 'overall_length']:
                    if not length and key in source:
                        length = self._extract_dimension_value(source[key])
                        break
                
                for key in ['width', 'width_mm', 'overall_width']:
                    if not width and key in source:
                        width = self._extract_dimension_value(source[key])
                        break
                
                for key in ['height', 'height_mm', 'overall_height']:
                    if not height and key in source:
                        height = self._extract_dimension_value(source[key])
                        break
                
                for key in ['wheelbase', 'wheelbase_mm']:
                    if not wheelbase and key in source:
                        wheelbase = self._extract_dimension_value(source[key])
                        break
                
                for key, value in source.items():
                    if key.lower() not in ['length', 'width', 'height', 'wheelbase', 'length_mm', 'width_mm', 'height_mm', 'wheelbase_mm']:
                        dim_value = self._extract_dimension_value(value)
                        if dim_value:
                            other_dimensions.append(f"{key}: {dim_value} mm")
            
            elif isinstance(source, str):
                dimensions = self._parse_dimensions_string(source)
                if dimensions:
                    if not length and len(dimensions) > 0:
                        length = dimensions[0]
                    if not width and len(dimensions) > 1:
                        width = dimensions[1]
                    if not height and len(dimensions) > 2:
                        height = dimensions[2]
        
        result_parts = []
        if length and width and height:
            result_parts.append(f"{length} mm x {width} mm x {height} mm")
        elif length and width:
            result_parts.append(f"{length} mm x {width} mm")
        elif length:
            result_parts.append(f"Length: {length} mm")
        
        if wheelbase:
            result_parts.append(f"Wheelbase: {wheelbase} mm")
        
        result_parts.extend(other_dimensions)
        return "; ".join(result_parts) if result_parts else None
    
    def _extract_dimension_value(self, value: Any) -> Optional[int]:
        """寸法値の抽出"""
        if isinstance(value, (int, float)):
            return int(value)
        
        if isinstance(value, str):
            mm_match = re.search(r'(\d{3,4})\s*mm', value)
            if mm_match:
                return int(mm_match.group(1))
            
            cm_match = re.search(r'(\d+(?:\.\d+)?)\s*cm', value)
            if cm_match:
                return int(float(cm_match.group(1)) * 10)
            
            m_match = re.search(r'(\d+(?:\.\d+)?)\s*m', value)
            if m_match:
                return int(float(m_match.group(1)) * 1000)
            
            num_match = re.search(r'(\d{3,4})', value)
            if num_match:
                return int(num_match.group(1))
        
        return None
    
    def _parse_dimensions_string(self, dim_string: str) -> List[int]:
        """寸法文字列から数値リストを抽出"""
        if not dim_string:
            return []
        
        numbers = re.findall(r'(\d{1,2}[,.]?\d{3,4})', dim_string)
        return [int(num.replace(',', '').replace('.', '')) for num in numbers if len(num.replace(',', '').replace('.', '')) >= 3]
    
    def _process_array_field(self, field_data: Optional[List]) -> List[str]:
        """配列フィールドの処理"""
        if not field_data:
            return []
        return [str(item) for item in field_data if item]
    
    def _process_media_urls(self, urls: List[str]) -> List[str]:
        """メディアURLの処理"""
        return list(set(urls))[:40]
    
    def _safe_numeric(self, value: Any) -> Optional[float]:
        """安全な数値変換"""
        if value is None:
            return None
        
        if isinstance(value, (int, float)):
            return float(value)
        
        if isinstance(value, str):
            cleaned = value.replace(',', '').replace('£', '').replace('¥', '')
            try:
                return float(cleaned)
            except (ValueError, TypeError):
                pass
        
        return None
    
    def _safe_smallint(self, value: Any) -> Optional[int]:
        """安全なsmallint変換"""
        if value is None:
            return None
        
        try:
            num = int(float(str(value).replace(',', '')))
            if -32768 <= num <= 32767:
                return num
        except (ValueError, TypeError):
            pass
        
        return None
    
    def _convert_to_jpy(self, gbp: Optional[float]) -> Optional[float]:
        """GBPからJPYに変換"""
        if gbp is None:
            return None
        return gbp * GBP_TO_JPY
    
    def _is_valid_trim_name_for_processing(self, trim_name: str) -> bool:
        """トリム名妥当性チェック"""
        return bool(trim_name and len(trim_name.strip()) >= 2)
    
    def _create_default_payload(self, raw_data: Dict, make_en: str, make_ja: str, 
                              model_en: str, model_ja: str, overview_ja: str,
                              body_types_en: List, body_types_ja: List,
                              price_min_gbp: float, price_max_gbp: float, price_used_gbp: float,
                              price_min_jpy: float, price_max_jpy: float, price_used_jpy: float,
                              doors: int, seats: int, colors: List, media_urls: List) -> List[Dict]:
        """デフォルトのStandardトリムペイロードを作成"""
        dimensions_processed = self._extract_and_format_dimensions(raw_data)
        vehicle_id = str(uuid.uuid5(UUID_NAMESPACE, f"{raw_data['slug']}_Standard_0"))
        
        default_grades = {
            'Standard': {
                'name': 'Standard',
                'engine': '',
                'fuel_type': raw_data.get('fuel_type', ''),
                'transmission': raw_data.get('transmission', '')
            }
        }
        
        default_engines = {}
        
        spec_json = raw_data.get('specifications', {})
        spec_json['trim_info'] = {
            'trim_name': 'Standard',
            'engine': '',
            'fuel_type': raw_data.get('fuel_type', ''),
            'power_bhp': None,
            'transmission': raw_data.get('transmission', ''),
            'drive_type': ''
        }
        
        payload = {
            'id': vehicle_id,
            'slug': raw_data['slug'],
            'make_en': make_en,
            'model_en': model_en,
            'make_ja': make_ja,
            'model_ja': model_ja,
            'body_type': body_types_en,
            'fuel': raw_data.get('fuel_type', ''),
            'price_min_gbp': price_min_gbp,
            'price_max_gbp': price_max_gbp,
            'price_min_jpy': price_min_jpy,
            'price_max_jpy': price_max_jpy,
            'overview_en': raw_data.get('overview', ''),
            'overview_ja': overview_ja,
            'spec_json': spec_json,
            'media_urls': media_urls,
            'updated_at': datetime.utcnow(),
            'body_type_ja': body_types_ja,
            'catalog_url': raw_data.get('url'),
            'grades': default_grades,
            'engines': default_engines,
            'doors': doors,
            'seats': seats,
            'dimensions_mm': dimensions_processed,
            'drive_type': '',
            'full_model_ja': f"{make_ja} {model_en}",
            'colors': colors,
            'drive_type_ja': '',
            'price_used_gbp': price_used_gbp,
            'price_used_jpy': price_used_jpy,
            'transmission': raw_data.get('transmission', ''),
            'transmission_ja': self.translator.translate_transmission(raw_data.get('transmission', '')),
            'fuel_ja': self.translator.translate_fuel(raw_data.get('fuel_type', '')),
            'power_bhp': None,
            'trim_name': 'Standard',
            'engine': ''
        }
        
        return [payload]
    
    def _clean_make_name(self, make: str) -> str:
        """メーカー名のクリーニング"""
        make = make.replace('-', ' ')
        special_cases = {
            'alfa romeo': 'Alfa Romeo',
            'aston martin': 'Aston Martin',
            'land rover': 'Land Rover',
            'mercedes benz': 'Mercedes-Benz',
            'rolls royce': 'Rolls-Royce'
        }
        make_lower = make.lower()
        return special_cases.get(make_lower, make.title())
    
    def _clean_trim_name(self, trim_name: str) -> str:
        """トリム名のクリーニング"""
        return trim_name.strip() or "Standard"
    
    def _safe_int(self, value: Any) -> Optional[int]:
        """安全な整数変換"""
        if value is None:
            return None
        
        if isinstance(value, (int, float)):
            return int(value)
        
        if isinstance(value, str):
            cleaned = value.replace(',', '').replace('£', '').replace('¥', '')
            try:
                return int(float(cleaned))
            except (ValueError, TypeError):
                pass
        
        return None

# ======================== Validation ========================
class DataValidator:
    """データ検証クラス"""
    
    @staticmethod
    def validate_payload(payload: Dict) -> tuple[bool, List[str]]:
        """データの妥当性を検証"""
        errors = []
        
        required_fields = ['id', 'slug', 'make_en', 'model_en']
        for field in required_fields:
            if not payload.get(field):
                errors.append(f"Missing required field: {field}")
        
        if 'id' in payload:
            try:
                uuid.UUID(payload['id'])
            except (ValueError, TypeError):
                errors.append(f"Invalid UUID format: {payload.get('id')}")
        
        if 'slug' in payload:
            slug = payload['slug']
            if not slug or '/' not in slug:
                errors.append(f"Invalid slug format: {slug}")
        
        trim_name = payload.get('trim_name', '')
        if trim_name and len(trim_name.strip()) < 2:
            errors.append(f"Invalid trim_name: {trim_name}")
        
        price_min = payload.get('price_min_gbp')
        price_max = payload.get('price_max_gbp')
        if price_min is not None and price_max is not None:
            if price_min > price_max:
                errors.append(f"Price min ({price_min}) > Price max ({price_max})")
        
        numeric_fields = {
            'doors': (1, 10),
            'seats': (1, 20),
            'power_bhp': (0, 2000)
        }
        
        for field, (min_val, max_val) in numeric_fields.items():
            value = payload.get(field)
            if value is not None:
                if not isinstance(value, (int, float)) or value < min_val or value > max_val:
                    errors.append(f"Invalid {field}: {value} (must be between {min_val} and {max_val})")
        
        array_fields = ['body_type', 'body_type_ja', 'colors', 'media_urls']
        for field in array_fields:
            value = payload.get(field)
            if value is not None and not isinstance(value, list):
                errors.append(f"Field {field} must be an array, got {type(value)}")
        
        jsonb_fields = ['spec_json', 'grades', 'engines']
        for field in jsonb_fields:
            value = payload.get(field)
            if value is not None:
                if not isinstance(value, dict):
                    errors.append(f"Field {field} must be a dict for JSONB, got {type(value)}")
                else:
                    try:
                        json.dumps(value, ensure_ascii=False)
                    except (TypeError, ValueError) as e:
                        errors.append(f"Field {field} is not JSON serializable: {e}")
        
        if 'updated_at' in payload:
            updated_at = payload['updated_at']
            if not isinstance(updated_at, (datetime, str)):
                errors.append(f"Field updated_at must be datetime or string, got {type(updated_at)}")
            elif isinstance(updated_at, str):
                try:
                    datetime.fromisoformat(updated_at.replace('Z', '+00:00'))
                except ValueError:
                    errors.append(f"Field updated_at has invalid datetime format: {updated_at}")
        
        return len(errors) == 0, errors
    
    @staticmethod
    def validate_schema_compatibility(payload: Dict, table_schema: List[Dict]) -> tuple[bool, List[str]]:
        """テーブルスキーマとの互換性を検証"""
        errors = []
        schema_columns = {col['column_name']: col for col in table_schema}
        
        for field_name, field_value in payload.items():
            if field_name not in schema_columns:
                errors.append(f"Field {field_name} not found in table schema")
                continue
            
            column_info = schema_columns[field_name]
            data_type = column_info['data_type'].lower()
            is_nullable = column_info['is_nullable'] == 'YES'
            
            if field_value is None and not is_nullable:
                errors.append(f"Field {field_name} cannot be NULL")
                continue
            
            if field_value is None:
                continue
            
            type_checks = {
                'uuid': lambda v: isinstance(v, str) and DataValidator._is_valid_uuid(v),
                'text': lambda v: isinstance(v, str),
                'numeric': lambda v: isinstance(v, (int, float)),
                'integer': lambda v: isinstance(v, int),
                'array': lambda v: isinstance(v, list),
                'jsonb': lambda v: isinstance(v, dict)
            }
            
            if data_type in type_checks and not type_checks[data_type](field_value):
                errors.append(f"Field {field_name} has invalid type for {data_type}")
        
        for column_name, column_info in schema_columns.items():
            if column_info['is_nullable'] == 'NO' and column_name not in payload:
                errors.append(f"Required field {column_name} is missing")
        
        return len(errors) == 0, errors
    
    @staticmethod
    def _is_valid_uuid(uuid_string: str) -> bool:
        """UUID形式の検証"""
        try:
            uuid.UUID(uuid_string)
            return True
        except (ValueError, TypeError):
            return False

# ======================== Helper Functions ========================
def process_batch_data(raw_data_list: List[Dict], validator_enabled: bool = True) -> Dict:
    """バッチデータの処理"""
    processor = DataProcessor()
    validator = DataValidator()
    
    all_payloads = []
    all_errors = []
    success_count = 0
    error_count = 0
    
    for i, raw_data in enumerate(raw_data_list):
        try:
            payloads = processor.process_vehicle_data(raw_data)
            
            if validator_enabled:
                valid_payloads = []
                for payload in payloads:
                    is_valid, validation_errors = validator.validate_payload(payload)
                    if is_valid:
                        valid_payloads.append(payload)
                    else:
                        error_count += 1
                        all_errors.extend([f"Item {i} - {error}" for error in validation_errors])
                
                all_payloads.extend(valid_payloads)
                success_count += len(valid_payloads)
            else:
                all_payloads.extend(payloads)
                success_count += len(payloads)
                
        except Exception as e:
            error_count += 1
            all_errors.append(f"Item {i} - Processing error: {str(e)}")
    
    return {
        'success_count': success_count,
        'error_count': error_count,
        'payloads': all_payloads,
        'errors': all_errors
    }

def validate_against_schema(payloads: List[Dict], table_schema: List[Dict]) -> Dict:
    """テーブルスキーマに対するバリデーション"""
    validator = DataValidator()
    valid_payloads = []
    schema_errors = []
    
    for i, payload in enumerate(payloads):
        is_valid, errors = validator.validate_schema_compatibility(payload, table_schema)
        if is_valid:
            valid_payloads.append(payload)
        else:
            schema_errors.extend([f"Payload {i} - {error}" for error in errors])
    
    return {
        'valid_count': len(valid_payloads),
        'invalid_count': len(payloads) - len(valid_payloads),
        'valid_payloads': valid_payloads,
        'schema_errors': schema_errors
    }
