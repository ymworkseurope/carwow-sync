#!/usr/bin/env python3
"""
data_processor.py
データ変換、翻訳、価格換算などの処理モジュール
"""
import os
import re
import json
import uuid
import requests
from datetime import datetime
from typing import Dict, List, Optional, Any, Union

# ======================== Configuration ========================
GBP_TO_JPY = float(os.getenv("GBP_TO_JPY", "195"))
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

TRANSMISSION_JA = {
    "Automatic": "AT",
    "Manual": "MT",
    "CVT": "CVT",
    "Electric": "EV",
    "Semi-automatic": "セミAT",
    "DSG": "DSG",
    "DCT": "DCT"
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
        if not trans_en:
            return ""
        
        for eng, jpn in TRANSMISSION_JA.items():
            if eng.lower() in trans_en.lower():
                return jpn
        
        return trans_en
    
    @staticmethod
    def translate_fuel(fuel_en: str) -> str:
        """燃料タイプを日本語に変換"""
        if not fuel_en:
            return ""
        
        fuel_map = {
            'petrol': 'ガソリン',
            'diesel': 'ディーゼル',
            'electric': '電気',
            'hybrid': 'ハイブリッド',
            'phev': 'プラグインハイブリッド',
            'mild hybrid': 'マイルドハイブリッド'
        }
        
        fuel_lower = fuel_en.lower()
        for eng, jpn in fuel_map.items():
            if eng in fuel_lower:
                return jpn
        
        return fuel_en
    
    @staticmethod
    def translate_drive_type(drive_en: str) -> str:
        """ドライブタイプを日本語に変換"""
        if not drive_en:
            return ""
        
        drive_map = {
            'front wheel drive': 'FF（前輪駆動）',
            'rear wheel drive': 'FR（後輪駆動）',
            'all wheel drive': 'AWD（全輪駆動）',
            'four wheel drive': '4WD（四輪駆動）',
            'fwd': 'FF',
            'rwd': 'FR',
            'awd': 'AWD',
            '4wd': '4WD'
        }
        
        drive_lower = drive_en.lower()
        for eng, jpn in drive_map.items():
            if eng in drive_lower:
                return jpn
        
        return drive_en

# ======================== Data Processor ========================
class DataProcessor:
    """データ変換・処理クラス"""
    
    def __init__(self):
        self.translator = TranslationService()
    
    def process_vehicle_data(self, raw_data: Dict) -> List[Dict]:
        """
        生データをデータベース形式に変換（トリムごとに複数行生成）
        修正版: 実際に見つかったトリムのみ処理
        """
        # 基本情報の処理
        make_en = self._clean_make_name(raw_data.get('make', ''))
        model_en = raw_data.get('model', '')
        
        # 日本語翻訳
        make_ja = self.translator.translate_make(make_en)
        model_ja = model_en  # モデル名は通常そのまま
        overview_ja = self.translator.translate_with_deepl(raw_data.get('overview', ''))
        
        # ボディタイプ処理
        body_types_en = raw_data.get('body_types', [])
        body_types_ja = [self.translator.translate_body_type(bt) for bt in body_types_en]
        
        # 基本価格処理 - 整数に変換
        price_min_gbp = self._safe_int(raw_data.get('price_min_gbp'))
        price_max_gbp = self._safe_int(raw_data.get('price_max_gbp'))
        price_used_gbp = self._safe_int(raw_data.get('price_used_gbp'))
        
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
        
        # 寸法情報の処理（改善版）
        dimensions_processed = self._extract_and_format_dimensions(raw_data)
        
        # トリム情報を取得（実際に見つかったもののみ）
        trims = raw_data.get('trims', [])
        
        # トリムデータの検証とフィルタリング
        valid_trims = []
        for trim in trims:
            trim_name = self._clean_trim_name(trim.get('trim_name', ''))
            if self._is_valid_trim_name_for_processing(trim_name):
                trim['trim_name'] = trim_name
                valid_trims.append(trim)
        
        # 有効なトリムがない場合は「Information not available」を作成
        if not valid_trims:
            valid_trims = [{'trim_name': 'Information not available'}]
        
        # グレードとエンジン情報の収集
        all_grades = self._collect_grades(valid_trims)
        all_engines = self._collect_engines(valid_trims)
        
        # デバッグ用ログ
        print(f"  Found {len(valid_trims)} valid trims: {[t.get('trim_name', 'Unknown') for t in valid_trims]}")
        
        # 各トリムごとにペイロードを作成
        payloads = []
        for trim_index, trim in enumerate(valid_trims):
            # トリム名の検証とクリーニング
            trim_name = trim.get('trim_name', 'Information not available')
            
            # UUID生成（トリムごとにユニーク）
            trim_id = f"{raw_data['slug']}_{trim_name}_{trim_index}"
            vehicle_id = str(uuid.uuid5(UUID_NAMESPACE, trim_id))
            
            # トリム固有情報
            engine = trim.get('engine', '')
            
            # トランスミッション処理
            transmission_en = trim.get('transmission') or raw_data.get('transmission', '')
            transmission_ja = self.translator.translate_transmission(transmission_en)
            
            # 燃料タイプ処理
            fuel_en = trim.get('fuel_type') or raw_data.get('fuel_type', '')
            fuel_ja = self.translator.translate_fuel(fuel_en)
            
            # ドライブタイプ処理
            drive_en = trim.get('drive_type', '')
            drive_ja = self.translator.translate_drive_type(drive_en)
            
            # full_model_jaの生成
            full_model_parts = [make_ja, model_en]
            if trim_name and trim_name != 'Information not available':
                full_model_parts.append(trim_name)
            full_model_ja = ' '.join(full_model_parts)
            
            # トリム固有の価格があれば使用（なければベース価格） - 整数に変換
            trim_price_min = self._safe_int(trim.get('price_rrp')) or price_min_gbp
            trim_price_max = trim_price_min or price_max_gbp
            
            trim_price_min_jpy = self._convert_to_jpy(trim_price_min)
            trim_price_max_jpy = self._convert_to_jpy(trim_price_max)
            
            # power_bhp
            power_bhp = self._safe_int(trim.get('power_bhp'))
            
            # スペック情報の整理
            spec_json = self._compile_specifications(raw_data)
            spec_json.update({
                'trim_info': {
                    'trim_name': trim_name,
                    'engine': engine,
                    'fuel_type': fuel_en,
                    'power_bhp': power_bhp,
                    'transmission': transmission_en,
                    'drive_type': drive_en
                },
                'doors': doors,
                'seats': seats,
                'grade': trim_name,
                'engine': engine
            })
            
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
                'updated_at': datetime.utcnow().isoformat(),  # ISOフォーマットに変換
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
                'engine': engine,
                'grade': trim_name
            }
            
            payloads.append(payload)
        
        return payloads
    
    def _extract_and_format_dimensions(self, raw_data: Dict) -> Optional[str]:
        """寸法データの抽出とフォーマット（改善版）"""
        
        # specificationsから寸法を探す
        specs = raw_data.get('specifications', {})
        
        length = None
        width = None  
        height = None
        wheelbase = None
        
        # 様々なキー名で寸法を探す
        dimension_keys = {
            'length': ['length', 'overall length', 'length (mm)', 'overall length (mm)', 'length mm'],
            'width': ['width', 'overall width', 'width (mm)', 'overall width (mm)', 'width mm'],
            'height': ['height', 'overall height', 'height (mm)', 'overall height (mm)', 'height mm'],
            'wheelbase': ['wheelbase', 'wheelbase (mm)', 'wheelbase mm']
        }
        
        for dim_type, keys in dimension_keys.items():
            for key in keys:
                if key in specs and specs[key]:
                    value = self._extract_dimension_value(specs[key])
                    if value:
                        if dim_type == 'length':
                            length = value
                        elif dim_type == 'width':
                            width = value
                        elif dim_type == 'height':
                            height = value
                        elif dim_type == 'wheelbase':
                            wheelbase = value
                        break
        
        # dimensions_structuredから取得を試みる
        if 'dimensions_structured' in specs:
            dim_struct = specs['dimensions_structured']
            if isinstance(dim_struct, str):
                # "4000 x 1800 x 1500 mm" のような形式をパース
                dimensions = self._parse_dimensions_string(dim_struct)
                if dimensions:
                    if not length and len(dimensions) > 0:
                        length = dimensions[0]
                    if not width and len(dimensions) > 1:
                        width = dimensions[1]
                    if not height and len(dimensions) > 2:
                        height = dimensions[2]
        
        # フォーマット
        if length and width and height:
            return f"{length} x {width} x {height} mm"
        elif length and width:
            return f"{length} x {width} mm"
        elif length:
            return f"Length: {length} mm"
        
        return None
    
    def _extract_dimension_value(self, value: Any) -> Optional[int]:
        """寸法値の抽出"""
        if isinstance(value, (int, float)):
            return int(value)
        
        if isinstance(value, str):
            # カンマを除去
            value = value.replace(',', '')
            
            # mm単位の数値を探す
            mm_match = re.search(r'(\d{3,4})\s*(?:mm)?', value)
            if mm_match:
                return int(mm_match.group(1))
            
            # cm単位の場合（mmに変換）
            cm_match = re.search(r'(\d+(?:\.\d+)?)\s*cm', value)
            if cm_match:
                return int(float(cm_match.group(1)) * 10)
            
            # m単位の場合（mmに変換）  
            m_match = re.search(r'(\d+(?:\.\d+)?)\s*m(?!m)', value)
            if m_match:
                return int(float(m_match.group(1)) * 1000)
        
        return None
    
    def _parse_dimensions_string(self, dim_string: str) -> List[int]:
        """寸法文字列から数値リストを抽出"""
        if not dim_string:
            return []
        
        # カンマを除去してから数値を探す
        dim_string = dim_string.replace(',', '')
        numbers = re.findall(r'(\d{3,4})', dim_string)
        
        result = []
        for num_str in numbers:
            try:
                num = int(num_str)
                # 車の寸法として妥当な範囲（1000mm〜9999mm）
                if 1000 <= num <= 9999:
                    result.append(num)
            except ValueError:
                continue
        
        return result
    
    def _collect_grades(self, trims: List[Dict]) -> Dict:
        """全トリムからグレード情報を収集"""
        grades = {}
        
        for trim in trims:
            trim_name = trim.get('trim_name', 'なし')
            grade_info = {
                'name': trim_name,
                'engine': trim.get('engine', ''),
                'power_bhp': self._safe_int(trim.get('power_bhp')),
                'transmission': trim.get('transmission', ''),
                'fuel_type': trim.get('fuel_type', ''),
                'drive_type': trim.get('drive_type', ''),
                'price_rrp': self._safe_int(trim.get('price_rrp'))
            }
            
            # Noneの値を除去
            grade_info = {k: v for k, v in grade_info.items() if v is not None and v != ''}
            grades[trim_name] = grade_info
        
        return grades
    
    def _collect_engines(self, trims: List[Dict]) -> Dict:
        """全トリムからエンジン情報を収集"""
        engines = {}
        
        for trim in trims:
            engine = trim.get('engine', '')
            if engine and engine not in engines:
                engine_info = {
                    'name': engine,
                    'power_bhp': self._safe_int(trim.get('power_bhp')),
                    'fuel_type': trim.get('fuel_type', ''),
                    'associated_trims': []
                }
                engines[engine] = engine_info
            
            # トリム名をエンジンに関連付け
            if engine and engine in engines:
                trim_name = trim.get('trim_name', 'なし')
                if trim_name not in engines[engine]['associated_trims']:
                    engines[engine]['associated_trims'].append(trim_name)
        
        return engines
    
    def _process_array_field(self, field_data: Union[List, str, None]) -> List[str]:
        """配列フィールドの処理"""
        if not field_data:
            return []
        
        if isinstance(field_data, list):
            return [str(item) for item in field_data if item]
        
        if isinstance(field_data, str):
            return [item.strip() for item in field_data.split(',') if item.strip()]
        
        return []
    
    def _safe_int(self, value: Any) -> Optional[int]:
        """安全な整数変換（小数点を除去）"""
        if value is None:
            return None
        
        try:
            if isinstance(value, (int, float)):
                return int(value)
            
            if isinstance(value, str):
                cleaned = value.replace(',', '').replace('£', '').replace('¥', '')
                return int(float(cleaned))
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
    
    def _convert_to_jpy(self, gbp: Optional[int]) -> Optional[int]:
        """GBPからJPYに変換（整数）"""
        if gbp is None:
            return None
        return int(gbp * GBP_TO_JPY)
    
    def _is_valid_trim_name_for_processing(self, trim_name: str) -> bool:
        """処理用のトリム名妥当性チェック"""
        if not trim_name or len(trim_name.strip()) < 2:
            return False
        
        # 無効なパターンを除外
        invalid_patterns = [
            r'^\d+\s*s?$',  # "0", "2", "0s", "2s"
            r'^s$',         # "s" のみ
            r'^\W+$',       # 記号のみ
        ]
        
        for pattern in invalid_patterns:
            if re.match(pattern, trim_name.strip(), re.IGNORECASE):
                return False
        
        return True
    
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
        if make_lower in special_cases:
            return special_cases[make_lower]
        
        return make.title()
    
    def _clean_trim_name(self, trim_name: str) -> str:
        """トリム名のクリーニング"""
        if not trim_name:
            return "Information not available"
        
        # 前後の空白を除去
        cleaned = trim_name.strip()
        
        # 無効なパターンを「Information not available」に置換
        invalid_patterns = [
            r'^\d+\s*s?
    
    def _compile_specifications(self, raw_data: Dict) -> Dict:
        """スペック情報をまとめる"""
        specs = raw_data.get('specifications', {}).copy()
        
        # 基本スペックを追加
        specs.update({
            'fuel_type': raw_data.get('fuel_type'),
            'doors': raw_data.get('doors'),
            'seats': raw_data.get('seats'),
            'transmission': raw_data.get('transmission')
        })
        
        # Noneの値を除去
        specs = {k: v for k, v in specs.items() if v is not None}
        
        return specs
    
    def _process_media_urls(self, urls: List[str]) -> List[str]:
        """メディアURLの処理"""
        processed = []
        seen = set()
        
        for url in urls:
            if not url:
                continue
                
            # 正規化
            if '?' in url:
                url = url.split('?')[0]
            
            # 重複チェック
            if url not in seen:
                processed.append(url)
                seen.add(url)
        
        return processed[:40]
    
    def prepare_for_database(self, payload: Dict) -> Dict:
        """データベース保存用に準備"""
        # datetimeオブジェクトをISO文字列に変換
        if 'updated_at' in payload:
            if isinstance(payload['updated_at'], datetime):
                payload['updated_at'] = payload['updated_at'].isoformat()
        
        return payload

# ======================== Validation ========================
class DataValidator:
    """データ検証クラス"""
    
    @staticmethod
    def validate_payload(payload: Dict) -> tuple[bool, List[str]]:
        """データの妥当性を検証"""
        errors = []
        
        # 必須フィールドのチェック
        required_fields = ['id', 'slug', 'make_en', 'model_en']
        for field in required_fields:
            if not payload.get(field):
                errors.append(f"Missing required field: {field}")
        
        # UUIDの形式チェック
        if 'id' in payload:
            try:
                uuid.UUID(payload['id'])
            except (ValueError, TypeError):
                errors.append(f"Invalid UUID format: {payload.get('id')}")
        
        # スラッグの形式チェック
        if 'slug' in payload:
            slug = payload['slug']
            if not slug or '/' not in slug:
                errors.append(f"Invalid slug format: {slug}")
        
        # 価格の妥当性チェック
        price_min = payload.get('price_min_gbp')
        price_max = payload.get('price_max_gbp')
        if price_min is not None and price_max is not None:
            if price_min > price_max:
                errors.append(f"Price min ({price_min}) > Price max ({price_max})")
        
        # 数値フィールドの範囲チェック
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
        
        # 配列フィールドの検証
        array_fields = ['body_type', 'body_type_ja', 'colors', 'media_urls']
        for field in array_fields:
            value = payload.get(field)
            if value is not None and not isinstance(value, list):
                errors.append(f"Field {field} must be an array, got {type(value)}")
        
        # JSONB フィールドの検証
        jsonb_fields = ['spec_json', 'grades', 'engines']
        for field in jsonb_fields:
            value = payload.get(field)
            if value is not None:
                if not isinstance(value, dict):
                    errors.append(f"Field {field} must be a dict for JSONB, got {type(value)}")
                else:
                    # JSON serializable かチェック
                    try:
                        json.dumps(value, ensure_ascii=False)
                    except (TypeError, ValueError) as e:
                        errors.append(f"Field {field} is not JSON serializable: {e}")
        
        # タイムスタンプの検証
        if 'updated_at' in payload:
            updated_at = payload['updated_at']
            if not isinstance(updated_at, (datetime, str)):
                errors.append(f"Field updated_at must be datetime or string, got {type(updated_at)}")
            elif isinstance(updated_at, str):
                # ISO形式の日時文字列かチェック
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
            
            # NULL値のチェック
            if field_value is None and not is_nullable:
                errors.append(f"Field {field_name} cannot be NULL")
                continue
            
            if field_value is None:
                continue  # NULL値は許可されている
            
            # データ型の検証
            type_checks = {
                'uuid': lambda v: isinstance(v, str) and DataValidator._is_valid_uuid(v),
                'text': lambda v: isinstance(v, str),
                'numeric': lambda v: isinstance(v, (int, float)),
                'integer': lambda v: isinstance(v, int),
                'smallint': lambda v: isinstance(v, int) and -32768 <= v <= 32767,
                'jsonb': lambda v: isinstance(v, dict),
                'array': lambda v: isinstance(v, list),
                'timestamp with time zone': lambda v: isinstance(v, (datetime, str))
            }
            
            # 配列型の特別処理
            if data_type == 'array':
                if not isinstance(field_value, list):
                    errors.append(f"Field {field_name} must be array, got {type(field_value)}")
            elif data_type in type_checks:
                if not type_checks[data_type](field_value):
                    errors.append(f"Field {field_name} has invalid type for {data_type}")
        
        # 必須フィールドの存在チェック
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
def process_batch_data(raw_data_list: List[Dict], validator_enabled: bool = True, 
                      prepare_for_db: bool = True) -> Dict:
    """
    バッチデータの処理
    
    Args:
        raw_data_list: 生データのリスト
        validator_enabled: バリデーション有効フラグ
        prepare_for_db: データベース用の準備を行うかどうか
    
    Returns:
        処理結果の辞書 (success_count, error_count, payloads, errors)
    """
    processor = DataProcessor()
    validator = DataValidator()
    
    all_payloads = []
    all_errors = []
    success_count = 0
    error_count = 0
    
    for i, raw_data in enumerate(raw_data_list):
        try:
            # データ処理
            payloads = processor.process_vehicle_data(raw_data)
            
            # データベース用の準備
            if prepare_for_db:
                payloads = [processor.prepare_for_database(payload) for payload in payloads]
            
            if validator_enabled:
                # 各ペイロードのバリデーション
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
    """
    テーブルスキーマに対するバリデーション
    
    Args:
        payloads: ペイロードのリスト
        table_schema: テーブルスキーマの定義
    
    Returns:
        バリデーション結果
    """
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

# ======================== Example Usage ========================
if __name__ == "__main__":
    # サンプルデータでのテスト
    sample_raw_data = {
        'slug': 'bmw/3-series',
        'make': 'BMW',
        'model': '3 Series',
        'body_types': ['Saloon', 'Estate'],
        'fuel_type': 'Petrol',
        'transmission': 'Automatic',
        'price_min_gbp': 35000,
        'price_max_gbp': 55000,
        'overview': 'The BMW 3 Series is a compact executive car.',
        'doors': 4,
        'seats': 5,
        'colors': ['Alpine White', 'Jet Black', 'Storm Bay'],
        'images': ['https://example.com/img1.jpg', 'https://example.com/img2.jpg'],
        'specifications': {
            'length (mm)': '4,713',
            'width (mm)': '1,827',
            'height (mm)': '1,440',
            'wheelbase (mm)': '2,851'
        },
        'trims': [
            {
                'trim_name': '320i',
                'engine': '2.0L TwinPower Turbo',
                'power_bhp': 184,
                'fuel_type': 'Petrol',
                'transmission': 'Automatic',
                'price_rrp': 35000
            },
            {
                'trim_name': '330i',
                'engine': '2.0L TwinPower Turbo',
                'power_bhp': 258,
                'fuel_type': 'Petrol',
                'transmission': 'Automatic',
                'price_rrp': 45000
            }
        ]
    }
    
    # 処理の実行
    processor = DataProcessor()
    payloads = processor.process_vehicle_data(sample_raw_data)
    
    # データベース用に準備
    db_ready_payloads = [processor.prepare_for_database(payload) for payload in payloads]
    
    print(f"Generated {len(payloads)} payloads:")
    for i, payload in enumerate(db_ready_payloads):
        print(f"  Payload {i+1}: {payload['make_en']} {payload['model_en']} {payload['trim_name']}")
        print(f"    Price: £{payload['price_min_gbp']} - £{payload['price_max_gbp']}")
        print(f"    Dimensions: {payload['dimensions_mm']}")
        print(f"    Updated at: {payload['updated_at']} ({type(payload['updated_at'])})")
    
    # バリデーションのテスト
    validator = DataValidator()
    for i, payload in enumerate(db_ready_payloads):
        is_valid, errors = validator.validate_payload(payload)
        print(f"  Payload {i+1} validation: {'PASS' if is_valid else 'FAIL'}")
        if errors:
            for error in errors:
                print(f"    - {error}"),  # "0", "2", "0s", "2s"
            r'^s
    
    def _compile_specifications(self, raw_data: Dict) -> Dict:
        """スペック情報をまとめる"""
        specs = raw_data.get('specifications', {}).copy()
        
        # 基本スペックを追加
        specs.update({
            'fuel_type': raw_data.get('fuel_type'),
            'doors': raw_data.get('doors'),
            'seats': raw_data.get('seats'),
            'transmission': raw_data.get('transmission')
        })
        
        # Noneの値を除去
        specs = {k: v for k, v in specs.items() if v is not None}
        
        return specs
    
    def _process_media_urls(self, urls: List[str]) -> List[str]:
        """メディアURLの処理"""
        processed = []
        seen = set()
        
        for url in urls:
            if not url:
                continue
                
            # 正規化
            if '?' in url:
                url = url.split('?')[0]
            
            # 重複チェック
            if url not in seen:
                processed.append(url)
                seen.add(url)
        
        return processed[:40]
    
    def prepare_for_database(self, payload: Dict) -> Dict:
        """データベース保存用に準備"""
        # datetimeオブジェクトをISO文字列に変換
        if 'updated_at' in payload:
            if isinstance(payload['updated_at'], datetime):
                payload['updated_at'] = payload['updated_at'].isoformat()
        
        return payload

# ======================== Validation ========================
class DataValidator:
    """データ検証クラス"""
    
    @staticmethod
    def validate_payload(payload: Dict) -> tuple[bool, List[str]]:
        """データの妥当性を検証"""
        errors = []
        
        # 必須フィールドのチェック
        required_fields = ['id', 'slug', 'make_en', 'model_en']
        for field in required_fields:
            if not payload.get(field):
                errors.append(f"Missing required field: {field}")
        
        # UUIDの形式チェック
        if 'id' in payload:
            try:
                uuid.UUID(payload['id'])
            except (ValueError, TypeError):
                errors.append(f"Invalid UUID format: {payload.get('id')}")
        
        # スラッグの形式チェック
        if 'slug' in payload:
            slug = payload['slug']
            if not slug or '/' not in slug:
                errors.append(f"Invalid slug format: {slug}")
        
        # 価格の妥当性チェック
        price_min = payload.get('price_min_gbp')
        price_max = payload.get('price_max_gbp')
        if price_min is not None and price_max is not None:
            if price_min > price_max:
                errors.append(f"Price min ({price_min}) > Price max ({price_max})")
        
        # 数値フィールドの範囲チェック
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
        
        # 配列フィールドの検証
        array_fields = ['body_type', 'body_type_ja', 'colors', 'media_urls']
        for field in array_fields:
            value = payload.get(field)
            if value is not None and not isinstance(value, list):
                errors.append(f"Field {field} must be an array, got {type(value)}")
        
        # JSONB フィールドの検証
        jsonb_fields = ['spec_json', 'grades', 'engines']
        for field in jsonb_fields:
            value = payload.get(field)
            if value is not None:
                if not isinstance(value, dict):
                    errors.append(f"Field {field} must be a dict for JSONB, got {type(value)}")
                else:
                    # JSON serializable かチェック
                    try:
                        json.dumps(value, ensure_ascii=False)
                    except (TypeError, ValueError) as e:
                        errors.append(f"Field {field} is not JSON serializable: {e}")
        
        # タイムスタンプの検証
        if 'updated_at' in payload:
            updated_at = payload['updated_at']
            if not isinstance(updated_at, (datetime, str)):
                errors.append(f"Field updated_at must be datetime or string, got {type(updated_at)}")
            elif isinstance(updated_at, str):
                # ISO形式の日時文字列かチェック
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
            
            # NULL値のチェック
            if field_value is None and not is_nullable:
                errors.append(f"Field {field_name} cannot be NULL")
                continue
            
            if field_value is None:
                continue  # NULL値は許可されている
            
            # データ型の検証
            type_checks = {
                'uuid': lambda v: isinstance(v, str) and DataValidator._is_valid_uuid(v),
                'text': lambda v: isinstance(v, str),
                'numeric': lambda v: isinstance(v, (int, float)),
                'integer': lambda v: isinstance(v, int),
                'smallint': lambda v: isinstance(v, int) and -32768 <= v <= 32767,
                'jsonb': lambda v: isinstance(v, dict),
                'array': lambda v: isinstance(v, list),
                'timestamp with time zone': lambda v: isinstance(v, (datetime, str))
            }
            
            # 配列型の特別処理
            if data_type == 'array':
                if not isinstance(field_value, list):
                    errors.append(f"Field {field_name} must be array, got {type(field_value)}")
            elif data_type in type_checks:
                if not type_checks[data_type](field_value):
                    errors.append(f"Field {field_name} has invalid type for {data_type}")
        
        # 必須フィールドの存在チェック
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
def process_batch_data(raw_data_list: List[Dict], validator_enabled: bool = True, 
                      prepare_for_db: bool = True) -> Dict:
    """
    バッチデータの処理
    
    Args:
        raw_data_list: 生データのリスト
        validator_enabled: バリデーション有効フラグ
        prepare_for_db: データベース用の準備を行うかどうか
    
    Returns:
        処理結果の辞書 (success_count, error_count, payloads, errors)
    """
    processor = DataProcessor()
    validator = DataValidator()
    
    all_payloads = []
    all_errors = []
    success_count = 0
    error_count = 0
    
    for i, raw_data in enumerate(raw_data_list):
        try:
            # データ処理
            payloads = processor.process_vehicle_data(raw_data)
            
            # データベース用の準備
            if prepare_for_db:
                payloads = [processor.prepare_for_database(payload) for payload in payloads]
            
            if validator_enabled:
                # 各ペイロードのバリデーション
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
    """
    テーブルスキーマに対するバリデーション
    
    Args:
        payloads: ペイロードのリスト
        table_schema: テーブルスキーマの定義
    
    Returns:
        バリデーション結果
    """
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

# ======================== Example Usage ========================
if __name__ == "__main__":
    # サンプルデータでのテスト
    sample_raw_data = {
        'slug': 'bmw/3-series',
        'make': 'BMW',
        'model': '3 Series',
        'body_types': ['Saloon', 'Estate'],
        'fuel_type': 'Petrol',
        'transmission': 'Automatic',
        'price_min_gbp': 35000,
        'price_max_gbp': 55000,
        'overview': 'The BMW 3 Series is a compact executive car.',
        'doors': 4,
        'seats': 5,
        'colors': ['Alpine White', 'Jet Black', 'Storm Bay'],
        'images': ['https://example.com/img1.jpg', 'https://example.com/img2.jpg'],
        'specifications': {
            'length (mm)': '4,713',
            'width (mm)': '1,827',
            'height (mm)': '1,440',
            'wheelbase (mm)': '2,851'
        },
        'trims': [
            {
                'trim_name': '320i',
                'engine': '2.0L TwinPower Turbo',
                'power_bhp': 184,
                'fuel_type': 'Petrol',
                'transmission': 'Automatic',
                'price_rrp': 35000
            },
            {
                'trim_name': '330i',
                'engine': '2.0L TwinPower Turbo',
                'power_bhp': 258,
                'fuel_type': 'Petrol',
                'transmission': 'Automatic',
                'price_rrp': 45000
            }
        ]
    }
    
    # 処理の実行
    processor = DataProcessor()
    payloads = processor.process_vehicle_data(sample_raw_data)
    
    # データベース用に準備
    db_ready_payloads = [processor.prepare_for_database(payload) for payload in payloads]
    
    print(f"Generated {len(payloads)} payloads:")
    for i, payload in enumerate(db_ready_payloads):
        print(f"  Payload {i+1}: {payload['make_en']} {payload['model_en']} {payload['trim_name']}")
        print(f"    Price: £{payload['price_min_gbp']} - £{payload['price_max_gbp']}")
        print(f"    Dimensions: {payload['dimensions_mm']}")
        print(f"    Updated at: {payload['updated_at']} ({type(payload['updated_at'])})")
    
    # バリデーションのテスト
    validator = DataValidator()
    for i, payload in enumerate(db_ready_payloads):
        is_valid, errors = validator.validate_payload(payload)
        print(f"  Payload {i+1} validation: {'PASS' if is_valid else 'FAIL'}")
        if errors:
            for error in errors:
                print(f"    - {error}"),         # "s" のみ
            r'^0\s',        # "0 " で始まる
        ]
        
        for pattern in invalid_patterns:
            if re.match(pattern, cleaned, re.IGNORECASE):
                return 'Information not available'
        
        # 不要な部分を除去
        patterns_to_remove = [
            r'\b\d+\s*s\b',      # "2 s", "0 s" など
            r'^\d+\s+',          # 先頭の数字とスペース
            r'\s+\d+
    
    def _compile_specifications(self, raw_data: Dict) -> Dict:
        """スペック情報をまとめる"""
        specs = raw_data.get('specifications', {}).copy()
        
        # 基本スペックを追加
        specs.update({
            'fuel_type': raw_data.get('fuel_type'),
            'doors': raw_data.get('doors'),
            'seats': raw_data.get('seats'),
            'transmission': raw_data.get('transmission')
        })
        
        # Noneの値を除去
        specs = {k: v for k, v in specs.items() if v is not None}
        
        return specs
    
    def _process_media_urls(self, urls: List[str]) -> List[str]:
        """メディアURLの処理"""
        processed = []
        seen = set()
        
        for url in urls:
            if not url:
                continue
                
            # 正規化
            if '?' in url:
                url = url.split('?')[0]
            
            # 重複チェック
            if url not in seen:
                processed.append(url)
                seen.add(url)
        
        return processed[:40]
    
    def prepare_for_database(self, payload: Dict) -> Dict:
        """データベース保存用に準備"""
        # datetimeオブジェクトをISO文字列に変換
        if 'updated_at' in payload:
            if isinstance(payload['updated_at'], datetime):
                payload['updated_at'] = payload['updated_at'].isoformat()
        
        return payload

# ======================== Validation ========================
class DataValidator:
    """データ検証クラス"""
    
    @staticmethod
    def validate_payload(payload: Dict) -> tuple[bool, List[str]]:
        """データの妥当性を検証"""
        errors = []
        
        # 必須フィールドのチェック
        required_fields = ['id', 'slug', 'make_en', 'model_en']
        for field in required_fields:
            if not payload.get(field):
                errors.append(f"Missing required field: {field}")
        
        # UUIDの形式チェック
        if 'id' in payload:
            try:
                uuid.UUID(payload['id'])
            except (ValueError, TypeError):
                errors.append(f"Invalid UUID format: {payload.get('id')}")
        
        # スラッグの形式チェック
        if 'slug' in payload:
            slug = payload['slug']
            if not slug or '/' not in slug:
                errors.append(f"Invalid slug format: {slug}")
        
        # 価格の妥当性チェック
        price_min = payload.get('price_min_gbp')
        price_max = payload.get('price_max_gbp')
        if price_min is not None and price_max is not None:
            if price_min > price_max:
                errors.append(f"Price min ({price_min}) > Price max ({price_max})")
        
        # 数値フィールドの範囲チェック
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
        
        # 配列フィールドの検証
        array_fields = ['body_type', 'body_type_ja', 'colors', 'media_urls']
        for field in array_fields:
            value = payload.get(field)
            if value is not None and not isinstance(value, list):
                errors.append(f"Field {field} must be an array, got {type(value)}")
        
        # JSONB フィールドの検証
        jsonb_fields = ['spec_json', 'grades', 'engines']
        for field in jsonb_fields:
            value = payload.get(field)
            if value is not None:
                if not isinstance(value, dict):
                    errors.append(f"Field {field} must be a dict for JSONB, got {type(value)}")
                else:
                    # JSON serializable かチェック
                    try:
                        json.dumps(value, ensure_ascii=False)
                    except (TypeError, ValueError) as e:
                        errors.append(f"Field {field} is not JSON serializable: {e}")
        
        # タイムスタンプの検証
        if 'updated_at' in payload:
            updated_at = payload['updated_at']
            if not isinstance(updated_at, (datetime, str)):
                errors.append(f"Field updated_at must be datetime or string, got {type(updated_at)}")
            elif isinstance(updated_at, str):
                # ISO形式の日時文字列かチェック
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
            
            # NULL値のチェック
            if field_value is None and not is_nullable:
                errors.append(f"Field {field_name} cannot be NULL")
                continue
            
            if field_value is None:
                continue  # NULL値は許可されている
            
            # データ型の検証
            type_checks = {
                'uuid': lambda v: isinstance(v, str) and DataValidator._is_valid_uuid(v),
                'text': lambda v: isinstance(v, str),
                'numeric': lambda v: isinstance(v, (int, float)),
                'integer': lambda v: isinstance(v, int),
                'smallint': lambda v: isinstance(v, int) and -32768 <= v <= 32767,
                'jsonb': lambda v: isinstance(v, dict),
                'array': lambda v: isinstance(v, list),
                'timestamp with time zone': lambda v: isinstance(v, (datetime, str))
            }
            
            # 配列型の特別処理
            if data_type == 'array':
                if not isinstance(field_value, list):
                    errors.append(f"Field {field_name} must be array, got {type(field_value)}")
            elif data_type in type_checks:
                if not type_checks[data_type](field_value):
                    errors.append(f"Field {field_name} has invalid type for {data_type}")
        
        # 必須フィールドの存在チェック
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
def process_batch_data(raw_data_list: List[Dict], validator_enabled: bool = True, 
                      prepare_for_db: bool = True) -> Dict:
    """
    バッチデータの処理
    
    Args:
        raw_data_list: 生データのリスト
        validator_enabled: バリデーション有効フラグ
        prepare_for_db: データベース用の準備を行うかどうか
    
    Returns:
        処理結果の辞書 (success_count, error_count, payloads, errors)
    """
    processor = DataProcessor()
    validator = DataValidator()
    
    all_payloads = []
    all_errors = []
    success_count = 0
    error_count = 0
    
    for i, raw_data in enumerate(raw_data_list):
        try:
            # データ処理
            payloads = processor.process_vehicle_data(raw_data)
            
            # データベース用の準備
            if prepare_for_db:
                payloads = [processor.prepare_for_database(payload) for payload in payloads]
            
            if validator_enabled:
                # 各ペイロードのバリデーション
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
    """
    テーブルスキーマに対するバリデーション
    
    Args:
        payloads: ペイロードのリスト
        table_schema: テーブルスキーマの定義
    
    Returns:
        バリデーション結果
    """
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

# ======================== Example Usage ========================
if __name__ == "__main__":
    # サンプルデータでのテスト
    sample_raw_data = {
        'slug': 'bmw/3-series',
        'make': 'BMW',
        'model': '3 Series',
        'body_types': ['Saloon', 'Estate'],
        'fuel_type': 'Petrol',
        'transmission': 'Automatic',
        'price_min_gbp': 35000,
        'price_max_gbp': 55000,
        'overview': 'The BMW 3 Series is a compact executive car.',
        'doors': 4,
        'seats': 5,
        'colors': ['Alpine White', 'Jet Black', 'Storm Bay'],
        'images': ['https://example.com/img1.jpg', 'https://example.com/img2.jpg'],
        'specifications': {
            'length (mm)': '4,713',
            'width (mm)': '1,827',
            'height (mm)': '1,440',
            'wheelbase (mm)': '2,851'
        },
        'trims': [
            {
                'trim_name': '320i',
                'engine': '2.0L TwinPower Turbo',
                'power_bhp': 184,
                'fuel_type': 'Petrol',
                'transmission': 'Automatic',
                'price_rrp': 35000
            },
            {
                'trim_name': '330i',
                'engine': '2.0L TwinPower Turbo',
                'power_bhp': 258,
                'fuel_type': 'Petrol',
                'transmission': 'Automatic',
                'price_rrp': 45000
            }
        ]
    }
    
    # 処理の実行
    processor = DataProcessor()
    payloads = processor.process_vehicle_data(sample_raw_data)
    
    # データベース用に準備
    db_ready_payloads = [processor.prepare_for_database(payload) for payload in payloads]
    
    print(f"Generated {len(payloads)} payloads:")
    for i, payload in enumerate(db_ready_payloads):
        print(f"  Payload {i+1}: {payload['make_en']} {payload['model_en']} {payload['trim_name']}")
        print(f"    Price: £{payload['price_min_gbp']} - £{payload['price_max_gbp']}")
        print(f"    Dimensions: {payload['dimensions_mm']}")
        print(f"    Updated at: {payload['updated_at']} ({type(payload['updated_at'])})")
    
    # バリデーションのテスト
    validator = DataValidator()
    for i, payload in enumerate(db_ready_payloads):
        is_valid, errors = validator.validate_payload(payload)
        print(f"  Payload {i+1} validation: {'PASS' if is_valid else 'FAIL'}")
        if errors:
            for error in errors:
                print(f"    - {error}"),          # 末尾のスペースと数字
        ]
        
        for pattern in patterns_to_remove:
            cleaned = re.sub(pattern, '', cleaned, flags=re.IGNORECASE)
        
        cleaned = cleaned.strip()
        
        # 最終チェック: 空文字や短すぎる場合、またはStandardの場合
        if not cleaned or len(cleaned) < 2:
            return 'Information not available'
        
        # "Standard"という名前が実際のトリム名でない場合の処理
        # （scraper側でデフォルト値として使われている場合）
        if cleaned == 'Standard':
            return 'Information not available'
        
        return cleaned
    
    def _compile_specifications(self, raw_data: Dict) -> Dict:
        """スペック情報をまとめる"""
        specs = raw_data.get('specifications', {}).copy()
        
        # 基本スペックを追加
        specs.update({
            'fuel_type': raw_data.get('fuel_type'),
            'doors': raw_data.get('doors'),
            'seats': raw_data.get('seats'),
            'transmission': raw_data.get('transmission')
        })
        
        # Noneの値を除去
        specs = {k: v for k, v in specs.items() if v is not None}
        
        return specs
    
    def _process_media_urls(self, urls: List[str]) -> List[str]:
        """メディアURLの処理"""
        processed = []
        seen = set()
        
        for url in urls:
            if not url:
                continue
                
            # 正規化
            if '?' in url:
                url = url.split('?')[0]
            
            # 重複チェック
            if url not in seen:
                processed.append(url)
                seen.add(url)
        
        return processed[:40]
    
    def prepare_for_database(self, payload: Dict) -> Dict:
        """データベース保存用に準備"""
        # datetimeオブジェクトをISO文字列に変換
        if 'updated_at' in payload:
            if isinstance(payload['updated_at'], datetime):
                payload['updated_at'] = payload['updated_at'].isoformat()
        
        return payload

# ======================== Validation ========================
class DataValidator:
    """データ検証クラス"""
    
    @staticmethod
    def validate_payload(payload: Dict) -> tuple[bool, List[str]]:
        """データの妥当性を検証"""
        errors = []
        
        # 必須フィールドのチェック
        required_fields = ['id', 'slug', 'make_en', 'model_en']
        for field in required_fields:
            if not payload.get(field):
                errors.append(f"Missing required field: {field}")
        
        # UUIDの形式チェック
        if 'id' in payload:
            try:
                uuid.UUID(payload['id'])
            except (ValueError, TypeError):
                errors.append(f"Invalid UUID format: {payload.get('id')}")
        
        # スラッグの形式チェック
        if 'slug' in payload:
            slug = payload['slug']
            if not slug or '/' not in slug:
                errors.append(f"Invalid slug format: {slug}")
        
        # 価格の妥当性チェック
        price_min = payload.get('price_min_gbp')
        price_max = payload.get('price_max_gbp')
        if price_min is not None and price_max is not None:
            if price_min > price_max:
                errors.append(f"Price min ({price_min}) > Price max ({price_max})")
        
        # 数値フィールドの範囲チェック
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
        
        # 配列フィールドの検証
        array_fields = ['body_type', 'body_type_ja', 'colors', 'media_urls']
        for field in array_fields:
            value = payload.get(field)
            if value is not None and not isinstance(value, list):
                errors.append(f"Field {field} must be an array, got {type(value)}")
        
        # JSONB フィールドの検証
        jsonb_fields = ['spec_json', 'grades', 'engines']
        for field in jsonb_fields:
            value = payload.get(field)
            if value is not None:
                if not isinstance(value, dict):
                    errors.append(f"Field {field} must be a dict for JSONB, got {type(value)}")
                else:
                    # JSON serializable かチェック
                    try:
                        json.dumps(value, ensure_ascii=False)
                    except (TypeError, ValueError) as e:
                        errors.append(f"Field {field} is not JSON serializable: {e}")
        
        # タイムスタンプの検証
        if 'updated_at' in payload:
            updated_at = payload['updated_at']
            if not isinstance(updated_at, (datetime, str)):
                errors.append(f"Field updated_at must be datetime or string, got {type(updated_at)}")
            elif isinstance(updated_at, str):
                # ISO形式の日時文字列かチェック
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
            
            # NULL値のチェック
            if field_value is None and not is_nullable:
                errors.append(f"Field {field_name} cannot be NULL")
                continue
            
            if field_value is None:
                continue  # NULL値は許可されている
            
            # データ型の検証
            type_checks = {
                'uuid': lambda v: isinstance(v, str) and DataValidator._is_valid_uuid(v),
                'text': lambda v: isinstance(v, str),
                'numeric': lambda v: isinstance(v, (int, float)),
                'integer': lambda v: isinstance(v, int),
                'smallint': lambda v: isinstance(v, int) and -32768 <= v <= 32767,
                'jsonb': lambda v: isinstance(v, dict),
                'array': lambda v: isinstance(v, list),
                'timestamp with time zone': lambda v: isinstance(v, (datetime, str))
            }
            
            # 配列型の特別処理
            if data_type == 'array':
                if not isinstance(field_value, list):
                    errors.append(f"Field {field_name} must be array, got {type(field_value)}")
            elif data_type in type_checks:
                if not type_checks[data_type](field_value):
                    errors.append(f"Field {field_name} has invalid type for {data_type}")
        
        # 必須フィールドの存在チェック
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
def process_batch_data(raw_data_list: List[Dict], validator_enabled: bool = True, 
                      prepare_for_db: bool = True) -> Dict:
    """
    バッチデータの処理
    
    Args:
        raw_data_list: 生データのリスト
        validator_enabled: バリデーション有効フラグ
        prepare_for_db: データベース用の準備を行うかどうか
    
    Returns:
        処理結果の辞書 (success_count, error_count, payloads, errors)
    """
    processor = DataProcessor()
    validator = DataValidator()
    
    all_payloads = []
    all_errors = []
    success_count = 0
    error_count = 0
    
    for i, raw_data in enumerate(raw_data_list):
        try:
            # データ処理
            payloads = processor.process_vehicle_data(raw_data)
            
            # データベース用の準備
            if prepare_for_db:
                payloads = [processor.prepare_for_database(payload) for payload in payloads]
            
            if validator_enabled:
                # 各ペイロードのバリデーション
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
    """
    テーブルスキーマに対するバリデーション
    
    Args:
        payloads: ペイロードのリスト
        table_schema: テーブルスキーマの定義
    
    Returns:
        バリデーション結果
    """
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

# ======================== Example Usage ========================
if __name__ == "__main__":
    # サンプルデータでのテスト
    sample_raw_data = {
        'slug': 'bmw/3-series',
        'make': 'BMW',
        'model': '3 Series',
        'body_types': ['Saloon', 'Estate'],
        'fuel_type': 'Petrol',
        'transmission': 'Automatic',
        'price_min_gbp': 35000,
        'price_max_gbp': 55000,
        'overview': 'The BMW 3 Series is a compact executive car.',
        'doors': 4,
        'seats': 5,
        'colors': ['Alpine White', 'Jet Black', 'Storm Bay'],
        'images': ['https://example.com/img1.jpg', 'https://example.com/img2.jpg'],
        'specifications': {
            'length (mm)': '4,713',
            'width (mm)': '1,827',
            'height (mm)': '1,440',
            'wheelbase (mm)': '2,851'
        },
        'trims': [
            {
                'trim_name': '320i',
                'engine': '2.0L TwinPower Turbo',
                'power_bhp': 184,
                'fuel_type': 'Petrol',
                'transmission': 'Automatic',
                'price_rrp': 35000
            },
            {
                'trim_name': '330i',
                'engine': '2.0L TwinPower Turbo',
                'power_bhp': 258,
                'fuel_type': 'Petrol',
                'transmission': 'Automatic',
                'price_rrp': 45000
            }
        ]
    }
    
    # 処理の実行
    processor = DataProcessor()
    payloads = processor.process_vehicle_data(sample_raw_data)
    
    # データベース用に準備
    db_ready_payloads = [processor.prepare_for_database(payload) for payload in payloads]
    
    print(f"Generated {len(payloads)} payloads:")
    for i, payload in enumerate(db_ready_payloads):
        print(f"  Payload {i+1}: {payload['make_en']} {payload['model_en']} {payload['trim_name']}")
        print(f"    Price: £{payload['price_min_gbp']} - £{payload['price_max_gbp']}")
        print(f"    Dimensions: {payload['dimensions_mm']}")
        print(f"    Updated at: {payload['updated_at']} ({type(payload['updated_at'])})")
    
    # バリデーションのテスト
    validator = DataValidator()
    for i, payload in enumerate(db_ready_payloads):
        is_valid, errors = validator.validate_payload(payload)
        print(f"  Payload {i+1} validation: {'PASS' if is_valid else 'FAIL'}")
        if errors:
            for error in errors:
                print(f"    - {error}")
