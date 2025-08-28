#!/usr/bin/env python3
"""
data_processor.py
データ変換、翻訳、価格換算などの処理モジュール - 完全修正版
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
        
        # 基本価格処理 - 整数に変換（小数点を削除）
        price_min_gbp = self._safe_int_price(raw_data.get('price_min_gbp'))
        price_max_gbp = self._safe_int_price(raw_data.get('price_max_gbp'))
        price_used_gbp = self._safe_int_price(raw_data.get('price_used_gbp'))
        
        price_min_jpy = self._convert_to_jpy_int(price_min_gbp)
        price_max_jpy = self._convert_to_jpy_int(price_max_gbp)
        price_used_jpy = self._convert_to_jpy_int(price_used_gbp)
        
        # doors/seatsを取得
        doors = self._safe_smallint(raw_data.get('doors'))
        seats = self._safe_smallint(raw_data.get('seats'))
        
        # カラー処理
        colors = self._process_array_field(raw_data.get('colors', []))
        
        # メディアURL処理
        media_urls = self._process_media_urls(raw_data.get('images', []))
        
        # 寸法情報の処理（共通）
        dimensions_processed = self._extract_and_format_dimensions(raw_data)
        
        # トリム情報を取得
        trims = raw_data.get('trims', [])
        
        # トリムデータの検証とフィルタリング
        valid_trims = []
        for trim in trims:
            trim_name = self._clean_trim_name_for_grade(trim.get('trim_name', ''))
            if self._is_valid_trim_name_for_processing(trim_name) and trim_name != 'Standard':
                trim['trim_name'] = trim_name
                valid_trims.append(trim)
        
        # グレードとエンジン情報の収集
        all_grades = self._collect_grades(valid_trims)
        all_engines = self._collect_engines(valid_trims)
        
        # デバッグ用ログ
        if valid_trims:
            print(f"  Found {len(valid_trims)} valid trims: {[t.get('trim_name', 'Unknown') for t in valid_trims]}")
        else:
            print("  No valid trims found")
        
        # 車両データを1つのペイロードとして作成
        vehicle_id = str(uuid.uuid5(UUID_NAMESPACE, raw_data['slug']))
        
        # トランスミッション処理（メインデータから）
        transmission_en = raw_data.get('transmission', '')
        transmission_ja = self.translator.translate_transmission(transmission_en)
        
        # 燃料タイプ処理（メインデータから）
        fuel_en = raw_data.get('fuel_type', '')
        fuel_ja = self.translator.translate_fuel(fuel_en)
        
        # ドライブタイプ処理（メインデータから）
        drive_en = raw_data.get('drive_type', '')
        drive_ja = self.translator.translate_drive_type(drive_en)
        
        # full_model_jaの生成
        full_model_ja = f"{make_ja} {model_en}"
        
        # power_bhp
        power_bhp = None
        if valid_trims:
            power_bhp = self._safe_int(valid_trims[0].get('power_bhp'))
        
        # グレード名の決定 - トリムが見つからない場合は「無し」
        grade = "無し"
        engine = ""
        
        if valid_trims:
            # 複数のトリムがある場合は代表的なものを選択
            representative_trim = valid_trims[0]
            grade = representative_trim.get('trim_name', '無し')
            engine = representative_trim.get('engine', '')
            
            # power_bhpを更新
            power_bhp = self._safe_int(representative_trim.get('power_bhp'))
            
            # 代表トリムの情報があれば使用
            if representative_trim.get('transmission'):
                transmission_en = representative_trim.get('transmission', '')
                transmission_ja = self.translator.translate_transmission(transmission_en)
            
            if representative_trim.get('fuel_type'):
                fuel_en = representative_trim.get('fuel_type', '')
                fuel_ja = self.translator.translate_fuel(fuel_en)
            
            if representative_trim.get('drive_type'):
                drive_en = representative_trim.get('drive_type', '')
                drive_ja = self.translator.translate_drive_type(drive_en)
        
        # スペック情報の整理
        spec_json = self._compile_specifications(raw_data)
        spec_json.update({
            'doors': doors,
            'seats': seats,
            'grade': grade,
            'engine': engine,
            'representative_trim': grade if grade != "無し" else None
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
            'price_min_gbp': price_min_gbp,
            'price_max_gbp': price_max_gbp,
            'price_min_jpy': price_min_jpy,
            'price_max_jpy': price_max_jpy,
            'overview_en': raw_data.get('overview', ''),
            'overview_ja': overview_ja,
            'spec_json': spec_json,
            'media_urls': media_urls,
            'updated_at': datetime.utcnow().isoformat() + 'Z',
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
            'engine': engine,
            'grade': grade
        }
        
        return [payload]
    
    def _safe_int_price(self, value: Any) -> Optional[int]:
        """価格用の安全な整数変換（小数点を除去）"""
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
    
    def _convert_to_jpy_int(self, gbp: Optional[int]) -> Optional[int]:
        """GBPからJPYに変換（整数）"""
        if gbp is None:
            return None
        return int(gbp * GBP_TO_JPY)
    
    def _collect_grades(self, trims: List[Dict]) -> Dict:
        """全トリムからグレード情報を収集してJSONB形式で構造化"""
        grades = {}
        
        for trim in trims:
            trim_name = trim.get('trim_name', 'Standard')
            if trim_name == 'Standard':
                continue
                
            grade_info = {
                'name': trim_name,
                'engine': trim.get('engine', ''),
                'power_bhp': self._safe_int(trim.get('power_bhp')),
                'transmission': trim.get('transmission', ''),
                'fuel_type': trim.get('fuel_type', ''),
                'drive_type': trim.get('drive_type', ''),
                'price_rrp': self._safe_int_price(trim.get('price_rrp'))
            }
            
            # Noneの値を除去
            grade_info = {k: v for k, v in grade_info.items() if v is not None and v != ''}
            grades[trim_name] = grade_info
        
        return grades
    
    def _collect_engines(self, trims: List[Dict]) -> Dict:
        """全トリムからエンジン情報を収集してJSONB形式で構造化"""
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
                trim_name = trim.get('trim_name', 'Standard')
                if trim_name != 'Standard' and trim_name not in engines[engine]['associated_trims']:
                    engines[engine]['associated_trims'].append(trim_name)
        
        return engines
    
    def _extract_and_format_dimensions(self, raw_data: Dict) -> Optional[str]:
        """寸法データの抽出とフォーマット（改善版）"""
        # 複数のソースから寸法情報を取得
        dimensions_sources = [
            raw_data.get('dimensions'),
            raw_data.get('external_dimensions'),
            raw_data.get('specifications', {}).get('dimensions'),
            raw_data.get('specifications', {}).get('external_dimensions'),
            raw_data.get('specifications', {}).get('dimensions_structured'),
            raw_data.get('specs', {}).get('dimensions')
        ]
        
        # specifications内の個別フィールドもチェック
        spec_data = raw_data.get('specifications', {})
        
        # 寸法の抽出
        length = None
        width = None
        height = None
        wheelbase = None
        other_dimensions = {}
        
        # specifications内の個別フィールドから抽出
        dimension_keys = {
            'length': ['length', 'length (mm)', 'length mm', 'overall length', 'overall length (mm)', 'length (excluding bumpers)'],
            'width': ['width', 'width (mm)', 'width mm', 'overall width', 'overall width (mm)', 'width (excluding mirrors)', 'width (including mirrors)'],
            'height': ['height', 'height (mm)', 'height mm', 'overall height', 'overall height (mm)'],
            'wheelbase': ['wheelbase', 'wheelbase (mm)', 'wheelbase mm']
        }
        
        for dim_type, keys in dimension_keys.items():
            for key in keys:
                if key.lower() in [k.lower() for k in spec_data.keys()]:
                    # 実際のキーを見つける
                    actual_key = next(k for k in spec_data.keys() if k.lower() == key.lower())
                    value = self._extract_dimension_value(spec_data[actual_key])
                    if value:
                        if dim_type == 'length' and not length:
                            length = value
                        elif dim_type == 'width' and not width:
                            width = value
                        elif dim_type == 'height' and not height:
                            height = value
                        elif dim_type == 'wheelbase' and not wheelbase:
                            wheelbase = value
                        break
        
        # 他のソースからも情報を取得
        for source in dimensions_sources:
            if not source:
                continue
                
            if isinstance(source, dict):
                for key, value in source.items():
                    key_lower = key.lower()
                    dim_value = self._extract_dimension_value(value)
                    
                    if dim_value:
                        if not length and any(k in key_lower for k in ['length', 'long']):
                            length = dim_value
                        elif not width and 'width' in key_lower:
                            width = dim_value
                        elif not height and 'height' in key_lower:
                            height = dim_value
                        elif not wheelbase and 'wheelbase' in key_lower:
                            wheelbase = dim_value
                        else:
                            other_dimensions[key] = dim_value
            
            elif isinstance(source, str):
                # 文字列から寸法を抽出
                dimensions = self._parse_dimensions_string(source)
                if dimensions:
                    if not length and len(dimensions) > 0:
                        length = dimensions[0]
                    if not width and len(dimensions) > 1:
                        width = dimensions[1]
                    if not height and len(dimensions) > 2:
                        height = dimensions[2]
        
        # フォーマット
        result_parts = []
        
        # L x W x H 形式
        if length and width and height:
            result_parts.append(f"L{length} × W{width} × H{height} mm")
        elif length and width:
            result_parts.append(f"L{length} × W{width} mm")
        elif length:
            result_parts.append(f"Length: {length} mm")
        
        # ホイールベース
        if wheelbase:
            result_parts.append(f"Wheelbase: {wheelbase} mm")
        
        # その他の重要な寸法
        important_dims = ['ground clearance', 'boot capacity', 'turning circle', 'kerb weight']
        for key, value in other_dimensions.items():
            if any(dim in key.lower() for dim in important_dims):
                result_parts.append(f"{key}: {value}")
        
        return "; ".join(result_parts) if result_parts else None
    
    def _extract_dimension_value(self, value: Any) -> Optional[int]:
        """寸法値の抽出（文字列から数値を抽出）"""
        if isinstance(value, (int, float)):
            return int(value)
        
        if isinstance(value, str):
            import re
            
            # mm単位の場合
            mm_match = re.search(r'(\d{3,5})\s*mm', value)
            if mm_match:
                return int(mm_match.group(1))
            
            # cm単位の場合（mmに変換）
            cm_match = re.search(r'(\d+(?:\.\d+)?)\s*cm', value)
            if cm_match:
                return int(float(cm_match.group(1)) * 10)
            
            # m単位の場合（mmに変換）
            m_match = re.search(r'(\d+(?:\.\d+)?)\s*m', value)
            if m_match:
                return int(float(m_match.group(1)) * 1000)
            
            # カンマ区切りの数値
            comma_match = re.search(r'(\d{1,2},\d{3})', value)
            if comma_match:
                return int(comma_match.group(1).replace(',', ''))
            
            # 単位なしの数値（mmと仮定、3桁以上）
            num_match = re.search(r'(\d{3,5})', value)
            if num_match:
                return int(num_match.group(1))
        
        return None
    
    def _parse_dimensions_string(self, dim_string: str) -> List[int]:
        """寸法文字列から数値リストを抽出"""
        if not dim_string:
            return []
        
        import re
        numbers = re.findall(r'(\d{1,2}[,.]?\d{3,4})', dim_string)
        
        result = []
        for num_str in numbers:
            clean_num = num_str.replace(',', '').replace('.', '')
            try:
                if len(clean_num) >= 3:
                    result.append(int(clean_num))
            except ValueError:
                continue
        
        return result
    
    def _process_array_field(self, field_data: Union[List, str, None]) -> List[str]:
        """配列フィールドの適切な処理"""
        if not field_data:
            return []
        
        if isinstance(field_data, list):
            return [str(item) for item in field_data if item]
        
        if isinstance(field_data, str):
            return [item.strip() for item in field_data.split(',') if item.strip()]
        
        return []
    
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
    
    def _is_valid_trim_name_for_processing(self, trim_name: str) -> bool:
        """処理用のトリム名妥当性チェック"""
        if not trim_name or len(trim_name.strip()) < 2:
            return False
        
        # 無効なパターンを除外
        invalid_patterns = [
            r'^\d+\s*s?$',
            r'^s$',
            r'^\W+$',
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
    
    def _clean_trim_name_for_grade(self, trim_name: str) -> str:
        """グレード用のトリム名のクリーニング"""
        if not trim_name:
            return "無し"
        
        cleaned = trim_name.strip()
        
        # 無効なパターンをチェック
        invalid_patterns = [
            r'^\d+\s*s?$',
            r'^s$',
            r'^0\s',
            r'^standard$',
        ]
        
        for pattern in invalid_patterns:
            if re.match(pattern, cleaned, re.IGNORECASE):
                return "無し"
        
        # 不要な部分を除去
        patterns_to_remove = [
            r'\b\d+\s*s\b',
            r'^\d+\s+',
            r'\s+\d+$',
        ]
        
        for pattern in patterns_to_remove:
            cleaned = re.sub(pattern, '', cleaned, flags=re.IGNORECASE)
        
        cleaned = cleaned.strip()
        
        if not cleaned or len(cleaned) < 2:
            return "無し"
        
        return cleaned
    
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
    
    def _compile_specifications(self, raw_data: Dict) -> Dict:
        """スペック情報をまとめる"""
        specs = raw_data.get('specifications', {}).copy()
        
        specs.update({
            'fuel_type': raw_data.get('fuel_type'),
            'doors': raw_data.get('doors'),
            'seats': raw_data.get('seats'),
            'transmission': raw_data.get('transmission')
        })
        
        specs = {k: v for k, v in specs.items() if v is not None}
        
        return specs
    
    def _process_media_urls(self, urls: List[str]) -> List[str]:
        """メディアURLの処理"""
        processed = []
        seen = set()
        
        for url in urls:
            if not url:
                continue
                
            if '?' in url:
                url = url.split('?')[0]
            
            if url not in seen:
                processed.append(url)
                seen.add(url)
        
        return processed[:40]

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
        
        # グレード名の妥当性チェック
        grade = payload.get('grade', '')
        if not grade:
            errors.append("Grade field is missing")
        
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
            if not isinstance(updated_at, str):
                errors.append(f"Field updated_at must be string, got {type(updated_at)}")
            else:
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
                'timestamp with time zone': lambda v: isinstance(v, str)
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
            # データ処理
            payloads = processor.process_vehicle_data(raw_data)
            
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
    
    print(f"Generated {len(payloads)} payload(s):")
    for i, payload in enumerate(payloads):
        print(f"  Payload {i+1}: {payload['make_en']} {payload['model_en']} - Grade: {payload['grade']}")
        print(f"    Updated at: {payload['updated_at']} ({type(payload['updated_at'])})")
        print(f"    Price: £{payload.get('price_min_gbp', 'N/A')}")
    
    # バリデーションのテスト
    validator = DataValidator()
    for i, payload in enumerate(payloads):
        is_valid, errors = validator.validate_payload(payload)
        print(f"  Payload {i+1} validation: {'PASS' if is_valid else 'FAIL'}")
        if errors:
            for error in errors:
                print(f"    - {error}")
