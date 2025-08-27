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
from typing import Dict, List, Optional, Any

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
        
        # 基本価格処理
        price_min_gbp = self._safe_int(raw_data.get('price_min_gbp'))
        price_max_gbp = self._safe_int(raw_data.get('price_max_gbp'))
        price_used_gbp = self._safe_int(raw_data.get('price_used_gbp'))
        
        price_min_jpy = self._convert_to_jpy(price_min_gbp)
        price_max_jpy = self._convert_to_jpy(price_max_gbp)
        price_used_jpy = self._convert_to_jpy(price_used_gbp)
        
        # doors/seatsを取得
        doors = self._safe_int(raw_data.get('doors'))
        seats = self._safe_int(raw_data.get('seats'))
        
        # カラー処理
        colors = raw_data.get('colors', [])
        
        # メディアURL処理
        media_urls = self._process_media_urls(raw_data.get('images', []))
        
        # トリム情報を取得
        trims = raw_data.get('trims', [])
        if not trims:
            trims = [{'trim_name': 'Standard'}]
        
        # 各トリムごとにペイロードを作成
        payloads = []
        for trim_index, trim in enumerate(trims):
            # UUID生成（トリムごとにユニーク）
            trim_id = f"{raw_data['slug']}_{trim.get('trim_name', 'Standard')}_{trim_index}"
            vehicle_id = str(uuid.uuid5(UUID_NAMESPACE, trim_id))
            
            # トリム固有情報
            trim_name = self._clean_trim_name(trim.get('trim_name', 'Standard'))
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
            if trim_name and trim_name != 'Standard':
                full_model_parts.append(trim_name)
            full_model_ja = ' '.join(full_model_parts)
            
            # スペック情報の整理
            spec_json = self._compile_specifications(raw_data)
            spec_json.update({
                'trim_info': {
                    'trim_name': trim_name,
                    'engine': engine,
                    'fuel_type': fuel_en,
                    'power_bhp': trim.get('power_bhp'),
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
                'trim_name': trim_name,
                'grade': trim_name,  # grade列として追加
                'engine': engine,    # engine列として追加
                'body_type': body_types_en,
                'body_type_ja': body_types_ja,
                'fuel': fuel_en,
                'fuel_ja': fuel_ja,
                'transmission': transmission_en,
                'transmission_ja': transmission_ja,
                'price_min_gbp': price_min_gbp,
                'price_max_gbp': price_max_gbp,
                'price_used_gbp': price_used_gbp,
                'price_min_jpy': price_min_jpy,
                'price_max_jpy': price_max_jpy,
                'price_used_jpy': price_used_jpy,
                'overview_en': raw_data.get('overview', ''),
                'overview_ja': overview_ja,
                'doors': doors,
                'seats': seats,
                'power_bhp': trim.get('power_bhp'),
                'drive_type': drive_en,
                'drive_type_ja': drive_ja,
                'dimensions_mm': raw_data.get('dimensions') or spec_json.get('dimensions_structured'),
                'colors': colors,
                'media_urls': media_urls,
                'catalog_url': raw_data.get('url'),
                'full_model_ja': full_model_ja,
                'updated_at': datetime.utcnow().isoformat(timespec='seconds') + 'Z',
                'spec_json': json.dumps(spec_json, ensure_ascii=False)
            }
            
            payloads.append(payload)
        
        return payloads
    
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
            return "Standard"
        
        # 不要なパターンを除去
        patterns_to_remove = [
            r'\b\d+\s*s\b',      # "0 s", "2 s" など
            r'^\d+\s+',           # 先頭の数字
            r'\s+\d+$',           # 末尾の数字
            r'^0\s',              # 先頭の"0 "
        ]
        
        cleaned = trim_name
        for pattern in patterns_to_remove:
            cleaned = re.sub(pattern, '', cleaned, flags=re.IGNORECASE)
        
        cleaned = cleaned.strip()
        
        # 空になった場合や無効な値の場合はStandardを返す
        if not cleaned or cleaned in ['s', 'S', '0']:
            return 'Standard'
        
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
    
    def _convert_to_jpy(self, gbp: Optional[int]) -> Optional[int]:
        """GBPからJPYに変換"""
        if gbp is None:
            return None
        return int(gbp * GBP_TO_JPY)
    
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
            # 正規化
            if '?' in url:
                url = url.split('?')[0]
            
            # 重複チェック
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
        
        # 必須フィールドのチェック
        required_fields = ['id', 'slug', 'make_en', 'model_en']
        for field in required_fields:
            if not payload.get(field):
                errors.append(f"Missing required field: {field}")
        
        # スラッグの形式チェック
        if 'slug' in payload:
            slug = payload['slug']
            if '/' not in slug:
                errors.append(f"Invalid slug format: {slug}")
        
        # 価格の妥当性チェック
        price_min = payload.get('price_min_gbp')
        price_max = payload.get('price_max_gbp')
        if price_min and price_max:
            if price_min > price_max:
                errors.append(f"Price min ({price_min}) > Price max ({price_max})")
        
        return len(errors) == 0, errors
