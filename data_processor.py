#!/usr/bin/env python3
"""
data_processor.py
データ変換、翻訳、価格換算などの処理モジュール
"""
import os
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
    "alfa-romeo": "アルファロメオ",
    "alpine": "アルピーヌ",
    "aston-martin": "アストンマーチン",
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
    "land-rover": "ランドローバー",
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
    "rolls-royce": "ロールスロイス",
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
    "SUVs": "SUV",
    "Electric": "電気自動車",
    "Hybrid": "ハイブリッド",
    "Convertible": "カブリオレ",
    "Estate": "ステーションワゴン",
    "Hatchback": "ハッチバック",
    "Saloon": "セダン",
    "Coupe": "クーペ",
    "Sports": "スポーツカー",
    "Small cars": "小型車",
    "Hot hatches": "ホットハッチ",
    "People carriers": "ミニバン",
    "Camper vans": "キャンピングカー",
    "Electric vans": "電気バン"
}

TRANSMISSION_JA = {
    "Automatic": "AT",
    "Manual": "MT",
    "CVT": "CVT",
    "Electric": "EV",
    "Semi-automatic": "セミAT",
    "Tiptronic": "ティプトロニック",
    "DSG": "DSG",
    "DCT": "DCT"
}

# ======================== Translation Service ========================
class TranslationService:
    """翻訳サービス"""
    
    @staticmethod
    def translate_with_deepl(text: str, target_lang: str = "JA") -> str:
        """DeepL APIで翻訳（エラー時は原文を返す）"""
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
        
        # 単語単位でチェック
        for eng, jpn in TRANSMISSION_JA.items():
            if eng.lower() in trans_en.lower():
                return jpn
        
        return trans_en

# ======================== Data Processor ========================
class DataProcessor:
    """データ変換・処理クラス"""
    
    def __init__(self):
        self.translator = TranslationService()
    
    def process_vehicle_data(self, raw_data: Dict) -> Dict:
        """
        生データをデータベース形式に変換
        
        Args:
            raw_data: スクレイパーから取得した生データ
        
        Returns:
            データベース用に整形されたデータ
        """
        # UUID生成
        vehicle_id = str(uuid.uuid5(UUID_NAMESPACE, raw_data['slug']))
        
        # メーカー・モデル名の処理
        make_en = self._clean_make_name(raw_data.get('make', ''))
        model_en = self._clean_model_name(raw_data.get('model', ''))
        
        # 日本語翻訳
        make_ja = self.translator.translate_make(make_en)
        model_ja = model_en  # モデル名は通常そのまま
        overview_ja = self.translator.translate_with_deepl(raw_data.get('overview', ''))
        
        # ボディタイプ処理
        body_types_en = raw_data.get('body_types', [])
        body_types_ja = [self.translator.translate_body_type(bt) for bt in body_types_en]
        
        # 価格処理
        price_min_gbp = self._safe_int(raw_data.get('price_min_gbp'))
        price_max_gbp = self._safe_int(raw_data.get('price_max_gbp'))
        price_used_gbp = self._safe_int(raw_data.get('price_used_gbp'))
        
        price_min_jpy = self._convert_to_jpy(price_min_gbp)
        price_max_jpy = self._convert_to_jpy(price_max_gbp)
        price_used_jpy = self._convert_to_jpy(price_used_gbp)
        
        # トランスミッション処理
        transmission_en = raw_data.get('transmission', '')
        transmission_ja = self.translator.translate_transmission(transmission_en)
        
        # スペック情報の整理
        spec_json = self._compile_specifications(raw_data)
        
        # 画像URLの処理
        media_urls = self._process_media_urls(raw_data.get('images', []))
        
        # 最終的なペイロード構築
        payload = {
            'id': vehicle_id,
            'slug': raw_data['slug'],
            'make_en': make_en,
            'model_en': model_en,
            'make_ja': make_ja,
            'model_ja': model_ja,
            'body_type': body_types_en,
            'body_type_ja': body_types_ja,
            'fuel': raw_data.get('fuel_type'),
            'price_min_gbp': price_min_gbp,
            'price_max_gbp': price_max_gbp,
            'price_used_gbp': price_used_gbp,
            'price_min_jpy': price_min_jpy,
            'price_max_jpy': price_max_jpy,
            'price_used_jpy': price_used_jpy,
            'overview_en': raw_data.get('overview', ''),
            'overview_ja': overview_ja,
            'spec_json': json.dumps(spec_json, ensure_ascii=False),
            'media_urls': media_urls,
            'catalog_url': raw_data.get('url'),
            'doors': self._safe_int(raw_data.get('doors')),
            'seats': self._safe_int(raw_data.get('seats')),
            'dimensions_mm': raw_data.get('dimensions') or spec_json.get('dimensions_structured'),
            'drive_type': transmission_en,
            'drive_type_ja': transmission_ja,
            'grades': raw_data.get('grades'),
            'engines': raw_data.get('engines'),
            'colors': raw_data.get('colors'),
            'full_model_ja': f"{make_ja} {model_en}",
            'updated_at': datetime.utcnow().isoformat(timespec='seconds') + 'Z'
        }
        
        return payload
    
    def _clean_make_name(self, make: str) -> str:
        """メーカー名のクリーニング"""
        # ハイフンをスペースに変換して正規化
        make = make.replace('-', ' ')
        
        # 特殊ケースの処理
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
        
        # 通常は各単語の頭文字を大文字化
        return make.title()
    
    def _clean_model_name(self, model: str) -> str:
        """モデル名のクリーニング"""
        # 基本的に大文字化
        model = model.upper()
        
        # 特殊ケース（例：e-tron → e-tron）
        if 'E-TRON' in model:
            model = model.replace('E-TRON', 'e-tron')
        
        return model
    
    def _safe_int(self, value: Any) -> Optional[int]:
        """安全な整数変換"""
        if value is None:
            return None
        
        if isinstance(value, (int, float)):
            return int(value)
        
        if isinstance(value, str):
            # カンマや通貨記号を除去
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
        specs = raw_data.get('specifications', {})
        
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
        
        return processed[:40]  # 最大40枚

# ======================== Validation ========================
class DataValidator:
    """データ検証クラス"""
    
    @staticmethod
    def validate_payload(payload: Dict) -> tuple[bool, List[str]]:
        """
        データの妥当性を検証
        
        Returns:
            (is_valid, error_messages)
        """
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
        
        # 画像URLの検証
        media_urls = payload.get('media_urls', [])
        if not isinstance(media_urls, list):
            errors.append("media_urls must be a list")
        
        return len(errors) == 0, errors

# ======================== Test Function ========================
def test_processor():
    """データプロセッサーのテスト"""
    
    # テストデータ
    raw_data = {
        'slug': 'audi/a4',
        'make': 'audi',
        'model': 'a4',
        'title': 'Audi A4 Review',
        'overview': 'The Audi A4 is a premium compact executive car.',
        'price_min_gbp': 35000,
        'price_max_gbp': 55000,
        'price_used_gbp': 28000,
        'fuel_type': 'Petrol',
        'doors': 4,
        'seats': 5,
        'transmission': 'Automatic',
        'body_types': ['Saloon', 'Estate'],
        'colors': ['Glacier White', 'Mythos Black'],
        'images': [
            'https://example.com/image1.jpg',
            'https://example.com/image2.jpg'
        ],
        'url': 'https://www.carwow.co.uk/audi/a4',
        'specifications': {
            'Engine': '2.0 TFSI',
            'Power': '190 hp'
        }
    }
    
    # 処理実行
    processor = DataProcessor()
    payload = processor.process_vehicle_data(raw_data)
    
    # 結果表示
    print("Processed payload:")
    for key, value in payload.items():
        if isinstance(value, list):
            print(f"  {key}: {value[:2]}... ({len(value)} items)")
        elif isinstance(value, str) and len(value) > 50:
            print(f"  {key}: {value[:50]}...")
        else:
            print(f"  {key}: {value}")
    
    # 検証
    validator = DataValidator()
    is_valid, errors = validator.validate_payload(payload)
    
    print(f"\nValidation: {'PASS' if is_valid else 'FAIL'}")
    if errors:
        for error in errors:
            print(f"  - {error}")


if __name__ == "__main__":
    test_processor()
