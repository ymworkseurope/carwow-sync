#!/usr/bin/env python3
"""
data_processor.py - 改良版
データ変換、データベース形式への整形を行うモジュール
"""
import os
import re
import json
import uuid
from datetime import datetime
from typing import Dict, List, Optional, Any, Union

# ======================== Configuration ========================
GBP_TO_JPY = 195.0  # 固定レート（実装時は環境変数から取得可能）
UUID_NAMESPACE = uuid.UUID("12345678-1234-5678-1234-123456789012")

# 日本語翻訳マッピング
MAKE_JA_MAP = {
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
    "fiat": "フィアット",
    "ford": "フォード",
    "genesis": "ジェネシス",
    "honda": "ホンダ",
    "hyundai": "ヒュンダイ",
    "jaguar": "ジャガー",
    "jeep": "ジープ",
    "kia": "キア",
    "land rover": "ランドローバー",
    "lexus": "レクサス",
    "lotus": "ロータス",
    "mazda": "マツダ",
    "mercedes-benz": "メルセデス・ベンツ",
    "mg": "MG",
    "mini": "MINI",
    "nissan": "日産",
    "peugeot": "プジョー",
    "polestar": "ポールスター",
    "porsche": "ポルシェ",
    "renault": "ルノー",
    "seat": "セアト",
    "skoda": "シュコダ",
    "smart": "スマート",
    "subaru": "スバル",
    "suzuki": "スズキ",
    "tesla": "テスラ",
    "toyota": "トヨタ",
    "vauxhall": "ボクスホール",
    "volkswagen": "フォルクスワーゲン",
    "volvo": "ボルボ"
}

BODY_TYPE_JA_MAP = {
    "Electric": "電気自動車",
    "Hybrid": "ハイブリッド",
    "Convertible": "カブリオレ",
    "Estate": "ステーションワゴン",
    "Hatchback": "ハッチバック",
    "Saloon": "セダン",
    "Coupe": "クーペ",
    "Sports": "スポーツカー",
    "SUV": "SUV"
}

FUEL_JA_MAP = {
    "Electric": "電気",
    "Petrol": "ガソリン",
    "Diesel": "ディーゼル",
    "Hybrid": "ハイブリッド",
    "Plug-in Hybrid": "プラグインハイブリッド"
}

TRANSMISSION_JA_MAP = {
    "Automatic": "AT",
    "Manual": "MT",
    "CVT": "CVT",
    "DSG": "DSG",
    "DCT": "DCT"
}

DRIVE_TYPE_JA_MAP = {
    "Front wheel drive": "FF（前輪駆動）",
    "Rear wheel drive": "FR（後輪駆動）",
    "All wheel drive": "AWD（全輪駆動）",
    "Four wheel drive": "4WD（四輪駆動）"
}

# ======================== Data Processor ========================
class DataProcessor:
    """スクレイピングデータをデータベース形式に変換"""
    
    def process_vehicle_data(self, scraped_data: Dict) -> List[Dict]:
        """
        スクレイピングデータをデータベース形式に変換
        グレードごとに1レコード生成
        """
        records = []
        
        # 基本情報の取得
        slug = scraped_data['slug']
        make_en = scraped_data['make_en']
        model_en = scraped_data['model_en']
        make_ja = self._translate_make(make_en)
        model_ja = model_en  # モデル名は通常そのまま
        overview_en = scraped_data['overview_en']
        overview_ja = overview_en  # DeepL APIは後で実装
        catalog_url = scraped_data['catalog_url']
        
        # 共通価格情報
        base_prices = scraped_data['prices']
        
        # ボディタイプ
        body_type = scraped_data.get('body_types', [])
        body_type_ja = [BODY_TYPE_JA_MAP.get(bt, bt) for bt in body_type]
        
        # カラー
        colors = scraped_data.get('colors', [])
        
        # メディアURL
        media_urls = scraped_data.get('media_urls', [])
        
        # 仕様情報
        specs = scraped_data.get('specifications', {})
        
        # 各グレードごとにレコード生成
        grades = scraped_data.get('grades', [])
        if not grades:
            grades = [{'grade': '-', 'engine': '-'}]
        
        for grade_data in grades:
            grade_name = grade_data.get('grade', '-')
            engine = grade_data.get('engine', '-')
            
            # グレード固有の価格（なければベース価格を使用）
            price_min_gbp = grade_data.get('price_min_gbp') or base_prices.get('price_min_gbp')
            price_max_gbp = grade_data.get('price_max_gbp') or base_prices.get('price_max_gbp')
            price_used_gbp = base_prices.get('price_used_gbp')
            
            # 円換算
            price_min_jpy = self._convert_to_jpy(price_min_gbp)
            price_max_jpy = self._convert_to_jpy(price_max_gbp)
            price_used_jpy = self._convert_to_jpy(price_used_gbp)
            
            # スペックから情報抽出
            doors = self._extract_number(specs.get('doors') or specs.get('number of doors'))
            seats = self._extract_number(specs.get('seats') or specs.get('number of seats'))
            power_bhp = self._extract_number(specs.get('power_bhp') or specs.get('power'))
            
            # 燃料タイプ
            fuel = specs.get('fuel') or specs.get('fuel type') or '-'
            fuel_ja = FUEL_JA_MAP.get(fuel, fuel)
            
            # トランスミッション
            transmission = specs.get('transmission') or '-'
            transmission_ja = TRANSMISSION_JA_MAP.get(transmission, transmission)
            
            # ドライブタイプ
            drive_type = specs.get('drive_type') or '-'
            drive_type_ja = DRIVE_TYPE_JA_MAP.get(drive_type, drive_type)
            
            # 寸法
            dimensions_mm = self._format_dimensions(specs)
            dimensions_ja = dimensions_mm.replace('mm', 'mm').replace(',', '、') if dimensions_mm else '-'
            
            # full_model_ja
            full_model_ja = f"{make_ja} {model_en}"
            if grade_name and grade_name != '-' and grade_name != 'Standard':
                full_model_ja += f" {grade_name}"
            
            # UUID生成
            unique_key = f"{slug}_{grade_name}"
            record_id = str(uuid.uuid5(UUID_NAMESPACE, unique_key))
            
            # spec_json作成
            spec_json = {
                "doors": doors,
                "seats": seats,
                "power_bhp": power_bhp,
                "fuel_type": fuel,
                "transmission": transmission,
                "drive_type": drive_type,
                "grade": grade_name,
                "engine": engine,
                "dimensions": dimensions_mm,
                **{k: v for k, v in specs.items() if k not in ['doors', 'seats', 'power_bhp', 'fuel', 'transmission', 'drive_type']}
            }
            
            # カラー情報を日本語に変換（簡易的に）
            colors_ja = colors.copy()  # 実際のDeepL実装時に変換
            
            # レコード作成
            record = {
                'id': record_id,
                'slug': slug,
                'make_en': make_en,
                'model_en': model_en,
                'make_ja': make_ja,
                'model_ja': model_ja,
                'grade': grade_name,
                'engine': engine if engine != '-' else None,
                'body_type': body_type,
                'body_type_ja': body_type_ja,
                'fuel': fuel if fuel != '-' else None,
                'fuel_ja': fuel_ja if fuel_ja != '-' else None,
                'transmission': transmission if transmission != '-' else None,
                'transmission_ja': transmission_ja if transmission_ja != '-' else None,
                'price_min_gbp': price_min_gbp,
                'price_max_gbp': price_max_gbp,
                'price_used_gbp': price_used_gbp,
                'price_min_jpy': price_min_jpy,
                'price_max_jpy': price_max_jpy,
                'price_used_jpy': price_used_jpy,
                'overview_en': overview_en,
                'overview_ja': overview_ja,
                'doors': doors,
                'seats': seats,
                'power_bhp': power_bhp,
                'drive_type': drive_type if drive_type != '-' else None,
                'drive_type_ja': drive_type_ja if drive_type_ja != '-' else None,
                'dimensions_mm': dimensions_mm if dimensions_mm != '-' else None,
                'dimensions_ja': dimensions_ja if dimensions_ja != '-' else None,
                'colors': colors,
                'colors_ja': colors_ja,
                'media_urls': media_urls,
                'catalog_url': catalog_url,
                'full_model_ja': full_model_ja,
                'updated_at': datetime.utcnow().isoformat(),
                'spec_json': spec_json
            }
            
            records.append(record)
        
        return records
    
    def _translate_make(self, make_en: str) -> str:
        """メーカー名を日本語に変換"""
        make_lower = make_en.lower()
        return MAKE_JA_MAP.get(make_lower, make_en)
    
    def _convert_to_jpy(self, gbp: Optional[float]) -> Optional[float]:
        """GBPをJPYに変換"""
        if gbp is None:
            return None
        return round(gbp * GBP_TO_JPY)
    
    def _extract_number(self, value: Any) -> Optional[int]:
        """値から数値を抽出"""
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return int(value)
        if isinstance(value, str):
            match = re.search(r'\d+', value)
            if match:
                return int(match.group())
        return None
    
    def _format_dimensions(self, specs: Dict) -> Optional[str]:
        """寸法情報をフォーマット"""
        # dimensions_mmが既にある場合
        if 'dimensions_mm' in specs:
            return specs['dimensions_mm']
        
        # 個別の寸法から組み立て
        length = self._extract_number(specs.get('length') or specs.get('overall length'))
        width = self._extract_number(specs.get('width') or specs.get('overall width'))
        height = self._extract_number(specs.get('height') or specs.get('overall height'))
        
        if length and width and height:
            return f"{length} mm, {width} mm, {height} mm"
        
        return None

# ======================== Test Function ========================
def test_processor():
    """データプロセッサのテスト"""
    
    # テストデータ（スクレイパーの出力形式）
    test_data = {
        'slug': 'abarth/500e',
        'make_en': 'Abarth',
        'model_en': '500e',
        'overview_en': 'A complete review of the Abarth 500e.',
        'prices': {
            'price_min_gbp': 29985,
            'price_max_gbp': 34485,
            'price_used_gbp': 22958
        },
        'grades': [
            {'grade': 'Standard', 'engine': '114kW 42.2kWh Auto', 'price_min_gbp': 29985},
            {'grade': 'Turismo', 'engine': '114kW 42.2kWh Auto', 'price_min_gbp': 33985},
            {'grade': 'Scorpionissima', 'engine': '114kW 42.2kWh Auto', 'price_min_gbp': 34485}
        ],
        'specifications': {
            'doors': '3',
            'seats': '4',
            'power_bhp': '155',
            'fuel': 'Electric',
            'transmission': 'Automatic',
            'drive_type': 'Front wheel drive',
            'length': '3673',
            'width': '1682',
            'height': '1518'
        },
        'colors': ['Acid Green', 'Antidote White', 'Poison Blue'],
        'media_urls': ['https://example.com/img1.jpg', 'https://example.com/img2.jpg'],
        'body_types': ['Electric', 'Hatchback'],
        'catalog_url': 'https://www.carwow.co.uk/abarth/500e'
    }
    
    processor = DataProcessor()
    records = processor.process_vehicle_data(test_data)
    
    print(f"Generated {len(records)} records:")
    for i, record in enumerate(records, 1):
        print(f"\nRecord {i}:")
        print(f"  ID: {record['id']}")
        print(f"  Model: {record['full_model_ja']}")
        print(f"  Grade: {record['grade']}")
        print(f"  Engine: {record['engine']}")
        print(f"  Price: ¥{record['price_min_jpy']:,} - ¥{record['price_max_jpy']:,}")
        print(f"  Dimensions: {record['dimensions_mm']}")

if __name__ == "__main__":
    test_processor()
