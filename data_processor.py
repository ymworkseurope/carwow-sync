#!/usr/bin/env python3
"""
data_processor.py - データ処理モジュール（改良版）
エンジン単位でレコードを生成、統一された"Information not available"処理
"""
import re
import json
from typing import Dict, List, Optional, Any
from datetime import datetime

class DataProcessor:
    """データ処理クラス"""
    
    def __init__(self):
        self.gbp_to_jpy = 185  # 為替レート
        self.na_value = 'Information not available'  # 統一された未取得値
        self.dash_value = 'ー'  # 日本語での未取得値
    
    def process_vehicle_data(self, raw_data: Optional[Dict]) -> List[Dict]:
        """
        車両データを処理してデータベース用レコードに変換
        エンジン単位で別レコードとして返す
        """
        if not raw_data:
            return []
        
        records = []
        base_data = self._extract_base_data(raw_data)
        
        # grades_enginesから各エンジン単位でレコードを作成
        grades_engines = raw_data.get('grades_engines', [])
        
        if not grades_engines:
            # データがない場合はデフォルトレコードを作成
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
            
            # グレード・エンジン情報（統一された値チェック）
            record['grade'] = self._normalize_value(grade_engine.get('grade'), self.na_value)
            record['engine'] = self._normalize_value(grade_engine.get('engine'), self.na_value)
            
            # 各種仕様（統一された値チェック）
            record['fuel'] = self._normalize_value(grade_engine.get('fuel'), self.na_value)
            record['transmission'] = self._normalize_value(grade_engine.get('transmission'), self.na_value)
            record['drive_type'] = self._normalize_value(grade_engine.get('drive_type'), self.na_value)
            
            # power_bhp（修正：Noneの場合はna_valueに）
            power_bhp = grade_engine.get('power_bhp')
            record['power_bhp'] = self.na_value if power_bhp is None else power_bhp
            
            # エンジンごとの価格（修正：Noneの場合はna_valueに）
            engine_price_gbp = grade_engine.get('engine_price_gbp')
            if engine_price_gbp is not None:
                record['engine_price_gbp'] = engine_price_gbp
                record['engine_price_jpy'] = int(engine_price_gbp * self.gbp_to_jpy)
            else:
                record['engine_price_gbp'] = self.na_value
                record['engine_price_jpy'] = self.dash_value
            
            # グレード別価格（フォールバック）
            if grade_engine.get('price_min_gbp'):
                record['price_min_gbp'] = grade_engine['price_min_gbp']
                record['price_min_jpy'] = int(grade_engine['price_min_gbp'] * self.gbp_to_jpy)
            
            # 日本語フィールドを追加
            record = self._add_japanese_fields(record)
            
            # full_model_jaを生成（make_ja + model_ja + grade + engine）
            parts = [record['make_ja'], record['model_ja']]
            if record['grade'] and record['grade'] != self.na_value:
                parts.append(record['grade'])
            if record['engine'] and record['engine'] != self.na_value:
                # エンジン情報を短縮して追加
                engine_short = self._shorten_engine_text(record['engine'])
                if engine_short:
                    parts.append(engine_short)
            record['full_model_ja'] = ' '.join(parts)
            
            # spec_jsonフィールドに詳細情報を格納
            record['spec_json'] = self._create_spec_json(raw_data, grade_engine)
            
            # タイムスタンプ
            record['updated_at'] = datetime.now().isoformat()
            
            # IDを生成（slug + grade + engine のハッシュ）
            unique_key = f"{record['slug']}_{record['grade']}_{record['engine']}"
            record['id'] = abs(hash(unique_key)) % (10**9)
            
            records.append(record)
        
        return records
    
    def _normalize_value(self, value: Any, default: str) -> str:
        """値を正規化（空値、N/A、Standard等を統一）"""
        if value is None or value == '' or value == 'N/A' or value == '-':
            return default
        # Standardをそのまま返すように修正（gradeでStandardは有効な値）
        return str(value)
    
    def _shorten_engine_text(self, engine_text: str) -> str:
        """エンジンテキストを短縮"""
        if engine_text == self.na_value:
            return ''
        
        # 例: "2.0L Petrol 200bhp" -> "2.0L"
        match = re.search(r'(\d+\.?\d*)\s*[Ll]', engine_text)
        if match:
            return match.group(0)
        
        # 電気自動車の場合
        if 'kWh' in engine_text:
            match = re.search(r'([\d.]+)\s*kWh', engine_text)
            if match:
                return f"{match.group(1)}kWh"
        
        # その他の場合は最初の単語のみ
        words = engine_text.split()
        return words[0] if words else ''
    
    def _extract_base_data(self, raw_data: Dict) -> Dict:
        """基本データを抽出"""
        specs = raw_data.get('specifications', {})
        prices = raw_data.get('prices', {})
        
        base = {
            'slug': raw_data.get('slug', ''),
            'make_en': raw_data.get('make_en', ''),
            'model_en': raw_data.get('model_en', ''),
            'overview_en': raw_data.get('overview_en', ''),
            'body_type': raw_data.get('body_types', []),
            'colors': self.na_value if not raw_data.get('colors') else raw_data.get('colors', []),
            'media_urls': raw_data.get('media_urls', []),
            'catalog_url': raw_data.get('catalog_url', ''),
            
            # スペック（修正：Noneの場合はna_valueに）
            'doors': self.na_value if specs.get('doors') is None else specs.get('doors'),
            'seats': self.na_value if specs.get('seats') is None else specs.get('seats'),
            'dimensions_mm': self.na_value if not specs.get('dimensions_mm') else specs.get('dimensions_mm'),
            
            # 価格（修正：Noneの場合はna_valueに）
            'price_min_gbp': self.na_value if prices.get('price_min_gbp') is None else prices.get('price_min_gbp'),
            'price_max_gbp': self.na_value if prices.get('price_max_gbp') is None else prices.get('price_max_gbp'),
            'price_used_gbp': self.na_value if prices.get('price_used_gbp') is None else prices.get('price_used_gbp'),
        }
        
        # 日本円価格を計算（修正：元の値がna_valueの場合はdash_valueに）
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
    
    def _add_japanese_fields(self, record: Dict) -> Dict:
        """日本語フィールドを追加"""
        
        # メーカー名の日本語変換
        make_ja_map = {
            'Abarth': 'アバルト',
            'Alfa Romeo': 'アルファロメオ',
            'Alpine': 'アルピーヌ',
            'Aston Martin': 'アストンマーティン',
            'Audi': 'アウディ',
            'Bentley': 'ベントレー',
            'BMW': 'BMW',
            'BYD': 'BYD',
            'Citroen': 'シトロエン',
            'Cupra': 'クプラ',
            'Dacia': 'ダチア',
            'DS': 'DS',
            'Fiat': 'フィアット',
            'Ford': 'フォード',
            'Genesis': 'ジェネシス',
            'Honda': 'ホンダ',
            'Hyundai': 'ヒュンダイ',
            'Jaguar': 'ジャガー',
            'Jeep': 'ジープ',
            'Kia': 'キア',
            'Land Rover': 'ランドローバー',
            'Lexus': 'レクサス',
            'Lotus': 'ロータス',
            'Mazda': 'マツダ',
            'Mercedes-Benz': 'メルセデス・ベンツ',
            'MG': 'MG',
            'MINI': 'ミニ',
            'Mini': 'ミニ',
            'Nissan': '日産',
            'Peugeot': 'プジョー',
            'Polestar': 'ポールスター',
            'Porsche': 'ポルシェ',
            'Renault': 'ルノー',
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
        record['model_ja'] = record['model_en']  # モデル名は基本的にそのまま
        
        # ボディタイプの日本語変換
        body_type_ja_map = {
            'Convertible': 'コンバーチブル',
            'SUV': 'SUV',
            'Hatchback': 'ハッチバック',
            'Saloon': 'セダン',
            'Estate': 'ステーションワゴン',
            'Coupe': 'クーペ',
            'Sports Car': 'スポーツカー',
            'People Carrier': 'ミニバン',
            'Electric': '電気自動車'
        }
        
        record['body_type_ja'] = [
            body_type_ja_map.get(bt, bt) for bt in record.get('body_type', [])
        ]
        
        # 燃料タイプの日本語変換（修正：na_valueの場合はdash_valueに）
        fuel_ja_map = {
            'Petrol': 'ガソリン',
            'Diesel': 'ディーゼル',
            'Electric': '電気',
            'Hybrid': 'ハイブリッド',
            'Plug-in Hybrid': 'プラグインハイブリッド',
            self.na_value: self.dash_value
        }
        record['fuel_ja'] = fuel_ja_map.get(record.get('fuel', self.na_value), self.dash_value)
        
        # トランスミッションの日本語変換（修正：na_valueの場合はdash_valueに）
        trans_ja_map = {
            'Automatic': 'オートマチック',
            'Manual': 'マニュアル',
            'CVT': 'CVT',
            'DCT': 'DCT',
            self.na_value: self.dash_value
        }
        
        # トランスミッションが文章形式の場合も処理
        trans = record.get('transmission', '')
        if trans == self.na_value:
            record['transmission_ja'] = self.dash_value
        elif 'Automatic' in trans:
            record['transmission_ja'] = 'オートマチック'
        elif 'Manual' in trans:
            record['transmission_ja'] = 'マニュアル'
        else:
            record['transmission_ja'] = trans_ja_map.get(trans, self.dash_value)
        
        # 駆動方式の日本語変換（修正：不明をdash_valueに）
        drive_ja_map = {
            'Front-wheel drive': '前輪駆動',
            'Rear-wheel drive': '後輪駆動',
            'All-wheel drive': '全輪駆動',
            'Four-wheel drive': '4WD',
            self.na_value: self.dash_value
        }
        
        # 駆動方式が文章形式の場合も処理
        drive = record.get('drive_type', '')
        if 'Front' in drive or 'front' in drive:
            record['drive_type_ja'] = '前輪駆動'
        elif 'Rear' in drive or 'rear' in drive:
            record['drive_type_ja'] = '後輪駆動'
        elif 'All' in drive or 'all' in drive:
            record['drive_type_ja'] = '全輪駆動'
        elif 'Four' in drive or '4' in drive:
            record['drive_type_ja'] = '4WD'
        else:
            record['drive_type_ja'] = drive_ja_map.get(drive, drive)
        
        # カラーの日本語変換（修正：colorsがna_valueの場合はdash_valueに）
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
            colors_ja = []
            for color in colors:
                # 基本色を探して変換
                ja_color = color
                for en, ja in color_ja_map.items():
                    if en.lower() in color.lower():
                        ja_color = color.replace(en, ja)
                        break
                colors_ja.append(ja_color)
            record['colors_ja'] = colors_ja
        
        # 寸法の日本語説明（修正：フォーマットを変更）
        if record.get('dimensions_mm') and record['dimensions_mm'] != self.na_value:
            record['dimensions_ja'] = self._format_dimensions_ja(record['dimensions_mm'])
        else:
            record['dimensions_ja'] = self.dash_value
        
        # 概要の翻訳プレースホルダー
        if not record.get('overview_ja'):
            record['overview_ja'] = record.get('overview_en', '')
        
        return record
    
    def _create_spec_json(self, raw_data: Dict, grade_engine: Dict) -> Dict:
        """詳細仕様のJSONを作成"""
        spec_json = {
            'raw_specifications': raw_data.get('specifications', {}),
            'grade_engine_details': grade_engine,
            'body_types': raw_data.get('body_types', []),
            'available_colors': raw_data.get('colors', []),
            'media_count': len(raw_data.get('media_urls', [])),
            'scrape_date': datetime.now().isoformat()
        }
        
        # エンジン情報の詳細パース
        engine_text = grade_engine.get('engine', '')
        if engine_text and engine_text != self.na_value:
            spec_json['engine_parsed'] = self._parse_engine_details(engine_text)
        
        # 追加の仕様情報
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
        
        # Information not availableの場合は空の辞書を返す
        if engine_text == self.na_value:
            return details
    
    def _format_dimensions_ja(self, dimensions_mm: str) -> str:
        """寸法を日本語形式にフォーマット（例：全長3.4m、全幅1.48m、全高2.0m）"""
        if not dimensions_mm or dimensions_mm == self.na_value:
            return self.dash_value
        
        # 例: "3400 x 1480 x 2000 mm" や "3400x1480x2000" の形式を想定
        # 数字を抽出
        numbers = re.findall(r'\d+', dimensions_mm)
        
        if len(numbers) >= 3:
            length_m = float(numbers[0]) / 1000  # mmをmに変換
            width_m = float(numbers[1]) / 1000
            height_m = float(numbers[2]) / 1000
            
            return f"全長{length_m}m、全幅{width_m}m、全高{height_m}m"
        
        # パースできない場合は元の値を返す
        return dimensions_mm
        
        # 電気自動車の場合（kW/kWh形式）
        electric_match = re.search(r'(\d+)\s*kW\s+([\d.]+)\s*kWh', engine_text)
        if electric_match:
            details['type'] = 'Electric'
            details['power_kw'] = int(electric_match.group(1))
            details['battery_kwh'] = float(electric_match.group(2))
            details['power_hp'] = int(details['power_kw'] * 1.341)
            return details
        
        # 内燃機関の場合
        # エンジンサイズ
        size_match = re.search(r'(\d+\.?\d*)\s*L', engine_text)
        if size_match:
            details['engine_size_l'] = float(size_match.group(1))
        
        # パワー
        hp_match = re.search(r'(\d+)\s*(?:hp|bhp)', engine_text, re.IGNORECASE)
        if hp_match:
            details['power_hp'] = int(hp_match.group(1))
        
        kw_match = re.search(r'(\d+)\s*kW', engine_text)
        if kw_match:
            details['power_kw'] = int(kw_match.group(1))
            if 'power_hp' not in details:
                details['power_hp'] = int(details['power_kw'] * 1.341)
        
        # トルク
        torque_match = re.search(r'(\d+)\s*(?:Nm|lb-ft)', engine_text)
        if torque_match:
            details['torque'] = torque_match.group(0)
        
        # 燃料タイプ
        if 'petrol' in engine_text.lower():
            details['type'] = 'Petrol'
        elif 'diesel' in engine_text.lower():
            details['type'] = 'Diesel'
        elif 'hybrid' in engine_text.lower():
            details['type'] = 'Hybrid'
        
        return details
