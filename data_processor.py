#!/usr/bin/env python3
"""
data_processor.py - データ処理モジュール
スクレイピングデータを処理して保存用フォーマットに変換
"""
import re
import json
from typing import Dict, List, Optional, Any
from datetime import datetime

class DataProcessor:
    """データ処理クラス"""
    
    def __init__(self):
        self.gbp_to_jpy = 185  # 為替レート（更新可能）
    
    def process_vehicle_data(self, raw_data: Optional[Dict]) -> List[Dict]:
        """
        車両データを処理してデータベース用レコードに変換
        グレードごとに別レコードとして返す
        """
        if not raw_data:
            return []
        
        records = []
        base_data = self._extract_base_data(raw_data)
        
        # グレードごとにレコードを作成
        grades = raw_data.get('grades', [])
        if not grades:
            grades = [{'grade': 'Standard', 'engine': '-', 'price_min_gbp': None}]
        
        for grade_info in grades:
            record = base_data.copy()
            
            # グレード情報
            record['grade'] = grade_info.get('grade', 'Standard')
            record['engine'] = self._clean_engine_info(grade_info.get('engine', ''))
            
            # グレード別価格
            if grade_info.get('price_min_gbp'):
                record['price_min_gbp'] = grade_info['price_min_gbp']
                record['price_min_jpy'] = int(grade_info['price_min_gbp'] * self.gbp_to_jpy)
            
            # 日本語変換
            record = self._add_japanese_fields(record)
            
            # spec_jsonフィールドに詳細情報を格納
            record['spec_json'] = self._create_spec_json(raw_data, grade_info)
            
            # タイムスタンプ
            record['updated_at'] = datetime.now().isoformat()
            
            # IDを生成（slug + grade のハッシュ）
            record['id'] = abs(hash(f"{record['slug']}_{record['grade']}")) % (10**9)
            
            records.append(record)
        
        return records
    
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
            'colors': raw_data.get('colors', []),
            'media_urls': raw_data.get('media_urls', []),
            'catalog_url': raw_data.get('catalog_url', ''),
            
            # スペック
            'doors': self._extract_number(specs.get('doors')),
            'seats': self._extract_number(specs.get('seats')),
            'transmission': specs.get('transmission', ''),
            'dimensions_mm': specs.get('dimensions_mm', ''),
            
            # 価格（基本）
            'price_min_gbp': prices.get('price_min_gbp'),
            'price_max_gbp': prices.get('price_max_gbp'),
            'price_used_gbp': prices.get('price_used_gbp'),
        }
        
        # 日本円価格を計算
        if base['price_min_gbp']:
            base['price_min_jpy'] = int(base['price_min_gbp'] * self.gbp_to_jpy)
        if base['price_max_gbp']:
            base['price_max_jpy'] = int(base['price_max_gbp'] * self.gbp_to_jpy)
        if base['price_used_gbp']:
            base['price_used_jpy'] = int(base['price_used_gbp'] * self.gbp_to_jpy)
        
        # 燃料タイプとパワーを推定
        base['fuel'] = self._detect_fuel_type(raw_data)
        base['power_bhp'] = self._extract_power(raw_data)
        base['drive_type'] = self._detect_drive_type(raw_data)
        
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
            'Nissan': '日産',
            'Peugeot': 'プジョー',
            'Polestar': 'ポールスター',
            'Porsche': 'ポルシェ',
            'Renault': 'ルノー',
            'SEAT': 'セアト',
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
        
        # フルモデル名（日本語）
        record['full_model_ja'] = f"{record['make_ja']} {record['model_ja']}"
        if record.get('grade') and record['grade'] != 'Standard':
            record['full_model_ja'] += f" {record['grade']}"
        
        # ボディタイプの日本語変換
        body_type_ja_map = {
            'Electric': '電気自動車',
            'Hatchback': 'ハッチバック',
            'SUV': 'SUV',
            'Convertible': 'コンバーチブル',
            'Sedan': 'セダン',
            'Coupe': 'クーペ',
            'Estate': 'ステーションワゴン',
            'MPV': 'ミニバン'
        }
        
        record['body_type_ja'] = [
            body_type_ja_map.get(bt, bt) for bt in record.get('body_type', [])
        ]
        
        # 燃料タイプの日本語変換
        fuel_ja_map = {
            'Petrol': 'ガソリン',
            'Diesel': 'ディーゼル',
            'Electric': '電気',
            'Hybrid': 'ハイブリッド',
            'Plug-in Hybrid': 'プラグインハイブリッド'
        }
        record['fuel_ja'] = fuel_ja_map.get(record.get('fuel', ''), record.get('fuel', ''))
        
        # トランスミッションの日本語変換
        trans_ja_map = {
            'Automatic': 'オートマチック',
            'Manual': 'マニュアル',
            'CVT': 'CVT',
            'DCT': 'DCT'
        }
        record['transmission_ja'] = trans_ja_map.get(
            record.get('transmission', ''), 
            record.get('transmission', '')
        )
        
        # 駆動方式の日本語変換
        drive_ja_map = {
            'FWD': '前輪駆動',
            'RWD': '後輪駆動',
            'AWD': '全輪駆動',
            '4WD': '4WD'
        }
        record['drive_type_ja'] = drive_ja_map.get(
            record.get('drive_type', ''),
            record.get('drive_type', '')
        )
        
        # カラーの日本語変換（基本的な色のみ）
        color_ja_map = {
            'White': 'ホワイト',
            'Black': 'ブラック',
            'Silver': 'シルバー',
            'Grey': 'グレー',
            'Red': 'レッド',
            'Blue': 'ブルー',
            'Green': 'グリーン',
            'Yellow': 'イエロー',
            'Orange': 'オレンジ',
            'Brown': 'ブラウン',
            'Pearl': 'パール',
            'Metallic': 'メタリック'
        }
        
        colors_ja = []
        for color in record.get('colors', []):
            # 基本色を探して変換
            ja_color = color
            for en, ja in color_ja_map.items():
                if en.lower() in color.lower():
                    ja_color = color.replace(en, ja)
                    break
            colors_ja.append(ja_color)
        record['colors_ja'] = colors_ja
        
        # 寸法の日本語説明
        if record.get('dimensions_mm'):
            record['dimensions_ja'] = f"全長×全幅×全高: {record['dimensions_mm']}"
        
        # 概要の翻訳（簡易的）
        if not record.get('overview_ja'):
            record['overview_ja'] = record.get('overview_en', '')
        
        return record
    
    def _create_spec_json(self, raw_data: Dict, grade_info: Dict) -> Dict:
        """詳細仕様のJSONを作成"""
        spec_json = {
            'raw_specifications': raw_data.get('specifications', {}),
            'grade_details': grade_info,
            'body_types': raw_data.get('body_types', []),
            'available_colors': raw_data.get('colors', []),
            'media_count': len(raw_data.get('media_urls', [])),
            'scrape_date': datetime.now().isoformat()
        }
        
        # エンジン情報のパース
        engine_text = grade_info.get('engine', '')
        if engine_text:
            spec_json['engine_details'] = self._parse_engine_details(engine_text)
        
        return spec_json
    
    def _clean_engine_info(self, engine_text: str) -> str:
        """エンジン情報をクリーンアップ"""
        if not engine_text or engine_text == '-':
            return ''
        
        # 不要な文字を削除
        engine_text = re.sub(r'\s+', ' ', engine_text)
        engine_text = engine_text.strip()
        
        return engine_text
    
    def _parse_engine_details(self, engine_text: str) -> Dict:
        """エンジン情報を詳細にパース"""
        details = {}
        
        # パワー（馬力）
        hp_match = re.search(r'(\d+)\s*(?:hp|bhp)', engine_text, re.IGNORECASE)
        if hp_match:
            details['power_hp'] = int(hp_match.group(1))
        
        # パワー（kW）
        kw_match = re.search(r'(\d+)\s*kW', engine_text)
        if kw_match:
            details['power_kw'] = int(kw_match.group(1))
        
        # トルク
        torque_match = re.search(r'(\d+)\s*(?:Nm|lb-ft)', engine_text)
        if torque_match:
            details['torque'] = torque_match.group(0)
        
        # エンジンサイズ
        size_match = re.search(r'(\d+\.?\d*)\s*(?:L|litre|liter)', engine_text, re.IGNORECASE)
        if size_match:
            details['engine_size'] = float(size_match.group(1))
        
        # シリンダー数
        cyl_match = re.search(r'(\d+)\s*(?:cyl|cylinder)', engine_text, re.IGNORECASE)
        if cyl_match:
            details['cylinders'] = int(cyl_match.group(1))
        
        return details
    
    def _extract_number(self, value: Any) -> Optional[int]:
        """文字列から数値を抽出"""
        if value is None:
            return None
        
        if isinstance(value, int):
            return value
        
        if isinstance(value, str):
            match = re.search(r'\d+', value)
            if match:
                return int(match.group(0))
        
        return None
    
    def _extract_power(self, raw_data: Dict) -> Optional[int]:
        """パワー（馬力）を抽出"""
        # グレード情報から探す
        for grade in raw_data.get('grades', []):
            engine = grade.get('engine', '')
            if engine:
                hp_match = re.search(r'(\d+)\s*(?:hp|bhp)', engine, re.IGNORECASE)
                if hp_match:
                    return int(hp_match.group(1))
                
                # kWから変換
                kw_match = re.search(r'(\d+)\s*kW', engine)
                if kw_match:
                    kw = int(kw_match.group(1))
                    return int(kw * 1.341)  # kW to hp conversion
        
        return None
    
    def _detect_fuel_type(self, raw_data: Dict) -> str:
        """燃料タイプを検出"""
        text_to_check = ' '.join([
            raw_data.get('overview_en', ''),
            ' '.join(raw_data.get('body_types', [])),
            ' '.join(grade.get('engine', '') for grade in raw_data.get('grades', []))
        ]).lower()
        
        if 'electric' in text_to_check or 'ev' in text_to_check:
            return 'Electric'
        elif 'plug-in hybrid' in text_to_check or 'phev' in text_to_check:
            return 'Plug-in Hybrid'
        elif 'hybrid' in text_to_check:
            return 'Hybrid'
        elif 'diesel' in text_to_check:
            return 'Diesel'
        else:
            return 'Petrol'  # デフォルト
    
    def _detect_drive_type(self, raw_data: Dict) -> str:
        """駆動方式を検出"""
        text_to_check = ' '.join([
            raw_data.get('overview_en', ''),
            str(raw_data.get('specifications', {}))
        ]).lower()
        
        if 'awd' in text_to_check or 'all-wheel' in text_to_check:
            return 'AWD'
        elif '4wd' in text_to_check or 'four-wheel' in text_to_check:
            return '4WD'
        elif 'rwd' in text_to_check or 'rear-wheel' in text_to_check:
            return 'RWD'
        else:
            return 'FWD'  # デフォルト（前輪駆動）
