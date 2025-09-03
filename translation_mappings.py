#!/usr/bin/env python3
"""
translation_mappings.py - 翻訳辞書とマッピング定数
data_processor.pyから分離された辞書を管理
"""

# メーカー名のマッピング（英語→日本語）
MAKE_JA_MAP = {
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

# ボディタイプのマッピング（英語→日本語）
BODY_TYPE_JA_MAP = {
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

# 燃料タイプのマッピング（英語→日本語）
FUEL_JA_MAP = {
    'Petrol': 'ガソリン',
    'Diesel': 'ディーゼル',
    'Electric': '電気',
    'Hybrid': 'ハイブリッド',
    'Plug-in Hybrid': 'プラグインハイブリッド'
}

# トランスミッションのマッピング（英語→日本語）
TRANSMISSION_JA_MAP = {
    'Automatic': 'オートマチック',
    'Manual': 'マニュアル',
    'CVT': 'CVT',
    'DCT': 'DCT'
}

# ドライブタイプのマッピング（英語→日本語）
DRIVE_TYPE_JA_MAP = {
    'Front-wheel drive': 'FF',
    'Rear-wheel drive': 'FR',
    'All-wheel drive': 'AWD',
    'Four-wheel drive': '4WD'
}

# カラーのマッピング（英語→日本語）
COLOR_JA_MAP = {
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

# デフォルト値定数
DEFAULT_VALUES = {
    'na_value': 'Information not available',
    'dash_value': 'ー'
}

def get_translation(mapping: dict, key: str, default_value: str) -> str:
    """辞書から翻訳を取得"""
    if key == DEFAULT_VALUES['na_value'] or not key:
        return default_value
    return mapping.get(key, default_value)

def translate_body_types(body_types: list) -> list:
    """ボディタイプリストを翻訳"""
    if not body_types or body_types == ['Information not available']:
        return ['ー']
    return [
        BODY_TYPE_JA_MAP.get(bt, BODY_TYPE_JA_MAP.get(bt + 's', bt)) 
        for bt in body_types
    ]

def get_transmission_ja(transmission_text: str) -> str:
    """トランスミッションを翻訳"""
    if transmission_text == DEFAULT_VALUES['na_value']:
        return DEFAULT_VALUES['dash_value']
    elif 'Automatic' in transmission_text:
        return 'オートマチック'
    elif 'Manual' in transmission_text:
        return 'マニュアル'
    else:
        return TRANSMISSION_JA_MAP.get(transmission_text, DEFAULT_VALUES['dash_value'])

def get_drive_type_ja(drive_text: str) -> str:
    """駆動方式を翻訳"""
    if 'Front' in drive_text or 'front' in drive_text:
        return 'FF'
    elif 'Rear' in drive_text or 'rear' in drive_text:
        return 'FR'
    elif 'All' in drive_text or 'all' in drive_text:
        return 'AWD'
    elif 'Four' in drive_text or '4' in drive_text:
        return '4WD'
    else:
        return DRIVE_TYPE_JA_MAP.get(drive_text, DEFAULT_VALUES['dash_value'])
