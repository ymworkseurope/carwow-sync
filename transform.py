# transform.py
# rev: 2025-08-24 UUID修正版
import uuid
import hashlib
import json
from datetime import datetime
from typing import Dict, Any

def generate_uuid_from_slug(slug: str) -> str:
    """slugから決定的なUUIDを生成"""
    # 固定のnamespaceを使用（プロジェクト固有）
    namespace = uuid.UUID('12345678-1234-5678-1234-123456789012')
    return str(uuid.uuid5(namespace, slug))

def translate_to_japanese(english_text: str) -> str:
    """英語を日本語に翻訳（簡易版）"""
    if not english_text:
        return ""
    
    # 基本的な翻訳マッピング
    translation_map = {
        'Abarth': 'アバルト',
        'Alfa Romeo': 'アルファロメオ',
        'Alpine': 'アルピーヌ',
        'Audi': 'アウディ',
        'BMW': 'BMW',
        'BYD': 'BYD',
        'Citroen': 'シトロエン',
        'Cupra': 'クプラ',
        'DS': 'DS',
        'Dacia': 'ダチア',
        'Fiat': 'フィアット',
        'Ford': 'フォード',
        'GWM': 'GWM',
        'Genesis': 'ジェネシス',
        'Honda': 'ホンダ',
        'Hyundai': 'ヒュンダイ',
        'Ineos': 'イネオス',
        'Jaecoo': 'ジェイク',
        'Jeep': 'ジープ',
        'KGM Motors': 'KGMモーターズ',
        'Kia': 'キア',
        'Land Rover': 'ランドローバー',
        'Leapmotor': 'リープモーター',
        'Lexus': 'レクサス',
        'Lotus': 'ロータス',
        'MG': 'MG',
        'MINI': 'ミニ',
        'Mazda': 'マツダ',
        'Mercedes': 'メルセデス',
        'Mercedes-Benz': 'メルセデス・ベンツ',
        'Nissan': 'ニッサン',
        'Omoda': 'オモダ',
        'Peugeot': 'プジョー',
        'Polestar': 'ポールスター',
        'Renault': 'ルノー',
        'SEAT': 'セアト',
        'Skoda': 'シュコダ',
        'Skywell': 'スカイウェル',
        'Smart': 'スマート',
        'Subaru': 'スバル',
        'Suzuki': 'スズキ',
        'Tesla': 'テスラ',
        'Toyota': 'トヨタ',
        'Vauxhall': 'ボクスホール',
        'Volkswagen': 'フォルクスワーゲン',
        'Volvo': 'ボルボ',
        'XPeng': 'エックスペン',
        # モデル名の翻訳例
        '500E': '500E',
        '500E Cabrio': '500Eカブリオ',
        '595 Abarth': '595アバルト',
        '595C': '595C',
        # 車体タイプ
        'Sports cars': 'スポーツカー',
        'Convertibles': 'オープンカー',
        'Hatchbacks': 'ハッチバック',
        'Saloons': 'セダン',
        'SUVs': 'SUV',
        'Estates': 'エステート',
        'MPVs': 'MPV',
        'Coupes': 'クーペ',
        # 燃料タイプ
        'Electric': '電気',
        'Petrol': 'ガソリン',
        'Diesel': 'ディーゼル',
        'Hybrid': 'ハイブリッド',
        'Plug-in Hybrid': 'プラグインハイブリッド'
    }
    
    return translation_map.get(english_text, english_text)

def safe_price_conversion(price_gbp, exchange_rate=195):
    """安全な価格変換"""
    if price_gbp is None:
        return None
    try:
        price_float = float(price_gbp)
        if price_float < 0:
            return None
        return int(price_float * exchange_rate)
    except (ValueError, TypeError):
        return None

def to_payload(raw: Dict[str, Any]) -> Dict[str, Any]:
    """スクレイピングデータをSupabase用ペイロードに変換"""
    
    # slugの取得と検証
    slug = raw.get("slug", "").strip()
    if not slug:
        raise ValueError("Missing required field: slug")
    
    # UUIDを生成（重要な修正点）
    uuid_id = generate_uuid_from_slug(slug)
    
    # 基本情報の取得
    make_en = raw.get("make_en", "").strip()
    model_en = raw.get("model_en", "").strip()
    
    # 価格情報の処理
    price_min_gbp = raw.get("price_min_gbp")
    price_max_gbp = raw.get("price_max_gbp")
    
    # JPY変換
    price_min_jpy = safe_price_conversion(price_min_gbp)
    price_max_jpy = safe_price_conversion(price_max_gbp)
    
    # spec_jsonの処理
    spec_json = raw.get("spec_json", "{}")
    if isinstance(spec_json, dict):
        spec_json = json.dumps(spec_json, ensure_ascii=False)
    elif spec_json is None:
        spec_json = "{}"
    
    # media_urlsの処理
    media_urls = raw.get("media_urls", "[]")
    if isinstance(media_urls, list):
        media_urls = json.dumps(media_urls, ensure_ascii=False)
    elif media_urls is None:
        media_urls = "[]"
    
    # overview情報の処理
    overview_en = raw.get("overview_en", "").strip()
    
    # ペイロード構築
    payload = {
        "id": uuid_id,  # 修正：UUID形式で生成
        "slug": slug,
        "make_en": make_en,
        "model_en": model_en,
        "make_ja": translate_to_japanese(make_en),
        "model_ja": translate_to_japanese(model_en),
        "overview_en": overview_en,
        "overview_ja": translate_to_japanese(overview_en),
        "body_type": raw.get("body_type"),
        "fuel": raw.get("fuel"),
        "price_min_gbp": price_min_gbp,
        "price_max_gbp": price_max_gbp,
        "price_min_jpy": price_min_jpy,
        "price_max_jpy": price_max_jpy,
        "spec_json": spec_json,
        "media_urls": media_urls,
        "updated_at": datetime.utcnow().isoformat() + 'Z'
    }
    
    return payload
