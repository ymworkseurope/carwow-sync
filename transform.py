# transform.py
# rev: 2025-08-24 修正版（slug検証強化）
"""
raw.jsonl → 変換済み dict を返すユーティリティ
表記ゆれ対策のためのローカルグロッサリー機能付き
"""
import os, json, datetime as dt, backoff, requests
import hashlib
import re

DEEPL_KEY  = os.getenv("DEEPL_KEY")
GBP_TO_JPY = float(os.getenv("GBP_TO_JPY", "195"))

# 自動車業界専用グロッサリー（表記統一用）
AUTO_GLOSSARY = {
    # イギリス系メーカー
    "Aston Martin": "アストンマーチン",
    "Bentley": "ベントレー",
    "Jaguar": "ジャガー",
    "Land Rover": "ランドローバー",
    "Lotus": "ロータス",
    "MG": "MG",
    "Rolls-Royce": "ロールスロイス",
    "Vauxhall": "ボクスホール",
    "McLaren": "マクラーレン",
    
    # イタリア系メーカー
    "Abarth": "アバルト",
    "Alfa Romeo": "アルファロメオ",
    "Fiat": "フィアット",
    "Lamborghini": "ランボルギーニ",
    "Maserati": "マセラティ",
    "Ferrari": "フェラーリ",
    
    # その他ヨーロッパ系
    "Polestar": "ポールスター",
    "Volvo": "ボルボ",
    "Cupra": "クプラ",
    "SEAT": "セアト",
    "Skoda": "シュコダ",
    "Audi": "アウディ",
    "BMW": "BMW",
    "Ford": "フォード",
    "Mercedes": "メルセデス",
    "Mercedes-Benz": "メルセデス・ベンツ",
    "MINI": "ミニ",
    "Mini": "ミニ",
    "Porsche": "ポルシェ",
    "Smart": "スマート",
    "Volkswagen": "フォルクスワーゲン",
    "Alpine": "アルピーヌ",
    "Citroen": "シトロエン",
    "DS": "DSオートモビル",
    "Peugeot": "プジョー",
    "Renault": "ルノー",
    "Dacia": "ダチア",
    
    # 韓国・アメリカ・その他
    "Hyundai": "ヒュンダイ",
    "Jeep": "ジープ",
    "Kia": "キア",
    "Tesla": "テスラ",
    "Genesis": "ジェネシス",
    
    # 日本系メーカー
    "Honda": "ホンダ",
    "Infiniti": "インフィニティ",
    "Lexus": "レクサス",
    "Mazda": "マツダ",
    "Mitsubishi": "三菱",
    "Nissan": "日産自動車",
    "Subaru": "スバル",
    "Suzuki": "スズキ",
    "Toyota": "トヨタ",
    
    # 車種・技術用語
    "Electric": "電気",
    "Hybrid": "ハイブリッド", 
    "Petrol": "ガソリン",
    "Diesel": "ディーゼル",
    "SUV": "SUV",
    "Hatchback": "ハッチバック",
    "Saloon": "セダン",
    "Estate": "エステート",
    "Coupe": "クーペ",
    "Convertible": "コンバーチブル",
    "MPV": "MPV",
    "Crossover": "クロスオーバー",
}

# 翻訳キャッシュ（セッション中の重複翻訳を防ぐ）
_translation_cache = {}

def _apply_glossary(text: str, glossary: dict) -> str:
    """テキストにグロッサリーを適用（大文字小文字を考慮）"""
    if not text:
        return text
    
    result = text
    # 完全一致を優先（長い語句から処理）
    for en_term, ja_term in sorted(glossary.items(), key=lambda x: len(x[0]), reverse=True):
        # 単語境界を考慮した置換
        pattern = r'\b' + re.escape(en_term) + r'\b'
        result = re.sub(pattern, ja_term, result, flags=re.IGNORECASE)
    
    return result

@backoff.on_exception(backoff.expo, requests.RequestException,
                      max_tries=3, jitter=None)
def _deepl(text: str, src="EN", tgt="JA") -> str:
    if not (text and text.strip()):
        return ""
    
    # キャッシュチェック
    cache_key = f"{src}:{tgt}:{text}"
    if cache_key in _translation_cache:
        return _translation_cache[cache_key]
    
    # DeepL翻訳
    translated = ""
    if DEEPL_KEY:
        try:
            r = requests.post(
                "https://api-free.deepl.com/v2/translate",
                data={
                    "auth_key": DEEPL_KEY,
                    "text": text, 
                    "source_lang": src, 
                    "target_lang": tgt,
                    "formality": "default"
                },
                timeout=30
            )
            r.raise_for_status()
            translated = r.json()["translations"][0]["text"]
        except Exception as e:
            print(f"翻訳エラー: {e}")
            translated = text
    else:
        translated = text
    
    # グロッサリー適用（翻訳後）
    final_result = _apply_glossary(translated, AUTO_GLOSSARY)
    
    # キャッシュに保存
    _translation_cache[cache_key] = final_result
    
    return final_result

def _generate_id(make: str, model: str, slug: str) -> str:
    """メーカー名、モデル名、スラッグからユニークIDを生成"""
    combined = f"{make}-{model}-{slug}".lower()
    return hashlib.md5(combined.encode()).hexdigest()[:12]

def _normalize_make_model(text: str) -> str:
    """メーカー名・モデル名の正規化"""
    if not text:
        return ""
    
    # 既に正規化済みの場合はそのまま返す
    text = text.strip()
    
    return text

def validate_slug_format(slug: str) -> bool:
    """slugの形式を検証"""
    if not slug:
        return False
    
    # メーカー名-モデル名の形式をチェック
    if not re.match(r'^[a-z0-9\-]+-[a-z0-9\-\+]+$', slug):
        return False
    
    # スラッシュが含まれていないことを確認
    if '/' in slug:
        return False
    
    return True

def to_payload(row: dict) -> dict:
    make   = _normalize_make_model(row.get("make_en", ""))
    model  = _normalize_make_model(row.get("model_en", ""))
    slug   = row.get("slug", "").strip()
    overview_en = row.get("overview_en", "").strip()
    
    # slugの検証
    if not validate_slug_format(slug):
        raise ValueError(f"Invalid slug format: {slug}")
    
    # IDの生成
    record_id = _generate_id(make, model, slug)
    
    # 価格の変換
    price_min_jpy = None
    price_max_jpy = None
    if row.get("price_min_gbp"):
        price_min_jpy = int(row["price_min_gbp"] * GBP_TO_JPY)
    if row.get("price_max_gbp"):
        price_max_jpy = int(row["price_max_gbp"] * GBP_TO_JPY)
    
    # media_urlsの処理
    media_urls = row.get("media_urls", [])
    if isinstance(media_urls, list):
        media_urls_str = json.dumps(media_urls)
    else:
        media_urls_str = str(media_urls) if media_urls else "[]"
    
    return {
        "id":            record_id,
        "slug":          slug,
        "make_en":       make,
        "model_en":      model,
        "make_ja":       _deepl(make),
        "model_ja":      _deepl(model),
        "overview_en":   overview_en,
        "overview_ja":   _deepl(overview_en),
        "body_type":     row.get("body_type"),
        "fuel":          row.get("fuel"),
        "price_min_gbp": row.get("price_min_gbp"),
        "price_max_gbp": row.get("price_max_gbp"),
        "price_min_jpy": price_min_jpy,
        "price_max_jpy": price_max_jpy,
        "spec_json":     row.get("spec_json", "{}"),
        "media_urls":    media_urls_str,
        "updated_at":    dt.datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }

def get_translation_stats():
    """翻訳統計を取得（デバッグ用）"""
    return {
        "cached_translations": len(_translation_cache),
        "cache_keys": list(_translation_cache.keys())[:10]  # 最初の10件のみ表示
    }

# スクリプト単体起動用
if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        for line in open(sys.argv[1], encoding="utf-8"):
            result = to_payload(json.loads(line))
            print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        # テスト用
        test_data = {
            "make_en": "BMW",
            "model_en": "i3",
            "overview_en": "The BMW i3 is an electric vehicle with innovative design.",
            "slug": "bmw-i3"
        }
        result = to_payload(test_data)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        print("\nTranslation stats:", get_translation_stats())
