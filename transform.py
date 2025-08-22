# transform.py
# rev: 2025-08-24 表記ゆれ対策版
"""
raw.jsonl → 変換済み dict を返すユーティリティ
表記ゆれ対策のためのローカルグロッサリー機能付き
"""
import os, json, datetime as dt, backoff, requests, slugify
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
    "Morgan": "モーガン",
    "TVR": "TVR",
    "Caterham": "ケータハム",
    "Westfield": "ウエストフィールド",
    "Austin": "オースチン",
    "Marcos": "マーコス",
    "Triumph": "トライアンフ",
    "Morris": "モーリス",
    "Vanden Plas": "バンデンプラ",
    "Austin Healey": "オースチン ヒーレー",
    "British Leyland": "ブリティッシュ レイランド",
    "Wolseley": "ウーズレイ",
    "Mini Moke": "ミニモーク",
    "Riley": "ライレー",
    "Panther Westwinds": "パンサー ウェストウインズ",
    
    # イタリア系メーカー
    "Abarth": "アバルト",
    "Alfa Romeo": "アルファロメオ",
    "Fiat": "フィアット",
    "Lamborghini": "ランボルギーニ",
    "Maserati": "マセラティ",
    "Lancia": "ランチア",
    "Ferrari": "フェラーリ",
    "De Tomaso": "デトマソ",
    "Autobianchi": "アウトビアンキ",
    "Innocenti": "イノチェンティ",
    "Pagani": "パガーニ",
    "Diatto": "ディアット",
    "Osca": "オスカ",
    "Bertone": "ベルトーネ",
    "Bizzarrini": "ビッザリーニ",
    "Iso": "イソ",
    
    # その他ヨーロッパ系
    "KTM X-Bow": "KTM クロスボウ",
    "Donkervoort": "ドンカーブート",
    "Polestar": "ポールスター",
    "Volvo": "ボルボ",
    "Saab": "サーブ",
    "Koenigsegg": "ケーニグセグ",
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
    "Opel": "オペル",
    "BMW Alpina": "BMWアルピナ",
    "Daimler": "デイムラー",
    "Brabus": "ブラバス",
    "Roof": "ルーフ",
    "Burstner": "バーストナー",
    "YES!": "イエス",
    "Artega": "アルデガ",
    "Alpine": "アルピーヌ",
    "Citroen": "シトロエン",
    "DS": "DSオートモビル",
    "Peugeot": "プジョー",
    "Renault": "ルノー",
    "Venturi": "ヴェンチュリー",
    "Bugatti": "ブガッティ",
    "Dacia": "ダチア",
    "Lada": "ラーダ",
    "UAZ": "ワズ",
    "Ligier": "リジェ",
    "Tauro Sport Auto": "タウロスポーツオート",
    "AMG": "AMG",
    "Borgward": "ボルクヴァルト",
    "Irmscher": "イルムシャー",
    "Maybach": "マイバッハ",
    "Aixam": "エクサム",
    "Microcar": "マイクロカー",
    "Scania": "スカニア",
    "Irizar": "イリサール",
    "Spyker": "スパイカーカーズ",
    "Spykercars": "スパイカーカーズ",
    "Tatra": "タトラ",
    "Iveco": "イベコ",
    "Maxus": "マクサス",
    "Daimler Truck": "ダイムラー トラック",
    
    # 韓国・アメリカ・その他
    "Hyundai": "ヒュンダイ",
    "Jeep": "ジープ",
    "Kia": "キア",
    "SsangYong": "サンヨン",
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
    "Isuzu": "いすゞ",
    
    # 車種・技術用語
    "Electric": "電気",
    "Hybrid": "ハイブリッド", 
    "Mild Hybrid": "マイルドハイブリッド",
    "Plugin Hybrid": "プラグインハイブリッド",
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
    
    # 仕様・技術用語（Carwowから抽出）
    "Automatic": "オートマチック",
    "Manual": "マニュアル",
    "CVT": "CVT",
    "DSG": "DSG",
    "Tiptronic": "ティプトロニック",
    "Multitronic": "マルチトロニック",
    "PowerShift": "パワーシフト",
    
    # 駆動方式
    "Front-wheel drive": "前輪駆動",
    "Rear-wheel drive": "後輪駆動", 
    "All-wheel drive": "全輪駆動",
    "Four-wheel drive": "四輪駆動",
    "AWD": "AWD",
    "4WD": "4WD",
    "FWD": "前輪駆動",
    "RWD": "後輪駆動",
    
    # 電気自動車関連
    "Battery capacity": "バッテリー容量",
    "kWh": "kWh",
    "Range": "航続距離",
    "Charging time": "充電時間",
    "Fast charging": "急速充電",
    "DC fast charging": "DC急速充電",
    "AC charging": "AC充電",
    "Regenerative braking": "回生ブレーキ",
    "Electric motor": "電気モーター",
    "Power output": "出力",
    "Torque": "トルク",
    
    # 寸法・容量
    "Number of doors": "ドア数",
    "Number of seats": "定員",
    "Boot space": "ブートスペース",
    "Boot capacity": "ブート容量",
    "Seats up": "シート展開時",
    "Seats down": "シート格納時",
    "Turning circle": "回転半径",
    "Wheelbase": "ホイールベース",
    "External dimensions": "外形寸法",
    "Internal dimensions": "内部寸法",
    "Ground clearance": "最低地上高",
    
    # 安全・装備
    "Airbags": "エアバッグ",
    "ABS": "ABS",
    "ESP": "ESP",
    "Traction control": "トラクションコントロール",
    "Stability control": "スタビリティコントロール",
    "Cruise control": "クルーズコントロール",
    "Adaptive cruise control": "アダプティブクルーズコントロール",
    "Lane keeping assist": "レーンキープアシスト",
    "Parking sensors": "パーキングセンサー",
    "Reversing camera": "バックカメラ",
    "Blind spot monitoring": "ブラインドスポットモニタリング",
    "Climate control": "クライメートコントロール",
    "Air conditioning": "エアコンディショニング",
    "Heated seats": "シートヒーター",
    "Keyless entry": "キーレスエントリー",
    "Keyless start": "キーレススタート",
    "Push button start": "プッシュボタンスタート",
    
    # インフォテインメント
    "Infotainment system": "インフォテインメントシステム",
    "Touchscreen": "タッチスクリーン",
    "Navigation system": "ナビゲーションシステム",
    "Satellite navigation": "衛星ナビゲーション",
    "Bluetooth": "ブルートゥース",
    "USB": "USB",
    "Apple CarPlay": "Apple CarPlay",
    "Android Auto": "Android Auto",
    "Digital instrument cluster": "デジタル・インストゥルメントクラスター",
    
    # 性能関連
    "Horsepower": "馬力",
    "HP": "馬力",
    "BHP": "制動馬力",
    "PS": "PS",
    "kW": "kW",
    "Nm": "Nm",
    "lb-ft": "lb-ft",
    "Top speed": "最高速度",
    "0-62mph": "0-100km/h加速",
    "Acceleration": "加速",
    "Fuel economy": "燃費",
    "MPG": "マイル/ガロン",
    "L/100km": "L/100km",
    "CO2 emissions": "CO2排出量",
    "WLTP": "WLTP",
    "NEDC": "NEDC",
    
    # ホイール・タイヤ
    "Alloy wheels": "アロイホイール",
    "Steel wheels": "スチールホイール",
    "Tyre size": "タイヤサイズ",
    "Run-flat tyres": "ランフラットタイヤ",
    "Spare wheel": "スペアホイール",
    
    # 特別仕様・トリム名
    "Standard": "標準",
    "Base": "ベース",
    "Sport": "スポーツ",
    "Luxury": "ラグジュアリー",
    "Premium": "プレミアム",
    "S-Line": "Sライン",
    "M-Sport": "Mスポーツ",
    "AMG": "AMG",
    "RS": "RS",
    "GTI": "GTI",
    "R-Line": "R-Line",
    "Scorpionissima": "スコルピオニッシマ",
    "Limited Edition": "限定版",
    
    # モデル名（一部）
    "500e": "500e",
    "600e": "600e",
    "i3": "i3",
    "i4": "i4",
    "iX": "iX",
    "EQS": "EQS",
    "EQE": "EQE",
    "Model S": "モデルS",
    "Model 3": "モデル3",
    "Model X": "モデルX",
    "Model Y": "モデルY",
    "e-tron": "e-tron",
    "ID.3": "ID.3",
    "ID.4": "ID.4",
    "EX30": "EX30",
    "EX40": "EX40",
    "EX90": "EX90",
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

def _preprocess_text(text: str) -> str:
    """翻訳前の前処理（固有名詞の保護など）"""
    if not text:
        return text
    
    # ハイフンで繋がった車種名を保護
    text = re.sub(r'\b([A-Z][a-z]+)-([A-Z][a-z]+)\b', r'\1 \2', text)
    
    return text

@backoff.on_exception(backoff.expo, requests.RequestException,
                      max_tries=3, jitter=None)
def _deepl(text: str, src="EN", tgt="JA") -> str:
    if not (text and text.strip()):
        return ""
    
    # キャッシュチェック
    cache_key = f"{src}:{tgt}:{text}"
    if cache_key in _translation_cache:
        return _translation_cache[cache_key]
    
    # グロッサリー適用（翻訳前）
    processed_text = _preprocess_text(text)
    
    # DeepL翻訳
    translated = ""
    if DEEPL_KEY:
        try:
            r = requests.post(
                "https://api-free.deepl.com/v2/translate",
                data={
                    "auth_key": DEEPL_KEY,
                    "text": processed_text, 
                    "source_lang": src, 
                    "target_lang": tgt,
                    "formality": "default"  # 敬語レベル統一
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
    
    # ハイフンをスペースに
    text = text.replace('-', ' ')
    # タイトルケースに
    text = text.title()
    # 連続スペースを単一スペースに
    text = re.sub(r'\s+', ' ', text)
    
    return text.strip()

def to_payload(row: dict) -> dict:
    make   = _normalize_make_model(row.get("make_en", ""))
    model  = _normalize_make_model(row.get("model_en", ""))
    slug   = row.get("slug", "").strip()
    overview_en = row.get("overview_en", "").strip()
    
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
            "make_en": "bmw",
            "model_en": "i3",
            "overview_en": "The BMW i3 is an electric vehicle with innovative design.",
            "slug": "bmw-i3"
        }
        result = to_payload(test_data)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        print("\nTranslation stats:", get_translation_stats())
