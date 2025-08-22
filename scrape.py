# scrape.py
# rev: 2025-08-24 色データURL除外強化版
import os, re, json, sys, time, random, requests, bs4, backoff
from urllib.parse import urlparse
from typing import List, Dict
from tqdm import tqdm
from model_scraper import scrape as scrape_one
from transform      import to_payload
try:
    from gsheets_helper import upsert as gsheets_upsert
except ImportError:
    gsheets_upsert = None

UA   = "Mozilla/5.0 (+https://github.com/ymworkseurope/carwow-sync 2025-08-24)"
HEAD = {"User-Agent": UA}
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

@backoff.on_exception(backoff.expo, requests.RequestException,
                      max_tries=5, jitter=None)
def _get(url: str) -> requests.Response:
    r = requests.get(url, headers=HEAD, timeout=30)
    r.raise_for_status()
    return r

def _sleep(): 
    time.sleep(random.uniform(1.5, 3.0))

# 動的メーカー検出のための既知メーカーリスト
KNOWN_MANUFACTURERS = {
    'abarth', 'alfa-romeo', 'alpine', 'audi', 'bmw', 'byd', 'citroen', 
    'cupra', 'ds', 'dacia', 'fiat', 'ford', 'gwm', 'genesis', 'honda', 
    'hyundai', 'ineos', 'jaecoo', 'jeep', 'kgm-motors', 'kia', 
    'land-rover', 'leapmotor', 'lexus', 'lotus', 'mg', 'mini', 'mazda', 
    'mercedes', 'mercedes-benz', 'nissan', 'omoda', 'peugeot', 'polestar', 
    'renault', 'seat', 'skoda', 'skywell', 'smart', 'subaru', 'suzuki', 
    'tesla', 'toyota', 'vauxhall', 'volkswagen', 'volvo', 'xpeng'
}

# 除外すべきキーワード（より包括的）
EXCLUDE_KEYWORDS = {
    'used', 'lease', 'deals', 'cheap', 'economical', 'electric-cars', 
    'hybrid-cars', '4x4-cars', '7-seater-cars', 'automatic-cars', 'convertible-cars',
    'estate-cars', 'hatchback-cars', 'sports-cars', 'suvs', 'small-cars',
    'family-cars', 'first-cars', 'luxury-cars', 'mpvs', 'by-range',
    'efficient', 'fast', 'safe', 'towing', 'big-boot', 'students',
    'teenagers', 'nil-deposit', 'motability', 'wav', 'saloon-cars',
    'supermini', 'coupe', 'petrol', 'diesel', 'manual-cars', 'company-cars',
    'learner', 'gt-cars', 'hot-hatches', 'medium-sized', 'reliable',
    'sporty', 'ulez-compliant', 'chinese-cars', 'crossover'
}

# 車両色の包括的リスト（色データURL除外用）
CAR_COLORS = {
    'white', 'black', 'silver', 'grey', 'gray', 'red', 'blue', 'green', 
    'yellow', 'orange', 'brown', 'purple', 'pink', 'gold', 'bronze',
    'beige', 'cream', 'ivory', 'pearl', 'metallic', 'matt', 'matte',
    'gloss', 'satin', 'dark', 'light', 'bright', 'deep', 'pale',
    'midnight', 'arctic', 'polar', 'crystal', 'diamond', 'platinum',
    'champagne', 'copper', 'steel', 'anthracite', 'charcoal', 'slate',
    'navy', 'royal', 'lime', 'forest', 'olive', 'burgundy',
    'maroon', 'crimson', 'scarlet', 'azure', 'cyan', 'turquoise',
    'emerald', 'jade', 'amber', 'rust', 'copper', 'bronze', 'titanium',
    'magma', 'volcano', 'storm', 'thunder', 'lightning', 'glacier',
    'alpine', 'cosmic', 'galaxy', 'stellar', 'lunar', 'solar',
    'phantom', 'ghost', 'shadow', 'mystic', 'magic', 'elegant',
    'prestige', 'premium', 'luxury', 'exclusive', 'special', 'limited',
    # 車種特有の色名
    'tango', 'flame', 'sunset', 'sunrise', 'twilight', 'dawn',
    'moondust', 'stardust', 'cosmos', 'nova', 'aurora', 'spectrum'
}

# モデル名として有効な英数字パターン（色を除外）
VALID_MODEL_PATTERNS = {
    r'^\d+[a-z]*$',  # 数字＋英字（例：500e, 3008, i4）
    r'^[a-z]+\d+',   # 英字＋数字（例：a3, x5, q7）
    r'^[a-z]+-[a-z]+(?:-[a-z0-9]+)*$',  # ハイフン区切り（例：grand-cherokee）
    r'^[a-z]+(?:-[a-z0-9]+)*$',  # 一般的なモデル名（例：corolla, prius）
}

def is_color_based_url(manufacturer: str, model: str) -> bool:
    """URLが色データかどうかを判定（強化版）"""
    model_lower = model.lower().replace('-', ' ')
    
    # 1. モデル名が色名のみで構成されているかチェック
    model_words = model_lower.split()
    if all(word in CAR_COLORS for word in model_words if word):
        print(f"DETECTED COLOR URL: {manufacturer}/{model} (色名のみで構成)")
        return True
    
    # 2. 色名が50%以上を占める場合
    color_word_count = sum(1 for word in model_words if word in CAR_COLORS)
    if len(model_words) > 0 and (color_word_count / len(model_words)) >= 0.5:
        print(f"DETECTED COLOR URL: {manufacturer}/{model} (色名が50%以上)")
        return True
    
    # 3. 特定の色パターンをチェック
    color_patterns = [
        r'^(alpine|arctic|polar|crystal|diamond)-?(white|silver)$',
        r'^(jet|midnight|deep|dark)-?(black|blue|grey)$',
        r'^(metallic|pearl|matt|matte|gloss|satin)-.+$',
        r'^.+-(white|black|silver|grey|red|blue|green)$',
        r'^(bright|light|dark|deep|pale)-.+$',
    ]
    
    for pattern in color_patterns:
        if re.match(pattern, model_lower):
            print(f"DETECTED COLOR URL: {manufacturer}/{model} (パターン: {pattern})")
            return True
    
    return False

def is_valid_model_name(model: str) -> bool:
    """モデル名が車両モデルとして有効かチェック"""
    model_clean = model.lower().replace('-', '').replace('+', '')
    
    # 1. 有効なパターンとマッチするか
    for pattern in VALID_MODEL_PATTERNS:
        if re.match(pattern, model.lower()):
            return True
    
    # 2. 数字が含まれている（車種コードの可能性）
    if any(c.isdigit() for c in model):
        return True
    
    # 3. 一般的な車種名パターン
    common_model_words = {
        'sportback', 'coupe', 'sedan', 'wagon', 'touring', 'avant',
        'alltrack', 'cross', 'sport', 'line', 'edition', 'plus',
        'comfort', 'luxury', 'premium', 'ultimate', 'executive',
        'dynamic', 'elegance', 'design', 'style', 'trend', 'active'
    }
    
    model_words = set(model.lower().replace('-', ' ').split())
    if model_words & common_model_words:
        return True
    
    return False

def is_valid_car_catalog_url(url: str) -> bool:
    """車両カタログURLかどうかを判定（強化版）"""
    parsed = urlparse(url)
    if parsed.netloc != 'www.carwow.co.uk':
        return False
    
    path_parts = parsed.path.strip('/').split('/')
    
    # パス長チェック：メーカー名/モデル名の2部構成のみ許可
    if len(path_parts) != 2:
        return False
    
    manufacturer, model = path_parts
    
    # 既知のメーカー名チェック
    if manufacturer.lower() not in KNOWN_MANUFACTURERS:
        return False
    
    # 色データURLの除外（最優先チェック）
    if is_color_based_url(manufacturer, model):
        return False
    
    # 除外キーワードチェック
    full_path = f"{manufacturer}/{model}".lower()
    for keyword in EXCLUDE_KEYWORDS:
        if keyword in full_path:
            print(f"EXCLUDED by keyword '{keyword}': {manufacturer}/{model}")
            return False
    
    # モデル名の基本バリデーション
    if not re.match(r'^[a-zA-Z0-9\-\+]+$', model):
        print(f"EXCLUDED by regex pattern: {manufacturer}/{model}")
        return False
    
    # モデル名の有効性チェック
    if not is_valid_model_name(model):
        print(f"EXCLUDED: Invalid model name pattern: {manufacturer}/{model}")
        return False
    
    return True

def iter_model_urls() -> List[str]:
    """サイトマップから車両カタログURLのみを抽出"""
    try:
        # サイトマップインデックス取得
        xml = _get("https://www.carwow.co.uk/sitemap.xml").text
        sitemap_urls = re.findall(r"<loc>(https://[^<]+\.xml)</loc>", xml)
        
        all_urls = set()
        excluded_urls = []
        
        # 各サイトマップから車両URL抽出
        for sitemap_url in sitemap_urls:
            try:
                sub_xml = _get(sitemap_url).text
                urls = re.findall(r"<loc>(https://www\.carwow\.co\.uk/[^<]+)</loc>", sub_xml)
                
                # 車両カタログURLのみフィルタリング
                for url in urls:
                    if is_valid_car_catalog_url(url):
                        all_urls.add(url)
                    else:
                        # 除外されたURLの記録（メーカー/モデル形式のみ）
                        parsed = urlparse(url)
                        path_parts = parsed.path.strip('/').split('/')
                        if len(path_parts) == 2 and path_parts[0].lower() in KNOWN_MANUFACTURERS:
                            excluded_urls.append(f"{path_parts[0]}/{path_parts[1]}")
                        
            except Exception as e:
                print(f"サイトマップ {sitemap_url} の処理に失敗: {e}")
                continue
        
        result = sorted(list(all_urls))
        print(f"有効な車両カタログURL: {len(result)}件")
        
        if excluded_urls:
            print(f"除外されたURL: {len(excluded_urls)}件")
            print("除外例 (最初の10件):")
            for excluded in excluded_urls[:10]:
                print(f"  - {excluded}")
        
        return result
        
    except Exception as e:
        print(f"サイトマップ取得エラー: {e}")
        return []

def validate_supabase_payload(payload: Dict) -> tuple[bool, str]:
    """Supabaseのペイロードを詳細検証"""
    errors = []
    
    # 必須フィールドのチェック
    required_fields = ["id", "slug", "make_en", "model_en"]
    for field in required_fields:
        if not payload.get(field):
            errors.append(f"Missing required field: {field}")
    
    # slugが色データでないことを再確認
    slug = payload.get("slug", "")
    if slug:
        parts = slug.split("-")
        if len(parts) >= 2 and is_color_based_url(parts[0], "-".join(parts[1:])):
            errors.append(f"Slug contains color data: {slug}")
    
    # データ型のチェック
    if payload.get("price_min_gbp") is not None:
        if not isinstance(payload["price_min_gbp"], (int, float)) or payload["price_min_gbp"] < 0:
            errors.append(f"Invalid price_min_gbp: {payload['price_min_gbp']}")
    
    if payload.get("price_max_gbp") is not None:
        if not isinstance(payload["price_max_gbp"], (int, float)) or payload["price_max_gbp"] < 0:
            errors.append(f"Invalid price_max_gbp: {payload['price_max_gbp']}")
    
    if payload.get("price_min_jpy") is not None:
        if not isinstance(payload["price_min_jpy"], (int, float)) or payload["price_min_jpy"] < 0:
            errors.append(f"Invalid price_min_jpy: {payload['price_min_jpy']}")
    
    if payload.get("price_max_jpy") is not None:
        if not isinstance(payload["price_max_jpy"], (int, float)) or payload["price_max_jpy"] < 0:
            errors.append(f"Invalid price_max_jpy: {payload['price_max_jpy']}")
    
    # JSON文字列の検証
    spec_json = payload.get("spec_json", "{}")
    if spec_json:
        try:
            json.loads(spec_json)
        except json.JSONDecodeError as e:
            errors.append(f"Invalid JSON in spec_json: {e}")
    
    # media_urlsの検証（配列型として）
    media_urls = payload.get("media_urls", [])
    if media_urls is not None:
        if not isinstance(media_urls, list):
            errors.append(f"media_urls must be an array, got: {type(media_urls)}")
        else:
            # 各URLが文字列かチェック
            for i, url in enumerate(media_urls):
                if not isinstance(url, str):
                    errors.append(f"media_urls[{i}] must be a string, got: {type(url)}")
    
    # 文字列長の検証
    string_fields = {
        "slug": 100,
        "make_en": 50, 
        "model_en": 50,
        "make_ja": 50,
        "model_ja": 50,
        "body_type": 50,
        "fuel": 50,
        "overview_en": 2000,
        "overview_ja": 2000
    }
    
    for field, max_len in string_fields.items():
        value = payload.get(field)
        if value and len(str(value)) > max_len:
            errors.append(f"{field} too long ({len(str(value))} > {max_len}): {str(value)[:50]}...")
    
    # ID形式の検証（UUID形式）
    if payload.get("id"):
        if not re.match(r'^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$', payload["id"]):
            errors.append(f"Invalid UUID format: {payload['id']}")
    
    return len(errors) == 0, "; ".join(errors)

def db_upsert(item: Dict):
    """Supabase UPSERT（詳細デバッグ版）"""
    if not (SUPABASE_URL and SUPABASE_KEY):
        print("SKIP Supabase:", item.get("slug", "unknown"))
        return
    
    try:
        # 詳細なデータ検証
        is_valid, error_msg = validate_supabase_payload(item)
        if not is_valid:
            print(f"VALIDATION ERROR for {item.get('slug', 'unknown')}: {error_msg}")
            print(f"Payload: {json.dumps(item, indent=2, default=str)}")
            return
        
        # Supabaseへのリクエスト
        print(f"Sending to Supabase: {item['slug']}")
        
        r = requests.post(
            f"{SUPABASE_URL}/rest/v1/cars",
            headers={
                "apikey": SUPABASE_KEY,
                "Authorization": f"Bearer {SUPABASE_KEY}",
                "Prefer": "resolution=merge-duplicates",
                "Content-Type": "application/json",
            },
            json=item, 
            timeout=30
        )
        
        # 詳細なエラー情報の表示
        if not r.ok:
            print(f"\n=== SUPABASE ERROR [{r.status_code}] for {item['slug']} ===")
            print(f"Request URL: {r.url}")
            print(f"Request headers: {dict(r.request.headers)}")
            print(f"Response headers: {dict(r.headers)}")
            print(f"Response body: {r.text}")
            
            # ペイロードの詳細表示
            print(f"\nSent payload:")
            print(json.dumps(item, indent=2, ensure_ascii=False, default=str))
            
            try:
                error_json = r.json()
                print(f"\nParsed error: {json.dumps(error_json, indent=2)}")
            except:
                pass
            
            r.raise_for_status()
        
        print("SUPABASE OK", item["slug"])
        
    except requests.exceptions.HTTPError as e:
        print(f"\nHTTP Error for {item.get('slug', 'unknown')}: {e}")
        raise
    except Exception as e:
        print(f"\nUnexpected error for {item.get('slug', 'unknown')}: {e}")
        import traceback
        traceback.print_exc()
        raise

def save_to_backup(payload: Dict):
    """バックアップ用にローカルファイルに保存"""
    backup_file = "backup_data.jsonl"
    with open(backup_file, "a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False, default=str) + "\n")

def validate_payload(payload: Dict) -> bool:
    """ペイロードの基本検証"""
    required_fields = ["slug", "make_en", "model_en"]
    
    for field in required_fields:
        if not payload.get(field):
            print(f"WARNING: 必須フィールド '{field}' がありません")
            return False
    
    # slugの形式チェック
    slug = payload.get("slug", "")
    if not re.match(r'^[a-z0-9\-]+-[a-z0-9\-\+]+$', slug):
        print(f"WARNING: 無効なslug形式: {slug}")
        return False
    
    # slugが色データでないことを最終確認
    parts = slug.split("-")
    if len(parts) >= 2 and is_color_based_url(parts[0], "-".join(parts[1:])):
        print(f"WARNING: Color data detected in slug: {slug}")
        return False
    
    return True

if __name__ == "__main__":
    urls = iter_model_urls()
    print("Total target models:", len(urls))
    
    if urls:
        print("sample 5:", ["/".join(urlparse(u).path.strip("/").split("/"))
                            for u in urls[:5]])
    
    # デバッグモード（最初の3件のみでテスト）
    DEBUG_MODE = os.getenv("DEBUG_MODE", "false").lower() == "true"
    if DEBUG_MODE:
        urls = urls[:3]
        print(f"DEBUG MODE: 最初の{len(urls)}件のみ処理")
    
    raw_fp = open("raw.jsonl", "w", encoding="utf-8")
    ok = err = 0
    
    for i, url in enumerate(tqdm(urls, desc="scrape")):
        _sleep()
        try:
            print(f"\n--- Processing {i+1}/{len(urls)}: {url} ---")
            
            # 1. スクレイピング
            raw = scrape_one(url)
            print(f"Scraped data keys: {list(raw.keys())}")
            
            # 2. データ変換
            payload = to_payload(raw)
            print(f"Transformed payload keys: {list(payload.keys())}")
            
            # 3. データ検証
            if not validate_payload(payload):
                print(f"SKIP: データ検証失敗 - {url}")
                err += 1
                continue
            
            # 4. バックアップ保存
            save_to_backup(payload)
            
            # 5. データベース更新
            db_upsert(payload)
            
            # 6. Google Sheets更新
            if gsheets_upsert:
                try:
                    gsheets_upsert(payload)
                except Exception as e:
                    print(f"Google Sheets エラー: {e}")
            
            # 7. RAWデータ保存
            raw_fp.write(json.dumps(raw, ensure_ascii=False, default=str) + "\n")
            ok += 1
            
            print(f"SUCCESS: {payload['slug']}")
            
        except Exception as e:
            print(f"[ERR] {url} {repr(e)}", file=sys.stderr)
            
            # より詳細なエラー情報
            import traceback
            traceback.print_exc()
            
            err += 1
            
            # 早期エラー停止（デバッグ用）
            if DEBUG_MODE and err >= 1:
                print("デバッグモード: エラーが発生したため停止します。")
                break
            
            # エラーが多い場合は停止
            if err > 10 and ok == 0:
                print("多数のエラーが発生しました。処理を停止します。")
                break
    
    raw_fp.close()
    print(f"\nFinished: {ok} upserted / {err} skipped → raw.jsonl")
    print(f"Backup saved to: backup_data.jsonl")
