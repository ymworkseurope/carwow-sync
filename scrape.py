# scrape.py
# rev: 2025-08-24 デバッグ版（詳細エラー表示+修正）
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

def is_valid_car_catalog_url(url: str) -> bool:
    """車両カタログURLかどうかを判定"""
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
    
    # 除外キーワードチェック
    full_path = f"{manufacturer}/{model}".lower()
    for keyword in EXCLUDE_KEYWORDS:
        if keyword in full_path:
            return False
    
    # モデル名の基本バリデーション
    if not re.match(r'^[a-zA-Z0-9\-\+]+$', model):
        return False
    
    return True

def iter_model_urls() -> List[str]:
    """サイトマップから車両カタログURLのみを抽出"""
    try:
        # サイトマップインデックス取得
        xml = _get("https://www.carwow.co.uk/sitemap.xml").text
        sitemap_urls = re.findall(r"<loc>(https://[^<]+\.xml)</loc>", xml)
        
        all_urls = set()
        
        # 各サイトマップから車両URL抽出
        for sitemap_url in sitemap_urls:
            try:
                sub_xml = _get(sitemap_url).text
                urls = re.findall(r"<loc>(https://www\.carwow\.co\.uk/[^<]+)</loc>", sub_xml)
                
                # 車両カタログURLのみフィルタリング
                for url in urls:
                    if is_valid_car_catalog_url(url):
                        all_urls.add(url)
                        
            except Exception as e:
                print(f"サイトマップ {sitemap_url} の処理に失敗: {e}")
                continue
        
        result = sorted(list(all_urls))
        print(f"有効な車両カタログURL: {len(result)}件")
        
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
    
    media_urls = payload.get("media_urls", "[]")
    if media_urls:
        try:
            parsed = json.loads(media_urls)
            if not isinstance(parsed, list):
                errors.append(f"media_urls must be a JSON array, got: {type(parsed)}")
        except json.JSONDecodeError as e:
            errors.append(f"Invalid JSON in media_urls: {e}")
    
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
    
    # ID形式の検証
    if payload.get("id"):
        if not re.match(r'^[a-f0-9]{12}$', payload["id"]):
            errors.append(f"Invalid ID format: {payload['id']}")
    
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
