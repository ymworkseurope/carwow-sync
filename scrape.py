# scrape.py
# rev: 2025-08-24 修正版（動的車両カタログ検出対応）
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

def db_upsert(item: Dict):
    """Supabase UPSERT（改良版）"""
    if not (SUPABASE_URL and SUPABASE_KEY):
        print("SKIP Supabase:", item.get("slug", "unknown"))
        return
    
    try:
        # データ検証
        if not item.get("slug"):
            print(f"WARNING: slugがありません - {item}")
            return
            
        # リクエスト送信
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
        
        # エラー詳細の取得
        if not r.ok:
            error_detail = r.text
            print(f"SUPABASE ERROR [{r.status_code}] {item['slug']}: {error_detail}")
            
            if r.status_code == 400:
                try:
                    error_json = r.json()
                    print(f"Error details: {json.dumps(error_json, indent=2)}")
                except:
                    print(f"Raw error response: {r.text}")
            
            r.raise_for_status()
        
        print("SUPABASE OK", item["slug"])
        
    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error for {item.get('slug', 'unknown')}: {e}")
        if hasattr(e, 'response'):
            print(f"Response: {e.response.text}")
        raise
    except Exception as e:
        print(f"Unexpected error for {item.get('slug', 'unknown')}: {e}")
        raise

def save_to_backup(payload: Dict):
    """バックアップ用にローカルファイルに保存"""
    backup_file = "backup_data.jsonl"
    with open(backup_file, "a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")

def validate_payload(payload: Dict) -> bool:
    """ペイロードの基本検証（修正版）"""
    required_fields = ["slug", "make_en", "model_en"]
    
    for field in required_fields:
        if not payload.get(field):
            print(f"WARNING: 必須フィールド '{field}' がありません")
            return False
    
    # slugの形式チェック（修正）
    slug = payload.get("slug", "")
    # メーカー名-モデル名の形式であることをチェック
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
    
    # デバッグモード（最初の10件のみ）
    DEBUG_MODE = os.getenv("DEBUG_MODE", "false").lower() == "true"
    if DEBUG_MODE:
        urls = urls[:10]
        print(f"DEBUG MODE: 最初の{len(urls)}件のみ処理")
    
    raw_fp = open("raw.jsonl", "w", encoding="utf-8")
    ok = err = 0
    
    for i, url in enumerate(tqdm(urls, desc="scrape")):
        _sleep()
        try:
            # 1. スクレイピング
            raw = scrape_one(url)
            
            # 2. データ変換
            payload = to_payload(raw)
            
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
            raw_fp.write(json.dumps(raw, ensure_ascii=False) + "\n")
            ok += 1
            
            # 進捗表示
            if (i + 1) % 50 == 0:
                print(f"\n進捗: {i + 1}/{len(urls)} ({ok} success, {err} errors)")
            
        except Exception as e:
            print(f"[ERR] {url} {repr(e)}", file=sys.stderr)
            
            # より詳細なエラー情報
            import traceback
            traceback.print_exc()
            
            err += 1
            
            # エラーが多い場合は停止
            if err > 10 and ok == 0:
                print("多数のエラーが発生しました。処理を停止します。")
                break
    
    raw_fp.close()
    print(f"\nFinished: {ok} upserted / {err} skipped → raw.jsonl")
    print(f"Backup saved to: backup_data.jsonl")
