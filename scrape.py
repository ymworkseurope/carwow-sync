# rev: 2025-08-23 T23:55Z
import os, re, json, sys, time, random, requests, bs4, backoff
from urllib.parse import urlparse
from typing import List, Dict
from tqdm import tqdm
from model_scraper import scrape as scrape_one
from transform      import to_payload                 # ← 名称一致
try:
    from gsheets_helper import upsert as gsheets_upsert
except ImportError:
    gsheets_upsert = None

UA   = "Mozilla/5.0 (+https://github.com/ymworkseurope/carwow-sync 2025-08-23)"
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
    # レート制限を少し厳しくする
    time.sleep(random.uniform(1.5, 3.0))

# ───────── sitemap → /make/model URL 一覧 ──────────
def iter_model_urls() -> List[str]:
    # Carwowのsitemapを取得
    sitemap_url = "https://www.carwow.co.uk/sitemap.xml"
    xml = _get(sitemap_url).text
    
    # 車両カタログURLを抽出（/make/model 形式）
    locs = re.findall(r"<loc[](https://www\.carwow\.co\.uk/[^<]+)</loc>", xml)
    model_urls = []
    
    for loc in locs:
        path = urlparse(loc).path.strip('/')
        # /make/model 形式かつ不要なパスを除外
        if (len(path.split('/')) == 2 and
            all(part for part in path.split('/')) and
            not re.search(r"(blog|used|news|best|review)", path, re.IGNORECASE)):
            model_urls.append(loc)
    
    return sorted(set(model_urls))  # 重複を除去してソート

# ───────── Supabase UPSERT（改良版） ──────────
def db_upsert(item: Dict):
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
            f"{SUPABASE_URL}/rest/v1/cars",  # on_conflictパラメータを削除
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
            
            # 400エラーの場合、レスポンス内容を詳しく見る
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
        print(f"Response: {e.response.text if hasattr(e, 'response') else 'No response'}")
        raise
    except Exception as e:
        print(f"Unexpected error for {item.get('slug', 'unknown')}: {e}")
        raise

# ───────── バックアップ用のローカル保存 ──────────
def save_to_backup(payload: Dict):
    """バックアップ用にローカルファイルに保存"""
    backup_file = "backup_data.jsonl"
    with open(backup_file, "a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")

# ───────── データ検証 ──────────
def validate_payload(payload: Dict) -> bool:
    """ペイロードの基本検証"""
    required_fields = ["slug"]
    
    for field in required_fields:
        if not payload.get(field):
            print(f"WARNING: 必須フィールド '{field}' がありません")
            return False
    
    # slugの形式チェック
    slug = payload.get("slug", "")
    if not re.match(r"^[a-z0-9-]+/[a-z0-9-]+$", slug):
        print(f"WARNING: 無効なslug形式: {slug}")
        return False
    
    return True

# ───────── main（改良版） ──────────
if __name__ == "__main__":
    urls = iter_model_urls()
    print("Total target models:", len(urls))
    print("sample 5:", ["/".join(urlparse(u).path.strip("/").split("/")[-2:])
                        for u in urls[:5]])
    
    # デバッグモード（最初の10件のみ）
    DEBUG_MODE = True
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
