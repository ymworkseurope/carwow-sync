#!/usr/bin/env python3
# scrape.py – rev: 2025-08-30 full (dedup + review filter fixed)

import os, re, json, sys, time, random, requests, bs4, backoff, traceback
from urllib.parse import urlparse
from typing import List, Dict, Any, Iterator, Tuple, Set
from tqdm import tqdm
from model_scraper import scrape as scrape_one
from transform import to_payload
try:
    from gsheets_helper import upsert as gsheets_upsert
except ImportError:
    gsheets_upsert = None

# ──────────────── Const ─────────────────
UA   = "Mozilla/5.0 (+https://github.com/ymworkseurope/carwow-sync 2025-08-30)"
HEAD = {"User-Agent": UA}

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# ──────────────── helpers ───────────────
@backoff.on_exception(backoff.expo, requests.RequestException,
                      max_tries=5, jitter=None)
def _get(url: str) -> requests.Response:
    r = requests.get(url, headers=HEAD, timeout=30)
    r.raise_for_status()
    return r

def _bs(url: str) -> bs4.BeautifulSoup:
    return bs4.BeautifulSoup(_get(url).text, "lxml")

def _sleep():
    time.sleep(random.uniform(1.5, 3.0))

# ──────────────── URL フィルタ & 収集 ───
KNOWN_MANUFACTURERS = {
    'abarth','alfa-romeo','alpine','audi','bmw','byd','citroen','cupra','dacia',
    'ds','fiat','ford','gwm','genesis','honda','hyundai','jeep','kia','land-rover',
    'lexus','lotus','mg','mini','mazda','mercedes','mercedes-benz','nissan',
    'peugeot','polestar','renault','seat','skoda','smart','subaru','suzuki',
    'tesla','toyota','vauxhall','volkswagen','volvo','xpeng'
}

# 除外キーワード（強化版）
EXCLUDE_KEYWORDS = {
    # リストページ系
    'used','lease','deals','cheap','economical','automatic','manual',
    'electric-cars','hybrid-cars','4x4-cars','7-seater-cars','automatic-cars',
    'convertible-cars','estate-cars','hatchback-cars','sports-cars','suvs',
    'small-cars','family-cars','first-cars','luxury-cars','mpvs','by-range',
    'efficient','fast','safe','towing','big-boot','students','teenagers',
    'nil-deposit','motability','wav','saloon-cars','supermini','coupe',
    'petrol','diesel','manual-cars','company-cars','learner','gt-cars',
    'hot-hatches','medium-sized','reliable','sporty','ulez-compliant',
    'chinese-cars','crossover',
    # 色系（強化）
    'colours','color','colour','multi-colour','multi-color',
    'white','black','silver','grey','gray','red','blue','green','yellow',
    'orange','brown','purple','pink','gold','bronze','beige','cream',
    # その他の非車種ページ
    'suv','suvs','saloon','hatchback','estate','convertible','coupe',
    'specifications','reviews','prices','finance','insurance'
}

# 車の色関連キーワード
CAR_COLORS = {
    'white','black','silver','grey','gray','red','blue','green','yellow','orange',
    'brown','purple','pink','gold','bronze','beige','cream','ivory','pearl',
    'metallic','matt','matte','gloss','satin','dark','light','bright','deep',
    'pale','midnight','arctic','polar','crystal','diamond','platinum','champagne',
    'copper','steel','anthracite','charcoal','slate','navy','royal','lime',
    'forest','olive','burgundy','maroon','crimson','scarlet','azure','cyan',
    'turquoise','emerald','jade','amber','rust','titanium','magma','volcano'
}

def is_valid_review_url(url: str) -> bool:
    """
    レビューページURLかどうかを厳密に判定
    """
    p = urlparse(url)
    if p.netloc != 'www.carwow.co.uk':
        return False
    
    parts = p.path.strip('/').split('/')
    if len(parts) != 2:
        return False
    
    make, model = parts
    
    # メーカー名チェック
    if make not in KNOWN_MANUFACTURERS:
        return False
    
    # 除外キーワードチェック（model部分）
    model_lower = model.lower()
    for keyword in EXCLUDE_KEYWORDS:
        if keyword in model_lower:
            return False
    
    # 色関連URLを除外
    if model_lower in CAR_COLORS or 'colour' in model_lower or 'color' in model_lower:
        return False
    
    # カテゴリページを除外（suv, saloon等）
    category_words = {'suv', 'suvs', 'saloon', 'hatchback', 'estate', 'convertible', 
                     'coupe', 'mpv', 'mpvs', 'crossover'}
    if model_lower in category_words:
        return False
    
    # モデル名は数字を含むか、ハイフンで区切られた複合語であることが多い
    if not re.match(r'^[a-z0-9][a-z0-9\-\+]*[a-z0-9]$', model_lower):
        return False
    
    # 最低2文字以上
    if len(model) < 2:
        return False
    
    return True

def is_review_page_by_content(url: str) -> bool:
    """
    実際にページを取得してレビューページか確認（キャッシュ付き）
    """
    try:
        doc = _bs(url)
        
        # 1. car-reviews タブがアクティブか
        tab = doc.select_one('a[data-main-menu-section="car-reviews"]')
        if tab and 'is-active' in tab.get('class', []):
            return True
        
        # 2. レビュー固有の要素があるか
        if doc.select_one('.review-overview__at-a-glance-model'):
            return True
        
        # 3. リストページ固有の要素がないか
        if doc.select('.filter-panel') or doc.select('.results-list'):
            return False
        
        return False
    except Exception:
        return False

def iter_model_urls() -> Iterator[str]:
    """
    サイトマップからモデルURLを取得し、重複を除去してレビューページのみを返す
    """
    try:
        print("サイトマップを取得中...")
        idx_xml = _get("https://www.carwow.co.uk/sitemap.xml").text
        sitemaps = re.findall(r"<loc>(https://[^<]+\.xml)</loc>", idx_xml)
        
        seen: Set[str] = set()  # 重複除去用
        excluded = []
        checked_count = 0
        
        print("URLフィルタリング開始...")
        
        for sm in sitemaps:
            try:
                sub = _get(sm).text
                urls = re.findall(r"<loc>(https://www\.carwow\.co\.uk/[^<]+)</loc>", sub)
                
                for url in urls:
                    # URLを正規化（末尾の/やハッシュを除去）
                    normalized_url = url.rstrip('/').split('#')[0]
                    
                    # 既に処理済みならスキップ
                    if normalized_url in seen:
                        continue
                    
                    # URLパターンで1次フィルタ
                    if not is_valid_review_url(normalized_url):
                        excluded.append(normalized_url)
                        continue
                    
                    # コンテンツベースの判定（負荷軽減のため最初の100件のみ）
                    if checked_count < 100:
                        if is_review_page_by_content(normalized_url):
                            seen.add(normalized_url)
                            checked_count += 1
                            print(f"✓ レビューページ確認済み: {normalized_url}")
                        else:
                            excluded.append(normalized_url)
                            print(f"✗ 非レビューページ: {normalized_url}")
                        _sleep()
                    else:
                        # 100件以降はURLパターンのみで判定
                        seen.add(normalized_url)
                    
            except Exception as e:
                print(f"Sitemap error: {sm} - {e}")
                continue
        
        print(f"\n有効なレビューページ: {len(seen)}件")
        print(f"除外されたURL: {len(excluded)}件")
        
        if excluded[:10]:
            print("除外例 (最初の10件):")
            for x in excluded[:10]:
                print(f"  - {x}")
        
        return iter(sorted(seen))
        
    except Exception as e:
        print(f"サイトマップ取得エラー: {e}")
        return iter([])

# ─────────── validate_supabase_payload ───────────
def validate_supabase_payload(payload: Dict[str, Any]) -> Tuple[bool, str]:
    """transform で作った dict を最終検証"""
    errs: List[str] = []

    # 必須フィールド
    for f in ("id", "slug", "make_en", "model_en"):
        if not payload.get(f):
            errs.append(f"Missing {f}")

    # 数値フィールド
    for f in ("price_min_gbp", "price_max_gbp", "price_min_jpy", "price_max_jpy"):
        v = payload.get(f)
        if v is not None and not isinstance(v, (int, float)):
            errs.append(f"{f} not number: {v}")

    # spec_json
    sj = payload.get("spec_json")
    if sj is not None:
        if isinstance(sj, dict):
            pass
        elif isinstance(sj, str):
            try:
                json.loads(sj)
            except json.JSONDecodeError as e:
                errs.append(f"spec_json bad JSON: {e}")
        else:
            errs.append(f"spec_json type {type(sj)} invalid")

    # list型フィールド
    for f in ("media_urls", "body_type", "body_type_ja", "colors"):
        v = payload.get(f)
        if v is not None and not isinstance(v, list):
            errs.append(f"{f} must be list, got {type(v)}")

    return (not errs, "; ".join(errs))

# ─────────── Supabase upsert ───────────
def db_upsert(item: Dict[str, Any]):
    if not (SUPABASE_URL and SUPABASE_KEY):
        print("SKIP Supabase:", item.get("slug"))
        return
    
    ok, msg = validate_supabase_payload(item)
    if not ok:
        print("VALIDATION ERROR:", msg)
        print(json.dumps(item, indent=2, ensure_ascii=False))
        return

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
    
    if r.ok:
        print("SUPABASE OK", item["slug"])
    else:
        print(f"SUPABASE ERROR [{r.status_code}] {item['slug']}\n{r.text}")
        r.raise_for_status()

# ─────────── Main loop ───────────
if __name__ == "__main__":
    urls = list(iter_model_urls())
    print(f"Total target models (deduplicated review pages): {len(urls)}")

    DEBUG = os.getenv("DEBUG_MODE", "false").lower() == "true"
    if DEBUG:
        urls = urls[:10]
        print("DEBUG MODE → first 10 only")

    success = failed = 0
    processed_slugs = set()  # 処理済みslugを記録
    
    for url in tqdm(urls, desc="scrape"):
        _sleep()
        
        # slugを事前にチェック
        slug = "-".join(urlparse(url).path.strip("/").split("/"))
        if slug in processed_slugs:
            print(f"SKIP duplicate slug: {slug}")
            continue
        
        try:
            raw = scrape_one(url)
            payload = to_payload(raw)
            
            # Supabase
            db_upsert(payload)
            
            # Google Sheets（エラーをキャッチ）
            if gsheets_upsert:
                try:
                    gsheets_upsert(payload)
                except Exception as e:
                    print(f"Google Sheets error for {slug}: {e}")
                    # Sheetsエラーでも処理は継続
            
            processed_slugs.add(slug)
            success += 1
            
        except Exception as e:
            print(f"[ERR] {url}: {repr(e)}")
            traceback.print_exc()
            failed += 1
            if DEBUG and failed >= 3:
                break

    print(f"\nFinished: {success} success / {failed} error")
    print(f"Processed slugs: {len(processed_slugs)}")
