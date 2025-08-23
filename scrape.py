#!/usr/bin/env python3
# scrape.py – rev: 2025-08-30 full (spec_json dict 対応版)

import os, re, json, sys, time, random, requests, bs4, backoff, traceback
from urllib.parse import urlparse
from typing import List, Dict, Any, Iterator, Tuple
from tqdm import tqdm
from model_scraper import scrape as scrape_one
from transform      import to_payload
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

EXCLUDE_KEYWORDS = {
    'used','lease','deals','cheap','economical','electric-cars','hybrid-cars',
    '4x4-cars','7-seater-cars','automatic-cars','convertible-cars','estate-cars',
    'hatchback-cars','sports-cars','suvs','small-cars','family-cars','first-cars',
    'luxury-cars','mpvs','by-range','efficient','fast','safe','towing','big-boot',
    'students','teenagers','nil-deposit','motability','wav','saloon-cars',
    'supermini','coupe','petrol','diesel','manual-cars','company-cars','learner',
    'gt-cars','hot-hatches','medium-sized','reliable','sporty','ulez-compliant',
    'chinese-cars','crossover'
}

CAR_COLORS = {
    'white','black','silver','grey','gray','red','blue','green','yellow','orange',
    'brown','purple','pink','gold','bronze','beige','cream','ivory','pearl',
    'metallic','matt','matte','gloss','satin','dark','light','bright','deep',
    'pale','midnight','arctic','polar','crystal','diamond','platinum','champagne',
    'copper','steel','anthracite','charcoal','slate','navy','royal','lime',
    'forest','olive','burgundy','maroon','crimson','scarlet','azure','cyan',
    'turquoise','emerald','jade','amber','rust','titanium','magma','volcano',
    'storm','thunder','lightning','glacier','alpine','cosmic','galaxy','stellar',
    'lunar','solar','phantom','ghost','shadow','mystic','magic','elegant',
    'prestige','premium','luxury','exclusive','special','limited','tango','flame',
    'sunset','sunrise','twilight','dawn','moondust','stardust','cosmos','nova',
    'aurora','spectrum'
}

VALID_MODEL_PATTERNS = {
    r'^\d+[a-z]*$', r'^[a-z]+\d+', r'^[a-z]+-[a-z]+(?:-[a-z0-9]+)*$', r'^[a-z]+(?:-[a-z0-9]+)*$'
}

def is_color_based_url(make:str, model:str) -> bool:
    m = model.lower().replace('-', ' ')
    words = m.split()
    if all(w in CAR_COLORS for w in words if w): return True
    cnt = sum(1 for w in words if w in CAR_COLORS)
    if words and cnt/len(words) >= .5: return True
    for p in (r'^(alpine|arctic|polar|crystal|diamond)-?(white|silver)$',
              r'^(jet|midnight|deep|dark)-?(black|blue|grey)$',
              r'^(metallic|pearl|matt|matte|gloss|satin)-.+$',
              r'^.+-(white|black|silver|grey|red|blue|green)$',
              r'^(bright|light|dark|deep|pale)-.+$'):
        if re.match(p, m): return True
    return False

def is_valid_model_name(model:str) -> bool:
    mc = model.lower().replace('-','').replace('+','')
    if any(re.match(p, model.lower()) for p in VALID_MODEL_PATTERNS): return True
    if any(c.isdigit() for c in model): return True
    common = {'sportback','coupe','sedan','wagon','touring','avant','alltrack',
              'cross','sport','line','edition','plus','comfort','luxury',
              'premium','ultimate','executive','dynamic','elegance','design',
              'style','trend','active'}
    if set(model.lower().replace('-',' ').split()) & common: return True
    return False

def is_valid_car_catalog_url(url:str) -> bool:
    p = urlparse(url)
    if p.netloc != 'www.carwow.co.uk': return False
    parts = p.path.strip('/').split('/')
    if len(parts)!=2: return False
    make,model = parts
    if make not in KNOWN_MANUFACTURERS: return False
    if is_color_based_url(make,model): return False
    if any(k in f"{make}/{model}" for k in EXCLUDE_KEYWORDS): return False
    if not re.match(r'^[a-z0-9\-\+]+$', model): return False
    return is_valid_model_name(model)

def iter_model_urls() -> Iterator[str]:
    try:
        idx_xml = _get("https://www.carwow.co.uk/sitemap.xml").text
        sitemaps = re.findall(r"<loc>(https://[^<]+\.xml)</loc>", idx_xml)
        seen, excluded = set(), []
        for sm in sitemaps:
            try:
                sub = _get(sm).text
                for url in re.findall(r"<loc>(https://www\.carwow\.co\.uk/[^<]+)</loc>", sub):
                    if is_valid_car_catalog_url(url):
                        seen.add(url)
                    else:
                        pp = urlparse(url).path.strip('/').split('/')
                        if len(pp)==2 and pp[0] in KNOWN_MANUFACTURERS:
                            excluded.append("/".join(pp))
            except Exception as e:
                print("sitemap sub error:", e); continue
        print(f"有効な車両カタログURL: {len(seen)}件")
        if excluded:
            print(f"除外されたURL: {len(excluded)}件\n除外例 (最初の10件):")
            for x in excluded[:10]: print("  -", x)
        return iter(sorted(seen))
    except Exception as e:
        print("サイトマップ取得エラー:", e)
        return iter([])

# ─────────── 🆕 validate_supabase_payload() ───────────
def validate_supabase_payload(payload: Dict[str, Any]) -> Tuple[bool, str]:
    """transform で作った dict を最終検証（spec_json dict 対応版）"""
    errs:List[str] = []

    # 必須
    for f in ("id","slug","make_en","model_en"):
        if not payload.get(f):
            errs.append(f"Missing {f}")

    # 数値
    for f in ("price_min_gbp","price_max_gbp","price_min_jpy","price_max_jpy"):
        v = payload.get(f)
        if v is not None and not isinstance(v,(int,float)):
            errs.append(f"{f} not number: {v}")

    # spec_json
    sj = payload.get("spec_json")
    if sj is not None:
        if isinstance(sj, dict): pass
        elif isinstance(sj, str):
            try: json.loads(sj)
            except json.JSONDecodeError as e: errs.append(f"spec_json bad JSON: {e}")
        else:
            errs.append(f"spec_json type {type(sj)} invalid")

    # list 型
    for f in ("media_urls","body_type","body_type_ja","colors"):
        v = payload.get(f)
        if v is not None and not isinstance(v, list):
            errs.append(f"{f} must list, got {type(v)}")

    return (not errs, "; ".join(errs))

# ─────────── Supabase upsert ───────────
def db_upsert(item: Dict[str, Any]):
    if not (SUPABASE_URL and SUPABASE_KEY):
        print("SKIP Supabase:", item.get("slug")); return
    ok, msg = validate_supabase_payload(item)
    if not ok:
        print("VALIDATION ERROR:", msg)
        print(json.dumps(item, indent=2, ensure_ascii=False)); return

    r = requests.post(
        f"{SUPABASE_URL}/rest/v1/cars",
        headers={
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Prefer": "resolution=merge-duplicates",
            "Content-Type": "application/json",
        },
        json=item, timeout=30
    )
    if r.ok:
        print("SUPABASE OK", item["slug"])
    else:
        print(f"SUPABASE ERROR [{r.status_code}] {item['slug']}\n{r.text}")
        r.raise_for_status()

# ─────────── Main loop ───────────
if __name__ == "__main__":
    urls = list(iter_model_urls())
    print("Total target models:", len(urls))

    DEBUG = os.getenv("DEBUG_MODE","false").lower()=="true"
    if DEBUG:
        urls = urls[:10]
        print("DEBUG MODE → first 10 only")

    success = failed = 0
    for url in tqdm(urls, desc="scrape"):
        _sleep()
        try:
            raw      = scrape_one(url)
            payload  = to_payload(raw)
            db_upsert(payload)
            if gsheets_upsert: gsheets_upsert(payload)
            success += 1
        except Exception as e:
            print("[ERR]", url, repr(e))
            traceback.print_exc()
            failed += 1
            if DEBUG and failed>=1: break

    print(f"\nFinished: {success} success / {failed} error")
