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

def _sleep(): time.sleep(random.uniform(0.6, 1.1))

# ───────── sitemap → /make/model URL 一覧 ──────────
def iter_model_urls() -> List[str]:
    xml = _get("https://www.carwow.co.uk/sitemap.xml").text
    locs = re.findall(r"<loc>(https://[^<]+)</loc>", xml)
    models: list[str] = []
    for loc in locs:
        if not loc.endswith(".xml"): continue
        sub = _get(loc).text
        models += re.findall(r"<loc>(https://www\.carwow\.co\.uk/[^<]+)</loc>", sub)

    return sorted({
        u for u in models
        if re.match(r"https://www\.carwow\.co\.uk/[a-z0-9-]+/[a-z0-9-]+/?$", u)
        and not re.search(r"(blog|used|news)", u)
    })

# ───────── Supabase UPSERT ──────────
@backoff.on_exception(backoff.expo, requests.HTTPError,
                      max_time=60, jitter=None)
def db_upsert(item: Dict):
    if not (SUPABASE_URL and SUPABASE_KEY):
        print("SKIP Supabase:", item["slug"])
        return
    r = requests.post(
        f"{SUPABASE_URL}/rest/v1/cars?on_conflict=slug",
        headers={
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Prefer": "resolution=merge-duplicates",
            "Content-Type": "application/json",
        },
        json=item, timeout=30
    )
    r.raise_for_status()
    print("SUPABASE", item["slug"])

# ───────── main ──────────
if __name__ == "__main__":
    urls = iter_model_urls()
    print("Total target models:", len(urls))
    print("sample 5:", ["/".join(urlparse(u).path.strip("/").split("/")[-2:])
                        for u in urls[:5]])

    raw_fp = open("raw.jsonl", "w", encoding="utf-8")
    ok = err = 0

    for url in tqdm(urls, desc="scrape"):
        _sleep()
        try:
            raw      = scrape_one(url)
            payload  = to_payload(raw)

            db_upsert(payload)
            if gsheets_upsert:
                gsheets_upsert(payload)

            raw_fp.write(json.dumps(raw, ensure_ascii=False) + "\n")
            ok += 1
        except Exception as e:
            print("[ERR]", url, repr(e), file=sys.stderr)
            err += 1

    raw_fp.close()
    print(f"\nFinished: {ok} upserted / {err} skipped → raw.jsonl")
