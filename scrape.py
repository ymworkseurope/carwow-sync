# scrape.py  (Carwow 全モデル収集 & Supabase UPSERT)
# rev: 2025-08-22 T22:40Z  ←← ★ここが最新版のタイムスタンプ
# -------------------------------------------------------------
import re, json, time, random, requests, bs4, backoff, sys
from urllib.parse import urlparse, urljoin
from typing import List, Dict
from slugify import slugify
from tqdm import tqdm

GBP_TO_JPY = 195.0              # 必要ならワークフロー側で毎日更新
UA          = ("Mozilla/5.0 (+https://github.com/ymworkseurope/"
               "carwow-sync 2025-08-22)")
HEAD        = {"User-Agent": UA}

# ---------- HTTP helpers ---------- #
@backoff.on_exception(backoff.expo,
                      (requests.RequestException,),
                      max_tries=5, jitter=None)
def _get(url: str, allow_404=False) -> requests.Response:
    r = requests.get(url, headers=HEAD, timeout=30)
    if allow_404 and r.status_code == 404:
        return r
    r.raise_for_status()
    return r

def fetch_bs(url: str, allow_404=False):
    r = _get(url, allow_404)
    if allow_404 and r.status_code == 404:
        return None
    return bs4.BeautifulSoup(r.text, "lxml")

def sleep():
    time.sleep(random.uniform(0.6, 1.1))

# ---------- sitemap crawler ---------- #
def iter_model_urls() -> List[str]:
    """<url><loc>https://www.carwow.co.uk/aaa/bbb</loc></url> を全回収"""
    sm_url = "https://www.carwow.co.uk/sitemap.xml"
    xml = _get(sm_url).text
    locs = re.findall(r"<loc>(https://[^<]+)</loc>", xml)
    # 下層 sitemap を再帰的に読む
    models = []
    for loc in locs:
        if loc.endswith(".xml"):
            sub_xml = _get(loc).text
            models += [u for u in re.findall(r"<loc>(https://[^<]+)</loc>", sub_xml)
                       if re.match(r"https://www\.carwow\.co\.uk/[a-z0-9-]+/[a-z0-9-]+/?$",
                                   u)]
    return sorted(list(set(models)))

# ---------- extraction helpers ---------- #
def extract_make_model(url: str, title: str):
    p = urlparse(url).path.strip("/").split("/")
    make  = p[0].capitalize()
    model = p[1].upper()
    if title:
        t = title.split()
        if len(t) > 1:
            make, model = t[0], t[1]
    return make, model

def extract_price_range(text: str):
    m = re.search(r"£([\d,]+)\s*[–-]\s*£([\d,]+)", text)
    if m:
        return int(m[1].replace(",","")), int(m[2].replace(",",""))
    m = re.search(r"RRP range of £([\d,]+) to £([\d,]+)", text)
    if m:
        return int(m[1].replace(",","")), int(m[2].replace(",",""))
    prices = [int(p.replace(",","")) for p in re.findall(r"£([\d,]+)", text)]
    return (min(prices), max(prices)) if len(prices) >= 2 else (None, None)

def clean_imgs(soup, base):
    urls=[]
    for img in soup.select("img[src]"):
        src = img["src"]
        if any(k in src.lower() for k in ("logo","icon","favicon","badge","sprite")):
            continue
        full = urljoin(base, src)
        if full.startswith("http") and full not in urls:
            urls.append(full)
        if len(urls) >= 12:     # 上限 12 枚
            break
    return urls

# ---------- main page ---------- #
def parse_main(s, url):
    title = s.select_one("h1").get_text(strip=True) if s.select_one("h1") else ""
    make, model = extract_make_model(url, title)
    pmin, pmax  = extract_price_range(s.get_text(" "))
    meta_desc   = s.select_one("meta[name='description']")
    return {
        "slug"          : slugify(f"{make}-{model}"),
        "title"         : title,
        "make_en"       : make,
        "model_en"      : model,
        "price_min_gbp" : pmin,
        "price_max_gbp" : pmax,
        "price_min_jpy" : int(pmin*GBP_TO_JPY) if pmin else None,
        "price_max_jpy" : int(pmax*GBP_TO_JPY) if pmax else None,
        "overview_en"   : meta_desc["content"] if meta_desc else "",
        "media_urls"    : clean_imgs(s, url),
    }

# ---------- specifications page ---------- #
def parse_specs(s):
    if s is None:
        return {}
    rows = {th.get_text(strip=True):td.get_text(strip=True)
            for th,td in zip(s.select("th"), s.select("td"))}
    return {
        "fuel"      : rows.get("Fuel type") or rows.get("Fuel"),
        "body_type" : rows.get("Body style") or rows.get("Body type"),
        "spec_json" : json.dumps(rows, ensure_ascii=False)[:8000]  # サイズ保険
    }

# ---------- colours page ---------- #
def parse_colours(s):
    if s is None:
        return []
    colors=set()
    for img in s.select("img[alt]"):
        alt=img["alt"].strip()
        if re.search(r"(black|white|blue|green|red|silver|grey|gray|yellow|orange|purple)", alt, re.I):
            colors.add(alt.title())
    return sorted(colors)

# ---------- scrape one model ----------- #
def scrape_one(url)->Dict:
    top  = fetch_bs(url)
    spec = fetch_bs(url+"/specifications", allow_404=True)
    col  = fetch_bs(url+"/colours",        allow_404=True)
    data = parse_main(top, url)
    data.update(parse_specs(spec))
    data["colours"] = parse_colours(col)
    return data

# ---------- Supabase (スタブ) ----------- #
def db_upsert(payload:Dict):
    """本番では supabase-py などで置換。ここでは print のみ"""
    print("UPSERT", payload["slug"])

# ---------- CLI / GitHub Actions ---------- #
if __name__ == "__main__":
    urls = iter_model_urls()
    print(f"Total target models: {len(urls)}")
    sample = ", ".join(urls[:10])
    print("sample 10 urls:", [u.split('/')[-2:] for u in urls[:10]])

    up, skip = 0, 0
    for u in tqdm(urls, desc="scrape"):
        sleep()
        try:
            data = scrape_one(u)
            # 価格無しなどはスキップ例
            if not data["price_min_gbp"]:
                skip += 1
                continue
            db_upsert(data)
            up += 1
        except Exception as e:
            print("[ERR]", u, e, file=sys.stderr)
            skip += 1
    print(f"\nFinished: {up} upserted / {skip} skipped")
