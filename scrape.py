# scrape.py ― 完全版 2025-08-21
import time, random, re, requests, bs4, json
from urllib.parse import urljoin, urlparse

UA   = "Mozilla/5.0 (+https://github.com/your/carwow-sync)"
HEAD = {"User-Agent": UA}
GBP_TO_JPY = 195.0
delay = lambda: time.sleep(random.uniform(0.8, 1.3))

def fetch(url, allow_404=False):
    delay()
    r = requests.get(url, headers=HEAD, timeout=30)
    if allow_404 and r.status_code == 404:
        return None
    r.raise_for_status()
    return bs4.BeautifulSoup(r.text, "lxml")

# ---------- ヘルパ ----------
def extract_make_model(url: str, title: str):
    parts = urlparse(url).path.strip('/').split('/')
    make = parts[0].capitalize() if parts else ""
    model = parts[1] if len(parts) > 1 else ""
    if title:
        t = title.split()
        if not make and t:  make  = t[0]
        if not model and len(t) > 1: model = t[1]
    return make, model.upper()

def extract_price_range(s):
    text = s.get_text(" ")
    for patt in [r'£([\d,]+)\s*[-–—]\s*£([\d,]+)',
                 r'RRP range of £([\d,]+) to £([\d,]+)']:
        m = re.search(patt, text)
        if m:
            return int(m.group(1).replace(',','')), int(m.group(2).replace(',',''))
    single = re.search(r'£([\d,]+)', text)
    if single:
        v = int(single.group(1).replace(',',''))
        return v, v
    return None, None

def extract_media_urls(s, base):
    urls = []
    for img in s.select('img[src]'):
        src = img['src']
        if any(x in src.lower() for x in ['logo','icon','favicon']): continue
        full = urljoin(base, src)
        if full not in urls: urls.append(full)
    return urls

# ---------- ① main ----------
def parse_main(s, url):
    title = s.select_one('h1').text.strip() if s.select_one('h1') else ""
    make, model = extract_make_model(url, title)
    pmin, pmax  = extract_price_range(s)
    return {
        "title"         : title,
        "make_en"       : make,
        "model_en"      : model,
        "price_min_gbp" : pmin,
        "price_max_gbp" : pmax,
        "price_min_jpy" : int(pmin*GBP_TO_JPY) if pmin else None,
        "price_max_jpy" : int(pmax*GBP_TO_JPY) if pmax else None,
        "overview_en"   : s.select_one("meta[name='description']")["content"]
                          if s.select_one("meta[name='description']") else "",
        "media_urls"    : extract_media_urls(s, url),
    }

# ---------- ② specs ----------
def parse_specs(s):
    if s is None: return {}
    rows = {dt.text.strip(): dd.text.strip()
            for dt,dd in zip(s.select('dt'), s.select('dd'))}
    door = rows.get('Doors') or rows.get('Number of doors')
    seat = rows.get('Seats') or rows.get('Number of seats')
    fuel = rows.get('Fuel type') or rows.get('Fuel')
    body = rows.get('Body style') or rows.get('Body type')
    spec_json = {k:v for k,v in {
        "door_count": door,
        "seat_count": seat,
        "transmission": rows.get('Transmission'),
        "battery_capacity": rows.get('Battery capacity'),
        "engine_size": rows.get('Engine size'),
        "power": rows.get('Power'),
    }.items() if v}
    return {"fuel": fuel, "body_type": body,
            "spec_json": json.dumps(spec_json, ensure_ascii=False)}

# ---------- ③ colours ----------
def parse_colors(s):
    if s is None: return []
    colors = {img['alt'].strip().title()
              for img in s.select("img[alt]")}
    return sorted(c for c in colors if len(c) > 2)

# ---------- ④ 合体 ----------
def scrape_one(model_url):
    top  = fetch(model_url)
    spec = fetch(model_url+"/specifications", allow_404=True)
    col  = fetch(model_url+"/colours",        allow_404=True)

    data = parse_main(top, model_url)
    data.update(parse_specs(spec))
    data["colours"] = parse_colors(col)
    return data
