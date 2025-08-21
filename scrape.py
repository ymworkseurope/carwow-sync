# scrape.py — Carwow モデル 1 台を dict で返す（2025-08-21）
import time, random, re, json, requests, bs4
from urllib.parse import urljoin, urlparse

UA   = "Mozilla/5.0 (+https://github.com/your/carwow-sync)"
HEAD = {"User-Agent": UA}
GBP_TO_JPY = 195.0                               # 2025-08 時点の概算
delay = lambda: time.sleep(random.uniform(0.8, 1.3))

# ---------- 共通 GET ----------
def fetch(url, allow_404=False):
    delay()
    r = requests.get(url, headers=HEAD, timeout=30)
    if allow_404 and r.status_code == 404:
        return None
    r.raise_for_status()
    return bs4.BeautifulSoup(r.text, "lxml")

# ---------- ヘルパ ----------
def extract_make_model(url, title):
    parts = urlparse(url).path.strip("/").split("/")
    make  = parts[0].capitalize() if parts else ""
    model = parts[1].upper()       if len(parts) > 1 else ""
    if title:
        t  = title.split()
        make  = make  or (t[0] if t else "")
        model = model or (t[1] if len(t) > 1 else "")
    return make, model

def extract_price_range(soup):
    text = soup.get_text(" ")
    # ① £xx,xxx – £yy,yyy
    m = re.search(r"£([\d,]+)\s*[–-]\s*£([\d,]+)", text)
    if m:
        return int(m[1].replace(",","")), int(m[2].replace(",",""))
    # ② “RRP range of £xx,xxx to £yy,yyy”
    m = re.search(r"RRP range of £([\d,]+) to £([\d,]+)", text)
    if m:
        return int(m[1].replace(",","")), int(m[2].replace(",",""))
    # ③ 単一価格多数 → min/max
    prices = [int(p.replace(",",""))
              for p in re.findall(r"£([\d,]+)", text)]
    return (min(prices), max(prices)) if len(prices) >= 2 else (None, None)

def extract_media_urls(soup, base):
    urls=[]
    for img in soup.select("img[src]"):
        src = img["src"]
        if any(k in src.lower() for k in ["logo","icon","favicon","badge"]):
            continue
        full = urljoin(base, src)
        if full.startswith("http") and full not in urls:
            urls.append(full)
    return urls

# ---------- ① TOP ----------
def parse_main(soup, url):
    title  = soup.select_one("h1").text.strip() if soup.select_one("h1") else ""
    make, model = extract_make_model(url, title)
    pmin, pmax  = extract_price_range(soup)

    return {
        "title"         : title,
        "make_en"       : make,
        "model_en"      : model,
        "price_min_gbp" : pmin,
        "price_max_gbp" : pmax,
        "price_min_jpy" : int(pmin*GBP_TO_JPY) if pmin else None,
        "price_max_jpy" : int(pmax*GBP_TO_JPY) if pmax else None,
        "overview_en"   : soup.select_one("meta[name='description']")["content"]
                          if soup.select_one("meta[name='description']") else "",
        "media_urls"    : extract_media_urls(soup, url),
    }

# ---------- ② SPEC ----------
def parse_specs(soup):
    if soup is None:
        return {}
    rows = {dt.text.strip(): dd.text.strip()
            for dt, dd in zip(soup.select("dt"), soup.select("dd"))}

    door = rows.get("Number of doors") or rows.get("Doors")
    seat = rows.get("Number of seats") or rows.get("Seats")
    fuel = rows.get("Fuel type")       or rows.get("Fuel")
    body = rows.get("Body style")      or rows.get("Body type")

    spec_json = {k:v for k,v in {
        "door_count": door,
        "seat_count": seat,
        "transmission": rows.get("Transmission"),
        "engine_size": rows.get("Engine size"),
        "power": rows.get("Power"),
    }.items() if v}

    return {"fuel": fuel,
            "body_type": body,
            "spec_json": json.dumps(spec_json, ensure_ascii=False)}

# ---------- ③ COLOUR ----------
def parse_colors(soup):
    if soup is None:
        return []
    colors = {img["alt"].strip().title()
              for img in soup.select("img[alt]")}
    # フィルタ：英語色名らしいものだけ
    return sorted([c for c in colors
                   if re.search(r"(black|white|blue|green|red|silver|grey|gray|yellow|orange|purple)", c, re.I)])

# ---------- ④ 統合 ----------
def scrape_one(model_url):
    top  = fetch(model_url)
    spec = fetch(model_url + "/specifications", allow_404=True)
    col  = fetch(model_url + "/colours",        allow_404=True)

    data = parse_main(top, model_url)
    data.update(parse_specs(spec))
    data["colours"] = parse_colors(col)
    return data
