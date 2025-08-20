import time, random, re, requests, bs4
BASE = "https://www.carwow.co.uk"
HEAD = {"User-Agent": "Mozilla/5.0 (+https://github.com/your/cwow-sync)"}
delay = lambda: time.sleep(random.uniform(0.8, 1.2))

def fetch(url):
    delay()
    r = requests.get(url, headers=HEAD, timeout=30)
    r.raise_for_status()
    return bs4.BeautifulSoup(r.text, "lxml")

def parse_main(s):
    return {
        "title"       : s.select_one("h1").text.strip(),
        "price_gbp"   : re.sub(r"[^\d]", "", s.select_one("[data-test='price']").text),
        "overview_en" : s.select_one("meta[name='description']")["content"]
    }

def parse_specs(s):
    rows = {dt.text: dd.text for dt,dd in zip(s.select("dt"), s.select("dd"))}
    return {
        "door_count": rows.get("Doors"),
        "fuel"      : rows.get("Fuel type"),
        "body_type" : rows.get("Body style")
    }

def parse_colors(s):
    return [img["alt"] for img in s.select("img[alt][loading='lazy']")]

def scrape_one(model_url):
    s_main  = fetch(model_url)
    s_spec  = fetch(model_url + "/specifications")
    s_color = fetch(model_url + "/colours")
    d = {}
    d.update(parse_main(s_main))
    d.update(parse_specs(s_spec))
    d["colours"] = parse_colors(s_color)
    return d
