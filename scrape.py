# scrape.py
import time, random, requests, bs4, re
HEAD = {"User-Agent": "Mozilla/5.0 (+https://github.com/your/carwow-sync)"}
delay = lambda: time.sleep(random.uniform(0.8, 1.3))

def fetch(url, allow_404=False):
    delay()
    r = requests.get(url, headers=HEAD, timeout=30)
    if allow_404 and r.status_code == 404:
        return None
    r.raise_for_status()
    return bs4.BeautifulSoup(r.text, "lxml")

# ---------- パーサは以前のまま ----------
# parse_main / parse_specs / parse_colors は省略
# ----------------------------------------

def scrape_one(model_url):
    s_main = fetch(model_url)                     # ここで 200 保証
    s_spec = fetch(model_url + "/specifications", allow_404=True)
    s_col  = fetch(model_url + "/colours",        allow_404=True)

    data = {}
    data.update(parse_main(s_main))
    if s_spec:  data.update(parse_specs(s_spec))
    data["colours"] = parse_colors(s_col) if s_col else []
    return data
