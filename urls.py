# urls.py  (置き換え用)
import re, requests, bs4
from urllib.parse import urlparse

HEAD = {"User-Agent": "Mozilla/5.0 (+github.com/your/carwow-sync)"}
BASE = "https://www.carwow.co.uk"

MODEL_RE   = re.compile(r"^/([a-z0-9\-]+)/([a-z0-9\-]+)$")
EXCLUDE    = (
    "used", "lease", "automatic", "manual",
    "sell-my-car", "valuation", "van-valuation",
    "best-", "most-", "small-cars", "suvs-",
)

def fetch_xml(url: str) -> bs4.BeautifulSoup:
    r = requests.get(url, headers=HEAD, timeout=30)
    r.raise_for_status()
    return bs4.BeautifulSoup(r.text, "xml")

def iter_model_urls():
    # ① インデックスサイトマップを取得
    index = fetch_xml(f"{BASE}/sitemap.xml")

    # ② <loc> 内で “models” を含むサイトマップだけ読む
    model_maps = [
        loc.get_text()
        for loc in index.select("sitemap > loc")
        if "/models" in loc.get_text()
    ]

    for sm_url in model_maps:
        sm = fetch_xml(sm_url)
        for loc in sm.select("url > loc"):
            url  = loc.get_text().strip()
            path = "/" + "/".join(urlparse(url).path.strip("/").split("/")[:2])

            if not MODEL_RE.match(path):
                continue
            if any(w in path for w in EXCLUDE):
                continue

            yield BASE + path
