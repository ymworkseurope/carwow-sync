# urls.py – Carwow “純粋モデル” URL だけをイテレート
import re, requests, bs4
from urllib.parse import urlparse

HEAD = {"User-Agent": "Mozilla/5.0 (+github.com/your/carwow-sync)"}

# 例: /abarth/500e           → ✅
#     /sell-my-car-ford      → ❌
#     /vauxhall/used         → ❌
MODEL_PATTERN = re.compile(r"^/([a-z0-9\-]+)/([a-z0-9\-]+)$")

EXCLUDE_WORDS = (
    "used", "lease", "automatic", "manual",        # 派生・中古
    "sell-my-car", "valuation", "van-valuation",   # 査定
    "best-", "most-", "small-cars", "suvs-",       # ランキング
)

def iter_model_urls():
    xml = requests.get(
        "https://www.carwow.co.uk/sitemap/model_pages.xml",
        headers=HEAD, timeout=30
    ).text
    soup = bs4.BeautifulSoup(xml, "xml")

    for loc in soup.select("url > loc"):
        url = loc.text.strip()
        path = "/" + "/".join(urlparse(url).path.strip("/").split("/")[:2])

        # ① パス形式が /make/model ?
        if not MODEL_PATTERN.match(path):
            continue

        # ② NG ワードを含む？ → スキップ
        if any(w in path for w in EXCLUDE_WORDS):
            continue

        yield "https://www.carwow.co.uk" + path
