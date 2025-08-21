# urls.py  ─ モデル URL を列挙して yield
import re, requests, bs4, itertools
from urllib.parse import urlparse

BASE  = "https://www.carwow.co.uk"
HEAD  = {"User-Agent": "Mozilla/5.0 (+github.com/your/carwow-sync)"}

# /make/model だけ欲しい。派生ページは除外
EXCLUDE = (
    "used", "lease", "automatic", "manual",
    "sell-my-car", "valuation", "van-valuation",
    "best-", "most-", "small-cars", "suvs-",
)

def fetch_xml(url: str) -> bs4.BeautifulSoup:
    r = requests.get(url, headers=HEAD, timeout=30)
    r.raise_for_status()
    return bs4.BeautifulSoup(r.text, "xml")   # ← XML パーサ

def all_loc_urls(xml_soup: bs4.BeautifulSoup):
    """<loc> のテキストを namespace 無視で全部取り出す"""
    return [loc.get_text(strip=True) for loc in xml_soup.find_all("loc")]

def iter_model_urls():
    # ① インデックス
    index = fetch_xml(f"{BASE}/sitemap.xml")

    # ② “models” サイトマップ群を収集
    model_sitemaps = [url for url in all_loc_urls(index) if "/models" in url]

    # ③ 各 models-N.xml をたどる
    for sm_url in model_sitemaps:
        sm = fetch_xml(sm_url)
        for url in all_loc_urls(sm):
            parts = urlparse(url).path.strip("/").split("/")
            if len(parts) != 2:
                continue
            make, model = parts
            path = f"/{make}/{model}"
            if any(key in path for key in EXCLUDE):
                continue
            yield BASE + path

# ---- 簡易テスト ----
if __name__ == "__main__":
    gen = iter_model_urls()
    print("sample 10 urls:", list(itertools.islice(gen, 10)))
