"""
urls.py – carwow モデルページ URL 一覧を生成
------------------------------------------------
* https://www.carwow.co.uk/sitemap.xml から再帰的に子サイトマップを取得
* /make/model 形式 (例: /tesla/model-3) だけ抽出
* LP や広告 (used-cars-, sell-my-car- など) は除外
最終結果は変数 OUT (list[str]) に格納される
"""

import re, gzip, io, requests, xml.etree.ElementTree as ET
from typing import Generator, List

INDEX_URL = "https://www.carwow.co.uk/sitemap.xml"
HEADERS   = {
    "User-Agent": "Mozilla/5.0 (+https://github.com/ymworkseurope/carwow-sync)",
    "Accept-Encoding": "gzip,deflate",
}

# ────────────────────────────────────────
def _fetch_xml(url: str) -> ET.Element:
    """GET → (gzip 対応) → ElementTree"""
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()

    data = (
        gzip.decompress(r.content)
        if (r.headers.get("Content-Encoding") == "gzip" or url.endswith(".gz"))
        else r.content
    )
    # Carwow の XML は xmlns を持つのでワイルドカードで OK
    return ET.fromstring(data)

# ────────────────────────────────────────
def _iter_child_sitemaps(index_url: str) -> Generator[str, None, None]:
    """インデックス XML から <loc> を列挙"""
    root = _fetch_xml(index_url)
    for sm in root.findall(".//{*}sitemap"):
        loc = sm.find("{*}loc")
        if loc is not None and loc.text:
            yield loc.text.strip()

# ────────────────────────────────────────
_RE_MODEL   = re.compile(r"^/([\w-]+)/([\w-]+)/?$")
_SKIP_TOKENS = (
    "sell-my", "used-", "lease", "deals", "review", "news", "vs", "best-",
    "-cheap", "-automatic", "-manual", "-black", "-blue", "-red", "-green",
)

def _iter_model_urls(sitemap_url: str) -> Generator[str, None, None]:
    """子サイトマップから /make/model だけ抽出"""
    root = _fetch_xml(sitemap_url)
    for url_tag in root.findall(".//{*}url"):
        loc = url_tag.find("{*}loc")
        if loc is None or not loc.text:
            continue
        url = loc.text.strip()
        # ドメインを取り除いたパスだけで判定
        path = re.sub(r"^https?://www\.carwow\.co\.uk", "", url)
        if (
            _RE_MODEL.match(path) and
            not any(tok in path for tok in _SKIP_TOKENS)
        ):
            yield url

# ────────────────────────────────────────
def _build_out() -> List[str]:
    out, seen = [], set()

    for sm_url in _iter_child_sitemaps(INDEX_URL):
        # モデルページが入っているのは prismic_pages 系だけ
        if "prismic_pages" not in sm_url:
            continue
        for model_url in _iter_model_urls(sm_url):
            slug = "/".join(model_url.rstrip("/").split("/")[-2:])
            if slug not in seen:
                seen.add(slug)
                out.append(model_url)

    return sorted(out)

# このリストを main.py / scrape.py が import して使う
OUT = _build_out()

# ────────────────── デバッグ実行 ────────────────
if __name__ == "__main__":
    print(f"Total model pages: {len(OUT)}")
    for u in OUT[:20]:
        print("  ", u)
