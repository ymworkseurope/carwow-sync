"""
urls.py  –  Carwow 全モデル URL 一覧を生成
rev: 2025-08-22 T23:20Z
-------------------------------------------
OUT という list[str] をモジュール import だけで得られる設計
"""

from __future__ import annotations
import gzip, re, requests, xml.etree.ElementTree as ET
from typing import Iterator, List

INDEX_URL = "https://www.carwow.co.uk/sitemap.xml"
UA        = ("Mozilla/5.0 (+https://github.com/ymworkseurope/"
             "carwow-sync 2025-08-22)")
HEAD      = {"User-Agent": UA}

# ---------- low-level ---------- #
def _fetch_xml(url: str) -> ET.Element:
    """url ⇒ bytes ⇒ (必要なら gunzip) ⇒ ElementTree root"""
    r = requests.get(url, headers=HEAD, timeout=30)
    r.raise_for_status()

    raw = r.content
    if r.headers.get("Content-Encoding") == "gzip" or url.endswith(".gz"):
        raw = gzip.decompress(raw)

    return ET.fromstring(raw)

def _iter_child_sitemaps(index_url: str) -> Iterator[str]:
    root = _fetch_xml(index_url)
    for loc in root.iterfind(".//{*}loc"):
        url = loc.text.strip()
        if url.endswith((".xml", ".xml.gz")) and "sitemap" in url and "prismic" not in url:
            yield url

def _iter_model_loc(xml_root: ET.Element) -> Iterator[str]:
    for loc in xml_root.iterfind(".//{*}loc"):
        url = loc.text.strip()
        # https://www.carwow.co.uk/{make}/{model}
        if re.fullmatch(r"https://www\.carwow\.co\.uk/[a-z0-9-]+/[a-z0-9-]+/?", url):
            yield url

# ---------- public ---------- #
def _build_out() -> List[str]:
    models: list[str] = []
    for sm_url in _iter_child_sitemaps(INDEX_URL):
        root = _fetch_xml(sm_url)
        models.extend(_iter_model_loc(root))
    return sorted(set(models))

# 実行時にすぐ使えるリスト
OUT: List[str] = _build_out()

# ---------- CLI (optional) ---------- #
if __name__ == "__main__":
    print(f"Total models: {len(OUT)}")
    print("sample 20:", OUT[:20])
