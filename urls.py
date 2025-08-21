"""
urls.py  –  Carwow 全モデル URL 一覧を生成
rev: 2025-08-22 T23:55Z
-------------------------------------------
OUT という list[str] を import だけで取得できる設計
"""

from __future__ import annotations
import gzip, re, requests, xml.etree.ElementTree as ET
from typing import Iterator, List

INDEX_URL = "https://www.carwow.co.uk/sitemap.xml"
UA        = ("Mozilla/5.0 (+https://github.com/ymworkseurope/"
             "carwow-sync 2025-08-22)")
HEAD      = {"User-Agent": UA}

# ---------- low-level ---------- #
def _maybe_gunzip(raw: bytes) -> bytes:
    """先頭2バイト magic 判定で gzip なら解凍。失敗したらそのまま返す。"""
    if raw[:2] == b"\x1f\x8b":          # gzip magic number
        try:
            return gzip.decompress(raw)
        except gzip.BadGzipFile:
            pass
    return raw

def _fetch_xml(url: str) -> ET.Element:
    r = requests.get(url, headers=HEAD, timeout=30)
    r.raise_for_status()
    xml_bytes = _maybe_gunzip(r.content)
    return ET.fromstring(xml_bytes)

def _iter_child_sitemaps(index_url: str) -> Iterator[str]:
    root = _fetch_xml(index_url)
    for loc in root.iterfind(".//{*}loc"):
        url = loc.text.strip()
        if url.endswith((".xml", ".xml.gz")) and "prismic" not in url:
            yield url

def _iter_model_loc(xml_root: ET.Element) -> Iterator[str]:
    for loc in xml_root.iterfind(".//{*}loc"):
        url = loc.text.strip()
        # “/make/model” だけを採用
        if re.fullmatch(r"https://www\.carwow\.co\.uk/[a-z0-9-]+/[a-z0-9-]+/?", url):
            yield url

# ---------- public ---------- #
def _build_out() -> List[str]:
    models: list[str] = []
    for sm_url in _iter_child_sitemaps(INDEX_URL):
        root = _fetch_xml(sm_url)
        models.extend(_iter_model_loc(root))
    return sorted(set(models))

OUT: List[str] = _build_out()

# ---------- optional CLI ---------- #
if __name__ == "__main__":
    print(f"Total models = {len(OUT)}")
    print("First 20:", OUT[:20])
