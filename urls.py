"""
urls.py – Carwow 全モデル URL 一覧を生成
rev: 2025-08-22 T24:15Z
-------------------------------------------
* `OUT : list[str]`       … 全モデル URL（重複なし・昇順）
* `iter_model_urls()`     … 後方互換イテレータ
  └─ どちらも import だけで利用できます
"""

from __future__ import annotations
import gzip
import re
import requests
import xml.etree.ElementTree as ET
from typing import Iterator, List

INDEX_URL = "https://www.carwow.co.uk/sitemap.xml"
UA = (
    "Mozilla/5.0 (+https://github.com/ymworkseurope/"
    "carwow-sync 2025-08-22)"
)
HEAD = {"User-Agent": UA}

# ────────── low-level ──────────
def _maybe_gunzip(raw: bytes) -> bytes:
    """gzip で圧縮されていれば解凍して返す"""
    if raw.startswith(b"\x1f\x8b"):           # gzip magic
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
    """トップサイトマップから階層下の .xml/.xml.gz を列挙"""
    root = _fetch_xml(index_url)
    for loc in root.iterfind(".//{*}loc"):
        url = loc.text.strip()
        if url.endswith((".xml", ".xml.gz")) and "prismic" not in url:
            yield url


def _iter_model_loc(xml_root: ET.Element) -> Iterator[str]:
    """子サイトマップから “/make/model” パスだけ抽出"""
    pat = re.compile(
        r"https://www\.carwow\.co\.uk/[a-z0-9-]+/[a-z0-9-]+/?\Z"
    )
    for loc in xml_root.iterfind(".//{*}loc"):
        url = loc.text.strip()
        if pat.fullmatch(url):
            yield url


# ────────── build list ──────────
def _build_out() -> List[str]:
    models: list[str] = []
    for sm_url in _iter_child_sitemaps(INDEX_URL):
        root = _fetch_xml(sm_url)
        models.extend(_iter_model_loc(root))
    return sorted(set(models))


# ---------- public ----------
OUT: List[str] = _build_out()


def iter_model_urls() -> Iterator[str]:
    """旧 main.py 互換：モデル URL を順次 yield"""
    yield from OUT


# ---------- optional CLI ----------
if __name__ == "__main__":
    print(f"Total models = {len(OUT)}")
    print("First 20:", OUT[:20])
