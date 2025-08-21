"""
urls.py – Carwow 全モデル URL 一覧を生成
rev: 2025-08-23
-------------------------------------------
`OUT` という list[str] を import だけで取得できる
"""

from __future__ import annotations
import gzip, re, requests, xml.etree.ElementTree as ET
from typing import Iterator, List

INDEX_URL = "https://www.carwow.co.uk/sitemap.xml"
UA        = ("Mozilla/5.0 (+https://github.com/ymworkseurope/"
             "carwow-sync 2025-08-23)")
HEAD      = {"User-Agent": UA}

# ────────── low-level ──────────
def _maybe_gunzip(raw: bytes) -> bytes:
    if raw[:2] == b"\x1f\x8b":           # gzip magic
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


# ────────── URL フィルタ ──────────
# ① make / model の 2 セグメントだけ     例) /bmw/i3
# ② blog-* / used-cars-* などを除外
_RE_MODEL = re.compile(
    r"https://www\.carwow\.co\.uk/"
    r"(?!blog-|used-"           # ←除外キーワード
      r"sell-"                  # 将来拡張用
    r")[a-z0-9-]+/[a-z0-9-]+/?$"
)

def _iter_model_loc(xml_root: ET.Element) -> Iterator[str]:
    for loc in xml_root.iterfind(".//{*}loc"):
        url = loc.text.strip()
        if _RE_MODEL.fullmatch(url):
            yield url


# ────────── public ──────────
def _build_out() -> List[str]:
    models: list[str] = []
    for sm_url in _iter_child_sitemaps(INDEX_URL):
        root = _fetch_xml(sm_url)
        models.extend(_iter_model_loc(root))
    return sorted(set(models))


# 既存: OUT は全モデル URL のリスト
OUT: List[str] = _build_out()

# ▼ main.py（旧版）互換イテレータ
def iter_model_urls() -> Iterator[str]:
    """yield でモデル URL を順次返す（後方互換）"""
    yield from OUT


# ────────── CLI ──────────
if __name__ == "__main__":
    print(f"Total models = {len(OUT)}")
    print("First 20:", OUT[:20])
