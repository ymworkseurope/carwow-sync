"""
urls.py – Carwow 全モデル URL 一覧を生成（修正版）
rev: 2025-08-24
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

# 除外したいキーワード（不要なページ）
EXCLUDE_KEYWORDS = [
    'blog', 'news', 'used', 'sell', 'sale', 'deals', 'prices', 'cars', 
    'uk', '2025', 'position', 'boots', 'plate', 'dealer', 'car', 'cash',
    'lease', 'finance', 'reviews', 'guides', 'advice'
]

# 有効なメーカー名のリスト（実在する自動車メーカーのみ）
VALID_MAKES = {
    'abarth', 'alfa-romeo', 'aston-martin', 'audi', 'bentley', 'bmw', 'citroen',
    'cupra', 'dacia', 'fiat', 'ford', 'genesis', 'honda', 'hyundai', 'infiniti',
    'jaguar', 'jeep', 'kia', 'lamborghini', 'land-rover', 'lexus', 'maserati',
    'mazda', 'mercedes-benz', 'mg', 'mini', 'mitsubishi', 'nissan', 'peugeot',
    'polestar', 'porsche', 'renault', 'seat', 'skoda', 'smart', 'subaru',
    'suzuki', 'tesla', 'toyota', 'vauxhall', 'volkswagen', 'volvo', 'xpeng'
}

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

# ────────── URL フィルタ（修正版） ──────────
def _is_valid_model_url(url: str) -> bool:
    """実際の車種ページかどうかを判定"""
    # 基本的なURL構造チェック
    pattern = r"https://www\.carwow\.co\.uk/([a-z0-9-]+)/([a-z0-9-]+)/?$"
    match = re.match(pattern, url)
    
    if not match:
        return False
    
    make, model = match.groups()
    
    # 除外キーワードチェック
    if any(keyword in make.lower() or keyword in model.lower() for keyword in EXCLUDE_KEYWORDS):
        return False
    
    # 有効なメーカー名かチェック
    if make not in VALID_MAKES:
        return False
    
    # モデル名が数字だけの場合は除外（年数など）
    if model.isdigit():
        return False
    
    # 一般的でないパターンを除外
    if len(model) < 2 or len(make) < 2:
        return False
    
    return True

def _iter_model_loc(xml_root: ET.Element) -> Iterator[str]:
    for loc in xml_root.iterfind(".//{*}loc"):
        url = loc.text.strip()
        if _is_valid_model_url(url):
            yield url

# ────────── public ──────────
def _build_out() -> List[str]:
    models: list[str] = []
    for sm_url in _iter_child_sitemaps(INDEX_URL):
        try:
            root = _fetch_xml(sm_url)
            models.extend(_iter_model_loc(root))
        except Exception as e:
            print(f"Error processing {sm_url}: {e}")
            continue
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
    print("\nSample makes extracted:")
    makes = set()
    for url in OUT[:50]:
        make = url.split('/')[-2]
        makes.add(make)
    print(sorted(makes))
