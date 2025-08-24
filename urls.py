#!/usr/bin/env python3
"""
urls.py  – 2025-09-xx new-simple
メーカー直下ページを回ってモデル URL を列挙
"""

from typing import Iterator
from make_scraper import get_model_urls

# 必要なメーカーだけ並べる。重複表記は一つに統一
MAKES = [
    "abarth","alfa-romeo","audi","bmw","byd","citroen","cupra","dacia",
    "fiat","ford","genesis","honda","hyundai","jeep","kia","land-rover",
    "lexus","mg","mini","mazda","mercedes-benz","nissan","peugeot",
    "polestar","renault","seat","skoda","smart","subaru","suzuki",
    "tesla","toyota","vauxhall","volkswagen","volvo","xpeng"
]

def iter_model_urls() -> Iterator[str]:
    """メーカーごとのモデル URL を順に yield"""
    for make in MAKES:
        for url in get_model_urls(make):
            yield url
