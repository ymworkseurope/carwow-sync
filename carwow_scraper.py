#!/usr/bin/env python3
"""
carwow_scraper.py
データ取得に特化したメインスクレイピングモジュール - 完全修正版
"""
from __future__ import annotations


import json
import re
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Set


import requests
from bs4 import BeautifulSoup


# ======================== Configuration ========================
BASE_URL = "https://www.carwow.co.uk"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; carwow-bot/1.0)"}
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3
RETRY_DELAY = 2 # seconds


EXCLUDE_SEGMENTS: frozenset[str] = frozenset(
{
# category / colour segments that are NOT models
"automatic",
"manual",
"lease",
"used",
"deals",
"finance",
"reviews",
"prices",
"news",
"hybrid",
"electric",
"suv",
"estate",
"hatchback",
"saloon",
"coupe",
"convertible",
"sports",
"mpv",
"people-carriers",
# colours
"white",
"black",
"silver",
"grey",
"gray",
"red",
"blue",
"green",
"yellow",
"orange",
"brown",
"purple",
"pink",
"gold",
"bronze",
"beige",
"cream",
"multi-colour",
"two-tone",
}
)




# ======================== HTTP Utilities ========================
class HTTPClient:
"""Centralised HTTP client with retry logic."""


@staticmethod
def get(url: str, *, allow_redirects: bool = True) -> requests.Response:
for attempt in range(1, MAX_RETRIES + 1):
try:
resp = requests.get(
url, headers=HEADERS, timeout=REQUEST_TIMEOUT, allow_redirects=allow_redirects
)
if resp.status_code == 200:
return resp
# 4xx other than 404 → unrecoverable
if 400 <= resp.status_code < 500:
resp.raise_for_status()
except requests.RequestException as exc:
if attempt == MAX_RETRIES:
raise exc
time.sleep(RETRY_DELAY * attempt)
# should never reach here
