"""
scrape.py – 既存ロジック＋簡易リトライ
------------------------------------------------
* HTTP 503, 429 時は指数バックオフで最大 5 回リトライ
* それ以外は従来どおり
（スクレイピングの parse_main / parse_specs などは変更していません）
"""

import time, random, requests, backoff
# 既存の import は省略 … parse_main, parse_specs, などそのまま

UA   = "Mozilla/5.0 (+https://github.com/ymworkseurope/carwow-sync)"
HEAD = {"User-Agent": UA, "Accept-Encoding": "gzip,deflate"}
delay = lambda: time.sleep(random.uniform(0.8, 1.3))

# ───────── リトライ付き fetch ──────────
@backoff.on_exception(
    backoff.expo,
    requests.exceptions.HTTPError,
    max_tries=5,
    giveup=lambda e: e.response is not None and e.response.status_code not in (429, 503),
)
def fetch(url: str, allow_404=False):
    delay()
    r = requests.get(url, headers=HEAD, timeout=30)
    if allow_404 and r.status_code == 404:
        return None
    r.raise_for_status()
    return bs4.BeautifulSoup(r.content, "lxml")

# 以降の parse_main / parse_specs / scrape_one などは
# あなたの最新版をそのまま残して下さい
