"""
model_scraper.py – 各モデルページから主要スペックを取得
   * Model名
   * Body type
   * Available fuel types
   * DeepL 翻訳 (任意)

使い方:
    $ python model_scraper.py https://www.carwow.co.uk/skoda/octavia-estate

環境変数:
    DEEPL_AUTH_KEY = <DeepL API キー>  (未設定なら翻訳スキップ)
"""

from __future__ import annotations
import os, re, sys, json, requests
from bs4 import BeautifulSoup
from typing import Dict, Optional, Tuple

UA = ("Mozilla/5.0 (+https://github.com/ymworkseurope/"
      "carwow-sync 2025-08-23)")
HEAD = {"User-Agent": UA}

# ────────── HTML パース ──────────
_KEYS = {
    "model": re.compile(r"\bmodel\b", re.I),
    "body_type": re.compile(r"\bbody\s*type\b", re.I),
    "fuel": re.compile(r"\bavailable\s*fuel\s*types?\b", re.I),
}

def parse_spec(html: str) -> Tuple[str, str, str]:
    """
    ページ HTML から (model, body_type, fuel) を返す。
    見つからなければ空文字。
    """
    soup = BeautifulSoup(html, "lxml")
    mdl = bod = ful = ""

    root = soup.select_one("div.review-overview__at-a-glance-model")
    if not root:
        return mdl, bod, ful

    # div → div のペアで (heading, value)
    divs = root.find_all("div", recursive=False)
    for i in range(0, len(divs) - 1, 2):
        head = divs[i].get_text(strip=True)
        val  = divs[i + 1].get_text(" ", strip=True)
        if _KEYS["model"].search(head):
            mdl = val
        elif _KEYS["body_type"].search(head):
            bod = val
        elif _KEYS["fuel"].search(head):
            ful = val
    return mdl, bod, ful


# ────────── DeepL 翻訳 ──────────
DEEPL_KEY = os.getenv("DEEPL_AUTH_KEY")

def deepl_translate(text: str, target_lang="JA") -> str:
    if not text or not DEEPL_KEY:
        return ""
    url = "https://api-free.deepl.com/v2/translate"
    payload = {
        "auth_key": DEEPL_KEY,
        "text": text,
        "target_lang": target_lang,
    }
    try:
        r = requests.post(url, data=payload, timeout=15)
        r.raise_for_status()
        return r.json()["translations"][0]["text"]
    except Exception as e:
        sys.stderr.write(f"[WARN] DeepL failed: {e}\n")
        return ""


# ────────── 単一ページ処理 ──────────
def scrape(url: str) -> Dict[str, str]:
    r = requests.get(url, headers=HEAD, timeout=30)
    r.raise_for_status()
    mdl, bod, ful = parse_spec(r.text)

    return {
        "url"        : url,
        "model_en"   : mdl,
        "model_ja"   : deepl_translate(mdl),
        "body_type"  : bod,
        "fuel"       : ful,
    }


# ────────── CLI ──────────
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("usage: python model_scraper.py <model-url>")
        sys.exit(1)

    info = scrape(sys.argv[1])
    print(json.dumps(info, ensure_ascii=False, indent=2))
