# transform.py
# rev: 2025-08-23 T23:50Z  (← 最新版タイムスタンプを必ず更新)
"""
使い方:
    python transform.py raw.jsonl > payload.jsonl

環境変数:
    DEEPL_KEY        : DeepL Free / Pro の API キー
    GBP_TO_JPY       : 1GBP あたりの円レート (例 195)   ← scrape.py と揃える
"""
import os, sys, json, datetime as dt, backoff, requests, slugify

DEEPL_KEY   = os.getenv("DEEPL_KEY")          # 無い場合は翻訳しない
GBP_TO_JPY  = float(os.getenv("GBP_TO_JPY", "195"))

# ------------- DeepL helper --------------------------------------------------
@backoff.on_exception(backoff.expo,
                      (requests.RequestException,), max_tries=3, jitter=None)
def _deepl(text: str, src="EN", tgt="JA") -> str:
    if not (DEEPL_KEY and text):
        return text or ""
    res = requests.post(
        "https://api-free.deepl.com/v2/translate",
        data={
            "auth_key"    : DEEPL_KEY,
            "text"        : text,
            "source_lang" : src,
            "target_lang" : tgt
        },
        timeout=30
    )
    res.raise_for_status()
    return res.json()["translations"][0]["text"]

# ------------- transform 1 row ----------------------------------------------
def transform(row: dict) -> dict:
    """
    raw (scrape.py) dict -> Supabase / Sheets 共通フォーマット
    """
    # slug 再生成（念の為）
    make  = row.get("make_en")  or ""
    model = row.get("model_en") or ""
    slug  = slugify.slugify(f"{make}-{model}") if (make and model) else row["slug"]

    # DeepL 翻訳
    make_ja     = _deepl(make)
    model_ja    = _deepl(model)
    overview_ja = _deepl(row.get("overview_en", ""))

    # 出力
    return {
        "slug"          : slug,
        "make_en"       : make,
        "model_en"      : model,
        "make_ja"       : make_ja,
        "model_ja"      : model_ja,
        "overview_en"   : row.get("overview_en", ""),
        "overview_ja"   : overview_ja,
        "body_type"     : row.get("body_type"),
        "fuel"          : row.get("fuel"),
        "price_min_gbp" : row.get("price_min_gbp"),
        "price_max_gbp" : row.get("price_max_gbp"),
        "price_min_jpy" : (row.get("price_min_gbp") or 0) * GBP_TO_JPY
                          if row.get("price_min_gbp") else None,
        "price_max_jpy" : (row.get("price_max_gbp") or 0) * GBP_TO_JPY
                          if row.get("price_max_gbp") else None,
        "spec_json"     : row.get("spec_json", "{}"),
        "media_urls"    : row.get("media_urls", []),
        "updated_at"    : dt.datetime.utcnow().isoformat(timespec="seconds") + "Z"
    }

# ------------- main ----------------------------------------------------------
def main():
    if len(sys.argv) != 2:
        print("Usage: python transform.py raw.jsonl > payload.jsonl", file=sys.stderr)
        sys.exit(1)

    with open(sys.argv[1], "r", encoding="utf-8") as fh:
        for line in fh:
            raw = json.loads(line)
            clean = transform(raw)
            print(json.dumps(clean, ensure_ascii=False))

if __name__ == "__main__":
    main()

