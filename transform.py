# rev: 2025-08-23 T23:55Z
"""
raw.jsonl → 変換済み dict を返すユーティリティ
(スクレイパ本体から `from transform import to_payload` で使用)
"""
import os, json, datetime as dt, backoff, requests, slugify

DEEPL_KEY  = os.getenv("DEEPL_KEY")          # 無ければ翻訳スキップ
GBP_TO_JPY = float(os.getenv("GBP_TO_JPY", "195"))

@backoff.on_exception(backoff.expo, requests.RequestException,
                      max_tries=3, jitter=None)
def _deepl(text: str, src="EN", tgt="JA") -> str:
    if not (DEEPL_KEY and text):
        return text or ""
    r = requests.post(
        "https://api-free.deepl.com/v2/translate",
        data={"auth_key": DEEPL_KEY,
              "text": text, "source_lang": src, "target_lang": tgt},
        timeout=30
    )
    r.raise_for_status()
    return r.json()["translations"][0]["text"]

def to_payload(row: dict) -> dict:           # ← ★呼び出し名を固定
    make   = row.get("make_en", "")  or ""
    model  = row.get("model_en", "") or ""
    slug   = slugify.slugify(f"{make}-{model}") or row["slug"]

    price_min_jpy = (row.get("price_min_gbp") or 0) * GBP_TO_JPY \
                    if row.get("price_min_gbp") else None
    price_max_jpy = (row.get("price_max_gbp") or 0) * GBP_TO_JPY \
                    if row.get("price_max_gbp") else None

    return {
        "slug":          slug,
        "make_en":       make,
        "model_en":      model,
        "make_ja":       _deepl(make),
        "model_ja":      _deepl(model),
        "overview_en":   row.get("overview_en", ""),
        "overview_ja":   _deepl(row.get("overview_en", "")),
        "body_type":     row.get("body_type"),
        "fuel":          row.get("fuel"),
        "price_min_gbp": row.get("price_min_gbp"),
        "price_max_gbp": row.get("price_max_gbp"),
        "price_min_jpy": price_min_jpy,
        "price_max_jpy": price_max_jpy,
        "spec_json":     row.get("spec_json", "{}"),
        "media_urls":    row.get("media_urls", []),
        "updated_at":    dt.datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }

# スクリプト単体起動用（任意）
if __name__ == "__main__":
    import sys
    for line in open(sys.argv[1], encoding="utf-8"):
        print(json.dumps(to_payload(json.loads(line)), ensure_ascii=False))


