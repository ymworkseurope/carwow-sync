# transform.py
# rev: 2025-08-24 修正版
"""
raw.jsonl → 変換済み dict を返すユーティリティ
(スクレイパ本体から `from transform import to_payload` で使用)
"""
import os, json, datetime as dt, backoff, requests, slugify
import hashlib

DEEPL_KEY  = os.getenv("DEEPL_KEY")          # 無ければ翻訳スキップ
GBP_TO_JPY = float(os.getenv("GBP_TO_JPY", "195"))

@backoff.on_exception(backoff.expo, requests.RequestException,
                      max_tries=3, jitter=None)
def _deepl(text: str, src="EN", tgt="JA") -> str:
    if not (DEEPL_KEY and text and text.strip()):
        return text or ""
    try:
        r = requests.post(
            "https://api-free.deepl.com/v2/translate",
            data={"auth_key": DEEPL_KEY,
                  "text": text, "source_lang": src, "target_lang": tgt},
            timeout=30
        )
        r.raise_for_status()
        return r.json()["translations"][0]["text"]
    except Exception as e:
        print(f"翻訳エラー: {e}")
        return text or ""

def _generate_id(make: str, model: str, slug: str) -> str:
    """メーカー名、モデル名、スラッグからユニークIDを生成"""
    combined = f"{make}-{model}-{slug}".lower()
    return hashlib.md5(combined.encode()).hexdigest()[:12]

def to_payload(row: dict) -> dict:           # ← ★呼び出し名を固定
    make   = row.get("make_en", "").strip()
    model  = row.get("model_en", "").strip()
    slug   = row.get("slug", "").strip()
    
    # IDの生成
    record_id = _generate_id(make, model, slug)
    
    # 価格の変換
    price_min_jpy = None
    price_max_jpy = None
    if row.get("price_min_gbp"):
        price_min_jpy = int(row["price_min_gbp"] * GBP_TO_JPY)
    if row.get("price_max_gbp"):
        price_max_jpy = int(row["price_max_gbp"] * GBP_TO_JPY)
    
    # media_urlsの処理
    media_urls = row.get("media_urls", [])
    if isinstance(media_urls, list):
        media_urls_str = json.dumps(media_urls)
    else:
        media_urls_str = str(media_urls) if media_urls else "[]"
    
    return {
        "id":            record_id,
        "slug":          slug,
        "make_en":       make,
        "model_en":      model,
        "make_ja":       _deepl(make),
        "model_ja":      _deepl(model),
        "overview_en":   row.get("overview_en", "").strip(),
        "overview_ja":   _deepl(row.get("overview_en", "").strip()),
        "body_type":     row.get("body_type"),
        "fuel":          row.get("fuel"),
        "price_min_gbp": row.get("price_min_gbp"),
        "price_max_gbp": row.get("price_max_gbp"),
        "price_min_jpy": price_min_jpy,
        "price_max_jpy": price_max_jpy,
        "spec_json":     row.get("spec_json", "{}"),
        "media_urls":    media_urls_str,
        "updated_at":    dt.datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }

# スクリプト単体起動用（任意）
if __name__ == "__main__":
    import sys
    for line in open(sys.argv[1], encoding="utf-8"):
        print(json.dumps(to_payload(json.loads(line)), ensure_ascii=False, indent=2))
