#!/usr/bin/env python3
# transform.py – 2025-08-26 full

import os, uuid, json, requests
from datetime import datetime
from typing import Dict, Any

# ───────── 設定 ─────────
GBP_TO_JPY = float(os.getenv("GBP_TO_JPY", "195"))
DEEPL_KEY  = os.getenv("DEEPL_KEY", "")  # 空なら翻訳スキップ

# ───────── 翻訳テーブル ─────────
BODY_TYPE_JA = {
    "Small cars":             "小型車",
    "Sports cars":            "スポーツカー",
    "Hybrid & electric cars": "ハイブリッド・EV",
    "Convertibles":           "オープンカー",
    "SUVs":                   "SUV",
    "Hot hatches":            "ホットハッチ",
    "Hatchbacks":             "ハッチバック",
}
MAKE_JA = {
    "Abarth": "アバルト", "Honda": "ホンダ", "Ford": "フォード",
    "Audi": "アウディ", "BMW": "BMW", "Mercedes": "メルセデス",
    "Volkswagen": "フォルクスワーゲン", "Toyota": "トヨタ",
    # 必要に応じ追加
}

# ───────── util ─────────
def uuidv5(slug: str) -> str:
    ns = uuid.UUID("12345678-1234-5678-1234-123456789012")
    return str(uuid.uuid5(ns, slug))


def to_jpy(gbp):
    return int(gbp * GBP_TO_JPY) if gbp else None


def deepl(text: str) -> str:
    if not (text and DEEPL_KEY):
        return ""
    r = requests.post(
        "https://api-free.deepl.com/v2/translate",
        data={
            "auth_key": DEEPL_KEY,
            "text": text,
            "source_lang": "EN",
            "target_lang": "JA",
        },
        timeout=15,
    )
    return r.json()["translations"][0]["text"]


def translate_body(bt_en: str) -> str:
    return BODY_TYPE_JA.get(bt_en, bt_en)


# ───────── main ─────────
def to_payload(raw: Dict[str, Any]) -> Dict[str, Any]:
    body_en = raw.get("body_type") or []

    payload = {
        "id": uuidv5(raw["slug"]),
        "slug": raw["slug"],
        "make_en": raw.get("make_en"),
        "model_en": raw.get("model_en"),
        "make_ja": MAKE_JA.get(raw.get("make_en"), raw.get("make_en")),
        "model_ja": raw.get("model_en"),
        "overview_en": raw.get("overview_en", ""),
        "overview_ja": deepl(raw.get("overview_en", "")),
        "body_type": body_en,
        "body_type_ja": [translate_body(bt) for bt in body_en],
        "fuel": raw.get("fuel"),
        "price_min_gbp": raw.get("price_min_gbp"),
        "price_max_gbp": raw.get("price_max_gbp"),
        "price_min_jpy": to_jpy(raw.get("price_min_gbp")),
        "price_max_jpy": to_jpy(raw.get("price_max_gbp")),
        "spec_json": raw.get("spec_json", "{}"),
        "media_urls": raw.get("media_urls", []),
        "doors": raw.get("doors"),
        "seats": raw.get("seats"),
        "dimensions_mm": raw.get("dimensions_mm"),
        "drive_type": raw.get("drive_type"),
        "grades": raw.get("grades"),
        "engines": raw.get("engines"),
        "colors": raw.get("colors"),
        "catalog_url": raw.get("catalog_url"),
        "full_model_ja": f"{MAKE_JA.get(raw.get('make_en'), raw.get('make_en'))} {raw.get('model_en')}",
        "updated_at": datetime.utcnow().isoformat() + "Z",
    }
    return payload
