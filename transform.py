#!/usr/bin/env python3
# transform.py – 2025-08-25 fixed

import uuid, json
from datetime import datetime
from typing import Dict, Any

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
    "Abarth": "アバルト",
    # 必要ならメーカーを追加
}

# ───────── util ─────────
def generate_uuid_from_slug(slug: str) -> str:
    """slug から決定論的 UUID v5 を生成"""
    ns = uuid.UUID("12345678-1234-5678-1234-123456789012")
    return str(uuid.uuid5(ns, slug))


def translate_to_japanese(text: str) -> str:
    return MAKE_JA.get(text, text)


def translate_body_type(bt_en: str) -> str:
    return BODY_TYPE_JA.get(bt_en, bt_en)


def safe_price(val):
    try:
        return int(val) if val is not None else "データ無"
    except Exception:
        return "データ無"


# ───────── main ─────────
def to_payload(raw: Dict[str, Any]) -> Dict[str, Any]:
    slug = raw["slug"].strip()
    uid = generate_uuid_from_slug(slug)

    make_en = raw.get("make_en", "")
    model_en = raw.get("model_en", "")
    body_en = raw.get("body_type") or []
    body_ja = [translate_body_type(bt) for bt in body_en]

    payload = {
        "id": uid,
        "slug": slug,
        "make_en": make_en,
        "model_en": model_en,
        "make_ja": translate_to_japanese(make_en),
        "model_ja": translate_to_japanese(model_en),
        "overview_en": raw.get("overview_en", ""),
        "overview_ja": "",                       # DeepL などで後処理
        "body_type": body_en,                    # text[]
        "body_type_ja": body_ja,                 # text[]
        "fuel": raw.get("fuel"),
        "price_min_gbp": safe_price(raw.get("price_min_gbp")),
        "price_max_gbp": safe_price(raw.get("price_max_gbp")),
        "price_min_jpy": None,
        "price_max_jpy": None,
        "spec_json": raw.get("spec_json", "{}"),
        "media_urls": raw.get("media_urls", []),
        "doors": raw.get("doors"),
        "seats": raw.get("seats"),
        "dimensions_mm": raw.get("dimensions_mm"),
        "drive_type": raw.get("drive_type"),
        "grades": raw.get("grades"),
        "engines": raw.get("engines"),
        "catalog_url": raw.get("catalog_url"),
        "full_model_ja": f"{translate_to_japanese(make_en)} {translate_to_japanese(model_en)}",
        "updated_at": datetime.utcnow().isoformat() + "Z",
    }
    return payload
