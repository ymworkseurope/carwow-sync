#!/usr/bin/env python3
# transform.py – 2025‑08‑26 array‑safe
import uuid, json
from datetime import datetime
from typing import Dict, Any


BODY_TYPE_JA = {"Small cars": "小型車", "Sports cars": "スポーツカー", "Hybrid & electric cars": "ハイブリッド・EV", "Convertibles": "オープンカー", "SUVs": "SUV", "Hot hatches": "ホットハッチ", "Hatchbacks": "ハッチバック"}
MAKE_JA = {"Abarth": "アバルト"}


def generate_uuid_from_slug(slug: str) -> str:
ns = uuid.UUID("12345678-1234-5678-1234-123456789012")
return str(uuid.uuid5(ns, slug))


def translate_body_type(bt_en: str) -> str:
return BODY_TYPE_JA.get(bt_en, bt_en)


def safe_price(v):
try: return int(v) if v is not None else None
except: return None


def to_payload(raw: Dict[str, Any]) -> Dict[str, Any]:
body_en = raw.get("body_type") or []
payload = {
"id": generate_uuid_from_slug(raw["slug"]),
"slug": raw["slug"],
"make_en": raw.get("make_en"),
"model_en": raw.get("model_en"),
"make_ja": MAKE_JA.get(raw.get("make_en"), raw.get("make_en")),
"model_ja": raw.get("model_en"),
"overview_en": raw.get("overview_en", ""),
"overview_ja": "",
"body_type": body_en,
"body_type_ja": [translate_body_type(bt) for bt in body_en],
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
"full_model
