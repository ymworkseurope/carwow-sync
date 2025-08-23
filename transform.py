#!/usr/bin/env python3
# transform.py – 2025-08-30 完全修正版
#  変更点
#  ───────────────────────────────────────────
#  • spec_json / media_urls / colors など “配列・辞書” は
#    文字列化せずそのまま渡す   → Supabase の jsonb / array 型に一致
#  • seats / doors / price など、数値 or 文字列どちらでも安全に型変換
#  • Deepl が無ければ自動でスキップ
#  • 日本語訳テーブルを拡充（必要ならさらに追加してください）
#  • full_model_ja を MAKE+MODEL の日本語で生成
#  • updated_at は常に ISO8601 (UTC)

import os, uuid, json, requests
from datetime import datetime
from typing import Dict, Any, List, Union

# ───────── 設定 ─────────
GBP_TO_JPY = float(os.getenv("GBP_TO_JPY", "195"))
DEEPL_KEY  = os.getenv("DEEPL_KEY", "")   # 空なら Deepl 呼ばない

# ───────── 翻訳テーブル ─────────
BODY_TYPE_JA = {
    "Small cars":             "小型車",
    "Sports cars":            "スポーツカー",
    "Hybrid & electric cars": "ハイブリッド・EV",
    "Hybrid and electric":    "ハイブリッド・EV",
    "Convertibles":           "オープンカー",
    "SUVs":                   "SUV",
    "Hot hatches":            "ホットハッチ",
    "Hatchbacks":             "ハッチバック",
    "Estate cars":            "エステート",
    "People carriers":        "ミニバン",
}

MAKE_JA = {
    "Abarth": "アバルト",  "Alfa Romeo": "アルファロメオ", "Audi": "アウディ",
    "BMW": "BMW",          "Citroen": "シトロエン",        "Cupra": "クプラ",
    "Dacia": "ダチア",      "Fiat": "フィアット",           "Ford": "フォード",
    "Honda": "ホンダ",      "Hyundai": "ヒュンダイ",        "Kia": "キア",
    "Mazda": "マツダ",      "Mercedes": "メルセデス",       "Mini": "ミニ",
    "Nissan": "日産",        "Peugeot": "プジョー",          "Polestar": "ポールスター",
    "Renault": "ルノー",    "Skoda": "シュコダ",            "Suzuki": "スズキ",
    "Tesla": "テスラ",      "Toyota": "トヨタ",             "Volkswagen": "フォルクスワーゲン",
    "Volvo": "ボルボ",
}

# ───────── util ─────────
def uuidv5(slug: str) -> str:
    """slug から決定論的 UUIDv5 を生成"""
    ns = uuid.UUID("12345678-1234-5678-1234-123456789012")
    return str(uuid.uuid5(ns, slug))

def to_number(v: Union[str, float, int, None]) -> float | None:
    """数値 or 文字列 → float へ安全変換"""
    if v is None or v == "":
        return None
    try:
        return float(str(v).replace(",", ""))
    except ValueError:
        return None

def to_jpy(v_gbp: Union[str, float, int, None]) -> int | None:
    val = to_number(v_gbp)
    return int(val * GBP_TO_JPY) if val is not None else None

def deepl_translate(text: str) -> str:
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
    r.raise_for_status()
    return r.json()["translations"][0]["text"]

def ja_make(name_en: str | None) -> str | None:
    if not name_en:
        return None
    return MAKE_JA.get(name_en, name_en)

def ja_body_types(bt_list: List[str] | None) -> List[str] | None:
    if not bt_list:
        return None
    return [BODY_TYPE_JA.get(bt, bt) for bt in bt_list]

# ───────── main ─────────
def to_payload(raw: Dict[str, Any]) -> Dict[str, Any]:
    """model_scraper が返す dict → Supabase 送信用 dict へ"""

    # 数値変換
    price_min_gbp = to_number(raw.get("price_min_gbp"))
    price_max_gbp = to_number(raw.get("price_max_gbp"))

    # -------- ペイロード作成 --------
    payload = {
        "id":           uuidv5(raw["slug"]),
        "slug":         raw["slug"],

        # Make / Model
        "make_en":      raw.get("make_en"),
        "model_en":     raw.get("model_en"),
        "make_ja":      ja_make(raw.get("make_en")),
        "model_ja":     raw.get("model_en"),

        # Overview
        "overview_en":  raw.get("overview_en", ""),
        "overview_ja":  deepl_translate(raw.get("overview_en", "")),

        # Body type
        "body_type":    raw.get("body_type") or None,
        "body_type_ja": ja_body_types(raw.get("body_type")),

        # Fuel
        "fuel":         raw.get("fuel"),

        # Price
        "price_min_gbp": price_min_gbp,
        "price_max_gbp": price_max_gbp,
        "price_min_jpy": to_jpy(price_min_gbp),
        "price_max_jpy": to_jpy(price_max_gbp),

        # Spec / media
        "spec_json":    raw.get("spec_json") or {},    # dict のまま
        "media_urls":   raw.get("media_urls") or [],   # list のまま

        # 詳細スペック
        "doors":        raw.get("doors"),
        "seats":        raw.get("seats"),
        "dimensions_mm":raw.get("dimensions_mm"),
        "drive_type":   raw.get("drive_type"),

        # グレード・エンジン・カラー
        "grades":       raw.get("grades"),
        "engines":      raw.get("engines"),
        "colors":       raw.get("colors"),

        # URL, 日本語フル名, 更新時刻
        "catalog_url":  raw.get("catalog_url"),
        "full_model_ja":f"{ja_make(raw.get('make_en'))} {raw.get('model_en')}",
        "updated_at":   datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }

    return payload

# ───────── CLI テスト用 ─────────
if __name__ == "__main__":
    import sys, pprint, json as j
    raw = j.loads(sys.stdin.read())
    pprint.pp(to_payload(raw))

