#!/usr/bin/env python3
# transform.py – 2025-08-30 full (和訳テーブル拡充 + drive_type_ja)
import os, uuid, json, requests
from datetime import datetime
from typing import Dict, Any

GBP_TO_JPY = float(os.getenv("GBP_TO_JPY", "195"))
DEEPL_KEY  = os.getenv("DEEPL_KEY", "")

BODY_TYPE_JA = {
    "Small cars":             "小型車",
    "Sports cars":            "スポーツカー",
    "Hybrid & electric cars": "ハイブリッド・EV",
    "Convertibles":           "カブリオレ",
    "SUVs":                   "SUV",
    "Hot hatches":            "ホットハッチ",
    "Hatchbacks":             "ハッチバック",
    "Camper vans":            "キャンピングカー",
    "Coupes":                 "クーペ",
    "Saloons":                "セダン",
    "Estate cars":            "ステーションワゴン",
    "People carriers":        "ミニバン",
    "Electric vans":          "電気バン",
}

MAKE_JA = {
    # 既存
    "Abarth":"アバルト", "Audi":"アウディ", "Ford":"フォード", "Honda":"ホンダ",
    "Mercedes":"メルセデス・ベンツ", "Volkswagen":"フォルクスワーゲン",
    "Toyota":"トヨタ", "BMW":"BMW", "MINI":"MINI",
    # 追加分（翻訳されてないワード）
    "Alfa Romeo":"アルファロメオ",
    "Alpine":"アルピーヌ",
    "BYD":"BYD",
    "Byd":"BYD",
    "Bmw":"BMW",
    "Citroen":"シトロエン",
    "Cupra":"クプラ",
    "Dacia":"ダチア",
    "DS":"DS",
    "Ds":"DS",
    "Fiat":"フィアット",
    "Genesis":"ジェネシス",
    "GWM":"GWM",
    "Gwm":"GWM",
    "Hyundai":"ヒュンダイ",
    "Jeep":"ジープ",
    "Kia":"キア",
    "Land Rover":"ランドローバー",
    "Lexus":"レクサス",
    "Lotus":"ロータス",
    "Mazda":"マツダ",
    "MG":"MG",
    "Mg":"MG",
    "Mini":"MINI",
    "Nissan":"日産",
    "Peugeot":"プジョー",
    "Polestar":"ポールスター",
    "Renault":"ルノー",
    "Seat":"セアト",
    "Skoda":"シュコダ",
    "Smart":"スマート",
    "Subaru":"スバル",
    "Suzuki":"スズキ",
    "Tesla":"テスラ",
    "Vauxhall":"ボクスホール",
    "Volvo":"ボルボ",
    "Xpeng":"エクスペン",
    # 参考リストから主要ブランド追加
    "Aston Martin":"アストンマーチン",
    "Bentley":"ベントレー",
    "Ferrari":"フェラーリ",
    "Jaguar":"ジャガー",
    "Lamborghini":"ランボルギーニ",
    "Lancia":"ランチア",
    "Maserati":"マセラティ",
    "McLaren":"マクラーレン",
    "Porsche":"ポルシェ",
    "Rolls-Royce":"ロールスロイス",
    "Saab":"サーブ",
    "Infiniti":"インフィニティ",
    "Mitsubishi":"三菱",
    "Isuzu":"いすゞ",
    "Daihatsu":"ダイハツ",
    "Cadillac":"キャデラック",
    "Chevrolet":"シボレー",
    "Chrysler":"クライスラー",
    "Dodge":"ダッジ",
    "Buick":"ビュイック",
    "GMC":"GMC",
    "Lincoln":"リンカーン",
}

DRIVE_TYPE_JA = {
    "Automatic":"AT",
    "Manual":"MT",
    "CVT":"CVT",
    "Electric":"EV",
    "Semi-automatic":"AT",
    "Tiptronic":"AT",
}

def uuidv5(slug: str) -> str:
    ns = uuid.UUID("12345678-1234-5678-1234-123456789012")
    return str(uuid.uuid5(ns, slug))

def to_jpy(gbp: float|None):
    return int(gbp * GBP_TO_JPY) if gbp else None

def deepl(text: str) -> str:
    if not (text and DEEPL_KEY): return ""
    r = requests.post(
        "https://api-free.deepl.com/v2/translate",
        data={"auth_key":DEEPL_KEY,"text":text,"source_lang":"EN","target_lang":"JA"},
        timeout=15,
    )
    return r.json()["translations"][0]["text"]

def t_body(bt_en: str) -> str:   
    return BODY_TYPE_JA.get(bt_en, bt_en)

def t_drive(dt_en: str) -> str:  
    return DRIVE_TYPE_JA.get(dt_en, dt_en)

def to_payload(raw: Dict[str,Any]) -> Dict[str,Any]:
    body_en  = raw.get("body_type") or []
    drive_en = raw.get("drive_type") or ""
    payload = {
        "id":           uuidv5(raw["slug"]),
        "slug":         raw["slug"],
        "make_en":      raw.get("make_en"),
        "model_en":     raw.get("model_en"),
        "make_ja":      MAKE_JA.get(raw.get("make_en"), raw.get("make_en")),
        "model_ja":     raw.get("model_en"),
        "overview_en":  raw.get("overview_en",""),
        "overview_ja":  deepl(raw.get("overview_en","")),
        "body_type":    body_en,
        "body_type_ja": [t_body(bt) for bt in body_en],
        "fuel":         raw.get("fuel"),
        "price_min_gbp":raw.get("price_min_gbp"),
        "price_max_gbp":raw.get("price_max_gbp"),
        "price_min_jpy":to_jpy(raw.get("price_min_gbp")),
        "price_max_jpy":to_jpy(raw.get("price_max_gbp")),
        "spec_json":    json.dumps(raw.get("spec_json",{}), ensure_ascii=False),
        "media_urls":   raw.get("media_urls", []),
        "doors":        raw.get("doors"),
        "seats":        raw.get("seats"),
        "dimensions_mm":raw.get("dimensions_mm"),
        "drive_type":   drive_en,
        "drive_type_ja":t_drive(drive_en),
        "grades":       raw.get("grades"),
        "engines":      raw.get("engines"),
        "colors":       raw.get("colors"),
        "catalog_url":  raw.get("catalog_url"),
        "full_model_ja":f"{MAKE_JA.get(raw.get('make_en'),raw.get('make_en'))} {raw.get('model_en')}",
        "updated_at":   datetime.utcnow().isoformat(timespec="seconds")+"Z",
    }
    return payload
