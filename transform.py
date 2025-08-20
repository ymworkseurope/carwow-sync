# transform.py
import slugify, datetime as dt, re

def to_payload(raw: dict) -> dict:
    """
    scrape_one() が返した raw dict を Supabase cars テーブル用に整形
    """
    # タイトル先頭2語を make / model として取得（例: "Ford Mustang GT" → Ford / Mustang）
    make, model = re.match(r"(\w+)\s+(\w+)", raw["title"]).groups()

    return {
        # ──────────────────────────── 固定カラム
        "slug"          : slugify.slugify(f"{make}-{model}"),
        "make_en"       : make,
        "model_en"      : model,
        "overview_en"   : raw.get("overview_en", ""),

        # ──────────────────────────── 価格
        # scrape.py では price_min_gbp のみ抽出済み。最大値は無いので None
        "price_min_gbp" : raw.get("price_min_gbp"),
        "price_max_gbp" : None,

        # ──────────────────────────── スペック
        "body_type"     : raw.get("body_type"),
        "fuel"          : raw.get("fuel"),

        # 数値であるべきフィールドは None ならそのまま
        "spec_json"     : {
            "door_count": raw.get("door_count")
        },

        # ──────────────────────────── 画像 URL
        "media_urls"    : raw.get("colours", []),

        # 更新日時（UTC）
        "updated_at"    : dt.datetime.utcnow()
                          .isoformat(timespec="seconds") + "Z"
    }
