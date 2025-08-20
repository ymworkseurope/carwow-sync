# transform.py
import slugify, datetime as dt, re

def to_payload(raw: dict) -> dict:
    make = raw.get("make_en") or ""
    model= raw.get("model_en") or ""

    # title から保険で取り直し
    if raw.get("title"):
        m = re.match(r"([A-Za-z]+)\s+([A-Za-z0-9\-]+)", raw["title"])
        if m:
            make = make or m.group(1)
            model= model or m.group(2)

    slug = slugify.slugify(f"{make}-{model}") if make and model else \
           slugify.slugify(raw.get("title","")) or raw.get("model_en","unknown")

    return {
        "slug"          : slug,
        "make_en"       : make,
        "model_en"      : model,
        "price_min_gbp" : raw.get("price_min_gbp"),
        "price_max_gbp" : raw.get("price_max_gbp"),
        "price_min_jpy" : raw.get("price_min_jpy"),
        "price_max_jpy" : raw.get("price_max_jpy"),
        "overview_en"   : raw.get("overview_en",""),
        "body_type"     : raw.get("body_type"),
        "fuel"          : raw.get("fuel"),
        "spec_json"     : raw.get("spec_json","{}"),
        "media_urls"    : raw.get("media_urls",[]) + raw.get("colours",[]),
        "updated_at"    : dt.datetime.utcnow().isoformat(timespec="seconds")+"Z"
    }
