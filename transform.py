import slugify, datetime as dt
def to_payload(raw):
    make, model = raw["title"].split()[0:2]
    return {
        "slug"           : slugify.slugify(f"{make}-{model}"),
        "make_en"        : make,
        "model_en"       : model,
        "price_min_gbp"  : raw["price_gbp"] or None,
        "overview_en"    : raw["overview_en"],
        "body_type"      : raw["body_type"],
        "fuel"           : raw["fuel"],
        "spec_json"      : {"door_count": raw["door_count"]},
        "media_urls"     : raw["colours"],
        "updated_at"     : dt.datetime.utcnow().isoformat(timespec="seconds") + "Z"
    }
