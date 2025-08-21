# transform.py
# rev: 2025-08-23 T10:30Z
import json, datetime as dt, os, re, requests, backoff, slugify

DEEPL_KEY = os.getenv("DEEPL_AUTH_KEY")   # 無い場合は翻訳スキップ

@backoff.on_exception(backoff.expo,
                      (requests.RequestException,), max_tries=3, jitter=None)
def _deepl(txt: str, src="EN", tgt="JA") -> str:
    url="https://api-free.deepl.com/v2/translate"
    r=requests.post(url,data={"auth_key":DEEPL_KEY,"text":txt,"source_lang":src,"target_lang":tgt},timeout=30)
    r.raise_for_status()
    return r.json()["translations"][0]["text"]

def to_payload(row: dict)->dict:
    make=model=""
    if row.get("title"):
        m=re.match(r"([A-Za-z]+)\s+([A-Za-z0-9\-]+)",row["title"])
        if m:
            make,model=m.group(1),m.group(2)
    slug = slugify.slugify(f"{make}-{model}") if make and model else row["slug"]
    # ja 翻訳
    model_ja = _deepl(row["model_en"]) if (DEEPL_KEY and row.get("model_en")) else None
    body_ja  = _deepl(row["body_type"])    if (DEEPL_KEY and row.get("body_type")) else None
    fuel_ja  = _deepl(row["fuel"])         if (DEEPL_KEY and row.get("fuel")) else None

    return {
        "slug"          : slug,
        "make_en"       : make,
        "model_en"      : row.get("model_en"),
        "model_ja"      : model_ja,
        "body_type"     : row.get("body_type"),
        "body_type_ja"  : body_ja,
        "fuel"          : row.get("fuel"),
        "fuel_ja"       : fuel_ja,
        "price_min_gbp" : row.get("price_min_gbp"),
        "price_max_gbp" : row.get("price_max_gbp"),
        "media_urls"    : row.get("media_urls",[]),
        "updated_at"    : dt.datetime.utcnow().isoformat(timespec="seconds")+"Z"
    }

if __name__=="__main__":
    import sys, csv
    rows=[]
    with open(sys.argv[1],"r",encoding="utf-8") as f:
        for j in f: rows.append(to_payload(json.loads(j)))
    out="cleaned.csv"
    with open(out,"w",newline="",encoding="utf-8") as fw:
        w=csv.DictWriter(fw,fieldnames=rows[0].keys())
        w.writeheader(); w.writerows(rows)
    print("Saved →",out)
