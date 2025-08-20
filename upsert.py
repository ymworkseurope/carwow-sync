import os, requests, backoff, json
URL = os.getenv("SUPABASE_URL")
KEY = os.getenv("SUPABASE_KEY")

@backoff.on_exception(backoff.expo, requests.HTTPError, max_time=60)
def upsert(item):
    r = requests.post(f"{URL}/rest/v1/cars?on_conflict=slug",
                      headers={
                        "apikey": KEY,
                        "Authorization": f"Bearer {KEY}",
                        "Prefer": "resolution=merge-duplicates"},
                      json=item, timeout=30)
    r.raise_for_status()
    print("UPSERT", item["slug"])
