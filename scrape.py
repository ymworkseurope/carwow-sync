# scrape.py
# rev: 2025-08-23 T10:30Z
import re, json, sys, time, random
from typing import List, Dict
from tqdm import tqdm
from urllib.parse import urlparse
import requests, bs4, backoff
from model_scraper import scrape as scrape_one     # ← ここだけで済む

UA   = ("Mozilla/5.0 (+https://github.com/ymworkseurope/"
        "carwow-sync 2025-08-23)")
HEAD = {"User-Agent": UA}

@backoff.on_exception(backoff.expo,
                      (requests.RequestException,), max_tries=5, jitter=None)
def _get(url): r=requests.get(url,headers=HEAD,timeout=30); r.raise_for_status(); return r

def _bs(url): return bs4.BeautifulSoup(_get(url).text,"lxml")

def _sleep(): time.sleep(random.uniform(0.6,1.1))

# ----- 1) 全モデル URL 取得 -----
def iter_model_urls()->List[str]:
    xml=_get("https://www.carwow.co.uk/sitemap.xml").text
    locs=re.findall(r"<loc>(https://[^<]+)</loc>",xml)
    models=[]
    for loc in locs:
        if not loc.endswith(".xml"): continue
        sub=_get(loc).text
        models+=re.findall(r"<loc>(https://www\.carwow\.co\.uk/[^<]+)</loc>",sub)
    # 2階層 (/make/model) で blog, used-car, news を含まない URL だけ取る
    good=[u for u in models
          if re.match(r"https://www\.carwow\.co\.uk/[a-z0-9-]+/[a-z0-9-]+/?$",u)
          and not re.search(r"(blog|used|news)",u)]
    return sorted(set(good))

# ----- 2) 走らせるだけ -----
if __name__=="__main__":
    urls=iter_model_urls()
    print("Targets:",len(urls))
    out=[]
    for u in tqdm(urls,desc="scrape"):
        _sleep()
        try:
            out.append(scrape_one(u))
        except Exception as e:
            print("[ERR]",u,e,file=sys.stderr)
    with open("raw.jsonl","w",encoding="utf-8") as f:
        for row in out: f.write(json.dumps(row,ensure_ascii=False)+"\n")
    print("Saved → raw.jsonl")
