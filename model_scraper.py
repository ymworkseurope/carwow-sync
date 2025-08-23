#!/usr/bin/env python3
# model_scraper.py – 2025-08-30 seats / dimensions / colours 完全対応版
import re, json, time, random, requests, bs4
from urllib.parse import urljoin, urlparse
from typing import Dict, List, Optional

UA   = "Mozilla/5.0 (+https://github.com/ymworkseurope/carwow-sync 2025-08-30)"
HEAD = {"User-Agent": UA}

def _get(url): r=requests.get(url, headers=HEAD, timeout=30); r.raise_for_status(); return r
def _bs(url):  return bs4.BeautifulSoup(_get(url).text, "lxml")
def _maybe_bs(url):
    try: return _bs(url)
    except requests.HTTPError as e:
        if e.response.status_code==404: return None
        raise
def _sleep(): time.sleep(random.uniform(0.6,1.1))

EXCL_IMG=("logo","icon","badge","sprite","favicon")
def _imgs(doc, base, lim=12):
    out=[]
    for img in doc.select("img[src]"):
        src=img["src"]
        if any(k in src.lower() for k in EXCL_IMG): continue
        u=urljoin(base,src)
        if u.startswith("http") and u not in out: out.append(u)
        if len(out)>=lim: break
    return out

LABEL_RE = {
    "doors": re.compile(r"\bdoor"),
    "seats": re.compile(r"\bseat\b"),
    "drive": re.compile(r"\bdrive|drivetrain|transmission"),
    "dims" : re.compile(r"dimension|length|size"),
}

def _scan_pairs(pairs, doors, seats, dims, drive):
    for lab,val in pairs:
        l=lab.lower()
        if LABEL_RE["doors"].search(l):
            m=re.search(r"(\d+)", val); doors=m.group(1) if m else doors
        elif LABEL_RE["seats"].search(l):
            m=re.search(r"\b(\d{1,2})\b", val)      # 1–2 桁のみ
            if m and int(m.group(1))<=9: seats=m.group(1)
        elif LABEL_RE["drive"].search(l):
            drive=val
        elif LABEL_RE["dims"].search(l) and "mm" in val:
            dims=val
    return doors,seats,dims,drive

# ───────────────────────────────
def scrape(url:str)->Dict:
    doc=_bs(url); _sleep()

    make_raw, model_raw = urlparse(url).path.strip("/").split("/")[:2]
    make_en = make_raw.replace("-"," ").title()
    model_en= model_raw.replace("-"," ").title()
    slug=f"{make_raw}-{model_raw}"

    title_el=doc.select_one("h1")
    title=title_el.get_text(" ",strip=True) if title_el else f"{make_en} {model_en}"

    pmin=pmax=None
    rng=re.search(r"£([\d,]+)\s*[–-]\s*£([\d,]+)", doc.text)
    if rng:
        pmin,pmax=(int(rng[1].replace(",","")), int(rng[2].replace(",","")))
    else:
        one=re.search(r"From\s+£([\d,]+)", doc.text)
        if one: pmin=pmax=int(one[1].replace(",",""))

    overview=doc.select_one("em")
    overview=overview.get_text(strip=True) if overview else ""

    body_type=fuel=None
    glance=doc.select_one(".review-overview__at-a-glance-model")
    if glance:
        kv=[b.get_text(strip=True) for b in glance.select("div")]
        for k,v in zip(kv[::2], kv[1::2]):
            if "Body type" in k: body_type=[t.strip() for t in re.split(r",|/&", v) if t.strip()]
            if "Available fuel" in k: fuel="要確認" if v.lower()=="chooser" else v

    doors=seats=dims=drive_type=None

    # ① TOP summary-list
    pairs=[(dt.get_text(" ",strip=True), dd.get_text(" ",strip=True))
           for item in doc.select(".summary-list__item")
           for dt,dd in [(item.select_one("dt"), item.select_one("dd"))] if dt and dd]
    doors,seats,dims,drive_type=_scan_pairs(pairs,doors,seats,dims,drive_type)

    # ② specifications
    grades,engines=[],[]
    spec=_maybe_bs(url.rstrip("/")+"/specifications")
    if spec:
        _sleep()
        grades=[g.get_text(strip=True) for g in spec.select("span.trim-article__title-part-2")]
        rows=[]
        rows += [(th.get_text(" ",strip=True), td.get_text(" ",strip=True))
                 for tr in spec.select("table tr")
                 for th,td in [tr.select_one("th"), tr.select_one("td")] if th and td]
        rows += [(dt.get_text(" ",strip=True), dd.get_text(" ",strip=True))
                 for dt,dd in [(i.select_one("dt"), i.select_one("dd"))
                               for i in spec.select(".summary-list__item")] if dt and dd]
        for lab,val in rows:
            if re.search(r"(ps|hp|kw)", val.lower()):
                engines.append(f"{lab} {val}")
        doors,seats,dims,drive_type=_scan_pairs(rows,doors,seats,dims,drive_type)

        # dimensions in SVG (spec page)
        if not dims:
            svg=spec.select_one("svg title")
            if svg:
                nums=re.findall(r"\d[\d,]+\s*mm", svg.text)
                if len(nums)==3: dims=" / ".join(nums)

        # External dimensions block
        if not dims:
            ext=re.search(r"External dimensions\s+([\d,\smm]+)", spec.text)
            if ext:
                nums=re.findall(r"[\d,]+\s*mm", ext.group(1))
                if len(nums)==3: dims=" / ".join(nums)

    # ③ SVG dimensions on TOP (一部モデルは TOP 側のみ)
    if not dims:
        svg=doc.select_one("svg title")
        if svg:
            nums=re.findall(r"\d[\d,]+\s*mm", svg.text)
            if len(nums)==3: dims=" / ".join(nums)

    # ④ colours
    colours=set()
    #   ④-1 TOP
    colours.update([c.get_text(" ",strip=True).split(" - ",1)[-1]
                    for c in doc.select("h4.model-hub__colour-details-title")])
    #   ④-2 /colours ページ (各 DOM パターン)
    col=_maybe_bs(url.rstrip("/")+"/colours")
    if col:
        _sleep()
        colours.update([c.get_text(" ",strip=True).split(" - ",1)[-1]
                        for c in col.select("h4.model-hub__colour-details-title,"
                                            "figcaption,"
                                            ".colour-picker__name")])

    return {
        "slug":slug,"url":url,"title":title,
        "make_en":make_en,"model_en":model_en,
        "overview_en":overview,
        "body_type":body_type,"fuel":fuel,
        "price_min_gbp":pmin,"price_max_gbp":pmax,
        # ★ spec_json は dict で返す（transform 側でそのまま送る）
        "spec_json":{"doors":doors,"seats":seats,"dimensions_mm":dims,"drive_type":drive_type},
        "media_urls":_imgs(doc,url),
        "doors":int(doors) if doors and doors.isdigit() else None,
        "seats":int(seats) if seats and seats.isdigit() else None,
        "dimensions_mm":dims,
        "drive_type":drive_type,
        "grades":grades or None,
        "engines":engines or None,
        "colors":sorted(colours) or None,
        "catalog_url":url,
    }
