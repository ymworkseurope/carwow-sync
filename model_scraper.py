#!/usr/bin/env python3
# model_scraper.py – 2025-08-28 robust summary parser

import re, json, time, random, requests, bs4
from urllib.parse import urljoin, urlparse
from typing import Dict, List, Optional

UA = "Mozilla/5.0 (+https://github.com/ymworkseurope/carwow-sync 2025-08-28)"
HEAD = {"User-Agent": UA}

def _get(url): r = requests.get(url, headers=HEAD, timeout=30); r.raise_for_status(); return r
def _bs(url):  return bs4.BeautifulSoup(_get(url).text, "lxml")
def _maybe_bs(url): 
    try: return _bs(url)
    except requests.HTTPError as e:
        if e.response.status_code == 404: return None
        raise
def _sleep(): time.sleep(random.uniform(0.6, 1.1))

EXCL_IMG = ("logo","icon","badge","sprite","favicon")
def _imgs(doc, base, lim=12):
    out=[]
    for img in doc.select("img[src]"):
        src=img["src"]
        if any(k in src.lower() for k in EXCL_IMG): continue
        u=urljoin(base,src)
        if u.startswith("http") and u not in out: out.append(u)
        if len(out)>=lim: break
    return out

# ──────────────────────────────────────────
LABEL_RE = {
    "doors" : re.compile(r"\bdoor"),
    "seats" : re.compile(r"\bseat"),
    "drive" : re.compile(r"\bdrive|drivetrain|transmission"),
    "dims"  : re.compile(r"dimension|length|size"),
}

def _extract_from_blocks(blocks, doors, seats, dims, drive):
    for label, val in blocks:
        l = label.lower()
        if LABEL_RE["doors"].search(l):
            m=re.search(r"(\d+)", val); doors = m.group(1) if m else doors
        elif LABEL_RE["seats"].search(l):
            m=re.search(r"(\d+)", val); seats = m.group(1) if m else seats
        elif LABEL_RE["drive"].search(l):
            drive = val
        elif LABEL_RE["dims"].search(l) and "mm" in val:
            dims = val
    return doors, seats, dims, drive

# ──────────────────────────────────────────
def scrape(url:str)->Dict:
    doc=_bs(url); _sleep()
    make_raw, model_raw = urlparse(url).path.strip("/").split("/")[:2]
    make_en = make_raw.replace("-"," ").title()
    model_en= model_raw.replace("-"," ").title()
    slug=f"{make_raw}-{model_raw}"

    h1=doc.select_one("h1")
    title=h1.get_text(" ",strip=True) if h1 else f"{make_en} {model_en}"

    price_m = re.search(r"£([\d,]+)\s*[–-]\s*£([\d,]+)", doc.text)
    if price_m:
        pmin,pmax=(int(price_m[1].replace(",","")), int(price_m[2].replace(",","")))
    else:
        one=re.search(r"From\s+£([\d,]+)", doc.text)
        pmin=pmax=int(one[1].replace(",","")) if one else None

    overview_el=doc.select_one("em"); overview=overview_el.get_text(strip=True) if overview_el else ""

    body_type=fuel=None
    glance=doc.select_one(".review-overview__at-a-glance-model")
    if glance:
        kv=[b.get_text(strip=True) for b in glance.select("div")]
        for k,v in zip(kv[::2], kv[1::2]):
            if "Body type" in k: body_type=[t.strip() for t in re.split(r",|/&",v) if t.strip()]
            if "Available fuel" in k: fuel="要確認" if v.lower()=="chooser" else v

    doors=seats=dims=drive_type=None

    # --- main page summary-list blocks ---
    blocks=[]
    for item in doc.select(".summary-list__item"):
        dt=item.select_one("dt"); dd=item.select_one("dd")
        if dt and dd:
            blocks.append((dt.get_text(" ",strip=True), dd.get_text(" ",strip=True)))
    doors,seats,dims,drive_type=_extract_from_blocks(blocks,doors,seats,dims,drive_type)

    # --- /specifications ---
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

        for label,val in rows:
            if re.search(r"(ps|hp|kw)", val.lower()):
                engines.append(f"{label} {val}")
        doors,seats,dims,drive_type=_extract_from_blocks(rows,doors,seats,dims,drive_type)

        # dimensions in svg title
        if not dims:
            svg=spec.select_one("svg title")
            if svg and "mm" in svg.text:
                parts=re.findall(r"\d[\d,]+\s*mm", svg.text)
                if len(parts)==3: dims=" / ".join(parts)

    # --- /colours ---
    colours=[]
    col=_maybe_bs(url.rstrip("/")+"/colours")
    if col:
        _sleep()
        colours=sorted({c.get_text(strip=True) for c in col.select("figcaption, .colour-picker__name")})

    return {
        "slug":slug,"url":url,"title":title,
        "make_en":make_en,"model_en":model_en,
        "overview_en":overview,
        "body_type":body_type,"fuel":fuel,
        "price_min_gbp":pmin,"price_max_gbp":pmax,
        "spec_json":json.dumps({"doors":doors,"seats":seats,"dimensions_mm":dims,"drive_type":drive_type}, ensure_ascii=False),
        "media_urls":_imgs(doc,url),
        "doors":int(doors) if doors and doors.isdigit() else None,
        "seats":int(seats) if seats and seats.isdigit() else None,
        "dimensions_mm":dims,
        "drive_type":drive_type,
        "grades":grades or None,
        "engines":engines or None,
        "colors":colours or None,
        "catalog_url":url,
    }
