#!/usr/bin/env python3
# model_scraper.py – 2025-08-30 full (画像スライダー優先 & 精度向上)

import re, json, time, random, requests, bs4
from urllib.parse import urljoin, urlparse
from typing import Dict, List

UA   = "Mozilla/5.0 (+https://github.com/ymworkseurope/carwow-sync 2025-08-30)"
HEAD = {"User-Agent": UA}

# ───────── helpers ─────────
def _get(url: str) -> requests.Response:
    r = requests.get(url, headers=HEAD, timeout=30)
    r.raise_for_status()
    return r

def _bs(url: str) -> bs4.BeautifulSoup:
    return bs4.BeautifulSoup(_get(url).text, "lxml")

def _sleep(): time.sleep(random.uniform(0.6,1.1))

# ───────── images ─────────
EXCLUDE_IMG = ("logo","icon","badge","sprite","favicon","headshot")

def _slider_imgs(doc:bs4.BeautifulSoup, limit:int=12) -> List[str]:
    """ページ最上部スライダー画像を srcset / src から抽出"""
    out=[]
    for img in doc.select(".media-slider__image[src], .media-slider__image[srcset]"):
        src = img.get("src") or ""
        if not src and img.has_attr("srcset"):
            src = img["srcset"].split()[0]
        if src and not any(k in src.lower() for k in EXCLUDE_IMG):
            if src.startswith("//"): src="https:"+src
            out.append(src)
        if len(out)>=limit: break
    return list(dict.fromkeys(out))  # uniq

def _fallback_imgs(doc:bs4.BeautifulSoup, base:str, limit:int=12)->List[str]:
    out=[]
    for img in doc.select("img[src]"):
        src=img["src"]
        if any(k in src.lower() for k in EXCLUDE_IMG): continue
        full=urljoin(base,src)
        if full.startswith("http") and full not in out: out.append(full)
        if len(out)>=limit: break
    return out

# ───────── private util ─────────
def _summary(doc:bs4.BeautifulSoup,label:str)->str|None:
    patt=re.compile(label,re.I)
    node=doc.select_one(f".summary-list__item:has(dt:-soup-contains('{label}')) dd")
    if node: return node.get_text(strip=True)
    # fallback text search
    for item in doc.select(".summary-list__item"):
        dt=item.select_one("dt")
        if dt and patt.search(dt.get_text(" ",strip=True)):
            dd=item.select_one("dd")
            if dd: return dd.get_text(strip=True)
    return None

def _dimensions(spec:bs4.BeautifulSoup)->str|None:
    # 1) SVG text
    t=[t.get_text(strip=True) for t in spec.select("svg text") if "mm" in t.text][:3]
    t=[re.sub(r"[^\d,]","",v) for v in t]
    if len(t)==3: return " / ".join(t)
    # 2) external dimensions block
    m=re.search(r"External dimensions[^0-9]*(\d[\d,]+\s*mm)[^\d]+(\d[\d,]+\s*mm)[^\d]+(\d[\d,]+\s*mm)", spec.get_text(" ",strip=True), re.I)
    if m: return " / ".join([m.group(i).replace(" ","") for i in (1,2,3)])
    return None

# ───────── main ─────────
def scrape(url:str)->Dict:
    doc=_bs(url); _sleep()

    # URL parts
    make_raw, model_raw = urlparse(url).path.strip("/").split("/")[:2]
    make_en  = make_raw.replace("-"," ").title()
    model_en = model_raw.replace("-"," ").title()
    slug     = f"{make_raw}-{model_raw}"

    # title & price
    title = doc.select_one("h1").get_text(" ",strip=True) if doc.select_one("h1") else f"{make_en} {model_en}"
    m=re.search(r"£([\d,]+)\D+£([\d,]+)", doc.get_text(" ",strip=True))
    pmin,pmax=(int(m[1].replace(",","")),int(m[2].replace(",",""))) if m else (None,None)

    # overview
    overview = (doc.select_one("em") or {}).get_text(strip=True) if doc.select_one("em") else ""

    # body type / fuel
    body_type=fuel=None
    glance=doc.select_one(".review-overview__at-a-glance-model")
    if glance:
        blocks=[b.get_text(strip=True) for b in glance.select("div")]
        for k,v in zip(blocks[::2],blocks[1::2]):
            if k.lower().startswith("body type"): body_type=[t.strip() for t in re.split(r",|/&",v) if t.strip()]
            if k.lower().startswith("available fuel"): fuel="要確認" if v.lower()=="chooser" else v

    # seats / doors / trans (upper page)
    doors=_summary(doc,"Number of doors")
    seats=_summary(doc,"Number of seats")
    trans=_summary(doc,"Transmission")

    # specs page
    dims=grades=engines=None
    spec_url=url.rstrip("/")+"/specifications"
    try:
        spec=_bs(spec_url); _sleep()
        if not seats: seats=_summary(spec,"Number of seats")
        if not doors: doors=_summary(spec,"Number of doors")
        if not trans: trans=_summary(spec,"Transmission")
        dims=_dimensions(spec)

        grades=[g.get_text(strip=True) for g in spec.select("span.trim-article__title-part-2")] or None
        engines=[]
        for tr in spec.select("table tr"):
            tds=[td.get_text(" ",strip=True) for td in tr.select("td")]
            if len(tds)==2 and re.search(r"(PS|hp|kW|kWh|capacity)",tds[1],re.I):
                engines.append(" – ".join(tds))
        if not engines: engines=None
    except Exception: pass

    # colours page
    colors=[]
    try:
        col=_bs(url.rstrip("/")+"/colours"); _sleep()
        for h4 in col.select(".model-hub__colour-details-title"):
            colors.append(h4.get_text(" ",strip=True).replace("  "," "))
    except Exception: pass

    # media
    media=_slider_imgs(doc)
    if not media: media=_fallback_imgs(doc,url)

    # numeric cast
    doors_i=int(doors) if doors and doors.isdigit() else None
    seats_i=int(seats) if seats and seats.isdigit() else None

    return {
        "slug":slug,
        "url":url,
        "title":title,
        "make_en":make_en,
        "model_en":model_en,
        "overview_en":overview,
        "body_type":body_type,
        "fuel":fuel,
        "price_min_gbp":pmin,
        "price_max_gbp":pmax,
        "spec_json":{
            "doors":doors,
            "seats":seats,
            "drive_type":trans,
            "dimensions_mm":dims
        },
        "media_urls":media,
        "doors":doors_i,
        "seats":seats_i,
        "dimensions_mm":dims,
        "drive_type":trans,
        "grades":grades,
        "engines":engines,
        "colors":colors or None,
        "catalog_url":url,
    }
