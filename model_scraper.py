#!/usr/bin/env python3
return r


def _bs(url: str):
return bs4.BeautifulSoup(_get(url).text, "lxml")


def _sleep():
time.sleep(random.uniform(0.6, 1.1))


EXCLUDE_IMG = ("logo", "icon", "badge", "sprite", "favicon")


def _clean_imgs(doc: bs4.BeautifulSoup, base: str, limit=12) -> List[str]:
out: List[str] = []
for img in doc.select("img[src]"):
src = img["src"]
if any(k in src.lower() for k in EXCLUDE_IMG):
continue
full = urljoin(base, src)
if full.startswith("http") and full not in out:
out.append(full)
if len(out) >= limit:
break
return out


# ---------- main ----------


def scrape(url: str) -> Dict:
doc = _bs(url); _sleep()


make_raw, model_raw = urlparse(url).path.strip("/").split("/")[:2]
make_en = make_raw.replace("-", " ").title()
model_en = model_raw.replace("-", " ").title()
slug = f"{make_raw}-{model_raw}"


# title / price
title = doc.select_one("h1").get_text(" ", strip=True) if doc.select_one("h1") else f"{make_en} {model_en}"
price_m = re.search(r"£([\d,]+)\D+£([\d,]+)", doc.text)
pmin, pmax = (int(price_m[1].replace(",", "")), int(price_m[2].replace(",", ""))) if price_m else (None, None)


# overview (<em>)
overview = doc.select_one("em").get_text(strip=True) if doc.select_one("em") else ""


# body_type / fuel (glance box)
body_type = fuel = None
glance = doc.select_one(".review-overview__at-a-glance-model")
if glance:
blocks = [b.get_text(strip=True) for b in glance.select("div")]
for k, v in zip(blocks[::2], blocks[1::2]):
if k.startswith("Body type"):
body_type = [t.strip() for t in re.split(r",|/&", v) if t.strip()]
if k.startswith("Available fuel"):
fuel = "要確認" if v.lower() == "chooser" else v


# --- spec items ---
def _summary(label: str):
n = doc.select_one(f".summary-list__item:has(dt:-soup-contains('{label}')) dd")
return n.get_text(strip=True) if n else None


doors = _summary("Number of doors")
seats = _summary("Number of seats")
trans = _summary("Transmission")


dim_m = re.search(r"([\d,]+\s*mm)\s*/\s*([\d,]+\s*mm)\s*/\s*([\d,]+\s*mm)", doc.text)
dims = " / ".join(dim_m.groups()) if dim_m else None


# grades / engines
grades, engines = [], []
try:
spec = _bs(url.rstrip("/") + "/specifications"); _sleep()
grades = [s.get_text(strip=True) for s in spec.select("span.trim-article__title-part-2")] or []
for tr in spec.select("table tr"):
tds = [td.get_text(" ", strip=True) for td in tr.select("td")]
if len(tds) == 2 and re.search(r"(PS|hp|kW)", tds[1]):
engines.append(" ".join(tds))
except Exception:
pass


return {
"slug": slug,
"url": url,
"title": title,
"make_en": make_en,
"model_en": model_en,
"overview_en": overview,
"body_type": body_type,
"fuel": fuel,
"price_min_gbp": pmin,
"price_max_gbp": pmax,
"spec_json": json.dumps({"doors": doors, "seats": seats, "dimensions_mm": dims, "drive_type": trans}, ensure_ascii=False),
"media_urls": _clean_imgs(doc, url),
"doors": int(doors) if doors and doors.isdigit() else None,
"seats": int(seats) if seats and seats.isdigit() else None,
"dimensions_mm": dims,
"drive_type": trans,
"grades": grades or None,
"engines": engines or None,
"catalog_url": url,
}
