# model_scraper.py – 2025‑08‑25 full
return [p.strip() for p in parts if p.strip()]


# ---------- main scrape ----------


def scrape(url: str) -> Dict:
"""scrape model page & return dict"""
soup = _bs(url); _sleep()


path_parts = urlparse(url).path.strip("/").split("/")
if len(path_parts) < 2:
raise ValueError("invalid url")
make_raw, model_raw = path_parts[:2]


make_en = make_raw.replace("-", " ").title()
model_en = model_raw.replace("-", " ").title()
slug = f"{make_raw}-{model_raw}"


# title
title = soup.select_one("h1").get_text(" ", strip=True) if soup.select_one("h1") else f"{make_en} {model_en}"


# price
txt = soup.get_text(" ", strip=True)
m = re.search(r"£([\d,]+)\s*[–-]\s*£([\d,]+)", txt)
pmin, pmax = (m and int(m[1].replace(",", "")), m and int(m[2].replace(",", ""))) if m else (None, None)


# overview (<em> … )
ov = soup.select_one("em")
overview = ov.get_text(strip=True) if ov else ""


# body_type from mapping cache
body_map = _load_body_map(make_raw)
body_types = body_map.get(model_raw) # list or None


# fuel & body type fallback (既存テーブル内)
glance = soup.select_one(".review-overview__at-a-glance-model")
fuel = None
if glance:
blocks = [b.get_text(strip=True) for b in glance.select("div")]
for k, v in zip(blocks[::2], blocks[1::2]):
if k.startswith("Available fuel"):
fuel = "要確認" if v.lower() == "chooser" else v
elif not body_types and k.startswith("Body type"):
body_types = _split_body_types(v)


# doors / seats / drive_type
def _summary(label: str):
sel = soup.select_one(f".summary-list__item:has(dt:-soup-contains('{label}')) dd")
return sel.get_text(strip=True) if sel else None


doors = _summary("Number of doors")
seats = _summary("Number of seats")
drive_type = _summary("Transmission")


# dimensions (3,673 mm / 1,682 mm / 1,518 mm)
dim_m = re.search(r"(\d{1,4},\d{3}\s*mm\s*/\s*\d{1,4},\d{3}\s*mm\s*/\s*\d{1,4},\d{3}\s*mm)", soup.text)
dimensions = dim_m.group(1).replace(" ", "") if dim_m else None


# grades (<span class="trim-article__title-part-2">)
grades = [s.get_text(strip=True) for s in soup.select("span.trim-article__title-part-2")] or None


# engines from /specifications
engines = []
spec_url = url.rstrip("/") + "/specifications"
try:
spec_bs = _bs(spec_url); _sleep()
for tr in spec_bs.select("table tr"):
tds = [td.get_text(" ", strip=True) for td in tr.select("td")]
if len(tds) == 2 and re.search(r"(PS|hp|kW)", tds[1]):
engines.append(" ".join(tds))
except Exception:
pass
engines = engines or None


# spec_json (簡易)
spec_json = json.dumps({
"doors": doors,
"seats": seats,
"dimensions_mm": dimensions,
"drive_type": drive_type,
}, ensure_ascii=False)


media_urls = _clean_imgs(soup, url)


return {
"slug": slug,
"url": url,
"title": title,
"make_en": make_en,
"model_en": model_en,
"overview_en": overview,
"body_type": body_types,
"fuel": fuel,
"price_min_gbp": pmin,
"price_max_gbp": pmax,
"spec_json": spec_json,
"media_urls": media_urls,
"doors": int(doors) if doors and doors.isdigit() else None,
"seats": int(seats) if seats and seats.isdigit() else None,
"dimensions_mm": dimensions,
"drive_type": drive_type,
"grades": grades,
"engines": engines,
"catalog_url": url,
}
