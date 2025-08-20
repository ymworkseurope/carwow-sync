# urls.py
import requests, json, datetime as dt, pathlib, bs4, re

SITEMAP_INDEX = "https://www.carwow.co.uk/sitemap.xml"
OUT           = pathlib.Path("data/url_index.json")
OUT.parent.mkdir(parents=True, exist_ok=True)

XML = lambda url: bs4.BeautifulSoup(requests.get(url, timeout=20).content, "xml")

# 2 段階：サイトマップ index → 各サブサイトマップ
def collect_urls():
    index = XML(SITEMAP_INDEX)
    submaps = [loc.text for loc in index.find_all("loc")
               if loc.text.endswith(".xml")]               # *.xml だけを取り出す

    model_urls, makes = set(), set()
    rx_model = re.compile(r"^https://www\.carwow\.co\.uk/([^/]+)/([^/.]+)/?$")

    for sm in submaps:
        sm_xml = XML(sm)
        for loc in sm_xml.find_all("loc"):
            u = loc.text.rstrip('/')
            m = rx_model.match(u)
            if m:                                          # make/model 形式だけ
                model_urls.add(u)
                makes.add(m.group(1))

    return sorted(makes), sorted(model_urls)

def main():
    makes, models = collect_urls()
    OUT.write_text(json.dumps({
        "fetched": dt.datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "makes"  : makes,
        "models" : models
    }, indent=2))

if __name__ == "__main__":
    main()
