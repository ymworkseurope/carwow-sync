import requests, lxml.etree as ET, re, json, pathlib, datetime as dt
SITEMAP = "https://www.carwow.co.uk/sitemap.xml"
OUT     = pathlib.Path("data/url_index.json")
OUT.parent.mkdir(parents=True, exist_ok=True)

def main():
    xml  = ET.fromstring(requests.get(SITEMAP, timeout=20).content)
    locs = [loc.text for loc in xml.findall(".//{*}loc")]
    makes  = sorted(set(re.findall(r"carwow\.co\.uk/([^/]+)/?$", u)[0]
                        for u in locs if re.match(r".+/(?!.*-)\w+$", u)))
    models = [u for u in locs if re.search(r"/[^/]+/[^/]+/?$", u)]
    OUT.write_text(json.dumps({
        "fetched": dt.datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "makes"  : makes,
        "models" : models }, indent=2))
if __name__ == "__main__": main()
