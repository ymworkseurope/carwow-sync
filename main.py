import json, pathlib, tqdm
from urls import OUT as URL_JSON
from scrape import scrape_one
from transform import to_payload
from upsert import upsert

def main():
    urls = json.loads(pathlib.Path(URL_JSON).read_text())["models"]
    for url in tqdm.tqdm(urls, desc="scrape"):
        raw = scrape_one(url)
        pl  = to_payload(raw)
        upsert(pl)

if __name__ == "__main__":
    main()
