# scrape.py  ― Carwow 1 台分をスクレイプして dict で返す
import time, random, re, requests, bs4

UA   = "Mozilla/5.0 (+https://github.com/your/carwow-sync)"
HEAD = {"User-Agent": UA}
delay = lambda: time.sleep(random.uniform(0.8, 1.3))


def fetch(url, allow_404=False):
    """HTTP GET→ BeautifulSoup（404 を許容する場合は None を返す）"""
    delay()
    r = requests.get(url, headers=HEAD, timeout=30)
    if allow_404 and r.status_code == 404:
        return None
    r.raise_for_status()
    return bs4.BeautifulSoup(r.text, "lxml")


# ---------- ① メインページ ----------
def parse_main(s: bs4.BeautifulSoup) -> dict:
    """モデル TOP ページから基本情報を抽出"""
    title = s.select_one("h1")
    price = s.select_one("[data-test='price']")

    # 価格 ￡xx,xxx 形式 → 数字だけ
    price_num = None
    if price:
        m = re.search(r"([\d,]+)", price.text)
        if m:
            price_num = int(m.group(1).replace(",", ""))

    return {
        "title": title.text.strip() if title else "",
        "price_min_gbp": price_num,
        "overview_en": s.select_one("meta[name='description']")["content"],
    }


# ---------- ② スペックページ ----------
def parse_specs(s: bs4.BeautifulSoup) -> dict:
    """specifications ページのスペックテーブル"""
    if s is None:
        return {}
    rows = {dt.text.strip(): dd.text.strip()
            for dt, dd in zip(s.select("dt"), s.select("dd"))}

    return {
        "door_count": rows.get("Doors"),
        "fuel": rows.get("Fuel type"),
        "body_type": rows.get("Body style"),
    }


# ---------- ③ カラーページ ----------
def parse_colors(s: bs4.BeautifulSoup) -> list[str]:
    """colours ページの画像 alt → 色名リスト"""
    if s is None:
        return []
    return [img["alt"].strip()
            for img in s.select("img[alt][loading='lazy']")]


# ---------- ④ 1 モデル統合 ----------
def scrape_one(model_url: str) -> dict:
    """TOP /specifications /colours の 3 ページを統合して返す"""
    s_top = fetch(model_url)
    s_spec = fetch(model_url + "/specifications", allow_404=True)
    s_col = fetch(model_url + "/colours", allow_404=True)

    data = {}
    data.update(parse_main(s_top))
    data.update(parse_specs(s_spec))
    data["colours"] = parse_colors(s_col)

    return data
