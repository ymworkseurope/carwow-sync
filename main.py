"""
main.py
--------

1.  urls.iter_model_urls()   …… モデル専用 URL を 1 本ずつ取得
2.  scrape.scrape_one()      …… 各 URL をスクレイプして dict へ
3.  transform.to_payload()   …… Supabase カラム形式に変換
4.  db.upsert()              …… cars テーブルへ UPSERT

途中で例外が出てもスキップして最後まで回ります。
"""

import sys, traceback
from tqdm import tqdm                       # 進捗バー
from urls      import iter_model_urls
from scrape    import scrape_one
from transform import to_payload

# Supabase へ登録する関数（あなたの既存モジュール）
try:
    from db import upsert                   # production 用
except ImportError:
    # --- スタブ（ローカルテスト用） -------------------------
    def upsert(payload):
        print("※ STUB UPSERT", payload["slug"])
    print("db.upsert が見つからないためスタブで実行しています", file=sys.stderr)
# -----------------------------------------------------------


def main() -> None:
    success = 0
    skipped = 0

    urls = list(iter_model_urls())          # イテレータ → リスト化
    print(f"Total target models: {len(urls)}")

    for url in tqdm(urls, desc="scrape"):
        try:
            raw      = scrape_one(url)      # dict (生データ)
            payload  = to_payload(raw)      # dict (DB 用)

            upsert(payload)                 # Supabase へ
            success += 1
            print("UPSERT", payload["slug"])

        except Exception as e:
            skipped += 1
            print("SKIP", url, "=>", repr(e), file=sys.stderr)
            traceback.print_exc()

    print(f"\nFinished: {success} upserted / {skipped} skipped")


if __name__ == "__main__":
    main()
