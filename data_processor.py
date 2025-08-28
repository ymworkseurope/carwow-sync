#!/usr/bin/env python3
"""
data_processor.py
データ変換、翻訳、価格換算などの処理モジュール
"""
from datetime import datetime
}
trim_payloads.append(payload)
return trim_payloads




# --------------------------------------------------
# sync_manager.py (Google Sheets / Supabase シンプル同期)
# --------------------------------------------------


import os
from typing import List


import requests




class SupabaseManager:
"""Minimal Supabase upsert."""


def __init__(self) -> None:
self.url = os.getenv("SUPABASE_URL")
self.key = os.getenv("SUPABASE_KEY")
self.enabled = bool(self.url and self.key)
if not self.enabled:
print("[WARN] Supabase disabled: credentials missing")


def upsert(self, table: str, payload: Dict) -> bool:
if not self.enabled:
return False
headers = {
"apikey": self.key,
"Authorization": f"Bearer {self.key}",
"Content-Type": "application/json",
"Prefer": "resolution=merge-duplicates",
}
resp = requests.post(f"{self.url}/rest/v1/{table}", headers=headers, json=payload, timeout=30)
if resp.status_code in (200, 201):
return True
print(f"[Supabase] {resp.status_code}: {resp.text[:80]}")
return False




class SyncManager:
"""High‑level orchestrator."""


def __init__(self) -> None:
self.scraper = CarwowScraper()
self.processor = DataProcessor()
self.supabase = SupabaseManager()


def run(self, makers: Optional[List[str]] = None, limit: Optional[int] = None) -> None:
makers = makers or self.scraper.get_all_makers()
processed = 0
for maker in makers:
for slug in self.scraper.get_models_for_maker(maker):
if limit and processed >= limit:
return
try:
vehicle = self.scraper.scrape_vehicle(slug)
for payload in self.processor.process_vehicle(vehicle):
self.supabase.upsert("system_cars", payload)
processed += 1
time.sleep(0.2) # gentle rate‑limit
except Exception as exc:
print(f"[ERROR] {slug}: {exc}")
