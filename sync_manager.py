#!/usr/bin/env python3
"""
sync_manager.py
実行管理とデータベース・スプレッドシート同期モジュール
"""
import os
import sys
import json
import time
import argparse
from datetime import datetime
from typing import Dict, List, Optional
from gspread.utils import rowcol_to_a1

import requests
import gspread
from google.oauth2.service_account import Credentials

from carwow_scraper import CarwowScraper
from data_processor import DataProcessor, DataValidator

# ======================== Configuration ========================
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
GS_CREDS_JSON = os.getenv("GS_CREDS_JSON")
GS_SHEET_ID = os.getenv("GS_SHEET_ID")

# Google Sheets設定
SHEET_NAME = "system_cars"
SHEET_HEADERS = [
    "id", "slug", "make_en", "model_en", "make_ja", "model_ja",
    "trim_name", "engine", "body_type", "body_type_ja", "fuel", "fuel_ja",
    "transmission", "transmission_ja",
    "price_min_gbp", "price_max_gbp", "price_used_gbp",
    "price_min_jpy", "price_max_jpy", "price_used_jpy",
    "overview_en", "overview_ja",
    "doors", "seats", "power_bhp",
    "drive_type", "drive_type_ja", "dimensions_mm",
    "colors", "media_urls", "catalog_url",
    "full_model_ja", "updated_at", "spec_json"
]

# ======================== Database Manager ========================
class SupabaseManager:
    """Supabaseデータベース管理"""
    
    def __init__(self):
        self.url = SUPABASE_URL
        self.key = SUPABASE_KEY
        self.enabled = bool(self.url and self.key)
        
        if not self.enabled:
            print("Warning: Supabase credentials not configured")
    
    def upsert(self, payload: Dict) -> bool:
        """データをUPSERT"""
        if not self.enabled:
            return False
        
        try:
            headers = {
                'apikey': self.key,
                'Authorization': f'Bearer {self.key}',
                'Content-Type': 'application/json',
                'Prefer': 'resolution=merge-duplicates'
            }
            
            response = requests.post(
                f"{self.url}/rest/v1/system_cars",
                headers=headers,
                json=payload,
                timeout=30
            )
            
            if response.status_code in [200, 201]:
                return True
            else:
                print(f"Supabase error {response.status_code}: {response.text}")
                return False
                
        except Exception as e:
            print(f"Supabase upsert error: {e}")
            return False

# ======================== Sheets Manager ========================
class GoogleSheetsManager:
    """Google Sheets管理"""
    
    def __init__(self):
        self.worksheet = None
        self.enabled = False
        self.headers = []
        self._initialize()
    
    def _initialize(self):
        """Google Sheets接続を初期化"""
        if not (GS_CREDS_JSON and GS_SHEET_ID):
            print("Warning: Google Sheets credentials not configured")
            return
        
        try:
            credentials = Credentials.from_service_account_info(
                json.loads(GS_CREDS_JSON),
                scopes=["https://www.googleapis.com/auth/spreadsheets"]
            )
            
            client = gspread.authorize(credentials)
            spreadsheet = client.open_by_key(GS_SHEET_ID)
            
            try:
                self.worksheet = spreadsheet.worksheet(SHEET_NAME)
                self.headers = self.worksheet.row_values(1)
                if not self.headers:
                    self.headers = SHEET_HEADERS
                    self.worksheet.update([self.headers], "A1")
            except gspread.WorksheetNotFound:
                self.worksheet = spreadsheet.add_worksheet(
                    title=SHEET_NAME,
                    rows=1000,
                    cols=len(SHEET_HEADERS)
                )
                self.headers = SHEET_HEADERS
                self.worksheet.update([self.headers], "A1")
            
            if set(SHEET_HEADERS) - set(self.headers):
                self.headers = SHEET_HEADERS
                self.worksheet.update([self.headers], f"A1:{rowcol_to_a1(1, len(self.headers))}")
                print(f"Updated headers to include all required columns")
            
            self.enabled = True
            print(f"Connected to Google Sheets: {SHEET_NAME}")
            
        except Exception as e:
            print(f"Google Sheets initialization error: {e}")
            self.enabled = False
    
    def upsert(self, payload: Dict) -> bool:
        """データをUPSERT"""
        if not self.enabled:
            return False
        
        try:
            slug = payload.get('slug')
            trim_name = payload.get('trim_name', 'Standard')
            
            if not slug:
                return False
            
            slug_col = self.headers.index('slug') + 1 if 'slug' in self.headers else None
            trim_col = self.headers.index('trim_name') + 1 if 'trim_name' in self.headers else None
            
            row_num = None
            if slug_col and trim_col:
                cell = self.worksheet.find(slug, in_column=slug_col)
                if cell:
                    row_values = self.worksheet.row_values(cell.row)
                    if len(row_values) >= trim_col and row_values[trim_col-1] == trim_name:
                        row_num = cell.row
            
            if not row_num:
                row_num = len(self.worksheet.get_all_values()) + 1
            
            row_data = []
            for header in self.headers:
                value = payload.get(header)
                if header in ['media_urls', 'colors'] and isinstance(value, list):
                    value = ', '.join(value) if value else ''
                elif isinstance(value, (list, dict)):
                    value = json.dumps(value, ensure_ascii=False)
                elif value is None:
                    value = ""
                else:
                    value = str(value)
                row_data.append(value)
            
            self.worksheet.update(
                [row_data],
                f"A{row_num}:{rowcol_to_a1(row_num, len(self.headers))}",
                value_input_option='RAW'
            )
            
            return True
            
        except Exception as e:
            print(f"Sheets upsert error: {e}")
            return False

# ======================== Sync Manager ========================
class SyncManager:
    """メイン同期管理クラス"""
    
    def __init__(self):
        self.scraper = CarwowScraper()
        self.processor = DataProcessor()
        self.validator = DataValidator()
        self.supabase = SupabaseManager()
        self.sheets = GoogleSheetsManager()
        
        self.stats = {
            'total': 0,
            'success': 0,
            'failed': 0,
            'skipped': 0,
            'errors': []
        }
    
    def sync_all(self, makers: Optional[List[str]] = None, limit: Optional[int] = None):
        """全データを同期"""
        print("=" * 60)
        print("Starting Carwow Data Sync")
        print(f"Time: {datetime.now().isoformat()}")
        print("=" * 60)
        
        if makers is None:
            makers = self.scraper.get_all_makers()
        
        print(f"Processing {len(makers)} makers")
        
        for maker_idx, maker in enumerate(makers):
            print(f"\n[{maker_idx + 1}/{len(makers)}] Processing: {maker}")
            
            try:
                models = self.scraper.get_models_for_maker(maker)
                print(f"  Found {len(models)} models")
                
                for model_idx, model_slug in enumerate(models):
                    if limit and self.stats['total'] >= limit:
                        print("\nReached limit, stopping...")
                        return
                    
                    self.stats['total'] += 1
                    self._process_vehicle(model_slug, model_idx + 1, len(models))
                    
                    time.sleep(0.5)
                    
            except Exception as e:
                print(f"  Error processing maker {maker}: {e}")
                self.stats['errors'].append(f"Maker {maker}: {str(e)}")
        
        self._print_statistics()
    
    def sync_specific(self, slugs: List[str]):
        """特定の車両のみ同期"""
        print(f"Syncing {len(slugs)} specific vehicles")
        
        for idx, slug in enumerate(slugs):
            self.stats['total'] += 1
            self._process_vehicle(slug, idx + 1, len(slugs))
            time.sleep(0.5)
        
        self._print_statistics()
    
    def _process_vehicle(self, slug: str, current: int, total: int):
        """個別車両を処理"""
        try:
            print(f"  [{current}/{total}] {slug}...", end=" ")
            
            raw_data = self.scraper.scrape_vehicle(slug)
            payloads = self.processor.process_vehicle_data(raw_data)
            
            if not isinstance(payloads, list):
                payloads = [payloads]
            
            success = False
            for payload in payloads:
                is_valid, errors = self.validator.validate_payload(payload)
                if not is_valid:
                    print(f"INVALID ({payload.get('trim_name', '')}): {', '.join(errors)}")
                    self.stats['skipped'] += 1
                    continue
                
                db_success = self.supabase.upsert(payload)
                sheets_success = self.sheets.upsert(payload)
                
                if db_success or sheets_success:
                    success = True
            
            if success:
                print(f"OK ({len(payloads)} trims)")
                self.stats['success'] += 1
            else:
                print("FAILED")
                self.stats['failed'] += 1
                
        except Exception as e:
            print(f"ERROR: {str(e)[:50]}")
            self.stats['failed'] += 1
            self.stats['errors'].append(f"{slug}: {str(e)}")
    
    def _print_statistics(self):
        """統計情報を表示"""
        print("\n" + "=" * 60)
        print("Sync Statistics")
        print("=" * 60)
        print(f"Total processed: {self.stats['total']}")
        print(f"Success: {self.stats['success']}")
        print(f"Failed: {self.stats['failed']}")
        print(f"Skipped: {self.stats['skipped']}")
        
        if self.stats['errors']:
            print(f"\nErrors ({len(self.stats['errors'])} total):")
            for error in self.stats['errors']:
                print(f"  - {error}")
        
        if self.stats['total'] > 0:
            success_rate = (self.stats['success'] / self.stats['total']) * 100
            print(f"\nSuccess rate: {success_rate:.1f}%")

# ======================== CLI Interface ========================
def main():
    """メインエントリポイント"""
    parser = argparse.ArgumentParser(
        description="Carwow Data Sync Manager",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        '--makers', 
        nargs='+',
        help='Specific makers to process (e.g., --makers audi bmw)'
    )
    
    parser.add_argument(
        '--models',
        nargs='+',
        help='Specific model slugs to process (e.g., --models audi/a4 bmw/x5)'
    )
    
    parser.add_argument(
        '--limit',
        type=int,
        help='Limit number of vehicles to process'
    )
    
    args = parser.parse_args()
    
    manager = SyncManager()
    
    try:
        if args.models:
            manager.sync_specific(args.models)
        else:
            manager.sync_all(makers=args.makers, limit=args.limit)
    
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        manager._print_statistics()
    
    except Exception as e:
        print(f"\n\nFatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
