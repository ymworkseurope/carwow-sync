#!/usr/bin/env python3
"""
sync_manager.py
実行管理とデータベース・スプレッドシート同期モジュール - 修正版
"""
import os
import sys
import json
import time
import argparse
import traceback
from datetime import datetime
from typing import Dict, List, Optional, Any
from pathlib import Path

import requests
import gspread
from google.oauth2.service_account import Credentials

# 他のモジュールをインポート
try:
    from carwow_scraper import CarwowScraper
    from data_processor import DataProcessor, DataValidator
except ImportError as e:
    print(f"Error importing modules: {e}")
    print("Make sure carwow_scraper.py and data_processor.py are in the same directory")
    sys.exit(1)

# ======================== Configuration ========================
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
GS_CREDS_JSON = os.getenv("GS_CREDS_JSON")
GS_SHEET_ID = os.getenv("GS_SHEET_ID")

# Google Sheets設定（trim_name削除）
SHEET_NAME = "system_cars"
SHEET_HEADERS = [
    "id", "slug", "make_en", "model_en", "make_ja", "model_ja",
    "grade", "engine", "body_type", "body_type_ja", "fuel", "fuel_ja",
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
            
            # updated_atがdatetimeオブジェクトの場合は文字列に変換
            if 'updated_at' in payload:
                if isinstance(payload['updated_at'], datetime):
                    payload['updated_at'] = payload['updated_at'].isoformat() + 'Z'
            
            # JSONシリアライズ可能かテスト
            try:
                json.dumps(payload, ensure_ascii=False)
            except TypeError as e:
                print(f"JSON serialization error: {e}")
                return False
            
            # 正しいテーブル名を使用
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
    
    def _get_column_letter(self, col_num: int) -> str:
        """列番号を列文字に変換"""
        letters = ''
        while col_num > 0:
            col_num -= 1
            letters = chr(65 + (col_num % 26)) + letters
            col_num //= 26
        return letters
    
    def _initialize(self):
        """Google Sheets接続を初期化"""
        if not (GS_CREDS_JSON and GS_SHEET_ID):
            print("Warning: Google Sheets credentials not configured")
            return
        
        try:
            # 認証情報ファイル作成
            creds_path = Path("secrets/gs_creds.json")
            creds_path.parent.mkdir(exist_ok=True)
            
            if not creds_path.exists():
                creds_path.write_text(GS_CREDS_JSON)
            
            # 認証と接続
            scopes = [
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"
            ]
            
            credentials = Credentials.from_service_account_file(
                str(creds_path), 
                scopes=scopes
            )
            
            client = gspread.authorize(credentials)
            spreadsheet = client.open_by_key(GS_SHEET_ID)
            
            # ワークシート取得または作成
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
            
            # 必要な列を追加
            self._ensure_required_columns()
            
            self.enabled = True
            print(f"Connected to Google Sheets: {SHEET_NAME}")
            
        except Exception as e:
            print(f"Google Sheets initialization error: {e}")
            traceback.print_exc()
            self.enabled = False
    
    def _ensure_required_columns(self):
        """必要な列を追加"""
        if not self.enabled:
            return
        
        try:
            current_headers = self.worksheet.row_values(1)
            
            missing_columns = []
            for required_col in SHEET_HEADERS:
                if required_col not in current_headers:
                    missing_columns.append(required_col)
            
            if missing_columns:
                new_headers = current_headers + missing_columns
                
                if len(new_headers) > self.worksheet.col_count:
                    self.worksheet.resize(cols=len(new_headers))
                
                self.worksheet.update([new_headers], f"A1:{self._get_column_letter(len(new_headers))}1")
                self.headers = new_headers
                
                print(f"Added missing columns: {missing_columns}")
        
        except Exception as e:
            print(f"Column update error: {e}")
            traceback.print_exc()
    
    def upsert(self, payload: Dict) -> bool:
        """データをUPSERT（trim_name削除、grade使用）"""
        if not self.enabled:
            return False
        
        try:
            slug = payload.get('slug')
            grade = payload.get('grade', '無し')
            
            if not slug:
                return False
            
            # ユニークキーとしてslug + gradeを使用
            unique_key = f"{slug}_{grade}"
            
            # 既存データ検索
            try:
                if 'slug' in self.headers and 'grade' in self.headers:
                    slug_col = self.headers.index('slug') + 1
                    grade_col = self.headers.index('grade') + 1
                    
                    all_values = self.worksheet.get_all_values()
                    
                    row_num = None
                    for i, row in enumerate(all_values[1:], start=2):
                        if len(row) > max(slug_col-1, grade_col-1):
                            if row[slug_col-1] == slug and row[grade_col-1] == grade:
                                row_num = i
                                break
                    
                    if not row_num:
                        row_num = len(all_values) + 1
                else:
                    all_values = self.worksheet.get_all_values()
                    row_num = len(all_values) + 1
                    
            except Exception as e:
                print(f"Error searching for existing row: {e}")
                all_values = self.worksheet.get_all_values()
                row_num = len(all_values) + 1
            
            # 行データ作成
            row_data = []
            for header in self.headers:
                value = payload.get(header)
                
                # media_urlsの処理 - 制限を削除してすべてのURLを保存
                if header == 'media_urls' and isinstance(value, list):
                    # 全てのURLをカンマ区切りで保存（制限なし）
                    value = ', '.join(value) if value else ''
                elif header == 'colors' and isinstance(value, list):
                    value = ', '.join(value) if value else ''
                elif isinstance(value, list):
                    value = json.dumps(value, ensure_ascii=False)
                elif isinstance(value, dict):
                    value = json.dumps(value, ensure_ascii=False)
                elif value is None:
                    value = ""
                else:
                    value = str(value)
                
                row_data.append(value)
            
            # データ更新
            end_col = self._get_column_letter(len(self.headers))
            range_name = f"A{row_num}:{end_col}{row_num}"
            
            self.worksheet.update(
                [row_data], 
                range_name,
                value_input_option='RAW'
            )
            
            return True
            
        except Exception as e:
            print(f"Sheets upsert error: {e}")
            traceback.print_exc()
            return False
    
    def batch_upsert(self, payloads: List[Dict]) -> int:
        """複数データを一括更新"""
        if not self.enabled:
            return 0
        
        success_count = 0
        for payload in payloads:
            if self.upsert(payload):
                success_count += 1
            time.sleep(0.5)
        
        return success_count

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
            makers = [m for m in makers if m not in ['editorial', 'leasing', 'jaecoo', 'omoda']]
        
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
            
            # スクレイピング
            raw_data = self.scraper.scrape_vehicle(slug)
            
            # データ処理（1つまたは複数のペイロードが返される）
            payloads = self.processor.process_vehicle_data(raw_data)
            
            if not isinstance(payloads, list):
                payloads = [payloads]
            
            success = False
            for payload in payloads:
                # 検証
                is_valid, errors = self.validator.validate_payload(payload)
                if not is_valid:
                    print(f"INVALID ({payload.get('grade', '')}): {', '.join(errors)}")
                    self.stats['skipped'] += 1
                    continue
                
                # データベース同期
                db_success = self.supabase.upsert(payload)
                sheets_success = self.sheets.upsert(payload)
                
                if db_success or sheets_success:
                    success = True
            
            if success:
                grade_info = payloads[0].get('grade', '無し') if payloads else '無し'
                print(f"OK (Grade: {grade_info})")
                self.stats['success'] += 1
            else:
                print("FAILED")
                self.stats['failed'] += 1
                
        except Exception as e:
            error_msg = str(e)
            if "Not a valid model page" in error_msg:
                print("ERROR: Not a valid model page")
            else:
                print(f"ERROR: {error_msg[:50]}")
            self.stats['failed'] += 1
            self.stats['errors'].append(f"{slug}: {error_msg}")
    
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
            error_types = {}
            for error in self.stats['errors']:
                if "Not a valid model page" in error:
                    error_type = "Not a valid model page"
                elif "Failed to scrape" in error:
                    error_type = "Scraping failed"
                else:
                    error_type = "Other"
                
                if error_type not in error_types:
                    error_types[error_type] = []
                error_types[error_type].append(error.split(':')[0])
            
            for error_type, slugs in error_types.items():
                print(f"  {error_type}: {len(slugs)} cases")
                if len(slugs) <= 5:
                    for slug in slugs:
                        print(f"    - {slug}")
        
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
    
    parser.add_argument(
        '--test',
        action='store_true',
        help='Run in test mode (process only 5 vehicles)'
    )
    
    parser.add_argument(
        '--validate-only',
        action='store_true',
        help='Only validate data without syncing'
    )
    
    args = parser.parse_args()
    
    if args.test:
        args.limit = 5
        print("Running in TEST mode (limit=5)")
    
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
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
