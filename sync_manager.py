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

# Google Sheets設定
SHEET_NAME = "system_cars"
SHEET_HEADERS = [
    "id", "slug", "make_en", "model_en", "make_ja", "model_ja",
    "body_type", "body_type_ja", "fuel", 
    "price_min_gbp", "price_max_gbp", "price_used_gbp",
    "price_min_jpy", "price_max_jpy", "price_used_jpy",
    "overview_en", "overview_ja", "spec_json", "media_urls",
    "catalog_url", "doors", "seats", "dimensions_mm",
    "drive_type", "drive_type_ja", "grades", "engines", 
    "colors", "full_model_ja", "updated_at"
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
            response = requests.post(
                f"{self.url}/rest/v1/cars",
                headers={
                    "apikey": self.key,
                    "Authorization": f"Bearer {self.key}",
                    "Content-Type": "application/json",
                    "Prefer": "resolution=merge-duplicates"
                },
                json=payload,
                timeout=30
            )
            
            if response.status_code in [200, 201]:
                return True
            else:
                print(f"Supabase error: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            print(f"Supabase exception: {e}")
            return False
    
    def batch_upsert(self, payloads: List[Dict]) -> int:
        """複数データを一括UPSERT"""
        if not self.enabled:
            return 0
        
        success_count = 0
        for payload in payloads:
            if self.upsert(payload):
                success_count += 1
            time.sleep(0.1)  # レート制限対策
        
        return success_count

# ======================== Sheets Manager ========================
class GoogleSheetsManager:
    """Google Sheets管理"""
    
    def __init__(self):
        self.worksheet = None
        self.enabled = False
        self._initialize()
    
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
            except gspread.WorksheetNotFound:
                self.worksheet = spreadsheet.add_worksheet(
                    title=SHEET_NAME,
                    rows=1000,
                    cols=len(SHEET_HEADERS)
                )
                # ヘッダー設定
                self.worksheet.update([SHEET_HEADERS], "A1")
            
            self.enabled = True
            print(f"Connected to Google Sheets: {SHEET_NAME}")
            
        except Exception as e:
            print(f"Google Sheets initialization error: {e}")
            self.enabled = False
    
    def _ensure_headers(self):
        """ヘッダー行を確認・設定"""
        if not self.enabled:
            return
        
        try:
            current_headers = self.worksheet.row_values(1)
            if current_headers != SHEET_HEADERS:
                self.worksheet.update([SHEET_HEADERS], "A1")
        except Exception as e:
            print(f"Header update error: {e}")
    
    def upsert(self, payload: Dict) -> bool:
        """データをUPSERT"""
        if not self.enabled:
            return False
        
        try:
            # スラッグで既存行を検索
            slug = payload.get('slug')
            if not slug:
                return False
            
            # 既存データ検索
            try:
                cell = self.worksheet.find(slug)
                row_num = cell.row
            except gspread.CellNotFound:
                # 新規追加
                row_num = len(self.worksheet.get_all_values()) + 1
            
            # 行データ作成
            row_data = []
            for header in SHEET_HEADERS:
                value = payload.get(header)
                
                # 特殊な型の処理
                if isinstance(value, list):
                    value = json.dumps(value, ensure_ascii=False)
                elif isinstance(value, dict):
                    value = json.dumps(value, ensure_ascii=False)
                elif value is None:
                    value = ""
                else:
                    value = str(value)
                
                row_data.append(value)
            
            # データ更新
            range_name = f"A{row_num}:{chr(64 + len(SHEET_HEADERS))}{row_num}"
            self.worksheet.update([row_data], range_name)
            
            return True
            
        except Exception as e:
            print(f"Sheets upsert error: {e}")
            return False
    
    def batch_upsert(self, payloads: List[Dict]) -> int:
        """複数データを一括更新"""
        if not self.enabled:
            return 0
        
        success_count = 0
        for payload in payloads:
            if self.upsert(payload):
                success_count += 1
            time.sleep(0.5)  # API制限対策
        
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
        
        # 統計情報
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
        
        # メーカーリスト取得
        if makers is None:
            makers = self.scraper.get_all_makers()
        
        print(f"Processing {len(makers)} makers")
        
        # 各メーカーを処理
        for maker_idx, maker in enumerate(makers):
            print(f"\n[{maker_idx + 1}/{len(makers)}] Processing: {maker}")
            
            try:
                models = self.scraper.get_models_for_maker(maker)
                print(f"  Found {len(models)} models")
                
                # 各モデルを処理
                for model_idx, model_slug in enumerate(models):
                    if limit and self.stats['total'] >= limit:
                        print("\nReached limit, stopping...")
                        return
                    
                    self.stats['total'] += 1
                    self._process_vehicle(model_slug, model_idx + 1, len(models))
                    
                    # レート制限対策
                    time.sleep(0.5)
                    
            except Exception as e:
                print(f"  Error processing maker {maker}: {e}")
                self.stats['errors'].append(f"Maker {maker}: {str(e)}")
        
        # 統計表示
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
            
            # データ処理
            payload = self.processor.process_vehicle_data(raw_data)
            
            # 検証
            is_valid, errors = self.validator.validate_payload(payload)
            if not is_valid:
                print(f"INVALID: {', '.join(errors)}")
                self.stats['skipped'] += 1
                return
            
            # データベース同期
            db_success = self.supabase.upsert(payload)
            sheets_success = self.sheets.upsert(payload)
            
            if db_success or sheets_success:
                print("OK")
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
            for error in self.stats['errors'][:10]:  # 最初の10個のみ表示
                print(f"  - {error}")
            if len(self.stats['errors']) > 10:
                print(f"  ... and {len(self.stats['errors']) - 10} more")
        
        # 成功率
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
    
    # テストモード
    if args.test:
        args.limit = 5
        print("Running in TEST mode (limit=5)")
    
    # 同期マネージャー初期化
    manager = SyncManager()
    
    # 実行
    try:
        if args.models:
            # 特定モデルの処理
            manager.sync_specific(args.models)
        else:
            # メーカー単位の処理
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
