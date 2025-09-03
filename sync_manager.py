#!/usr/bin/env python3
"""
sync_manager.py
"""
import os
import sys
import json
import time
import uuid
import argparse
import logging
from datetime import datetime
from typing import Dict, List, Optional
from pathlib import Path

import requests
import gspread
from google.oauth2.service_account import Credentials

# 他のモジュールをインポート
try:
    from carwow_scraper import CarwowScraper
    from data_processor import DataProcessor
except ImportError as e:
    print(f"Error importing modules: {e}")
    sys.exit(1)

# ロギング設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('sync.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
GS_CREDS_JSON = os.getenv("GS_CREDS_JSON")
GS_SHEET_ID = os.getenv("GS_SHEET_ID")
DEEPL_KEY = os.getenv("DEEPL_KEY")

# Google Sheets設定
SHEET_NAME = "system_cars"
SHEET_HEADERS = [
    "id", "slug", "make_en", "model_en", "make_ja", "model_ja",
    "grade", "engine", "engine_price_gbp", "engine_price_jpy",
    "body_type", "body_type_ja", "fuel", "fuel_ja",
    "transmission", "transmission_ja", "price_min_gbp", "price_max_gbp", 
    "price_used_gbp", "price_min_jpy", "price_max_jpy", "price_used_jpy",
    "overview_en", "overview_ja", "doors", "seats", "power_bhp",
    "drive_type", "drive_type_ja", "dimensions_mm", "dimensions_ja",
    "colors", "colors_ja", "media_urls", "catalog_url",
    "full_model_ja", "updated_at", "spec_json", "is_active"
]

def initialize_cache_files():
    """空のキャッシュファイルを初期化"""
    cache_files = {
        'body_type_cache.json': {},
        'translation_cache.json': {},
        'exchange_rate_cache.json': {},
        'deepl_quota.json': {'month': datetime.now().strftime('%Y-%m'), 'used': 0}
    }
    
    for filename, default_content in cache_files.items():
        file_path = Path(filename)
        if not file_path.exists() or file_path.stat().st_size == 0:
            try:
                with open(file_path, 'w') as f:
                    json.dump(default_content, f)
                logger.info(f"Initialized {filename}")
            except Exception as e:
                logger.error(f"Failed to initialize {filename}: {e}")

# SupabaseManager クラス
class SupabaseManager:
    """Supabaseデータベース管理"""
    
    def __init__(self):
        self.url = SUPABASE_URL
        self.key = SUPABASE_KEY
        self.enabled = bool(self.url and self.key)
        
        if not self.enabled:
            logger.warning("Supabase credentials not configured")
    
    def upsert(self, payload: Dict) -> bool:
        """データをUPSERT"""
        if not self.enabled:
            return False
        
        try:
            headers = {
                'apikey': self.key,
                'Authorization': f'Bearer {self.key}',
                'Content-Type': 'application/json',
                'Prefer': 'resolution=merge-duplicates,return=minimal'
            }
            
            clean_payload = self._prepare_payload(payload)
            
            response = requests.post(
                f"{self.url}/rest/v1/cars",
                headers=headers,
                json=clean_payload,
                timeout=30
            )
            
            if response.status_code in [200, 201, 204]:
                return True
            elif response.status_code == 409 and 'id' in clean_payload:
                update_url = f"{self.url}/rest/v1/cars?id=eq.{clean_payload['id']}"
                update_response = requests.patch(
                    update_url,
                    headers=headers,
                    json=clean_payload,
                    timeout=30
                )
                return update_response.status_code in [200, 204]
            else:
                logger.error(f"Supabase error {response.status_code}: {response.text[:200]}")
                return False
                
        except Exception as e:
            logger.error(f"Supabase upsert error: {e}")
            return False
    
    def mark_inactive(self, slug: str, grade: str = None, engine: str = None) -> bool:
        """レコードを非アクティブに設定"""
        if not self.enabled:
            return False
        
        try:
            headers = {
                'apikey': self.key,
                'Authorization': f'Bearer {self.key}',
                'Content-Type': 'application/json',
                'Prefer': 'return=minimal'
            }
            
            update_url = f"{self.url}/rest/v1/cars?slug=eq.{slug}"
            if grade:
                update_url += f"&grade=eq.{grade}"
            if engine:
                update_url += f"&engine=eq.{engine}"
            
            update_data = {
                'is_active': False,
                'updated_at': datetime.now().isoformat()
            }
            
            response = requests.patch(
                update_url,
                headers=headers,
                json=update_data,
                timeout=30
            )
            
            return response.status_code in [200, 204]
                
        except Exception as e:
            logger.error(f"Error marking inactive: {e}")
            return False
    
    def _prepare_payload(self, payload: Dict) -> Dict:
        """ペイロードを準備"""
        clean = {}
        
        for key, value in payload.items():
            if value is None or value in ['-', 'N/A', 'ー']:
                continue
            
            if key in ['body_type', 'body_type_ja', 'colors', 'colors_ja', 'media_urls']:
                if isinstance(value, list):
                    if value == ['Information not available'] or value == ['ー']:
                        clean[key] = []
                    else:
                        clean[key] = value
                else:
                    clean[key] = []
            elif key == 'spec_json':
                if isinstance(value, dict):
                    clean[key] = value
                else:
                    clean[key] = {}
            elif key in ['price_min_gbp', 'price_max_gbp', 'price_used_gbp',
                        'price_min_jpy', 'price_max_jpy', 'price_used_jpy',
                        'engine_price_gbp', 'engine_price_jpy']:
                if value not in ['Information not available', 'ー'] and value is not None:
                    try:
                        clean[key] = float(value)
                    except (ValueError, TypeError):
                        pass
            elif key in ['doors', 'seats', 'power_bhp']:
                if value not in ['Information not available', 'ー'] and value is not None:
                    try:
                        clean[key] = int(value)
                    except (ValueError, TypeError):
                        pass
            elif key == 'is_active':
                clean[key] = bool(value)
            else:
                if value not in ['Information not available', 'ー']:
                    clean[key] = value
        
        return clean

# GoogleSheetsManager クラス
class GoogleSheetsManager:
    """Google Sheets管理"""
    
    def __init__(self):
        self.worksheet = None
        self.enabled = False
        self.headers = SHEET_HEADERS
        self.last_request_time = 0
        self.request_count = 0
        self.rate_limit_per_100_seconds = 95
        self._initialize()
    
    def _rate_limit_check(self):
        """レート制限チェック"""
        current_time = time.time()
        
        if current_time - self.last_request_time > 100:
            self.request_count = 0
            self.last_request_time = current_time
        
        if self.request_count >= self.rate_limit_per_100_seconds:
            sleep_time = 100 - (current_time - self.last_request_time)
            if sleep_time > 0:
                logger.info(f"Rate limit reached, waiting {sleep_time:.1f} seconds...")
                time.sleep(sleep_time)
                self.request_count = 0
                self.last_request_time = time.time()
        
        self.request_count += 1
    
    def _initialize(self):
        """初期化"""
        if not (GS_CREDS_JSON and GS_SHEET_ID):
            logger.warning("Google Sheets credentials not configured")
            return
        
        creds_path = Path("temp_creds.json")
        
        try:
            if GS_CREDS_JSON.startswith('{'):
                creds_data = json.loads(GS_CREDS_JSON)
            else:
                with open(GS_CREDS_JSON, 'r') as f:
                    creds_data = json.load(f)
            
            creds_path.write_text(json.dumps(creds_data))
            
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
            
            try:
                self.worksheet = spreadsheet.worksheet(SHEET_NAME)
            except gspread.WorksheetNotFound:
                self.worksheet = spreadsheet.add_worksheet(
                    title=SHEET_NAME,
                    rows=10000,
                    cols=len(SHEET_HEADERS)
                )
            
            self._setup_headers()
            self.enabled = True
            logger.info(f"Connected to Google Sheets: {SHEET_NAME}")
            
        except Exception as e:
            logger.error(f"Google Sheets initialization error: {e}")
            self.enabled = False
        
        finally:
            if creds_path.exists():
                creds_path.unlink()
    
    def _setup_headers(self):
        """ヘッダー設定"""
        try:
            self._rate_limit_check()
            current_headers = self.worksheet.row_values(1)
            if not current_headers or current_headers != self.headers:
                self._rate_limit_check()
                self.worksheet.update([self.headers], "A1")
        except Exception as e:
            logger.error(f"Error setting headers: {e}")
    
    def upsert(self, payload: Dict) -> bool:
        """データをUPSERT"""
        if not self.enabled:
            return False
        
        try:
            self._rate_limit_check()
            
            id_value = payload.get('id')
            if not id_value:
                return False
            
            try:
                all_values = self.worksheet.get_all_values()
                row_num = None
                
                for i, row in enumerate(all_values[1:], start=2):
                    if len(row) > 0 and row[0] == id_value:
                        row_num = i
                        break
                
                if not row_num:
                    row_num = len(all_values) + 1
                    
            except Exception:
                row_num = self.worksheet.row_count + 1
            
            row_data = self._prepare_row_data(payload)
            
            self._rate_limit_check()
            self.worksheet.update(
                [row_data], 
                f"A{row_num}",
                value_input_option='RAW'
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Sheets upsert error: {e}")
            return False
    
    def mark_inactive(self, slug: str, grade: str = None, engine: str = None) -> bool:
        """レコードを非アクティブに設定"""
        if not self.enabled:
            return False
        
        try:
            self._rate_limit_check()
            all_values = self.worksheet.get_all_values()
            
            for i, row in enumerate(all_values[1:], start=2):
                if len(row) > 7:
                    if (row[1] == slug and 
                        (not grade or row[6] == grade) and 
                        (not engine or row[7] == engine)):
                        
                        is_active_index = self.headers.index('is_active')
                        updated_at_index = self.headers.index('updated_at')
                        
                        self._rate_limit_check()
                        self.worksheet.update_cell(i, is_active_index + 1, 'FALSE')
                        self.worksheet.update_cell(i, updated_at_index + 1, datetime.now().isoformat())
                        
                        return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error marking inactive in Sheets: {e}")
            return False
    
    def _prepare_row_data(self, payload: Dict) -> List[str]:
        """行データを準備"""
        row_data = []
        
        for header in self.headers:
            value = payload.get(header)
            
            if value is None or value in ['-', 'N/A', 'Information not available', 'ー']:
                row_data.append('')
            elif isinstance(value, list):
                if value == ['Information not available'] or value == ['ー']:
                    row_data.append('')
                else:
                    row_data.append(', '.join(str(v) for v in value))
            elif isinstance(value, dict):
                row_data.append(json.dumps(value, ensure_ascii=False))
            elif isinstance(value, datetime):
                row_data.append(value.isoformat())
            elif isinstance(value, bool):
                row_data.append('TRUE' if value else 'FALSE')
            else:
                row_data.append(str(value))
        
        return row_data

# SyncManager クラス
class SyncManager:
    """メイン同期管理クラス"""
    
    def __init__(self):
        # キャッシュファイルを初期化
        initialize_cache_files()
        
        self.scraper = CarwowScraper()
        self.processor = DataProcessor()
        self.supabase = SupabaseManager()
        self.sheets = GoogleSheetsManager()
        
        if not os.getenv('DEEPL_KEY'):
            logger.warning("DeepL API key not configured")
        
        self.stats = {
            'total': 0,
            'success': 0,
            'failed': 0,
            'skipped': 0,
            'inactive': 0,
            'records_saved': 0,
            'errors': []
        }
    
    def sync_all(self, makers: Optional[List[str]] = None, limit: Optional[int] = None):
        """全データを同期"""
        logger.info("=" * 60)
        logger.info("Starting Carwow Data Sync")
        logger.info(f"Time: {datetime.now().isoformat()}")
        logger.info(f"Supabase: {'Enabled' if self.supabase.enabled else 'Disabled'}")
        logger.info(f"Google Sheets: {'Enabled' if self.sheets.enabled else 'Disabled'}")
        logger.info(f"DeepL: {'Enabled' if os.getenv('DEEPL_KEY') else 'Disabled'}")
        logger.info("=" * 60)
        
        if makers is None:
            # get_all_makers メソッドの存在を確認
            if hasattr(self.scraper, 'get_all_makers'):
                makers = self.scraper.get_all_makers()
            else:
                # デフォルトのメーカーリストを使用
                makers = [
                    'audi', 'bmw', 'mercedes-benz', 'volkswagen', 'toyota',
                    'honda', 'nissan', 'mazda', 'ford', 'tesla'
                ]
                logger.warning("Using default makers list")
            
            exclude = ['editorial', 'leasing', 'news', 'reviews', 'deals', 'advice']
            makers = [m for m in makers if m not in exclude]
        
        logger.info(f"Processing {len(makers)} makers")
        
        for maker_idx, maker in enumerate(makers):
            logger.info(f"\n[{maker_idx + 1}/{len(makers)}] Processing: {maker}")
            
            try:
                models = self.scraper.get_models_for_maker(maker)
                logger.info(f"  Found {len(models)} models")
                
                for model_idx, model_slug in enumerate(models):
                    if limit and self.stats['total'] >= limit:
                        logger.info("\nReached limit, stopping...")
                        self._print_statistics()
                        return
                    
                    self.stats['total'] += 1
                    self._process_vehicle(model_slug, model_idx + 1, len(models))
                    time.sleep(0.5)
                    
            except Exception as e:
                logger.error(f"Error processing maker {maker}: {e}")
                self.stats['errors'].append(f"Maker {maker}: {str(e)}")
            
            finally:
                if maker_idx % 10 == 0:
                    import gc
                    gc.collect()
        
        self.scraper.cleanup()
        self._print_statistics()
    
    def sync_specific(self, slugs: List[str]):
        """特定の車両のみ同期"""
        logger.info(f"Syncing {len(slugs)} specific vehicles")
        
        for idx, slug in enumerate(slugs):
            self.stats['total'] += 1
            self._process_vehicle(slug, idx + 1, len(slugs))
            time.sleep(0.5)
        
        self.scraper.cleanup()
        self._print_statistics()
    
    def _process_vehicle(self, slug: str, current: int, total: int):
        """個別車両を処理"""
        try:
            logger.info(f"  [{current}/{total}] {slug}...")
            
            raw_data = self.scraper.scrape_vehicle(slug)
            
            if not raw_data:
                logger.info(f"    NO DATA - marking as inactive")
                self.supabase.mark_inactive(slug)
                self.sheets.mark_inactive(slug)
                self.stats['inactive'] += 1
                self.stats['skipped'] += 1
                return
            
            records = self.processor.process_vehicle_data(raw_data)
            
            saved_count = 0
            for record in records:
                supabase_success = self.supabase.upsert(record)
                sheets_success = self.sheets.upsert(record)
                
                if supabase_success or sheets_success:
                    saved_count += 1
                    self.stats['records_saved'] += 1
            
            if saved_count > 0:
                logger.info(f"    OK ({saved_count}/{len(records)} records)")
                self.stats['success'] += 1
            else:
                logger.info(f"    FAILED")
                self.stats['failed'] += 1
                
        except Exception as e:
            logger.error(f"    ERROR: {e}")
            self.stats['failed'] += 1
            self.stats['errors'].append(f"{slug}: {str(e)}")
    
    def _print_statistics(self):
        """統計情報を表示"""
        logger.info("\n" + "=" * 60)
        logger.info("Sync Statistics")
        logger.info("=" * 60)
        logger.info(f"Total vehicles: {self.stats['total']}")
        logger.info(f"Successful: {self.stats['success']}")
        logger.info(f"Failed: {self.stats['failed']}")
        logger.info(f"Skipped: {self.stats['skipped']}")
        logger.info(f"Marked inactive: {self.stats['inactive']}")
        logger.info(f"Records saved: {self.stats['records_saved']}")
        
        if self.stats['errors']:
            logger.info(f"\nErrors (first 10):")
            for error in self.stats['errors'][:10]:
                logger.info(f"  - {error[:100]}")
        
        if self.stats['total'] > 0:
            success_rate = (self.stats['success'] / self.stats['total']) * 100
            logger.info(f"\nSuccess rate: {success_rate:.1f}%")

def main():
    """メインエントリポイント"""
    parser = argparse.ArgumentParser(description="Carwow Data Sync Manager")
    parser.add_argument('--test', action='store_true', help='Test mode (5 vehicles)')
    parser.add_argument('--makers', nargs='+', help='Specific makers')
    parser.add_argument('--models', nargs='+', help='Specific models')
    parser.add_argument('--limit', type=int, help='Limit vehicles')
    parser.add_argument('--no-supabase', action='store_true', help='Disable Supabase')
    parser.add_argument('--no-sheets', action='store_true', help='Disable Sheets')
    parser.add_argument('--no-deepl', action='store_true', help='Disable DeepL')
    
    args = parser.parse_args()
    
    if args.no_supabase:
        os.environ['SUPABASE_URL'] = ''
        os.environ['SUPABASE_KEY'] = ''
    
    if args.no_sheets:
        os.environ['GS_CREDS_JSON'] = ''
        os.environ['GS_SHEET_ID'] = ''
    
    if args.no_deepl:
        os.environ['DEEPL_KEY'] = ''
    
    if args.test:
        args.limit = 5
        logger.info("Running in TEST mode (limit=5)")
    
    manager = SyncManager()
    
    if not manager.supabase.enabled and not manager.sheets.enabled:
        logger.error("No sync destination configured")
        sys.exit(1)
    
    try:
        if args.models:
            manager.sync_specific(args.models)
        else:
            manager.sync_all(makers=args.makers, limit=args.limit)
    
    except KeyboardInterrupt:
        logger.info("\nInterrupted by user")
        manager._print_statistics()
    
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
