#!/usr/bin/env python3
"""
sync_manager.py - 修正版
実行管理とデータベース・スプレッドシート同期モジュール
logging統合、レート制限最適化、UPSERT改善
"""
import os
import sys
import json
import time
import hashlib
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

# ======================== Configuration ========================
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
    "full_model_ja", "updated_at", "spec_json"
]

# ======================== Logging Setup ========================
def setup_logging():
    """ログ設定を初期化"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('sync.log', encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

# ======================== Database Manager ========================
class SupabaseManager:
    """Supabaseデータベース管理（UPSERT改善版）"""
    
    def __init__(self):
        self.url = SUPABASE_URL
        self.key = SUPABASE_KEY
        self.enabled = bool(self.url and self.key)
        self.logger = logging.getLogger(__name__)
        
        if not self.enabled:
            self.logger.warning("Supabase credentials not configured")
    
    def upsert(self, payload: Dict) -> bool:
        """データをUPSERT（IDベース、重複キーの一貫性改善）"""
        if not self.enabled:
            return False
        
        try:
            headers = {
                'apikey': self.key,
                'Authorization': f'Bearer {self.key}',
                'Content-Type': 'application/json',
                'Prefer': 'return=minimal'
            }
            
            # データ型の変換
            clean_payload = self._prepare_payload(payload)
            
            # IDベースでUPSERTを試みる
            upsert_headers = headers.copy()
            upsert_headers['Prefer'] = 'resolution=merge-duplicates,return=minimal'
            
            response = requests.post(
                f"{self.url}/rest/v1/cars",
                headers=upsert_headers,
                json=clean_payload,
                timeout=30
            )
            
            if response.status_code in [200, 201, 204]:
                return True
            elif response.status_code == 409:
                # 重複エラーの場合はIDでUPDATEを試みる
                if 'id' in clean_payload:
                    update_url = f"{self.url}/rest/v1/cars?id=eq.{clean_payload['id']}"
                    
                    update_response = requests.patch(
                        update_url,
                        headers=headers,
                        json=clean_payload,
                        timeout=30
                    )
                    
                    if update_response.status_code in [200, 204]:
                        return True
                    else:
                        self.logger.error(f"Supabase update error {update_response.status_code}: {update_response.text[:200]}")
                        return False
                return False
            else:
                self.logger.error(f"Supabase error {response.status_code}: {response.text[:200]}")
                return False
                
        except Exception as e:
            self.logger.error(f"Supabase upsert error: {e}")
            return False
    
    def mark_inactive(self, slug: str, grade: str, engine: str) -> bool:
        """レコードを非アクティブにマーク（販売終了処理）"""
        if not self.enabled:
            return False
        
        try:
            headers = {
                'apikey': self.key,
                'Authorization': f'Bearer {self.key}',
                'Content-Type': 'application/json'
            }
            
            update_data = {
                'is_active': False,
                'last_updated': datetime.now().isoformat()
            }
            
            update_url = f"{self.url}/rest/v1/cars"
            update_url += f"?slug=eq.{slug}&grade=eq.{grade}&engine=eq.{engine}"
            
            response = requests.patch(
                update_url,
                headers=headers,
                json=update_data,
                timeout=30
            )
            
            return response.status_code in [200, 204]
            
        except Exception as e:
            self.logger.error(f"Supabase mark_inactive error: {e}")
            return False
    
    def _prepare_payload(self, payload: Dict) -> Dict:
        """ペイロードをデータベース用に準備"""
        clean = {}
        
        for key, value in payload.items():
            # None、'-'、'N/A'、'ー'の値は除外
            if value is None or value in ['-', 'N/A', 'ー']:
                continue
            
            # 配列型のフィールド
            if key in ['body_type', 'body_type_ja', 'colors', 'colors_ja', 'media_urls']:
                if isinstance(value, list):
                    if value == ['Information not available'] or value == ['ー']:
                        clean[key] = []
                    else:
                        clean[key] = value
                else:
                    clean[key] = []
            
            # JSONB型のフィールド
            elif key == 'spec_json':
                if isinstance(value, dict):
                    clean_spec = {}
                    for k, v in value.items():
                        if v not in ['-', 'N/A', 'ー']:
                            clean_spec[k] = v
                    clean[key] = clean_spec
                else:
                    clean[key] = {}
            
            # 数値型のフィールド
            elif key in ['price_min_gbp', 'price_max_gbp', 'price_used_gbp',
                        'price_min_jpy', 'price_max_jpy', 'price_used_jpy',
                        'engine_price_gbp', 'engine_price_jpy']:
                if value not in ['Information not available', 'ー'] and value is not None:
                    try:
                        clean[key] = float(value)
                    except (ValueError, TypeError):
                        pass
            
            # 整数型のフィールド
            elif key in ['doors', 'seats', 'power_bhp']:
                if value not in ['Information not available', 'ー'] and value is not None:
                    try:
                        clean[key] = int(value)
                    except (ValueError, TypeError):
                        pass
            
            # その他のフィールド
            else:
                if value == 'Information not available' or value == 'ー':
                    pass
                else:
                    clean[key] = value
        
        return clean

# ======================== Google Sheets Manager ========================
class GoogleSheetsManager:
    """Google Sheets管理（レート制限最適化版）"""
    
    def __init__(self):
        self.worksheet = None
        self.enabled = False
        self.headers = SHEET_HEADERS
        self.last_request_time = 0
        self.request_count = 0
        self.rate_limit_per_100_seconds = 95  # Google Sheets API制限に近づける
        self.logger = logging.getLogger(__name__)
        self._initialize()
    
    def _rate_limit_check(self):
        """Google Sheets APIレート制限チェック（最適化版）"""
        current_time = time.time()
        
        # 100秒経過したらカウントリセット
        if current_time - self.last_request_time > 100:
            self.request_count = 0
            self.last_request_time = current_time
        
        # レート制限に達したら待機
        if self.request_count >= self.rate_limit_per_100_seconds:
            sleep_time = 100 - (current_time - self.last_request_time)
            if sleep_time > 0:
                self.logger.info(f"Rate limit reached, waiting {sleep_time:.1f} seconds...")
                time.sleep(sleep_time)
                self.request_count = 0
                self.last_request_time = time.time()
        
        self.request_count += 1
    
    def _initialize(self):
        """Google Sheets接続を初期化"""
        if not (GS_CREDS_JSON and GS_SHEET_ID):
            self.logger.warning("Google Sheets credentials not configured")
            return
        
        creds_path = Path("temp_creds.json")
        
        try:
            # 認証情報の処理
            if GS_CREDS_JSON.startswith('{'):
                creds_data = json.loads(GS_CREDS_JSON)
            else:
                with open(GS_CREDS_JSON, 'r') as f:
                    creds_data = json.load(f)
            
            # 一時ファイルに保存
            creds_path.write_text(json.dumps(creds_data))
            
            # 認証
            scopes = [
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"
            ]
            
            credentials = Credentials.from_service_account_file(
                str(creds_path), 
                scopes=scopes
            )
            
            # スプレッドシート接続
            client = gspread.authorize(credentials)
            spreadsheet = client.open_by_key(GS_SHEET_ID)
            
            # ワークシート取得または作成
            try:
                self.worksheet = spreadsheet.worksheet(SHEET_NAME)
            except gspread.WorksheetNotFound:
                self.worksheet = spreadsheet.add_worksheet(
                    title=SHEET_NAME,
                    rows=10000,
                    cols=len(SHEET_HEADERS)
                )
            
            # ヘッダー設定
            self._setup_headers()
            
            self.enabled = True
            self.logger.info(f"Connected to Google Sheets: {SHEET_NAME}")
            
        except Exception as e:
            self.logger.error(f"Google Sheets initialization error: {e}")
            self.enabled = False
        
        finally:
            # 一時ファイルを必ず削除
            if creds_path.exists():
                creds_path.unlink()
    
    def _setup_headers(self):
        """ヘッダー行を設定"""
        try:
            self._rate_limit_check()
            current_headers = self.worksheet.row_values(1)
            if not current_headers or current_headers != self.headers:
                self._rate_limit_check()
                self.worksheet.update([self.headers], "A1")
        except Exception as e:
            self.logger.error(f"Error setting headers: {e}")
    
    def upsert(self, payload: Dict) -> bool:
        """データをUPSERT"""
        if not self.enabled:
            return False
        
        try:
            # レート制限チェック
            self._rate_limit_check()
            
            # ユニークキーでの検索
            slug = payload.get('slug')
            grade = payload.get('grade', 'Standard')
            engine = payload.get('engine', 'N/A')
            
            if not slug:
                return False
            
            # 既存の行を検索
            try:
                all_values = self.worksheet.get_all_values()
                row_num = None
                
                for i, row in enumerate(all_values[1:], start=2):
                    if len(row) > 7:
                        if row[1] == slug and row[6] == grade and row[7] == engine:
                            row_num = i
                            break
                
                if not row_num:
                    row_num = len(all_values) + 1
                    
            except Exception:
                row_num = self.worksheet.row_count + 1
            
            # 行データの準備
            row_data = self._prepare_row_data(payload)
            
            # データ更新
            self._rate_limit_check()
            self.worksheet.update(
                [row_data], 
                f"A{row_num}",
                value_input_option='RAW'
            )
            
            return True
            
        except Exception as e:
            self.logger.error(f"Sheets upsert error: {e}")
            return False
    
    def mark_inactive(self, slug: str, grade: str, engine: str) -> bool:
        """レコードを非アクティブにマーク（販売終了処理）"""
        if not self.enabled:
            return False
        
        try:
            # レート制限チェック
            self._rate_limit_check()
            
            # 既存の行を検索
            all_values = self.worksheet.get_all_values()
            
            for i, row in enumerate(all_values[1:], start=2):
                if len(row) > 7:
                    if row[1] == slug and row[6] == grade and row[7] == engine:
                        # is_activeカラムとlast_updatedカラムを更新
                        # 注意: スプレッドシートの構造に応じてカラム位置を調整する必要があります
                        self._rate_limit_check()
                        self.worksheet.update_cell(i, len(SHEET_HEADERS) + 1, 'FALSE')  # is_active
                        self._rate_limit_check()
                        self.worksheet.update_cell(i, len(SHEET_HEADERS) + 2, datetime.now().isoformat())  # last_updated
                        return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"Sheets mark_inactive error: {e}")
            return False
    
    def _prepare_row_data(self, payload: Dict) -> List[str]:
        """シート用の行データを準備"""
        row_data = []
        
        for header in self.headers:
            value = payload.get(header)
            
            # 値の変換
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
            else:
                row_data.append(str(value))
        
        return row_data
    
    def batch_upsert(self, payloads: List[Dict]) -> int:
        """複数データを一括更新（改善版）"""
        if not self.enabled:
            return 0
        
        success_count = 0
        
        try:
            # レート制限チェック
            self._rate_limit_check()
            
            # 既存データを取得してマッピング作成
            existing_data = self.worksheet.get_all_values()
            existing_map = {}
            
            # 既存データのマッピング作成
            for i, row in enumerate(existing_data[1:], start=2):
                if len(row) > 7:
                    key = f"{row[1]}_{row[6]}_{row[7]}"  # slug_grade_engine
                    existing_map[key] = i
            
            # 更新用と新規用に分類
            updates = []
            new_rows = []
            
            for payload in payloads:
                slug = payload.get('slug', '')
                grade = payload.get('grade', 'Standard')
                engine = payload.get('engine', 'N/A')
                key = f"{slug}_{grade}_{engine}"
                row_data = self._prepare_row_data(payload)
                
                if key in existing_map:
                    updates.append((existing_map[key], row_data))
                else:
                    new_rows.append(row_data)
            
            # 既存レコードを更新（バッチで）
            if updates:
                for row_num, row_data in updates:
                    self._rate_limit_check()
                    self.worksheet.update([row_data], f"A{row_num}")
                    success_count += 1
            
            # 新規レコードを追加（バッチで）
            if new_rows:
                start_row = len(existing_data) + 1
                end_row = start_row + len(new_rows) - 1
                
                # 列文字の計算
                num_cols = len(self.headers)
                if num_cols <= 26:
                    col_letter = chr(65 + num_cols - 1)
                else:
                    first_letter_index = (num_cols - 1) // 26
                    second_letter_index = (num_cols - 1) % 26
                    col_letter = chr(65 + first_letter_index - 1) + chr(65 + second_letter_index)
                
                range_name = f"A{start_row}:{col_letter}{end_row}"
                
                # 一括更新
                self._rate_limit_check()
                self.worksheet.update(
                    new_rows,
                    range_name,
                    value_input_option='RAW'
                )
                
                success_count += len(new_rows)
                
        except Exception as e:
            self.logger.error(f"Batch upsert error: {e}")
            # エラー時は個別処理にフォールバック
            for payload in payloads:
                if self.upsert(payload):
                    success_count += 1
                time.sleep(0.5)
        
        return success_count

# ======================== Sync Manager ========================
class SyncManager:
    """メイン同期管理クラス（logging統合版）"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.scraper = CarwowScraper()
        self.processor = DataProcessor()
        self.supabase = SupabaseManager()
        self.sheets = GoogleSheetsManager()
        
        # DeepL APIキーの確認
        if not os.getenv('DEEPL_KEY'):
            self.logger.warning("DeepL API key not configured - translations will not be available")
        
        self.stats = {
            'total': 0,
            'success': 0,
            'failed': 0,
            'skipped': 0,
            'records_saved': 0,
            'errors': []
        }
    
    def sync_all(self, makers: Optional[List[str]] = None, limit: Optional[int] = None):
        """全データを同期"""
        self.logger.info("=" * 60)
        self.logger.info("Starting Carwow Data Sync")
        self.logger.info(f"Time: {datetime.now().isoformat()}")
        self.logger.info(f"Supabase: {'Enabled' if self.supabase.enabled else 'Disabled'}")
        self.logger.info(f"Google Sheets: {'Enabled' if self.sheets.enabled else 'Disabled'}")
        self.logger.info(f"DeepL Translation: {'Enabled' if os.getenv('DEEPL_KEY') else 'Disabled'}")
        self.logger.info(f"Exchange Rate: Auto-fetching enabled")
        self.logger.info("=" * 60)
        
        if makers is None:
            makers = self.scraper.get_all_makers()
            # 不要なメーカーを除外
            exclude = ['editorial', 'leasing', 'news', 'reviews', 'deals', 'advice']
            makers = [m for m in makers if m not in exclude]
        
        self.logger.info(f"Processing {len(makers)} makers")
        
        for maker_idx, maker in enumerate(makers):
            self.logger.info(f"\n[{maker_idx + 1}/{len(makers)}] Processing: {maker}")
            
            try:
                models = self.scraper.get_models_for_maker(maker)
                self.logger.info(f"  Found {len(models)} models")
                
                for model_idx, model_slug in enumerate(models):
                    if limit and self.stats['total'] >= limit:
                        self.logger.info("\nReached limit, stopping...")
                        self._print_statistics()
                        return
                    
                    self.stats['total'] += 1
                    self._process_vehicle(model_slug, model_idx + 1, len(models))
                    
                    # レート制限対策
                    time.sleep(0.5)
                    
            except Exception as e:
                error_msg = f"Error processing maker {maker}: {e}"
                self.logger.error(error_msg)
                self.stats['errors'].append(error_msg)
            
            finally:
                # クリーンアップ（メモリ解放）
                if maker_idx % 10 == 0:
                    import gc
                    gc.collect()
        
        # 最終クリーンアップ
        self.scraper.cleanup()
        self._print_statistics()
    
    def sync_specific(self, slugs: List[str]):
        """特定の車両のみ同期"""
        self.logger.info(f"Syncing {len(slugs)} specific vehicles")
        self.logger.info(f"Supabase: {'Enabled' if self.supabase.enabled else 'Disabled'}")
        self.logger.info(f"Google Sheets: {'Enabled' if self.sheets.enabled else 'Disabled'}")
        self.logger.info(f"DeepL Translation: {'Enabled' if os.getenv('DEEPL_KEY') else 'Disabled'}")
        self.logger.info("=" * 60)
        
        for idx, slug in enumerate(slugs):
            self.stats['total'] += 1
            self._process_vehicle(slug, idx + 1, len(slugs))
            time.sleep(0.5)
        
        # クリーンアップ
        self.scraper.cleanup()
        self._print_statistics()
    
    def _process_vehicle(self, slug: str, current: int, total: int):
        """個別車両を処理（販売終了処理追加）"""
        try:
            self.logger.info(f"  [{current}/{total}] {slug}...")
            
            # スクレイピング
            raw_data = self.scraper.scrape_vehicle(slug)
            
            if not raw_data:
                self.logger.info("NO DATA (redirect or not found)")
                # 販売終了処理：既存レコードを非アクティブにマーク
                self._mark_vehicle_inactive(slug)
                self.stats['skipped'] += 1
                return
            
            # データ処理（複数のレコードが返される）
            records = self.processor.process_vehicle_data(raw_data)
            
            # 各レコードを保存
            saved_count = 0
            for record in records:
                supabase_success = self.supabase.upsert(record)
                sheets_success = self.sheets.upsert(record)
                
                if supabase_success or sheets_success:
                    saved_count += 1
                    self.stats['records_saved'] += 1
            
            if saved_count > 0:
                self.logger.info(f"OK ({saved_count}/{len(records)} records)")
                self.stats['success'] += 1
            else:
                self.logger.error("FAILED")
                self.stats['failed'] += 1
                
        except Exception as e:
            error_msg = str(e)
            if "404" in error_msg:
                self.logger.info("NOT FOUND")
                self._mark_vehicle_inactive(slug)
                self.stats['skipped'] += 1
            elif "redirect" in error_msg.lower():
                self.logger.info("REDIRECT")
                self._mark_vehicle_inactive(slug)
                self.stats['skipped'] += 1
            else:
                self.logger.error(f"ERROR: {error_msg[:50]}")
                self.stats['failed'] += 1
            self.stats['errors'].append(f"{slug}: {error_msg}")
    
    def _mark_vehicle_inactive(self, slug: str):
        """車両を非アクティブにマーク（全グレード・エンジン対象）"""
        try:
            # デフォルトのグレード・エンジンでマーク
            self.supabase.mark_inactive(slug, 'Information not available', 'Information not available')
            self.sheets.mark_inactive(slug, 'Information not available', 'Information not available')
        except Exception as e:
            self.logger.error(f"Error marking {slug} as inactive: {e}")
    
    def _print_statistics(self):
        """統計情報を表示"""
        self.logger.info("\n" + "=" * 60)
        self.logger.info("Sync Statistics")
        self.logger.info("=" * 60)
        self.logger.info(f"Total vehicles processed: {self.stats['total']}")
        self.logger.info(f"Successful: {self.stats['success']}")
        self.logger.info(f"Failed: {self.stats['failed']}")
        self.logger.info(f"Skipped (redirects): {self.stats['skipped']}")
        self.logger.info(f"Total records saved: {self.stats['records_saved']}")
        
        if self.stats['errors']:
            self.logger.info(f"\nErrors (showing first 10):")
            for error in self.stats['errors'][:10]:
                self.logger.error(f"  - {error[:100]}")
        
        if self.stats['total'] > 0:
            success_rate = (self.stats['success'] / self.stats['total']) * 100
            self.logger.info(f"\nSuccess rate: {success_rate:.1f}%")
            
            if self.stats['success'] > 0:
                avg_records = self.stats['records_saved'] / self.stats['success']
                self.logger.info(f"Average records per vehicle: {avg_records:.1f}")

# ======================== CLI Interface ========================
def main():
    """メインエントリポイント"""
    # ログ設定を最初に実行
    logger = setup_logging()
    
    parser = argparse.ArgumentParser(
        description="Carwow Data Sync Manager",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python sync_manager.py --test                    # Test mode (5 vehicles)
  python sync_manager.py --makers audi bmw         # Specific makers
  python sync_manager.py --models audi/a4 bmw/x5   # Specific models
  python sync_manager.py --limit 10                # Limit to 10 vehicles
        """
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
        '--no-supabase',
        action='store_true',
        help='Disable Supabase sync'
    )
    
    parser.add_argument(
        '--no-sheets',
        action='store_true',
        help='Disable Google Sheets sync'
    )
    
    parser.add_argument(
        '--no-deepl',
        action='store_true',
        help='Disable DeepL translation'
    )
    
    args = parser.parse_args()
    
    # 環境変数の上書き
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
    
    # 少なくとも1つの同期先が有効かチェック
    if not manager.supabase.enabled and not manager.sheets.enabled:
        logger.error("No sync destination configured.")
        logger.error("Please set either Supabase or Google Sheets credentials.")
        sys.exit(1)
    
    try:
        if args.models:
            manager.sync_specific(args.models)
        else:
            manager.sync_all(makers=args.makers, limit=args.limit)
    
    except KeyboardInterrupt:
        logger.info("\n\nInterrupted by user")
        manager._print_statistics()
    
    except Exception as e:
        logger.error(f"\n\nFatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
