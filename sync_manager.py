#!/usr/bin/env python3
"""
sync_manager.py - 完全版（為替API・DeepL対応）
実行管理とデータベース・スプレッドシート同期モジュール
"""
import os
import sys
import json
import time
import argparse
from datetime import datetime
from typing import Dict, List, Optional, Any
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
DEEPL_KEY = os.getenv("DEEPL_KEY")  # DeepL APIキーを環境変数から取得

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
            
            # データ型の変換
            clean_payload = self._prepare_payload(payload)
            
            response = requests.post(
                f"{self.url}/rest/v1/cars",
                headers=headers,
                json=clean_payload,
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
                    # "Information not available"を含む場合は空配列にする
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
            
            # 数値型のフィールド（"Information not available"をスキップ）
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
            
            # その他のフィールド（"Information not available"をNULLに変換）
            else:
                if value == 'Information not available' or value == 'ー':
                    # データベースではNULLとして扱う（キーを含めない）
                    pass
                else:
                    clean[key] = value
        
        return clean

# ======================== Google Sheets Manager ========================
class GoogleSheetsManager:
    """Google Sheets管理"""
    
    def __init__(self):
        self.worksheet = None
        self.enabled = False
        self.headers = SHEET_HEADERS
        self._initialize()
    
    def _initialize(self):
        """Google Sheets接続を初期化"""
        if not (GS_CREDS_JSON and GS_SHEET_ID):
            print("Warning: Google Sheets credentials not configured")
            return
        
        try:
            # 認証情報の処理
            if GS_CREDS_JSON.startswith('{'):
                # JSON文字列の場合
                creds_data = json.loads(GS_CREDS_JSON)
            else:
                # ファイルパスの場合
                with open(GS_CREDS_JSON, 'r') as f:
                    creds_data = json.load(f)
            
            # 一時ファイルに保存
            creds_path = Path("temp_creds.json")
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
            
            # 一時ファイルを削除
            creds_path.unlink()
            
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
            print(f"Connected to Google Sheets: {SHEET_NAME}")
            
        except Exception as e:
            print(f"Google Sheets initialization error: {e}")
            self.enabled = False
    
    def _setup_headers(self):
        """ヘッダー行を設定"""
        try:
            current_headers = self.worksheet.row_values(1)
            if not current_headers or current_headers != self.headers:
                self.worksheet.update([self.headers], "A1")
        except Exception as e:
            print(f"Error setting headers: {e}")
    
    def upsert(self, payload: Dict) -> bool:
        """データをUPSERT"""
        if not self.enabled:
            return False
        
        try:
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
                    if len(row) > 7:  # slug, grade, engine列が存在
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
            self.worksheet.update(
                [row_data], 
                f"A{row_num}",
                value_input_option='RAW'
            )
            
            return True
            
        except Exception as e:
            print(f"Sheets upsert error: {e}")
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
                # 配列は文字列結合
                if value == ['Information not available'] or value == ['ー']:
                    row_data.append('')
                else:
                    row_data.append(', '.join(str(v) for v in value))
            elif isinstance(value, dict):
                # 辞書はJSON文字列
                row_data.append(json.dumps(value, ensure_ascii=False))
            elif isinstance(value, datetime):
                # 日時はISO形式
                row_data.append(value.isoformat())
            else:
                row_data.append(str(value))
        
        return row_data
    
    def batch_upsert(self, payloads: List[Dict]) -> int:
        """複数データを一括更新"""
        if not self.enabled:
            return 0
        
        success_count = 0
        
        # バッチ処理のためにデータを準備
        all_data = []
        for payload in payloads:
            row_data = self._prepare_row_data(payload)
            all_data.append(row_data)
        
        try:
            # 既存データを取得
            existing_data = self.worksheet.get_all_values()
            
            # 新規データを追加
            if all_data:
                start_row = len(existing_data) + 1
                end_row = start_row + len(all_data) - 1
                
                # 正しい列計算
                num_cols = len(self.headers)
                if num_cols <= 26:
                    col_letter = chr(65 + num_cols - 1)  # A=1, B=2, ..., Z=26
                else:
                    # AA, AB, AC...の形式（正しい計算）
                    first_letter_index = (num_cols - 1) // 26
                    second_letter_index = (num_cols - 1) % 26
                    col_letter = chr(65 + first_letter_index - 1) + chr(65 + second_letter_index)
                
                range_name = f"A{start_row}:{col_letter}{end_row}"
                
                # 一括更新
                self.worksheet.update(
                    all_data,
                    range_name,
                    value_input_option='RAW'
                )
                
                success_count = len(all_data)
                
        except Exception as e:
            print(f"Batch upsert error: {e}")
            # エラー時は個別処理にフォールバック
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
        self.supabase = SupabaseManager()
        self.sheets = GoogleSheetsManager()
        
        # DeepL APIキーの確認
        if not os.getenv('DEEPL_KEY'):
            print("Warning: DeepL API key not configured - translations will not be available")
        
        self.stats = {
            'total': 0,
            'success': 0,
            'failed': 0,
            'skipped': 0,  # リダイレクトでスキップされた数
            'records_saved': 0,
            'errors': []
        }
    
    def sync_all(self, makers: Optional[List[str]] = None, limit: Optional[int] = None):
        """全データを同期"""
        print("=" * 60)
        print("Starting Carwow Data Sync")
        print(f"Time: {datetime.now().isoformat()}")
        print(f"Supabase: {'Enabled' if self.supabase.enabled else 'Disabled'}")
        print(f"Google Sheets: {'Enabled' if self.sheets.enabled else 'Disabled'}")
        print(f"DeepL Translation: {'Enabled' if os.getenv('DEEPL_KEY') else 'Disabled'}")
        print(f"Exchange Rate: Auto-fetching enabled")
        print("=" * 60)
        
        if makers is None:
            makers = self.scraper.get_all_makers()
            # 不要なメーカーを除外
            exclude = ['editorial', 'leasing', 'news', 'reviews', 'deals', 'advice']
            makers = [m for m in makers if m not in exclude]
        
        print(f"Processing {len(makers)} makers")
        
        for maker_idx, maker in enumerate(makers):
            print(f"\n[{maker_idx + 1}/{len(makers)}] Processing: {maker}")
            
            try:
                models = self.scraper.get_models_for_maker(maker)
                print(f"  Found {len(models)} models")
                
                for model_idx, model_slug in enumerate(models):
                    if limit and self.stats['total'] >= limit:
                        print("\nReached limit, stopping...")
                        self._print_statistics()
                        return
                    
                    self.stats['total'] += 1
                    self._process_vehicle(model_slug, model_idx + 1, len(models))
                    
                    # レート制限対策
                    time.sleep(0.5)
                    
            except Exception as e:
                print(f"  Error processing maker {maker}: {e}")
                self.stats['errors'].append(f"Maker {maker}: {str(e)}")
        
        self._print_statistics()
    
    def sync_specific(self, slugs: List[str]):
        """特定の車両のみ同期"""
        print(f"Syncing {len(slugs)} specific vehicles")
        print(f"Supabase: {'Enabled' if self.supabase.enabled else 'Disabled'}")
        print(f"Google Sheets: {'Enabled' if self.sheets.enabled else 'Disabled'}")
        print(f"DeepL Translation: {'Enabled' if os.getenv('DEEPL_KEY') else 'Disabled'}")
        print("=" * 60)
        
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
            
            if not raw_data:
                print("NO DATA (redirect or not found)")
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
                print(f"OK ({saved_count}/{len(records)} records)")
                self.stats['success'] += 1
            else:
                print("FAILED")
                self.stats['failed'] += 1
                
        except Exception as e:
            error_msg = str(e)
            if "404" in error_msg:
                print("NOT FOUND")
                self.stats['skipped'] += 1
            elif "redirect" in error_msg.lower():
                print("REDIRECT")
                self.stats['skipped'] += 1
            else:
                print(f"ERROR: {error_msg[:50]}")
                self.stats['failed'] += 1
            self.stats['errors'].append(f"{slug}: {error_msg}")
    
    def _print_statistics(self):
        """統計情報を表示"""
        print("\n" + "=" * 60)
        print("Sync Statistics")
        print("=" * 60)
        print(f"Total vehicles processed: {self.stats['total']}")
        print(f"Successful: {self.stats['success']}")
        print(f"Failed: {self.stats['failed']}")
        print(f"Skipped (redirects): {self.stats['skipped']}")
        print(f"Total records saved: {self.stats['records_saved']}")
        
        if self.stats['errors']:
            print(f"\nErrors (showing first 10):")
            for error in self.stats['errors'][:10]:
                print(f"  - {error[:100]}")
        
        if self.stats['total'] > 0:
            success_rate = (self.stats['success'] / self.stats['total']) * 100
            print(f"\nSuccess rate: {success_rate:.1f}%")
            
            if self.stats['success'] > 0:
                avg_records = self.stats['records_saved'] / self.stats['success']
                print(f"Average records per vehicle: {avg_records:.1f}")

# ======================== CLI Interface ========================
def main():
    """メインエントリポイント"""
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
        print("Running in TEST mode (limit=5)")
    
    manager = SyncManager()
    
    # 少なくとも1つの同期先が有効かチェック
    if not manager.supabase.enabled and not manager.sheets.enabled:
        print("Error: No sync destination configured.")
        print("Please set either Supabase or Google Sheets credentials.")
        sys.exit(1)
    
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
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
