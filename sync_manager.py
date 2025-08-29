#!/usr/bin/env python3
"""
sync_manager.py - 改良版
実行管理とデータベース同期モジュール
"""
import os
import sys
import json
import time
import argparse
from datetime import datetime
from typing import Dict, List, Optional, Any

import requests

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
                f"{self.url}/rest/v1/cars",  # テーブル名を適切に設定
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
            # Noneの値は除外
            if value is None:
                continue
            
            # 配列型のフィールド
            if key in ['body_type', 'body_type_ja', 'colors', 'colors_ja', 'media_urls']:
                if isinstance(value, list):
                    clean[key] = value
                else:
                    clean[key] = []
            
            # JSONB型のフィールド
            elif key == 'spec_json':
                if isinstance(value, dict):
                    clean[key] = value
                else:
                    clean[key] = {}
            
            # 数値型のフィールド
            elif key in ['price_min_gbp', 'price_max_gbp', 'price_used_gbp',
                        'price_min_jpy', 'price_max_jpy', 'price_used_jpy']:
                if value is not None:
                    clean[key] = float(value)
            
            # 整数型のフィールド  
            elif key in ['doors', 'seats', 'power_bhp']:
                if value is not None:
                    clean[key] = int(value)
            
            # その他はそのまま
            else:
                clean[key] = value
        
        return clean

# ======================== Sync Manager ========================
class SyncManager:
    """メイン同期管理クラス"""
    
    def __init__(self):
        self.scraper = CarwowScraper()
        self.processor = DataProcessor()
        self.supabase = SupabaseManager()
        
        self.stats = {
            'total': 0,
            'success': 0,
            'failed': 0,
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
            # 不要なメーカーを除外
            makers = [m for m in makers if m not in ['editorial', 'leasing', 'news', 'reviews']]
        
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
            
            # データ処理（複数のレコードが返される）
            records = self.processor.process_vehicle_data(raw_data)
            
            # 各レコードを保存
            saved_count = 0
            for record in records:
                if self.supabase.upsert(record):
                    saved_count += 1
            
            if saved_count > 0:
                print(f"OK ({saved_count}/{len(records)} records)")
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
        
        if self.stats['errors']:
            print(f"\nErrors (showing first 10):")
            for error in self.stats['errors'][:10]:
                print(f"  - {error}")
        
        if self.stats['total'] > 0:
            success_rate = (self.stats['success'] / self.stats['total']) * 100
            print(f"\nSuccess rate: {success_rate:.1f}%")

# ======================== CLI Interface ========================
def main():
    """メインエントリポイント"""
    parser = argparse.ArgumentParser(
        description="Carwow Data Sync Manager"
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
        sys.exit(1)

if __name__ == "__main__":
    main()
