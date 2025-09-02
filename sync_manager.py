#!/usr/bin/env python3
"""
sync_manager.py - デバッグ版
エラー詳細を出力して問題を特定
"""
import os
import sys
import json
import time
import argparse
import traceback
from datetime import datetime
from pathlib import Path

print("=== Starting sync_manager.py ===")
print(f"Python version: {sys.version}")
print(f"Current directory: {os.getcwd()}")
print(f"Script location: {__file__}")

# 環境変数の確認
print("\n=== Environment Variables ===")
env_vars = {
    'SUPABASE_URL': os.getenv('SUPABASE_URL', 'NOT SET'),
    'SUPABASE_KEY': os.getenv('SUPABASE_KEY', 'NOT SET'),
    'GS_CREDS_JSON': os.getenv('GS_CREDS_JSON', 'NOT SET')[:50] if os.getenv('GS_CREDS_JSON') else 'NOT SET',
    'GS_SHEET_ID': os.getenv('GS_SHEET_ID', 'NOT SET'),
    'DEEPL_KEY': os.getenv('DEEPL_KEY', 'NOT SET')[:10] if os.getenv('DEEPL_KEY') else 'NOT SET'
}
for key, value in env_vars.items():
    print(f"{key}: {value}")

# モジュールのインポートテスト
print("\n=== Importing Modules ===")

try:
    import requests
    print("✓ requests imported successfully")
except ImportError as e:
    print(f"✗ Failed to import requests: {e}")
    sys.exit(1)

try:
    from bs4 import BeautifulSoup
    print("✓ BeautifulSoup imported successfully")
except ImportError as e:
    print(f"✗ Failed to import BeautifulSoup: {e}")
    sys.exit(1)

try:
    import gspread
    print("✓ gspread imported successfully")
except ImportError as e:
    print(f"✗ Failed to import gspread: {e}")
    sys.exit(1)

try:
    from google.oauth2.service_account import Credentials
    print("✓ google.oauth2 imported successfully")
except ImportError as e:
    print(f"✗ Failed to import google.oauth2: {e}")
    sys.exit(1)

# 他のモジュールをインポート
print("\n=== Importing Local Modules ===")
try:
    from carwow_scraper import CarwowScraper
    print("✓ CarwowScraper imported successfully")
except ImportError as e:
    print(f"✗ Failed to import CarwowScraper: {e}")
    traceback.print_exc()
    sys.exit(1)

try:
    from data_processor import DataProcessor
    print("✓ DataProcessor imported successfully")
except ImportError as e:
    print(f"✗ Failed to import DataProcessor: {e}")
    traceback.print_exc()
    sys.exit(1)

print("\n=== All imports successful ===")

# 簡単なテスト実行
def test_basic_functionality():
    """基本機能のテスト"""
    print("\n=== Testing Basic Functionality ===")
    
    try:
        # スクレイパーの初期化
        print("Initializing CarwowScraper...")
        scraper = CarwowScraper()
        print("✓ CarwowScraper initialized")
        
        # プロセッサーの初期化
        print("Initializing DataProcessor...")
        processor = DataProcessor()
        print("✓ DataProcessor initialized")
        
        # テストリクエスト
        print("\nTesting web request to carwow.co.uk...")
        response = requests.get("https://www.carwow.co.uk", timeout=10)
        print(f"✓ Response status: {response.status_code}")
        
        # メーカー一覧を取得（最初の3つだけ）
        print("\nFetching makers list...")
        makers = scraper.get_all_makers()
        print(f"✓ Found {len(makers)} makers")
        if makers:
            print(f"  First 3 makers: {makers[:3]}")
        
        return True
        
    except Exception as e:
        print(f"\n✗ Error during test: {e}")
        traceback.print_exc()
        return False

def main():
    """メインエントリポイント"""
    parser = argparse.ArgumentParser(description="Carwow Data Sync Manager - Debug Version")
    parser.add_argument('--test', action='store_true', help='Run in test mode')
    parser.add_argument('--makers', nargs='+', help='Specific makers to process')
    parser.add_argument('--models', nargs='+', help='Specific model slugs to process')
    parser.add_argument('--limit', type=int, help='Limit number of vehicles to process')
    parser.add_argument('--no-supabase', action='store_true', help='Disable Supabase sync')
    parser.add_argument('--no-sheets', action='store_true', help='Disable Google Sheets sync')
    parser.add_argument('--no-deepl', action='store_true', help='Disable DeepL translation')
    
    args = parser.parse_args()
    
    print(f"\n=== Command Line Arguments ===")
    print(f"Test mode: {args.test}")
    print(f"Makers: {args.makers}")
    print(f"Models: {args.models}")
    print(f"Limit: {args.limit}")
    print(f"No Supabase: {args.no_supabase}")
    print(f"No Sheets: {args.no_sheets}")
    print(f"No DeepL: {args.no_deepl}")
    
    # 基本機能テスト
    if not test_basic_functionality():
        print("\n✗ Basic functionality test failed")
        sys.exit(1)
    
    print("\n=== Test Complete ===")
    print("Basic setup appears to be working.")
    print("To run full sync, please ensure environment variables are set correctly.")
    
    # テストモードの場合、1台だけ処理してみる
    if args.test:
        print("\n=== Running Test Mode (1 vehicle) ===")
        try:
            scraper = CarwowScraper()
            processor = DataProcessor()
            
            # テスト用のslug
            test_slug = "audi/a3"
            print(f"Testing with: {test_slug}")
            
            raw_data = scraper.scrape_vehicle(test_slug)
            if raw_data:
                print(f"✓ Scraped data for {test_slug}")
                records = processor.process_vehicle_data(raw_data)
                print(f"✓ Processed {len(records)} records")
                if records:
                    print(f"  Sample record ID: {records[0].get('id')}")
                    print(f"  Model: {records[0].get('model_en')}")
            else:
                print(f"✗ No data returned for {test_slug}")
                
        except Exception as e:
            print(f"✗ Test mode error: {e}")
            traceback.print_exc()
    
    print("\n=== Script Completed ===")
    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print(f"\n✗ Fatal error: {e}")
        traceback.print_exc()
        sys.exit(1)
