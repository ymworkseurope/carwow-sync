#!/usr/bin/env python3
"""
sync_manager.py - 修正版
ロギング統合、レート制限改善、is_active管理追加
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
    logging.error(f"Error importing modules: {e}")
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
    "full_model_ja", "updated_at", "spec_json", "is_active"
]

# ======================== Database Manager ========================
class SupabaseManager:
    """Supabaseデータベース管理"""
    
    def __init__(self):
        self.url = SUPABASE_URL
        self.key = SUPABASE_KEY
        self.enabled = bool(self.url and self.key)
        
        if not self.enabled:
            logger.warning("Supabase credentials not configured")
    
    def upsert(self, payload: Dict) -> bool:
        """データをUPSERT（ID重複時は更新）"""
        if not self.enabled:
            return False
        
        try:
            headers = {
                'apikey': self.key,
                'Authorization': f'Bearer {self.key}',
                'Content-Type': 'application/json',
                'Prefer': 'resolution=merge-duplicates,return=minimal'
            }
            
            # データ型の変換
            clean_payload = self._prepare_payload(payload)
            
            # IDベースでUPSERT
            response = requests.post(
                f"{self.url}/rest/v1/cars",
                headers=headers,
                json=clean_payload,
                timeout=30
            )
            
            if response.status_code in [200, 201, 204]:
                return True
            elif response.status_code == 409 and 'id' in clean_payload:
                # 重複エラーの場合はIDベースでUPDATE
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
                    logger.error(f"Supabase update error {update_response.status_code}: {update_response.text[:200]}")
                    return False
            else:
                logger.error(f"Supabase error {response.status_code}: {response.text[:200]}")
                return False
                
        except Exception as e:
            logger.error(f"Supabase upsert error: {e}")
            return False
    
    def mark_inactive(self, slug: str, grade: str = None, engine: str = None) -> bool:
        """特定のレコードをis_active=Falseに更新"""
        if not self.enabled:
            return False
        
        try:
            headers = {
                'apikey': self.key,
                'Authorization': f'Bearer {self.key}',
                'Content-Type': 'application/json',
                'Prefer': 'return=minimal'
            }
            
            # 更新対象の特定
            update_url = f"{self.url}/rest/v1/cars?slug=eq.{slug}"
            if grade:
                update_url += f"&grade=eq.{grade}"
            if engine:
                update_url += f"&engine=eq.{engine}"
            
            # is_activeとlast_updatedを更新
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
            
            if response.status_code in [200, 204]:
                logger.info(f"Marked as inactive: {slug} {grade or ''} {engine or ''}")
                return True
            else:
                logger.error(f"Failed to mark inactive: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"Error marking inactive: {e}")
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
            
            # ブール型のフィールド
            elif key == 'is_active':
                clean[key] = bool(value)
            
            # その他のフィールド
            else:
                if value == 'Information not available' or value == 'ー':
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
        self.last_request_time = 0
        self.request_count = 0
        self.rate_limit_per_100_seconds = 95  # 最適化されたレート制限
        self._initialize()
    
    def _rate_limit_check(self):
        """Google Sheets APIレート制限チェック"""
        current_time = time.time()
        
        # 100秒経過したらカウントリセット
        if current_time - self.last_request_time > 100:
            self.request_count = 0
            self.last_request_time = current_time
        
        # レート制限に達したら待機
        if self.request_count >= self.rate_limit_per_100_seconds:
            sleep_time = 100 - (current_time - self.last_request_time)
            if sleep_time > 0:
                logger.info(f"Rate limit reached, waiting {sleep_time:.1f} seconds...")
                time.sleep(sleep_time)
                self.request_count = 0
                self.last_request_time = time.time()
        
        self.request_count += 1
    
    def _initialize(self):
        """Google Sheets接続を初期化"""
        if not (GS_CREDS_JSON and GS_SHEET_ID):
            logger.warning("Google Sheets credentials not configured")
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
            logger.info(f"Connected to Google Sheets: {SHEET_NAME}")
            
        except Exception as e:
            logger.error(f"Google Sheets initialization error: {e}")
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
            logger.error(f"Error setting headers: {e}")
    
    def upsert(self, payload: Dict) -> bool:
        """データをUPSERT"""
        if not self.enabled:
            return False
        
        try:
            # レート制限チェック
            self._rate_limit_check()
            
            # IDベースでの検索
            id_value = payload.get('id')
            if not id_value:
                return False
            
            # 既存の行を検索
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
            logger.error(f"Sheets upsert error: {e}")
            return False
    
    def mark_inactive(self, slug: str, grade: str = None, engine: str = None) -> bool:
        """特定のレコードをis_active=Falseに更新"""
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
                        
                        # is_activeカラムのインデックスを取得
                        is_active_index = self.headers.index('is_active')
                        updated_at_index = self.headers.index('updated_at')
                        
                        # 更新
                        self._rate_limit_check()
                        self.worksheet.update_cell(i, is_active_index + 1, 'FALSE')
                        self.worksheet.update_cell(i, updated_at_index + 1, datetime.now().isoformat())
                        
                        logger.info(f"Marked as inactive in Sheets: {slug} {grade or ''} {engine or ''}")
                        return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error marking inactive in Sheets: {e}")
            return False
