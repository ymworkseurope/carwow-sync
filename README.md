# Carwow Vehicle Data Sync System

英国の自動車情報サイト Carwow から車両データを自動収集し、Supabase と Google Sheets に同期するシステム。

## システム構成
```
carwow_scraper.py    # データ取得（スクレイピング）
data_processor.py    # データ変換・翻訳処理
sync_manager.py      # 実行管理・DB同期
```

## セットアップ

### 環境変数
```bash
SUPABASE_URL=https://xxx.supabase.co
SUPABASE_KEY=eyJhbGci...
GS_CREDS_JSON={"type":"service_account",...}  # 1行化
GS_SHEET_ID=1ABC...xyz
```

### データベース構造（Supabase）

```sql
CREATE TABLE cars (
  id BIGINT PRIMARY KEY,
  slug TEXT NOT NULL,
  make_en TEXT,
  model_en TEXT,
  make_ja TEXT,
  model_ja TEXT,
  grade TEXT,
  engine TEXT,
  engine_price_gbp INTEGER,
  engine_price_jpy INTEGER,
  body_type TEXT[],
  body_type_ja TEXT[],
  fuel TEXT,
  fuel_ja TEXT,
  transmission TEXT,
  transmission_ja TEXT,
  drive_type TEXT,
  drive_type_ja TEXT,
  power_bhp INTEGER,
  price_min_gbp INTEGER,
  price_max_gbp INTEGER,
  price_used_gbp INTEGER,
  price_min_jpy INTEGER,
  price_max_jpy INTEGER,
  price_used_jpy INTEGER,
  overview_en TEXT,
  overview_ja TEXT,
  doors INTEGER,
  seats INTEGER,
  dimensions_mm TEXT,
  dimensions_ja TEXT,
  colors TEXT[],
  colors_ja TEXT[],
  media_urls TEXT[],
  catalog_url TEXT,
  full_model_ja TEXT,
  spec_json JSONB,
  updated_at TIMESTAMPTZ DEFAULT NOW()
);
```

### Google Sheets構造
自動作成される `system_cars` シートのカラム：

| カラム | 型 | 説明 |
|--------|-----|------|
| id | BIGINT | ユニークID |
| slug | TEXT | メーカー/モデル形式 |
| make_en/ja | TEXT | メーカー名 |
| model_en/ja | TEXT | モデル名 |
| grade | TEXT | グレード |
| engine | TEXT | エンジン |
| engine_price_gbp/jpy | NUMBER | エンジン別価格 |
| fuel/fuel_ja | TEXT | 燃料タイプ |
| transmission/transmission_ja | TEXT | トランスミッション |
| drive_type/drive_type_ja | TEXT | 駆動方式 |
| power_bhp | NUMBER | パワー |
| price_*_gbp/jpy | NUMBER | 価格情報 |
| body_type/body_type_ja | JSON | ボディタイプ |
| colors/colors_ja | JSON | カラー |
| media_urls | JSON | 画像URL配列 |
| spec_json | JSON | 詳細スペック |

## 実行方法

### コマンドライン
```bash
# 全データ同期
python sync_manager.py

# テストモード（5件）
python sync_manager.py --test

# 特定メーカー
python sync_manager.py --makers audi bmw

# 特定モデル
python sync_manager.py --models audi/a4 bmw/x5

# 処理数制限
python sync_manager.py --limit 10

# Supabaseのみ
python sync_manager.py --no-sheets

# Sheetsのみ
python sync_manager.py --no-supabase
```

### GitHub Actions
- 毎日 JST 02:00 自動実行
- 手動実行: Actions → "Daily Carwow Sync" → Run workflow

## データ構造の特徴

### エンジン単位レコード
各車両のグレード×エンジンの組み合わせで別レコードを生成：
- `audi/a4` に 3グレード × 2エンジン = 6レコード
- 重複排除: 同一グレード+エンジンは1レコードに統合

### 価格情報
- `price_min/max_gbp`: 車種全体の価格帯
- `engine_price_gbp`: エンジン別個別価格
- 自動GBP→JPY変換（レート185）

### 日本語対応
- メーカー名、燃料タイプ、駆動方式の自動翻訳
- `full_model_ja`: 日本語フルモデル名生成

### メディア処理
- 最大10枚の高解像度画像URL取得
- `thumbnail-carousel-vertical__img` クラスから優先取得
- 重複排除とURL最適化

## スクレイピング戦略

### 対象ページ
- `/brands` - メーカー一覧
- `/{maker}` - モデル一覧  
- `/{maker}/{model}` - 基本情報
- `/{maker}/{model}/specifications` - 詳細スペック
- `/{maker}/{model}/colours` - カラー情報

### データ取得方法
1. **価格**: `deals-cta-list__rrp-price` → `summary-list__item` → テキスト全体
2. **画像**: `thumbnail-carousel-vertical__img` → その他imgタグ
3. **スペック**: specifications ページ → メインページフォールバック
4. **グレード/エンジン**: `trim-article` セクション別処理

## エラーハンドリング
- 個別エラーは全体を止めない
- 404/タイムアウトは自動スキップ
- フォールバック処理（specifications → main page）
- デフォルトレコード生成（データなし時）

## パフォーマンス
- 0.5秒間隔でレート制限対策
- バッチ処理（Google Sheets）
- 重複排除と最適化
- タイムアウト設定（30秒）
