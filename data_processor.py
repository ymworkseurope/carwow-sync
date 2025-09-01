#!/usr/bin/env python3
"""
data_processor.py - Improved Version with Better Japanese Support
エンジン単位でレコードを生成、ID生成を改善、full_model_ja改良
"""
import re
import json
import os
import time
import hashlib
import logging
from typing import Dict, List, Optional
from datetime import datetime
import requests

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('processor.log'),
        logging.StreamHandler()
    ]
)

class ExchangeRateAPI:
    """為替レートAPI管理クラス"""
    
    def __init__(self):
        self.cache_file = 'exchange_rate_cache.json'
        self.cache_duration = 3600  # 1時間キャッシュ
        self.rate = None
        self.logger = logging.getLogger(__name__)
        self._load_cached_rate()
    
    def _load_cached_rate(self):
        """キャッシュされた為替レートを読み込み"""
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, 'r') as f:
                    cache = json.load(f)
                    if time.time() - cache.get('timestamp', 0) < self.cache_duration:
                        self.rate = cache.get('rate')
                        self.logger.info(f"Using cached exchange rate: 1 GBP = {self.rate} JPY")
            except:
                pass
    
    def get_rate(self) -> float:
        """GBPからJPYへの為替レートを取得"""
        if self.rate:
            return self.rate
        
        try:
            # 無料の為替APIを使用
            response = requests.get(
                'https://api.exchangerate-api.com/v4/latest/GBP',
                timeout=10
            )
            if response.status_code == 200:
                data = response.json()
                self.rate = data['rates']['JPY']
                
                # キャッシュに保存
                with open(self.cache_file, 'w') as f:
                    json.dump({
                        'rate': self.rate,
                        'timestamp': time.time()
                    }, f)
                
                self.logger.info(f"Fetched exchange rate: 1 GBP = {self.rate} JPY")
                return self.rate
        except Exception as e:
            self.logger.error(f"Error fetching exchange rate: {e}")
        
        # フォールバック値（設定可能にする）
        self.rate = float(os.getenv('FALLBACK_EXCHANGE_RATE', '185.0'))
        self.logger.warning(f"Using fallback exchange rate: 1 GBP = {self.rate} JPY")
        return self.rate

class DeepLTranslator:
    """DeepL翻訳クラス（クォータ管理付き）"""
    
    def __init__(self):
        self.api_key = os.getenv('DEEPL_KEY')
        self.enabled = bool(self.api_key)
        self.cache = {}
        self.cache_file = 'translation_cache.json'
        self.quota_file = 'deepl_quota.json'
        self.quota_limit = 500000  # Free版の月間制限
        self.quota_used = 0
        self.logger = logging.getLogger(__name__)
        
        # 拡張カラー辞書
        self.color_map = self._load_color_map()
        
        self._load_cache()
        self._load_quota()
        
        if not self.enabled:
            self.logger.warning("DeepL API key not configured")
    
    def _load_color_map(self) -> Dict[str, str]:
        """カラー翻訳辞書を読み込み"""
        return {
            # 基本色
            'White': 'ホワイト',
            'Black': 'ブラック',
            'Silver': 'シルバー',
            'Grey': 'グレー',
            'Gray': 'グレー',
            'Red': 'レッド',
            'Blue': 'ブルー',
            'Green': 'グリーン',
            'Yellow': 'イエロー',
            'Orange': 'オレンジ',
            'Brown': 'ブラウン',
            'Pearl': 'パール',
            'Metallic': 'メタリック',
            
            # 拡張カラー（提供されたリストから）
            'Abarth Red': 'アバルトレッド',
            'Abyss Blue': 'アビスブルー',
            'Acid Green': 'アシッドグリーン',
            'Acropolis Orange': 'アクロポリスオレンジ',
            'Alpine Blue': 'アルピーヌブルー',
            'Vision Blue': 'ビジョンブルー',
            'Aluminite Silver': 'アルミナイトシルバー',
            'AM Heritage Racing Green': 'AMヘリテージレーシンググリーン',
            'Antidote White': 'アンチドートホワイト',
            'Apex Grey': 'エイペックスグレー',
            'Arden Green': 'アーデングリーン',
            'Arese Grey': 'アレーゼグレー',
            'Arizona Bronze': 'アリゾナブロンズ',
            'Ash Green': 'アッシュグリーン',
            'Asphalt Grey': 'アスファルトグレー',
            'Aston Martin Racing Green': 'アストンマーティンレーシンググリーン',
            'Banchise White': 'バンキーズホワイト',
            'Black Pearl': 'ブラックパール',
            'Blush Pearl': 'ブラッシュパール',
            'Bordeaux Pontevecchio': 'ボルドーポンテヴェッキオ',
            'Brera Red': 'ブレラレッド',
            'Buckinghamshire Green': 'バッキンガムシャーグリーン',
            'California Sage': 'カリフォルニアセージ',
            'Campovolo Grey': 'カンポヴォーログレー',
            'Carbon Black': 'カーボンブラック',
            'Caribbean Blue Pearl': 'カリビアンブルーパール',
            'Casino Royale': 'カジノロワイヤル',
            'Ceramic Blue': 'セラミックブルー',
            'Ceramic Grey': 'セラミックグレー',
            'Chiltern Green': 'チルターングリーン',
            'China Grey': 'チャイナグレー',
            'Cinnabar Orange': 'シナバーオレンジ',
            'Circuit Grey': 'サーキットグレー',
            'Clubsport White': 'クラブスポーツホワイト',
            'Concours Blue': 'コンクールブルー',
            'Coral Orange': 'コーラルオレンジ',
            'Cordolo Red': 'コルドロレッド',
            'Cornaline Beige': 'コーネリアンベージュ',
            'Cosmos Orange': 'コスモスオレンジ',
            'Cosmopolitan Yellow': 'コスモポリタンイエロー',
            'Cumberland Grey': 'カンバーランドグレー',
            'Dark Blue': 'ダークブルー',
            'Deep Black': 'ディープブラック',
            'Digital Violet': 'デジタルバイオレット',
            'Divine Red': 'ディヴァインレッド',
            'Dubonnet Rosso': 'デュボネロッソ',
            'Dune': 'デューン',
            'Eclipse Mat': 'エクリプスマット',
            'Electronic Blue': 'エレクトロニックブルー',
            'Elwood Blue': 'エルウッドブルー',
            'Etna Red': 'エトナレッド',
            'Frosted Glass Blue': 'フロステッドグラスブルー',
            'Frosted Glass Yellow': 'フロステッドグラスイエロー',
            'Gara White': 'ガーラホワイト',
            'Ghiaccio White': 'ギアッチョホワイト',
            'Glacier White': 'グレイシャーホワイト',
            'Golden Saffron': 'ゴールデンサフラン',
            'Green Monza': 'グリーンモンツァ',
            'Heather Pink': 'ヘザーピンク',
            'Hypnotic Purple': 'ヒプノティックパープル',
            'Hyper Red': 'ハイパーレッド',
            'Intense Blue': 'インテンスブルー',
            'Ion Blue': 'イオンブルー',
            'Iridescent Emerald': 'イリデセントエメラルド',
            'Jet Black': 'ジェットブラック',
            'Kermit Green': 'カーミットグリーン',
            'Kopi Bronze': 'コピブロンズ',
            'Legends Blue': 'レジェンズブルー',
            'Lightning Silver': 'ライトニングシルバー',
            'Lime Essence': 'ライムエッセンス',
            'Lipari Ochre': 'リパーリオークル',
            'Liquid Crimson': 'リキッドクリムゾン',
            'Lunar White': 'ルナーホワイト',
            'Magnetic Silver': 'マグネティックシルバー',
            'Magneto Bronze': 'マグニートブロンズ',
            'Mako Blue': 'マコブルー',
            'Marron Black': 'マロンブラック',
            'Midnight Blue': 'ミッドナイトブルー',
            'Ming Blue': 'ミンブルー',
            'Minotaur Green': 'ミノタウロスグリーン',
            'Misano Blue': 'ミザーノブルー',
            'Modena Yellow': 'モデナイエロー',
            'Montecarlo Blue': 'モンテカルロブルー',
            'Montreal Green': 'モントリオールグリーン',
            'Morning Frost White': 'モーニングフロストホワイト',
            'Navigli Blue': 'ナヴィッリブルー',
            'Neptune Blue': 'ネプチューンブルー',
            'Neutron White': 'ニュートロンホワイト',
            'Nival White': 'ニバルホワイト',
            'Normandy Green': 'ノルマンディーグリーン',
            'Oberon Black': 'オベロンブラック',
            'Ocellus Teal': 'オセラスティール',
            'Officina Red': 'オフィチーナレッド',
            'Onyx Black': 'オニキスブラック',
            'Passione Red': 'パッシオーネレッド',
            'Peacock Blue': 'ピーコックブルー',
            'Pearl Blonde': 'パールブロンド',
            'Pentland Green': 'ペントランドグリーン',
            'Performance Grey': 'パフォーマンスグレー',
            'Photon Lime': 'フォトンライム',
            'Plasma Blue': 'プラズマブルー',
            'Platinum White': 'プラチナホワイト',
            'Podium Blue': 'ポディウムブルー',
            'Podium Green': 'ポディウムグリーン',
            'Poison Blue': 'ポイズンブルー',
            'Quantum Silver': 'クアンタムシルバー',
            'Racing Blue': 'レーシングブルー',
            'Racing Orange': 'レーシングオレンジ',
            'Rally Beige': 'ラリーベージュ',
            'Record Grey': 'レコードグレー',
            'Riva Blue': 'リーヴァブルー',
            'Royal Indigo': 'ロイヤルインディゴ',
            'Ruby Red': 'ルビーレッド',
            'Sabiro Blue': 'サビロブルー',
            'Satin Jet Black': 'サテンジェットブラック',
            'Scala Ivory': 'スカーラアイボリー',
            'Scintilla Silver': 'シンティラシルバー',
            'Scorpion Black': 'スコーピオンブラック',
            'Scorpione Black': 'スコルピオーネブラック',
            'Scorpus Red': 'スコーパスレッド',
            'Sempione White': 'センピオーネホワイト',
            'Seismic Red': 'セイズミックレッド',
            'Seychelles Blue': 'セイシェルブルー',
            'Shock Orange': 'ショックオレンジ',
            'Silver Birch': 'シルバーバーチ',
            'Silverstone Grey': 'シルバーストーングレー',
            'Skyfall Silver': 'スカイフォールシルバー',
            'Solar Bronze': 'ソーラーブロンズ',
            'Solar Orange': 'ソーラーオレンジ',
            'Spirit Silver': 'スピリットシルバー',
            'Steel Grey': 'スチールグレー',
            'Stirling Green': 'スターリンググリーン',
            'Storm Blue': 'ストームブルー',
            'Storm Purple': 'ストームパープル',
            'Stratus White': 'ストラタスホワイト',
            'Supernova Red': 'スーパーノヴァレッド',
            'Synapse Orange': 'シナプスオレンジ',
            'Thunder Grey': 'サンダーグレー',
            'Titanium Grey': 'チタニウムグレー',
            'Tornado Grey': 'トルネードグレー',
            'Tortona Black': 'トルトーナブラック',
            'Trofeo Grey': 'トロフェオグレー',
            'Trofeo White': 'トロフェオホワイト',
            'Tungsten Silver': 'タングステンシルバー',
            'Ultra Yellow': 'ウルトライエロー',
            'Ultramarine Black': 'ウルトラマリンブラック',
            'Vanilla': 'バニラ',
            'Venom Black': 'ヴェノムブラック',
            'Vesuvio Grey': 'ヴェスヴィオグレー',
            'Volcano Red': 'ヴォルケーノレッド',
            'Vulcano Black': 'ヴルカーノブラック',
            'White Stone': 'ホワイトストーン',
            'Xenon Grey': 'ゼノングレー',
            'Yellow Tang': 'イエロータング',
            'Zaffre Blue': 'ザファイヤブルー',
            'Zenith White': 'ゼニスホワイト',
            
            # ペイントタイプ
            'Solid': 'ソリッド',
            'Special Solid': 'スペシャルソリッド',
            'Metallic': 'メタリック',
            'Special Metallic': 'スペシャルメタリック',
            'Mica': 'マイカ',
            'Pearl': 'パール',
            'Pearlescent': 'パールセント',
            'Pastel': 'パステル',
            'Special Pastel': 'スペシャルパステル',
            'Matt': 'マット',
            'Matte': 'マット',
            'Special Matt Paint': 'スペシャルマットペイント',
            'Satin': 'サテン',
            'Tri-coat': 'トライコート',
            'Bi-Colour': 'バイカラー',
            'Two Tone': 'ツートン',
            'Premium': 'プレミアム',
            'Signature': 'シグネチャー',
            'Contemporary': 'コンテンポラリー',
            'Exclusive': 'エクスクルーシブ',
            'Heritage': 'ヘリテージ',
            'Icon': 'アイコン',
            'Provenance': 'プロヴェナンス',
            'Commission': 'コミッション',
            'Tinted Clear Coat': 'ティンテッドクリアコート'
        }
    
    def _load_cache(self):
        """翻訳キャッシュを読み込み"""
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, 'r') as f:
                    self.cache = json.load(f)
            except:
                self.cache = {}
    
    def _save_cache(self):
        """翻訳キャッシュを保存"""
        try:
            with open(self.cache_file, 'w') as f:
                json.dump(self.cache, f, ensure_ascii=False, indent=2)
        except:
            pass
    
    def _load_quota(self):
        """クォータ使用状況を読み込み"""
        if os.path.exists(self.quota_file):
            try:
                with open(self.quota_file, 'r') as f:
                    data = json.load(f)
                    # 月が変わったらリセット
                    saved_month = data.get('month', '')
                    current_month = datetime.now().strftime('%Y-%m')
                    if saved_month == current_month:
                        self.quota_used = data.get('used', 0)
                    else:
                        self.quota_used = 0
            except:
                self.quota_used = 0
    
    def _save_quota(self):
        """クォータ使用状況を保存"""
        try:
            with open(self.quota_file, 'w') as f:
                json.dump({
                    'month': datetime.now().strftime('%Y-%m'),
                    'used': self.quota_used
                }, f)
        except:
            pass
    
    def _check_quota(self, text: str) -> bool:
        """クォータチェック"""
        char_count = len(text)
        if self.quota_used + char_count > self.quota_limit * 0.9:  # 90%で警告
            self.logger.warning(f"DeepL quota nearly exhausted ({self.quota_used}/{self.quota_limit})")
            return False
        return True
    
    def translate(self, text: str, target_lang: str = 'JA') -> str:
        """テキストを翻訳"""
        if not self.enabled or not text or text == 'Information not available':
            return text
        
        # クォータチェック
        if not self._check_quota(text):
            return text
        
        # キャッシュチェック
        cache_key = f"{text}_{target_lang}"
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        try:
            # DeepL API Free版のエンドポイント
            url = 'https://api-free.deepl.com/v2/translate'
            
            params = {
                'auth_key': self.api_key,
                'text': text,
                'target_lang': target_lang
            }
            
            response = requests.post(url, data=params, timeout=10)
            
            if response.status_code == 200:
                result = response.json()
                translated = result['translations'][0]['text']
                
                # クォータ更新
                self.quota_used += len(text)
                self._save_quota()
                
                # キャッシュに保存
                self.cache[cache_key] = translated
                self._save_cache()
                
                return translated
            elif response.status_code == 456:
                self.logger.warning(f"DeepL API quota exceeded - using original text")
                return text
            else:
                self.logger.error(f"DeepL API error: {response.status_code}")
                return text
                
        except Exception as e:
            self.logger.error(f"Translation error: {e}")
            return text
    
    def translate_colors(self, colors: List[str]) -> List[str]:
        """色名を翻訳（拡張辞書使用）"""
        if not colors or colors == ['Information not available']:
            return ['ー']
        
        translated = []
        for color in colors:
            # 完全一致を最初にチェック
            if color in self.color_map:
                translated.append(self.color_map[color])
                continue
            
            # 部分一致をチェック
            ja_color = None
            for en_key, ja_value in self.color_map.items():
                if en_key.lower() in color.lower():
                    ja_color = color.lower().replace(en_key.lower(), ja_value)
                    break
            
            # マッピングで翻訳されなかった場合はDeepL使用
            if not ja_color:
                if self.enabled:
                    ja_color = self.translate(color)
                else:
                    ja_color = color
            
            translated.append(ja_color)
        
        return translated

class DataProcessor:
    """データ処理クラス"""
    
    def __init__(self):
        self.exchange_api = ExchangeRateAPI()
        self.translator = DeepLTranslator()
        self.gbp_to_jpy = self.exchange_api.get_rate()
        self.na_value = 'Information not available'
        self.dash_value = 'ー'
        self.logger = logging.getLogger(__name__)
    
    def _generate_consistent_id(self, unique_key: str) -> int:
        """一貫性のあるIDを生成（MD5ハッシュ使用）"""
        # MD5ハッシュを使用して一貫性のあるIDを生成
        # unique_keyが同じなら常に同じIDが生成される
        hash_obj = hashlib.md5(unique_key.encode('utf-8'))
        # 16進数の最初の8文字を10進数に変換
        return int(hash_obj.hexdigest()[:8], 16)
    
    def process_vehicle_data(self, raw_data: Optional[Dict]) -> List[Dict]:
        """
        車両データを処理してデータベース用レコードに変換
        エンジン単位で別レコードとして返す
        """
        if not raw_data:
            return []
        
        records = []
        base_data = self._extract_base_data(raw_data)
        
        # is_activeフラグを追加
        base_data['is_active'] = raw_data.get('is_active', True)
        base_data['last_updated'] = datetime.utcnow().isoformat()
        
        grades_engines = raw_data.get('grades_engines', [])
        
        if not grades_engines:
            grades_engines = [{
                'grade': self.na_value,
                'engine': self.na_value,
                'engine_price_gbp': None,
                'fuel': self.na_value,
                'transmission': self.na_value,
                'drive_type': self.na_value,
                'power_bhp': None
            }]
        
        for grade_engine in grades_engines:
            record = base_data.copy()
            
            record['grade'] = self._normalize_value(grade_engine.get('grade'), self.na_value)
            record['engine'] = self._normalize_value(grade_engine.get('engine'), self.na_value)
            record['fuel'] = self._normalize_value(grade_engine.get('fuel'), self.na_value)
            record['transmission'] = self._normalize_value(grade_engine.get('transmission'), self.na_value)
            record['drive_type'] = self._normalize_value(grade_engine.get('drive_type'), self.na_value)
            
            power_bhp = grade_engine.get('power_bhp')
            record['power_bhp'] = self.na_value if power_bhp is None else power_bhp
            
            engine_price_gbp = grade_engine.get('engine_price_gbp')
            if engine_price_gbp is not None:
                record['engine_price_gbp'] = engine_price_gbp
                record['engine_price_jpy'] = int(engine_price_gbp * self.gbp_to_jpy)
            else:
                record['engine_price_gbp'] = self.na_value
                record['engine_price_jpy'] = self.dash_value
            
            if grade_engine.get('price_min_gbp'):
                record['price_min_gbp'] = grade_engine['price_min_gbp']
                record['price_min_jpy'] = int(grade_engine['price_min_gbp'] * self.gbp_to_jpy)
            
            record = self._add_japanese_fields(record, raw_data)
            
            # full_model_jaの改良版生成
            parts = [record['make_ja']]
            
            # model_jaをDeepLで翻訳（キャッシュ活用）
            if record['model_en'] and record['model_en'] != self.na_value:
                model_ja = self.translator.translate(record['model_en'])
                record['model_ja'] = model_ja
                parts.append(model_ja)
            else:
                parts.append(self.dash_value)
            
            # gradeを追加（空の場合はダッシュ）
            if record['grade'] and record['grade'] != self.na_value:
                parts.append(record['grade'])
            else:
                parts.append(self.dash_value)
            
            # engineを短縮形で追加（ハイブリッド/EV情報保持）
            if record['engine'] and record['engine'] != self.na_value:
                engine_short = self._shorten_engine_text_improved(record['engine'])
                parts.append(engine_short)
            else:
                parts.append(self.dash_value)
            
            record['full_model_ja'] = ' '.join(parts)
            
            record['spec_json'] = self._create_spec_json(raw_data, grade_engine)
            record['updated_at'] = datetime.now().isoformat()
            
            # 一貫性のあるID生成
            unique_key = f"{record['slug']}_{record['grade']}_{record['engine']}"
            record['id'] = self._generate_consistent_id(unique_key)
            
            records.append(record)
        
        return records
    
    def _normalize_value(self, value, default: str) -> str:
        """値を正規化"""
        if value is None or value == '' or value == 'N/A' or value == '-':
            return default
        return str(value)
    
    def _shorten_engine_text(self, engine_text: str) -> str:
        """エンジンテキストを短縮（旧版）"""
        if engine_text == self.na_value:
            return ''
        
        match = re.search(r'(\d+\.?\d*)\s*[Ll]', engine_text)
        if match:
            return match.group(0)
        
        if 'kWh' in engine_text:
            match = re.search(r'([\d.]+)\s*kWh', engine_text)
            if match:
                return f"{match.group(1)}kWh"
        
        words = engine_text.split()
        return words[0] if words else ''
    
    def _shorten_engine_text_improved(self, engine_text: str) -> str:
        """エンジンテキストを短縮（改良版：ハイブリッド/EV情報保持）"""
        if engine_text == self.na_value:
            return self.dash_value
        
        # ハイブリッドの場合
        if 'Hybrid' in engine_text or 'hybrid' in engine_text or 'e:HEV' in engine_text:
            match = re.search(r'(\d+\.?\d*)\s*[Ll]', engine_text)
            if match:
                return f"{match.group(0)} HEV"
            else:
                return 'HEV'
        
        # プラグインハイブリッドの場合
        if 'Plug-in' in engine_text or 'PHEV' in engine_text:
            match = re.search(r'(\d+\.?\d*)\s*[Ll]', engine_text)
            if match:
                return f"{match.group(0)} PHEV"
            else:
                return 'PHEV'
        
        # 電気自動車の場合
        if 'kWh' in engine_text or 'Electric' in engine_text or 'electric' in engine_text:
            match = re.search(r'([\d.]+)\s*kWh', engine_text)
            if match:
                return f"{match.group(1)}kWh"
            else:
                return 'EV'
        
        # 通常のエンジンの場合
        match = re.search(r'(\d+\.?\d*)\s*[Ll]', engine_text)
        if match:
            return match.group(0)
        
        # その他の場合は最初の単語
        words = engine_text.split()
        return words[0] if words else self.dash_value
