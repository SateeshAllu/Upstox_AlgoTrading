import upstox_client
import sys
from datetime import datetime, time as dt_time, timedelta
import logging
import threading
import json
import os
import requests
import gzip
import pandas as pd
import numpy as np
from typing import Dict, List, Optional
import ssl
import warnings as std_warnings
import certifi
import shutil
from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager
from urllib3.exceptions import InsecureRequestWarning
import re
import time
import calendar
from upstox_client.rest import ApiException
from PyQt5.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, 
                             QCheckBox, QPushButton, QTableWidget, QTableWidgetItem, 
                             QHeaderView, QLabel, QTabWidget, QLineEdit, QFrame, QTextEdit, 
                             QStackedWidget, QGridLayout, QFileDialog)
from PyQt5.QtCore import Qt, QTimer
import sqlite3
import queue
import glob
from requests.exceptions import RequestException
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import asyncio
import websocket
from typing import Callable
import MarketDataFeed_Custom_pb2 as MarketDataFeedV3_pb2

# Configure logging
log_dir = os.path.join(os.path.expanduser("~"), "Documents", "TradingBotLogs11")
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f'trading_bot_{datetime.now().strftime("%Y%m%d")}.log')

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler(log_file), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

trade_entry_logger = logging.getLogger('trade_entry')
target_hit_logger = logging.getLogger('target_hit')
stop_loss_logger = logging.getLogger('stop_loss')

for log_type, log_name in [(trade_entry_logger, 'trade_entries'), (target_hit_logger, 'target_hits'), (stop_loss_logger, 'stop_losses')]:
    log_type.setLevel(logging.INFO)
    handler = logging.FileHandler(os.path.join(log_dir, f'{log_name}_{datetime.now().strftime("%Y%m%d")}.log'))
    handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
    log_type.addHandler(handler)

class CustomSSLAdapter(HTTPAdapter):
    def __init__(self, certfile=None, *args, **kwargs):
        self.certfile = certfile
        super().__init__(*args, **kwargs)

    def init_poolmanager(self, connections, maxsize, block=False, **pool_kwargs):
        ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
        if self.certfile:
            ctx.load_verify_locations(cafile=self.certfile)
        else:
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
        self.poolmanager = PoolManager(num_pools=connections, maxsize=maxsize, block=block, ssl_context=ctx, **pool_kwargs)

CERT_FILE_PATHS = [os.path.join(os.path.dirname(os.path.abspath(__file__)), "Amazon Root CA 1.crt"),
                   os.path.join(os.path.dirname(os.path.abspath(__file__)), "Zscaler Root CA.crt")]

def create_custom_cert_bundle():
    try:
        for cert_path in CERT_FILE_PATHS:
            if not os.path.exists(cert_path):
                logger.error(f"Certificate file not found at {cert_path}")
                return None
        custom_bundle_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "custom_cert_bundle.pem")
        shutil.copy(certifi.where(), custom_bundle_path)
        for cert_path in CERT_FILE_PATHS:
            with open(cert_path, "rb") as cert_file, open(custom_bundle_path, "ab") as bundle_file:
                bundle_file.write(b"\n")
                bundle_file.write(cert_file.read())
            logger.info(f"Appended certificate from {cert_path} to custom bundle")
        logger.info(f"Created custom certificate bundle at {custom_bundle_path}")
        return custom_bundle_path
    except Exception as e:
        logger.error(f"Error creating custom certificate bundle: {str(e)}")
        return None

def create_ssl_session():
    try:
        custom_bundle = create_custom_cert_bundle()
        session = requests.Session()
        if custom_bundle and os.path.exists(custom_bundle):
            adapter = CustomSSLAdapter(certfile=custom_bundle)
            session.mount('https://', adapter)
            session.verify = custom_bundle
            logger.info("Using custom certificate bundle for HTTPS requests")
        else:
            std_warnings.filterwarnings('ignore', category=InsecureRequestWarning)
            adapter = CustomSSLAdapter()
            session.mount('https://', adapter)
            session.verify = False
            logger.warning("SSL verification disabled - no valid certificate found")
        return session
    except Exception as e:
        logger.error(f"Error creating SSL session: {str(e)}")
        session = requests.Session()
        session.verify = False
        return session

def patch_upstox_client_ssl():
    try:
        custom_bundle = create_custom_cert_bundle()
        if custom_bundle and os.path.exists(custom_bundle):
            os.environ['REQUESTS_CA_BUNDLE'] = custom_bundle
            os.environ['SSL_CERT_FILE'] = custom_bundle
            logger.info(f"Environment variables set for SSL certificates")
            try:
                default_context = ssl.create_default_context()
                default_context.load_verify_locations(cafile=custom_bundle)
                logger.info("Modified default SSL context with custom certificates")
            except Exception as e:
                logger.warning(f"Could not modify default SSL context: {str(e)}")
    except Exception as e:
        logger.error(f"Failed to patch Upstox client SSL: {str(e)}")

class Config:
    API_KEY = os.getenv('UPSTOX_API_KEY', '193ad638-6837-45f2-8558-a17a6476bf73')
    API_SECRET = os.getenv('UPSTOX_API_SECRET', 't21vuv7mi8')
    try:
        token_file = os.path.join(os.path.expanduser("~"), "Documents", "TradingBotLogs", "accessToken.json")
        with open(token_file, 'r') as file:
            ACCESS_TOKEN = os.getenv('UPSTOX_ACCESS_TOKEN', file.read().strip())
    except Exception as e:
        logger.error(f"Failed to load access token from file: {str(e)}")
        ACCESS_TOKEN = os.getenv('UPSTOX_ACCESS_TOKEN', 'default_token_here')
    NIFTY_INDEX = "NSE_INDEX|Nifty 50"
    SENSEX_INDEX = "BSE_INDEX|SENSEX"
    #MCX_INDEX=
    PREMIUM_RANGE = (1, 1000)
    ENTRY_TIME = dt_time(9, 15)
    MARKET_CLOSE = dt_time(15, 30)
    API_VERSION = "v2"
    INSTRUMENT_URL = "https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz"
    QUANTITY = 1000
    PRODUCT_TYPE = "INTRADAY"
    ORDER_TYPE = "LIMIT"

class ConnectionMonitor:
    def __init__(self):
        self.connection_failures = 0
        self.last_success = None
        self.last_failure = None
        self.max_retries = 5
        self.retry_delay = 2  # Initial delay in seconds
        
    def record_success(self):
        self.connection_failures = 0
        self.last_success = datetime.now()
        
    def record_failure(self):
        self.connection_failures += 1
        self.last_failure = datetime.now()
        logger.warning(f"Connection failure #{self.connection_failures}")
        
    def should_reconnect(self):
        return self.connection_failures >= self.max_retries
    
    def get_status(self):
        return {
            "failures": self.connection_failures,
            "last_success": self.last_success,
            "last_failure": self.last_failure,
            "is_stable": self.connection_failures < self.max_retries
        }

class WebSocketManager:
    def __init__(self, api_client, symbols, on_message_callback: Callable, max_retries=5):
        self.api_client = api_client
        self.symbols = symbols  # e.g., ["NSE_FO|50144"]
        self.on_message_callback = on_message_callback
        self.max_retries = max_retries
        self.running = False
        self.ws_url = None
        self.reconnect_delay = 2
        self.ws = None
        self.thread = None

    def fetch_websocket_url(self):
        """Fetch the WebSocket URL via the V3 authorize endpoint."""
        logger.info(f" Access Token : {self.api_client.configuration.access_token}")
        url = "https://api.upstox.com/v3/feed/market-data-feed"
        headers = {
            "Authorization": f"Bearer {self.api_client.configuration.access_token}",
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Api-Version": "3.0"
        }
        try:
            logger.debug(f"Fetching WebSocket URL with headers: {headers}")
            response = requests.get(url, headers=headers, allow_redirects=False)
            logger.debug(f"Response status: {response.status_code}, headers: {response.headers}")
            if response.status_code in (302, 307):  # Handle both redirect codes
                self.ws_url = response.headers.get("Location")
                if self.ws_url:
                    logger.info(f"Fetched WebSocket URL: {self.ws_url}")
                    return True
                else:
                    logger.error("No Location header in redirect response")
                    return False
            else:
                logger.error(f"Unexpected status code: {response.status_code} - {response.text}")
                return False
        except Exception as e:
            logger.error(f"Error fetching WebSocket URL: {str(e)}")
            return False

    def on_open(self, ws):
        logger.info("WebSocket connected")
        subscription_message = json.dumps({
            "guid": "someguid123",
            "method": "sub",
            "data": {
                "mode": "ltpc",
                "instrumentKeys": self.symbols
            }
        })
        logger.debug(f"Sending subscription message: {subscription_message}")
        ws.send(subscription_message)
        logger.info(f"Subscribed to instrument keys: {self.symbols}")

    def on_message(self, ws, message):
        try:
            # Parse the Protobuf message
            feed_response = MarketDataFeedV3_pb2.FeedResponse()
            logger.info(f"Received message: {message}")
            logger.info(f"Received message type: {type(message)}")
            logger.info(f"Received message length: {feed_response}")
            feed_response.ParseFromString(message)
            logger.debug(f"Received Protobuf message: {feed_response}")

            # Initialize the response dictionary
            feed_dict = {}

            # Handle different message types
            if feed_response.type == MarketDataFeedV3_pb2.Type.Value("market_info"):
                feed_dict["type"] = "market_info"
                feed_dict["currentTs"] = feed_response.currentTs
                feed_dict["marketInfo"] = {
                    key: MarketDataFeedV3_pb2.MarketStatus.Name(value)
                    for key, value in feed_response.marketInfo.segmentStatus.items()
                }
                logger.info(f"Market status update: {feed_dict['marketInfo']}")

            elif feed_response.type in (MarketDataFeedV3_pb2.Type.Value("initial_feed"), 
                                    MarketDataFeedV3_pb2.Type.Value("live_feed")):
                feed_dict["type"] = MarketDataFeedV3_pb2.Type.Name(feed_response.type)
                feed_dict["currentTs"] = feed_response.currentTs
                feed_dict["feeds"] = {}
                for key, feed in feed_response.feeds.items():
                    feed_data = {}
                    
                    # Handle LTPC
                    if feed.HasField("ltpc"):
                        feed_data["ltpc"] = {
                            "ltp": feed.ltpc.ltp,
                            "ltt": feed.ltpc.ltt,
                            "ltq": feed.ltpc.ltq,
                            "cp": feed.ltpc.cp
                        }

                    # Handle FullFeed (MarketFullFeed or IndexFullFeed)
                    if feed.HasField("fullFeed"):
                        full_feed_data = {}
                        if feed.fullFeed.HasField("marketFF"):
                            market_ff = feed.fullFeed.marketFF
                            full_feed_data["marketFF"] = {
                                "ltpc": {
                                    "ltp": market_ff.ltpc.ltp if market_ff.HasField("ltpc") else None,
                                    "ltt": market_ff.ltpc.ltt if market_ff.HasField("ltpc") else None,
                                    "ltq": market_ff.ltpc.ltq if market_ff.HasField("ltpc") else None,
                                    "cp": market_ff.ltpc.cp if market_ff.HasField("ltpc") else None
                                },
                                "marketLevel": {
                                    "bidAskQuote": [
                                        {"bidQ": quote.bidQ, "bidP": quote.bidP, "askQ": quote.askQ, "askP": quote.askP}
                                        for quote in market_ff.marketLevel.bidAskQuote
                                    ] if market_ff.HasField("marketLevel") else None
                                },
                                "optionGreeks": {
                                    "delta": market_ff.optionGreeks.delta if market_ff.HasField("optionGreeks") else None,
                                    "theta": market_ff.optionGreeks.theta if market_ff.HasField("optionGreeks") else None,
                                    "gamma": market_ff.optionGreeks.gamma if market_ff.HasField("optionGreeks") else None,
                                    "vega": market_ff.optionGreeks.vega if market_ff.HasField("optionGreeks") else None,
                                    "rho": market_ff.optionGreeks.rho if market_ff.HasField("optionGreeks") else None
                                } if market_ff.HasField("optionGreeks") else None,
                                "marketOHLC": {
                                    "ohlc": [
                                        {
                                            "interval": ohlc.interval,
                                            "open": ohlc.open,
                                            "high": ohlc.high,
                                            "low": ohlc.low,
                                            "close": ohlc.close,
                                            "vol": ohlc.vol,
                                            "ts": ohlc.ts
                                        } for ohlc in market_ff.marketOHLC.ohlc
                                    ] if market_ff.HasField("marketOHLC") else None
                                } if market_ff.HasField("marketOHLC") else None,
                                "atp": market_ff.atp if market_ff.HasField("atp") else None,
                                "vtt": market_ff.vtt if market_ff.HasField("vtt") else None,
                                "oi": market_ff.oi if market_ff.HasField("oi") else None,
                                "iv": market_ff.iv if market_ff.HasField("iv") else None,
                                "tbq": market_ff.tbq if market_ff.HasField("tbq") else None,
                                "tsq": market_ff.tsq if market_ff.HasField("tsq") else None
                            }
                        elif feed.fullFeed.HasField("indexFF"):
                            index_ff = feed.fullFeed.indexFF
                            full_feed_data["indexFF"] = {
                                "ltpc": {
                                    "ltp": index_ff.ltpc.ltp if index_ff.HasField("ltpc") else None,
                                    "ltt": index_ff.ltpc.ltt if index_ff.HasField("ltpc") else None,
                                    "ltq": index_ff.ltpc.ltq if index_ff.HasField("ltpc") else None,
                                    "cp": index_ff.ltpc.cp if index_ff.HasField("ltpc") else None
                                },
                                "marketOHLC": {
                                    "ohlc": [
                                        {
                                            "interval": ohlc.interval,
                                            "open": ohlc.open,
                                            "high": ohlc.high,
                                            "low": ohlc.low,
                                            "close": ohlc.close,
                                            "vol": ohlc.vol,
                                            "ts": ohlc.ts
                                        } for ohlc in index_ff.marketOHLC.ohlc
                                    ] if index_ff.HasField("marketOHLC") else None
                                } if index_ff.HasField("marketOHLC") else None
                            }
                        feed_data["fullFeed"] = full_feed_data

                    # Handle FirstLevelWithGreeks
                    if feed.HasField("firstLevelWithGreeks"):
                        first_level = feed.firstLevelWithGreeks
                        feed_data["firstLevelWithGreeks"] = {
                            "ltpc": {
                                "ltp": first_level.ltpc.ltp if first_level.HasField("ltpc") else None,
                                "ltt": first_level.ltpc.ltt if first_level.HasField("ltpc") else None,
                                "ltq": first_level.ltpc.ltq if first_level.HasField("ltpc") else None,
                                "cp": first_level.ltpc.cp if first_level.HasField("ltpc") else None
                            },
                            "firstDepth": {
                                "bidQ": first_level.firstDepth.bidQ if first_level.HasField("firstDepth") else None,
                                "bidP": first_level.firstDepth.bidP if first_level.HasField("firstDepth") else None,
                                "askQ": first_level.firstDepth.askQ if first_level.HasField("firstDepth") else None,
                                "askP": first_level.firstDepth.askP if first_level.HasField("firstDepth") else None
                            } if first_level.HasField("firstDepth") else None,
                            "optionGreeks": {
                                "delta": first_level.optionGreeks.delta if first_level.HasField("optionGreeks") else None,
                                "theta": first_level.optionGreeks.theta if first_level.HasField("optionGreeks") else None,
                                "gamma": first_level.optionGreeks.gamma if first_level.HasField("optionGreeks") else None,
                                "vega": first_level.optionGreeks.vega if first_level.HasField("optionGreeks") else None,
                                "rho": first_level.optionGreeks.rho if first_level.HasField("optionGreeks") else None
                            } if first_level.HasField("optionGreeks") else None,
                            "vtt": first_level.vtt if first_level.HasField("vtt") else None,
                            "oi": first_level.oi if first_level.HasField("oi") else None,
                            "iv": first_level.iv if first_level.HasField("iv") else None
                        }

                    # Handle requestMode
                    if feed.HasField("requestMode"):
                        feed_data["requestMode"] = MarketDataFeedV3_pb2.RequestMode.Name(feed.requestMode)

                    feed_dict["feeds"][key] = feed_data

                logger.info(f"Received market data for {list(feed_dict['feeds'].keys())}")

            else:
                logger.warning(f"Unhandled message type: {MarketDataFeedV3_pb2.Type.Name(feed_response.type)}")

            if feed_dict:
                asyncio.run(self.on_message_callback(feed_dict))

        except Exception as e:
            logger.error(f"Error processing WebSocket message: {str(e)}")

    def on_error(self, ws, error):
        logger.error(f"WebSocket error: {str(error)}")

    def on_close(self, ws, close_status_code, close_msg):
        logger.info(f"WebSocket closed: {close_status_code} - {close_msg}")

    def run(self):
        attempt = 0
        while self.running and attempt < self.max_retries:
            if not self.ws_url and not self.fetch_websocket_url():
                logger.error("Cannot proceed without WebSocket URL")
                break
            try:
                logger.debug("Starting WebSocketManager with websocket-client")
                headers = {
                    "Authorization": f"Bearer {self.api_client.configuration.access_token}",
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    "Api-Version": "3.0"
                }
                logger.debug(f"Connecting with headers: {headers}")
                self.ws = websocket.WebSocketApp(
                    self.ws_url,
                    header=headers,
                    on_open=self.on_open,
                    on_message=self.on_message,
                    on_error=self.on_error,
                    on_close=self.on_close
                )
                self.ws.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as e:
                attempt += 1
                logger.error(f"WebSocket error (attempt {attempt}/{self.max_retries}): {str(e)}")
                if attempt >= self.max_retries:
                    logger.error("Max retries reached, giving up")
                    break
                delay = min(self.reconnect_delay * (2 ** (attempt - 1)), 60)
                logger.info(f"Reconnecting in {delay} seconds...")
                time.sleep(delay)

    def start(self):
        if not self.running:
            self.running = True
            self.thread = threading.Thread(target=self.run, daemon=True)
            self.thread.start()
            logger.info("WebSocket thread started")

    def stop(self):
        self.running = False
        if self.ws:
            self.ws.close()
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=5)
            if self.thread.is_alive():
                logger.warning("WebSocket thread did not terminate within timeout")
            else:
                logger.info("WebSocket thread terminated successfully")
        logger.info("WebSocket manager stopped")

class TradingBot:
    def __init__(self):
        self.config = Config()
        self.ssl_session = create_ssl_session()
        self.connection_monitor = ConnectionMonitor()
        self.api_client = self._initialize_api()
        
        self.instrument_master: List[Dict] = []
        self.last_master_fetch: Optional[datetime] = None
        self.filtered_contracts: Dict = {}
        self.eligible_contracts: Dict = {}
        self.vwap_data: Dict[str, pd.DataFrame] = {}
        self.first_candle_high: Dict[str, float] = {}
        self.previous_candle: Dict[str, Dict] = {}
        self.positions: List = []
        self.paper_trades: List = []
        self.market_data_cache: Dict = {}
        self.ws_manager = None
        self.running = False
        self.trade_log = []
        self.last_summary_time = None
        self.enable_trailing_sl = True
        self.paper_trading = True
        self.data_queue = queue.Queue()
        self._load_instrument_master()
        self.setup_database()
        self.bot_thread = None
        self.websocket_thread = None

    def _initialize_api(self) -> upstox_client.ApiClient:
        try:
            if not all([self.config.API_KEY, self.config.API_SECRET, self.config.ACCESS_TOKEN]):
                raise ValueError("Missing API credentials")
            configuration = upstox_client.Configuration()
            configuration.access_token = self.config.ACCESS_TOKEN
            custom_bundle = create_custom_cert_bundle()
            if custom_bundle and os.path.exists(custom_bundle):
                configuration.ssl_ca_cert = custom_bundle
                logger.info("Set custom SSL certificate bundle for API client")
            else:
                configuration.ssl_ca_cert = None
                configuration.verify_ssl = False
                logger.warning("Disabled SSL verification for API client")
            api_client = upstox_client.ApiClient(configuration)
            self.connection_monitor.record_success()
            return api_client
        except Exception as e:
            logger.error(f"Failed to initialize API client: {str(e)}")
            self.connection_monitor.record_failure()
            raise

    def _normalize_response(self, response) -> Dict:
        try:
            if isinstance(response, dict):
                return response
            elif hasattr(response, 'to_dict'):
                return response.to_dict()
            elif isinstance(response, str):
                return json.loads(response)
            else:
                logger.warning(f"Unexpected response type: {type(response)}")
                return {}
        except Exception as e:
            logger.error(f"Error normalizing response: {str(e)}")
            return {}

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), 
           retry=retry_if_exception_type((RequestException, ApiException)))
    def _load_instrument_master(self) -> None:
        try:
            current_date = datetime.now().date()
            if self.last_master_fetch and self.last_master_fetch.date() == current_date:
                logger.info("Using cached instrument master")
                return
            response = self.ssl_session.get(self.config.INSTRUMENT_URL)
            if response.status_code != 200:
                raise ValueError(f"Failed to fetch instrument master: HTTP {response.status_code}")
            decompressed_data = gzip.decompress(response.content)
            self.instrument_master = json.loads(decompressed_data.decode('utf-8'))
            self.last_master_fetch = datetime.now()
            logger.info(f"Loaded {len(self.instrument_master)} instruments from master file")
            self.connection_monitor.record_success()
            if self.instrument_master:
                logger.debug(f"Sample instrument: {json.dumps(self.instrument_master[0])}")
        except Exception as e:
            logger.error(f"Error loading instrument master: {str(e)}")
            self.connection_monitor.record_failure()
            self.instrument_master = []

    def setup_database(self):
        try:
            conn = sqlite3.connect(os.path.join(log_dir, "trading_data.db"))
            c = conn.cursor()
            c.execute('''CREATE TABLE IF NOT EXISTS candles
                        (instrument_key TEXT, timestamp DATETIME, open REAL, high REAL, low REAL, close REAL, volume INTEGER, vwap REAL, date DATE)''')
            c.execute('''CREATE TABLE IF NOT EXISTS trades
                        (trade_id INTEGER PRIMARY KEY AUTOINCREMENT, instrument_key TEXT, entry_time DATETIME, entry_price REAL, status TEXT)''')
            c.execute("PRAGMA table_info(trades)")
            columns = [info[1] for info in c.fetchall()]
            required_columns = {
                "contract_name": "TEXT",
                "symbol": "TEXT",
                "exit_time": "DATETIME",
                "exit_price": "REAL",
                "pnl": "REAL",
                "quantity": "INTEGER",
                "trade_date": "DATE",
                "trade_type": "TEXT"
            }
            for col_name, col_type in required_columns.items():
                if col_name not in columns:
                    c.execute(f"ALTER TABLE trades ADD COLUMN {col_name} {col_type}")
                    logger.info(f"Added column {col_name} to trades table")

            c.execute("SELECT trade_id, entry_time, quantity, trade_date, symbol FROM trades")
            rows = c.fetchall()
            for row in rows:
                trade_id, entry_time, quantity, trade_date, symbol = row
                if trade_date is None or trade_date == "Unknown":
                    try:
                        entry_dt = datetime.fromisoformat(entry_time.replace('Z', '+00:00'))
                        new_trade_date = entry_dt.date().isoformat()
                        c.execute("UPDATE trades SET trade_date = ? WHERE trade_id = ?", (new_trade_date, trade_id))
                        logger.info(f"Updated trade_date for trade_id {trade_id} to {new_trade_date}")
                    except Exception as e:
                        logger.error(f"Error updating trade_date for trade_id {trade_id}: {str(e)}")
                if quantity is None:
                    c.execute("UPDATE trades SET quantity = ? WHERE trade_id = ?", (self.config.QUANTITY, trade_id))
                    logger.info(f"Updated quantity for trade_id {trade_id} to {self.config.QUANTITY}")
                trade_type = "CE" if "CE" in symbol else "PE" if "PE" in symbol else "Unknown"
                c.execute("UPDATE trades SET trade_type = ? WHERE trade_id = ?", (trade_type, trade_id))
                logger.info(f"Updated trade_type for trade_id {trade_id} to {trade_type}")

            c.execute("""
                UPDATE trades 
                SET pnl = (exit_price - entry_price) * quantity
                WHERE status IN ('TARGET_HIT', 'STOP_LOSS_HIT') 
                AND exit_price IS NOT NULL 
                AND entry_price IS NOT NULL 
                AND quantity IS NOT NULL
                AND (pnl IS NULL OR pnl = 0.0)
            """)
            conn.commit()
            logger.info("Backfilled pnl for closed trades")

            c.execute("""
                UPDATE trades 
                SET contract_name = 'Unknown', symbol = instrument_key
                WHERE contract_name IS NULL OR contract_name = ''
            """)
            c.execute("""
                UPDATE trades 
                SET symbol = instrument_key
                WHERE symbol IS NULL OR symbol = '' OR symbol = contract_name
            """)
            conn.commit()
            logger.info("Updated contract_name and symbol for existing trades")

            conn.close()
            logger.info("Database setup completed with updated schema and backfilled data")
        except Exception as e:
            logger.error(f"Error setting up database: {str(e)}")

    def save_candle_data(self, instrument_key, candle_data):
        try:
            conn = sqlite3.connect(os.path.join(log_dir, "trading_data.db"))
            c = conn.cursor()
            c.execute("INSERT INTO candles VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", (
                instrument_key,
                candle_data["timestamp"].isoformat() if hasattr(candle_data["timestamp"], 'isoformat') else candle_data["timestamp"],
                candle_data["open"], candle_data["high"], candle_data["low"], candle_data["close"],
                candle_data["volume"], candle_data["vwap"],
                candle_data["timestamp"].date().isoformat() if hasattr(candle_data["timestamp"], 'date') else candle_data["timestamp"].date()
            ))
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"Error saving candle data: {str(e)}")

    def save_trade_data(self, trade):
        try:
            conn = sqlite3.connect(os.path.join(log_dir, "trading_data.db"))
            c = conn.cursor()
            if trade.get("exit_price") is not None and trade.get("entry_price") is not None:
                quantity = trade.get("quantity", self.config.QUANTITY)
                pnl = (trade["exit_price"] - trade["entry_price"]) * quantity
            else:
                pnl = trade.get("pnl", 0.0)
            
            entry_time_str = trade["entry_time"]
            entry_time = datetime.fromisoformat(entry_time_str.replace('Z', '+00:00'))
            trade_date = entry_time.date().isoformat()
            quantity = trade.get("quantity", self.config.QUANTITY)
            trade_type = "CE" if "CE" in trade["symbol"] else "PE" if "PE" in trade["symbol"] else "Unknown"
            
            c.execute("""
                INSERT INTO trades (instrument_key, contract_name, symbol, entry_time, entry_price, exit_time, exit_price, status, pnl, quantity, trade_date, trade_type) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                trade["key"], 
                trade.get("contract_name", "Unknown"), 
                trade["symbol"], 
                entry_time_str, 
                trade["entry_price"],
                trade.get("exit_time"), 
                trade.get("exit_price"), 
                trade["status"], 
                pnl,
                quantity,
                trade_date,
                trade_type
            ))
            conn.commit()
            conn.close()
            logger.debug(f"Saved trade: {trade['key']} with trade_date={trade_date}, quantity={quantity}, pnl={pnl}, trade_type={trade_type}")
        except Exception as e:
            logger.error(f"Error saving trade data: {str(e)}")

    def fetch_trade_data(self, status=None):
        try:
            conn = sqlite3.connect(os.path.join(log_dir, "trading_data.db"))
            c = conn.cursor()
            actual_columns = ["trade_id", "instrument_key", "entry_time", "entry_price", "exit_time", 
                             "exit_price", "status", "pnl", "contract_name", "symbol", "quantity", "trade_date", "trade_type"]
            query = f"SELECT {', '.join(actual_columns)} FROM trades"
            if status:
                query += " WHERE status = ?"
                c.execute(query, (status,))
            else:
                c.execute(query)
            data = c.fetchall()
            conn.close()
            #logger.debug(f"Fetched trade data: {data}")
            return data
        except Exception as e:
            logger.error(f"Error fetching trade data: {str(e)}")
            return []

    def fetch_historical_trades(self, start_date=None, end_date=None):
        try:
            conn = sqlite3.connect(os.path.join(log_dir, "trading_data.db"))
            c = conn.cursor()
            actual_columns = ["trade_id", "instrument_key", "entry_time", "entry_price", "exit_time", 
                             "exit_price", "status", "pnl", "contract_name", "symbol", "quantity", "trade_date", "trade_type"]
            query = f"SELECT {', '.join(actual_columns)} FROM trades WHERE status IN ('TARGET_HIT', 'STOP_LOSS_HIT', 'MARKET_CLOSE')"
            params = []
            if start_date or end_date:
                conditions = []
                if start_date:
                    conditions.append("trade_date >= ?")
                    params.append(start_date)
                if end_date:
                    conditions.append("trade_date <= ?")
                    params.append(end_date)
                query += " AND " + " AND ".join(conditions)
            c.execute(query, params)
            trades = c.fetchall()
            logger.debug(f"Fetched historical trades: {trades}")
            conn.close()
            return trades
        except Exception as e:
            logger.error(f"Error fetching historical trades: {str(e)}")
            return []

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), 
           retry=retry_if_exception_type((RequestException, ApiException)))
    def fetch_option_contracts(self) -> Dict:
        try:
            self._load_instrument_master()
            if not self.instrument_master:
                logger.error("No instrument master data available")
                self.data_queue.put(("error", "Failed to load instrument master"))
                return {}
            instruments = {}
            for index in [self.config.NIFTY_INDEX, self.config.SENSEX_INDEX]:
                option_symbols = self._get_option_symbols(index)
                if not option_symbols:
                    logger.warning(f"No option symbols found for {index}")
                    continue
                symbols_str = ",".join(option_symbols)
                market_api = upstox_client.MarketQuoteApi(self.api_client)
                response = market_api.get_full_market_quote(symbols_str, api_version=self.config.API_VERSION)
                response_dict = self._normalize_response(response)
                data = response_dict.get('data', {})
                if not data:
                    logger.warning(f"No market data received for {index} options")
                    continue
                instruments[index] = [
                    {"symbol": data[key]["symbol"].split("|")[1] if "|" in data[key]["symbol"] else data[key]["symbol"],
                     "key": data[key]["instrument_token"],
                     "strike": self._extract_strike(data[key]["symbol"]),
                     "ltp": float(data[key].get("last_price", 0)),
                     "ohlc": data[key].get("ohlc", {}) or {}}
                    for key in data if data[key]["instrument_token"] in option_symbols
                ]
                logger.info(f"Fetched {len(instruments[index])} option contracts for {index}")
            self.connection_monitor.record_success()
            return instruments
        except Exception as e:
            logger.error(f"Error fetching contracts: {str(e)}")
            self.connection_monitor.record_failure()
            self.data_queue.put(("error", f"Failed to fetch contracts: {str(e)}"))
            return {}

    def _get_option_symbols(self, index: str) -> List[str]:
        try:
            underlying_key = self.config.NIFTY_INDEX if "Nifty" in index else self.config.SENSEX_INDEX
            segment = "NSE_FO" if "Nifty" in index else "BSE_FO"
            today = datetime.now().date()
            today_ts = int(datetime(today.year, today.month, today.day).timestamp() * 1000)
            option_data = [item for item in self.instrument_master if (item.get("segment") == segment and
                                                                     item.get("underlying_key") == underlying_key and
                                                                     item.get("instrument_type") in ["CE", "PE"] and
                                                                     item.get("expiry") is not None)]
            if not option_data:
                logger.warning(f"No option instruments found for {underlying_key}")
                return []
            expiries = sorted(set(item["expiry"] for item in option_data), key=lambda x: x)
            future_expiries = [exp for exp in expiries if exp >= today_ts]
            if not future_expiries:
                logger.warning(f"No future expiries found for {underlying_key}")
                return []
            nearest_expiry = future_expiries[0]
            expiry_date = datetime.fromtimestamp(nearest_expiry / 1000).date()
            logger.debug(f"Nearest expiry for {underlying_key}: {expiry_date} (ts: {nearest_expiry})")
            symbols = [item["instrument_key"] for item in option_data if item.get("expiry") == nearest_expiry]
            logger.debug(f"Found {len(symbols)} option symbols for {underlying_key}")
            return symbols[:500]
        except Exception as e:
            logger.error(f"Error filtering option symbols: {str(e)}")
            return []

    def _extract_strike(self, symbol: str) -> float:
        try:
            parts = symbol.split()
            for part in parts:
                if part.isdigit() and 1000 <= float(part) <= 100000:
                    return float(part)
            match = re.search(r'(\d{4,5})(?:CE|PE)', symbol)
            if match:
                return float(match.group(1))
            logger.warning(f"Could not extract strike from {symbol}")
            return 0.0
        except Exception as e:
            logger.error(f"Error extracting strike from {symbol}: {str(e)}")
            return 0.0

    def filter_contracts(self, contracts: Dict) -> None:
        try:
            output_dir = os.path.join(os.path.expanduser("~"), "Documents", "TradingBotLogs11")
            os.makedirs(output_dir, exist_ok=True)
            all_filtered_contracts = []
            for index, options in contracts.items():
                if not options or not isinstance(options, list):
                    logger.debug(f"No valid options for {index}")
                    continue
                for contract in options:
                    try:
                        ltp = float(contract["ltp"])
                        open_price = float(contract["ohlc"].get("open", 0))
                        high_price = float(contract["ohlc"].get("high", 0))
                        if (self.config.PREMIUM_RANGE[0] <= ltp <= self.config.PREMIUM_RANGE[1] and
                            open_price == high_price and open_price > 0):
                            formatted_contract = {"index": "Nifty" if "Nifty" in index else "Sensex",
                                                 "contract_name": contract["symbol"],
                                                 "symbol_key": contract["key"],
                                                 "open": open_price,
                                                 "high": high_price,
                                                 "ltp": ltp,
                                                 "strike": contract["strike"]}
                            all_filtered_contracts.append(formatted_contract)
                            logger.debug(f"Contract meets OH criteria: {contract['symbol']} - {formatted_contract['open']}")
                    except (ValueError, TypeError) as e:
                        logger.debug(f"Invalid contract data: {str(e)} - {contract}")
            if all_filtered_contracts:
                df = pd.DataFrame(all_filtered_contracts)
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = f"filtered_contracts_{timestamp}.csv"
                filepath = os.path.join(output_dir, filename)
                df.to_csv(filepath, index=False)
                logger.info(f"Saved {len(all_filtered_contracts)} filtered contracts to {filepath}")
                self.data_queue.put(("all_filtered_contracts", all_filtered_contracts))
            else:
                logger.warning("No contracts found where open == high")
                self.data_queue.put(("error", "No contracts found where open == high"))
            self.filtered_contracts = {}
            for index, options in contracts.items():
                if not options or not isinstance(options, list):
                    logger.debug(f"No valid options to process for {index}")
                    continue
                ce_contracts = [opt for opt in options if "CE" in opt["symbol"]]
                pe_contracts = [opt for opt in options if "PE" in opt["symbol"]]
                selected_contracts = []
                for contract_list, opt_type in [(ce_contracts, "CE"), (pe_contracts, "PE")]:
                    valid_contracts = [opt for opt in contract_list if (self.config.PREMIUM_RANGE[0] <= float(opt["ltp"]) <= self.config.PREMIUM_RANGE[1] and
                                                                      float(opt["ohlc"].get("open", 0)) == float(opt["ohlc"].get("high", 0)) and
                                                                      float(opt["ohlc"].get("open", 0)) > 0)]
                    if valid_contracts:
                        selected = min(valid_contracts, key=lambda x: abs(x["ltp"] - 50))
                        formatted = {"index": "Nifty" if "Nifty" in index else "Sensex",
                                     "contract_name": selected["symbol"],
                                     "symbol_key": selected["key"],
                                     "open": float(selected["ohlc"].get("open", 0)),
                                     "high": float(selected["ohlc"].get("high", 0)),
                                     "ltp": selected["ltp"],
                                     "strike": selected["strike"]}
                        selected_contracts.append(formatted)
                        logger.info(f"Selected {opt_type} contract for {index} (closest to ltp 50): {selected['symbol']} (LTP: {selected['ltp']})")
                if selected_contracts:
                    self.filtered_contracts[index] = selected_contracts
                    logger.info(f"Selected {len(selected_contracts)} contracts for processing in {index}: {[c['contract_name'] for c in selected_contracts]}")
            self.eligible_contracts = self.filtered_contracts.copy()
            self.data_queue.put(("filtered_contracts", list(self.filtered_contracts.values())))
            self.data_queue.put(("eligible_contracts", list(self.eligible_contracts.values())))
        except Exception as e:
            logger.error(f"Error filtering contracts: {str(e)}")
            self.data_queue.put(("error", f"Failed to filter contracts: {str(e)}"))

    def _calculate_atr(self, df: pd.DataFrame, period: int = 14) -> float:
        try:
            if len(df) < period:
                return 0.0
            high_low = df["high"] - df["low"]
            high_close = np.abs(df["high"] - df["close"].shift())
            low_close = np.abs(df["low"] - df["close"].shift())
            true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
            atr = true_range.rolling(window=period).mean().iloc[-1]
            return atr if not np.isnan(atr) else 0.0
        except Exception as e:
            logger.error(f"Error calculating ATR: {str(e)}")
            return 0.0

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), 
           retry=retry_if_exception_type((RequestException, ApiException)))
    def calculate_vwap(self) -> None:
        try:
            historical_api = upstox_client.HistoryApi(self.api_client)
            current_time = datetime.now().time()
            while current_time < self.config.MARKET_CLOSE and self.running:
                for index, contracts in self.filtered_contracts.items():
                    for contract in contracts:
                        try:
                            instrument_key = contract["symbol_key"]
                            response = historical_api.get_intra_day_candle_data(instrument_key=instrument_key, interval="1minute", api_version=self.config.API_VERSION)
                            response_dict = self._normalize_response(response)
                            candles = response_dict.get("data", {}).get("candles", [])
                            if not candles:
                                logger.warning(f"No historical data for {contract['symbol_key']}")
                                continue
                            df = pd.DataFrame(candles, columns=["timestamp", "open", "high", "low", "close", "volume", "oi"])
                            df["timestamp"] = pd.to_datetime(df["timestamp"])
                            session_start = pd.Timestamp(df["timestamp"].dt.date.iloc[0]).replace(hour=9, minute=15)
                            session_start = session_start.tz_localize('Asia/Kolkata')
                            df = df[df["timestamp"] >= session_start]
                            if df.empty:
                                logger.warning(f"No data after session start for {contract['symbol_key']}")
                                continue
                            df["typical_price"] = (df["high"] + df["low"] + df["close"]) / 3
                            df["tp_volume"] = df["typical_price"] * df["volume"]
                            df["cum_tp_volume"] = df["tp_volume"].cumsum()
                            df["cum_volume"] = df["volume"].cumsum()
                            df["vwap"] = df["cum_tp_volume"] / df["cum_volume"]
                            self.vwap_data[contract["symbol_key"]] = df
                            if instrument_key not in self.first_candle_high:
                                self.first_candle_high[instrument_key] = self._fetch_first_candle_high(instrument_key)
                            csv_dir = os.path.join(os.path.expanduser("~"), "Documents", "TradingBotLogs2")
                            os.makedirs(csv_dir, exist_ok=True)
                            filename = os.path.join(csv_dir, f"vwap_ohlc_{contract['symbol_key'].replace('|', '_')}_{datetime.now().strftime('%Y%m%d')}.csv")
                            df.to_csv(filename, index=False)
                            logger.debug(f"Saved VWAP and OHLC data for {contract['symbol_key']} to {filename}")
                        except Exception as e:
                            logger.error(f"Error calculating VWAP for {contract['symbol_key']}: {str(e)}")
                logger.info(f"VWAP calculation completed at {datetime.now().time()}. VWAP data: {list(self.vwap_data.keys())}")
                threading.Event().wait(60)
                current_time = datetime.now().time()
            logger.info("VWAP calculation stopped as market closed or bot stopped")
        except Exception as e:
            logger.error(f"Error in VWAP calculation: {str(e)}")
    
    def on_market_message(self, message):
        try:
            if isinstance(message, str):
                data = json.loads(message)
            elif isinstance(message, dict):
                data = message
            else:
                logger.warning(f"Unexpected WebSocket message type: {type(message)}")
                return
            current_time = datetime.now().time()
            if current_time >= self.config.ENTRY_TIME and current_time < self.config.MARKET_CLOSE:
                feeds = data.get("feeds", {})
                if not feeds:
                    logger.warning(f"No 'feeds' in WebSocket data: {data}")
                    return
                for instrument_key, feed_data in feeds.items():
                    ltp = feed_data.get("ff", {}).get("marketFF", {}).get("ltpc", {}).get("ltp", 0)
                    ltp = float(ltp) if ltp else 0
                    if not instrument_key or not ltp:
                        logger.warning(f"Invalid feed data for {instrument_key}: {feed_data}")
                        continue
                    for index, contracts in self.filtered_contracts.items():
                        for contract in contracts:
                            if instrument_key == contract["symbol_key"]:
                                self._process_trade(contract, ltp)
                                self.data_queue.put(("ltp_update", (instrument_key, ltp)))
            self._log_trade_summary()
            self.connection_monitor.record_success()
        except Exception as e:
            logger.error(f"WebSocket message error: {str(e)}")
            self.connection_monitor.record_failure()
            if self.connection_monitor.should_reconnect():
                self._reconnect_websocket()

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), 
           retry=retry_if_exception_type((RequestException, ApiException)))
    def _fetch_first_candle_high(self, instrument_key: str) -> float:
        try:
            historical_api = upstox_client.HistoryApi(self.api_client)
            response = historical_api.get_intra_day_candle_data(instrument_key=instrument_key, interval="1minute", api_version=self.config.API_VERSION)
            response_dict = self._normalize_response(response)
            candles = response_dict.get("data", {}).get("candles", [])
            if not candles:
                logger.warning(f"No historical data for {instrument_key}")
                return 0.0
            df = pd.DataFrame(candles, columns=["timestamp", "open", "high", "low", "close", "volume", "oi"])
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            df.set_index("timestamp", inplace=True)
            session_start = pd.Timestamp(df.index[0].date()).replace(hour=9, minute=15).tz_localize('Asia/Kolkata')
            first_candle = df.loc[session_start] if session_start in df.index else df.iloc[0]
            high = float(first_candle["high"])
            logger.info(f"First candle high for {instrument_key} at 9:15: {high}")
            self.connection_monitor.record_success()
            return high
        except Exception as e:
            logger.error(f"Error fetching first candle high for {instrument_key}: {str(e)}")
            self.connection_monitor.record_failure()
            return 0.0

    def _save_trade_to_excel(self, trade: Dict):
        try:
            output_dir = os.path.join(os.path.expanduser("~"), "Documents", "TradingBotLogs11")
            os.makedirs(output_dir, exist_ok=True)
            filename = f"trades_{datetime.now().strftime('%Y%m%d')}.xlsx"
            filepath = os.path.join(output_dir, filename)
            if trade["status"] in ["TARGET_HIT", "STOP_LOSS_HIT"]:
                entry_price = trade["entry_price"]
                exit_price = trade["exit_price"]
                pnl = (exit_price - entry_price) * trade.get("quantity", self.config.QUANTITY)
                trade["pnl"] = pnl
            else:
                trade["pnl"] = 0.0
            self.trade_log.append(trade)
            df = pd.DataFrame(self.trade_log)
            columns = ["contract_name", "symbol", "entry_time", "entry_price", "stop_loss", "target", "exit_time", "exit_price", "status", "pnl"]
            df = df[columns]
            df.to_excel(filepath, index=False)
            logger.info(f"Saved trade to {filepath}: {trade}")
        except Exception as e:
            logger.error(f"Error saving trade to Excel: {str(e)}")

    def _log_trade_summary(self):
        try:
            current_time = datetime.now()
            if self.last_summary_time is None or (current_time - self.last_summary_time) >= timedelta(minutes=5):
                total_trades, total_pnl, win_rate, risk_reward = self.calculate_monthly_summary()
                summary = (f"Trade Summary: Total Trades={total_trades}, Total P/L={total_pnl:.2f}, "
                        f"Win Rate={win_rate:.2f}%, Risk-Reward={risk_reward:.2f}")
                logger.info(summary)
                self.last_summary_time = current_time
                self.data_queue.put(("summary_update", (total_trades, total_pnl, win_rate, risk_reward)))
        except Exception as e:
            logger.error(f"Error logging trade summary: {str(e)}")

    def _place_buy_order(self, instrument_key: str, price: float, quantity: int) -> str:
        try:
            if self.paper_trading:
                logger.info(f"Paper Trading: Buy order placed for {instrument_key} at {price}, Qty: {quantity}")
                return "PAPER_ORDER_ID"
            order_api = upstox_client.OrderApi(self.api_client)
            order_params = {"quantity": quantity, "product": self.config.PRODUCT_TYPE, "order_type": self.config.ORDER_TYPE,
                            "price": price, "tradingsymbol": instrument_key.split("|")[1],
                            "exchange": "NSE_FO" if "NSE" in instrument_key else "BSE_FO",
                            "transaction_type": "BUY", "trigger_price": None, "disclosed_quantity": None, "duration": "DAY"}
            response = order_api.place_order(order_params)
            order_id = response.get("order_id")
            logger.info(f"Real Trading: Buy order placed for {instrument_key} at {price}, Order ID: {order_id}")
            self.connection_monitor.record_success()
            return order_id
        except ApiException as e:
            logger.error(f"API Error placing buy order for {instrument_key}: {str(e)}")
            self.connection_monitor.record_failure()
            return None

    def _place_sell_order(self, instrument_key: str, price: float, quantity: int, order_id: str = None) -> str:
        try:
            if self.paper_trading:
                logger.info(f"Paper Trading: Sell order placed for {instrument_key} at {price}, Qty: {quantity}")
                return "PAPER_ORDER_ID"
            order_api = upstox_client.OrderApi(self.api_client)
            order_params = {"quantity": quantity, "product": self.config.PRODUCT_TYPE, "order_type": self.config.ORDER_TYPE,
                            "price": price, "tradingsymbol": instrument_key.split("|")[1],
                            "exchange": "NSE_FO" if "NSE" in instrument_key else "BSE_FO",
                            "transaction_type": "SELL", "trigger_price": None, "disclosed_quantity": None, "duration": "DAY"}
            response = order_api.place_order(order_params)
            order_id = response.get("order_id")
            logger.info(f"Real Trading: Sell order placed for {instrument_key} at {price}, Order ID: {order_id}")
            self.connection_monitor.record_success()
            return order_id
        except ApiException as e:
            logger.error(f"API Error placing sell order for {instrument_key}: {str(e)}")
            self.connection_monitor.record_failure()
            return None

    def _modify_stop_loss(self, instrument_key: str, new_stop_loss: float, order_id: str) -> bool:
        try:
            if self.paper_trading:
                logger.info(f"Paper Trading: Modified stop-loss for {instrument_key} to {new_stop_loss}")
                return True
            order_api = upstox_client.OrderApi(self.api_client)
            order_params = {"order_id": order_id, "trigger_price": new_stop_loss, "quantity": self.config.QUANTITY, "price": None}
            response = order_api.modify_order(order_params)
            logger.info(f"Real Trading: Modified stop-loss for {instrument_key} to {new_stop_loss}, Order ID: {order_id}")
            self.connection_monitor.record_success()
            return True
        except ApiException as e:
            logger.error(f"API Error modifying stop-loss for {instrument_key}: {str(e)}")
            self.connection_monitor.record_failure()
            return False

    def _validate_df(self, df: pd.DataFrame) -> None:
        if df.empty or len(df) < 2:
            raise ValueError("Insufficient data for trade processing")
        if not df.index.is_monotonic_increasing:
            df.sort_index(ascending=True, inplace=True)
        if df[['open', 'high', 'low', 'close', 'volume']].lt(0).any().any():
            raise ValueError("Negative values detected in OHLCV data")

    def _process_trade(self, contract: Dict, current_price: float) -> None:
        try:
            instrument_key = contract["symbol_key"]
            vwap_df = self.vwap_data.get(instrument_key)
            #logger.info(f"Vwap Length :   {len(vwap_df)}")
            #logger.info(f"Vwap Data for Instrument {instrument_key} :   {vwap_df}")
            if vwap_df is None or vwap_df.empty or len(vwap_df) < 2:
                logger.warning(f"Insufficient VWAP data for {instrument_key}")
                return

            vwap_df.set_index("timestamp", inplace=True, drop=False)
            vwap_df.sort_index(ascending=True, inplace=True)
            #self._validate_df(vwap_df)

            running_candle = vwap_df.iloc[0]
            previous_candle = vwap_df.iloc[-1] if len(vwap_df) >= 2 else None
            vwap = running_candle["vwap"]

            if previous_candle is None:
                logger.warning(f"No previous candle available for {instrument_key}")
                return

            self.previous_candle[instrument_key] = {
                "high": float(previous_candle["high"]),
                "close": float(previous_candle["close"]),
                "timestamp": previous_candle.name
            }

            candle_data = {
                "timestamp": previous_candle.name,
                "open": float(previous_candle["open"]),
                "high": float(previous_candle["high"]),
                "low": float(previous_candle["low"]),
                "close": float(previous_candle["close"]),
                "volume": int(previous_candle["volume"]),
                "vwap": vwap
            }
            self.save_candle_data(instrument_key, candle_data)
            self.data_queue.put(("candle", (instrument_key, candle_data)))

            breakout_triggered = previous_candle["close"] > vwap and current_price > previous_candle["high"]
            if breakout_triggered:
                logger.info(f"Breakout triggered for {instrument_key}: {current_price} > {previous_candle['high']}")

            target = self.first_candle_high.get(instrument_key, 0.0)
            #logger.info(f"Previous Candle: {self.previous_candle[instrument_key]}, Current Price: {current_price}, VWAP: {vwap}, Target: {target}")
            #logger.info(f"Data Frame: {vwap_df}")

            if breakout_triggered and instrument_key not in [p["key"] for p in self.positions] and target > 0:
                entry_price = current_price
                atr = self._calculate_atr(vwap_df)
                stop_loss = entry_price * 0.7

                if stop_loss >= entry_price or target <= entry_price:
                    logger.warning(f"Invalid trade for {instrument_key}: SL={stop_loss}, Target={target}, Entry={entry_price}")
                    return

                order_id = self._place_buy_order(instrument_key, entry_price, self.config.QUANTITY)
                if not order_id:
                    logger.error(f"Failed to place buy order for {instrument_key}")
                    return

                trade = {
                    "key": instrument_key,
                    "contract_name": contract["contract_name"],
                    "symbol": contract["contract_name"],
                    "entry_time": datetime.now().isoformat(),
                    "entry_price": entry_price,
                    "stop_loss": stop_loss,
                    "target": target,
                    "status": "OPEN",
                    "highest_price": entry_price,
                    "exit_time": None,
                    "exit_price": None,
                    "order_id": order_id,
                    "initial_atr": atr,
                    "first_target_hit": False
                }
                self.positions.append(trade)
                if self.paper_trading:
                    self.paper_trades.append(trade.copy())

                self.save_trade_data(trade)
                self.data_queue.put(("trade", trade))
                logger.info(f"New position opened: {json.dumps(trade)}")

            for pos in self.positions:
                if pos["key"] != instrument_key or pos["status"] != "OPEN":
                    continue

                pos["highest_price"] = max(pos["highest_price"], current_price)

                if not pos["first_target_hit"] and current_price >= pos["target"]:
                    pos["first_target_hit"] = True
                    self.data_queue.put(("target_hit", pos))

                if self.enable_trailing_sl and pos["first_target_hit"]:
                    profit = pos["highest_price"] - pos["entry_price"]
                    atr = self._calculate_atr(vwap_df)
                    trail_percentage = 0.2 if profit >= pos["initial_atr"] * 2 else 0.3
                    new_stop_loss = pos["highest_price"] * (1 - trail_percentage)

                    if new_stop_loss > pos["stop_loss"] and new_stop_loss < pos["highest_price"]:
                        old_stop = pos["stop_loss"]
                        pos["stop_loss"] = new_stop_loss
                        if self._modify_stop_loss(instrument_key, new_stop_loss, pos["order_id"]):
                            logger.info(f"Trailing SL updated for {pos['symbol']}: {old_stop:.2f} -> {new_stop_loss:.2f}")
                        self.data_queue.put(("stop_loss_update", pos))

                current_time = datetime.now().time()

                if current_time >= self.config.MARKET_CLOSE:
                    self._close_position(pos, current_price, "MARKET_CLOSE")
                elif current_price >= pos["target"] and not self.enable_trailing_sl:
                    self._close_position(pos, current_price, "TARGET_HIT")
                elif current_price <= pos["stop_loss"]:
                    self._close_position(pos, current_price, "STOP_LOSS_HIT")

        except Exception as e:
            logger.error(f"Error processing trade for {contract['symbol_key']}: {str(e)}", exc_info=True)

    def _close_position(self, pos, exit_price, status):
        pos["status"] = status
        pos["exit_price"] = exit_price
        pos["exit_time"] = datetime.now().isoformat()
        self._place_sell_order(pos["key"], exit_price, self.config.QUANTITY, pos["order_id"])
        self.save_trade_data(pos)
        self.data_queue.put(("trade", pos))

        if status == "TARGET_HIT":
            logger.info(f"Target hit: {json.dumps(pos)}")
            target_hit_logger.info(f"Target Hit: {json.dumps(pos)}")
        elif status == "STOP_LOSS_HIT":
            logger.info(f"Stop loss hit: {json.dumps(pos)}")
            stop_loss_logger.info(f"Stop Loss Hit: {json.dumps(pos)}")
        else:
            logger.info(f"Position closed: {json.dumps(pos)}")

    def _reconnect_websocket(self):
        logger.info("Attempting WebSocket reconnection...")
        self.stop_websocket()
        time.sleep(self.connection_monitor.retry_delay)
        self.start_websocket()
        self.connection_monitor.connection_failures = 0
        logger.info("WebSocket reconnected successfully")
        self.data_queue.put(("status", "WebSocket reconnected"))

    def start_websocket(self):
        try:
            all_symbols = [c["symbol_key"] for contracts in self.filtered_contracts.values() for c in contracts]
            if not all_symbols:
                logger.error("No symbols to subscribe to WebSocket")
                self.data_queue.put(("error", "No symbols to subscribe to WebSocket"))
                return
            self.streamer = upstox_client.MarketDataStreamer(self.api_client, all_symbols, "full")
            self.streamer.on("message", self.on_market_message)
            self.streamer.on("open", lambda: logger.info("Websocket connected"))
            self.streamer.on("error", lambda error: logger.error(f"Websocket error: {error}"))
            self.running = True
            self.websocket_thread = threading.Thread(target=self.streamer.connect, daemon=True)
            self.websocket_thread.start()
            logger.info("WebSocket started")
            self.data_queue.put(("status", "WebSocket started"))
        except Exception as e:
            logger.error(f"WebSocket start error: {str(e)}")
            self.data_queue.put(("error", f"Failed to start WebSocket: {str(e)}"))
            self.running = False

    def stop_websocket(self):
        try:
            if self.running and self.streamer:
                self.running = False
                logger.info("Stopping WebSocket...")
                if self.websocket_thread and self.websocket_thread.is_alive():
                    self.websocket_thread.join(timeout=5)
                    if self.websocket_thread.is_alive():
                        logger.warning("WebSocket thread did not terminate within timeout")
                    else:
                        logger.info("WebSocket thread terminated successfully")
                self.streamer = None
                self.websocket_thread = None
                logger.info("WebSocket stopped")
                self.data_queue.put(("status", "WebSocket stopped"))
            else:
                logger.info("WebSocket already stopped or not running")
        except Exception as e:
            logger.error(f"Error stopping WebSocket: {str(e)}")
            self.data_queue.put(("error", f"Error stopping WebSocket: {str(e)}"))

    def save_results(self):
        try:
            output_dir = os.path.join(os.path.expanduser("~"), "Documents", "TradingBotLogs11")
            os.makedirs(output_dir, exist_ok=True)
            trades_file = os.path.join(output_dir, f'paper_trades_{datetime.now().strftime("%Y%m%d")}.json')
            with open(trades_file, 'w') as f:
                json.dump(self.paper_trades, f, indent=2)
            logger.info("Paper trading results saved")
        except Exception as e:
            logger.error(f"Error saving results: {str(e)}")

    def run(self):
        try:
            logger.info("Starting trading bot")
            self.data_queue.put(("status", "Fetching option contracts..."))
            contracts = self.fetch_option_contracts()
            if not contracts or not any(contracts.values()):
                logger.error("No contracts fetched, aborting")
                self.data_queue.put(("error", "No contracts fetched"))
                return
            self.data_queue.put(("status", "Filtering contracts..."))
            self.filter_contracts(contracts)
            if not self.filtered_contracts or not any(self.filtered_contracts.values()):
                logger.error("No contracts filtered, aborting")
                self.data_queue.put(("error", "No contracts filtered"))
                return
            self.running = True
            threading.Thread(target=self.calculate_vwap, daemon=True).start()
            self.start_websocket()
            while datetime.now().time() < self.config.MARKET_CLOSE and self.running:
                threading.Event().wait(60)
            self.running = False
            self.save_results()
            self._log_trade_summary()
            logger.info("Trading session completed")
            self.data_queue.put(("status", "Trading session completed"))
        except Exception as e:
            logger.error(f"Bot execution error: {str(e)}", exc_info=True)
            self.data_queue.put(("error", f"Bot execution error: {str(e)}"))
        finally:
            self.save_results()

    def stop(self):
        self.running = False
        self.stop_websocket()
        if self.bot_thread and self.bot_thread.is_alive():
            self.bot_thread.join(timeout=5)
        self.data_queue.put(("status", "Bot stopped"))

    def restart(self):
        self.stop()
        self.filtered_contracts = {}
        self.eligible_contracts = {}
        self.vwap_data = {}
        self.first_candle_high = {}
        self.previous_candle = {}
        self.positions = []
        self.paper_trades = []
        self.trade_log = []
        self.last_summary_time = None
        self.connection_monitor = ConnectionMonitor()
        self.bot_thread = threading.Thread(target=self.run, daemon=True)
        self.bot_thread.start()
        self.data_queue.put(("status", "Bot restarted"))

    def calculate_monthly_summary(self, month=None, year=None):
        if not month:
            month = datetime.now().month
        if not year:
            year = datetime.now().year
        start_date = datetime(year, month, 1).date().isoformat()
        end_date = (datetime(year, month, calendar.monthrange(year, month)[1]).date() + timedelta(days=1)).isoformat()
        trades = self.fetch_historical_trades(start_date, end_date)
        if not trades:
            logger.warning(f"No trades found for {month}/{year}")
            return 0, 0.0, 0.0, 0.0
        
        total_trades = len(trades)
        total_pnl = 0.0
        winners = 0
        for trade in trades:
            trade_dict = dict(zip(["trade_id", "instrument_key", "entry_time", "entry_price", "exit_time", 
                                   "exit_price", "status", "pnl", "contract_name", "symbol", "quantity", "trade_date", "trade_type"], trade))
            try:
                pnl = float(trade_dict.get("pnl", 0.0)) if trade_dict.get("pnl") is not None else 0.0
            except (ValueError, TypeError) as e:
                logger.error(f"Error converting trade data: {trade_dict}, error: {str(e)}")
                pnl = 0.0
            total_pnl += pnl
            if pnl > 0:
                winners += 1
        
        win_rate = (winners / total_trades) * 100 if total_trades > 0 else 0.0
        risk_reward = self.calculate_risk_reward(trades) if trades else 0.0
        logger.info(f"Monthly summary - Total Trades: {total_trades}, Total P/L: {total_pnl:.2f}, Win Rate: {win_rate:.2f}%, Risk-Reward: {risk_reward:.2f}")
        return total_trades, total_pnl, win_rate, risk_reward
    
    def calculate_risk_reward(self, trades):
        if not trades:
            return 0.0
        
        actual_columns = ["trade_id", "instrument_key", "entry_time", "entry_price", "exit_time", 
                         "exit_price", "status", "pnl", "contract_name", "symbol", "quantity", "trade_date", "trade_type"]
        
        winning_trades = []
        losing_trades = []
        
        for trade in trades:
            trade_dict = dict(zip(actual_columns, trade))
            try:
                trade_pnl = float(trade_dict.get("pnl", 0.0)) if trade_dict.get("pnl") is not None else 0.0
                if trade_pnl > 0:
                    winning_trades.append(trade_pnl)
                elif trade_pnl < 0:
                    losing_trades.append(abs(trade_pnl))
            except (ValueError, TypeError) as e:
                logger.error(f"Error calculating risk-reward for trade: {trade_dict}, error: {str(e)}")
                continue
        
        avg_win = sum(winning_trades) / len(winning_trades) if winning_trades else 0.0
        avg_loss = sum(losing_trades) / len(losing_trades) if losing_trades else 0.0
        
        risk_reward = avg_win / avg_loss if avg_loss != 0 else 0.0
        return risk_reward

    def backtest(self, instrument_key: str) -> Dict:
        try:
            conn = sqlite3.connect(os.path.join(log_dir, "trading_data.db"))
            c = conn.cursor()
            c.execute("SELECT * FROM candles WHERE instrument_key = ? ORDER BY timestamp", (instrument_key,))
            candles = c.fetchall()
            conn.close()
            if not candles:
                logger.info(f"No candle data for {instrument_key}")
                return {"instrument_key": instrument_key, "trades": [], "total_pnl": 0.0}
            df = pd.DataFrame(candles, columns=["instrument_key", "timestamp", "open", "high", "low", "close", "volume", "vwap", "date"])
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            df.set_index("timestamp", inplace=True)
            trades = []
            position = None
            for i in range(1, len(df)):
                prev_candle = df.iloc[i-1]
                current_candle = df.iloc[i]
                if position is None:
                    if prev_candle["close"] > prev_candle["vwap"] and current_candle["open"] > prev_candle["high"]:
                        entry_price = current_candle["open"]
                        atr = self._calculate_atr(df.iloc[:i])
                        stop_loss = entry_price - (atr * 1.5)
                        target = prev_candle["high"] * 1.05
                        position = {
                            "entry_time": current_candle.name,
                            "entry_price": entry_price,
                            "stop_loss": stop_loss,
                            "target": target
                        }
                elif position:
                    if current_candle["low"] <= position["stop_loss"]:
                        exit_price = min(current_candle["open"], position["stop_loss"])
                        pnl = exit_price - position["entry_price"]
                        trades.append({"entry_time": position["entry_time"], "exit_time": current_candle.name, "pnl": pnl})
                        position = None
                    elif current_candle["high"] >= position["target"]:
                        exit_price = max(current_candle["open"], position["target"])
                        pnl = exit_price - position["entry_price"]
                        trades.append({"entry_time": position["entry_time"], "exit_time": current_candle.name, "pnl": pnl})
                        position = None
            total_pnl = sum(t["pnl"] for t in trades) if trades else 0.0
            logger.info(f"Backtest for {instrument_key}: Total P/L = {total_pnl}, Trades = {len(trades)}")
            return {"instrument_key": instrument_key, "trades": trades, "total_pnl": total_pnl}
        except Exception as e:
            logger.error(f"Backtest error for {instrument_key}: {str(e)}")
            return {"instrument_key": instrument_key, "trades": [], "total_pnl": 0.0}

class TradingGUI(QMainWindow):
    def __init__(self, bot):
        super().__init__()
        self.bot = bot
        self.setWindowTitle("Trading Bot Dashboard")
        self.setGeometry(100, 100, 1200, 800)

        self.setStyleSheet("""
            QMainWindow { background-color: #1F252A; }
            QLabel { color: #D5D8DC; font-size: 14px; font-family: 'Segoe UI', sans-serif; }
            QCheckBox { color: #D5D8DC; font-size: 14px; font-family: 'Segoe UI', sans-serif; padding: 5px; }
            QCheckBox::indicator { width: 18px; height: 18px; border: 2px solid #5DADE2; background-color: #2C3539; border-radius: 4px; }
            QCheckBox::indicator:checked { background-color: #5DADE2; border: 2px solid #5DADE2; image: url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAgAAAAICAYAAADED76LAAAAHUlEQVQYV2P8z8Dwn4GBgYHB/4z8z8jAwMCgBQAeZQcXvP4iZQAAAABJRU5ErkJggg==); }
            QPushButton { background-color: #5DADE2; color: #FFFFFF; border: none; padding: 10px 20px; font-size: 14px; font-family: 'Segoe UI', sans-serif; border-radius: 6px; margin: 5px; }
            QPushButton:hover { background-color: #3498DB; }
            QPushButton:pressed { background-color: #2E86C1; }
            QPushButton:disabled { background-color: #4A5359; color: #85929E; }
            QTableWidget { background-color: #2C3539; color: #D5D8DC; font-size: 13px; font-family: 'Segoe UI', sans-serif; gridline-color: #4A5359; border: 1px solid #4A5359; border-radius: 6px; selection-background-color: #5DADE2; }
            QTableWidget::item { padding: 8px; border: none; }
            QTableWidget::item:alternate { background-color: #343B40; }
            QHeaderView::section { background-color: #5DADE2; color: #FFFFFF; padding: 8px; border: none; border-bottom: 1px solid #4A5359; font-size: 14px; font-family: 'Segoe UI', sans-serif; }
            QTabWidget::pane { border: 1px solid #4A5359; background-color: #2C3539; border-radius: 6px; }
            QTabBar::tab { background-color: #2C3539; color: #D5D8DC; padding: 10px 20px; margin-right: 4px; border-top-left-radius: 6px; border-top-right-radius: 6px; font-family: 'Segoe UI', sans-serif; font-size: 14px; }
            QTabBar::tab:selected { background-color: #5DADE2; color: #FFFFFF; }
            QTabBar::tab:hover { background-color: #4A5359; }
            QLineEdit { background-color: #2C3539; color: #D5D8DC; border: 1px solid #4A5359; padding: 8px; border-radius: 6px; font-family: 'Segoe UI', sans-serif; font-size: 14px; }
            QLineEdit:focus { border: 2px solid #5DADE2; }
            QFrame#summaryFrame { background-color: #2C3539; border: 1px solid #5DADE2; border-radius: 10px; padding: 10px; }
            QLabel#summaryTitle { color: #5DADE2; font-size: 16px; font-weight: bold; font-family: 'Segoe UI', sans-serif; padding-bottom: 5px; }
            QLabel#summaryValue { color: #D5D8DC; font-size: 14px; font-family: 'Segoe UI', sans-serif; padding: 2px 10px; }
            QLabel#statusBar { background-color: #1F252A; color: #D5D8DC; padding: 8px; font-size: 12px; border-top: 1px solid #4A5359; font-family: 'Segoe UI', sans-serif; }
            QTextEdit { background-color: #2C3539; color: #D5D8DC; border: 1px solid #4A5359; border-radius: 6px; font-family: 'Segoe UI', sans-serif; font-size: 14px; padding: 8px; }
        """)

        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)
        main_layout.setContentsMargins(10, 10, 10, 10)
        main_layout.setSpacing(10)

        nav_bar = QHBoxLayout()
        nav_bar.setSpacing(10)
        self.enable_trailing_sl = QCheckBox("Enable Trailing SL")
        self.enable_trailing_sl.setChecked(self.bot.enable_trailing_sl)
        self.enable_trailing_sl.stateChanged.connect(self.update_settings)
        self.paper_trading = QCheckBox("Paper Trading")
        self.paper_trading.setChecked(self.bot.paper_trading)
        self.paper_trading.stateChanged.connect(self.update_settings)
        self.start_button = QPushButton("Start Bot")
        self.start_button.clicked.connect(self.start_bot)
        self.stop_button = QPushButton("Stop Bot")
        self.stop_button.clicked.connect(self.stop_bot)
        self.restart_button = QPushButton("Restart Bot")
        self.restart_button.clicked.connect(self.restart_bot)
        self.clear_button = QPushButton("Clear Data")
        self.clear_button.clicked.connect(self.clear_data)
        self.export_button = QPushButton("Export Trades")
        self.export_button.clicked.connect(self.export_trades)
        self.exit_button = QPushButton("Exit")
        self.exit_button.clicked.connect(self.exit_app)
        self.refresh_button = QPushButton("Refresh Data")
        self.refresh_button.clicked.connect(self.refresh_data)

        nav_bar.addWidget(self.enable_trailing_sl)
        nav_bar.addWidget(self.paper_trading)
        nav_bar.addStretch()
        nav_bar.addWidget(self.start_button)
        nav_bar.addWidget(self.stop_button)
        nav_bar.addWidget(self.restart_button)
        nav_bar.addWidget(self.clear_button)
        nav_bar.addWidget(self.export_button)
        nav_bar.addWidget(self.exit_button)
        nav_bar.addWidget(self.refresh_button)
        main_layout.addLayout(nav_bar)

        content_layout = QHBoxLayout()
        content_layout.setSpacing(10)

        self.tabs = QTabWidget()
        self.tabs.setTabPosition(QTabWidget.North)
        content_layout.addWidget(self.tabs, stretch=3)

        self.all_filtered_tab = QWidget()
        self.all_filtered_layout = QVBoxLayout(self.all_filtered_tab)
        self.all_filtered_layout.setSpacing(10)
        self.search_bar = QLineEdit()
        self.search_bar.setPlaceholderText("Search contracts (e.g., Nifty, Strike)...")
        self.search_bar.textChanged.connect(self.filter_contracts_table)
        self.all_filtered_layout.addWidget(self.search_bar)
        self.all_filtered_table = QTableWidget()
        self.all_filtered_table.setColumnCount(7)
        self.all_filtered_table.setHorizontalHeaderLabels(["Index", "Contract Name", "Symbol Key", "Open", "High", "LTP", "Strike"])
        header = self.all_filtered_table.horizontalHeader()
        header.setSectionResizeMode(QHeaderView.Stretch)
        header.setDefaultAlignment(Qt.AlignCenter)
        self.all_filtered_table.setMinimumHeight(400)
        self.all_filtered_table.setSortingEnabled(True)
        self.all_filtered_layout.addWidget(self.all_filtered_table)
        self.all_filtered_status = QLabel("No filtered contracts")
        self.all_filtered_status.setAlignment(Qt.AlignCenter)
        self.all_filtered_layout.addWidget(self.all_filtered_status)
        self.tabs.addTab(self.all_filtered_tab, "All Filtered Contracts")

        self.active_tab = QWidget()
        self.active_layout = QVBoxLayout(self.active_tab)
        self.active_layout.setSpacing(10)
        self.active_table = QTableWidget()
        self.active_table.setColumnCount(9)
        self.active_table.setHorizontalHeaderLabels(["Contract Name", "Symbol", "Entry Time", "Entry Price", "Stop Loss", "Target", "Highest Price", "Order ID", "LTP"])
        header = self.active_table.horizontalHeader()
        header.setSectionResizeMode(QHeaderView.Stretch)
        header.setDefaultAlignment(Qt.AlignCenter)
        self.active_table.setSortingEnabled(True)
        self.active_layout.addWidget(self.active_table)
        self.active_status = QLabel("No active trades")
        self.active_status.setAlignment(Qt.AlignCenter)
        self.active_layout.addWidget(self.active_status)
        self.tabs.addTab(self.active_tab, "Active Trades")

        self.target_tab = QWidget()
        self.target_layout = QVBoxLayout(self.target_tab)
        self.target_layout.setSpacing(10)
        self.target_table = QTableWidget()
        self.target_table.setColumnCount(9)
        self.target_table.setHorizontalHeaderLabels(["Date", "Contract Name", "Symbol", "Type", "Entry Price", "Exit Price", "PNL", "Quantity", "Profit or Loss"])
        header = self.target_table.horizontalHeader()
        header.setSectionResizeMode(QHeaderView.Stretch)
        header.setDefaultAlignment(Qt.AlignCenter)
        self.target_table.setSortingEnabled(True)
        self.target_layout.addWidget(self.target_table)
        self.target_status = QLabel("No target trades")
        self.target_status.setAlignment(Qt.AlignCenter)
        self.target_layout.addWidget(self.target_status)
        self.tabs.addTab(self.target_tab, "Target")

        self.loss_tab = QWidget()
        self.loss_layout = QVBoxLayout(self.loss_tab)
        self.loss_layout.setSpacing(10)
        self.loss_table = QTableWidget()
        self.loss_table.setColumnCount(9)
        self.loss_table.setHorizontalHeaderLabels(["Date", "Contract Name", "Symbol", "Type", "Entry Price", "Exit Price", "PNL", "Quantity", "Profit or Loss"])
        header = self.loss_table.horizontalHeader()
        header.setSectionResizeMode(QHeaderView.Stretch)
        header.setDefaultAlignment(Qt.AlignCenter)
        self.loss_table.setSortingEnabled(True)
        self.loss_layout.addWidget(self.loss_table)
        self.loss_status = QLabel("No loss trades")
        self.loss_status.setAlignment(Qt.AlignCenter)
        self.loss_layout.addWidget(self.loss_status)
        self.tabs.addTab(self.loss_tab, "Loss")

        self.closed_tab = QWidget()
        self.closed_layout = QVBoxLayout(self.closed_tab)
        self.closed_layout.setSpacing(10)
        self.closed_table = QTableWidget()
        self.closed_table.setColumnCount(9)
        self.closed_table.setHorizontalHeaderLabels(["Date", "Contract Name", "Symbol", "Type", "Entry Price", "Exit Price", "PNL", "Quantity", "Profit or Loss"])
        header = self.closed_table.horizontalHeader()
        header.setSectionResizeMode(QHeaderView.Stretch)
        header.setDefaultAlignment(Qt.AlignCenter)
        self.closed_table.setSortingEnabled(True)
        self.closed_layout.addWidget(self.closed_table)
        self.closed_status = QLabel("No closed trades")
        self.closed_status.setAlignment(Qt.AlignCenter)
        self.closed_layout.addWidget(self.closed_status)
        self.tabs.addTab(self.closed_tab, "Closed")

        self.backtest_tab = QWidget()
        self.backtest_layout = QVBoxLayout(self.backtest_tab)
        self.backtest_layout.setSpacing(10)
        backtest_input_layout = QHBoxLayout()
        self.backtest_symbol_input = QLineEdit()
        self.backtest_symbol_input.setPlaceholderText("Enter Instrument Key (e.g., NSE_FO|12345)")
        backtest_button = QPushButton("Run Backtest")
        backtest_button.clicked.connect(self.run_backtest)
        backtest_input_layout.addWidget(self.backtest_symbol_input)
        backtest_input_layout.addWidget(backtest_button)
        self.backtest_layout.addLayout(backtest_input_layout)
        self.backtest_result = QTextEdit()
        self.backtest_result.setReadOnly(True)
        self.backtest_result.setMinimumHeight(400)
        self.backtest_layout.addWidget(self.backtest_result)
        self.backtest_status = QLabel("Enter an instrument key to run backtest")
        self.backtest_status.setAlignment(Qt.AlignCenter)
        self.backtest_layout.addWidget(self.backtest_status)
        self.tabs.addTab(self.backtest_tab, "Backtest")

        right_panel = QVBoxLayout()
        summary_frame = QFrame()
        summary_frame.setObjectName("summaryFrame")
        summary_layout = QVBoxLayout(summary_frame)
        summary_layout.setSpacing(5)
        summary_title = QLabel("Trade Summary")
        summary_title.setObjectName("summaryTitle")
        summary_title.setAlignment(Qt.AlignCenter)
        summary_layout.addWidget(summary_title)

        self.total_trades_label = QLabel("Total Trades: 0")
        self.total_trades_label.setObjectName("summaryValue")
        summary_layout.addWidget(self.total_trades_label)
        self.total_pnl_label = QLabel("Total P/L: 0.00")
        self.total_pnl_label.setObjectName("summaryValue")
        summary_layout.addWidget(self.total_pnl_label)
        self.win_rate_label = QLabel("Win Rate: 0.00%")
        self.win_rate_label.setObjectName("summaryValue")
        summary_layout.addWidget(self.win_rate_label)
        self.risk_reward_label = QLabel("Risk-Reward: 0.00")
        self.risk_reward_label.setObjectName("summaryValue")
        summary_layout.addWidget(self.risk_reward_label)
        
        right_panel.addWidget(summary_frame)
        right_panel.addStretch()

        content_layout.addLayout(right_panel, stretch=1)
        main_layout.addLayout(content_layout)

        self.status_bar = QLabel("Bot Status: Idle")
        self.status_bar.setObjectName("statusBar")
        self.status_bar.setAlignment(Qt.AlignLeft | Qt.AlignVCenter)
        main_layout.addWidget(self.status_bar)

                # Check auto-start status
        current_time = datetime.now().time()
        start_time = dt_time(9, 15)
        if current_time < start_time:
            self.status_bar.setText(f"Bot Status: Scheduled to start at 09:15")
        elif not self.bot.bot_thread or not self.bot.bot_thread.is_alive():
            self.status_bar.setText(f"Bot Status: Starting immediately (past 09:15)")
        else:
            self.status_bar.setText(f"Bot Status: Already Running")

        self.timer = QTimer(self)
        self.timer.timeout.connect(self.update_ui)
        self.timer.start(1000)

        self.update_trade_tables()
        self.load_initial_data()

    def update_settings(self):
        self.bot.enable_trailing_sl = self.enable_trailing_sl.isChecked()
        self.bot.paper_trading = self.paper_trading.isChecked()
        logger.info(f"Settings updated - Trailing SL: {self.bot.enable_trailing_sl}, Paper Trading: {self.bot.paper_trading}")
        self.status_bar.setText(f"Settings updated: Trailing SL={'On' if self.bot.enable_trailing_sl else 'Off'}, Paper Trading={'On' if self.bot.paper_trading else 'Off'}")

    def start_bot(self):
        if not self.bot.bot_thread or not self.bot.bot_thread.is_alive():
            self.bot.bot_thread = threading.Thread(target=self.bot.run, daemon=True)
            self.bot.bot_thread.start()
            self.start_button.setEnabled(False)
            self.stop_button.setEnabled(True)
            self.restart_button.setEnabled(True)
            self.status_bar.setText("Bot Status: Running")
            logger.info("Bot started from GUI")
        else:
            self.status_bar.setText("Bot Status: Already Running")

    def stop_bot(self):
        self.bot.stop()
        self.start_button.setEnabled(True)
        self.stop_button.setEnabled(False)
        self.restart_button.setEnabled(True)
        self.status_bar.setText("Bot Status: Stopped")
        logger.info("Bot stopped from GUI")

    def restart_bot(self):
        self.bot.restart()
        self.start_button.setEnabled(False)
        self.stop_button.setEnabled(True)
        self.restart_button.setEnabled(True)
        self.status_bar.setText("Bot Status: Restarted")
        logger.info("Bot restarted from GUI")

    def clear_data(self):
        self.bot.positions.clear()
        self.bot.paper_trades.clear()
        self.bot.trade_log.clear()
        self.all_filtered_table.setRowCount(0)
        self.active_table.setRowCount(0)
        self.target_table.setRowCount(0)
        self.loss_table.setRowCount(0)
        self.closed_table.setRowCount(0)
        self.total_trades_label.setText("Total Trades: 0")
        self.total_pnl_label.setText("Total P/L: 0.00")
        self.win_rate_label.setText("Win Rate: 0.00%")
        self.risk_reward_label.setText("Risk-Reward: 0.00")
        self.all_filtered_status.setText("No filtered contracts")
        self.active_status.setText("No active trades")
        self.target_status.setText("No target trades")
        self.loss_status.setText("No loss trades")
        self.closed_status.setText("No closed trades")
        self.status_bar.setText("Bot Status: Data Cleared")
        logger.info("Data cleared from GUI")

    def export_trades(self):
        try:
            output_dir = os.path.join(os.path.expanduser("~"), "Documents", "TradingBotLogs11")
            os.makedirs(output_dir, exist_ok=True)
            filename = QFileDialog.getSaveFileName(self, "Export Trades", output_dir, "Excel Files (*.xlsx)")[0]
            if filename:
                trades = self.bot.fetch_trade_data()
                if not trades:
                    self.status_bar.setText("No trades to export")
                    return
                df = pd.DataFrame(trades, columns=["Trade ID", "Instrument Key", "Entry Time", "Entry Price", "Exit Time", 
                                                 "Exit Price", "Status", "P/L", "Contract Name", "Symbol", "Quantity", "Trade Date", "Trade Type"])
                df.to_excel(filename, index=False)
                self.status_bar.setText(f"Trades exported to {filename}")
                logger.info(f"Trades exported to {filename}")
        except Exception as e:
            self.status_bar.setText(f"Export failed: {str(e)}")
            logger.error(f"Error exporting trades: {str(e)}")

    def exit_app(self):
        self.bot.stop()
        QApplication.quit()

    def refresh_data(self):
        self.update_trade_tables()
        total_trades, total_pnl, win_rate, risk_reward = self.bot.calculate_monthly_summary()
        self.total_trades_label.setText(f"Total Trades: {total_trades}")
        self.total_pnl_label.setText(f"Total P/L: {total_pnl:.2f}")
        self.win_rate_label.setText(f"Win Rate: {win_rate:.2f}%")
        self.risk_reward_label.setText(f"Risk-Reward: {risk_reward:.2f}")
        self.status_bar.setText("Data refreshed")
        logger.info("Data refreshed from GUI")

    def filter_contracts_table(self):
        search_text = self.search_bar.text().lower()
        for row in range(self.all_filtered_table.rowCount()):
            row_hidden = True
            for col in range(self.all_filtered_table.columnCount()):
                item = self.all_filtered_table.item(row, col)
                if item and search_text in item.text().lower():
                    row_hidden = False
                    break
            self.all_filtered_table.setRowHidden(row, row_hidden)

    def update_trade_tables(self):
        self.active_table.setRowCount(0)
        active_trades = [pos for pos in self.bot.positions if pos["status"] == "OPEN"]
        self.active_table.setRowCount(len(active_trades))
        for row, trade in enumerate(active_trades):
            self.active_table.setItem(row, 0, QTableWidgetItem(trade["contract_name"]))
            self.active_table.setItem(row, 1, QTableWidgetItem(trade["symbol"]))
            self.active_table.setItem(row, 2, QTableWidgetItem(trade["entry_time"]))
            self.active_table.setItem(row, 3, QTableWidgetItem(f"{trade['entry_price']:.2f}"))
            self.active_table.setItem(row, 4, QTableWidgetItem(f"{trade['stop_loss']:.2f}"))
            self.active_table.setItem(row, 5, QTableWidgetItem(f"{trade['target']:.2f}"))
            self.active_table.setItem(row, 6, QTableWidgetItem(f"{trade['highest_price']:.2f}"))
            self.active_table.setItem(row, 7, QTableWidgetItem(trade["order_id"]))
            ltp = self.bot.market_data_cache.get(trade["key"], {}).get("ltp", trade["entry_price"])
            self.active_table.setItem(row, 8, QTableWidgetItem(f"{ltp:.2f}"))
        self.active_status.setText(f"Active Trades: {len(active_trades)}")

        target_trades = self.bot.fetch_trade_data("TARGET_HIT")
        self.target_table.setRowCount(len(target_trades))
        for row, trade in enumerate(target_trades):
            trade_dict = dict(zip(["trade_id", "instrument_key", "entry_time", "entry_price", "exit_time", 
                                   "exit_price", "status", "pnl", "contract_name", "symbol", "quantity", "trade_date", "trade_type"], trade))
            self.target_table.setItem(row, 0, QTableWidgetItem(trade_dict["trade_date"] or "Unknown"))
            self.target_table.setItem(row, 1, QTableWidgetItem(trade_dict["contract_name"]))
            self.target_table.setItem(row, 2, QTableWidgetItem(trade_dict["symbol"]))
            self.target_table.setItem(row, 3, QTableWidgetItem(trade_dict["trade_type"]))
            self.target_table.setItem(row, 4, QTableWidgetItem(f"{trade_dict['entry_price']:.2f}"))
            self.target_table.setItem(row, 5, QTableWidgetItem(f"{trade_dict['exit_price']:.2f}" if trade_dict['exit_price'] else "N/A"))
            self.target_table.setItem(row, 6, QTableWidgetItem(f"{trade_dict['pnl']:.2f}" if trade_dict['pnl'] is not None else "0.00"))
            self.target_table.setItem(row, 7, QTableWidgetItem(str(trade_dict["quantity"])))
            self.target_table.setItem(row, 8, QTableWidgetItem("Profit" if trade_dict["pnl"] and trade_dict["pnl"] > 0 else "Loss"))
        self.target_status.setText(f"Target Trades: {len(target_trades)}")

        loss_trades = self.bot.fetch_trade_data("STOP_LOSS_HIT")
        self.loss_table.setRowCount(len(loss_trades))
        for row, trade in enumerate(loss_trades):
            trade_dict = dict(zip(["trade_id", "instrument_key", "entry_time", "entry_price", "exit_time", 
                                   "exit_price", "status", "pnl", "contract_name", "symbol", "quantity", "trade_date", "trade_type"], trade))
            self.loss_table.setItem(row, 0, QTableWidgetItem(trade_dict["trade_date"] or "Unknown"))
            self.loss_table.setItem(row, 1, QTableWidgetItem(trade_dict["contract_name"]))
            self.loss_table.setItem(row, 2, QTableWidgetItem(trade_dict["symbol"]))
            self.loss_table.setItem(row, 3, QTableWidgetItem(trade_dict["trade_type"]))
            self.loss_table.setItem(row, 4, QTableWidgetItem(f"{trade_dict['entry_price']:.2f}"))
            self.loss_table.setItem(row, 5, QTableWidgetItem(f"{trade_dict['exit_price']:.2f}" if trade_dict['exit_price'] else "N/A"))
            self.loss_table.setItem(row, 6, QTableWidgetItem(f"{trade_dict['pnl']:.2f}" if trade_dict['pnl'] is not None else "0.00"))
            self.loss_table.setItem(row, 7, QTableWidgetItem(str(trade_dict["quantity"])))
            self.loss_table.setItem(row, 8, QTableWidgetItem("Loss" if trade_dict["pnl"] and trade_dict["pnl"] < 0 else "Profit"))
        self.loss_status.setText(f"Loss Trades: {len(loss_trades)}")

        closed_trades = self.bot.fetch_trade_data("MARKET_CLOSE")
        self.closed_table.setRowCount(len(closed_trades))
        for row, trade in enumerate(closed_trades):
            trade_dict = dict(zip(["trade_id", "instrument_key", "entry_time", "entry_price", "exit_time", 
                                   "exit_price", "status", "pnl", "contract_name", "symbol", "quantity", "trade_date", "trade_type"], trade))
            self.closed_table.setItem(row, 0, QTableWidgetItem(trade_dict["trade_date"] or "Unknown"))
            self.closed_table.setItem(row, 1, QTableWidgetItem(trade_dict["contract_name"]))
            self.closed_table.setItem(row, 2, QTableWidgetItem(trade_dict["symbol"]))
            self.closed_table.setItem(row, 3, QTableWidgetItem(trade_dict["trade_type"]))
            self.closed_table.setItem(row, 4, QTableWidgetItem(f"{trade_dict['entry_price']:.2f}"))
            self.closed_table.setItem(row, 5, QTableWidgetItem(f"{trade_dict['exit_price']:.2f}" if trade_dict['exit_price'] else "N/A"))
            self.closed_table.setItem(row, 6, QTableWidgetItem(f"{trade_dict['pnl']:.2f}" if trade_dict['pnl'] is not None else "0.00"))
            self.closed_table.setItem(row, 7, QTableWidgetItem(str(trade_dict["quantity"])))
            self.closed_table.setItem(row, 8, QTableWidgetItem("Profit" if trade_dict["pnl"] and trade_dict["pnl"] > 0 else "Loss"))
        self.closed_status.setText(f"Closed Trades: {len(closed_trades)}")

    def load_initial_data(self):
        contracts = []
        for contract_list in self.bot.filtered_contracts.values():
            contracts.extend(contract_list)
        self.all_filtered_table.setRowCount(len(contracts))
        for row, contract in enumerate(contracts):
            self.all_filtered_table.setItem(row, 0, QTableWidgetItem(contract["index"]))
            self.all_filtered_table.setItem(row, 1, QTableWidgetItem(contract["contract_name"]))
            self.all_filtered_table.setItem(row, 2, QTableWidgetItem(contract["symbol_key"]))
            self.all_filtered_table.setItem(row, 3, QTableWidgetItem(f"{contract['open']:.2f}"))
            self.all_filtered_table.setItem(row, 4, QTableWidgetItem(f"{contract['high']:.2f}"))
            self.all_filtered_table.setItem(row, 5, QTableWidgetItem(f"{contract['ltp']:.2f}"))
            self.all_filtered_table.setItem(row, 6, QTableWidgetItem(f"{contract['strike']:.2f}"))
        self.all_filtered_status.setText(f"All Filtered Contracts: {len(contracts)}")

    def update_ui(self):
        try:
            while not self.bot.data_queue.empty():
                msg_type, data = self.bot.data_queue.get_nowait()
                if msg_type == "all_filtered_contracts":
                    self.all_filtered_table.setRowCount(len(data))
                    for row, contract in enumerate(data):
                        self.all_filtered_table.setItem(row, 0, QTableWidgetItem(contract["index"]))
                        self.all_filtered_table.setItem(row, 1, QTableWidgetItem(contract["contract_name"]))
                        self.all_filtered_table.setItem(row, 2, QTableWidgetItem(contract["symbol_key"]))
                        self.all_filtered_table.setItem(row, 3, QTableWidgetItem(f"{contract['open']:.2f}"))
                        self.all_filtered_table.setItem(row, 4, QTableWidgetItem(f"{contract['high']:.2f}"))
                        self.all_filtered_table.setItem(row, 5, QTableWidgetItem(f"{contract['ltp']:.2f}"))
                        self.all_filtered_table.setItem(row, 6, QTableWidgetItem(f"{contract['strike']:.2f}"))
                    self.all_filtered_status.setText(f"All Filtered Contracts: {len(data)}")
                elif msg_type == "trade":
                    self.update_trade_tables()
                elif msg_type == "ltp_update":
                    instrument_key, ltp = data
                    self.bot.market_data_cache[instrument_key] = {"ltp": ltp}
                    self.update_trade_tables()
                elif msg_type == "status":
                    self.status_bar.setText(f"Bot Status: {data}")
                elif msg_type == "error":
                    self.status_bar.setText(f"Error: {data}")
                elif msg_type == "summary_update":
                    total_trades, total_pnl, win_rate, risk_reward = data
                    self.total_trades_label.setText(f"Total Trades: {total_trades}")
                    self.total_pnl_label.setText(f"Total P/L: {total_pnl:.2f}")
                    self.win_rate_label.setText(f"Win Rate: {win_rate:.2f}%")
                    self.risk_reward_label.setText(f"Risk-Reward: {risk_reward:.2f}")
                elif msg_type == "candle":
                    pass  # Optionally handle candle updates if needed
                elif msg_type == "stop_loss_update" or msg_type == "target_hit":
                    self.update_trade_tables()
        except Exception as e:
            logger.error(f"UI update error: {str(e)}")
            self.status_bar.setText(f"UI Error: {str(e)}")

    def run_backtest(self):
        symbol = self.backtest_symbol_input.text().strip()
        if not symbol:
            self.backtest_status.setText("Please enter a valid instrument key")
            return
        self.backtest_status.setText(f"Running backtest for {symbol}...")
        result = self.bot.backtest(symbol)
        output = f"Backtest Results for {symbol}:\n"
        output += f"Total Trades: {len(result['trades'])}\n"
        output += f"Total P/L: {result['total_pnl']:.2f}\n"
        for trade in result["trades"]:
            output += f"Entry: {trade['entry_time']} | Exit: {trade['exit_time']} | P/L: {trade['pnl']:.2f}\n"
        self.backtest_result.setText(output)
        self.backtest_status.setText(f"Backtest completed for {symbol}")

if __name__ == "__main__":
    patch_upstox_client_ssl()
    bot = TradingBot()
    app = QApplication(sys.argv)
    gui = TradingGUI(bot)
    
    # Auto-start logic
    current_time = datetime.now().time()
    start_time = dt_time(9, 15)
    
    if current_time < start_time:
        # Calculate delay until 09:15
        now = datetime.now()
        start_datetime = datetime.combine(now.date(), start_time)
        if now > start_datetime:
            start_datetime += timedelta(days=1)  # Next day if already past 09:15 today
        delay_seconds = (start_datetime - now).total_seconds()
        logger.info(f"Bot will auto-start in {delay_seconds:.0f} seconds at 09:15")
        QTimer.singleShot(int(delay_seconds * 1000), gui.start_bot)
    else:
        # Start immediately if past 09:15
        logger.info("Current time is past 09:15, starting bot immediately")
        gui.start_bot()

    gui.show()
    sys.exit(app.exec_())