#!/usr/bin/env python3
# ============================================================
# Advanced Merged ETH Trading Bot - Best of Both Worlds
# ============================================================
# Features:
# - Multi-provider WebSocket streams (Binance primary, Kraken fallback)
# - Advanced signal engine with Camarilla pivots, Alligator, MACD
# - Sophisticated trade management (TP1/TP2/TP3 with partial closes)
# - Comprehensive Discord interface with rich analytics
# - Real-time monitoring with minute-aligned scanning
# - Google Sheets integration with full rehydration
# - Health monitoring server
# - Advanced risk management with ATR-based stops
# ============================================================

import os
import csv
import json
import math
import time
import uuid
import asyncio
import logging
import signal
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict, Any, Tuple
from collections import defaultdict
from threading import Thread

import aiohttp
import pandas as pd
import numpy as np
import discord
from discord.ext import tasks, commands
from flask import Flask

# ----- Logging -----
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s')
log = logging.getLogger("MergedETHBot")

# ----- Config -----
def env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)).strip())
    except Exception:
        return default

def env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)).strip())
    except Exception:
        return default

@dataclass
class Config:
    # Discord
    discord_token: str = field(default_factory=lambda: os.getenv("DISCORD_TOKEN", "").strip())
    channel_id: int = field(default_factory=lambda: env_int("DISCORD_CHANNEL_ID", 0))
    
    # Data providers
    provider: str = field(default_factory=lambda: os.getenv("PROVIDER", "binance").lower())
    use_websocket: bool = field(default_factory=lambda: os.getenv("USE_WEBSOCKET", "1") == "1")
    binance_symbol: str = field(default_factory=lambda: os.getenv("BINANCE_SYMBOL", "ETHUSDT").strip())
    kraken_pair: str = field(default_factory=lambda: os.getenv("KRAKEN_PAIR", "ETHUSD").strip())
    
    # Google Sheets
    sheets_webhook: Optional[str] = field(default_factory=lambda: os.getenv("GOOGLE_SHEETS_WEBHOOK", "").strip() or None)
    sheets_secret: Optional[str] = field(default_factory=lambda: os.getenv("SHEETS_SHARED_SECRET", "").strip() or None)
    
    # Trading parameters
    scan_every_s: int = field(default_factory=lambda: env_int("SCAN_EVERY_SECONDS", 60))
    alert_cooldown_minutes: int = field(default_factory=lambda: env_int("ALERT_COOLDOWN_MINUTES", 30))
    min_signal_score: int = field(default_factory=lambda: env_int("MIN_SIGNAL_SCORE", 4))
    partial_fraction: float = field(default_factory=lambda: env_float("PARTIAL_FRACTION", 0.5))
    
    # Risk management
    risk_atr_mult: float = field(default_factory=lambda: env_float("RISK_ATR_MULT", 1.0))
    tp1_r_multiple: float = field(default_factory=lambda: env_float("TP1_R_MULTIPLE", 1.5))
    tp2_r_multiple: float = field(default_factory=lambda: env_float("TP2_R_MULTIPLE", 3.0))
    tp3_r_multiple: float = field(default_factory=lambda: env_float("TP3_R_MULTIPLE", 4.5))
    use_tp3_close: bool = field(default_factory=lambda: os.getenv("USE_TP3_CLOSE", "0") == "1")
    
    # Technical indicators
    donchian_len: int = field(default_factory=lambda: env_int("DONCHIAN_LENGTH", 20))
    ema_fast: int = field(default_factory=lambda: env_int("EMA_FAST", 20))
    ema_slow: int = field(default_factory=lambda: env_int("EMA_SLOW", 50))
    ema_long: int = field(default_factory=lambda: env_int("EMA_LONG", 200))
    rsi_len: int = field(default_factory=lambda: env_int("RSI_LENGTH", 14))
    atr_len: int = field(default_factory=lambda: env_int("ATR_LENGTH", 14))
    macd_fast: int = field(default_factory=lambda: env_int("MACD_FAST", 12))
    macd_slow: int = field(default_factory=lambda: env_int("MACD_SLOW", 26))
    macd_signal: int = field(default_factory=lambda: env_int("MACD_SIGNAL", 9))
    
    # Signal filters
    volume_spike_mult: float = field(default_factory=lambda: env_float("VOLUME_SPIKE_MULT", 1.2))
    min_body_ratio: float = field(default_factory=lambda: env_float("MIN_BODY_RATIO", 0.5))
    entry_buffer_atr: float = field(default_factory=lambda: env_float("ENTRY_BUFFER_ATR", 0.25))
    
    # Timezone
    timezone_str: str = field(default_factory=lambda: os.getenv("TIMEZONE", "America/Chicago"))

CFG = Config()

# ----- Time Helpers -----
try:
    from zoneinfo import ZoneInfo
    CT_TZ = ZoneInfo(CFG.timezone_str)
except Exception:
    CT_TZ = timezone.utc

def ts_now():
    return datetime.now(timezone.utc)

def fmt_money(x: float) -> str:
    return f"${x:,.2f}"

def now_footer() -> str:
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    try:
        now_ct = datetime.now(CT_TZ).strftime("%Y-%m-%d %I:%M %p CT")
        return f"{now_ct} • {now_utc}"
    except Exception:
        return now_utc

def sleep_time_to_next_bar(seconds=60) -> float:
    t = time.time()
    delay = seconds - (t % seconds)
    return max(0.001, min(seconds, delay + 0.002))

# ----- Data Models -----
@dataclass
class Trade:
    id: str
    pair: str
    side: str               # LONG or SHORT
    strategy: str           # Breakout or Pullback
    status: str             # OPEN/CLOSED
    created_ts: float       # epoch seconds
    entry_low: float
    entry_high: float
    entry_mid: float
    stop_loss: float
    tp1: float
    tp2: float
    tp3: Optional[float] = None
    reason: str = ""
    path: str = ""
    score: int = 0          # Signal quality score
    market_structure: str = "Unknown"
    opened_price: Optional[float] = None
    exit_price: Optional[float] = None
    exit_reason: Optional[str] = None
    hit_tp1: bool = False
    hit_tp2: bool = False
    hit_tp3: bool = False
    partial_done: bool = False
    moved_to_be: bool = False
    be_price: Optional[float] = None
    r_multiple: float = 0.0

@dataclass
class PerformanceStats:
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    tp1_hits: int = 0
    tp2_hits: int = 0
    tp3_hits: int = 0
    sl_hits: int = 0
    be_hits: int = 0
    total_r: float = 0.0
    
    @property
    def win_rate(self) -> float:
        if self.total_trades == 0:
            return 0.0
        return (self.winning_trades / self.total_trades) * 100
    
    @property
    def avg_r(self) -> float:
        if self.total_trades == 0:
            return 0.0
        return self.total_r / self.total_trades

# ----- Market Data Provider -----
class MarketDataProvider:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.session: Optional[aiohttp.ClientSession] = None
        self.df: Optional[pd.DataFrame] = None
        self._latest_price: float = np.nan
        self._ws_task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()

    async def start(self):
        if not self.session:
            timeout = aiohttp.ClientTimeout(total=20)
            self.session = aiohttp.ClientSession(timeout=timeout)
        
        # Bootstrap with REST data
        await self._bootstrap_df()
        
        # Start WebSocket if enabled
        if self.cfg.use_websocket:
            self._ws_task = asyncio.create_task(self._ws_loop())

    async def stop(self):
        self._stop.set()
        if self._ws_task:
            self._ws_task.cancel()
        if self.session:
            await self.session.close()
            self.session = None

    async def _bootstrap_df(self):
        try:
            if self.cfg.provider == "binance":
                df = await self._binance_rest_klines(self.cfg.binance_symbol, "1m", 600)
            else:
                df = await self._kraken_rest_ohlc(self.cfg.kraken_pair, 1, 600)
            
            if df is not None and not df.empty:
                self.df = df
                self._latest_price = float(df["close"].iloc[-1])
                log.info(f"Bootstrapped {len(df)} candles from {self.cfg.provider}")
        except Exception as e:
            log.error(f"Bootstrap failed: {e}")

    async def _binance_rest_klines(self, symbol: str, interval: str, limit: int) -> pd.DataFrame:
        url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval={interval}&limit={limit}"
        async with self.session.get(url) as r:
            data = await r.json()
        
        cols = ["open_time", "open", "high", "low", "close", "volume", "close_time",
                "quote_volume", "trades", "taker_base_vol", "taker_quote_vol", "ignore"]
        df = pd.DataFrame(data, columns=cols)
        df["time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
        
        for c in ["open", "high", "low", "close", "volume"]:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        
        df = df[["time", "open", "high", "low", "close", "volume"]].dropna()
        df = df.sort_values("time").reset_index(drop=True)
        return df

    async def _kraken_rest_ohlc(self, pair: str, interval: int, limit: int) -> pd.DataFrame:
        url = f"https://api.kraken.com/0/public/OHLC?pair={pair}&interval={interval}"
        async with self.session.get(url) as r:
            data = await r.json()
        
        if data.get("error"):
            raise Exception(f"Kraken error: {data['error']}")
        
        result = data.get("result", {})
        key = next((k for k in result.keys() if k != "last"), None)
        if not key:
            raise Exception("No OHLC data in response")
        
        rows = result[key]
        cols = ["time", "open", "high", "low", "close", "vwap", "volume", "count"]
        df = pd.DataFrame(rows, columns=cols)
        df["time"] = pd.to_datetime(df["time"], unit="s", utc=True)
        
        for c in ["open", "high", "low", "close", "volume"]:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        
        df = df[["time", "open", "high", "low", "close", "volume"]].dropna()
        df = df.sort_values("time").tail(limit).reset_index(drop=True)
        return df

    async def _ws_loop(self):
        backoff = [1, 2, 5, 10, 20, 30]
        i = 0
        while not self._stop.is_set():
            try:
                if self.cfg.provider == "binance":
                    await self._binance_ws_kline()
                else:
                    await self._kraken_ws_ohlc()
                i = 0  # Reset backoff on success
            except asyncio.CancelledError:
                break
            except Exception as e:
                delay = backoff[min(i, len(backoff)-1)]
                log.warning(f"WS error ({self.cfg.provider}): {e}. Reconnecting in {delay}s")
                await asyncio.sleep(delay)
                i += 1

    async def _binance_ws_kline(self):
        symbol = self.cfg.binance_symbol.lower()
        url = f"wss://stream.binance.com:9443/stream?streams={symbol}@kline_1m"
        
        async with self.session.ws_connect(url, heartbeat=20) as ws:
            log.info("Connected to Binance WebSocket")
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = json.loads(msg.data)
                    k = data.get("data", {}).get("k", {})
                    
                    t_open = datetime.fromtimestamp(k.get("t", 0)/1000, tz=timezone.utc)
                    o = float(k.get("o", 0))
                    h = float(k.get("h", 0))
                    l = float(k.get("l", 0))
                    c = float(k.get("c", 0))
                    v = float(k.get("v", 0))
                    final = bool(k.get("x", False))
                    
                    self._latest_price = c
                    self._upsert_candle(t_open, o, h, l, c, v, final)
                    
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    raise RuntimeError("WebSocket error")

    async def _kraken_ws_ohlc(self):
        url = "wss://ws.kraken.com/"
        
        async with self.session.ws_connect(url, heartbeat=20) as ws:
            # Subscribe to OHLC
            sub = {
                "event": "subscribe",
                "pair": [self.cfg.kraken_pair],
                "subscription": {"name": "ohlc", "interval": 1}
            }
            await ws.send_json(sub)
            log.info("Connected to Kraken WebSocket")
            
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = json.loads(msg.data)
                    if isinstance(data, list) and len(data) > 1 and isinstance(data[1], list):
                        arr = data[1]
                        t_open = datetime.fromtimestamp(float(arr[0]), tz=timezone.utc)
                        o = float(arr[2])
                        h = float(arr[3])
                        l = float(arr[4])
                        c = float(arr[5])
                        v = float(arr[7])
                        
                        # Estimate if candle is final
                        etime = float(arr[1])
                        final = (int(etime) % 60 == 0)
                        
                        self._latest_price = c
                        self._upsert_candle(t_open, o, h, l, c, v, final)
                        
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    raise RuntimeError("WebSocket error")

    def _upsert_candle(self, t_open: datetime, o: float, h: float, l: float, c: float, v: float, final: bool):
        if self.df is None:
            self.df = pd.DataFrame(columns=["time", "open", "high", "low", "close", "volume"])
        
        # Convert to timestamp for indexing
        ts = pd.Timestamp(t_open)
        
        # Update existing or add new candle
        mask = self.df["time"] == ts
        if mask.any():
            idx = mask.idxmax()
            self.df.loc[idx, ["open", "high", "low", "close", "volume"]] = [o, h, l, c, v]
        else:
            new_row = pd.DataFrame({
                "time": [ts],
                "open": [o],
                "high": [h],
                "low": [l],
                "close": [c],
                "volume": [v]
            })
            self.df = pd.concat([self.df, new_row], ignore_index=True)
        
        # Keep last 1000 candles
        if len(self.df) > 1200:
            self.df = self.df.tail(1000).reset_index(drop=True)

    async def fetch_latest_price(self) -> Optional[float]:
        try:
            if self.cfg.provider == "binance":
                url = f"https://api.binance.com/api/v3/ticker/price?symbol={self.cfg.binance_symbol}"
                async with self.session.get(url) as r:
                    data = await r.json()
                price = float(data.get("price"))
            else:
                url = f"https://api.kraken.com/0/public/Ticker?pair={self.cfg.kraken_pair}"
                async with self.session.get(url) as r:
                    data = await r.json()
                result = data.get("result", {})
                key = next(iter(result.keys()))
                price = float(result[key]["c"][0])
            
            self._latest_price = price
            return price
        except Exception as e:
            log.error(f"Failed to fetch latest price: {e}")
            return None

    def get_df(self) -> Optional[pd.DataFrame]:
        return self.df.copy() if self.df is not None else None

    def latest_price(self) -> Optional[float]:
        return None if math.isnan(self._latest_price) else self._latest_price

# ----- Technical Indicators -----
def ema(series: pd.Series, length: int) -> pd.Series:
    return series.ewm(span=length, adjust=False).mean()

def rma(series: pd.Series, length: int) -> pd.Series:
    return series.ewm(alpha=1/length, adjust=False).mean()

def rsi(series: pd.Series, length: int = 14) -> pd.Series:
    delta = series.diff()
    up = np.where(delta > 0, delta, 0.0)
    down = np.where(delta < 0, -delta, 0.0)
    roll_up = pd.Series(up, index=series.index).ewm(alpha=1/length, adjust=False).mean()
    roll_down = pd.Series(down, index=series.index).ewm(alpha=1/length, adjust=False).mean()
    rs = roll_up / (roll_down + 1e-9)
    return 100 - (100 / (1 + rs))

def true_range(df: pd.DataFrame) -> pd.Series:
    prev_close = df["close"].shift(1)
    tr1 = (df["high"] - df["low"]).abs()
    tr2 = (df["high"] - prev_close).abs()
    tr3 = (df["low"] - prev_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    return tr

def atr(df: pd.DataFrame, length: int = 14) -> pd.Series:
    return true_range(df).rolling(length).mean()

def vwap(df: pd.DataFrame) -> pd.Series:
    tp = (df["high"] + df["low"] + df["close"]) / 3.0
    vol = df["volume"].replace(0, np.nan).fillna(method="ffill")
    cum_tp_vol = (tp * vol).cumsum()
    cum_vol = vol.cumsum().replace(0, np.nan).fillna(method="ffill")
    return cum_tp_vol / cum_vol

def donchian(df: pd.DataFrame, length: int = 20) -> Tuple[pd.Series, pd.Series]:
    return df["high"].rolling(length).max(), df["low"].rolling(length).min()

def body_ratio(df: pd.DataFrame) -> pd.Series:
    body = (df["close"] - df["open"]).abs()
    rng = (df["high"] - df["low"]).replace(0, np.nan)
    return (body / rng).fillna(0)

def alligator(df: pd.DataFrame) -> Tuple[pd.Series, pd.Series, pd.Series]:
    jaw = rma(df["close"], 13)
    teeth = rma(df["close"], 8)
    lips = rma(df["close"], 5)
    return jaw, teeth, lips

def calculate_macd(df: pd.DataFrame, fast: int = 12, slow: int = 26, signal: int = 9) -> Tuple[pd.Series, pd.Series, pd.Series]:
    exp1 = df['close'].ewm(span=fast, adjust=False).mean()
    exp2 = df['close'].ewm(span=slow, adjust=False).mean()
    macd = exp1 - exp2
    macd_signal = macd.ewm(span=signal, adjust=False).mean()
    macd_hist = macd - macd_signal
    return macd, macd_signal, macd_hist

def calculate_camarilla(high: float, low: float, close: float) -> Dict[str, float]:
    range_ = high - low
    return {
        "H5": close + (range_ * 1.1 / 2),
        "H4": close + (range_ * 1.1 / 4),
        "H3": close + (range_ * 1.1 / 6),
        "H2": close + (range_ * 1.1 / 12),
        "H1": close + (range_ * 0.55 / 12),
        "P": (high + low + close) / 3,
        "L1": close - (range_ * 0.55 / 12),
        "L2": close - (range_ * 1.1 / 12),
        "L3": close - (range_ * 1.1 / 6),
        "L4": close - (range_ * 1.1 / 4),
        "L5": close - (range_ * 1.1 / 2)
    }

def previous_day_levels(df: pd.DataFrame) -> Dict[str, float]:
    if df.empty:
        return {}
    
    last_ts = df["time"].iloc[-1]
    day_today = last_ts.floor("D")
    day_yest = day_today - pd.Timedelta(days=1)
    
    mask = (df["time"] >= day_yest) & (df["time"] < day_today)
    sub = df.loc[mask]
    
    if sub.empty:
        return {}
    
    H = float(sub["high"].max())
    L = float(sub["low"].min())
    C = float(sub["close"].iloc[-1])
    
    cam_levels = calculate_camarilla(H, L, C)
    
    return {
        "pivot": cam_levels["P"],
        "H3": cam_levels["H3"],
        "H4": cam_levels["H4"],
        "L3": cam_levels["L3"],
        "L4": cam_levels["L4"],
        "H": H,
        "L": L,
        "C": C,
        **cam_levels
    }

# ----- Sheets Client -----
class SheetsClient:
    def __init__(self, url: Optional[str], secret: Optional[str] = None):
        self.url = url
        self.secret = secret

    async def _headers(self) -> Dict[str, str]:
        h = {"Content-Type": "application/json"}
        if self.secret:
            h["x-app-secret"] = self.secret
        return h

    async def create_entry(self, session: aiohttp.ClientSession, payload: Dict[str, Any]) -> None:
        if not self.url:
            return
        try:
            async with session.post(self.url, headers=await self._headers(), 
                                  data=json.dumps(payload), timeout=aiohttp.ClientTimeout(total=10)) as resp:
                await resp.text()
        except Exception as e:
            log.warning(f"Sheets create_entry failed: {e}")

    async def update_entry(self, session: aiohttp.ClientSession, payload: Dict[str, Any]) -> None:
        if not self.url:
            return
        try:
            data = dict(payload)
            data["action"] = "update"
            async with session.post(self.url, headers=await self._headers(), 
                                  data=json.dumps(data), timeout=aiohttp.ClientTimeout(total=10)) as resp:
                await resp.text()
        except Exception as e:
            log.warning(f"Sheets update_entry failed: {e}")

    async def rehydrate_open(self, session: aiohttp.ClientSession) -> List[Dict[str, Any]]:
        if not self.url:
            return []
        try:
            sep = "&" if "?" in self.url else "?"
            key = f"&key={self.secret}" if self.secret else ""
            url = f"{self.url}{sep}action=open{key}"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                txt = await resp.text()
                try:
                    data = json.loads(txt)
                except Exception:
                    log.warning(f"Sheets rehydrate non-JSON: {txt[:120]}")
                    return []
                if isinstance(data, list):
                    return data
                return data.get("rows", [])
        except Exception as e:
            log.warning(f"Sheets rehydrate_open failed: {e}")
            return []

# ----- Analytics Logging -----
ALERTS_CSV = "alerts_log.csv"
DECISIONS_CSV = "decisions_log.csv"
FILLS_CSV = "fills_log.csv"

def append_csv(path: str, row: Dict[str, Any]):
    exists = os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=row.keys())
        if not exists:
            w.writeheader()
        w.writerow(row)

# ----- Enhanced Signal Engine -----
class EnhancedSignalEngine:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.last_alert_time = {}

    def add_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df["ema_fast"] = ema(df["close"], self.cfg.ema_fast)
        df["ema_slow"] = ema(df["close"], self.cfg.ema_slow)
        df["ema_long"] = ema(df["close"], self.cfg.ema_long)
        df["rsi"] = rsi(df["close"], self.cfg.rsi_len)
        df["atr"] = atr(df, self.cfg.atr_len).fillna(method="ffill")
        df["vwap"] = vwap(df)
        df["body_ratio"] = body_ratio(df)
        
        dc_h, dc_l = donchian(df, self.cfg.donchian_len)
        df["dc_high"] = dc_h
        df["dc_low"] = dc_l
        
        df["vol_ma"] = df["volume"].rolling(20).mean()
        df["vol_ratio"] = df["volume"] / df["vol_ma"].replace(0, np.nan)
        
        jaw, teeth, lips = alligator(df)
        df["gator_jaw"], df["gator_teeth"], df["gator_lips"] = jaw, teeth, lips
        
        macd, macd_signal, macd_hist = calculate_macd(df, self.cfg.macd_fast, 
                                                     self.cfg.macd_slow, self.cfg.macd_signal)
        df["macd"] = macd
        df["macd_signal"] = macd_signal
        df["macd_hist"] = macd_hist
        
        df = df.dropna().reset_index(drop=True)
        return df

    def determine_market_structure(self, df: pd.DataFrame) -> str:
        if df.empty or len(df) < self.cfg.ema_long:
            return "Unknown"
            
        latest = df.iloc[-1]
        
        if "ema_fast" not in df.columns or "ema_slow" not in df.columns or "ema_long" not in df.columns:
            return "Unknown"
        
        if latest['ema_fast'] > latest['ema_slow'] > latest['ema_long']:
            return "Strong Uptrend"
        elif latest['ema_fast'] < latest['ema_slow'] < latest['ema_long']:
            return "Strong Downtrend"
        elif latest['ema_fast'] > latest['ema_slow']:
            return "Uptrend"
        elif latest['ema_fast'] < latest['ema_slow']:
            return "Downtrend"
        else:
            return "Ranging"

    def score_signal(self, df: pd.DataFrame, side: str, strategy: str) -> int:
        if df.empty:
            return 0
            
        score = 0
        last = df.iloc[-1]
        
        # EMA stack alignment (3 points possible)
        if side == "LONG":
            if last["ema_fast"] > last["ema_slow"] > last["ema_long"]:
                score += 3  # Perfect stack
            elif last["ema_fast"] > last["ema_slow"]:
                score += 2  # Partial alignment
            elif last["close"] > last["vwap"]:
                score += 1  # At least above VWAP
        else:  # SHORT
            if last["ema_fast"] < last["ema_slow"] < last["ema_long"]:
                score += 3  # Perfect stack
            elif last["ema_fast"] < last["ema_slow"]:
                score += 2  # Partial alignment
            elif last["close"] < last["vwap"]:
                score += 1  # At least below VWAP
        
        # RSI positioning (1 point)
        if side == "LONG" and 35 < last["rsi"] < 65:
            score += 1
        elif side == "SHORT" and 35 < last["rsi"] < 65:
            score += 1
        
        # Volume confirmation (1 point)
        if last["vol_ratio"] > self.cfg.volume_spike_mult:
            score += 1
        
        # MACD confirmation (1 point)
        if side == "LONG" and last["macd_hist"] > 0:
            score += 1
        elif side == "SHORT" and last["macd_hist"] < 0:
            score += 1
        
        return min(score, 6)

    def check_cooldown(self, side: str, strategy: str) -> bool:
        key = f"{side}_{strategy}"
        now = time.time()
        cooldown_seconds = self.cfg.alert_cooldown_minutes * 60
        
        if key in self.last_alert_time:
            if now - self.last_alert_time[key] < cooldown_seconds:
                log.info(f"Cooldown active for {key}, skipping signal")
                return False
        
        self.last_alert_time[key] = now
        return True

    def _build_targets_long(self, entry_mid: float, sl: float) -> Tuple[float, float, float]:
        r = abs(entry_mid - sl)
        return (entry_mid + self.cfg.tp1_r_multiple * r,
                entry_mid + self.cfg.tp2_r_multiple * r,
                entry_mid + self.cfg.tp3_r_multiple * r)

    def _build_targets_short(self, entry_mid: float, sl: float) -> Tuple[float, float, float]:
        r = abs(sl - entry_mid)
        return (entry_mid - self.cfg.tp1_r_multiple * r,
                entry_mid - self.cfg.tp2_r_multiple * r,
                entry_mid - self.cfg.tp3_r_multiple * r)

    def _breakout_long(self, df: pd.DataFrame, cam_levels: Dict[str, float]) -> Optional[Dict[str, Any]]:
        last = df.iloc[-1]
        prev = df.iloc[-2]
        
        # Breakout conditions
        cond_break = (last["close"] > last["dc_high"]) and (prev["close"] <= prev["dc_high"])
        trend = (last["close"] > last["ema_slow"]) and (last["close"] > last["vwap"])
        vol_ok = last["vol_ratio"] > self.cfg.volume_spike_mult
        body_ok = last["body_ratio"] >= self.cfg.min_body_ratio
        above_gator = last["close"] > max(last["gator_jaw"], last["gator_teeth"], last["gator_lips"])
        
        if cond_break and trend and vol_ok and body_ok and above_gator:
            atrv = float(last["atr"])
            level = float(last["dc_high"])
            entry_low = level
            entry_high = level + self.cfg.entry_buffer_atr * atrv
            entry_mid = (entry_low + entry_high) / 2.0
            
            # Use Camarilla L3 or ATR-based stop
            if cam_levels and "L3" in cam_levels:
                sl = min(cam_levels["L3"] - 5, level - self.cfg.risk_atr_mult * atrv)
            else:
                sl = level - self.cfg.risk_atr_mult * atrv
                
            tp1, tp2, tp3 = self._build_targets_long(entry_mid, sl)
            
            # Adjust targets to Camarilla levels if close
            if cam_levels:
                if "H3" in cam_levels and abs(tp1 - cam_levels["H3"]) < 10:
                    tp1 = cam_levels["H3"]
                if "H4" in cam_levels and abs(tp2 - cam_levels["H4"]) < 10:
                    tp2 = cam_levels["H4"]
            
            why = "Breakout above Donchian High with strong body, trend above EMA50/VWAP, above gator, and volume spike."
            if cam_levels and "L3" in cam_levels:
                why += f" Support at L3 (${cam_levels['L3']:.0f})."
                
            path = (f"Enter on retest of {fmt_money(level)} or momentum continuation within "
                    f"{fmt_money(entry_low)}–{fmt_money(entry_high)}. SL {fmt_money(sl)}.")
            
            market_structure = self.determine_market_structure(df)
            score = self.score_signal(df, "LONG", "Breakout")
            
            return dict(side="LONG", strategy="Breakout", entry_low=entry_low, entry_high=entry_high,
                        entry_mid=entry_mid, sl=sl, tp1=tp1, tp2=tp2, tp3=tp3, why=why, path=path,
                        score=score, market_structure=market_structure)
        return None

    def _breakout_short(self, df: pd.DataFrame, cam_levels: Dict[str, float]) -> Optional[Dict[str, Any]]:
        last = df.iloc[-1]
        prev = df.iloc[-2]
        
        cond_break = (last["close"] < last["dc_low"]) and (prev["close"] >= prev["dc_low"])
        trend = (last["close"] < last["ema_slow"]) and (last["close"] < last["vwap"])
        vol_ok = last["vol_ratio"] > self.cfg.volume_spike_mult
        body_ok = last["body_ratio"] >= self.cfg.min_body_ratio
        below_gator = last["close"] < min(last["gator_jaw"], last["gator_teeth"], last["gator_lips"])
        
        if cond_break and trend and vol_ok and body_ok and below_gator:
            atrv = float(last["atr"])
            level = float(last["dc_low"])
            entry_high = level
            entry_low = level - self.cfg.entry_buffer_atr * atrv
            entry_mid = (entry_low + entry_high) / 2.0
            
            if cam_levels and "H3" in cam_levels:
                sl = max(cam_levels["H3"] + 5, level + self.cfg.risk_atr_mult * atrv)
            else:
                sl = level + self.cfg.risk_atr_mult * atrv
                
            tp1, tp2, tp3 = self._build_targets_short(entry_mid, sl)
            
            if cam_levels:
                if "L3" in cam_levels and abs(tp1 - cam_levels["L3"]) < 10:
                    tp1 = cam_levels["L3"]
                if "L4" in cam_levels and abs(tp2 - cam_levels["L4"]) < 10:
                    tp2 = cam_levels["L4"]
            
            why = "Breakdown below Donchian Low with strong body, trend below EMA50/VWAP, below gator, and volume spike."
            if cam_levels and "H3" in cam_levels:
                why += f" Resistance at H3 (${cam_levels['H3']:.0f})."
                
            path = (f"Enter on retest of {fmt_money(level)} or momentum continuation within "
                    f"{fmt_money(entry_low)}–{fmt_money(entry_high)}. SL {fmt_money(sl)}.")
            
            market_structure = self.determine_market_structure(df)
            score = self.score_signal(df, "SHORT", "Breakout")
            
            return dict(side="SHORT", strategy="Breakout", entry_low=entry_low, entry_high=entry_high,
                        entry_mid=entry_mid, sl=sl, tp1=tp1, tp2=tp2, tp3=tp3, why=why, path=path,
                        score=score, market_structure=market_structure)
        return None

    def _pullback_long(self, df: pd.DataFrame, cam_levels: Dict[str, float]) -> Optional[Dict[str, Any]]:
        last = df.iloc[-1]
        trend_up = (last["ema_fast"] > last["ema_slow"]) and (last["close"] > last["vwap"])
        near_ema = abs(last["low"] - last["ema_fast"]) <= 0.2 * last["atr"]
        rsi_ok = (last["rsi"] > 45) and (last["rsi"] > df.iloc[-2]["rsi"])
        
        near_l3 = False
        if cam_levels and "L3" in cam_levels:
            near_l3 = abs(last["close"] - cam_levels["L3"]) / last["close"] < 0.005
        
        if trend_up and (near_ema or near_l3) and rsi_ok:
            atrv = float(last["atr"])
            base = float(last["ema_fast"])
            
            if near_l3 and cam_levels:
                base = cam_levels["L3"]
                
            entry_low = base
            entry_high = base + 0.3 * atrv
            entry_mid = (entry_low + entry_high) / 2.0
            
            if cam_levels and "L4" in cam_levels:
                sl = min(cam_levels["L4"], float(last["ema_slow"]) - 0.5 * atrv)
            else:
                sl = float(last["ema_slow"]) - 0.5 * atrv
                
            tp1, tp2, tp3 = self._build_targets_long(entry_mid, sl)
            
            why = f"Uptrend (EMA20>EMA50), price above VWAP, pullback near "
            if near_l3 and cam_levels:
                why += f"L3 support (${cam_levels['L3']:.0f}) "
            else:
                why += f"EMA20 "
            why += "with RSI curling up."
            
            path = (f"Wait for a small dip near {fmt_money(base)}; enter within "
                    f"{fmt_money(entry_low)}–{fmt_money(entry_high)}. SL below EMA50 {fmt_money(sl)}.")
            
            market_structure = self.determine_market_structure(df)
            score = self.score_signal(df, "LONG", "Pullback")
            
            return dict(side="LONG", strategy="Pullback", entry_low=entry_low, entry_high=entry_high,
                        entry_mid=entry_mid, sl=sl, tp1=tp1, tp2=tp2, tp3=tp3, why=why, path=path,
                        score=score, market_structure=market_structure)
        return None

    def _pullback_short(self, df: pd.DataFrame, cam_levels: Dict[str, float]) -> Optional[Dict[str, Any]]:
        last = df.iloc[-1]
        trend_dn = (last["ema_fast"] < last["ema_slow"]) and (last["close"] < last["vwap"])
        near_ema = abs(last["high"] - last["ema_fast"]) <= 0.2 * last["atr"]
        rsi_ok = (last["rsi"] < 55) and (last["rsi"] < df.iloc[-2]["rsi"])
        
        near_h3 = False
        if cam_levels and "H3" in cam_levels:
            near_h3 = abs(last["close"] - cam_levels["H3"]) / last["close"] < 0.005
        
        if trend_dn and (near_ema or near_h3) and rsi_ok:
            atrv = float(last["atr"])
            base = float(last["ema_fast"])
            
            if near_h3 and cam_levels:
                base = cam_levels["H3"]
                
            entry_high = base
            entry_low = base - 0.3 * atrv
            entry_mid = (entry_low + entry_high) / 2.0
            
            if cam_levels and "H4" in cam_levels:
                sl = max(cam_levels["H4"], float(last["ema_slow"]) + 0.5 * atrv)
            else:
                sl = float(last["ema_slow"]) + 0.5 * atrv
                
            tp1, tp2, tp3 = self._build_targets_short(entry_mid, sl)
            
            why = f"Downtrend (EMA20<EMA50), price below VWAP, pullback toward "
            if near_h3 and cam_levels:
                why += f"H3 resistance (${cam_levels['H3']:.0f}) "
            else:
                why += f"EMA20 "
            why += "with RSI curling down."
            
            path = (f"Wait for a small pop near {fmt_money(base)}; enter within "
                    f"{fmt_money(entry_low)}–{fmt_money(entry_high)}. SL above EMA50 {fmt_money(sl)}.")
            
            market_structure = self.determine_market_structure(df)
            score = self.score_signal(df, "SHORT", "Pullback")
            
            return dict(side="SHORT", strategy="Pullback", entry_low=entry_low, entry_high=entry_high,
                        entry_mid=entry_mid, sl=sl, tp1=tp1, tp2=tp2, tp3=tp3, why=why, path=path,
                        score=score, market_structure=market_structure)
        return None

    def scan_candidates(self, df_raw: pd.DataFrame, cam_levels: Dict[str, float] = None) -> Dict[str, Optional[Dict[str, Any]]]:
        if df_raw is None or df_raw.empty or len(df_raw) < max(self.cfg.donchian_len, self.cfg.ema_slow, self.cfg.ema_long, self.cfg.rsi_len, self.cfg.atr_len) + 3:
            return {"breakout": None, "pullback": None}
        
        df = self.add_indicators(df_raw)
        if df.empty:
            return {"breakout": None, "pullback": None}
        
        if cam_levels is None:
            cam_levels = previous_day_levels(df_raw)
        
        return {
            "breakout": (self._breakout_long(df, cam_levels) or self._breakout_short(df, cam_levels)),
            "pullback": (self._pullback_long(df, cam_levels) or self._pullback_short(df, cam_levels)),
        }

    def pick_primary(self, ideas: Dict[str, Optional[Dict[str, Any]]]) -> Optional[Dict[str, Any]]:
        breakout = ideas.get("breakout")
        pullback = ideas.get("pullback")
        
        # Filter by minimum score
        if breakout and breakout.get("score", 0) < self.cfg.min_signal_score:
            breakout = None
        if pullback and pullback.get("score", 0) < self.cfg.min_signal_score:
            pullback = None
        
        # Check cooldowns
        if breakout and not self.check_cooldown(breakout["side"], breakout["strategy"]):
            breakout = None
        if pullback and not self.check_cooldown(pullback["side"], pullback["strategy"]):
            pullback = None
        
        # Prefer higher score
        if breakout and pullback:
            if breakout.get("score", 0) >= pullback.get("score", 0):
                return breakout
            return pullback
        
        return breakout or pullback

# ----- Trade Book -----
class TradeBook:
    def __init__(self):
        self.open: Dict[str, Trade] = {}
        self.closed: List[Trade] = []

    def add(self, t: Trade):
        self.open[t.id] = t

    def remove(self, trade_id: str) -> Optional[Trade]:
        trade = self.open.pop(trade_id, None)
        if trade:
            self.closed.append(trade)
        return trade

    def list_open(self) -> List[Trade]:
        return list(self.open.values())
    
    def get_stats(self, days: int = 7) -> PerformanceStats:
        stats = PerformanceStats()
        cutoff = time.time() - (days * 86400)
        
        for trade in self.closed:
            if trade.created_ts < cutoff:
                continue
                
            stats.total_trades += 1
            
            if trade.exit_reason == "SL":
                stats.losing_trades += 1
                stats.sl_hits += 1
                stats.total_r -= 1.0
            elif trade.exit_reason == "BE":
                stats.be_hits += 1
                # Breakeven is neutral R
            else:
                stats.winning_trades += 1
                if trade.exit_reason == "TP1" or trade.hit_tp1:
                    stats.tp1_hits += 1
                    stats.total_r += CFG.tp1_r_multiple * CFG.partial_fraction
                if trade.exit_reason == "TP2" or trade.hit_tp2:
                    stats.tp2_hits += 1
                    stats.total_r += CFG.tp2_r_multiple * (1 - CFG.partial_fraction)
                if trade.exit_reason == "TP3" or trade.hit_tp3:
                    stats.tp3_hits += 1
                    stats.total_r += CFG.tp3_r_multiple
        
        return stats

# ----- Trade Manager -----
class TradeManager:
    def __init__(self, sheets: SheetsClient, md: MarketDataProvider, book: TradeBook):
        self.sheets = sheets
        self.md = md
        self.book = book
        self.session: Optional[aiohttp.ClientSession] = None

    async def start(self):
        if not self.session:
            self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15))
        
        # Rehydrate open trades
        rows = await self.sheets.rehydrate_open(self.session)
        count = 0
        for r in rows:
            try:
                t = Trade(
                    id=r.get("id") or r.get("Trade ID") or str(uuid.uuid4()),
                    pair=r.get("pair") or r.get("Asset") or "ETH",
                    side=r.get("side") or r.get("Direction") or "LONG",
                    strategy=r.get("strategy") or r.get("Strategy") or "Unknown",
                    status=r.get("status") or r.get("Status") or "OPEN",
                    created_ts=float(r.get("timestamp") or r.get("Timestamp") or time.time()),
                    entry_low=float(r.get("entry_low") or r.get("Entry Low", 0)),
                    entry_high=float(r.get("entry_high") or r.get("Entry High", 0)),
                    entry_mid=float(r.get("entry_mid") or r.get("Entry Price", 0)),
                    stop_loss=float(r.get("stop_loss") or r.get("Stop Loss", 0)),
                    tp1=float(r.get("tp1") or r.get("Take Profit 1", 0)),
                    tp2=float(r.get("tp2") or r.get("Take Profit 2", 0)),
                    tp3=float(r.get("tp3") or r.get("Take Profit 3", 0)) if (r.get("tp3") or r.get("Take Profit 3")) else None,
                    reason=r.get("reason") or r.get("Why") or "",
                    path=r.get("path") or r.get("Path") or "",
                    score=int(r.get("score", 0)),
                    market_structure=r.get("market_structure", "Unknown"),
                    opened_price=float(r.get("opened_price") or r.get("Opened Price", 0)) if r.get("opened_price") or r.get("Opened Price") else None,
                    hit_tp1=bool(str(r.get("hit_tp1", "False")).lower() in ("true", "1", "yes")),
                    hit_tp2=bool(str(r.get("hit_tp2", "False")).lower() in ("true", "1", "yes")),
                    hit_tp3=bool(str(r.get("hit_tp3", "False")).lower() in ("true", "1", "yes")),
                    partial_done=bool(str(r.get("partial_done", "False")).lower() in ("true", "1", "yes")),
                    moved_to_be=bool(str(r.get("moved_to_be", "False")).lower() in ("true", "1", "yes")),
                    be_price=float(r.get("be_price", 0)) if r.get("be_price") else None,
                    r_multiple=float(r.get("r_multiple", 0.0)),
                )
                if t.status == "OPEN":
                    self.book.add(t)
                    count += 1
            except Exception as e:
                log.warning(f"Rehydrate row parse error: {e}")
                continue
        log.info(f"Rehydrated {count} open trades from Sheets.")

    async def stop(self):
        if self.session:
            await self.session.close()
            self.session = None

    async def open_trade(self, sig: Dict[str, Any]) -> Trade:
        current_price = await self.md.fetch_latest_price() or self.md.latest_price()
        
        t = Trade(
            id=uuid.uuid4().hex[:12],
            pair=CFG.binance_symbol if CFG.provider == "binance" else CFG.kraken_pair,
            side=sig["side"],
            strategy=sig["strategy"],
            status="OPEN",
            created_ts=time.time(),
            entry_low=float(sig["entry_low"]),
            entry_high=float(sig["entry_high"]),
            entry_mid=float(sig["entry_mid"]),
            stop_loss=float(sig["sl"]),
            tp1=float(sig["tp1"]),
            tp2=float(sig["tp2"]),
            tp3=float(sig.get("tp3")) if sig.get("tp3") is not None else None,
            reason=sig["why"],
            path=sig["path"],
            score=int(sig["score"]),
            market_structure=sig.get("market_structure", "Unknown"),
            opened_price=current_price,
            r_multiple=abs(sig["entry_mid"] - sig["sl"]),
        )
        
        self.book.add(t)

        payload = {
            "id": t.id, "pair": t.pair, "side": t.side, "strategy": t.strategy,
            "status": t.status, "timestamp": t.created_ts,
            "entry_low": t.entry_low, "entry_high": t.entry_high, "entry_mid": t.entry_mid,
            "stop_loss": t.stop_loss, "tp1": t.tp1, "tp2": t.tp2, "tp3": t.tp3,
            "reason": t.reason, "path": t.path, "opened_price": t.opened_price,
            "score": t.score, "market_structure": t.market_structure,
            "hit_tp1": t.hit_tp1, "hit_tp2": t.hit_tp2, "hit_tp3": t.hit_tp3,
            "partial_done": t.partial_done, "moved_to_be": t.moved_to_be,
            "be_price": t.be_price, "r_multiple": t.r_multiple,
        }
        
        await self.sheets.create_entry(self.session, payload)
        append_csv(ALERTS_CSV, payload)
        
        return t

    async def close_trade(self, trade_id: str, price: float, reason: str):
        t = self.book.open.get(trade_id)
        if not t:
            return
        
        t.status = "CLOSED"
        t.exit_price = float(price)
        t.exit_reason = reason
        
        payload = {
            "id": t.id, "status": t.status, "exit_reason": t.exit_reason, 
            "exit_price": t.exit_price, "hit_tp1": t.hit_tp1, "hit_tp2": t.hit_tp2,
            "hit_tp3": t.hit_tp3, "partial_done": t.partial_done, 
            "moved_to_be": t.moved_to_be, "be_price": t.be_price
        }
        
        await self.sheets.update_entry(self.session, payload)
        self.book.remove(trade_id)
        
        append_csv(FILLS_CSV, {
            "timestamp": ts_now().isoformat(), 
            "trade_id": t.id, 
            "fill": reason, 
            "price": price
        })

    async def monitor(self):
        if not self.book.open:
            return
        
        price = await self.md.fetch_latest_price() or self.md.latest_price()
        if price is None:
            return
        
        to_close = []
        
        for t in list(self.book.open.values()):
            if t.side == "LONG":
                # TP1 - partial close and move to BE
                if (not t.partial_done) and price >= t.tp1:
                    t.hit_tp1 = True
                    t.partial_done = True
                    t.be_price = t.entry_mid
                    t.moved_to_be = True
                    append_csv(FILLS_CSV, {
                        "timestamp": ts_now().isoformat(), 
                        "trade_id": t.id, 
                        "fill": "TP1_PARTIAL", 
                        "price": price
                    })
                    log.info(f"Trade {t.id[:8]} hit TP1 at ${price:.2f}, moved SL to BE")
                
                # TP2 - close remaining position or continue to TP3
                if price >= t.tp2:
                    if not t.hit_tp2:
                        t.hit_tp2 = True
                        if CFG.use_tp3_close and t.tp3:
                            append_csv(FILLS_CSV, {
                                "timestamp": ts_now().isoformat(), 
                                "trade_id": t.id, 
                                "fill": "TP2_HOLD", 
                                "price": price
                            })
                            log.info(f"Trade {t.id[:8]} hit TP2 at ${price:.2f}, holding for TP3")
                        else:
                            to_close.append((t.id, price, "TP2"))
                
                # TP3 - final close
                if CFG.use_tp3_close and t.tp3 and price >= t.tp3:
                    if not t.hit_tp3:
                        t.hit_tp3 = True
                        to_close.append((t.id, price, "TP3"))
                
                # Breakeven stop
                if t.moved_to_be and t.be_price and price <= t.be_price:
                    to_close.append((t.id, price, "BE"))
                
                # Original stop loss (if not moved to BE)
                if (not t.moved_to_be) and price <= t.stop_loss:
                    to_close.append((t.id, price, "SL"))
                    
            else:  # SHORT
                # TP1 - partial close and move to BE
                if (not t.partial_done) and price <= t.tp1:
                    t.hit_tp1 = True
                    t.partial_done = True
                    t.be_price = t.entry_mid
                    t.moved_to_be = True
                    append_csv(FILLS_CSV, {
                        "timestamp": ts_now().isoformat(), 
                        "trade_id": t.id, 
                        "fill": "TP1_PARTIAL", 
                        "price": price
                    })
                    log.info(f"Trade {t.id[:8]} hit TP1 at ${price:.2f}, moved SL to BE")
                
                # TP2 - close remaining position or continue to TP3
                if price <= t.tp2:
                    if not t.hit_tp2:
                        t.hit_tp2 = True
                        if CFG.use_tp3_close and t.tp3:
                            append_csv(FILLS_CSV, {
                                "timestamp": ts_now().isoformat(), 
                                "trade_id": t.id, 
                                "fill": "TP2_HOLD", 
                                "price": price
                            })
                            log.info(f"Trade {t.id[:8]} hit TP2 at ${price:.2f}, holding for TP3")
                        else:
                            to_close.append((t.id, price, "TP2"))
                
                # TP3 - final close
                if CFG.use_tp3_close and t.tp3 and price <= t.tp3:
                    if not t.hit_tp3:
                        t.hit_tp3 = True
                        to_close.append((t.id, price, "TP3"))
                
                # Breakeven stop
                if t.moved_to_be and t.be_price and price >= t.be_price:
                    to_close.append((t.id, price, "BE"))
                
                # Original stop loss (if not moved to BE)
                if (not t.moved_to_be) and price >= t.stop_loss:
                    to_close.append((t.id, price, "SL"))
        
        # Execute closures
        for trade_id, price, reason in to_close:
            await self.close_trade(trade_id, price, reason)

# ----- Discord Bot -----
class MergedETHBot(discord.Client):
    def __init__(self, cfg: Config):
        intents = discord.Intents.default()
        intents.message_content = True
        super().__init__(intents=intents)
        self.cfg = cfg
        self.md = MarketDataProvider(cfg)
        self.sheets = SheetsClient(cfg.sheets_webhook, cfg.sheets_secret)
        self.engine = EnhancedSignalEngine(cfg)
        self.book = TradeBook()
        self.tm = TradeManager(self.sheets, self.md, self.book)
        self.channel: Optional[discord.TextChannel] = None

    async def setup_hook(self) -> None:
        self.scanner_loop.change_interval(seconds=self.cfg.scan_every_s)
        self.monitor_loop.change_interval(seconds=10)

    async def on_ready(self):
        log.info(f"Logged in as {self.user} (ID: {self.user.id})")
        
        # Find target channel
        if self.cfg.channel_id:
            chan = self.get_channel(self.cfg.channel_id)
            if isinstance(chan, discord.TextChannel):
                self.channel = chan
                log.info(f"Posting alerts to: #{chan.name} ({chan.id})")
        
        # Fallback to first available text channel
        if not self.channel:
            for guild in self.guilds:
                for ch in guild.text_channels:
                    if ch.permissions_for(guild.me).send_messages:
                        self.channel = ch
                        log.info(f"Using fallback channel: #{ch.name} ({ch.id})")
                        break
                if self.channel:
                    break

        # Start components
        await self.md.start()
        await self.tm.start()
        self.scanner_loop.start()
        self.monitor_loop.start()

    async def close(self):
        try:
            if hasattr(self, 'md'):
                await self.md.stop()
            if hasattr(self, 'tm'):
                await self.tm.stop()
        finally:
            await super().close()

    @tasks.loop(seconds=60)
    async def scanner_loop(self):
        # Minute alignment for precision
        await asyncio.sleep(sleep_time_to_next_bar(self.cfg.scan_every_s))
        
        try:
            df = self.md.get_df()
            if df is None or df.empty:
                return
            
            # Get Camarilla levels
            cam_levels = previous_day_levels(df)
            
            # Scan for signals
            ideas = self.engine.scan_candidates(df, cam_levels)
            idea = self.engine.pick_primary(ideas)
            
            latest_close = float(df.iloc[-1]["close"])
            
            # Log decision
            append_csv(DECISIONS_CSV, {
                "timestamp": ts_now().isoformat(),
                "pair": self.cfg.binance_symbol if self.cfg.provider == "binance" else self.cfg.kraken_pair,
                "price": latest_close,
                "breakout_score": ideas.get("breakout", {}).get("score", 0) if ideas.get("breakout") else 0,
                "pullback_score": ideas.get("pullback", {}).get("score", 0) if ideas.get("pullback") else 0,
                "note": "scan_complete",
            })
            
            if idea:
                trade = await self.tm.open_trade(idea)
                
                # Send both reference-style text and embed
                await self.post_reference_style(df, ideas.get("breakout"), ideas.get("pullback"), cam_levels)
                await self.post_embed(trade)
                
                log.info(f"Alert sent: {trade.side} {trade.strategy} at ${trade.opened_price:.2f} [Score: {trade.score}/6]")
                
        except Exception as e:
            log.error(f"Scanner loop error: {e}")

    @tasks.loop(seconds=10)
    async def monitor_loop(self):
        try:
            await self.tm.monitor()
        except Exception as e:
            log.error(f"Monitor loop error: {e}")

    def _build_context(self, df: pd.DataFrame, cam_levels: Dict[str, float]) -> str:
        df = self.engine.add_indicators(df)
        last = df.iloc[-1]
        bits = []
        
        # Gator status
        if last["close"] > last["gator_lips"] > last["gator_teeth"] > last["gator_jaw"]:
            bits.append("back above the gator")
        elif last["close"] < last["gator_lips"] < last["gator_teeth"] < last["gator_jaw"]:
            bits.append("below the gator")
        
        # EMA status
        if last["close"] >= last["ema_fast"]:
            bits.append(f"finding support above the {self.cfg.ema_fast} EMA")
        else:
            bits.append(f"under the {self.cfg.ema_fast} EMA")
        
        # Camarilla levels
        if cam_levels:
            if "L3" in cam_levels and last["close"] >= cam_levels["L3"]:
                bits.append(f"L3 Long (${cam_levels['L3']:.0f})")
            elif "L3" in cam_levels:
                bits.append(f"below L3 (${cam_levels['L3']:.0f})")
            
            if "P" in cam_levels and last["close"] >= cam_levels["P"]:
                bits.append(f"and Pivot (${cam_levels['P']:.0f})")
            elif "P" in cam_levels:
                bits.append(f"below Pivot (${cam_levels['P']:.0f})")
        
        s = f"ETH is " + ", ".join(bits) + "."
        
        # Market structure
        structure = self.engine.determine_market_structure(df)
        s += f" Currently in {structure.lower()}."
        
        if cam_levels and "L3" in cam_levels and last["close"] >= cam_levels["L3"]:
            s += " If support holds we should look for more upside."
        elif cam_levels and "H3" in cam_levels and last["close"] <= cam_levels["H3"]:
            s += " If resistance holds we could see further downside."
        
        return s

    def _format_reference_text(self, df: pd.DataFrame, breakout: Optional[Dict[str,Any]], 
                              pullback: Optional[Dict[str,Any]], cam_levels: Dict[str, float]) -> str:
        ctx = self._build_context(df, cam_levels)
        lines = [ctx, ""]
        
        if breakout:
            score_str = f" [Score: {breakout.get('score', 0)}/6]" if breakout.get('score') else ""
            if breakout["side"] == "LONG":
                lines.append(f"🟢 Breakout entry {fmt_money(breakout['entry_low'])}-{fmt_money(breakout['entry_high'])} zone{score_str}")
            else:
                lines.append(f"🔴 Breakdown entry {fmt_money(breakout['entry_low'])}-{fmt_money(breakout['entry_high'])} zone{score_str}")
            lines.append(f"Stop Loss: {fmt_money(breakout['sl'])}")
            lines.append(f"TP 1: {fmt_money(breakout['tp1'])}")
            lines.append(f"TP 2: {fmt_money(breakout['tp2'])}")
            if breakout.get("tp3") is not None:
                lines.append(f"TP 3: {fmt_money(breakout['tp3'])}")
            lines.append("")

        if pullback:
            score_str = f" [Score: {pullback.get('score', 0)}/6]" if pullback.get('score') else ""
            if pullback["side"] == "LONG":
                label = "🟢 Pullback entry"
            else:
                label = "🔴 Pop-to-sell entry"
            lines.append(f"{label}: {fmt_money(pullback['entry_low'])}-{fmt_money(pullback['entry_high'])} zone{score_str}")
            lines.append(f"Stop Loss: {fmt_money(pullback['sl'])}")
            lines.append(f"Take Profit: {fmt_money(pullback['tp1'])}")
        
        return "\n".join(lines)

    async def post_reference_style(self, df: pd.DataFrame, breakout: Optional[Dict[str,Any]], 
                                  pullback: Optional[Dict[str,Any]], cam_levels: Dict[str, float]):
        if not self.channel:
            return
        text = self._format_reference_text(df, breakout, pullback, cam_levels)
        await self.channel.send(text)

    async def post_embed(self, t: Trade):
        if not self.channel:
            log.warning("No channel configured; skipping Discord post.")
            return
        
        # Color based on direction and score
        if t.score >= 5:
            color = 0x00ff00 if t.side == "LONG" else 0xff0000
        else:
            color = 0x2ecc71 if t.side == "LONG" else 0xe74c3c
        
        # Title with score indicator
        score_emoji = "⭐" if t.score >= 5 else "🎯"
        title = f"{score_emoji} ETH {t.side} Alert – {t.strategy} Setup [{t.score}/6]"
        
        embed = discord.Embed(title=title, color=color, timestamp=datetime.now(timezone.utc))
        
        # Add market structure to description
        embed.description = f"**Market Structure:** {t.market_structure}"
        
        embed.add_field(name="🎯 Entry Range", 
                       value=f"{fmt_money(t.entry_low)} – {fmt_money(t.entry_high)} (mid {fmt_money(t.entry_mid)})", 
                       inline=False)
        embed.add_field(name="🛑 Stop Loss", value=fmt_money(t.stop_loss), inline=True)
        embed.add_field(name="🎯 TP1", value=fmt_money(t.tp1), inline=True)
        embed.add_field(name="🎯 TP2", value=fmt_money(t.tp2), inline=True)
        if t.tp3 is not None:
            embed.add_field(name="🎯 TP3", value=fmt_money(t.tp3), inline=True)
        
        # Risk/Reward
        risk = abs(t.entry_mid - t.stop_loss)
        reward1 = abs(t.tp1 - t.entry_mid)
        rr = reward1 / risk if risk > 0 else 0
        embed.add_field(name="📊 Risk:Reward", value=f"1:{rr:.1f}", inline=True)
        
        embed.add_field(name="🛤️ Path to Enter", value=t.path, inline=False)
        embed.add_field(name="💡 Why this trade", value=t.reason, inline=False)
        embed.set_footer(text=f"ID: {t.id[:8]} | {now_footer()}")
        
        await self.channel.send(embed=embed)

    async def post_close_notification(self, trade: Trade, price: float, reason: str):
        if not self.channel:
            return
            
        color = 0x3498db if reason != "SL" else 0x95a5a6
        emoji = "✅" if reason not in ["SL", "BE"] else "⚠️" if reason == "BE" else "❌"
        
        embed = discord.Embed(
            title=f"{emoji} ETH Trade Closed – {reason}",
            description=f"Trade {trade.id[:8]} ({trade.strategy} {trade.side}) closed at {fmt_money(price)}",
            color=color,
            timestamp=datetime.now(timezone.utc),
        )
        
        # Calculate R-multiple for this trade
        if reason == "SL":
            r_result = -1.0
        elif reason == "BE":
            r_result = 0.0
        elif reason == "TP1":
            r_result = self.cfg.tp1_r_multiple * self.cfg.partial_fraction
        elif reason == "TP2":
            r_result = self.cfg.tp2_r_multiple * (1 - self.cfg.partial_fraction)
        elif reason == "TP3":
            r_result = self.cfg.tp3_r_multiple
        else:
            r_result = 0.0
        
        embed.add_field(name="R-Multiple", value=f"{r_result:+.1f}R", inline=True)
        embed.add_field(name="Score", value=f"{trade.score}/6", inline=True)
        
        # Show partial management details
        if trade.partial_done:
            partial_text = f"TP1 hit at {fmt_money(trade.tp1)}, moved SL to BE"
            embed.add_field(name="Management", value=partial_text, inline=False)
        
        embed.set_footer(text=now_footer())
        await self.channel.send(embed=embed)

    # Discord Commands
    async def on_message(self, message: discord.Message):
        if message.author.bot:
            return
        content = message.content.strip().lower()
        
        if content == "!ping":
            await message.reply("pong")
            
        elif content == "!open":
            if not self.book.open:
                await message.reply("No open trades.")
                return
            lines = ["**Open Trades:**"]
            for t in self.book.list_open():
                tp1_mark = "✅" if t.hit_tp1 else "⏳"
                tp2_mark = "✅" if t.hit_tp2 else "⏳"
                tp3_mark = "✅" if t.hit_tp3 else "⏳" if t.tp3 else "—"
                be_mark = "🔒" if t.moved_to_be else ""
                lines.append(f"• `{t.id[:8]}` {t.strategy} {t.side} [{t.score}/6] "
                           f"{fmt_money(t.entry_low)}–{fmt_money(t.entry_high)} "
                           f"SL {fmt_money(t.stop_loss)} {be_mark} | TP1 {tp1_mark} TP2 {tp2_mark} TP3 {tp3_mark}")
            await message.reply("\n".join(lines))
            
        elif content == "!config":
            info = (
                f"**Configuration:**\n"
                f"```"
                f"Provider: {self.cfg.provider}\n"
                f"WebSocket: {self.cfg.use_websocket}\n"
                f"Symbol: {self.cfg.binance_symbol if self.cfg.provider == 'binance' else self.cfg.kraken_pair}\n"
                f"Scan cadence: {self.cfg.scan_every_s}s\n"
                f"Alert cooldown: {self.cfg.alert_cooldown_minutes}m\n"
                f"Min signal score: {self.cfg.min_signal_score}/6\n"
                f"Partial fraction: {self.cfg.partial_fraction}\n"
                f"Risk ATR: {self.cfg.risk_atr_mult}\n"
                f"TP1/TP2/TP3 R: {self.cfg.tp1_r_multiple}/{self.cfg.tp2_r_multiple}/{self.cfg.tp3_r_multiple}\n"
                f"Use TP3 close: {self.cfg.use_tp3_close}\n"
                f"Donchian: {self.cfg.donchian_len}\n"
                f"EMA: {self.cfg.ema_fast}/{self.cfg.ema_slow}/{self.cfg.ema_long}\n"
                f"MACD: {self.cfg.macd_fast}/{self.cfg.macd_slow}/{self.cfg.macd_signal}\n"
                f"```"
            )
            await message.reply(info)
            
        elif content.startswith("!stats"):
            parts = content.split()
            days = 7
            if len(parts) > 1:
                try:
                    days = int(parts[1])
                except ValueError:
                    pass
            
            stats = self.book.get_stats(days)
            
            embed = discord.Embed(
                title=f"📊 Performance Stats - Last {days} Days",
                color=discord.Color.gold(),
                timestamp=datetime.now(timezone.utc)
            )
            
            embed.add_field(name="Total Trades", value=str(stats.total_trades), inline=True)
            embed.add_field(name="Win Rate", value=f"{stats.win_rate:.1f}%", inline=True)
            embed.add_field(name="Avg R-Multiple", value=f"{stats.avg_r:.2f}R", inline=True)
            embed.add_field(name="Winners", value=str(stats.winning_trades), inline=True)
            embed.add_field(name="Losers", value=str(stats.losing_trades), inline=True)
            embed.add_field(name="Total R", value=f"{stats.total_r:+.1f}R", inline=True)
            embed.add_field(name="TP1 Hits", value=str(stats.tp1_hits), inline=True)
            embed.add_field(name="TP2 Hits", value=str(stats.tp2_hits), inline=True)
            embed.add_field(name="TP3 Hits", value=str(stats.tp3_hits), inline=True)
            embed.add_field(name="SL Hits", value=str(stats.sl_hits), inline=True)
            embed.add_field(name="BE Hits", value=str(stats.be_hits), inline=True)
            embed.add_field(name="Partial Mgmt", value=f"{self.cfg.partial_fraction*100:.0f}% at TP1", inline=True)
            
            embed.set_footer(text=now_footer())
            await message.reply(embed=embed)
            
        elif content == "!levels":
            try:
                df = self.md.get_df()
                if df is None or df.empty:
                    await message.reply("No data available.")
                    return
                
                cam_levels = previous_day_levels(df)
                if not cam_levels:
                    await message.reply("Unable to calculate Camarilla levels.")
                    return
                
                current_price = float(df.iloc[-1]["close"])
                
                embed = discord.Embed(
                    title="📈 ETH Camarilla Levels",
                    color=discord.Color.blue(),
                    timestamp=datetime.now(timezone.utc)
                )
                
                embed.add_field(name="Current Price", value=fmt_money(current_price), inline=False)
                
                # Resistance levels
                resistance_text = ""
                for level in ['H4', 'H3', 'H2', 'H1']:
                    if level in cam_levels:
                        mark = "→" if abs(current_price - cam_levels[level]) < 10 else ""
                        resistance_text += f"**{level}:** {fmt_money(cam_levels[level])} {mark}\n"
                embed.add_field(name="Resistance", value=resistance_text, inline=True)
                
                # Support levels
                support_text = ""
                for level in ['L1', 'L2', 'L3', 'L4']:
                    if level in cam_levels:
                        mark = "→" if abs(current_price - cam_levels[level]) < 10 else ""
                        support_text += f"**{level}:** {fmt_money(cam_levels[level])} {mark}\n"
                embed.add_field(name="Support", value=support_text, inline=True)
                
                # Pivot
                embed.add_field(name="Pivot", value=fmt_money(cam_levels['P']), inline=False)
                
                # Market structure
                df_with_indicators = self.engine.add_indicators(df)
                structure = self.engine.determine_market_structure(df_with_indicators)
                embed.add_field(name="Market Structure", value=structure, inline=False)
                
                embed.set_footer(text=now_footer())
                await message.reply(embed=embed)
                
            except Exception as e:
                await message.reply(f"Error retrieving levels: {e}")
                
        elif content == "!force":
            try:
                df = self.md.get_df()
                if df is None or df.empty:
                    await message.reply("No data available.")
                    return
                
                cam_levels = previous_day_levels(df)
                ideas = self.engine.scan_candidates(df, cam_levels)
                
                # Show all candidates with scores
                response_lines = ["**Signal Scan Results:**"]
                
                if ideas.get("breakout"):
                    b = ideas["breakout"]
                    response_lines.append(f"🔹 **Breakout {b['side']}** - Score: {b.get('score', 0)}/6")
                    response_lines.append(f"   Entry: {fmt_money(b['entry_mid'])} | SL: {fmt_money(b['sl'])}")
                else:
                    response_lines.append("🔹 **Breakout:** No setup")
                
                if ideas.get("pullback"):
                    p = ideas["pullback"]
                    response_lines.append(f"🔸 **Pullback {p['side']}** - Score: {p.get('score', 0)}/6")
                    response_lines.append(f"   Entry: {fmt_money(p['entry_mid'])} | SL: {fmt_money(p['sl'])}")
                else:
                    response_lines.append("🔸 **Pullback:** No setup")
                
                # Check what would be picked
                idea = self.engine.pick_primary(ideas)
                if idea:
                    response_lines.append(f"\n✅ **Would alert:** {idea['strategy']} {idea['side']} (Score: {idea.get('score', 0)}/6)")
                    
                    if idea.get('score', 0) < self.cfg.min_signal_score:
                        response_lines.append(f"   ⚠️ Score below minimum ({self.cfg.min_signal_score})")
                else:
                    response_lines.append("\n❌ **No viable setup** (score/cooldown filtered)")
                
                await message.reply("\n".join(response_lines))
                
                # If there's a good setup, show the full alert
                if idea and idea.get('score', 0) >= self.cfg.min_signal_score:
                    await self.post_reference_style(df, ideas.get("breakout"), ideas.get("pullback"), cam_levels)
                    
            except Exception as e:
                await message.reply(f"Force scan error: {e}")
                
        elif content == "!help":
            embed = discord.Embed(
                title="📚 Merged ETH Bot Commands",
                description="Advanced trading bot with multi-provider WebSocket feeds",
                color=discord.Color.blue()
            )
            
            commands_info = {
                "!ping": "Check if bot is responsive",
                "!open": "Show all open trades with detailed status",
                "!config": "Display bot configuration",
                "!stats [days]": "Show performance statistics (default 7 days)",
                "!levels": "Show current Camarilla levels and market structure",
                "!force": "Force scan and show all candidates with scores",
                "!help": "Show this help message"
            }
            
            for cmd, desc in commands_info.items():
                embed.add_field(name=cmd, value=desc, inline=False)
            
            embed.add_field(
                name="Signal Quality",
                value=f"Signals scored 0-6. Minimum score for alerts: {self.cfg.min_signal_score}/6\n"
                      f"⭐ = High quality (5-6), 🎯 = Normal quality (4)\n"
                      f"Partial close: {self.cfg.partial_fraction*100:.0f}% at TP1, move SL to BE",
                inline=False
            )
            
            embed.add_field(
                name="Data Sources",
                value=f"Provider: {self.cfg.provider.title()}\n"
                      f"WebSocket: {'Enabled' if self.cfg.use_websocket else 'Disabled'}\n"
                      f"Symbol: {self.cfg.binance_symbol if self.cfg.provider == 'binance' else self.cfg.kraken_pair}",
                inline=False
            )
            
            embed.set_footer(text=f"Scanning every {self.cfg.scan_every_s}s | Cooldown: {self.cfg.alert_cooldown_minutes}m")
            
            await message.reply(embed=embed)

# ----- Health Server -----
app = Flask(__name__)

@app.route("/")
def health_check():
    return {"status": "OK", "timestamp": datetime.now(timezone.utc).isoformat()}, 200

@app.route("/health")
def health_detailed():
    return {
        "status": "OK", 
        "provider": CFG.provider,
        "websocket": CFG.use_websocket,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }, 200

def run_health_server():
    port = int(os.getenv("PORT", "8000"))
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)

# ----- Signal Handler -----
def signal_handler(signum, frame):
    log.info(f"Received signal {signum}, shutting down gracefully...")
    # The bot's close() method will be called by asyncio

# ----- Main Entry Point -----
async def main():
    # Validate required environment variables
    missing = []
    if not CFG.discord_token:
        missing.append("DISCORD_TOKEN")
    
    if missing:
        raise SystemExit(f"Missing required env vars: {', '.join(missing)}")
    
    # Start health server in background
    health_thread = Thread(target=run_health_server, daemon=True)
    health_thread.start()
    
    # Setup signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    log.info("=" * 60)
    log.info("Advanced Merged ETH Trading Bot Starting")
    log.info(f"Provider: {CFG.provider} (WebSocket: {CFG.use_websocket})")
    log.info(f"Symbol: {CFG.binance_symbol if CFG.provider == 'binance' else CFG.kraken_pair}")
    log.info(f"Scan every: {CFG.scan_every_s}s")
    log.info(f"Alert cooldown: {CFG.alert_cooldown_minutes}m")
    log.info(f"Min signal score: {CFG.min_signal_score}/6")
    log.info(f"Partial management: {CFG.partial_fraction*100:.0f}% at TP1")
    log.info(f"R-multiples: TP1={CFG.tp1_r_multiple}, TP2={CFG.tp2_r_multiple}, TP3={CFG.tp3_r_multiple}")
    log.info(f"Health server: http://0.0.0.0:{os.getenv('PORT', '8000')}")
    log.info("=" * 60)
    
    # Create and run bot
    bot = MergedETHBot(CFG)
    
    try:
        await bot.start(CFG.discord_token)
    except KeyboardInterrupt:
        log.info("Keyboard interrupt received")
    except Exception as e:
        log.error(f"Bot error: {e}")
    finally:
        log.info("Shutting down...")
        await bot.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass