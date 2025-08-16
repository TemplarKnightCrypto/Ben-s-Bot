# ===============================================
# Scout Tower - Enhanced ETH Alert Bot (v3.0)
# Feature-rich alert system with auto-close functionality
# -----------------------------------------------
# NEW FEATURES:
# - Automatic alert closure on SL/TP hits
# - Performance tracking and statistics
# - Manual result logging and alert management
# - Market context in alerts
# - Alert expiration system
# - Session management improvements
# - Rate limiting protection
# - Enhanced signal validation
# ===============================================

import os, sys, json, math, csv, asyncio, aiohttp, logging, time, pathlib
from dataclasses import dataclass, field, asdict
from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime, timezone, timedelta
import pandas as pd
import numpy as np

import discord
from discord.ext import tasks, commands

from flask import Flask, jsonify
import threading

# ---------------- Logging ----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("ScoutTower")

# ---------------- Config -----------------
def getenv_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)).strip())
    except Exception:
        return default

def getenv_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)).strip())
    except Exception:
        return default

@dataclass
class Config:
    token: str = field(default_factory=lambda: os.getenv("DISCORD_TOKEN","").strip())
    sheets_webhook: str = field(default_factory=lambda: os.getenv("GOOGLE_SHEETS_WEBHOOK","").strip())
    sheets_token: Optional[str] = field(default_factory=lambda: os.getenv("SHEETS_TOKEN","").strip() or None)

    # Channels (pre-wired defaults; override via ENV)
    signals_channel_id: int = field(default_factory=lambda: getenv_int("SIGNALS_CHANNEL_ID", 1406315590569693346))
    status_channel_id:  int = field(default_factory=lambda: getenv_int("STATUS_CHANNEL_ID",  1406315723071946887))
    errors_channel_id:  int = field(default_factory=lambda: getenv_int("ERRORS_CHANNEL_ID",  1406315841330348265))
    startup_channel_id: int = field(default_factory=lambda: getenv_int("STARTUP_CHANNEL_ID", 1406315936989970485))

    provider: str = field(default_factory=lambda: os.getenv("PROVIDER","binance").strip().lower())
    use_websocket: bool = field(default_factory=lambda: os.getenv("USE_WEBSOCKET","0").strip() == "1")
    symbol_binance: str = field(default_factory=lambda: os.getenv("SYMBOL_BINANCE","ETHUSDT").strip())
    symbol_kraken:  str = field(default_factory=lambda: os.getenv("SYMBOL_KRAKEN","ETHUSDT").strip())

    scan_every_seconds: int = field(default_factory=lambda: getenv_int("SCAN_EVERY_SECONDS", 60))
    alert_cooldown_minutes: int = field(default_factory=lambda: getenv_int("ALERT_COOLDOWN_MINUTES", 15))
    min_signal_score: float = field(default_factory=lambda: getenv_float("MIN_SIGNAL_SCORE", 3.0))

    risk_atr_mult: float = field(default_factory=lambda: getenv_float("RISK_ATR_MULT", 1.0))
    tp1_r_multiple: float = field(default_factory=lambda: getenv_float("TP1_R_MULTIPLE", 1.5))
    tp2_r_multiple: float = field(default_factory=lambda: getenv_float("TP2_R_MULTIPLE", 3.0))
    
    # New configs
    alert_expire_hours: int = field(default_factory=lambda: getenv_int("ALERT_EXPIRE_HOURS", 24))
    position_risk_pct: float = field(default_factory=lambda: getenv_float("POSITION_RISK_PCT", 2.0))

    tz_name: str = field(default_factory=lambda: os.getenv("TZ_NAME", "America/Chicago").strip())
    port: int = field(default_factory=lambda: getenv_int("PORT", 10000))

    def validate(self):
        if not self.token:
            raise RuntimeError("DISCORD_TOKEN is required")
        if not self.sheets_webhook:
            raise RuntimeError("GOOGLE_SHEETS_WEBHOOK is required")

CFG = Config()
CFG.validate()

# ---------------- Global State ----------------
scanner_paused = False  # For mute/unmute functionality
_last_price: Optional[float] = None

# ---------------- Session Manager ----------------
class SessionManager:
    """Singleton session manager for all HTTP requests"""
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.session = None
        return cls._instance
    
    async def get_session(self) -> aiohttp.ClientSession:
        if not self.session or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=20)
            self.session = aiohttp.ClientSession(timeout=timeout)
        return self.session
    
    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()
            self.session = None

session_mgr = SessionManager()

# ---------------- Rate Limiter ----------------
class RateLimiter:
    """Rate limiting for API calls"""
    def __init__(self, max_calls: int = 10, period: float = 60):
        self.max_calls = max_calls
        self.period = period
        self.calls = []
    
    async def acquire(self):
        now = time.time()
        self.calls = [c for c in self.calls if c > now - self.period]
        if len(self.calls) >= self.max_calls:
            sleep_time = self.period - (now - self.calls[0])
            if sleep_time > 0:
                log.info(f"Rate limit reached, sleeping {sleep_time:.1f}s")
                await asyncio.sleep(sleep_time)
        self.calls.append(time.time())

binance_limiter = RateLimiter(max_calls=20, period=60)
kraken_limiter = RateLimiter(max_calls=15, period=60)

# ---------------- Utility Functions ----------------
def fmt_dt(dt: datetime, tzname: str) -> str:
    """Format datetime for display"""
    try:
        import pytz
        tz = pytz.timezone(tzname)
        return dt.astimezone(tz).strftime("%Y-%m-%d %H:%M:%S %Z")
    except Exception:
        return dt.strftime("%Y-%m-%d %H:%M:%S")

def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def validate_ohlc_data(df: pd.DataFrame) -> bool:
    """Validate OHLC data integrity"""
    if df is None or df.empty or len(df) < 50:
        return False
    
    # Check for missing values
    if df[["open", "high", "low", "close", "volume"]].isnull().any().any():
        return False
    
    # Check OHLC relationship
    invalid_candles = (
        (df["high"] < df["low"]) | 
        (df["high"] < df["open"]) | 
        (df["high"] < df["close"]) |
        (df["low"] > df["open"]) | 
        (df["low"] > df["close"])
    )
    
    if invalid_candles.any():
        log.warning(f"Invalid OHLC data detected: {invalid_candles.sum()} bad candles")
        return False
    
    return True

# ---------------- Data Provider (REST) -------------------
class MarketDataProvider:
    def __init__(self, cfg: Config):
        self.cfg = cfg

    async def fetch_binance_klines(self, symbol: str, limit: int = 200) -> Optional[pd.DataFrame]:
        await binance_limiter.acquire()
        session = await session_mgr.get_session()
        
        url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval=1m&limit={limit}"
        try:
            async with session.get(url) as r:
                if r.status != 200:
                    text = await r.text()
                    log.warning(f"Binance klines error {r.status}: {text}")
                    return None
                raw = await r.json()
        except Exception as e:
            log.error(f"Binance fetch error: {e}")
            return None
            
        if not raw:
            return None
            
        cols = ["open_time","open","high","low","close","volume","close_time","qv","ntrades","tb_base","tb_quote","ignore"]
        df = pd.DataFrame(raw, columns=cols)
        df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
        for c in ["open","high","low","close","volume"]:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        df.rename(columns={"open_time":"time"}, inplace=True)
        df = df[["time","open","high","low","close","volume"]]
        return df

    async def fetch_kraken_ohlc(self, pair: str, since: Optional[int] = None) -> Optional[pd.DataFrame]:
        await kraken_limiter.acquire()
        session = await session_mgr.get_session()
        
        url = f"https://api.kraken.com/0/public/OHLC?pair={pair}&interval=1"
        try:
            async with session.get(url) as r:
                if r.status != 200:
                    text = await r.text()
                    log.warning(f"Kraken OHLC error {r.status}: {text}")
                    return None
                raw = await r.json()
        except Exception as e:
            log.error(f"Kraken fetch error: {e}")
            return None
            
        if raw.get("error"):
            log.warning(f"Kraken error: {raw['error']}")
            return None
        result = raw.get("result", {})
        if not result:
            return None
        _, data = next(iter(result.items()))
        df = pd.DataFrame(data, columns=["time","open","high","low","close","vwap","volume","count"])
        df["time"] = pd.to_datetime(df["time"], unit="s", utc=True)
        for c in ["open","high","low","close","volume"]:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        df = df[["time","open","high","low","close","volume"]]
        return df

    async def fetch_ohlc(self, limit: int = 200) -> Optional[pd.DataFrame]:
        try:
            if self.cfg.provider == "binance":
                df = await self.fetch_binance_klines(self.cfg.symbol_binance, limit=limit)
                if df is not None and validate_ohlc_data(df):
                    return df
                # fallback
                log.info("Binance data invalid, trying Kraken fallback")
                df2 = await self.fetch_kraken_ohlc(self.cfg.symbol_kraken)
                if validate_ohlc_data(df2):
                    return df2
            else:
                df = await self.fetch_kraken_ohlc(self.cfg.symbol_kraken)
                if df is not None and validate_ohlc_data(df):
                    return df
                log.info("Kraken data invalid, trying Binance fallback")
                df2 = await self.fetch_binance_klines(self.cfg.symbol_binance, limit=limit)
                if validate_ohlc_data(df2):
                    return df2
        except Exception as e:
            log.error(f"fetch_ohlc error: {e}")
        return None

# -------------- Indicators -----------------
def ema(series: pd.Series, length: int) -> pd.Series:
    return series.ewm(span=length, adjust=False).mean()

def rsi(series: pd.Series, length: int = 14) -> pd.Series:
    delta = series.diff()
    up = delta.clip(lower=0)
    down = -1*delta.clip(upper=0)
    ma_up = up.ewm(alpha=1/length, adjust=False).mean()
    ma_down = down.ewm(alpha=1/length, adjust=False).mean()
    rs = ma_up / (ma_down + 1e-12)
    return 100 - (100/(1+rs))

def atr(df: pd.DataFrame, length: int = 14) -> pd.Series:
    hl = df["high"] - df["low"]
    hc = (df["high"] - df["close"].shift()).abs()
    lc = (df["low"] - df["close"].shift()).abs()
    tr = pd.concat([hl, hc, lc], axis=1).max(axis=1)
    return tr.ewm(alpha=1/length, adjust=False).mean()

def vwap(df: pd.DataFrame) -> pd.Series:
    pv = (df["close"] * df["volume"]).cumsum()
    vv = df["volume"].replace(0, np.nan).cumsum()
    out = pv / vv
    return out.ffill().fillna(df["close"])

def donchian(df: pd.DataFrame, length: int = 20) -> Tuple[pd.Series, pd.Series]:
    upper = df["high"].rolling(length).max()
    lower = df["low"].rolling(length).min()
    return upper, lower

def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["ema20"] = ema(df["close"], 20)
    df["ema50"] = ema(df["close"], 50)
    df["rsi"] = rsi(df["close"], 14)
    df["atr"] = atr(df, 14)
    df["vwap"] = vwap(df)
    dc_u, dc_l = donchian(df, 20)
    df["dc_u"] = dc_u
    df["dc_l"] = dc_l
    return df

# -------------- Performance Tracking --------------
DATA_DIR = pathlib.Path(os.getenv("DATA_DIR", "./data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
ALERTS_CSV   = DATA_DIR / "alerts.csv"
DECISIONS_CSV= DATA_DIR / "decisions.csv"
FILLS_CSV    = DATA_DIR / "fills.csv"

def append_csv(path: pathlib.Path, row: Dict[str, Any]):
    exists = path.exists()
    with path.open("a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(row.keys()))
        if not exists:
            writer.writeheader()
        writer.writerow(row)

class SignalMetrics:
    """Track and analyze historical signal performance"""
    def __init__(self):
        self.closed_signals = []
        self.load_historical()
    
    def load_historical(self):
        """Load closed alerts from CSV for metrics"""
        self.closed_signals = []
        if FILLS_CSV.exists():
            try:
                with open(FILLS_CSV, 'r') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        self.closed_signals.append({
                            'pnl_pct': float(row.get('pnl_pct', 0)),
                            'score': float(row.get('score', 0)),
                            'reason': row.get('reason', ''),
                            'time': row.get('time', '')
                        })
            except Exception as e:
                log.warning(f"Error loading historical data: {e}")
    
    def add_result(self, pnl_pct: float, score: float, reason: str):
        """Add a new result and save"""
        self.closed_signals.append({
            'pnl_pct': pnl_pct,
            'score': score,
            'reason': reason,
            'time': utc_now().isoformat()
        })
    
    def get_stats(self, min_score: Optional[float] = None) -> Optional[Dict]:
        """Get performance statistics"""
        filtered = self.closed_signals
        if min_score:
            filtered = [s for s in filtered if s.get('score', 0) >= min_score]
        
        if not filtered:
            return None
        
        pnls = [s['pnl_pct'] for s in filtered]
        wins = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p < 0]
        
        return {
            'count': len(filtered),
            'win_rate': (len(wins) / len(filtered)) * 100 if filtered else 0,
            'avg_win': np.mean(wins) if wins else 0,
            'avg_loss': np.mean(losses) if losses else 0,
            'total_pnl': sum(pnls),
            'sharpe': np.mean(pnls) / np.std(pnls) if len(pnls) > 1 and np.std(pnls) > 0 else 0,
            'best': max(pnls) if pnls else 0,
            'worst': min(pnls) if pnls else 0
        }

# -------------- Google Sheets Integration ---------------
@dataclass
class TradeData:
    id: str
    pair: str
    side: str
    entry_price: float
    stop_loss: float
    take_profit_1: float
    take_profit_2: float
    status: str = "OPEN"
    timestamp: str = field(default_factory=lambda: utc_now().isoformat())
    level_name: Optional[str] = None
    level_price: Optional[float] = None
    confidence: Optional[str] = None
    knight: Optional[str] = "Sir Leonis"
    score: Optional[float] = None
    market_context: Optional[str] = None  # New field

class GoogleSheetsIntegration:
    def __init__(self, url: str, token: Optional[str] = None):
        self.url = url
        self.token = token

    async def _post(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        session = await session_mgr.get_session()
        headers = {"Content-Type":"application/json"}
        if self.token:
            headers["x-app-secret"] = self.token
        try:
            async with session.post(self.url, data=json.dumps(payload), headers=headers) as r:
                txt = await r.text()
                if r.status != 200:
                    log.warning(f"Sheets POST {r.status}: {txt}")
                import json as _json
                try:
                    return _json.loads(txt) if txt else {"status":"error","message":"empty"}
                except Exception:
                    return {"status":"error","message":"non-json", "raw": txt[:200]}
        except Exception as e:
            log.error(f"Sheets POST error: {e}")
            return {"status":"error","message":str(e)}

    async def _get(self, params: Dict[str, Any]) -> Dict[str, Any]:
        session = await session_mgr.get_session()
        headers = {}
        if self.token:
            headers["x-app-secret"] = self.token
            params = {**params, "key": self.token}
        try:
            async with session.get(self.url, params=params, headers=headers) as r:
                txt = await r.text()
                if r.status != 200:
                    log.warning(f"Sheets GET {r.status}: {txt}")
                import json as _json
                try:
                    return _json.loads(txt) if txt else {"status":"error","message":"empty"}
                except Exception:
                    return {"status":"error","message":"non-json", "raw": txt[:200]}
        except Exception as e:
            log.error(f"Sheets GET error: {e}")
            return {"status":"error","message":str(e)}

    async def write_entry(self, t: TradeData) -> Dict[str, Any]:
        payload = asdict(t)
        return await self._post(payload)

    async def update_exit(self, trade_id: str, exit_price: float, exit_reason: str, pnl_pct: float) -> Dict[str, Any]:
        payload = {
            "action":"update",
            "id": trade_id,
            "exit_price": exit_price,
            "exit_reason": exit_reason,
            "pnl_pct": pnl_pct,
            "status": "CLOSED"
        }
        return await self._post(payload)

    async def rehydrate_open_trades(self) -> List[TradeData]:
        data = await self._get({"action":"open"})
        rows = data.get("rows", [])
        out: List[TradeData] = []
        for r in rows:
            try:
                out.append(TradeData(
                    id=str(r.get("Trade ID","")),
                    pair=r.get("Asset","ETH/USDT"),
                    side=r.get("Direction","LONG"),
                    entry_price=float(r.get("Entry Price", 0)),
                    stop_loss=float(r.get("Stop Loss", 0)),
                    take_profit_1=float(r.get("Take Profit 1", 0)),
                    take_profit_2=float(r.get("Take Profit 2", 0)),
                    status=r.get("Status","OPEN"),
                    timestamp=r.get("Timestamp", utc_now().isoformat()),
                    level_name=r.get("Level Name"),
                    level_price=float(r.get("Level Price")) if r.get("Level Price") else None,
                    confidence=r.get("Confidence"),
                    knight=r.get("Knight") or "Sir Leonis",
                    score=float(r.get("Original Score")) if r.get("Original Score") else None,
                    market_context=r.get("Market Context")
                ))
            except Exception as e:
                log.warning(f"rehydrate row parse error: {e}")
        return out

# -------------- Trade Manager -----------------
class TradeManager:
    def __init__(self, cfg: Config, sheets: GoogleSheetsIntegration):
        self.cfg = cfg
        self.sheets = sheets
        self.active: Dict[str, TradeData] = {}
        self.cooldown_until: Dict[str, float] = {}  # side -> unix time
        self.metrics = SignalMetrics()
        self.tp1_hits: set = set()  # Track which alerts have hit TP1

    async def rehydrate(self):
        rows = await self.sheets.rehydrate_open_trades()
        for t in rows:
            self.active[t.id] = t
        log.info(f"Rehydrated {len(rows)} open alerts from Sheets.")

    def on_cooldown(self, side: str) -> bool:
        ts = self.cooldown_until.get(side, 0)
        return time.time() < ts

    def set_cooldown(self, side: str):
        self.cooldown_until[side] = time.time() + self.cfg.alert_cooldown_minutes * 60

    def calculate_pnl(self, trade: TradeData, exit_price: float) -> float:
        """Calculate P&L percentage"""
        direction = 1 if trade.side.upper() == "LONG" else -1
        return direction * (exit_price - trade.entry_price) / trade.entry_price * 100.0

    async def open_trade(self, t: TradeData):
        self.active[t.id] = t
        # CSV log
        append_csv(ALERTS_CSV, {**asdict(t), "opened_at": utc_now().isoformat()})
        # Sheets
        await self.sheets.write_entry(t)

    async def close_trade(self, trade_id: str, exit_price: float, reason: str) -> Optional[float]:
        """Close a trade and return PnL%"""
        t = self.active.get(trade_id)
        if not t:
            return None
        
        pnl_pct = self.calculate_pnl(t, exit_price)
        
        # Update metrics
        self.metrics.add_result(pnl_pct, t.score or 0, reason)
        
        # Sheets & CSV
        await self.sheets.update_exit(trade_id, exit_price, reason, pnl_pct)
        append_csv(FILLS_CSV, {
            "id": trade_id, 
            "exit_price": exit_price, 
            "reason": reason, 
            "pnl_pct": pnl_pct, 
            "score": t.score,
            "time": utc_now().isoformat()
        })
        
        # mark closed
        t.status = "CLOSED"
        self.active.pop(trade_id, None)
        self.tp1_hits.discard(trade_id)
        
        return pnl_pct

    async def check_alert_exits(self, current_price: float) -> List[Dict]:
        """Check if any active alerts have hit their SL/TP levels"""
        closed_alerts = []
        
        for trade_id, trade in list(self.active.items()):
            if trade.status != "OPEN":
                continue
            
            exit_reason = None
            exit_price = current_price
            
            if trade.side == "LONG":
                if current_price <= trade.stop_loss:
                    exit_reason = "Stop Loss Hit"
                elif current_price >= trade.take_profit_2:
                    exit_reason = "TP2 Hit"
                elif current_price >= trade.take_profit_1 and trade_id not in self.tp1_hits:
                    # Track TP1 hit but don't close yet (could add partial close logic)
                    self.tp1_hits.add(trade_id)
                    log.info(f"Alert {trade_id} hit TP1 at {current_price:.2f}")
            else:  # SHORT
                if current_price >= trade.stop_loss:
                    exit_reason = "Stop Loss Hit"
                elif current_price <= trade.take_profit_2:
                    exit_reason = "TP2 Hit"
                elif current_price <= trade.take_profit_1 and trade_id not in self.tp1_hits:
                    self.tp1_hits.add(trade_id)
                    log.info(f"Alert {trade_id} hit TP1 at {current_price:.2f}")
            
            if exit_reason:
                pnl_pct = await self.close_trade(trade_id, exit_price, exit_reason)
                if pnl_pct is not None:
                    closed_alerts.append({
                        'trade': trade,
                        'exit_price': exit_price,
                        'exit_reason': exit_reason,
                        'pnl_pct': pnl_pct
                    })
                    log.info(f"Auto-closed alert {trade_id}: {exit_reason} at {exit_price:.2f} ({pnl_pct:+.2f}%)")
        
        return closed_alerts

    async def expire_old_alerts(self) -> List[str]:
        """Close alerts older than configured hours"""
        expired = []
        now = utc_now()
        
        for trade_id, trade in list(self.active.items()):
            if trade.status != "OPEN":
                continue
            
            trade_time = datetime.fromisoformat(trade.timestamp.replace('Z', '+00:00'))
            if (now - trade_time).total_seconds() > self.cfg.alert_expire_hours * 3600:
                await self.close_trade(trade_id, _last_price or trade.entry_price, "Expired")
                expired.append(trade_id)
                log.info(f"Expired alert {trade_id} after {self.cfg.alert_expire_hours} hours")
        
        return expired

# -------------- Signal Engine -----------------
@dataclass
class Signal:
    side: str   # LONG or SHORT
    score: float
    entry: float
    stop: float
    tp1: float
    tp2: float
    reason: str
    market_context: str = ""

def get_market_context(df: pd.DataFrame) -> str:
    """Provide market context for alerts"""
    if df is None or len(df) < 50:
        return ""
    
    latest = df.iloc[-1]
    context = []
    
    # Trend context
    if latest["ema20"] > latest["ema50"]:
        context.append("📈 Uptrend (EMA20>50)")
    else:
        context.append("📉 Downtrend (EMA20<50)")
    
    # RSI context  
    if latest["rsi"] > 70:
        context.append("⚠️ RSI Overbought")
    elif latest["rsi"] < 30:
        context.append("⚠️ RSI Oversold")
    elif latest["rsi"] > 60:
        context.append("💪 RSI Strong")
    elif latest["rsi"] < 40:
        context.append("😟 RSI Weak")
    
    # Volatility context
    atr_percentile = (latest["atr"] / latest["close"]) * 100
    if atr_percentile > 3:
        context.append("🌊 High Volatility")
    elif atr_percentile < 1:
        context.append("😴 Low Volatility")
    else:
        context.append("⚖️ Normal Volatility")
    
    # VWAP context
    if latest["close"] > latest["vwap"]:
        context.append("✅ Above VWAP")
    else:
        context.append("❌ Below VWAP")
    
    return " | ".join(context)

class EnhancedSignalEngine:
    def __init__(self, cfg: Config):
        self.cfg = cfg

    def _risk_targets(self, price: float, atr_val: float, side: str) -> Tuple[float, float, float]:
        """Calculate stop and targets based on ATR"""
        risk = self.cfg.risk_atr_mult * atr_val
        if side == "LONG":
            stop = price - risk
            tp1 = price + (self.cfg.tp1_r_multiple * risk)
            tp2 = price + (self.cfg.tp2_r_multiple * risk)
        else:
            stop = price + risk
            tp1 = price - (self.cfg.tp1_r_multiple * risk)
            tp2 = price - (self.cfg.tp2_r_multiple * risk)
        return stop, tp1, tp2

    def validate_signal(self, sig: Signal, df: pd.DataFrame) -> bool:
        """Additional signal validation before alerting"""
        latest = df.iloc[-1]
        
        # Check for extreme RSI
        if latest["rsi"] > 80 and sig.side == "LONG":
            log.info("Signal rejected: RSI > 80 for LONG")
            return False
        if latest["rsi"] < 20 and sig.side == "SHORT":
            log.info("Signal rejected: RSI < 20 for SHORT")
            return False
        
        # Check spread/volatility
        if latest["atr"] < df["atr"].mean() * 0.5:  # Too low volatility
            log.info("Signal rejected: ATR too low")
            return False
        
        return True

    def format_signal_reason(self, components: List[str], score: float) -> str:
        """Create detailed reason for Discord alert"""
        confidence = "🔥 STRONG" if score >= 4.5 else "✅ GOOD" if score >= 3.5 else "⚡ MODERATE"
        
        formatted = f"**Signal Confidence: {confidence} (Score: {score:.1f}/6.0)**\n\n"
        formatted += "**Triggered Conditions:**\n"
        for i, comp in enumerate(components, 1):
            formatted += f"{i}. {comp}\n"
        
        return formatted

    def generate(self, df: pd.DataFrame) -> Optional[Signal]:
        if df is None or len(df) < 60:
            return None
        latest = df.iloc[-1]
        prev   = df.iloc[-2]
        score_long = 0.0
        score_short= 0.0
        reason_long_parts  = []
        reason_short_parts = []

        # Trend filters
        uptrend   = latest["ema20"] > latest["ema50"]
        downtrend = latest["ema20"] < latest["ema50"]

        # Breakout conditions
        breakout_long  = latest["close"] > latest["dc_u"] and uptrend and latest["close"] > latest["vwap"]
        if breakout_long:
            score_long += 1.0
            reason_long_parts.append("Breakout above Donchian & VWAP in uptrend")

        breakout_short = latest["close"] < latest["dc_l"] and downtrend and latest["close"] < latest["vwap"]
        if breakout_short:
            score_short += 1.0
            reason_short_parts.append("Breakdown below Donchian & VWAP in downtrend")

        # Pullback: bounce from EMA20 in trend with RSI bias
        bounced_long = (prev["close"] < prev["ema20"]) and (latest["close"] > latest["ema20"]) and uptrend and latest["rsi"] > 50
        if bounced_long:
            score_long += 1.0
            reason_long_parts.append("Pullback bounce on EMA20 with RSI>50")

        bounced_short = (prev["close"] > prev["ema20"]) and (latest["close"] < latest["ema20"]) and downtrend and latest["rsi"] < 50
        if bounced_short:
            score_short += 1.0
            reason_short_parts.append("Pullback bounce under EMA20 with RSI<50")

        # Continuation momentum
        cont_long = uptrend and latest["close"] > latest["ema20"] and latest["rsi"] > 55 and latest["atr"] > df["atr"].iloc[-15]
        if cont_long:
            score_long += 1.0
            reason_long_parts.append("Continuation: >EMA20, RSI>55, ATR rising")

        cont_short = downtrend and latest["close"] < latest["ema20"] and latest["rsi"] < 45 and latest["atr"] > df["atr"].iloc[-15]
        if cont_short:
            score_short += 1.0
            reason_short_parts.append("Continuation: <EMA20, RSI<45, ATR rising")

        # Volume proxy: body vs range vs ATR
        body = abs(latest["close"] - latest["open"])
        rng  = latest["high"] - latest["low"]
        atrv = latest["atr"]
        if rng > 0 and atrv > 0:
            body_ratio = body / max(rng, 1e-9)
            atr_ratio  = rng / atrv
            if body_ratio > 0.5:  # strong body
                if latest["close"] > latest["open"]:
                    score_long += 0.5
                    reason_long_parts.append("Strong bullish candle")
                else:
                    score_short += 0.5
                    reason_short_parts.append("Strong bearish candle")
            if atr_ratio > 1.1:
                if score_long > score_short:
                    score_long += 0.5
                    reason_long_parts.append("High range expansion")
                elif score_short > score_long:
                    score_short += 0.5
                    reason_short_parts.append("High range expansion")

        # Decide
        side = None
        score = 0.0
        reasons = ""
        price = float(latest["close"])
        
        if score_long >= score_short and score_long >= self.cfg.min_signal_score:
            side = "LONG"
            score = score_long
            reasons = self.format_signal_reason(reason_long_parts, score)
        elif score_short > score_long and score_short >= self.cfg.min_signal_score:
            side = "SHORT"
            score = score_short
            reasons = self.format_signal_reason(reason_short_parts, score)
        else:
            return None

        stop, tp1, tp2 = self._risk_targets(price, float(atrv), side)
        market_ctx = get_market_context(df)
        
        sig = Signal(
            side=side,
            score=float(round(score, 2)),
            entry=price,
            stop=float(stop),
            tp1=float(tp1),
            tp2=float(tp2),
            reason=reasons,
            market_context=market_ctx
        )
        
        # Validate before returning
        if not self.validate_signal(sig, df):
            return None
        
        return sig

# -------------- Position Sizing --------------
def calculate_position_size(account_balance: float, risk_pct: float, 
                           entry: float, stop: float) -> float:
    """Calculate position size based on risk percentage"""
    risk_amount = account_balance * (risk_pct / 100)
    price_risk = abs(entry - stop)
    if price_risk == 0:
        return 0
    position_size = risk_amount / price_risk
    return round(position_size, 4)

# -------------- Discord Bot -----------------
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents, help_command=None)

class DiscordIO:
    def __init__(self, cfg: Config, client: commands.Bot):
        self.cfg = cfg
        self.client = client

    def channel(self, channel_id: int) -> Optional[discord.TextChannel]:
        ch = self.client.get_channel(channel_id)
        return ch

    async def safe_send(self, channel_id: int, embed_obj: 'discord.Embed' = None, **embed_kwargs):
        ch = self.channel(channel_id)
        if not ch:
            try:
                ch = await self.client.fetch_channel(channel_id)
            except Exception as fe:
                log.error(f"fetch_channel failed for {channel_id}: {fe}")
                ch = None
        if not ch:
            log.warning(f"Channel {channel_id} unavailable")
            return
        embed = embed_obj if embed_obj is not None else discord.Embed(**embed_kwargs)
        try:
            await ch.send(embed=embed)
        except Exception as e:
            log.error(f"send error: {e}")

# -------------- Embeds -----------------
def build_trade_embed(t: TradeData, cfg: Config) -> discord.Embed:
    now_utc = utc_now()
    ct_str  = fmt_dt(now_utc, cfg.tz_name)
    
    # Calculate position size example (assuming $10k account)
    pos_size = calculate_position_size(10000, cfg.position_risk_pct, t.entry_price, t.stop_loss)
    
    # Main description with formatting
    desc = (
        f"**Entry**: ${t.entry_price:.2f}\n"
        f"**Stop Loss**: ${t.stop_loss:.2f} ({abs(t.entry_price - t.stop_loss):.2f} pts)\n"
        f"**TP1**: ${t.take_profit_1:.2f} ({abs(t.take_profit_1 - t.entry_price):.2f} pts)\n"
        f"**TP2**: ${t.take_profit_2:.2f} ({abs(t.take_profit_2 - t.entry_price):.2f} pts)\n\n"
    )
    
    if t.level_name:
        desc += f"{t.level_name}\n\n"
    
    if t.market_context:
        desc += f"**Market Context:**\n{t.market_context}\n\n"
    
    desc += f"**Position Size (2% risk on $10k):** {pos_size:.4f} ETH"
    
    embed = discord.Embed(
        title=f"⚔️ Battle Signal — {t.pair} ({t.side})",
        description=desc,
        color=0x4CAF50 if t.side.upper()=="LONG" else 0xF44336,
        timestamp=now_utc
    )
    
    embed.add_field(name="Alert ID", value=t.id, inline=True)
    if t.score is not None:
        embed.add_field(name="Score", value=f"{t.score:.2f}/6.0", inline=True)
    
    # Risk:Reward ratio
    risk = abs(t.entry_price - t.stop_loss)
    reward = abs(t.take_profit_2 - t.entry_price)
    rr_ratio = reward / risk if risk > 0 else 0
    embed.add_field(name="R:R", value=f"1:{rr_ratio:.1f}", inline=True)
    
    embed.set_footer(text=f"CT: {ct_str} • UTC: {now_utc.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    return embed

def build_exit_embed(alert_data: Dict, cfg: Config) -> discord.Embed:
    """Build embed for closed alerts"""
    trade = alert_data['trade']
    pnl_pct = alert_data['pnl_pct']
    exit_reason = alert_data['exit_reason']
    exit_price = alert_data['exit_price']
    
    now_utc = utc_now()
    ct_str = fmt_dt(now_utc, cfg.tz_name)
    
    # Determine color and emoji based on P&L
    if pnl_pct > 0:
        color = 0x4CAF50  # Green
        emoji = "✅"
        result = "WIN"
    else:
        color = 0xF44336  # Red
        emoji = "❌"
        result = "LOSS"
    
    embed = discord.Embed(
        title=f"{emoji} Alert Closed: {result} ({exit_reason})",
        description=(
            f"**Alert ID:** {trade.id}\n"
            f"**Side:** {trade.side}\n"
            f"**Entry:** ${trade.entry_price:.2f}\n"
            f"**Exit:** ${exit_price:.2f}\n"
            f"**P&L:** {pnl_pct:+.2f}%\n"
            f"**Original Score:** {trade.score:.2f}/6.0"
        ),
        color=color,
        timestamp=now_utc
    )
    
    embed.set_footer(text=f"CT: {ct_str} • UTC: {now_utc.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    return embed

def build_error_embed(msg: str, cfg: Config) -> discord.Embed:
    now_utc = utc_now()
    ct_str  = fmt_dt(now_utc, cfg.tz_name)
    embed = discord.Embed(
        title="⚠️ Error Alert",
        description=msg,
        color=0xFF9800,
        timestamp=now_utc
    )
    embed.set_footer(text=f"CT: {ct_str} • UTC: {now_utc.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    return embed

def build_status_embed(cfg: Config, provider_ok: bool, last_price: Optional[float], tm: TradeManager) -> discord.Embed:
    now_utc = utc_now()
    ct_str  = fmt_dt(now_utc, cfg.tz_name)
    
    # Get performance stats
    stats = tm.metrics.get_stats()
    
    desc = (
        f"**Configuration:**\n"
        f"Provider: **{cfg.provider.upper()}** (REST)\n"
        f"Scan: **{cfg.scan_every_seconds}s** | Min Score: **{cfg.min_signal_score}**\n"
        f"Cooldown: **{cfg.alert_cooldown_minutes}m** | Expire: **{cfg.alert_expire_hours}h**\n"
        f"Risk: **{cfg.position_risk_pct}%** | TP1/TP2: **{cfg.tp1_r_multiple}R/{cfg.tp2_r_multiple}R**\n\n"
    )
    
    if last_price:
        desc += f"**Current Price:** ${last_price:.2f}\n"
    
    desc += f"**Active Alerts:** {len(tm.active)}\n"
    desc += f"**Scanner:** {'🔇 Muted' if scanner_paused else '🔊 Active'}\n\n"
    
    if stats:
        desc += (
            f"**Performance (Last {stats['count']} alerts):**\n"
            f"Win Rate: **{stats['win_rate']:.1f}%**\n"
            f"Avg Win: **{stats['avg_win']:+.2f}%** | Avg Loss: **{stats['avg_loss']:.2f}%**\n"
            f"Best: **{stats['best']:+.2f}%** | Worst: **{stats['worst']:.2f}%**"
        )
    
    embed = discord.Embed(title="🛰 Scout Tower Status", description=desc, color=0x00BCD4, timestamp=now_utc)
    embed.set_footer(text=f"CT: {ct_str} • UTC: {now_utc.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    return embed

def build_startup_embed(cfg: Config, open_count: int) -> discord.Embed:
    now_utc = utc_now()
    ct_str  = fmt_dt(now_utc, cfg.tz_name)
    desc = (
        f"Bot is online and scanning.\n"
        f"Open alerts rehydrated: **{open_count}**\n"
        f"Provider: **{cfg.provider.upper()}**\n"
        f"Scan: **{cfg.scan_every_seconds}s** | Min Score: **{cfg.min_signal_score}** | Cooldown: **{cfg.alert_cooldown_minutes}m**"
    )
    emb = discord.Embed(title="🚀 Scout Tower Online", description=desc, color=0x7C4DFF, timestamp=now_utc)
    emb.set_footer(text=f"CT: {ct_str} • UTC: {now_utc.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    return emb

# -------------- Globals -----------------
mdp   = MarketDataProvider(CFG)
sheets= GoogleSheetsIntegration(CFG.sheets_webhook, CFG.sheets_token)
tm    = TradeManager(CFG, sheets)
engine= EnhancedSignalEngine(CFG)
dio   = DiscordIO(CFG, bot)

# -------------- Presets -----------------
PRESETS = {
    "testing": {"scan": 15,  "min_score": 2.0, "cooldown": 2},
    "level1":  {"scan": 30,  "min_score": 2.5, "cooldown": 5},
    "level2":  {"scan": 60,  "min_score": 3.0, "cooldown": 15},
    "level3":  {"scan": 120, "min_score": 4.0, "cooldown": 30},
}

def _mode_name_from_alias(s: str) -> str:
    s = (s or "").strip().lower()
    aliases = {
        "l1":"level1", "level 1":"level1",
        "l2":"level2", "level 2":"level2",
        "l3":"level3", "level 3":"level3",
        "test":"testing", "t":"testing"
    }
    return aliases.get(s, s)

async def apply_preset(name: str) -> dict:
    """Apply a preset at runtime"""
    key = _mode_name_from_alias(name)
    p = PRESETS.get(key)
    if not p:
        raise ValueError(f"unknown preset '{name}'")
    CFG.scan_every_seconds = int(p["scan"])
    CFG.min_signal_score = float(p["min_score"])
    CFG.alert_cooldown_minutes = int(p["cooldown"])
    try:
        if scanner.is_running():
            scanner.change_interval(seconds=CFG.scan_every_seconds)
    except Exception as e:
        log.warning(f"change_interval failed: {e}")
    return {"name": key, **p}

def current_mode_snapshot() -> dict:
    return {
        "scan": CFG.scan_every_seconds,
        "min_score": CFG.min_signal_score,
        "cooldown": CFG.alert_cooldown_minutes
    }

# -------------- Commands -----------------
@bot.command()
async def ping(ctx):
    await ctx.reply("Pong! Bot is alive.")

@bot.command()
async def status(ctx):
    emb = build_status_embed(CFG, provider_ok=True, last_price=_last_price, tm=tm)
    await ctx.send(embed=emb)

@bot.command()
async def force(ctx):
    """Force-scan now and print score details"""
    try:
        df = await mdp.fetch_ohlc(limit=200)
        if df is None or df.empty:
            await ctx.reply("No data available")
            return
        df = add_indicators(df)
        sig = engine.generate(df)
        if sig:
            await ctx.reply(
                f"**Signal Found!**\n"
                f"Side: **{sig.side}** | Score: **{sig.score}**\n"
                f"Entry: ${sig.entry:.2f} | SL: ${sig.stop:.2f}\n"
                f"TP1: ${sig.tp1:.2f} | TP2: ${sig.tp2:.2f}\n\n"
                f"{sig.reason}\n\n"
                f"Market: {sig.market_context}"
            )
        else:
            await ctx.reply("No signal above threshold")
    except Exception as e:
        await ctx.reply(f"Force scan error: {e}")

@bot.command()
async def active(ctx):
    """Show all active alerts with current P&L"""
    if not tm.active:
        await ctx.reply("No active alerts")
        return
    
    if _last_price is None:
        await ctx.reply("No price data available")
        return
    
    lines = ["**Active Alerts:**\n"]
    for trade_id, trade in list(tm.active.items())[:10]:  # Limit to 10
        pnl_pct = tm.calculate_pnl(trade, _last_price)
        emoji = "🟢" if pnl_pct > 0 else "🔴"
        
        # Check if TP1 was hit
        tp1_marker = " ⭐" if trade_id in tm.tp1_hits else ""
        
        lines.append(
            f"{emoji} `{trade_id}` | {trade.side} from ${trade.entry_price:.2f}\n"
            f"   Current: ${_last_price:.2f} ({pnl_pct:+.2f}%){tp1_marker}"
        )
    
    await ctx.reply("\n".join(lines))

@bot.command()
async def close(ctx, trade_id: str, exit_price: float = None):
    """Manually close an alert: !close ETH-1234567 3500.50"""
    if trade_id not in tm.active:
        await ctx.reply(f"Alert {trade_id} not found")
        return
    
    exit_price = exit_price or _last_price
    if not exit_price:
        await ctx.reply("No price available for closing")
        return
    
    pnl = await tm.close_trade(trade_id, exit_price, "Manual Close")
    if pnl is not None:
        emoji = "✅" if pnl > 0 else "❌"
        await ctx.reply(f"{emoji} Alert {trade_id} closed at ${exit_price:.2f} ({pnl:+.2f}%)")

@bot.command(name="result")
async def log_result(ctx, trade_id: str, exit_price: float, *, notes: str = ""):
    """Log trade result: !result ETH-1234567 3500.50 Hit resistance"""
    if trade_id not in tm.active:
        # Check if it was already closed
        await ctx.reply(f"Alert {trade_id} not found in active alerts (may be already closed)")
        return
    
    pnl = await tm.close_trade(trade_id, exit_price, notes or "User reported")
    if pnl is not None:
        emoji = "✅" if pnl > 0 else "❌"
        await ctx.reply(
            f"{emoji} Result logged for {trade_id}\n"
            f"Exit: ${exit_price:.2f} | P&L: {pnl:+.2f}%\n"
            f"Notes: {notes or 'None'}"
        )

@bot.command()
async def stats(ctx):
    """Show signal performance statistics"""
    stats = tm.metrics.get_stats(CFG.min_signal_score)
    
    if not stats:
        await ctx.reply("No historical data available yet")
        return
    
    embed = discord.Embed(
        title="📊 Signal Performance Stats",
        description=(
            f"**Total Alerts:** {stats['count']}\n"
            f"**Win Rate:** {stats['win_rate']:.1f}%\n"
            f"**Avg Win:** {stats['avg_win']:+.2f}%\n"
            f"**Avg Loss:** {stats['avg_loss']:.2f}%\n"
            f"**Total P&L:** {stats['total_pnl']:+.2f}%\n"
            f"**Best Trade:** {stats['best']:+.2f}%\n"
            f"**Worst Trade:** {stats['worst']:.2f}%\n"
            f"**Sharpe Ratio:** {stats['sharpe']:.2f}\n\n"
            f"*Min Score Filter: {CFG.min_signal_score}*"
        ),
        color=0x4CAF50 if stats['total_pnl'] > 0 else 0xF44336
    )
    await ctx.send(embed=embed)

@bot.command()
async def mute(ctx):
    """Temporarily pause alerts"""
    global scanner_paused
    scanner_paused = True
    await ctx.reply("🔇 Alerts muted. Use !unmute to resume.")

@bot.command()
async def unmute(ctx):
    """Resume alerts"""
    global scanner_paused
    scanner_paused = False
    await ctx.reply("🔊 Alerts resumed.")

@bot.command()
async def posttest(ctx):
    """Send a test embed to signals channel"""
    t = TradeData(
        id="TEST-123",
        pair="ETH/USDT",
        side="LONG",
        entry_price=2000,
        stop_loss=1950,
        take_profit_1=2030,
        take_profit_2=2060,
        confidence="Test Alert",
        knight="Sir Leonis",
        score=5.0,
        level_name="**This is a test alert**\nUsed to verify Discord permissions.",
        market_context="📈 Uptrend | ✅ Above VWAP | ⚖️ Normal Volatility"
    )
    emb = build_trade_embed(t, CFG)
    await dio.safe_send(CFG.signals_channel_id, embed_obj=emb)
    await ctx.reply(f"Test alert posted to channel {CFG.signals_channel_id}")

@bot.command(name="mode")
async def set_mode(ctx, *, level: str):
    """Set threshold preset: testing | level1 | level2 | level3"""
    try:
        applied = await apply_preset(level)
        await ctx.reply(
            f"Mode set to **{applied['name']}**\n"
            f"Scan: {applied['scan']}s | Min Score: {applied['min_score']} | Cooldown: {applied['cooldown']}m"
        )
    except ValueError as ve:
        await ctx.reply(f"{ve}. Options: testing, level1, level2, level3")
    except Exception as e:
        await ctx.reply(f"Mode change error: {e}")

@bot.command(name="showmode")
async def show_mode(ctx):
    s = current_mode_snapshot()
    await ctx.reply(
        f"Current mode → Scan: {s['scan']}s | Min Score: {s['min_score']} | Cooldown: {s['cooldown']}m"
    )

@bot.command(name="help", aliases=["commands"])
async def help_command(ctx):
    commands_text = (
        "**📜 Available Commands**\n\n"
        "**Basic:**\n"
        "`!ping` - Check if bot is alive\n"
        "`!status` - Show full status and performance\n"
        "`!help` - Show this message\n\n"
        "**Alerts:**\n"
        "`!active` - Show active alerts with current P&L\n"
        "`!force` - Force immediate scan\n"
        "`!mute` / `!unmute` - Pause/resume alerts\n\n"
        "**Trading:**\n"
        "`!close <id> [price]` - Manually close alert\n"
        "`!result <id> <price> [notes]` - Log trade result\n"
        "`!stats` - Show performance statistics\n\n"
        "**Configuration:**\n"
        "`!mode <preset>` - Change alert sensitivity\n"
        "  Options: `testing`, `level1`, `level2`, `level3`\n"
        "`!showmode` - Display current settings\n\n"
        "**Testing:**\n"
        "`!posttest` - Send test alert to verify setup"
    )
    await ctx.send(commands_text)

# -------------- On Ready -----------------
@bot.event
async def on_ready():
    log.info(f"Discord logged in as {bot.user}")
    await tm.rehydrate()
    if not scanner.is_running():
        scanner.start()
    if not daily_summary.is_running():
        daily_summary.start()
    if not expiry_checker.is_running():
        expiry_checker.start()
    # Startup notice
    try:
        emb = build_startup_embed(CFG, open_count=len(tm.active))
        await dio.safe_send(CFG.startup_channel_id, embed_obj=emb)
    except Exception as e:
        log.warning(f"startup embed error: {e}")

# -------------- Scanner Loop -----------------
@tasks.loop(seconds=CFG.scan_every_seconds)
async def scanner():
    global _last_price
    
    if scanner_paused:
        return
    
    try:
        df = await mdp.fetch_ohlc(limit=200)
        if df is None or df.empty:
            return
        df = add_indicators(df)
        latest = df.iloc[-1]
        _last_price = float(latest["close"])

        # CHECK EXITS FOR EXISTING ALERTS
        closed_alerts = await tm.check_alert_exits(_last_price)
        for alert_info in closed_alerts:
            # Send exit notification
            emb = build_exit_embed(alert_info, CFG)
            await dio.safe_send(CFG.signals_channel_id, embed_obj=emb)

        # Generate new signals
        sig = engine.generate(df)
        if not sig:
            return

        # Cooldown per side
        if tm.on_cooldown(sig.side.upper()):
            log.info(f"Signal skipped - {sig.side} on cooldown")
            return

        # Build trade alert
        trade_id = f"ETH-{int(time.time())}"
        t = TradeData(
            id=trade_id,
            pair="ETH/USDT",
            side=sig.side.upper(),
            entry_price=float(sig.entry),
            stop_loss=float(sig.stop),
            take_profit_1=float(sig.tp1),
            take_profit_2=float(sig.tp2),
            confidence="Confluence",
            knight="Sir Leonis" if sig.side.upper()=="LONG" else "Sir Lucien",
            score=sig.score,
            level_name=sig.reason,
            market_context=sig.market_context
        )
        await tm.open_trade(t)

        # Send alert embed
        emb = build_trade_embed(t, CFG)
        await dio.safe_send(CFG.signals_channel_id, embed_obj=emb)
        
        # Set cooldown
        tm.set_cooldown(sig.side.upper())
        log.info(f"Alert posted: {trade_id} {sig.side} @ {sig.entry:.2f}")

    except Exception as e:
        log.error(f"Scanner error: {e}")
        emb = build_error_embed(f"Scanner error: {str(e)[:200]}", CFG)
        await dio.safe_send(CFG.errors_channel_id, embed_obj=emb)

# -------------- Daily Summary Task -----------------
@tasks.loop(hours=24)
async def daily_summary():
    try:
        emb = build_status_embed(CFG, provider_ok=True, last_price=_last_price, tm=tm)
        await dio.safe_send(CFG.status_channel_id, embed_obj=emb)
        log.info("Daily summary posted")
    except Exception as e:
        log.error(f"Daily summary error: {e}")

# -------------- Alert Expiry Checker -----------------
@tasks.loop(hours=1)
async def expiry_checker():
    """Check for expired alerts every hour"""
    try:
        expired = await tm.expire_old_alerts()
        if expired:
            log.info(f"Expired {len(expired)} old alerts")
            
            # Send notification about expired alerts
            if len(expired) <= 3:
                for trade_id in expired:
                    embed = discord.Embed(
                        title="⏰ Alert Expired",
                        description=f"Alert `{trade_id}` expired after {CFG.alert_expire_hours} hours",
                        color=0x9E9E9E,
                        timestamp=utc_now()
                    )
                    await dio.safe_send(CFG.status_channel_id, embed_obj=embed)
            else:
                embed = discord.Embed(
                    title="⏰ Alerts Expired",
                    description=f"{len(expired)} alerts expired after {CFG.alert_expire_hours} hours:\n" + 
                               "\n".join([f"• `{tid}`" for tid in expired[:10]]),
                    color=0x9E9E9E,
                    timestamp=utc_now()
                )
                await dio.safe_send(CFG.status_channel_id, embed_obj=embed)
    except Exception as e:
        log.error(f"Expiry checker error: {e}")

# -------------- Flask Health Endpoint -----------------
app = Flask(__name__)

@app.route("/")
def root():
    return jsonify({
        "ok": True,
        "bot": "Scout Tower v3.0",
        "time": utc_now().isoformat(),
        "active_alerts": len(tm.active),
        "scanner_paused": scanner_paused,
        "last_price": _last_price
    })

@app.route("/health")
def health():
    """Detailed health check endpoint"""
    health_status = {
        "status": "healthy",
        "checks": {
            "discord": bot.is_ready(),
            "scanner": scanner.is_running(),
            "data_age": None,
            "active_alerts": len(tm.active)
        }
    }
    
    # Check data freshness
    if _last_price:
        health_status["checks"]["last_price"] = _last_price
        health_status["checks"]["data_age"] = "fresh"
    else:
        health_status["status"] = "degraded"
        health_status["checks"]["data_age"] = "stale"
    
    status_code = 200 if health_status["status"] == "healthy" else 503
    return jsonify(health_status), status_code

def run_flask():
    app.run(host="0.0.0.0", port=CFG.port, debug=False, use_reloader=False)

# -------------- Cleanup -----------------
async def cleanup():
    """Cleanup resources on shutdown"""
    log.info("Shutting down...")
    await session_mgr.close()
    if scanner.is_running():
        scanner.stop()
    if daily_summary.is_running():
        daily_summary.stop()
    if expiry_checker.is_running():
        expiry_checker.stop()

# -------------- Entrypoint -----------------
def main():
    # Start Flask in thread
    import threading
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    
    # Run bot with cleanup handling
    try:
        bot.run(CFG.token)
    except KeyboardInterrupt:
        log.info("Received interrupt signal")
        asyncio.run(cleanup())
    except Exception as e:
        log.error(f"Bot crashed: {e}")
        asyncio.run(cleanup())
        raise

if __name__ == "__main__":
    main()