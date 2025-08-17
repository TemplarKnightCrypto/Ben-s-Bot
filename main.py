# ===============================================
# Scout Tower - Enhanced ETH Alert Bot v3.4.3
# - Engine: Donchian breakout + EMA trend + RSI/VWAP/ATR filters
# - Zones: Entry band, SL/TP from ATR + structure
# - Providers: Binance primary, Kraken fallback
# - Discord: commands (modes, stats, csv, active/close/result, sheets utils)
# - Sheets: webhook write + rehydrate of open alerts
# - CSV: alerts.csv / decisions.csv / fills.csv
# - Flask: /health and /metrics
# - ENHANCED: Heartbeat reports, market analysis, visual health indicators
# - EXIT ALERTS: Automatic TP1/TP2/SL detection and alerts
# ===============================================

import os, sys, csv, json, math, time, asyncio, logging, random, threading, io
from dataclasses import dataclass, field, asdict
from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime, timezone, timedelta
from pathlib import Path

import aiohttp
import numpy as np
import pandas as pd

import discord
from discord.ext import tasks, commands

from flask import Flask, jsonify

VERSION = "3.4.3"

# ---------------- Logging ----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("ScoutTower")

# ---------------- Helpers ----------------
def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def ts() -> str:
    return utc_now().strftime("%Y-%m-%d %H:%M:%S")

def rr(entry: float, sl: float, tp: float) -> float:
    risk = abs(entry - sl)
    reward = abs(tp - entry)
    return round(reward / risk, 2) if risk > 0 else 0.0

def clamp(v, lo, hi): return max(lo, min(hi, v))

def pct(a, b):  # (a vs b - 1)*100
    if b == 0: return 0
    return (a / b - 1.0) * 100.0

# ---------------- Files/CSV ----------------
DATA_DIR = Path(os.getenv("DATA_DIR", "./data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
ALERTS_CSV    = DATA_DIR / "alerts.csv"
DECISIONS_CSV = DATA_DIR / "decisions.csv"
FILLS_CSV     = DATA_DIR / "fills.csv"

def append_csv(path: Path, row: dict):
    exists = path.exists()
    with path.open("a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(row.keys()))
        if not exists:
            w.writeheader()
        w.writerow(row)

# ---------------- Config ----------------
@dataclass
class Config:
    token: str = field(default_factory=lambda: os.getenv("DISCORD_TOKEN","").strip())
    sheets_webhook: str = field(default_factory=lambda: os.getenv("GOOGLE_SHEETS_WEBHOOK","").strip())
    sheets_token: Optional[str] = field(default_factory=lambda: os.getenv("SHEETS_TOKEN","").strip() or None)

    # Channels
    signals_channel_id: int = field(default_factory=lambda: int(os.getenv("SIGNALS_CHANNEL_ID","0") or 0))
    status_channel_id:  int = field(default_factory=lambda: int(os.getenv("STATUS_CHANNEL_ID","0") or 0))
    errors_channel_id:  int = field(default_factory=lambda: int(os.getenv("ERRORS_CHANNEL_ID","0") or 0))
    startup_channel_id: int = field(default_factory=lambda: int(os.getenv("STARTUP_CHANNEL_ID","0") or 0))

    # Provider + pair
    provider: str = field(default_factory=lambda: os.getenv("PROVIDER","binance").lower())
    pair: str = field(default_factory=lambda: os.getenv("PAIR","ETHUSDT"))
    interval: str = field(default_factory=lambda: os.getenv("INTERVAL","1m"))

    # Engine / scan
    scan_every_seconds: int = field(default_factory=lambda: int(os.getenv("SCAN_EVERY","30") or 30))
    min_score: float = field(default_factory=lambda: float(os.getenv("MIN_SCORE","2.5") or 2.5))
    cooldown_minutes: int = field(default_factory=lambda: int(os.getenv("COOLDOWN_MIN","5") or 5))
    zone_bps: int = field(default_factory=lambda: int(os.getenv("ZONE_BPS","5") or 5)) # entry band: 5 bps = 0.05%

    # HTTP
    port: int = field(default_factory=lambda: int(os.getenv("PORT","10000") or 10000))

CFG = Config()

# ---------------- Discord ----------------
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents, help_command=None)

# ---------------- Indicator Utils ----------------
def ema(series: pd.Series, n: int) -> pd.Series:
    return series.ewm(span=n, adjust=False).mean()

def rsi(series: pd.Series, n: int = 14) -> pd.Series:
    delta = series.diff()
    up = np.where(delta > 0, delta, 0.0)
    dn = np.where(delta < 0, -delta, 0.0)
    roll_up = pd.Series(up, index=series.index).rolling(n).mean()
    roll_dn = pd.Series(dn, index=series.index).rolling(n).mean()
    rs = roll_up / (roll_dn.replace(0, np.nan))
    out = 100.0 - (100.0/(1.0+rs))
    return out.bfill().fillna(50.0)

def atr(df: pd.DataFrame, n: int = 14) -> pd.Series:
    h, l, c = df["high"], df["low"], df["close"]
    tr = np.maximum(h - l, np.maximum(abs(h - c.shift(1)), abs(l - c.shift(1))))
    return pd.Series(tr).rolling(n).mean().bfill()

def vwap(df: pd.DataFrame) -> pd.Series:
    pv = (df["close"] * df["volume"]).cumsum()
    vol = df["volume"].cumsum().replace(0, np.nan)
    return (pv / vol).fillna(df["close"])

def donchian(df: pd.DataFrame, n: int = 20) -> Tuple[pd.Series, pd.Series]:
    dc_u = df["high"].rolling(n).max()
    dc_l = df["low"].rolling(n).min()
    return dc_u, dc_l

def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["ema20"] = ema(df["close"], 20)
    df["ema50"] = ema(df["close"], 50)
    df["rsi"]   = rsi(df["close"], 14)
    df["atr"]   = atr(df, 14)
    df["vwap"]  = vwap(df)
    dc_u, dc_l  = donchian(df, 20)
    df["dc_u"], df["dc_l"] = dc_u, dc_l
    return df
# ---------------- Market Data Providers ----------------
# Global tracking for health monitoring
_last_successful_fetch: Optional[datetime] = None
_last_fetch_error: Optional[str] = None

class MarketData:
    def __init__(self, pair: str, interval: str):
        self.pair = pair
        self.interval = interval

    async def fetch_binance(self, session: aiohttp.ClientSession, limit: int = 250) -> Optional[pd.DataFrame]:
        # Binance klines
        m = {
            "1m":"1m","3m":"3m","5m":"5m","15m":"15m","30m":"30m",
            "1h":"1h","2h":"2h","4h":"4h","1d":"1d"
        }.get(self.interval, "1m")
        url = f"https://api.binance.com/api/v3/klines?symbol={self.pair}&interval={m}&limit={limit}"
        async with session.get(url, timeout=20) as r:
            if r.status != 200:
                raise RuntimeError(f"Binance status {r.status}")
            data = await r.json()
        cols = ["open_time","open","high","low","close","volume","close_time","qav","trades","taker_base","taker_quote","ignore"]
        df = pd.DataFrame(data, columns=cols)
        for col in ["open","high","low","close","volume"]:
            df[col] = df[col].astype(float)
        df["time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)
        return df[["time","open","high","low","close","volume"]].set_index("time")

    async def fetch_kraken(self, session: aiohttp.ClientSession, limit: int = 250) -> Optional[pd.DataFrame]:
        pair = "XETHZUSD" if self.pair.upper().endswith("USD") else "XETHZUSDT"
        interval_map = {"1m":"1","5m":"5","15m":"15","30m":"30","1h":"60","4h":"240","1d":"1440"}
        i = interval_map.get(self.interval, "1")
        url = f"https://api.kraken.com/0/public/OHLC?pair={pair}&interval={i}"
        async with session.get(url, timeout=20) as r:
            if r.status != 200:
                raise RuntimeError(f"Kraken status {r.status}")
            data = await r.json()
        key = next(iter(data["result"]))
        arr = data["result"][key]
        cols = ["time","open","high","low","close","vwap","volume","count"]
        df = pd.DataFrame(arr, columns=cols)
        for col in ["open","high","low","close","volume"]:
            df[col] = df[col].astype(float)
        df["time"] = pd.to_datetime(df["time"], unit="s", utc=True)
        return df[["time","open","high","low","close","volume"]].set_index("time")

    async def fetch_ohlc(self, limit: int = 250) -> Optional[pd.DataFrame]:
        global _last_successful_fetch, _last_fetch_error
        async with aiohttp.ClientSession() as session:
            try:
                df = await self.fetch_binance(session, limit=limit)
                _last_successful_fetch = utc_now()
                _last_fetch_error = None
                return df
            except Exception as e:
                log.warning(f"Binance failed: {e}; trying Kraken...")
                try:
                    df = await self.fetch_kraken(session, limit=limit)
                    _last_successful_fetch = utc_now()
                    _last_fetch_error = None
                    return df
                except Exception as e2:
                    log.error(f"Kraken failed: {e2}")
                    _last_fetch_error = str(e2)
                    return None

# ---------------- Health Monitoring Functions ----------------
def last_successful_fetch_within_5min() -> bool:
    if not _last_successful_fetch:
        return False
    return (utc_now() - _last_successful_fetch).total_seconds() < 300

def get_system_health_emoji() -> str:
    """Visual health indicator"""
    checks = [
        scanner.is_running() if 'scanner' in globals() else False,
        last_successful_fetch_within_5min(),
        len(tm.active) < 10 if 'tm' in globals() else True,  # Not overloaded
    ]
    healthy_count = sum(checks)
    
    if healthy_count == 3: return "🟢"
    elif healthy_count == 2: return "🟡"
    else: return "🔴"

def get_current_mode() -> str:
    """Determine current operational mode based on settings"""
    for mode, settings in PRESETS.items():
        if (CFG.scan_every_seconds == settings["scan"] and 
            CFG.min_score == settings["min_score"] and 
            CFG.cooldown_minutes == settings["cooldown"]):
            return mode
    return "custom"

def hours_since_last_signal() -> float:
    """Hours since last signal"""
    if not _last_alert_time:
        return 999.0
    return (utc_now() - _last_alert_time).total_seconds() / 3600

# ---------------- Market Analysis Functions ----------------
def detect_market_regime(df: pd.DataFrame) -> str:
    """Classify current market conditions"""
    if df is None or df.empty:
        return "❓ **UNKNOWN** - No data"
    
    recent = df.tail(20)
    
    # Volatility regime
    volatility = recent["atr"].mean() / recent["close"].mean() * 100
    
    # Trend strength
    ema_spread = abs(recent["ema20"].iloc[-1] - recent["ema50"].iloc[-1]) / recent["close"].iloc[-1] * 100
    
    # Range-bound detection
    price_range = (recent["high"].max() - recent["low"].min()) / recent["close"].mean() * 100
    
    if volatility < 1.0 and price_range < 3.0:
        return "😴 **CHOPPY/RANGING** - Low signal environment"
    elif ema_spread > 2.0:
        direction = "UP" if recent["ema20"].iloc[-1] > recent["ema50"].iloc[-1] else "DOWN"
        return f"📈 **TRENDING {direction}** - Good signal environment"
    elif volatility > 3.0:
        return "⚡ **HIGH VOLATILITY** - Breakout opportunities"
    else:
        return "📊 **NEUTRAL** - Mixed conditions"

async def check_near_misses(df: pd.DataFrame) -> List[str]:
    """Report signals that almost triggered"""
    if df is None or df.empty:
        return []
    
    row = df.iloc[-1]
    near_misses = []
    
    # Close to breakout but score too low
    if row["close"] > row["dc_u"] * 0.999:  # Within 0.1% of breakout
        score = score_signal(df, len(df)-1, "LONG")
        if score < CFG.min_score:
            near_misses.append(f"🔸 LONG breakout close (score: {score:.1f}/{CFG.min_score})")
    
    if row["close"] < row["dc_l"] * 1.001:  # Within 0.1% of breakdown
        score = score_signal(df, len(df)-1, "SHORT")
        if score < CFG.min_score:
            near_misses.append(f"🔸 SHORT breakdown close (score: {score:.1f}/{CFG.min_score})")
    
    return near_misses

def check_signal_drought() -> Optional[str]:
    """Alert if unusually long without signals"""
    hours = hours_since_last_signal()
    if hours > 6:  # 6+ hours without signal
        return f"🚨 **SIGNAL DROUGHT**: {hours:.1f} hours since last alert"
    return None

async def market_analysis_report() -> str:
    """Why no signals? Market analysis"""
    df = await md.fetch_ohlc(limit=100)
    if df is None:
        return "⚠️ **DATA ISSUE**: Cannot fetch market data"
    
    df = add_indicators(df)
    current = df.iloc[-1]
    
    # Analyze why no signals
    reasons = []
    if abs(current["ema20"] - current["ema50"]) < current["atr"] * 0.1:
        reasons.append("📊 EMA lines too close (sideways market)")
    
    if 45 < current["rsi"] < 55:
        reasons.append("📈 RSI in neutral zone (45-55)")
    
    volatility = current["atr"] / current["close"] * 100
    if volatility < 0.5:
        reasons.append(f"😴 Low volatility ({volatility:.2f}%)")
    
    # Check if close to breakout levels
    dc_distance_up = (current["dc_u"] - current["close"]) / current["close"] * 100
    dc_distance_down = (current["close"] - current["dc_l"]) / current["close"] * 100
    
    analysis = f"""**Market Analysis - Why No Signals?**
Current Price: ${current['close']:.2f}
Distance to upper breakout: {dc_distance_up:.2f}%
Distance to lower breakout: {dc_distance_down:.2f}%

**Conditions blocking signals:**
{chr(10).join(reasons) if reasons else "✅ All conditions met - waiting for breakout"}"""
    
    return analysis

# ---------------- Sheets Integration ----------------
class GoogleSheetsIntegration:
    def __init__(self, url: Optional[str], token: Optional[str]):
        self.url = url
        self.token = token

    async def post(self, payload: dict) -> bool:
        if not self.url:
            return False
        headers = {"Content-Type":"application/json"}
        if self.token:
            headers["x-app-secret"] = self.token
        try:
            async with aiohttp.ClientSession() as s:
                async with s.post(self.url, data=json.dumps(payload), headers=headers, timeout=15) as r:
                    ok = (r.status//100)==2
                    if not ok:
                        txt = await r.text()
                        log.error(f"Sheets POST {r.status}: {txt}")
                    return ok
        except Exception as e:
            log.error(f"Sheets POST error: {e}")
            return False

    async def rehydrate_open_trades(self) -> List[dict]:
        # GET /exec?action=open (optional)
        if not self.url:
            return []
        try:
            async with aiohttp.ClientSession() as s:
                sep = "&" if "?" in self.url else "?"
                url = f"{self.url}{sep}action=open"
                if self.token:
                    url += f"&key={self.token}"
                async with s.get(url, timeout=15) as r:
                    if (r.status//100)==2:
                        return await r.json()
        except Exception as e:
            log.error(f"Sheets rehydrate error: {e}")
        return []

# ---------------- Trade Structures ----------------
@dataclass
class Trade:
    id: str
    pair: str
    side: str  # LONG/SHORT
    zone_lo: float
    zone_hi: float
    entry: Optional[float]
    sl: float
    tp1: float
    tp2: float
    opened_at: str
    status: str = "OPEN"  # OPEN/CLOSED
    notes: str = ""
    rr1: float = 0.0
    rr2: float = 0.0
    tp1_hit: bool = False  # Track if TP1 was hit
    tp2_hit: bool = False  # Track if TP2 was hit
    sl_hit: bool = False   # Track if SL was hit
    entry_confirmed: bool = False  # Track if entry zone was entered

# ---------------- Trade Manager ----------------
class TradeManager:
    def __init__(self, cfg: Config, sheets: GoogleSheetsIntegration):
        self.cfg = cfg
        self.sheets = sheets
        self.active: Dict[str, Trade] = {}
        self.muted: bool = False
        self.metrics = None  # attached later

    def _new_id(self) -> str:
        return f"T{int(time.time()*1000)%10_000_000}"

    async def open_trade(self, side: str, zone_lo: float, zone_hi: float, sl: float, tp1: float, tp2: float, price_now: float) -> Trade:
        t = Trade(
            id=self._new_id(),
            pair=self.cfg.pair,
            side=side,
            zone_lo=zone_lo, zone_hi=zone_hi,
            entry=None,
            sl=sl, tp1=tp1, tp2=tp2,
            opened_at=ts(),
        )
        t.rr1 = rr((zone_lo+zone_hi)/2.0, sl, tp1)
        t.rr2 = rr((zone_lo+zone_hi)/2.0, sl, tp2)
        self.active[t.id] = t

        append_csv(ALERTS_CSV, {
            "id": t.id, "time": t.opened_at, "pair": t.pair, "side": t.side,
            "zone_lo": t.zone_lo, "zone_hi": t.zone_hi, "sl": t.sl, "tp1": t.tp1, "tp2": t.tp2,
            "rr1": t.rr1, "rr2": t.rr2
        })
        asyncio.create_task(self.sheets.post({"action":"entry", **asdict(t)}))
        return t

    async def close_trade(self, trade_id: str, exit_price: float, reason: str = "manual", exit_type: str = "MANUAL"):
        t = self.active.pop(trade_id, None)
        if not t: return False
        t.status = "CLOSED"
        pnl_pct = pct(exit_price, (t.zone_lo+t.zone_hi)/2.0) * (1 if t.side=="LONG" else -1)
        append_csv(FILLS_CSV, {
            "id": t.id, "time": ts(), "pair": t.pair, "side": t.side,
            "exit_price": exit_price, "reason": reason, "exit_type": exit_type, "pnl_pct": round(pnl_pct,2)
        })
        asyncio.create_task(self.sheets.post({"action":"update", "id": t.id, "status":"CLOSED",
                                              "exit_price": exit_price, "exit_reason": reason, "exit_type": exit_type, "pnl_pct": round(pnl_pct,2)}))
        return True

    async def check_exits(self, current_price: float) -> List[dict]:
            """Check all active trades for exit conditions"""
            exits = []
            trades_to_close = []
            
            for trade in list(self.active.values()):
                exit_info = None
                
                if trade.side == "LONG":
                    # Check entry confirmation
                    if not trade.entry_confirmed and trade.zone_lo <= current_price <= trade.zone_hi:
                        trade.entry_confirmed = True
                        trade.entry = current_price
                        exit_info = {
                            "trade": trade,
                            "type": "ENTRY",
                            "price": current_price,
                            "message": f"📍 Entry confirmed in zone"
                        }
                    
                    # Check stop loss
                    elif trade.entry_confirmed and not trade.sl_hit and current_price <= trade.sl:
                        trade.sl_hit = True
                        trades_to_close.append((trade.id, current_price, "SL hit", "SL"))
                        exit_info = {
                            "trade": trade,
                            "type": "SL",
                            "price": current_price,
                            "message": f"🔴 Stop Loss Hit"
                        }
                    
                    # Check TP1
                    elif trade.entry_confirmed and not trade.tp1_hit and current_price >= trade.tp1:
                        trade.tp1_hit = True
                        exit_info = {
                            "trade": trade,
                            "type": "TP1",
                            "price": current_price,
                            "message": f"🎯 Take Profit 1 Hit"
                        }
                    
                    # Check TP2
                    elif trade.entry_confirmed and not trade.tp2_hit and current_price >= trade.tp2:
                        trade.tp2_hit = True
                        trades_to_close.append((trade.id, current_price, "TP2 hit", "TP2"))
                        exit_info = {
                            "trade": trade,
                            "type": "TP2",
                            "price": current_price,
                            "message": f"🎯 Take Profit 2 Hit"
                        }
                
                elif trade.side == "SHORT":
                    # Check entry confirmation
                    if not trade.entry_confirmed and trade.zone_lo <= current_price <= trade.zone_hi:
                        trade.entry_confirmed = True
                        trade.entry = current_price
                        exit_info = {
                            "trade": trade,
                            "type": "ENTRY",
                            "price": current_price,
                            "message": f"📍 Entry confirmed in zone"
                        }
                    
                    # Check stop loss
                    elif trade.entry_confirmed and not trade.sl_hit and current_price >= trade.sl:
                        trade.sl_hit = True
                        trades_to_close.append((trade.id, current_price, "SL hit", "SL"))
                        exit_info = {
                            "trade": trade,
                            "type": "SL",
                            "price": current_price,
                            "message": f"🔴 Stop Loss Hit"
                        }
                    
                    # Check TP1
                    elif trade.entry_confirmed and not trade.tp1_hit and current_price <= trade.tp1:
                        trade.tp1_hit = True
                        exit_info = {
                            "trade": trade,
                            "type": "TP1",
                            "price": current_price,
                            "message": f"🎯 Take Profit 1 Hit"
                        }
                    
                    # Check TP2
                    elif trade.entry_confirmed and not trade.tp2_hit and current_price <= trade.tp2:
                        trade.tp2_hit = True
                        trades_to_close.append((trade.id, current_price, "TP2 hit", "TP2"))
                        exit_info = {
                            "trade": trade,
                            "type": "TP2",
                            "price": current_price,
                            "message": f"🎯 Take Profit 2 Hit"
                        }
                
                if exit_info:
                    exits.append(exit_info)
            
            # Close trades that hit final exits
            for trade_id, price, reason, exit_type in trades_to_close:
                await self.close_trade(trade_id, price, reason, exit_type)
            
            return exits

    def snapshot_active(self) -> List[dict]:
        out = []
        for t in self.active.values():
            status_indicators = []
            if t.entry_confirmed: status_indicators.append("✅")
            if t.tp1_hit: status_indicators.append("🎯1")
            if t.tp2_hit: status_indicators.append("🎯2")
            if t.sl_hit: status_indicators.append("🔴")
            
            out.append({
                "id": t.id, "side": t.side, "zone": f"{t.zone_lo:.2f}-{t.zone_hi:.2f}",
                "sl": round(t.sl,2), "tp1": round(t.tp1,2), "tp2": round(t.tp2,2), 
                "opened": t.opened_at, "status": " ".join(status_indicators) if status_indicators else "⏳"
            })
        return out

# ---------------- Metrics ----------------
class SignalMetrics:
    """Compute basic performance stats from fills.csv on demand"""
    def __init__(self):
        pass

    def _read_pnls(self) -> List[float]:
        if not FILLS_CSV.exists(): return []
        vals = []
        with FILLS_CSV.open("r") as f:
            r = csv.DictReader(f)
            for row in r:
                try:
                    vals.append(float(row.get("pnl_pct", 0)))
                except Exception:
                    pass
        return vals

    def stats(self) -> dict:
        pnls = self._read_pnls()
        wins = [x for x in pnls if x > 0]
        losses = [x for x in pnls if x <= 0]
        avg_win = sum(wins)/len(wins) if wins else 0.0
        avg_loss = sum(losses)/len(losses) if losses else 0.0
        wr = (len(wins)/len(pnls))*100 if pnls else 0.0
        sr = (np.mean(pnls)/np.std(pnls)) if len(pnls)>1 and np.std(pnls)>0 else 0.0
        return {
            "count": len(pnls),
            "win_rate_pct": round(wr,1),
            "avg_win_pct": round(avg_win,2),
            "avg_loss_pct": round(avg_loss,2),
            "sharpe_like": round(float(sr),2),
        }

def attach_metrics(tm: TradeManager):
    if getattr(tm, "metrics", None) is None:
        tm.metrics = SignalMetrics()

# ---------------- Presets (modes) ----------------
PRESETS = {
    "testing": {"scan": 15,  "min_score": 2.0, "cooldown": 2},
    "level1":  {"scan": 30,  "min_score": 2.5, "cooldown": 5},
    "level2":  {"scan": 60,  "min_score": 3.0, "cooldown": 15},
    "level3":  {"scan": 120, "min_score": 4.0, "cooldown": 30},
}

def _mode_alias(s: str) -> str:
    s = (s or "").strip().lower()
    return {
        "l1":"level1","level 1":"level1",
        "l2":"level2","level 2":"level2",
        "l3":"level3","level 3":"level3",
        "t":"testing","test":"testing"
    }.get(s, s)

async def apply_preset(name: str) -> dict:
    key = _mode_alias(name)
    if key not in PRESETS:
        raise ValueError("Unknown mode")
    p = PRESETS[key]
    CFG.scan_every_seconds = int(p["scan"])
    CFG.min_score = float(p["min_score"])
    CFG.cooldown_minutes = int(p["cooldown"])
    return {"mode": key, **p}

def current_mode_snapshot() -> dict:
    return {
        "scan": CFG.scan_every_seconds,
        "min_score": CFG.min_score,
        "cooldown": CFG.cooldown_minutes
    }

# ---------------- Signal Engine ----------------
def score_signal(df: pd.DataFrame, i: int, side: str) -> float:
    row = df.iloc[i]
    ema_trend = (row["ema20"] > row["ema50"]) if side=="LONG" else (row["ema20"] < row["ema50"])
    rsi_ok = (row["rsi"] > 45) if side=="LONG" else (row["rsi"] < 55)
    vwap_ok = (row["close"] >= row["vwap"]) if side=="LONG" else (row["close"] <= row["vwap"])

    base = 0.0
    base += 1.0 if ema_trend else 0.0
    base += 0.7 if rsi_ok else 0.0
    base += 0.7 if vwap_ok else 0.0
    # candle body quality
    body = abs(row["close"] - row["open"])
    rng  = row["high"] - row["low"]
    body_ok = (body >= 0.5 * rng) if rng > 0 else False
    base += 0.6 if body_ok else 0.0
    return round(base, 2)

def generate_signal(df: pd.DataFrame, price_now: float) -> Optional[dict]:
    i = len(df)-1
    row = df.iloc[i]
    # breakouts
    if row["close"] > row["dc_u"] and row["ema20"] > row["ema50"]:
        side = "LONG"
        score = score_signal(df, i, side)
        if score < CFG.min_score: return None
        atrv = float(row["atr"])
        level = float(row["dc_u"])
        zone = (level*(1 - CFG.zone_bps/10000.0), level*(1 + CFG.zone_bps/10000.0))
        sl = row["ema50"] - 0.5*atrv
        tp1 = level + 1.5*atrv
        tp2 = level + 3.0*atrv
        return {"side": side, "zone": zone, "sl": float(sl), "tp1": float(tp1), "tp2": float(tp2), "score": score}

    if row["close"] < row["dc_l"] and row["ema20"] < row["ema50"]:
        side = "SHORT"
        score = score_signal(df, i, side)
        if score < CFG.min_score: return None
        atrv = float(row["atr"])
        level = float(row["dc_l"])
        zone = (level*(1 - CFG.zone_bps/10000.0), level*(1 + CFG.zone_bps/10000.0))
        sl = row["ema50"] + 0.5*atrv
        tp1 = level - 1.5*atrv
        tp2 = level - 3.0*atrv
        return {"side": side, "zone": zone, "sl": float(sl), "tp1": float(tp1), "tp2": float(tp2), "score": score}
    return None

# ---------------- Embeds ----------------
def embed_trade_open(t: Trade, price_now: float) -> discord.Embed:
    title = f"🗼 Scout Tower | {t.pair} {t.side}"
    e = discord.Embed(title=title, colour=discord.Colour.blue(), timestamp=utc_now())
    e.add_field(name="Entry Zone", value=f"{t.zone_lo:.2f} → {t.zone_hi:.2f}", inline=True)
    e.add_field(name="Stop Loss", value=f"{t.sl:.2f}", inline=True)
    e.add_field(name="TP1 / TP2", value=f"{t.tp1:.2f} / {t.tp2:.2f}", inline=True)
    e.add_field(name="R:R (TP1/TP2)", value=f"{t.rr1} / {t.rr2}", inline=True)
    e.add_field(name="Current Price", value=f"${price_now:.2f}", inline=True)
    e.add_field(name="Trade ID", value=f"`{t.id}`", inline=True)
    e.set_footer(text=f"v{VERSION} • {utc_now().strftime('%Y-%m-%d %H:%M:%S')} UTC • Exit alerts enabled")
    return e

def embed_trade_exit(exit_info: dict) -> discord.Embed:
    """Create embed for exit alerts"""
    trade = exit_info["trade"]
    exit_type = exit_info["type"]
    price = exit_info["price"]
    message = exit_info["message"]
    
    # Color coding for different exit types
    colors = {
        "ENTRY": discord.Color.blue(),
        "TP1": discord.Color.green(),
        "TP2": discord.Color.gold(),
        "SL": discord.Color.red()
    }
    
    title = f"🗼 Scout Tower | {trade.pair} {trade.side} • {exit_type}"
    e = discord.Embed(title=title, colour=colors.get(exit_type, discord.Color.blue()), timestamp=utc_now())
    
    e.add_field(name="Alert", value=message, inline=False)
    e.add_field(name="Price", value=f"${price:.2f}", inline=True)
    e.add_field(name="Trade ID", value=f"`{trade.id}`", inline=True)
    
    if exit_type == "ENTRY":
        e.add_field(name="Next Targets", value=f"TP1: ${trade.tp1:.2f} | TP2: ${trade.tp2:.2f}", inline=False)
        e.add_field(name="Stop Loss", value=f"${trade.sl:.2f}", inline=True)
    elif exit_type == "TP1":
        e.add_field(name="Next Target", value=f"TP2: ${trade.tp2:.2f}", inline=True)
        e.add_field(name="Stop Loss", value=f"${trade.sl:.2f}", inline=True)
    elif exit_type in ["TP2", "SL"]:
        entry_price = trade.entry or (trade.zone_lo + trade.zone_hi) / 2
        pnl_pct = pct(price, entry_price) * (1 if trade.side == "LONG" else -1)
        e.add_field(name="PnL", value=f"{pnl_pct:+.2f}%", inline=True)
        e.add_field(name="Entry", value=f"${entry_price:.2f}", inline=True)
    
    # Show progress indicators
    progress = []
    if trade.entry_confirmed: progress.append("✅ Entry")
    if trade.tp1_hit: progress.append("🎯 TP1")
    if trade.tp2_hit: progress.append("🎯 TP2")
    if trade.sl_hit: progress.append("🔴 SL")
    
    if progress:
        e.add_field(name="Progress", value=" → ".join(progress), inline=False)
    
    e.set_footer(text=f"v{VERSION} • {utc_now().strftime('%Y-%m-%d %H:%M:%S')} UTC")
    return e

# ---------------- Error Reporting ----------------
async def send_error_alert(message: str):
    """Send error alerts to designated channel"""
    if CFG.errors_channel_id:
        try:
            ch = bot.get_channel(CFG.errors_channel_id)
            if ch:
                embed = discord.Embed(
                    title="⚠️ System Alert", 
                    description=message,
                    color=discord.Color.red(),
                    timestamp=utc_now()
                )
                await ch.send(embed=embed)
        except Exception as e:
            log.error(f"Failed to send error alert: {e}")

# ---------------- Enhanced Embed Functions ----------------
async def create_online_embed() -> discord.Embed:
    """Enhanced online status embed"""
    embed = discord.Embed(
        title="🗼 Scout Tower Online",
        color=discord.Color.green(),
        timestamp=utc_now()
    )
    
    # Add market context
    try:
        df = await md.fetch_ohlc(limit=50)
        if df is not None and not df.empty:
            current_price = df["close"].iloc[-1]
            daily_change = pct(current_price, df["close"].iloc[-min(24, len(df)-1)])  # 24h change or available
            
            embed.add_field(
                name="📊 Market Status", 
                value=f"ETH: ${current_price:,.2f} ({daily_change:+.1f}%)",
                inline=False
            )
    except Exception as e:
        log.error(f"Error fetching market data for embed: {e}")
    
    # Enhanced configuration with visual indicators
    mode_emoji = {"level1": "🟢", "level2": "🟡", "level3": "🔴", "testing": "🔵", "custom": "⚪"}
    current_mode = get_current_mode()
    data_health = "🟢" if last_successful_fetch_within_5min() else "🔴"
    
    embed.add_field(
        name="⚙️ Configuration",
        value=f"""Mode: {mode_emoji.get(current_mode, '⚪')} **{current_mode.upper()}**
Provider: {CFG.provider.upper()} {data_health}
Scan: {CFG.scan_every_seconds}s | Score: {CFG.min_score} | Cooldown: {CFG.cooldown_minutes}m""",
        inline=False
    )
    
    # Activity summary
    last_signal_time = "Never" if not _last_alert_time else f"{(utc_now() - _last_alert_time).total_seconds()//60:.0f}m ago"
    embed.add_field(
        name="🔍 Activity", 
        value=f"Open alerts: **{len(tm.active)}** | Last signal: {last_signal_time}",
        inline=False
    )
    
    # Add next scan countdown
    next_scan = CFG.scan_every_seconds - ((int(time.time()) % CFG.scan_every_seconds))
    embed.add_field(
        name="⏱️ Next Scan", 
        value=f"~{next_scan}s",
        inline=True
    )
    
    embed.set_footer(text=f"v{VERSION} • Ready to scan • {get_system_health_emoji()}")
    return embed

async def create_status_embed() -> discord.Embed:
    """Enhanced comprehensive status embed"""
    embed = discord.Embed(
        title="🛡️ Scout Tower Status",
        color=discord.Color.blue(),
        timestamp=utc_now()
    )
    
    # Market Overview Section
    try:
        df = await md.fetch_ohlc(limit=100)
        if df is not None and not df.empty:
            df = add_indicators(df)
            current = df.iloc[-1]
            
            # Market regime analysis
            regime = detect_market_regime(df)
            volatility = current["atr"] / current["close"] * 100
            
            embed.add_field(
                name="📈 Market Overview",
                value=f"""**Price:** ${current['close']:,.2f}
**Regime:** {regime}
**Volatility:** {volatility:.1f}% {'🔥' if volatility > 2 else '😴' if volatility < 1 else '📊'}
**RSI:** {current['rsi']:.0f} {'🔴' if current['rsi'] > 70 else '🟢' if current['rsi'] < 30 else '🟡'}""",
                inline=False
            )
            
            # Breakout distances
            dc_up_dist = (current["dc_u"] - current["close"]) / current["close"] * 100
            dc_down_dist = (current["close"] - current["dc_l"]) / current["close"] * 100
            
            embed.add_field(
                name="🎯 Breakout Levels",
                value=f"""**Upper:** ${current['dc_u']:.2f} ({dc_up_dist:+.1f}%)
**Lower:** ${current['dc_l']:.2f} ({dc_down_dist:+.1f}%)""",
                inline=True
            )
    except Exception as e:
        embed.add_field(
            name="📈 Market Overview",
            value=f"⚠️ Error fetching market data: {str(e)[:50]}...",
            inline=False
        )
    
    # Enhanced Configuration with health indicators
    data_health = "🟢" if last_successful_fetch_within_5min() else "🔴"
    sheets_health = "🟢" if CFG.sheets_webhook else "⚪"
    scanner_health = "🟢" if scanner.is_running() else "🔴"
    
    embed.add_field(
        name="⚙️ System Health",
        value=f"""**Data Feed:** {CFG.provider.upper()} {data_health}
**Sheets:** {'Enabled' if CFG.sheets_webhook else 'Disabled'} {sheets_health}
**Scanner:** {'Running' if scanner.is_running() else 'Stopped'} {scanner_health}""",
        inline=True
    )
    
    # Active trades with more detail
    if tm.active:
        active_summary = []
        for trade in list(tm.active.values())[:3]:  # Show max 3
            try:
                # Handle different datetime formats
                opened_time = trade.opened_at
                if isinstance(opened_time, str):
                    # Parse the timestamp string
                    if opened_time.endswith('Z'):
                        opened_time = opened_time[:-1] + '+00:00'
                    opened_dt = datetime.fromisoformat(opened_time)
                    if opened_dt.tzinfo is None:
                        opened_dt = opened_dt.replace(tzinfo=timezone.utc)
                else:
                    opened_dt = opened_time
                
                age = (utc_now() - opened_dt).total_seconds() // 60
                
                # Status indicators
                status = []
                if trade.entry_confirmed: status.append("✅")
                if trade.tp1_hit: status.append("🎯1")
                if trade.tp2_hit: status.append("🎯2")
                if trade.sl_hit: status.append("🔴")
                status_str = "".join(status) if status else "⏳"
                
                active_summary.append(f"**{trade.id}** {trade.side} {status_str} (R:R {trade.rr1}) • {age:.0f}m old")
            except Exception as e:
                active_summary.append(f"**{trade.id}** {trade.side} (R:R {trade.rr1}) • age unknown")
        
        if len(tm.active) > 3:
            active_summary.append(f"... and {len(tm.active) - 3} more")
            
        embed.add_field(
            name=f"🎯 Active Alerts ({len(tm.active)})",
            value="\n".join(active_summary),
            inline=False
        )
    else:
        embed.add_field(
            name="🎯 Active Alerts",
            value="No active alerts",
            inline=False
        )
    
    # Performance metrics
    if hasattr(tm, 'metrics') and tm.metrics:
        stats = tm.metrics.stats()
        embed.add_field(
            name="📊 Performance",
            value=f"""**Signals:** {stats.get('count', 0)} total
**Win Rate:** {stats.get('win_rate_pct', 0):.0f}%
**Avg Win:** +{stats.get('avg_win_pct', 0):.1f}%
**Avg Loss:** {stats.get('avg_loss_pct', 0):.1f}%""",
            inline=True
        )
    
    # Signal environment assessment
    hours_since_signal = hours_since_last_signal()
    signal_env = "🔥 Active" if hours_since_signal < 2 else "🟡 Quiet" if hours_since_signal < 6 else "😴 Dormant"
    
    embed.add_field(
        name="🔍 Signal Environment",
        value=f"""**Status:** {signal_env}
**Last Signal:** {f'{hours_since_signal:.1f}h ago' if _last_alert_time else 'Never'}
**Mode:** {get_current_mode().upper()}""",
        inline=True
    )
    
    embed.set_footer(text=f"v{VERSION} • Use !market for detailed analysis • {get_system_health_emoji()}")
    return embed

# ---------------- Global Singletons ----------------
md = MarketData(CFG.pair, CFG.interval)
sheets = GoogleSheetsIntegration(CFG.sheets_webhook, CFG.sheets_token)
tm = TradeManager(CFG, sheets)
attach_metrics(tm)

# ---------------- Scanner Loop ----------------
_last_alert_time: Optional[datetime] = None

@tasks.loop(seconds=1)
async def _tick_status():
    # status heartbeat in logs to show bot alive
    if random.random() < 0.02:
        log.info(f"Status: scan={CFG.scan_every_seconds}s min_score={CFG.min_score} cd={CFG.cooldown_minutes}m")

@tasks.loop(minutes=30)
async def heartbeat_report():
    """Send regular status updates even when no signals"""
    if CFG.status_channel_id:
        try:
            ch = bot.get_channel(CFG.status_channel_id)
            if ch:
                last_signal_ago = "Never" if not _last_alert_time else f"{(utc_now() - _last_alert_time).total_seconds()//60:.0f}m ago"
                
                df = await md.fetch_ohlc(limit=50)
                current_price = df["close"].iloc[-1] if df is not None and not df.empty else "Unknown"
                
                embed = discord.Embed(
                    title="🔍 Scout Tower Heartbeat",
                    color=discord.Color.green(),
                    description=f"Scanner active • Last signal: {last_signal_ago}",
                    timestamp=utc_now()
                )
                embed.add_field(name="Current ETH Price", value=f"${current_price:.2f}" if isinstance(current_price, (int, float)) else str(current_price), inline=True)
                embed.add_field(name="Mode", value=f"{get_current_mode()} (scan:{CFG.scan_every_seconds}s)", inline=True)
                embed.add_field(name="Score Threshold", value=f"{CFG.min_score}", inline=True)
                embed.add_field(name="System Health", value=get_system_health_emoji(), inline=True)
                embed.set_footer(text=f"v{VERSION} • Heartbeat")
                await ch.send(embed=embed)
        except Exception as e:
            log.error(f"Heartbeat report error: {e}")

@tasks.loop(minutes=15)
async def data_quality_check():
    """Monitor for data issues"""
    try:
        df = await md.fetch_ohlc(limit=10)
        if df is None:
            await send_error_alert("⚠️ **DATA FAILURE**: Cannot fetch OHLC data")
            return
        
        # Check for gaps or anomalies
        time_gaps = df.index.to_series().diff().dt.total_seconds()
        expected_gap = 60  # 1 minute intervals
        
        if time_gaps.max() > expected_gap * 3:
            await send_error_alert(f"⚠️ **DATA GAP**: {time_gaps.max()//60:.0f} minute gap detected")
        
        # Check for price anomalies
        price_changes = df["close"].pct_change().abs()
        if price_changes.max() > 0.10:  # 10% change in 1 minute
            await send_error_alert(f"⚠️ **PRICE ANOMALY**: {price_changes.max()*100:.1f}% change detected")
            
    except Exception as e:
        await send_error_alert(f"⚠️ **MONITOR ERROR**: {str(e)}")

@tasks.loop(hours=1)
async def smart_status_update():
    """Send status updates based on conditions"""
    if CFG.status_channel_id:
        try:
            ch = bot.get_channel(CFG.status_channel_id)
            if ch:
                # Only send if interesting conditions
                should_update = any([
                    len(tm.active) > 0,  # Has active trades
                    hours_since_last_signal() > 4,  # Long quiet period
                    get_system_health_emoji() != "🟢"  # Health issues
                ])
                
                if should_update:
                    embed = await create_status_embed()
                    await ch.send(embed=embed)
        except Exception as e:
            log.error(f"Smart status update error: {e}")

@tasks.loop(seconds=5)
async def scanner():
    global _last_alert_time
    try:
        now = utc_now()
        # rate control
        if _last_alert_time and (now - _last_alert_time).total_seconds() < CFG.scan_every_seconds:
            return

        df = await md.fetch_ohlc(limit=250)
        if df is None or df.empty: return
        df = df.rename(columns={"open":"open","high":"high","low":"low","close":"close","volume":"volume"})
        df["open"] = df["open"].astype(float)
        df["high"] = df["high"].astype(float)
        df["low"]  = df["low"].astype(float)
        df["close"]= df["close"].astype(float)
        df["volume"]=df["volume"].astype(float)
        df = add_indicators(df)

        price_now = float(df["close"].iloc[-1])
        
        # Check for exits on active trades first
        if tm.active:
            exits = await tm.check_exits(price_now)
            for exit_info in exits:
                if CFG.signals_channel_id and not tm.muted:
                    ch = bot.get_channel(CFG.signals_channel_id)
                    if ch:
                        await ch.send(embed=embed_trade_exit(exit_info))
        
        # Check for new signals
        sig = generate_signal(df, price_now)
        if not sig: return

        # cooldown per side simple guard
        if _last_alert_time and (utc_now()-_last_alert_time).total_seconds() < CFG.cooldown_minutes*60:
            return

        t = await tm.open_trade(
            side=sig["side"],
            zone_lo=sig["zone"][0], zone_hi=sig["zone"][1],
            sl=sig["sl"], tp1=sig["tp1"], tp2=sig["tp2"],
            price_now=price_now
        )
        _last_alert_time = utc_now()

        # Discord post
        if CFG.signals_channel_id and not tm.muted:
            ch = bot.get_channel(CFG.signals_channel_id)
            if ch:
                await ch.send(embed=embed_trade_open(t, price_now))

    except Exception as e:
        log.error(f"Scanner error: {e}")

# ---------------- Discord Commands ----------------
@bot.event
async def on_ready():
    log.info(f"Discord logged in as {bot.user}")
    if CFG.startup_channel_id:
        ch = bot.get_channel(CFG.startup_channel_id)
        if ch: 
            embed = await create_online_embed()
            await ch.send(embed=embed)
    _tick_status.start()
    scanner.start()
    heartbeat_report.start()
    data_quality_check.start()
    smart_status_update.start()

@bot.command(name="version")
async def version_cmd(ctx):
    await ctx.reply(f"Scout Tower v{VERSION} | scan={CFG.scan_every_seconds}s | min_score={CFG.min_score} | cd={CFG.cooldown_minutes}m")

@bot.command(name="status")
async def status_cmd(ctx):
    embed = await create_status_embed()
    await ctx.reply(embed=embed)

@bot.command(name="ping")
async def ping_cmd(ctx):
    await ctx.reply("Pong!")

@bot.command(name="mute")
async def mute_cmd(ctx):
    tm.muted = True
    await ctx.reply("Alerts muted.")

@bot.command(name="unmute")
async def unmute_cmd(ctx):
    tm.muted = False
    await ctx.reply("Alerts unmuted.")

@bot.command(name="active")
async def active_cmd(ctx):
    snap = tm.snapshot_active()
    if not snap:
        return await ctx.reply("No active trades.")
    await ctx.reply("\n".join([json.dumps(x) for x in snap]))

@bot.command(name="close")
async def close_cmd(ctx, trade_id: str, price: Optional[float] = None):
    try:
        price = float(price) if price is not None else None
    except Exception:
        price = None
    if not price:
        return await ctx.reply("Usage: !close <id> <price>")
    ok = await tm.close_trade(trade_id, price, reason="manual", exit_type="MANUAL")
    await ctx.reply("Closed." if ok else "Trade not found.")

@bot.command(name="result")
async def cmd_result(ctx, trade_id: str, price: float, *, notes: str = ""):
    ok = await tm.close_trade(trade_id, float(price), reason=f"manual: {notes or ''}", exit_type="MANUAL")
    await ctx.reply("Logged." if ok else "Trade not found.")

@bot.command(name="exits")
async def exits_cmd(ctx):
    """Show exit monitoring status for active trades"""
    if not tm.active:
        return await ctx.reply("No active trades to monitor.")
    
    embed = discord.Embed(
        title="🎯 Exit Monitoring Status", 
        color=discord.Color.blue(),
        timestamp=utc_now()
    )
    
    try:
        df = await md.fetch_ohlc(limit=5)
        current_price = df["close"].iloc[-1] if df is not None and not df.empty else 0
        embed.add_field(name="Current Price", value=f"${current_price:.2f}", inline=False)
    except:
        current_price = 0
    
    for trade in list(tm.active.values())[:5]:  # Show max 5 trades
        status_text = []
        
        if trade.side == "LONG":
            if not trade.entry_confirmed:
                status_text.append(f"⏳ Waiting for entry ({trade.zone_lo:.2f}-{trade.zone_hi:.2f})")
            else:
                status_text.append("✅ Entry confirmed")
                
            if trade.tp1_hit:
                status_text.append("🎯 TP1 hit")
            elif trade.entry_confirmed:
                distance_tp1 = ((trade.tp1 - current_price) / current_price * 100) if current_price > 0 else 0
                status_text.append(f"🎯 TP1: ${trade.tp1:.2f} ({distance_tp1:+.1f}%)")
                
            if trade.tp2_hit:
                status_text.append("🎯 TP2 hit")
            elif trade.entry_confirmed:
                distance_tp2 = ((trade.tp2 - current_price) / current_price * 100) if current_price > 0 else 0
                status_text.append(f"🎯 TP2: ${trade.tp2:.2f} ({distance_tp2:+.1f}%)")
                
            if trade.sl_hit:
                status_text.append("🔴 SL hit")
            elif trade.entry_confirmed:
                distance_sl = ((trade.sl - current_price) / current_price * 100) if current_price > 0 else 0
                status_text.append(f"🛡️ SL: ${trade.sl:.2f} ({distance_sl:+.1f}%)")
        
        elif trade.side == "SHORT":
            if not trade.entry_confirmed:
                status_text.append(f"⏳ Waiting for entry ({trade.zone_lo:.2f}-{trade.zone_hi:.2f})")
            else:
                status_text.append("✅ Entry confirmed")
                
            if trade.tp1_hit:
                status_text.append("🎯 TP1 hit")
            elif trade.entry_confirmed:
                distance_tp1 = ((current_price - trade.tp1) / current_price * 100) if current_price > 0 else 0
                status_text.append(f"🎯 TP1: ${trade.tp1:.2f} ({distance_tp1:+.1f}%)")
                
            if trade.tp2_hit:
                status_text.append("🎯 TP2 hit")
            elif trade.entry_confirmed:
                distance_tp2 = ((current_price - trade.tp2) / current_price * 100) if current_price > 0 else 0
                status_text.append(f"🎯 TP2: ${trade.tp2:.2f} ({distance_tp2:+.1f}%)")
                
            if trade.sl_hit:
                status_text.append("🔴 SL hit")
            elif trade.entry_confirmed:
                distance_sl = ((current_price - trade.sl) / current_price * 100) if current_price > 0 else 0
                status_text.append(f"🛡️ SL: ${trade.sl:.2f} ({distance_sl:+.1f}%)")
        
        embed.add_field(
            name=f"{trade.id} {trade.side}",
            value="\n".join(status_text),
            inline=False
        )
    
    if len(tm.active) > 5:
        embed.add_field(name="Note", value=f"... and {len(tm.active) - 5} more trades", inline=False)
    
    embed.set_footer(text=f"v{VERSION} • Monitoring every 5 seconds")
    await ctx.reply(embed=embed)

# ---- Enhanced Commands ----
@bot.command(name="market")
async def market_status(ctx):
    """Comprehensive market overview"""
    df = await md.fetch_ohlc(limit=100)
    if df is None:
        return await ctx.reply("❌ Cannot fetch market data")
    
    df = add_indicators(df)
    regime = detect_market_regime(df)
    near_misses = await check_near_misses(df)
    drought_warning = check_signal_drought()
    
    embed = discord.Embed(title="📊 Market Status Report", color=discord.Color.blue(), timestamp=utc_now())
    embed.add_field(name="Market Regime", value=regime, inline=False)
    
    if near_misses:
        embed.add_field(name="Near Misses", value="\n".join(near_misses), inline=False)
    
    if drought_warning:
        embed.add_field(name="Alert", value=drought_warning, inline=False)
    
    current = df.iloc[-1]
    embed.add_field(name="Price", value=f"${current['close']:.2f}", inline=True)
    embed.add_field(name="RSI", value=f"{current['rsi']:.1f}", inline=True)
    embed.add_field(name="Volatility", value=f"{current['atr']/current['close']*100:.2f}%", inline=True)
    
    await ctx.reply(embed=embed)

@bot.command(name="why")
async def why_no_signals(ctx):
    analysis = await market_analysis_report()
    await ctx.reply(analysis)

@bot.command(name="debug")
async def debug_status(ctx):
    """Technical debugging information"""
    info = []
    info.append(f"Scanner running: {scanner.is_running()}")
    info.append(f"Last successful fetch: {_last_successful_fetch}")
    info.append(f"Last fetch error: {_last_fetch_error}")
    info.append(f"Data provider: {CFG.provider}")
    info.append(f"Active tasks: heartbeat={heartbeat_report.is_running()}, data_check={data_quality_check.is_running()}")
    
    # Test data fetch
    try:
        df = await md.fetch_ohlc(limit=5)
        info.append(f"Data fetch: ✅ Latest: {df.index[-1] if df is not None and not df.empty else 'Failed'}")
    except Exception as e:
        info.append(f"Data fetch: ❌ {str(e)}")
    
    await ctx.reply("```\n" + "\n".join(info) + "\n```")

# ---- Mode Commands ----
@bot.command(name="mode")
async def cmd_mode(ctx, *, level: str = None):
    if not level:
        s = current_mode_snapshot()
        return await ctx.reply(f"Current mode → scan:{s['scan']}s | min_score:{s['min_score']} | cooldown:{s['cooldown']}m")
    try:
        applied = await apply_preset(level)
        await ctx.reply(f"Mode set → {applied['mode']} | scan:{applied['scan']}s | min_score:{applied['min_score']} | cooldown:{applied['cooldown']}m")
    except Exception as e:
        await ctx.reply(f"Error: {e}")

@bot.command(name="showmode")
async def cmd_showmode(ctx):
    s = current_mode_snapshot()
    await ctx.reply(f"scan:{s['scan']}s | min_score:{s['min_score']} | cooldown:{s['cooldown']}m")

# ---- CSV/Stats Commands ----
def _tail_csv(path: Path, n: int = 10) -> List[dict]:
    if not path.exists(): return []
    rows = list(csv.DictReader(path.open()))
    return rows[-n:]

@bot.command(name="stats")
async def stats_cmd(ctx):
    s = tm.metrics.stats() if tm.metrics else {}
    await ctx.reply(json.dumps(s, indent=2))

@bot.command(name="csvstats")
async def csvstats_cmd(ctx):
    def _fmt(path: Path):
        return f"{path.name}: {'exists' if path.exists() else 'missing'} {path.stat().st_size if path.exists() else 0} bytes"
    await ctx.reply("\n".join([_fmt(ALERTS_CSV), _fmt(DECISIONS_CSV), _fmt(FILLS_CSV)]))

@bot.command(name="lastalerts")
async def lastalerts_cmd(ctx, n: int = 5):
    rows = _tail_csv(ALERTS_CSV, n)
    if not rows: return await ctx.reply("No alerts yet.")
    await ctx.reply("\n".join([json.dumps(r) for r in rows]))

@bot.command(name="lastfills")
async def lastfills_cmd(ctx, n: int = 5):
    rows = _tail_csv(FILLS_CSV, n)
    if not rows: return await ctx.reply("No fills yet.")
    await ctx.reply("\n".join([json.dumps(r) for r in rows]))

@bot.command(name="exportday")
async def exportday_cmd(ctx, day: str = "today"):
    if not FILLS_CSV.exists(): return await ctx.reply("No fills.csv")
    day0 = utc_now().strftime("%Y-%m-%d") if day=="today" else day
    out = [r for r in csv.DictReader(FILLS_CSV.open()) if str(r.get("time","")).startswith(day0)]
    if not out: return await ctx.reply(f"No fills for {day0}")
    # attach as file
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=list(out[0].keys()))
    w.writeheader()
    w.writerows(out)
    buf.seek(0)
    await ctx.reply(file=discord.File(fp=io.BytesIO(buf.read().encode()), filename=f"fills_{day0}.csv"))

# ---- Sheets Utility Commands ----
@bot.command(name="testsheets")
async def testsheets_cmd(ctx):
    ok = await sheets.post({"action":"test", "time": ts()})
    await ctx.reply("Sheets ok" if ok else "Sheets failed")

@bot.command(name="checksheets")
async def checksheets_cmd(ctx):
    await ctx.reply("Webhook set" if CFG.sheets_webhook else "No webhook configured")

@bot.command(name="rehydrate")
async def rehydrate_cmd(ctx):
    rows = await sheets.rehydrate_open_trades()
    await ctx.reply(f"rehydrate: {len(rows)} rows")

@bot.command(name="getcsv")
async def getcsv_cmd(ctx):
    if not ALERTS_CSV.exists(): return await ctx.reply("No alerts.csv yet")
    await ctx.reply(file=discord.File(str(ALERTS_CSV)))

@bot.command(name="pushtosheet")
async def pushtosheet_cmd(ctx):
    if not ALERTS_CSV.exists(): return await ctx.reply("No alerts.csv")
    rows = list(csv.DictReader(ALERTS_CSV.open()))
    ok = True
    for r in rows[-10:]:
        ok = ok and await sheets.post({"action":"entry", **r})
    await ctx.reply("Pushed last 10 alerts." if ok else "Push encountered errors.")

@bot.command(name="sheetping")
async def sheetping_cmd(ctx):
    ok = await sheets.post({"action":"ping","time":ts()})
    await ctx.reply("pong" if ok else "no pong")

# ---- Help ----
@bot.command(name="help")
async def help_command(ctx):
    cmds = [
        "**Core:** !version, !status, !ping, !mute, !unmute",
        "**Modes:** !mode <testing|level1|level2|level3>, !showmode",
        "**Trades:** !active, !exits, !close <id> <price>, !result <id> <price> [notes]",
        "**Analysis:** !market, !why, !debug, !stats",
        "**Data:** !csvstats, !lastalerts [n], !lastfills [n], !exportday [YYYY-MM-DD|today]",
        "**Sheets:** !testsheets, !checksheets, !rehydrate, !getcsv, !pushtosheet, !sheetping",
    ]
    await ctx.reply("\n".join(cmds))

# ---------------- Flask Health/Metrics ----------------
app = Flask(__name__)

@app.get("/health")
def _health():
    return jsonify({
        "ok": True,
        "name": f"Scout Tower v{VERSION}",
        "time_utc": utc_now().strftime("%Y-%m-%d %H:%M:%S"),
        "scanner_running": scanner.is_running(),
        "scan": CFG.scan_every_seconds,
        "min_score": CFG.min_score,
        "cooldown": CFG.cooldown_minutes,
        "system_health": get_system_health_emoji(),
        "last_successful_fetch": _last_successful_fetch.isoformat() if _last_successful_fetch else None,
        "last_fetch_error": _last_fetch_error
    })

@app.get("/metrics")
def _metrics():
    s = tm.metrics.stats() if tm.metrics else {}
    return jsonify({
        "ok": True, 
        "metrics": s,
        "active_trades": len(tm.active),
        "hours_since_signal": hours_since_last_signal(),
        "system_health": get_system_health_emoji()
    })

def run_flask():
    app.run(host="0.0.0.0", port=CFG.port, debug=False)

# ---------------- Main ----------------
def main():
    threading.Thread(target=run_flask, daemon=True).start()
    token = CFG.token
    if not token:
        log.error("DISCORD_TOKEN missing.")
        return
    bot.run(token)

if __name__ == "__main__":
    main()