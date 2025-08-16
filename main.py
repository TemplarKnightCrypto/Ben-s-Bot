# ===============================================
# Scout Tower - Enhanced ETH Alert Bot (v3.3.15 MERGED)
# - Engine: Donchian breakout + EMA trend + RSI/VWAP/ATR filters
# - Zones: Entry band, SL/TP from ATR + structure
# - Providers: Binance primary, Kraken fallback
# - Discord: commands (modes, stats, csv, active/close/result, sheets utils)
# - Sheets: webhook write + rehydrate of open alerts
# - CSV: alerts.csv / decisions.csv / fills.csv
# - Flask: /health and /metrics
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

VERSION = "3.3.15"

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
    return out.fillna(method="bfill").fillna(50.0)

def atr(df: pd.DataFrame, n: int = 14) -> pd.Series:
    h, l, c = df["high"], df["low"], df["close"]
    tr = np.maximum(h - l, np.maximum(abs(h - c.shift(1)), abs(l - c.shift(1))))
    return pd.Series(tr).rolling(n).mean().fillna(method="bfill")

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
        async with aiohttp.ClientSession() as session:
            try:
                return await self.fetch_binance(session, limit=limit)
            except Exception as e:
                log.warning(f"Binance failed: {e}; trying Kraken...")
                try:
                    return await self.fetch_kraken(session, limit=limit)
                except Exception as e2:
                    log.error(f"Kraken failed: {e2}")
                    return None

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

    async def close_trade(self, trade_id: str, exit_price: float, reason: str = "manual"):
        t = self.active.pop(trade_id, None)
        if not t: return False
        t.status = "CLOSED"
        pnl_pct = pct(exit_price, (t.zone_lo+t.zone_hi)/2.0) * (1 if t.side=="LONG" else -1)
        append_csv(FILLS_CSV, {
            "id": t.id, "time": ts(), "pair": t.pair, "side": t.side,
            "exit_price": exit_price, "reason": reason, "pnl_pct": round(pnl_pct,2)
        })
        asyncio.create_task(self.sheets.post({"action":"update", "id": t.id, "status":"CLOSED",
                                              "exit_price": exit_price, "exit_reason": reason, "pnl_pct": round(pnl_pct,2)}))
        return True

    def snapshot_active(self) -> List[dict]:
        out = []
        for t in self.active.values():
            out.append({
                "id": t.id, "side": t.side, "zone": f"{t.zone_lo:.2f}-{t.zone_hi:.2f}",
                "sl": round(t.sl,2), "tp1": round(t.tp1,2), "tp2": round(t.tp2,2), "opened": t.opened_at
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
    e.set_footer(text=f"v{VERSION} • {utc_now().strftime('%Y-%m-%d %H:%M:%S')} UTC")
    return e

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
        if ch: await ch.send(f"🗼 Scout Tower v{VERSION} is online.")
    _tick_status.start()
    scanner.start()

@bot.command(name="version")
async def version_cmd(ctx):
    await ctx.reply(f"Scout Tower v{VERSION} | scan={CFG.scan_every_seconds}s | min_score={CFG.min_score} | cd={CFG.cooldown_minutes}m")

@bot.command(name="status")
async def status_cmd(ctx):
    snap = tm.snapshot_active()
    lines = [f"{x['id']} {x['side']} zone:{x['zone']} SL:{x['sl']} TP1:{x['tp1']} TP2:{x['tp2']} opened:{x['opened']}" for x in snap]
    text = "No active trades." if not lines else "\n".join(lines)
    await ctx.reply(f"**Active**\n{text}")

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
    ok = await tm.close_trade(trade_id, price, reason="manual")
    await ctx.reply("Closed." if ok else "Trade not found.")

@bot.command(name="result")
async def cmd_result(ctx, trade_id: str, price: float, *, notes: str = ""):
    ok = await tm.close_trade(trade_id, float(price), reason=f"manual: {notes or ''}")
    await ctx.reply("Logged." if ok else "Trade not found.")

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

# ---- Sheets Utility Commands (stubs preserved) ----
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
        "!version, !status, !ping, !mute, !unmute",
        "!mode <testing|level1|level2|level3>, !showmode",
        "!active, !close <id> <price>, !result <id> <price> [notes]",
        "!stats, !csvstats, !lastalerts [n], !lastfills [n], !exportday [YYYY-MM-DD|today]",
        "!testsheets, !checksheets, !rehydrate, !getcsv, !pushtosheet, !sheetping",
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
        "cooldown": CFG.cooldown_minutes
    })

@app.get("/metrics")
def _metrics():
    s = tm.metrics.stats() if tm.metrics else {}
    return jsonify({"ok": True, "metrics": s})

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
