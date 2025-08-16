
# ===============================================
# Scout Tower - Unified ETH Intraday Bot (v1.0)
# Feature-rich single-file for Render deployment
# -----------------------------------------------
# - Multi-exchange data: Binance primary, Kraken fallback (REST; no keys needed)
# - Indicators: EMA(20/50), RSI(14), ATR(14), VWAP, Donchian(20)
# - Signal Engine: breakout / pullback / trend continuation with scoring & cooldowns
# - Google Sheets: write entries/updates, rehydrate OPEN trades at startup
# - Discord: alerts channel, status channel, error reporting, basic commands
# - CSV logging: alerts.csv, decisions.csv, fills.csv under /data (auto-created)
# - Flask: health endpoint for uptime checks
# - Backtest: simple CLI stub (optional)
# - Timezone: Central (CT) and UTC in embeds
#
# ENV REQUIRED:
#   DISCORD_TOKEN                 - Discord bot token
#   GOOGLE_SHEETS_WEBHOOK         - Apps Script /exec URL
#
# ENV OPTIONAL (with sensible defaults / pre-wired IDs):
#   SIGNALS_CHANNEL_ID            - defaults to ⚔️ battle-signals (1399532925279666278)
#   STATUS_CHANNEL_ID             - defaults to 🏰 eth-battleground (1399532442075005038)
#   ERRORS_CHANNEL_ID             - defaults to 📜 scrolls-of-the-order (1399067396488302623)
#   PROVIDER                      - 'binance' (default) or 'kraken'
#   USE_WEBSOCKET                 - '0' or '1' (reserved; REST used here)
#   SYMBOL_BINANCE                - 'ETHUSDT'
#   SYMBOL_KRAKEN                 - 'ETHUSDT' (Kraken uses pair mapping internally)
#   SCAN_EVERY_SECONDS            - default 60
#   ALERT_COOLDOWN_MINUTES        - default 15
#   MIN_SIGNAL_SCORE              - default 3.0 (0-6 scale)
#   RISK_ATR_MULT                 - default 1.0
#   TP1_R_MULTIPLE                - default 1.5
#   TP2_R_MULTIPLE                - default 3.0
#   TZ_NAME                       - 'America/Chicago' (CT) for headers
#   SHEETS_TOKEN                  - optional shared secret; sent in x-app-secret and GET key
#
# Run locally:  python main.py
# Render: set ENV vars; uses Flask on PORT
# ===============================================

import os, sys, json, math, csv, asyncio, aiohttp, logging, time, pathlib
from dataclasses import dataclass, field, asdict
from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime, timezone
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
    signals_channel_id: int = field(default_factory=lambda: getenv_int("SIGNALS_CHANNEL_ID", 1406315936989970485))
    status_channel_id:  int = field(default_factory=lambda: getenv_int("STATUS_CHANNEL_ID",  1406315936989970485))
    errors_channel_id:  int = field(default_factory=lambda: getenv_int("ERRORS_CHANNEL_ID",  1406315936989970485))

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

    tz_name: str = field(default_factory=lambda: os.getenv("TZ_NAME", "America/Chicago").strip())
    port: int = field(default_factory=lambda: getenv_int("PORT", 10000))

    def validate(self):
        if not self.token:
            raise RuntimeError("DISCORD_TOKEN is required")
        if not self.sheets_webhook:
            raise RuntimeError("GOOGLE_SHEETS_WEBHOOK is required")

CFG = Config()
CFG.validate()

# ---------------- Utility: TZ formatting -----------------
def fmt_dt(dt: datetime, tzname: str) -> str:
    # For display only; not converting to local tz system-wide
    try:
        import pytz
        tz = pytz.timezone(tzname)
        return dt.astimezone(tz).strftime("%Y-%m-%d %H:%M:%S %Z")
    except Exception:
        return dt.strftime("%Y-%m-%d %H:%M:%S")

def utc_now() -> datetime:
    return datetime.now(timezone.utc)

# ---------------- Data Provider (REST) -------------------
class MarketDataProvider:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.session: Optional[aiohttp.ClientSession] = None

    async def start(self):
        if not self.session:
            timeout = aiohttp.ClientTimeout(total=15)
            self.session = aiohttp.ClientSession(timeout=timeout)

    async def stop(self):
        if self.session:
            await self.session.close()
            self.session = None

    async def fetch_binance_klines(self, symbol: str, limit: int = 200) -> Optional[pd.DataFrame]:
        # 1m klines
        url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval=1m&limit={limit}"
        async with self.session.get(url) as r:
            if r.status != 200:
                text = await r.text()
                log.warning(f"Binance klines error {r.status}: {text}")
                return None
            raw = await r.json()
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
        # 1m OHLC
        url = f"https://api.kraken.com/0/public/OHLC?pair={pair}&interval=1"
        async with self.session.get(url) as r:
            if r.status != 200:
                text = await r.text()
                log.warning(f"Kraken OHLC error {r.status}: {text}")
                return None
            raw = await r.json()
        if raw.get("error"):
            log.warning(f"Kraken error: {raw['error']}")
            return None
        result = raw.get("result", {})
        # pair key unknown; take first
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
        await self.start()
        try:
            if self.cfg.provider == "binance":
                df = await self.fetch_binance_klines(self.cfg.symbol_binance, limit=limit)
                if df is not None and len(df) >= 50:
                    return df
                # fallback
                df2 = await self.fetch_kraken_ohlc(self.cfg.symbol_kraken)
                return df2
            else:
                df = await self.fetch_kraken_ohlc(self.cfg.symbol_kraken)
                if df is not None and len(df) >= 50:
                    return df
                df2 = await self.fetch_binance_klines(self.cfg.symbol_binance, limit=limit)
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

class GoogleSheetsIntegration:
    def __init__(self, url: str, token: Optional[str] = None):
        self.url = url
        self.token = token

    async def _post(self, session: aiohttp.ClientSession, payload: Dict[str, Any]) -> Dict[str, Any]:
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

    async def _get(self, session: aiohttp.ClientSession, params: Dict[str, Any]) -> Dict[str, Any]:
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

    async def write_entry(self, session: aiohttp.ClientSession, t: TradeData) -> Dict[str, Any]:
        payload = asdict(t)
        return await self._post(session, payload)

    async def update_exit(self, session: aiohttp.ClientSession, trade_id: str, exit_price: float, exit_reason: str, pnl_pct: float) -> Dict[str, Any]:
        payload = {
            "action":"update",
            "id": trade_id,
            "exit_price": exit_price,
            "exit_reason": exit_reason,
            "pnl_pct": pnl_pct,
            "status": "CLOSED"
        }
        return await self._post(session, payload)

    async def rehydrate_open_trades(self, session: aiohttp.ClientSession) -> List[TradeData]:
        data = await self._get(session, {"action":"open"})
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
                    score=float(r.get("Original Score")) if r.get("Original Score") else None
                ))
            except Exception as e:
                log.warning(f"rehydrate row parse error: {e}")
        return out

# -------------- Trade Manager & Storage -----------------
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

class TradeManager:
    def __init__(self, cfg: Config, sheets: GoogleSheetsIntegration):
        self.cfg = cfg
        self.sheets = sheets
        self.active: Dict[str, TradeData] = {}
        self.cooldown_until: Dict[str, float] = {}  # side -> unix time
        self.session: Optional[aiohttp.ClientSession] = None

    async def start(self):
        if not self.session:
            timeout = aiohttp.ClientTimeout(total=20)
            self.session = aiohttp.ClientSession(timeout=timeout)

    async def stop(self):
        if self.session:
            await self.session.close()
            self.session = None

    async def rehydrate(self):
        await self.start()
        rows = await self.sheets.rehydrate_open_trades(self.session)
        for t in rows:
            self.active[t.id] = t
        log.info(f"Rehydrated {len(rows)} open trades from Sheets.")

    def on_cooldown(self, side: str) -> bool:
        ts = self.cooldown_until.get(side, 0)
        return time.time() < ts

    def set_cooldown(self, side: str):
        self.cooldown_until[side] = time.time() + self.cfg.alert_cooldown_minutes * 60

    async def open_trade(self, t: TradeData):
        await self.start()
        self.active[t.id] = t
        # CSV log
        append_csv(ALERTS_CSV, {**asdict(t), "opened_at": utc_now().isoformat()})
        # Sheets
        await self.sheets.write_entry(self.session, t)

    async def close_trade(self, trade_id: str, exit_price: float, reason: str):
        t = self.active.get(trade_id)
        if not t:
            return
        # simple PnL pct
        direction = 1 if t.side.upper() == "LONG" else -1
        pnl_pct = direction * (exit_price - t.entry_price) / t.entry_price * 100.0
        # Sheets & CSV
        await self.sheets.update_exit(self.session, trade_id, exit_price, reason, pnl_pct)
        append_csv(FILLS_CSV, {"id": trade_id, "exit_price": exit_price, "reason": reason, "pnl_pct": pnl_pct, "time": utc_now().isoformat()})
        # mark closed
        t.status = "CLOSED"
        self.active.pop(trade_id, None)

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

class EnhancedSignalEngine:
    """
    Simple but effective scoring:
    - Breakout above Donchian upper + EMA20>EMA50 + close>VWAP => +1 each
    - Pullback to EMA20 in uptrend (EMA20>EMA50, RSI>50) with bounce => +1 each
    - Trend continuation: price above EMA20/50, RSI>55, ATR rising => +1 each
    Volume proxy via range vs ATR; more range => +0.5
    """
    def __init__(self, cfg: Config):
        self.cfg = cfg

    def _risk_targets(self, price: float, atr_val: float, side: str) -> Tuple[float, float]:
        # Stop at ATR * mult; TPs at R multiples
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
            score_long += 1.0; reason_long_parts.append("Breakout > Donchian & VWAP in uptrend")

        breakout_short = latest["close"] < latest["dc_l"] and downtrend and latest["close"] < latest["vwap"]
        if breakout_short:
            score_short += 1.0; reason_short_parts.append("Breakdown < Donchian & VWAP in downtrend")

        # Pullback: bounce from EMA20 in trend with RSI bias
        bounced_long = (prev["close"] < prev["ema20"]) and (latest["close"] > latest["ema20"]) and uptrend and latest["rsi"] > 50
        if bounced_long:
            score_long += 1.0; reason_long_parts.append("Pullback bounce on EMA20 with RSI>50")

        bounced_short = (prev["close"] > prev["ema20"]) and (latest["close"] < latest["ema20"]) and downtrend and latest["rsi"] < 50
        if bounced_short:
            score_short += 1.0; reason_short_parts.append("Pullback bounce under EMA20 with RSI<50")

        # Continuation momentum
        cont_long = uptrend and latest["close"] > latest["ema20"] and latest["rsi"] > 55 and latest["atr"] > df["atr"].iloc[-15]
        if cont_long:
            score_long += 1.0; reason_long_parts.append("Continuation: >EMA20, RSI>55, ATR rising")

        cont_short = downtrend and latest["close"] < latest["ema20"] and latest["rsi"] < 45 and latest["atr"] > df["atr"].iloc[-15]
        if cont_short:
            score_short += 1.0; reason_short_parts.append("Continuation: <EMA20, RSI<45, ATR rising")

        # Volume proxy: body vs range vs ATR
        body = abs(latest["close"] - latest["open"])
        rng  = latest["high"] - latest["low"]
        atrv = latest["atr"]
        if rng > 0 and atrv > 0:
            body_ratio = body / max(rng, 1e-9)
            atr_ratio  = rng / atrv
            if body_ratio > 0.5:  # strong body
                score_long += 0.5 if latest["close"] > latest["open"] else 0.0
                score_short+= 0.5 if latest["close"] < latest["open"] else 0.0
            if atr_ratio > 1.1:
                score_long += 0.5; score_short += 0.5

        # Decide
        side = None
        score = 0.0
        reasons = ""
        price = float(latest["close"])
        if score_long >= score_short and score_long >= self.cfg.min_signal_score:
            side = "LONG"; score = score_long; reasons = " | ".join(reason_long_parts)
        elif score_short > score_long and score_short >= self.cfg.min_signal_score:
            side = "SHORT"; score = score_short; reasons = " | ".join(reason_short_parts)
        else:
            return None

        stop, tp1, tp2 = self._risk_targets(price, float(atrv), side)
        return Signal(side=side, score=float(round(score,2)), entry=price, stop=float(stop), tp1=float(tp1), tp2=float(tp2), reason=reasons or "Multi-factor confluence")

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

    async def safe_send(self, channel_id: int, **embed_kwargs):
        ch = self.channel(channel_id)
        if not ch:
            try:
                ch = await self.client.fetch_channel(channel_id)
            except Exception as fe:
                log.error(f"fetch_channel failed for {channel_id}: {fe}")
                ch = None
        if not ch:
            log.warning(f"Channel {channel_id} unavailable – check bot permissions and that it’s in the guild.")
            return
        embed = discord.Embed(**embed_kwargs)
        try:
            await ch.send(embed=embed)
        except Exception as e:
            log.error(f"send error: {e} – Missing Access usually means wrong channel ID or no Send Messages permission for the bot role.")

# -------------- Embeds -----------------
def build_trade_embed(t: TradeData, cfg: Config) -> discord.Embed:
    now_utc = utc_now()
    ct_str  = fmt_dt(now_utc, cfg.tz_name)
    embed = discord.Embed(
        title=f"⚔️ Battle Signal – {t.pair} ({t.side})",
        description=f"**Entry**: {t.entry_price:.2f}\n**SL**: {t.stop_loss:.2f}\n**TP1**: {t.take_profit_1:.2f}\n**TP2**: {t.take_profit_2:.2f}\n\n**Why**: {t.level_name or t.confidence or 'Signal engine confluence'}",
        color=0x4CAF50 if t.side.upper()=="LONG" else 0xF44336,
        timestamp=now_utc
    )
    embed.add_field(name="Trade ID", value=t.id, inline=True)
    if t.score is not None:
        embed.add_field(name="Score", value=f"{t.score:.2f}", inline=True)
    if t.level_price:
        embed.add_field(name="Level", value=f"{t.level_name or ''} {t.level_price}", inline=True)
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

def build_status_embed(cfg: Config, provider_ok: bool, last_price: Optional[float]) -> discord.Embed:
    now_utc = utc_now()
    ct_str  = fmt_dt(now_utc, cfg.tz_name)
    desc = f"Provider: **{cfg.provider.upper()}** (REST)\nScan: **{cfg.scan_every_seconds}s**\nMin Score: **{cfg.min_signal_score}**\nCooldown: **{cfg.alert_cooldown_minutes}m**\nATR Mult: **{cfg.risk_atr_mult}**\nTP1/TP2 R: **{cfg.tp1_r_multiple}/{cfg.tp2_r_multiple}**"
    if last_price:
        desc += f"\n\nCurrent Price: **{last_price:.2f}**"
    embed = discord.Embed(title="🛰 Scout Tower Status", description=desc, color=0x00BCD4, timestamp=now_utc)
    embed.set_footer(text=f"CT: {ct_str} • UTC: {now_utc.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    return embed

# -------------- Globals -----------------
mdp   = MarketDataProvider(CFG)
sheets= GoogleSheetsIntegration(CFG.sheets_webhook, CFG.sheets_token)
tm    = TradeManager(CFG, sheets)
engine= EnhancedSignalEngine(CFG)
dio   = DiscordIO(CFG, bot)

_last_price: Optional[float] = None

# -------------- Commands -----------------
@bot.command()
async def ping(ctx):
    await ctx.reply("Pong.")

@bot.command()
async def status(ctx):
    emb = build_status_embed(CFG, provider_ok=True, last_price=_last_price)
    await ctx.send(embed=emb)

@bot.command()
async def force(ctx):
    """Force-scan now and print score details (diagnostic)."""
    try:
        df = await mdp.fetch_ohlc(limit=200)
        if df is None or df.empty:
            await ctx.reply("No data")
            return
        df = add_indicators(df)
        sig = engine.generate(df)
        if sig:
            await ctx.reply(f"Signal {sig.side} score={sig.score} entry={sig.entry:.2f} sl={sig.stop:.2f} tp1={sig.tp1:.2f} tp2={sig.tp2:.2f}\n{sig.reason}")
        else:
            await ctx.reply("No signal above threshold")
    except Exception as e:
        await ctx.reply(f"force error: {e}")

@bot.command()
async def posttest(ctx):
    """Send a test embed to the signals channel to confirm permissions."""
    t = TradeData(
        id="TEST-123",
        pair="ETH/USDT",
        side="LONG",
        entry_price=2000,
        stop_loss=1950,
        take_profit_1=2030,
        take_profit_2=2060,
        confidence="Test",
        knight="Sir Leonis",
        score=5.0,
        level_name="Test embed"
    )
    emb = build_trade_embed(t, CFG)
    await dio.safe_send(CFG.signals_channel_id,
        title=emb.title, description=emb.description,
        color=emb.color, timestamp=emb.timestamp)
    await ctx.reply(f"Posted a test embed to channel {CFG.signals_channel_id}. If you don't see it, check bot permissions.")

# -------------- On Ready -----------------
@bot.event
async def on_ready():
    log.info(f"Discord logged in as {bot.user}")
    await mdp.start()
    await tm.start()
    await tm.rehydrate()
    if not scanner.is_running():
        scanner.start()
    if not daily_summary.is_running():
        daily_summary.start()

# -------------- Scanner Loop -----------------
@tasks.loop(seconds=CFG.scan_every_seconds)
async def scanner():
    global _last_price
    try:
        df = await mdp.fetch_ohlc(limit=200)
        if df is None or df.empty:
            return
        df = add_indicators(df)
        latest = df.iloc[-1]
        _last_price = float(latest["close"])

        sig = engine.generate(df)
        if not sig:
            return

        # Cooldown per side
        if tm.on_cooldown(sig.side.upper()):
            return

        # Build trade
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
            level_name=sig.reason
        )
        await tm.open_trade(t)

        # Send embed
        emb = build_trade_embed(t, CFG)
        await dio.safe_send(CFG.signals_channel_id, title=emb.title, description=emb.description, color=emb.color, timestamp=emb.timestamp)
        # Cooldown set
        tm.set_cooldown(sig.side.upper())

    except Exception as e:
        log.error(f"scanner error: {e}")
        emb = build_error_embed(f"Scanner error: {e}", CFG)
        await dio.safe_send(CFG.errors_channel_id, title=emb.title, description=emb.description, color=emb.color, timestamp=emb.timestamp)

# -------------- Daily Summary -----------------
@tasks.loop(hours=24)
async def daily_summary():
    try:
        emb = build_status_embed(CFG, provider_ok=True, last_price=_last_price)
        await dio.safe_send(CFG.status_channel_id, title=emb.title, description=emb.description, color=emb.color, timestamp=emb.timestamp)
    except Exception as e:
        log.error(f"daily_summary error: {e}")

# -------------- Flask Health -----------------
from flask import Flask, jsonify
app = Flask(__name__)

@app.route("/")
def root():
    return jsonify({"ok": True, "bot":"Scout Tower", "time": utc_now().isoformat()})

def run_flask():
    app.run(host="0.0.0.0", port=CFG.port, debug=False, use_reloader=False)

# -------------- Entrypoint -----------------
def main():
    # Start Flask in thread
    import threading
    threading.Thread(target=run_flask, daemon=True).start()
    # Start Discord
    bot.run(CFG.token)

if __name__ == "__main__":
    main()
