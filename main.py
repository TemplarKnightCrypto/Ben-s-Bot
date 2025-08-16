# ===============================================
# Scout Tower - Enhanced ETH Alert Bot (v3.3.1, Zones)
# - Adds commands: !checksheets, !rehydrate, !version
# - Keeps entry ZONES, percent SL/TP, no position-size by default
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

VERSION = "3.3.1"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("ScoutTower")

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

    alert_expire_hours: int = field(default_factory=lambda: getenv_int("ALERT_EXPIRE_HOURS", 24))
    position_risk_pct: float = field(default_factory=lambda: getenv_float("POSITION_RISK_PCT", 2.0))

    tz_name: str = field(default_factory=lambda: os.getenv("TZ_NAME", "America/Chicago").strip())
    port: int = field(default_factory=lambda: getenv_int("PORT", 10000))

    # Zones + toggles
    entry_zone_atr_mult: float = field(default_factory=lambda: getenv_float("ENTRY_ZONE_ATR_MULT", 0.25))
    entry_zone_min_pct:  float = field(default_factory=lambda: getenv_float("ENTRY_ZONE_MIN_PCT", 0.15))  # % of price
    show_position_size:  bool  = field(default_factory=lambda: os.getenv("SHOW_POSITION_SIZE","0")=="1")
    account_size_usd:    float = field(default_factory=lambda: getenv_float("ACCOUNT_SIZE_USD", 10000.0))

    def validate(self):
        if not self.token:
            raise RuntimeError("DISCORD_TOKEN is required")
        if not self.sheets_webhook:
            raise RuntimeError("GOOGLE_SHEETS_WEBHOOK is required")

CFG = Config()
CFG.validate()

# ----------- Session Manager -----------
class SessionManager:
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

# ----------- Helpers -----------
def fmt_dt(dt: datetime, tzname: str) -> str:
    try:
        import pytz
        tz = pytz.timezone(tzname)
        return dt.astimezone(tz).strftime("%Y-%m-%d %H:%M:%S %Z")
    except Exception:
        return dt.strftime("%Y-%m-%d %H:%M:%S")
def utc_now() -> datetime:
    return datetime.now(timezone.utc)

# ----------- Data & Indicators -----------
import pandas as pd
def validate_ohlc_data(df: pd.DataFrame) -> bool:
    if df is None or df.empty or len(df) < 50:
        return False
    if df[["open","high","low","close","volume"]].isnull().any().any():
        return False
    invalid = ((df["high"]<df["low"]) | (df["high"]<df["open"]) | (df["high"]<df["close"]) |
               (df["low"]>df["open"])  | (df["low"]>df["close"]))
    if invalid.any():
        return False
    return True

class MarketDataProvider:
    def __init__(self, cfg: Config):
        self.cfg = cfg
    async def fetch_binance_klines(self, symbol: str, limit: int = 200):
        session = await session_mgr.get_session()
        url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval=1m&limit={limit}"
        try:
            async with session.get(url) as r:
                if r.status != 200:
                    return None
                raw = await r.json()
        except Exception:
            return None
        cols = ["open_time","open","high","low","close","volume","close_time","qv","ntrades","tb_base","tb_quote","ignore"]
        df = pd.DataFrame(raw, columns=cols)
        df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
        for c in ["open","high","low","close","volume"]:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        df.rename(columns={"open_time":"time"}, inplace=True)
        return df[["time","open","high","low","close","volume"]]
    async def fetch_kraken_ohlc(self, pair: str):
        session = await session_mgr.get_session()
        url = f"https://api.kraken.com/0/public/OHLC?pair={pair}&interval=1"
        try:
            async with session.get(url) as r:
                if r.status != 200:
                    return None
                raw = await r.json()
        except Exception:
            return None
        if raw.get("error"):
            return None
        result = raw.get("result", {})
        if not result:
            return None
        _, data = next(iter(result.items()))
        df = pd.DataFrame(data, columns=["time","open","high","low","close","vwap","volume","count"])
        df["time"] = pd.to_datetime(df["time"], unit="s", utc=True)
        for c in ["open","high","low","close","volume"]:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        return df[["time","open","high","low","close","volume"]]
    async def fetch_ohlc(self, limit: int = 200):
        if CFG.provider == "binance":
            df = await self.fetch_binance_klines(CFG.symbol_binance, limit)
            if df is not None and validate_ohlc_data(df): return df
            df = await self.fetch_kraken_ohlc(CFG.symbol_kraken); 
            if df is not None and validate_ohlc_data(df): return df
        else:
            df = await self.fetch_kraken_ohlc(CFG.symbol_kraken)
            if df is not None and validate_ohlc_data(df): return df
            df = await self.fetch_binance_klines(CFG.symbol_binance, limit)
            if df is not None and validate_ohlc_data(df): return df
        return None

def ema(s: pd.Series, n: int): return s.ewm(span=n, adjust=False).mean()
def rsi(s: pd.Series, n: int=14):
    d=s.diff(); up=d.clip(lower=0); dn=-1*d.clip(upper=0)
    ma_up=up.ewm(alpha=1/n, adjust=False).mean(); ma_dn=dn.ewm(alpha=1/n, adjust=False).mean()
    rs = ma_up/(ma_dn+1e-12); return 100-(100/(1+rs))
def atr(df: pd.DataFrame, n: int=14):
    hl=df["high"]-df["low"]; hc=(df["high"]-df["close"].shift()).abs(); lc=(df["low"]-df["close"].shift()).abs()
    tr=pd.concat([hl,hc,lc],axis=1).max(axis=1); return tr.ewm(alpha=1/n, adjust=False).mean()
def vwap(df: pd.DataFrame):
    pv=(df["close"]*df["volume"]).cumsum(); vv=df["volume"].replace(0,np.nan).cumsum()
    return (pv/vv).ffill().fillna(df["close"])
def donchian(df: pd.DataFrame, n: int=20):
    up=df["high"].rolling(n).max(); lo=df["low"].rolling(n).min(); return up,lo
def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df=df.copy()
    df["ema20"]=ema(df["close"],20); df["ema50"]=ema(df["close"],50)
    df["rsi"]=rsi(df["close"],14); df["atr"]=atr(df,14); df["vwap"]=vwap(df)
    dc_u,dc_l=donchian(df,20); df["dc_u"]=dc_u; df["dc_l"]=dc_l; return df

DATA_DIR = pathlib.Path(os.getenv("DATA_DIR","./data")); DATA_DIR.mkdir(parents=True, exist_ok=True)
ALERTS_CSV = DATA_DIR/"alerts.csv"; DECISIONS_CSV = DATA_DIR/"decisions.csv"; FILLS_CSV = DATA_DIR/"fills.csv"

def append_csv(path: pathlib.Path, row: Dict[str, Any]):
    exists = path.exists()
    with path.open("a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(row.keys()))
        if not exists: w.writeheader()
        w.writerow(row)

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
    market_context: Optional[str] = None

class GoogleSheetsIntegration:
    def __init__(self, url: str, token: Optional[str] = None):
        self.url=url; self.token=token
    async def _post(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        session = await session_mgr.get_session()
        headers={"Content-Type":"application/json"}
        if self.token: headers["x-app-secret"]=self.token
        try:
            async with session.post(self.url, data=json.dumps(payload), headers=headers) as r:
                txt = await r.text()
                try: return json.loads(txt) if txt else {"status":"error","message":"empty"}
                except Exception: return {"status":"error","message":"non-json","raw":txt[:200]}
        except Exception as e:
            return {"status":"error","message":str(e)}
    async def _get(self, params: Dict[str, Any]) -> Dict[str, Any]:
        session = await session_mgr.get_session()
        headers={}
        qp=dict(params)
        if self.token:
            headers["x-app-secret"]=self.token; qp["key"]=self.token
        try:
            async with session.get(self.url, params=qp, headers=headers) as r:
                txt = await r.text()
                try: return json.loads(txt) if txt else {"status":"error","message":"empty"}
                except Exception: return {"status":"error","message":"non-json","raw":txt[:200]}
        except Exception as e:
            return {"status":"error","message":str(e)}
    async def write_entry(self, t: TradeData) -> Dict[str, Any]:
        payload = {
            "id": t.id, "pair": t.pair, "side": t.side,
            "entry_price": t.entry_price, "stop_loss": t.stop_loss,
            "take_profit_1": t.take_profit_1, "take_profit_2": t.take_profit_2,
            "status": t.status, "timestamp": t.timestamp, "level_name": t.level_name,
            "level_price": t.level_price, "confidence": t.confidence, "knight": t.knight,
            "score": t.score, "market_context": t.market_context
        }
        return await self._post(payload)
    async def write_entry_with_retry(self, t: TradeData, max_retries: int = 3) -> Dict[str, Any]:
        for i in range(max_retries):
            res = await self.write_entry(t)
            if res.get("status")!="error": return res
            await asyncio.sleep(2**i)
        return res
    async def update_exit(self, trade_id: str, exit_price: float, exit_reason: str, pnl_pct: float):
        return await self._post({"action":"update","id":trade_id,"exit_price":exit_price,"exit_reason":exit_reason,"pnl_pct":pnl_pct,"status":"CLOSED"})
    async def rehydrate_open_trades(self) -> List[TradeData]:
        data = await self._get({"action":"open"})
        rows = data.get("rows", []); out=[]
        for r in rows:
            try:
                out.append(TradeData(
                    id=str(r.get("Trade ID","")), pair=r.get("Asset","ETH/USDT"), side=r.get("Direction","LONG"),
                    entry_price=float(r.get("Entry Price",0)), stop_loss=float(r.get("Stop Loss",0)),
                    take_profit_1=float(r.get("Take Profit 1",0)), take_profit_2=float(r.get("Take Profit 2",0)),
                    status=r.get("Status","OPEN"), timestamp=r.get("Timestamp", utc_now().isoformat()),
                    level_name=r.get("Level Name"), level_price=float(r.get("Level Price")) if r.get("Level Price") else None,
                    confidence=r.get("Confidence"), knight=r.get("Knight") or "Sir Leonis",
                    score=float(r.get("Original Score")) if r.get("Original Score") else None,
                    market_context=r.get("Market Context")
                ))
            except Exception:
                continue
        return out

class TradeManager:
    def __init__(self, cfg: Config, sheets: GoogleSheetsIntegration):
        self.cfg=cfg; self.sheets=sheets
        self.active: Dict[str, TradeData] = {}
        self.cooldown_until: Dict[str, float] = {}
        self.tp1_hits=set()
    async def rehydrate(self):
        rows = await self.sheets.rehydrate_open_trades()
        for t in rows: self.active[t.id]=t
        log.info(f"Rehydrated {len(rows)} open alerts.")
        return len(rows)
    def on_cooldown(self, side:str)->bool: return time.time()<self.cooldown_until.get(side,0)
    def set_cooldown(self, side:str): self.cooldown_until[side]=time.time()+self.cfg.alert_cooldown_minutes*60
    def calculate_pnl(self, t: TradeData, price: float) -> float:
        d=1 if t.side.upper()=="LONG" else -1; return d*(price-t.entry_price)/t.entry_price*100.0
    async def open_trade(self, t: TradeData):
        self.active[t.id]=t; append_csv(ALERTS_CSV, {**asdict(t), "opened_at": utc_now().isoformat()})
        await self.sheets.write_entry_with_retry(t)
    async def close_trade(self, trade_id: str, exit_price: float, reason:str):
        t = self.active.get(trade_id)
        if not t:
            return None
        pnl=self.calculate_pnl(t, exit_price)
        await self.sheets.update_exit(trade_id, exit_price, reason, pnl)
        append_csv(FILLS_CSV, {"id":trade_id,"exit_price":exit_price,"reason":reason,"pnl_pct":pnl,"score":t.score,"time":utc_now().isoformat()})
        t.status="CLOSED"; self.active.pop(trade_id,None); self.tp1_hits.discard(trade_id); return pnl
    async def check_alert_exits(self, price: float):
        closed=[]; 
        for tid,t in list(self.active.items()):
            exit_reason=None; exit_price=price
            if t.side=="LONG":
                if price<=t.stop_loss: exit_reason="Stop Loss Hit"
                elif price>=t.take_profit_2: exit_reason="TP2 Hit"
                elif price>=t.take_profit_1 and tid not in self.tp1_hits: self.tp1_hits.add(tid)
            else:
                if price>=t.stop_loss: exit_reason="Stop Loss Hit"
                elif price<=t.take_profit_2: exit_reason="TP2 Hit"
                elif price<=t.take_profit_1 and tid not in self.tp1_hits: self.tp1_hits.add(tid)
            if exit_reason:
                pnl=await self.close_trade(tid, exit_price, exit_reason)
                if pnl is not None: closed.append({"trade":t,"exit_price":exit_price,"exit_reason":exit_reason,"pnl_pct":pnl})
        return closed

@dataclass
class Signal:
    side: str; score: float; entry: float; stop: float; tp1: float; tp2: float; reason: str
    market_context: str = ""; entry_low: float = 0.0; entry_high: float = 0.0; entry_label: str = ""

def get_market_context(df: pd.DataFrame)->str:
    latest=df.iloc[-1]; ctx=[]
    ctx.append("📈 Uptrend (EMA20>50)" if latest["ema20"]>latest["ema50"] else "📉 Downtrend (EMA20<50)")
    if latest["rsi"]>70: ctx.append("⚠️ RSI Overbought")
    elif latest["rsi"]<30: ctx.append("⚠️ RSI Oversold")
    elif latest["rsi"]>60: ctx.append("💪 RSI Strong")
    elif latest["rsi"]<40: ctx.append("😟 RSI Weak")
    atr_pct=(latest["atr"]/latest["close"])*100
    ctx.append("🌊 High Volatility" if atr_pct>3 else "😴 Low Volatility" if atr_pct<1 else "⚖️ Normal Volatility")
    ctx.append("✅ Above VWAP" if latest["close"]>latest["vwap"] else "❌ Below VWAP")
    return " | ".join(ctx)

class EnhancedSignalEngine:
    def __init__(self, cfg: Config): self.cfg=cfg
    def _risk_targets(self, price: float, atr_val: float, side: str):
        risk=self.cfg.risk_atr_mult*atr_val
        if side=="LONG": return price-risk, price+self.cfg.tp1_r_multiple*risk, price+self.cfg.tp2_r_multiple*risk
        else:            return price+risk, price-self.cfg.tp1_r_multiple*risk, price-self.cfg.tp2_r_multiple*risk
    def format_signal_reason(self, parts: List[str], score: float)->str:
        conf = "🔥 STRONG" if score>=4.5 else "✅ GOOD" if score>=3.5 else "⚡ MODERATE"
        s=f"**Signal Confidence: {conf} (Score: {score:.1f}/6.0)**\n\n**Triggered Conditions:**\n"
        for i,p in enumerate(parts,1): s+=f"{i}. {p}\n"; return s
    def generate(self, df: pd.DataFrame) -> Optional[Signal]:
        if df is None or len(df)<60: return None
        latest=df.iloc[-1]; prev=df.iloc[-2]
        up = latest["ema20"]>latest["ema50"]; dn = latest["ema20"]<latest["ema50"]
        sl=0.0; score_l=0.0; parts_l=[]; ss=0.0; score_s=0.0; parts_s=[]
        breakout_l = latest["close"]>latest["dc_u"] and up and latest["close"]>latest["vwap"]
        if breakout_l: score_l+=1; parts_l.append("Breakout above Donchian & VWAP in uptrend")
        breakout_s = latest["close"]<latest["dc_l"] and dn and latest["close"]<latest["vwap"]
        if breakout_s: score_s+=1; parts_s.append("Breakdown below Donchian & VWAP in downtrend")
        bounced_l = (prev["close"]<prev["ema20"]) and (latest["close"]>latest["ema20"]) and up and latest["rsi"]>50
        if bounced_l: score_l+=1; parts_l.append("Pullback bounce on EMA20 with RSI>50")
        bounced_s = (prev["close"]>prev["ema20"]) and (latest["close"]<latest["ema20"]) and dn and latest["rsi"]<50
        if bounced_s: score_s+=1; parts_s.append("Pullback bounce under EMA20 with RSI<50")
        cont_l = up and latest["close"]>latest["ema20"] and latest["rsi"]>55 and latest["atr"]>df["atr"].iloc[-15]
        if cont_l: score_l+=1; parts_l.append("Continuation: >EMA20, RSI>55, ATR rising")
        cont_s = dn and latest["close"]<latest["ema20"] and latest["rsi"]<45 and latest["atr"]>df["atr"].iloc[-15]
        if cont_s: score_s+=1; parts_s.append("Continuation: <EMA20, RSI<45, ATR rising")
        body=abs(latest["close"]-latest["open"]); rng=latest["high"]-latest["low"]; atrv=float(latest["atr"])
        if rng>0 and atrv>0:
            body_ratio=body/max(rng,1e-9); atr_ratio=rng/atrv
            if body_ratio>0.5:
                if latest["close"]>latest["open"]: score_l+=0.5; parts_l.append("Strong bullish candle")
                else: score_s+=0.5; parts_s.append("Strong bearish candle")
            if atr_ratio>1.1:
                if score_l>=score_s: score_l+=0.5; parts_l.append("High range expansion")
                else: score_s+=0.5; parts_s.append("High range expansion")
        side=None; score=0.0; price=float(latest["close"])
        if score_l>=score_s and score_l>=CFG.min_signal_score:
            side="LONG"; score=score_l; reasons=self.format_signal_reason(parts_l, score)
        elif score_s>score_l and score_s>=CFG.min_signal_score:
            side="SHORT"; score=score_s; reasons=self.format_signal_reason(parts_s, score)
        else:
            return None
        stop,tp1,tp2 = self._risk_targets(price, atrv, side)
        market_ctx = get_market_context(df)
        zone_w = max(atrv*CFG.entry_zone_atr_mult, price*(CFG.entry_zone_min_pct/100.0))
        if side=="LONG":
            if "Breakout" in reasons:
                base=float(latest["dc_u"]); entry_low=max(base, price-zone_w/2); entry_high=price+zone_w/2; entry_label="Breakout entry"
            else:
                ema20=float(latest["ema20"]); entry_low=ema20-zone_w; entry_high=ema20; entry_label="Pullback entry"
        else:
            if "Breakdown" in reasons:
                base=float(latest["dc_l"]); entry_low=price-zone_w/2; entry_high=min(base, price+zone_w/2); entry_label="Breakdown entry"
            else:
                ema20=float(latest["ema20"]); entry_low=ema20; entry_high=ema20+zone_w; entry_label="Pullback entry"
        entry_mid=(entry_low+entry_high)/2.0
        return Signal(side=side, score=round(score,2), entry=entry_mid, stop=stop, tp1=tp1, tp2=tp2,
                      reason=reasons, market_context=market_ctx, entry_low=entry_low, entry_high=entry_high, entry_label=entry_label)

def calculate_position_size(balance: float, risk_pct: float, entry: float, stop: float)->float:
    risk_amount=balance*(risk_pct/100); pr=abs(entry-stop); return 0 if pr==0 else round(risk_amount/pr,4)

intents = discord.Intents.default(); intents.message_content=True
bot = commands.Bot(command_prefix="!", intents=intents, help_command=None)

class DiscordIO:
    def __init__(self, cfg: Config, client: commands.Bot): self.cfg=cfg; self.client=client
    def channel(self, cid:int): return self.client.get_channel(cid)
    async def safe_send(self, cid:int, embed: 'discord.Embed'=None, **k):
        ch=self.channel(cid) or await self.client.fetch_channel(cid)
        if not ch: return
        await ch.send(embed=(embed or discord.Embed(**k)))

def build_trade_embed(t: TradeData, cfg: Config)->discord.Embed:
    now=utc_now(); ct=fmt_dt(now, cfg.tz_name)
    risk_pts=abs(t.entry_price-t.stop_loss); tp1_pts=abs(t.take_profit_1-t.entry_price); tp2_pts=abs(t.take_profit_2-t.entry_price)
    risk_pct=(risk_pts/t.entry_price*100) if t.entry_price else 0; tp1_pct=(tp1_pts/t.entry_price*100) if t.entry_price else 0; tp2_pct=(tp2_pts/t.entry_price*100) if t.entry_price else 0
    zone_low=getattr(t,"entry_low",t.entry_price); zone_high=getattr(t,"entry_high",t.entry_price); label=getattr(t,"entry_label","")
    desc=((f"**{label}**\n" if label else "") + f"**Entry Zone**: ${zone_low:.2f} – ${zone_high:.2f}\n"
          f"**Stop Loss**: ${t.stop_loss:.2f} ({risk_pct:.2f}%)\n"
          f"**TP1**: ${t.take_profit_1:.2f} ({tp1_pct:.2f}%)\n"
          f"**TP2**: ${t.take_profit_2:.2f} ({tp2_pct:.2f}%)\n\n")
    if t.level_name: desc += f"{t.level_name}\n\n"
    if t.market_context: desc += f"**Market Context:**\n{t.market_context}\n\n"
    if cfg.show_position_size:
        ps=calculate_position_size(cfg.account_size_usd, cfg.position_risk_pct, t.entry_price, t.stop_loss)
        desc+=f"**Position Size ({cfg.position_risk_pct:.1f}% risk on ${cfg.account_size_usd:,.0f}):** {ps:.4f} ETH"
    color=0x4CAF50 if t.side.upper()=="LONG" else 0xF44336
    emb=discord.Embed(title=f"⚔️ Battle Signal — {t.pair} ({t.side})", description=desc, color=color, timestamp=now)
    emb.add_field(name="Alert ID", value=t.id, inline=True)
    if t.score is not None: emb.add_field(name="Score", value=f"{t.score:.2f}/6.0", inline=True)
    risk=abs(t.entry_price-t.stop_loss); reward=abs(t.take_profit_2-t.entry_price); rr=reward/risk if risk>0 else 0
    emb.add_field(name="R:R", value=f"1:{rr:.1f}", inline=True)
    emb.set_footer(text=f"CT: {ct} • UTC: {now.strftime('%Y-%m-%d %H:%M:%S UTC')}"); return emb

def build_exit_embed(info: Dict, cfg: Config)->discord.Embed:
    t=info['trade']; pnl=info['pnl_pct']; reason=info['exit_reason']; px=info['exit_price']
    now=utc_now(); ct=fmt_dt(now,cfg.tz_name); color=0x4CAF50 if pnl>0 else 0xF44336; emoji="✅" if pnl>0 else "❌"
    emb=discord.Embed(title=f"{emoji} Alert Closed: {'WIN' if pnl>0 else 'LOSS'} ({reason})",
        description=(f"**Alert ID:** {t.id}\n**Side:** {t.side}\n**Entry:** ${t.entry_price:.2f}\n**Exit:** ${px:.2f}\n**P&L:** {pnl:+.2f}%\n**Original Score:** {t.score:.2f}/6.0"), color=color, timestamp=now)
    emb.set_footer(text=f"CT: {ct} • UTC: {now.strftime('%Y-%m-%d %H:%M:%S UTC')}"); return emb

def build_error_embed(msg:str, cfg: Config)->discord.Embed:
    now=utc_now(); ct=fmt_dt(now,cfg.tz_name)
    emb=discord.Embed(title="⚠️ Error Alert", description=msg, color=0xFF9800, timestamp=now)
    emb.set_footer(text=f"CT: {ct} • UTC: {now.strftime('%Y-%m-%d %H:%M:%S UTC')}"); return emb

def build_status_embed(cfg: Config, last_price: Optional[float], tm)->discord.Embed:
    now=utc_now(); ct=fmt_dt(now,cfg.tz_name)
    desc=(f"**Configuration:**\nProvider: **{cfg.provider.upper()}** (REST)\nScan: **{cfg.scan_every_seconds}s** | Min Score: **{cfg.min_signal_score}**\n"
          f"Cooldown: **{cfg.alert_cooldown_minutes}m** | Expire: **{cfg.alert_expire_hours}h**\nRisk: **{cfg.position_risk_pct}%** | TP1/TP2: **{cfg.tp1_r_multiple}R/{cfg.tp2_r_multiple}R**\n\n")
    if last_price: desc+=f"**Current Price:** ${last_price:.2f}\n"
    desc+=f"**Active Alerts:** {len(tm.active)}\n"
    emb=discord.Embed(title=f"🛰 Scout Tower Status (v{VERSION})", description=desc, color=0x00BCD4, timestamp=now)
    emb.set_footer(text=f"CT: {ct} • UTC: {now.strftime('%Y-%m-%d %H:%M:%S UTC')}"); return emb

mdp=MarketDataProvider(CFG); sheets=GoogleSheetsIntegration(CFG.sheets_webhook, CFG.sheets_token)
tm=TradeManager(CFG, sheets); engine=EnhancedSignalEngine(CFG); dio=None

intents = discord.Intents.default(); intents.message_content=True
bot = commands.Bot(command_prefix="!", intents=intents, help_command=None)
dio = None

@bot.event
async def on_ready():
    global dio
    dio = DiscordIO(CFG, bot)
    log.info(f"Discord logged in as {bot.user} (v{VERSION})")
    await tm.rehydrate()
    if not scanner.is_running(): scanner.start()
    if not daily_summary.is_running(): daily_summary.start()
    if not expiry_checker.is_running(): expiry_checker.start()
    try:
        await dio.safe_send(CFG.startup_channel_id, embed=build_status_embed(CFG, None, tm))
    except Exception as e:
        log.warning(f"startup send error: {e}")

scanner_paused=False; _last_price=None

@tasks.loop(seconds=CFG.scan_every_seconds)
async def scanner():
    global _last_price
    if scanner_paused: return
    try:
        df = await mdp.fetch_ohlc(limit=200)
        if df is None or df.empty: return
        df = add_indicators(df); latest=df.iloc[-1]; _last_price=float(latest["close"])
        for info in await tm.check_alert_exits(_last_price):
            await dio.safe_send(CFG.signals_channel_id, embed=build_exit_embed(info, CFG))
        sig = engine.generate(df)
        if not sig: return
        if tm.on_cooldown(sig.side.upper()): return
        trade_id=f"ETH-{int(time.time())}"
        t=TradeData(id=trade_id, pair="ETH/USDT", side=sig.side.upper(), entry_price=float(sig.entry),
                    stop_loss=float(sig.stop), take_profit_1=float(sig.tp1), take_profit_2=float(sig.tp2),
                    confidence="Confluence", knight="Sir Leonis" if sig.side.upper()=="LONG" else "Sir Lucien",
                    score=sig.score, level_name=sig.reason, market_context=sig.market_context)
        setattr(t,"entry_low",sig.entry_low); setattr(t,"entry_high",sig.entry_high); setattr(t,"entry_label",sig.entry_label)
        await tm.open_trade(t)
        await dio.safe_send(CFG.signals_channel_id, embed=build_trade_embed(t, CFG))
        tm.set_cooldown(sig.side.upper())
    except Exception as e:
        await dio.safe_send(CFG.errors_channel_id, embed=build_error_embed(f"Scanner error: {e}", CFG))

@tasks.loop(hours=24)
async def daily_summary():
    try:
        await dio.safe_send(CFG.status_channel_id, embed=build_status_embed(CFG, _last_price, tm))
    except Exception as e:
        log.error(f"Daily summary error: {e}")

@tasks.loop(hours=1)
async def expiry_checker():
    try:
        # here you could expire old alerts if desired
        pass
    except Exception as e:
        log.error(f"Expiry checker error: {e}")

# ---------------- Commands ----------------
@bot.command()
async def version(ctx): await ctx.reply(f"Scout Tower v{VERSION}")

@bot.command()
async def status(ctx): await ctx.send(embed=build_status_embed(CFG, _last_price, tm))

@bot.command()
async def testsheets(ctx):
    try:
        test_trade = TradeData(
            id=f"TEST-{int(time.time())}", pair="ETH/USDT", side="TEST",
            entry_price=9999.99, stop_loss=9900.00, take_profit_1=10050.00, take_profit_2=10100.00,
            confidence="Connection Test", score=0.0, level_name="Testing Google Sheets Integration"
        )
        res = await sheets.write_entry(test_trade)
        await ctx.reply(f"Sheets POST result:\n```{res}```")
    except Exception as e:
        await ctx.reply(f"❌ Sheets test error:\n```{e}```")

@bot.command()
async def checksheets(ctx):
    masked = CFG.sheets_webhook[:40] + "..." if len(CFG.sheets_webhook) > 40 else CFG.sheets_webhook
    token_state = "set ✅" if CFG.sheets_token else "empty ❌"
    try:
        res = await sheets._get({"action":"open"})
    except Exception as e:
        res = {"status":"error","message":str(e)}
    await ctx.reply(
        "**Sheets Config**\n"
        f"Webhook: `{masked}`\n"
        f"Token: {token_state}\n"
        f"Open-trades GET: ```{json.dumps(res)[:400]}```"
    )

@bot.command()
async def rehydrate(ctx):
    try:
        count = await tm.rehydrate()
        await ctx.reply(f"Rehydrated **{count}** open alerts from Sheets.")
    except Exception as e:
        await ctx.reply(f"❌ Rehydrate error: {e}")

@bot.command()
async def getcsv(ctx, filename: str = "alerts"):
    try:
        if filename not in ["alerts","fills","decisions"]:
            await ctx.reply("Valid options: alerts, fills, decisions"); return
        p = DATA_DIR / f"{filename}.csv"
        if not p.exists(): await ctx.reply(f"No {filename}.csv file found"); return
        with open(p,'rb') as f:
            await ctx.reply(file=discord.File(f, filename=f"{filename}_{utc_now().strftime('%Y%m%d_%H%M%S')}.csv"))
    except Exception as e:
        await ctx.reply(f"Error: {e}")

@bot.command()
async def pushtosheet(ctx, trade_id: str):
    t = tm.active.get(trade_id)
    if not t: await ctx.reply(f"Alert {trade_id} not found in active alerts"); return
    res = await sheets.write_entry_with_retry(t)
    await ctx.reply(f"Push result: ```{res}```")

# -------------- Flask health --------------
from flask import Flask, jsonify
app = Flask(__name__)
@app.route("/")
def root():
    return jsonify({"ok":True,"version":VERSION,"time":utc_now().isoformat(),"active_alerts":len(tm.active)})
def run_flask():
    app.run(host="0.0.0.0", port=CFG.port, debug=False, use_reloader=False)

async def cleanup():
    await session_mgr.close()
    if scanner.is_running(): scanner.stop()
    if daily_summary.is_running(): daily_summary.stop()
    if expiry_checker.is_running(): expiry_checker.stop()

def main():
    threading.Thread(target=run_flask, daemon=True).start()
    try: bot.run(CFG.token)
    except KeyboardInterrupt: asyncio.run(cleanup())

if __name__ == "__main__":
    main()
