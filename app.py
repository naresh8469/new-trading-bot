import os
import io
import time
import threading
import logging
from datetime import datetime, timezone, timedelta

import requests
from flask import Flask, jsonify, render_template_string

try:
    import pandas as pd
except ImportError:
    pd = None

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

TWELVE_DATA_API_KEY = os.environ.get("TWELVE_DATA_API_KEY", "")

# ---------------------------------------------------------------------------
# Dhan API configuration (for REAL order execution)
# ---------------------------------------------------------------------------
DHAN_CLIENT_ID = os.environ.get("DHAN_CLIENT_ID", "")
DHAN_ACCESS_TOKEN = os.environ.get("DHAN_ACCESS_TOKEN", "")
# Safety switch: bot will only place REAL orders if this is exactly "true".
DHAN_LIVE_TRADING = os.environ.get("DHAN_LIVE_TRADING", "false").lower() == "true"

DHAN_BASE_URL = "https://api.dhan.co/v2"
DHAN_ORDER_URL = f"{DHAN_BASE_URL}/orders"
DHAN_OPTIONCHAIN_URL = f"{DHAN_BASE_URL}/optionchain"
DHAN_EXPIRYLIST_URL = f"{DHAN_BASE_URL}/optionchain/expirylist"
DHAN_LTP_URL = f"{DHAN_BASE_URL}/marketfeed/ltp"
DHAN_QUOTE_URL = f"{DHAN_BASE_URL}/marketfeed/quote"
DHAN_SCRIP_MASTER_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"

DHAN_HEADERS = {
    "access-token": DHAN_ACCESS_TOKEN,
    "client-id": DHAN_CLIENT_ID,
    "Content-Type": "application/json",
}

# Quantity to trade per signal for NIFTY/BANKNIFTY ETF proxies (units, not lots)
DHAN_QUANTITY = {
    "NIFTY": int(os.environ.get("DHAN_QTY_NIFTY", "1")),
    "BANKNIFTY": int(os.environ.get("DHAN_QTY_BANKNIFTY", "1")),
}

# Dhan "Security ID" for each NSE ETF-proxy instrument — look these up from
# Dhan's instrument master CSV (SEM_TRADING_SYMBOL / SEM_SMST_SECURITY_ID).
DHAN_SECURITY_ID = {
    "NIFTY": os.environ.get("DHAN_SECURITY_ID_NIFTY", ""),
    "BANKNIFTY": os.environ.get("DHAN_SECURITY_ID_BANKNIFTY", ""),
}

# How many option lots to buy per signal for the daily top-5 F&O stocks
DHAN_STOCK_LOTS = int(os.environ.get("DHAN_STOCK_LOTS", "1"))

IST = timezone(timedelta(hours=5, minutes=30))

# ---------------------------------------------------------------------------
# Instrument configuration (index/crypto/forex signal-only + NIFTY/BankNifty real trades)
# ---------------------------------------------------------------------------
INSTRUMENTS = {
    "NIFTY":     {"type": "index",  "symbol": "NIFTYBEES", "tradable_on_dhan": True},
    "BANKNIFTY": {"type": "index",  "symbol": "BANKBEES",  "tradable_on_dhan": True},
    "BITCOIN":   {"type": "crypto", "symbol": "BTCUSDT",   "tradable_on_dhan": False},
    "ETHEREUM":  {"type": "crypto", "symbol": "ETHUSDT",   "tradable_on_dhan": False},
    "EURUSD":    {"type": "forex",  "symbol": "EUR/USD",   "tradable_on_dhan": False},
    "GBPUSD":    {"type": "forex",  "symbol": "GBP/USD",   "tradable_on_dhan": False},
}

EMA_FAST_PERIOD = 9
EMA_SLOW_PERIOD = 15
POLL_INTERVAL_SECONDS = 60  # ~1-minute "candles"
MAX_LOG_LINES = 40

# ---------------------------------------------------------------------------
# Nifty 50 + Bank Nifty universe (for daily top-5 by volume + OI)
# NOTE: index constituents change periodically — review this list every few months.
# ---------------------------------------------------------------------------
NIFTY50_SYMBOLS = [
    "RELIANCE", "TCS", "HDFCBANK", "ICICIBANK", "INFY", "HINDUNILVR", "ITC", "SBIN",
    "BHARTIARTL", "KOTAKBANK", "LT", "AXISBANK", "BAJFINANCE", "ASIANPAINT", "MARUTI",
    "HCLTECH", "SUNPHARMA", "TITAN", "ULTRACEMCO", "WIPRO", "NESTLEIND", "ONGC", "NTPC",
    "POWERGRID", "M&M", "TATAMOTORS", "TATASTEEL", "ADANIENT", "ADANIPORTS", "JSWSTEEL",
    "COALINDIA", "BAJAJFINSV", "TECHM", "HDFCLIFE", "SBILIFE", "DRREDDY", "GRASIM",
    "CIPLA", "EICHERMOT", "BRITANNIA", "DIVISLAB", "APOLLOHOSP", "HEROMOTOCO", "BPCL",
    "INDUSINDBK", "UPL", "TATACONSUM", "HINDALCO", "BAJAJ-AUTO", "SHRIRAMFIN",
]
BANKNIFTY_SYMBOLS = [
    "HDFCBANK", "ICICIBANK", "SBIN", "KOTAKBANK", "AXISBANK", "INDUSINDBK",
    "BANKBARODA", "PNB", "IDFCFIRSTB", "FEDERALBNK", "AUBANK", "BANDHANBNK",
]
FNO_UNIVERSE = sorted(set(NIFTY50_SYMBOLS) | set(BANKNIFTY_SYMBOLS))

TOP5_SELECTION_HOUR_IST = 9
TOP5_SELECTION_MINUTE_IST = 30
TOP5_REFRESH_SECONDS = 3600  # re-check top 5 every 1 hour, as requested

# ---------------------------------------------------------------------------
# In-memory state
# ---------------------------------------------------------------------------
state_lock = threading.Lock()
state = {
    name: {
        "ema_fast": None, "ema_slow": None, "prev_ema_fast": None, "prev_ema_slow": None,
        "candle_count": 0, "last_price": None, "position": None, "closed_trades": [],
    }
    for name in INSTRUMENTS
}
activity_log = []

# Daily top-5 F&O stock state
top5_lock = threading.Lock()
top5_state = {
    "date": None,           # date string this selection is valid for
    "last_ranked_at": None,
    "symbols": [],          # list of 5 symbols currently active
    "ranking_debug": [],    # last ranking table for the dashboard
}
stock_state = {}  # per-symbol EMA/position state, created on demand

instrument_master_lock = threading.Lock()
instrument_master = {
    "loaded_date": None,
    "equity_security_id": {},   # symbol -> security_id (str)
    "futures_security_id": {},  # symbol -> nearest-expiry FUTSTK security_id (str)
}


def log_activity(text: str):
    timestamp = datetime.now(IST).strftime("%H:%M:%S")
    line = f"{timestamp} - {text}"
    activity_log.insert(0, line)
    del activity_log[MAX_LOG_LINES:]
    logger.info(text)


# ---------------------------------------------------------------------------
# Dhan instrument master (CSV) — resolves symbol -> security_id automatically
# ---------------------------------------------------------------------------
def load_instrument_master_if_needed():
    today_str = datetime.now(IST).strftime("%Y-%m-%d")
    with instrument_master_lock:
        if instrument_master["loaded_date"] == today_str:
            return  # already loaded today

    if pd is None:
        log_activity("pandas not installed — cannot load Dhan instrument master. Add 'pandas' to requirements.txt.")
        return

    try:
        response = requests.get(DHAN_SCRIP_MASTER_URL, timeout=30)
        response.raise_for_status()
        df = pd.read_csv(io.StringIO(response.text), low_memory=False)

        equity_map = {}
        futures_map = {}

        equity_rows = df[
            (df["SEM_EXM_EXCH_ID"] == "NSE") & (df["SEM_INSTRUMENT_NAME"] == "EQUITY")
        ]
        for _, row in equity_rows.iterrows():
            sym = str(row["SEM_TRADING_SYMBOL"]).strip()
            if sym in FNO_UNIVERSE and sym not in equity_map:
                equity_map[sym] = str(int(row["SEM_SMST_SECURITY_ID"]))

        fut_rows = df[
            (df["SEM_EXM_EXCH_ID"] == "NSE") & (df["SEM_INSTRUMENT_NAME"] == "FUTSTK")
        ].copy()
        if not fut_rows.empty:
            fut_rows["SEM_EXPIRY_DATE"] = pd.to_datetime(fut_rows["SEM_EXPIRY_DATE"], errors="coerce")
            now = pd.Timestamp.now()
            fut_rows = fut_rows[fut_rows["SEM_EXPIRY_DATE"] >= now]
            fut_rows = fut_rows.sort_values("SEM_EXPIRY_DATE")
            for sym in FNO_UNIVERSE:
                if "SM_SYMBOL_NAME" in fut_rows.columns:
                    sym_rows = fut_rows[fut_rows["SM_SYMBOL_NAME"] == sym]
                else:
                    sym_rows = fut_rows[fut_rows["SEM_TRADING_SYMBOL"].str.startswith(sym)]
                if not sym_rows.empty:
                    futures_map[sym] = str(int(sym_rows.iloc[0]["SEM_SMST_SECURITY_ID"]))

        with instrument_master_lock:
            instrument_master["equity_security_id"] = equity_map
            instrument_master["futures_security_id"] = futures_map
            instrument_master["loaded_date"] = today_str

        log_activity(f"Instrument master loaded: {len(equity_map)} equities, {len(futures_map)} futures resolved.")
    except Exception as e:
        log_activity(f"Failed to load Dhan instrument master: {e}")


# ---------------------------------------------------------------------------
# Dhan order placement (equity / ETF)
# ---------------------------------------------------------------------------
def place_dhan_order(name: str, transaction_type: str):
    if not DHAN_LIVE_TRADING:
        log_activity(f"[{name}] LIVE TRADING OFF — would have placed {transaction_type} order (dry run).")
        return
    if not DHAN_CLIENT_ID or not DHAN_ACCESS_TOKEN:
        log_activity(f"[{name}] Dhan credentials missing — cannot place {transaction_type} order.")
        return
    security_id = DHAN_SECURITY_ID.get(name)
    if not security_id:
        log_activity(f"[{name}] Dhan security ID missing — cannot place {transaction_type} order.")
        return

    payload = {
        "dhanClientId": DHAN_CLIENT_ID,
        "transactionType": transaction_type,
        "exchangeSegment": "NSE_EQ",
        "productType": "INTRADAY",
        "orderType": "MARKET",
        "validity": "DAY",
        "securityId": security_id,
        "quantity": DHAN_QUANTITY.get(name, 1),
        "price": 0,
    }
    try:
        response = requests.post(DHAN_ORDER_URL, json=payload, headers=DHAN_HEADERS, timeout=10)
        if response.status_code in (200, 201):
            data = response.json()
            log_activity(f"[{name}] Dhan {transaction_type} order placed. Order ID: {data.get('orderId', '?')}")
        else:
            log_activity(f"[{name}] Dhan {transaction_type} order FAILED ({response.status_code}): {response.text[:200]}")
    except Exception as e:
        log_activity(f"[{name}] Dhan {transaction_type} order error: {e}")


def place_dhan_fno_order(security_id: str, transaction_type: str, quantity: int, label: str):
    if not DHAN_LIVE_TRADING:
        log_activity(f"[{label}] LIVE TRADING OFF — would have placed {transaction_type} order (dry run).")
        return None
    if not DHAN_CLIENT_ID or not DHAN_ACCESS_TOKEN:
        log_activity(f"[{label}] Dhan credentials missing — cannot place {transaction_type} order.")
        return None

    payload = {
        "dhanClientId": DHAN_CLIENT_ID,
        "transactionType": transaction_type,
        "exchangeSegment": "NSE_FNO",
        "productType": "INTRADAY",
        "orderType": "MARKET",
        "validity": "DAY",
        "securityId": security_id,
        "quantity": quantity,
        "price": 0,
    }
    try:
        response = requests.post(DHAN_ORDER_URL, json=payload, headers=DHAN_HEADERS, timeout=10)
        if response.status_code in (200, 201):
            data = response.json()
            log_activity(f"[{label}] Dhan {transaction_type} order placed. Order ID: {data.get('orderId', '?')}")
            return data.get("orderId")
        else:
            log_activity(f"[{label}] Dhan {transaction_type} order FAILED ({response.status_code}): {response.text[:200]}")
    except Exception as e:
        log_activity(f"[{label}] Dhan {transaction_type} order error: {e}")
    return None


# ---------------------------------------------------------------------------
# Market hours helpers
# ---------------------------------------------------------------------------
def is_nse_market_open() -> bool:
    now = datetime.now(IST)
    if now.weekday() >= 5:
        return False
    open_time = now.replace(hour=9, minute=15, second=0, microsecond=0)
    close_time = now.replace(hour=15, minute=30, second=0, microsecond=0)
    return open_time <= now <= close_time


def is_forex_market_open() -> bool:
    now = datetime.now(IST)
    if now.weekday() == 5:
        return False
    if now.weekday() == 6 and now.hour < 22:
        return False
    return True


# ---------------------------------------------------------------------------
# Price fetchers (Twelve Data / Binance) — existing instruments
# ---------------------------------------------------------------------------
def fetch_twelve_data_price(symbol: str):
    if not TWELVE_DATA_API_KEY:
        logger.error("TWELVE_DATA_API_KEY not set, price fetch skipped.")
        return None
    try:
        response = requests.get(
            "https://api.twelvedata.com/price",
            params={"symbol": symbol, "apikey": TWELVE_DATA_API_KEY},
            timeout=10,
        )
        response.raise_for_status()
        data = response.json()
        if "price" not in data:
            logger.error(f"Twelve Data price fetch error ({symbol}): {data}")
            return None
        return float(data["price"])
    except Exception as e:
        logger.error(f"Twelve Data price fetch error ({symbol}): {e}")
        return None


def fetch_crypto_price(symbol: str):
    try:
        response = requests.get(
            "https://api.binance.com/api/v3/ticker/price",
            params={"symbol": symbol},
            timeout=10,
        )
        response.raise_for_status()
        return float(response.json()["price"])
    except Exception as e:
        logger.error(f"Crypto price fetch error ({symbol}): {e}")
        return None


def get_price(name: str, config: dict):
    if config["type"] == "index":
        if not is_nse_market_open():
            return None
        return fetch_twelve_data_price(config["symbol"])
    elif config["type"] == "crypto":
        return fetch_crypto_price(config["symbol"])
    elif config["type"] == "forex":
        if not is_forex_market_open():
            return None
        return fetch_twelve_data_price(config["symbol"])
    return None


# ---------------------------------------------------------------------------
# Dhan market data helpers for the top-5 F&O stocks
# ---------------------------------------------------------------------------
def dhan_batch_ltp(security_ids: list):
    """Returns {security_id: last_price} for a list of NSE_EQ security ids."""
    if not security_ids or not DHAN_ACCESS_TOKEN:
        return {}
    try:
        payload = {"NSE_EQ": [int(sid) for sid in security_ids]}
        response = requests.post(DHAN_LTP_URL, json=payload, headers=DHAN_HEADERS, timeout=10)
        response.raise_for_status()
        data = response.json().get("data", {}).get("NSE_EQ", {})
        return {sid: v.get("last_price") for sid, v in data.items()}
    except Exception as e:
        logger.error(f"Dhan LTP batch fetch error: {e}")
        return {}


def dhan_batch_quote(segment: str, security_ids: list):
    """Returns {security_id: full quote dict} for a list of security ids in a segment."""
    if not security_ids or not DHAN_ACCESS_TOKEN:
        return {}
    try:
        payload = {segment: [int(sid) for sid in security_ids]}
        response = requests.post(DHAN_QUOTE_URL, json=payload, headers=DHAN_HEADERS, timeout=15)
        response.raise_for_status()
        return response.json().get("data", {}).get(segment, {})
    except Exception as e:
        logger.error(f"Dhan quote batch fetch error ({segment}): {e}")
        return {}


def get_nearest_expiry(security_id: str):
    try:
        payload = {"UnderlyingScrip": int(security_id), "UnderlyingSeg": "NSE_EQ"}
        response = requests.post(DHAN_EXPIRYLIST_URL, json=payload, headers=DHAN_HEADERS, timeout=10)
        response.raise_for_status()
        dates = response.json().get("data", [])
        return dates[0] if dates else None
    except Exception as e:
        logger.error(f"Dhan expiry list fetch error: {e}")
        return None


def get_option_chain(security_id: str, expiry: str):
    try:
        payload = {"UnderlyingScrip": int(security_id), "UnderlyingSeg": "NSE_EQ", "Expiry": expiry}
        response = requests.post(DHAN_OPTIONCHAIN_URL, json=payload, headers=DHAN_HEADERS, timeout=10)
        response.raise_for_status()
        return response.json().get("data", {})
    except Exception as e:
        logger.error(f"Dhan option chain fetch error: {e}")
        return None


def find_slightly_itm(oc_data: dict, spot_price: float, option_type: str):
    """option_type = 'ce' or 'pe'. Returns (strike, security_id, premium) or None."""
    oc = oc_data.get("oc", {})
    if not oc:
        return None
    strikes = sorted(float(k) for k in oc.keys())
    if option_type == "ce":
        candidates = [s for s in strikes if s < spot_price]
        strike = max(candidates) if candidates else min(strikes)
    else:
        candidates = [s for s in strikes if s > spot_price]
        strike = min(candidates) if candidates else max(strikes)
    key = f"{strike:.6f}"
    leg = oc.get(key, {}).get(option_type)
    if not leg:
        return None
    return strike, str(leg.get("security_id")), leg.get("last_price")


# ---------------------------------------------------------------------------
# Daily top-5 F&O stock selection (by volume + open interest)
# ---------------------------------------------------------------------------
def rank_and_select_top5():
    load_instrument_master_if_needed()
    equity_map = instrument_master["equity_security_id"]
    futures_map = instrument_master["futures_security_id"]

    symbols_with_ids = [s for s in FNO_UNIVERSE if s in equity_map]
    if not symbols_with_ids:
        log_activity("Top-5 ranking skipped: no equity security IDs resolved yet.")
        return

    eq_ids = [equity_map[s] for s in symbols_with_ids]
    eq_quotes = dhan_batch_quote("NSE_EQ", eq_ids)

    fut_symbols = [s for s in symbols_with_ids if s in futures_map]
    fut_ids = [futures_map[s] for s in fut_symbols]
    fut_quotes = dhan_batch_quote("NSE_FNO", fut_ids) if fut_ids else {}

    rows = []
    for sym in symbols_with_ids:
        eq_id = equity_map[sym]
        volume = eq_quotes.get(eq_id, {}).get("volume", 0) or 0
        oi = 0
        if sym in futures_map:
            fut_id = futures_map[sym]
            oi = fut_quotes.get(fut_id, {}).get("oi", 0) or 0
        rows.append({"symbol": sym, "security_id": eq_id, "volume": volume, "oi": oi})

    if not rows:
        return

    vol_ranked = sorted(rows, key=lambda r: r["volume"], reverse=True)
    for i, r in enumerate(vol_ranked):
        r["vol_rank"] = i + 1
    oi_ranked = sorted(rows, key=lambda r: r["oi"], reverse=True)
    for i, r in enumerate(oi_ranked):
        r["oi_rank"] = i + 1
    for r in rows:
        r["combined_rank"] = r["vol_rank"] + r["oi_rank"]

    rows.sort(key=lambda r: r["combined_rank"])
    top5 = [r["symbol"] for r in rows[:5]]

    with top5_lock:
        today_str = datetime.now(IST).strftime("%Y-%m-%d")
        old_symbols = set(top5_state["symbols"])
        top5_state["date"] = today_str
        top5_state["last_ranked_at"] = datetime.now(IST).strftime("%H:%M:%S")
        top5_state["symbols"] = top5
        top5_state["ranking_debug"] = rows[:10]

    for sym in top5:
        if sym not in stock_state:
            stock_state[sym] = {
                "ema_fast": None, "ema_slow": None, "prev_ema_fast": None, "prev_ema_slow": None,
                "candle_count": 0, "last_price": None,
                "option_position": None,  # {"type":"ce"/"pe","strike":..,"security_id":..,"entry_premium":..}
                "closed_trades": [],
            }

    dropped = old_symbols - set(top5)
    for sym in dropped:
        if sym in stock_state and stock_state[sym].get("option_position"):
            pos = stock_state[sym]["option_position"]
            place_dhan_fno_order(pos["security_id"], "SELL", pos["quantity"], f"{sym} {pos['type'].upper()}")
            stock_state[sym]["option_position"] = None
            log_activity(f"[{sym}] Dropped from top-5, exited open option position.")

    log_activity(f"Top-5 re-ranked: {', '.join(top5)}")


def top5_selector_loop():
    last_run_date = None
    last_run_time = None
    while True:
        try:
            now = datetime.now(IST)
            is_after_selection_time = (now.hour, now.minute) >= (TOP5_SELECTION_HOUR_IST, TOP5_SELECTION_MINUTE_IST)
            today_str = now.strftime("%Y-%m-%d")
            should_run = False
            if is_nse_market_open() and is_after_selection_time:
                if last_run_date != today_str:
                    should_run = True
                elif last_run_time is None or (time.time() - last_run_time) >= TOP5_REFRESH_SECONDS:
                    should_run = True
            if should_run:
                rank_and_select_top5()
                last_run_date = today_str
                last_run_time = time.time()
        except Exception as e:
            logger.error(f"Top-5 selector loop error: {e}")
        time.sleep(60)


# ---------------------------------------------------------------------------
# EMA + strategy helpers
# ---------------------------------------------------------------------------
def update_ema(prev_ema, price, period):
    alpha = 2 / (period + 1)
    if prev_ema is None:
        return price
    return alpha * price + (1 - alpha) * prev_ema


def process_instrument(name: str, config: dict):
    price = get_price(name, config)
    s = state[name]
    if price is None:
        return
    s["last_price"] = price
    s["prev_ema_fast"] = s["ema_fast"]
    s["prev_ema_slow"] = s["ema_slow"]
    s["ema_fast"] = update_ema(s["ema_fast"], price, EMA_FAST_PERIOD)
    s["ema_slow"] = update_ema(s["ema_slow"], price, EMA_SLOW_PERIOD)
    s["candle_count"] += 1

    if s["candle_count"] <= EMA_SLOW_PERIOD or s["prev_ema_fast"] is None:
        log_activity(f"[{name}] Warming up ({s['candle_count']}/{EMA_SLOW_PERIOD}). Price {price:.4f}")
        return

    bullish_cross = s["prev_ema_fast"] <= s["prev_ema_slow"] and s["ema_fast"] > s["ema_slow"]
    bearish_cross = s["prev_ema_fast"] >= s["prev_ema_slow"] and s["ema_fast"] < s["ema_slow"]

    if bullish_cross and s["position"] is None:
        s["position"] = {"entry_price": price, "entry_time": datetime.now(IST).strftime("%d-%b %H:%M")}
        log_activity(f"[{name}] BUY signal (EMA9 crossed above EMA15) @ {price:.4f}")
        if config.get("tradable_on_dhan"):
            place_dhan_order(name, "BUY")
    elif bearish_cross and s["position"] is not None:
        entry_price = s["position"]["entry_price"]
        pnl = price - entry_price
        s["closed_trades"].insert(0, {
            "entry_price": entry_price, "exit_price": price, "pnl": pnl,
            "entry_time": s["position"]["entry_time"], "exit_time": datetime.now(IST).strftime("%d-%b %H:%M"),
        })
        del s["closed_trades"][20:]
        s["position"] = None
        log_activity(f"[{name}] SELL / exit (EMA9 crossed below EMA15) @ {price:.4f}, PnL {pnl:+.4f}")
        if config.get("tradable_on_dhan"):
            place_dhan_order(name, "SELL")
    else:
        log_activity(f"[{name}] No signal. Price {price:.4f}, EMA9 {s['ema_fast']:.4f}, EMA15 {s['ema_slow']:.4f}")


def enter_option(sym: str, security_id_underlying: str, direction: str, spot_price: float):
    """direction = 'ce' (bullish) or 'pe' (bearish)"""
    expiry = get_nearest_expiry(security_id_underlying)
    if not expiry:
        log_activity(f"[{sym}] Could not fetch nearest expiry, skipping option entry.")
        return
    oc_data = get_option_chain(security_id_underlying, expiry)
    if not oc_data:
        log_activity(f"[{sym}] Could not fetch option chain, skipping option entry.")
        return
    result = find_slightly_itm(oc_data, spot_price, direction)
    if not result:
        log_activity(f"[{sym}] Could not resolve ITM strike, skipping option entry.")
        return
    strike, sec_id, premium = result
    quantity = DHAN_STOCK_LOTS  # NOTE: this should be lots * lot_size; see setup notes
    place_dhan_fno_order(sec_id, "BUY", quantity, f"{sym} {direction.upper()} {strike}")
    stock_state[sym]["option_position"] = {
        "type": direction, "strike": strike, "security_id": sec_id,
        "entry_premium": premium, "quantity": quantity, "expiry": expiry,
        "entry_time": datetime.now(IST).strftime("%d-%b %H:%M"),
    }
    log_activity(f"[{sym}] Entered {direction.upper()} {strike} (expiry {expiry}) @ ~{premium}")


def exit_option(sym: str):
    pos = stock_state[sym].get("option_position")
    if not pos:
        return
    place_dhan_fno_order(pos["security_id"], "SELL", pos["quantity"], f"{sym} {pos['type'].upper()} {pos['strike']}")
    stock_state[sym]["closed_trades"].insert(0, {
        "type": pos["type"], "strike": pos["strike"], "entry_premium": pos["entry_premium"],
        "entry_time": pos["entry_time"], "exit_time": datetime.now(IST).strftime("%d-%b %H:%M"),
    })
    del stock_state[sym]["closed_trades"][20:]
    stock_state[sym]["option_position"] = None
    log_activity(f"[{sym}] Exited {pos['type'].upper()} {pos['strike']}")


def process_top5_stocks():
    with top5_lock:
        symbols = list(top5_state["symbols"])
    if not symbols:
        return
    equity_map = instrument_master["equity_security_id"]
    sec_ids = [equity_map[s] for s in symbols if s in equity_map]
    if not sec_ids:
        return
    prices = dhan_batch_ltp(sec_ids)

    for sym in symbols:
        if sym not in equity_map or sym not in stock_state:
            continue
        sec_id = equity_map[sym]
        price = prices.get(sec_id)
        if price is None:
            continue
        s = stock_state[sym]
        s["last_price"] = price
        s["prev_ema_fast"] = s["ema_fast"]
        s["prev_ema_slow"] = s["ema_slow"]
        s["ema_fast"] = update_ema(s["ema_fast"], price, EMA_FAST_PERIOD)
        s["ema_slow"] = update_ema(s["ema_slow"], price, EMA_SLOW_PERIOD)
        s["candle_count"] += 1

        if s["candle_count"] <= EMA_SLOW_PERIOD or s["prev_ema_fast"] is None:
            continue

        bullish_cross = s["prev_ema_fast"] <= s["prev_ema_slow"] and s["ema_fast"] > s["ema_slow"]
        bearish_cross = s["prev_ema_fast"] >= s["prev_ema_slow"] and s["ema_fast"] < s["ema_slow"]
        pos = s.get("option_position")

        if bullish_cross:
            if pos and pos["type"] == "pe":
                exit_option(sym)
                enter_option(sym, sec_id, "ce", price)
            elif not pos:
                enter_option(sym, sec_id, "ce", price)
        elif bearish_cross:
            if pos and pos["type"] == "ce":
                exit_option(sym)
                enter_option(sym, sec_id, "pe", price)
            elif not pos:
                enter_option(sym, sec_id, "pe", price)


def worker_loop():
    logger.info("Trading bot worker thread shuru ho raha hai...")
    mode = "LIVE (real orders)" if DHAN_LIVE_TRADING else "DRY RUN (signals only, no real orders)"
    logger.info(f"Dhan trading mode: {mode}")
    while True:
        with state_lock:
            for name, config in INSTRUMENTS.items():
                try:
                    process_instrument(name, config)
                except Exception as e:
                    logger.error(f"[{name}] Unexpected error: {e}")
        try:
            if is_nse_market_open():
                process_top5_stocks()
        except Exception as e:
            logger.error(f"Top-5 stock processing error: {e}")
        time.sleep(POLL_INTERVAL_SECONDS)


# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------
app = Flask(__name__)

DASHBOARD_TEMPLATE = """
<!DOCTYPE html>
<html lang="hi">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Trading Bot Dashboard</title>
<style>
  body { font-family: sans-serif; background:#0e0e14; color:#e6e6f0; margin:0; padding:16px; }
  h1 { font-size:1.3em; } h2 { font-size:1.05em; margin-top:22px; }
  .banner { padding:8px 12px; border-radius:8px; margin-bottom:14px; font-size:0.85em; }
  .live { background:#4d1a1a; color:#ff8080; }
  .dry { background:#1a2a4d; color:#7ea8f0; }
  .card { background:#1a1a24; border-radius:10px; padding:14px; margin-bottom:14px; }
  .row { display:flex; justify-content:space-between; margin:4px 0; }
  .tag { padding:2px 8px; border-radius:6px; font-size:0.75em; margin-left:6px; }
  .crypto { background:#5a3d1a; color:#f0b96a; }
  .forex { background:#1a3d2a; color:#7de3a0; }
  .nse { background:#1a2a4d; color:#7ea8f0; }
  .fno { background:#3d1a4d; color:#c98af0; }
  .pos { color:#7de37d; } .neg { color:#ff8080; }
  .log { font-size:0.8em; color:#aab; margin:2px 0; }
</style>
</head>
<body>
<h1>📊 Trading Bot Dashboard</h1>
<div class="banner {{ 'live' if live_trading else 'dry' }}">
  {% if live_trading %}⚠️ LIVE TRADING ON — real orders being placed on Dhan
  {% else %}Dry run — signals only, no real orders placed (DHAN_LIVE_TRADING off){% endif %}
</div>

{% for name, s in instruments.items() %}
<div class="card">
  <div class="row"><b>{{ name }}</b><span class="tag {{ tag_class[name] }}">{{ tag_label[name] }}</span></div>
  <div class="row"><span>Last price</span><span>{{ "%.4f"|format(s.last_price) if s.last_price else "—" }}</span></div>
  <div class="row"><span>Total trades</span><span>{{ s.closed_trades|length }}</span></div>
  <div class="row"><span>Total P&amp;L (points)</span>
    <span class="{{ 'pos' if total_pnl[name] >= 0 else 'neg' }}">{{ "%+.4f"|format(total_pnl[name]) }}</span></div>
  {% if s.position %}<div class="row"><span>Open position</span><span>Entry {{ "%.4f"|format(s.position.entry_price) }} @ {{ s.position.entry_time }}</span></div>{% endif %}
</div>
{% endfor %}

<h2>Today's Top-5 F&O Stocks (Nifty50 + BankNifty, by Volume+OI)</h2>
<div class="card">
  <div class="row"><span>Selected on</span><span>{{ top5.date or "—" }}</span></div>
  <div class="row"><span>Last ranked</span><span>{{ top5.last_ranked_at or "—" }}</span></div>
  <div class="row"><span>Symbols</span><span>{{ top5.symbols|join(", ") if top5.symbols else "—" }}</span></div>
</div>

{% for sym in top5.symbols %}
{% set s = stocks.get(sym) %}
{% if s %}
<div class="card">
  <div class="row"><b>{{ sym }}</b><span class="tag fno">F&O OPTION</span></div>
  <div class="row"><span>Last price</span><span>{{ "%.2f"|format(s.last_price) if s.last_price else "—" }}</span></div>
  {% if s.option_position %}
  <div class="row"><span>Open</span><span>{{ s.option_position.type|upper }} {{ s.option_position.strike }} @ {{ s.option_position.entry_time }}</span></div>
  {% endif %}
  <div class="row"><span>Closed trades today</span><span>{{ s.closed_trades|length }}</span></div>
</div>
{% endif %}
{% endfor %}

<div class="card">
  <b>Bot Activity Log</b><br><br>
  {% for line in logs %}<div class="log">{{ line }}</div>{% endfor %}
</div>

<script>setTimeout(() => location.reload(), 30000);</script>
</body>
</html>
"""

TAG_LABEL = {"NIFTY": "NSE", "BANKNIFTY": "NSE", "BITCOIN": "CRYPTO", "ETHEREUM": "CRYPTO", "EURUSD": "FOREX", "GBPUSD": "FOREX"}
TAG_CLASS = {"NIFTY": "nse", "BANKNIFTY": "nse", "BITCOIN": "crypto", "ETHEREUM": "crypto", "EURUSD": "forex", "GBPUSD": "forex"}


@app.route("/")
def dashboard():
    with state_lock:
        total_pnl = {name: sum(t["pnl"] for t in s["closed_trades"]) for name, s in state.items()}
        with top5_lock:
            top5_snapshot = dict(top5_state)
        return render_template_string(
            DASHBOARD_TEMPLATE,
            instruments=state, tag_label=TAG_LABEL, tag_class=TAG_CLASS,
            total_pnl=total_pnl, logs=activity_log, live_trading=DHAN_LIVE_TRADING,
            top5=top5_snapshot, stocks=stock_state,
        )


@app.route("/api/data")
def api_data():
    with state_lock:
        with top5_lock:
            return jsonify({"instruments": state, "top5": top5_state, "stocks": stock_state})


@app.route("/healthz")
def health_check():
    return "OK", 200


_worker_thread = threading.Thread(target=worker_loop, daemon=True)
_worker_thread.start()
_top5_thread = threading.Thread(target=top5_selector_loop, daemon=True)
_top5_thread.start()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
