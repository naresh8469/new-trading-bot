import os
import time
import threading
import logging
from datetime import datetime, timezone, timedelta

import requests
from flask import Flask, jsonify, render_template_string

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

TWELVE_DATA_API_KEY = os.environ.get("TWELVE_DATA_API_KEY", "")

IST = timezone(timedelta(hours=5, minutes=30))

# ---------------------------------------------------------------------------
# Instrument configuration
# ---------------------------------------------------------------------------
# type: "nse" (yfinance), "crypto" (Binance), "forex" (Twelve Data)
INSTRUMENTS = {
    "NIFTY":     {"type": "nse",    "symbol": "^NSEI"},
    "BANKNIFTY": {"type": "nse",    "symbol": "^NSEBANK"},
    "BITCOIN":   {"type": "crypto", "symbol": "BTCUSDT"},
    "ETHEREUM":  {"type": "crypto", "symbol": "ETHUSDT"},
    "EURUSD":    {"type": "forex",  "symbol": "EUR/USD"},
    "GBPUSD":    {"type": "forex",  "symbol": "GBP/USD"},
}

EMA_FAST_PERIOD = 9
EMA_SLOW_PERIOD = 15
POLL_INTERVAL_SECONDS = 60  # ~1-minute "candles"
MAX_LOG_LINES = 40

# ---------------------------------------------------------------------------
# In-memory state (resets if the service restarts)
# ---------------------------------------------------------------------------
state_lock = threading.Lock()
state = {
    name: {
        "ema_fast": None,
        "ema_slow": None,
        "prev_ema_fast": None,
        "prev_ema_slow": None,
        "candle_count": 0,
        "last_price": None,
        "position": None,  # {"entry_price": float, "entry_time": str}
        "closed_trades": [],  # list of {"entry_price","exit_price","pnl","entry_time","exit_time"}
    }
    for name in INSTRUMENTS
}
activity_log = []


def log_activity(text: str):
    timestamp = datetime.now(IST).strftime("%H:%M:%S")
    line = f"{timestamp} - {text}"
    activity_log.insert(0, line)
    del activity_log[MAX_LOG_LINES:]
    logger.info(text)


# ---------------------------------------------------------------------------
# Market hours helpers
# ---------------------------------------------------------------------------
def is_nse_market_open() -> bool:
    now = datetime.now(IST)
    if now.weekday() >= 5:  # Saturday/Sunday
        return False
    open_time = now.replace(hour=9, minute=15, second=0, microsecond=0)
    close_time = now.replace(hour=15, minute=30, second=0, microsecond=0)
    return open_time <= now <= close_time


def is_forex_market_open() -> bool:
    # Forex trades ~24/5: closed roughly Saturday all day and Sunday before ~22:00 IST
    now = datetime.now(IST)
    if now.weekday() == 5:
        return False
    if now.weekday() == 6 and now.hour < 22:
        return False
    return True


# ---------------------------------------------------------------------------
# Price fetchers
# ---------------------------------------------------------------------------
def fetch_nse_price(symbol: str):
    try:
        import yfinance as yf
        ticker = yf.Ticker(symbol)
        data = ticker.history(period="1d", interval="1m")
        if data.empty:
            return None
        return float(data["Close"].iloc[-1])
    except Exception as e:
        logger.error(f"NSE price fetch error ({symbol}): {e}")
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


def fetch_forex_price(symbol: str):
    if not TWELVE_DATA_API_KEY:
        logger.error("TWELVE_DATA_API_KEY not set, forex price fetch skipped.")
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
            logger.error(f"Forex price fetch error ({symbol}): {data}")
            return None
        return float(data["price"])
    except Exception as e:
        logger.error(f"Forex price fetch error ({symbol}): {e}")
        return None


def get_price(name: str, config: dict):
    if config["type"] == "nse":
        if not is_nse_market_open():
            return None
        return fetch_nse_price(config["symbol"])
    elif config["type"] == "crypto":
        return fetch_crypto_price(config["symbol"])
    elif config["type"] == "forex":
        if not is_forex_market_open():
            return None
        return fetch_forex_price(config["symbol"])
    return None


# ---------------------------------------------------------------------------
# Strategy: EMA9 x EMA15 crossover only (no RSI)
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
        return  # market closed or fetch failed; skip silently

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
        s["position"] = {
            "entry_price": price,
            "entry_time": datetime.now(IST).strftime("%d-%b %H:%M"),
        }
        log_activity(f"[{name}] BUY signal (EMA9 crossed above EMA15) @ {price:.4f}")

    elif bearish_cross and s["position"] is not None:
        entry_price = s["position"]["entry_price"]
        pnl = price - entry_price
        s["closed_trades"].insert(0, {
            "entry_price": entry_price,
            "exit_price": price,
            "pnl": pnl,
            "entry_time": s["position"]["entry_time"],
            "exit_time": datetime.now(IST).strftime("%d-%b %H:%M"),
        })
        del s["closed_trades"][20:]
        s["position"] = None
        log_activity(f"[{name}] SELL / exit (EMA9 crossed below EMA15) @ {price:.4f}, PnL {pnl:+.4f}")

    else:
        log_activity(f"[{name}] No signal. Price {price:.4f}, EMA9 {s['ema_fast']:.4f}, EMA15 {s['ema_slow']:.4f}")


def worker_loop():
    logger.info("Trading bot worker thread shuru ho raha hai...")
    while True:
        with state_lock:
            for name, config in INSTRUMENTS.items():
                try:
                    process_instrument(name, config)
                except Exception as e:
                    logger.error(f"[{name}] Unexpected error: {e}")
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
  h1 { font-size:1.3em; }
  .card { background:#1a1a24; border-radius:10px; padding:14px; margin-bottom:14px; }
  .row { display:flex; justify-content:space-between; margin:4px 0; }
  .tag { padding:2px 8px; border-radius:6px; font-size:0.75em; margin-left:6px; }
  .crypto { background:#5a3d1a; color:#f0b96a; }
  .forex { background:#1a3d2a; color:#7de3a0; }
  .nse { background:#1a2a4d; color:#7ea8f0; }
  .pos { color:#7de37d; }
  .neg { color:#ff8080; }
  .log { font-size:0.8em; color:#aab; margin:2px 0; }
</style>
</head>
<body>
<h1>📊 Trading Bot Dashboard (Paper Trading)</h1>
<p style="color:#888; font-size:0.85em;">Strategy: EMA9 x EMA15 crossover only. Auto-refreshes every 30s.</p>

{% for name, s in instruments.items() %}
<div class="card">
  <div class="row">
    <b>{{ name }}</b>
    <span class="tag {{ tag_class[name] }}">{{ tag_label[name] }}</span>
  </div>
  <div class="row"><span>Last price</span><span>{{ "%.4f"|format(s.last_price) if s.last_price else "—" }}</span></div>
  <div class="row"><span>Total trades</span><span>{{ s.closed_trades|length }}</span></div>
  <div class="row"><span>Win rate</span><span>{{ win_rate[name] }}%</span></div>
  <div class="row"><span>Total P&amp;L (points)</span>
    <span class="{{ 'pos' if total_pnl[name] >= 0 else 'neg' }}">{{ "%+.4f"|format(total_pnl[name]) }}</span>
  </div>
  {% if s.position %}
  <div class="row"><span>Open position</span><span>Entry {{ "%.4f"|format(s.position.entry_price) }} @ {{ s.position.entry_time }}</span></div>
  {% endif %}
</div>
{% endfor %}

<div class="card">
  <b>Bot Activity Log</b><br><br>
  {% for line in logs %}
    <div class="log">{{ line }}</div>
  {% endfor %}
</div>

<script>setTimeout(() => location.reload(), 30000);</script>
</body>
</html>
"""

TAG_LABEL = {
    "NIFTY": "NSE", "BANKNIFTY": "NSE",
    "BITCOIN": "CRYPTO", "ETHEREUM": "CRYPTO",
    "EURUSD": "FOREX", "GBPUSD": "FOREX",
}
TAG_CLASS = {
    "NIFTY": "nse", "BANKNIFTY": "nse",
    "BITCOIN": "crypto", "ETHEREUM": "crypto",
    "EURUSD": "forex", "GBPUSD": "forex",
}


@app.route("/")
def dashboard():
    with state_lock:
        win_rate = {}
        total_pnl = {}
        for name, s in state.items():
            trades = s["closed_trades"]
            wins = len([t for t in trades if t["pnl"] > 0])
            win_rate[name] = round((wins / len(trades)) * 100, 1) if trades else 0
            total_pnl[name] = sum(t["pnl"] for t in trades)
        return render_template_string(
            DASHBOARD_TEMPLATE,
            instruments=state,
            tag_label=TAG_LABEL,
            tag_class=TAG_CLASS,
            win_rate=win_rate,
            total_pnl=total_pnl,
            logs=activity_log,
        )


@app.route("/api/data")
def api_data():
    with state_lock:
        return jsonify(state)


@app.route("/healthz")
def health_check():
    return "OK", 200


# Start the background worker thread once, when this module is imported
_worker_thread = threading.Thread(target=worker_loop, daemon=True)
_worker_thread.start()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
