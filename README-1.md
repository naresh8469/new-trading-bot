# Naya Trading Bot — EMA9 x EMA15 Crossover (Nifty + Crypto + Forex)

## Kya hai ye
Ye ek fresh, saaf trading bot hai jo teen tarah ke markets track karta hai:

- **NIFTY, BANKNIFTY** — India ke index (NSE market hours mein hi check hota
  hai: 9:15 AM - 3:30 PM, Mon-Fri)
- **BITCOIN, ETHEREUM** — Crypto (24/7 chalta hai, Binance se free price
  milta hai, koi key nahi chahiye)
- **EURUSD, GBPUSD** — Forex (~24/5 chalta hai, weekends chhodkar)

**Strategy: sirf EMA9 x EMA15 crossover.** Koi RSI filter nahi hai — jaisa
aapne maanga tha.

- Jab EMA9, EMA15 ke **upar** cross kare → BUY signal (paper position open)
- Jab EMA9, EMA15 ke **neeche** cross kare → us position ko close karke
  trade record ho jata hai

Har 60 second mein naya price check hota hai (isse "1-minute candle" maana
gaya hai).

## Ek zaroori API key chahiye — Twelve Data (sirf Forex ke liye)

Crypto aur Nifty ke liye koi key nahi chahiye. Forex (EURUSD, GBPUSD) ke liye
ek free key chahiye:

1. **twelvedata.com** kholiye, free account banaiye (card nahi chahiye)
2. Dashboard se apni **API Key** copy kar lijiye

## Render pe Deploy karne ke steps

1. Is folder (`app.py`, `requirements.txt`, `README.md`) ko GitHub par ek
   naye repo mein upload kijiye (jaise `new-trading-bot`)
2. Render dashboard → **New +** → **Web Service**
3. Us GitHub repo ko connect kijiye
4. Settings:
   - **Runtime:** Python 3
   - **Build Command:** `pip install -r requirements.txt`
   - **Start Command:** `gunicorn --workers 1 app:app`
     (⚠️ `--workers 1` zaroor likhiye, warna background worker do baar
     chalega aur double trades record honge)
   - **Instance Type:** Free
5. **Environment Variables** mein add kijiye:
   - `TWELVE_DATA_API_KEY` = aapki Twelve Data key
6. **Create Web Service** dabaiye

Deploy hone ke baad jo URL milega (jaise
`https://new-trading-bot-xxxx.onrender.com`), use kholkar aap live dashboard
dekh sakte hain — jaise pehle wale bot mein tha (Total trades, Win rate,
Total P&L, Open Positions, Bot Activity Log).

## Zaroori baat
- Ye **paper trading** hai — koi asli paisa involved nahi hai
- Data in-memory store hota hai — agar Render service restart ho jaye
  (jaise free tier "so" jaye aur phir jaage), to history reset ho jayegi
- Free Web Service 15 minute inactivity ke baad "so" sakti hai — isse
  bachne ke liye cron-job.org jaisi free service se har 10 minute mein
  URL ping karwaiye
