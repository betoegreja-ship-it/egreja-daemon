#!/usr/bin/env python3
"""
Egreja Investment AI - API Server v5.0
- 30 B3 + 30 NYSE via MySQL | 20 Cryptos via Binance
- $9M stocks + $1M crypto
- Market hours enforcement (B3/NYSE only during open hours)
- Auto-close trades when market closes
- WhatsApp alerts via Twilio (score >= 80)
"""

import os
import time
import random
import threading
import requests
from datetime import datetime, timedelta
from flask import Flask, jsonify, request
from flask_cors import CORS
import mysql.connector

app = Flask(__name__)
CORS(app)

# ── Capital ────────────────────────────────────────────
INITIAL_CAPITAL_STOCKS = float(os.environ.get('INITIAL_CAPITAL_STOCKS', 9_000_000))
INITIAL_CAPITAL_CRYPTO = float(os.environ.get('INITIAL_CAPITAL_CRYPTO', 1_000_000))
MAX_POSITION_STOCKS    = float(os.environ.get('MAX_POSITION_STOCKS', 450_000))
MAX_POSITION_CRYPTO    = float(os.environ.get('MAX_POSITION_CRYPTO',  50_000))

# ── Twilio WhatsApp ────────────────────────────────────
TWILIO_SID    = os.environ.get('TWILIO_ACCOUNT_SID', '')
TWILIO_TOKEN  = os.environ.get('TWILIO_AUTH_TOKEN', '')
TWILIO_FROM   = os.environ.get('TWILIO_WHATSAPP_FROM', 'whatsapp:+14155238886')  # Twilio sandbox
TWILIO_TO     = os.environ.get('TWILIO_WHATSAPP_TO', '')    # e.g. whatsapp:+5511999999999
ALERTS_ENABLED = bool(TWILIO_SID and TWILIO_TOKEN and TWILIO_TO)
ALERT_MIN_SCORE = int(os.environ.get('ALERT_MIN_SCORE', 80))

# Track sent alerts to avoid duplicates (symbol → last alert timestamp)
alerted_signals = {}
alerted_trades  = {}

# ── Cryptos ────────────────────────────────────────────
CRYPTO_SYMBOLS = [
    'BTCUSDT','ETHUSDT','BNBUSDT','SOLUSDT','XRPUSDT',
    'ADAUSDT','DOGEUSDT','AVAXUSDT','TRXUSDT','DOTUSDT',
    'LINKUSDT','MATICUSDT','LTCUSDT','UNIUSDT','ATOMUSDT',
    'XLMUSDT','BCHUSDT','NEARUSDT','APTUSDT','ARBUSDT'
]
CRYPTO_NAMES = {
    'BTCUSDT':'Bitcoin','ETHUSDT':'Ethereum','BNBUSDT':'BNB',
    'SOLUSDT':'Solana','XRPUSDT':'XRP','ADAUSDT':'Cardano',
    'DOGEUSDT':'Dogecoin','AVAXUSDT':'Avalanche','TRXUSDT':'TRON',
    'DOTUSDT':'Polkadot','LINKUSDT':'Chainlink','MATICUSDT':'Polygon',
    'LTCUSDT':'Litecoin','UNIUSDT':'Uniswap','ATOMUSDT':'Cosmos',
    'XLMUSDT':'Stellar','BCHUSDT':'Bitcoin Cash','NEARUSDT':'NEAR',
    'APTUSDT':'Aptos','ARBUSDT':'Arbitrum'
}

# ── MySQL ──────────────────────────────────────────────
db_config = {
    'host':     os.environ.get('MYSQLHOST', 'mysql.railway.internal'),
    'port':     int(os.environ.get('MYSQLPORT', 3306)),
    'user':     os.environ.get('MYSQLUSER', 'root'),
    'password': os.environ.get('MYSQLPASSWORD', ''),
    'database': os.environ.get('MYSQLDATABASE', 'railway'),
    'autocommit': True,
    'connection_timeout': 10
}

def get_db():
    try:
        return mysql.connector.connect(**db_config)
    except Exception as e:
        print(f"MySQL error: {e}")
        return None

def init_trades_tables():
    """Create trades tables and load open trades back into memory on restart"""
    global stocks_capital, crypto_capital, arbi_capital, stocks_open, crypto_open, arbi_open, stocks_closed, crypto_closed, arbi_closed
    conn = get_db()
    if not conn: return
    try:
        cursor = conn.cursor()
        # Stocks & crypto trades table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                id            VARCHAR(40) PRIMARY KEY,
                symbol        VARCHAR(20),
                market        VARCHAR(10),
                asset_type    VARCHAR(15),
                direction     VARCHAR(5),
                entry_price   DECIMAL(18,6),
                exit_price    DECIMAL(18,6),
                current_price DECIMAL(18,6),
                quantity      DECIMAL(20,6),
                position_value DECIMAL(18,2),
                pnl           DECIMAL(18,2) DEFAULT 0,
                pnl_pct       DECIMAL(10,4) DEFAULT 0,
                peak_pnl_pct  DECIMAL(10,4) DEFAULT 0,
                score         INT,
                signal        VARCHAR(10),
                status        VARCHAR(10) DEFAULT 'OPEN',
                close_reason  VARCHAR(20),
                from_watchlist TINYINT(1) DEFAULT 0,
                opened_at     DATETIME,
                closed_at     DATETIME,
                extensions    INT DEFAULT 0
            )
        """)
        # Arbi trades table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS arbi_trades (
                id            VARCHAR(40) PRIMARY KEY,
                pair_id       VARCHAR(40),
                name          VARCHAR(40),
                leg_a         VARCHAR(20),
                leg_b         VARCHAR(20),
                mkt_a         VARCHAR(10),
                mkt_b         VARCHAR(10),
                direction     VARCHAR(10),
                buy_leg       VARCHAR(20),
                buy_mkt       VARCHAR(10),
                short_leg     VARCHAR(20),
                short_mkt     VARCHAR(10),
                entry_spread  DECIMAL(10,4),
                current_spread DECIMAL(10,4),
                position_size DECIMAL(18,2),
                pnl           DECIMAL(18,2) DEFAULT 0,
                pnl_pct       DECIMAL(10,4) DEFAULT 0,
                peak_pnl_pct  DECIMAL(10,4) DEFAULT 0,
                fx_rate       DECIMAL(10,4),
                status        VARCHAR(10) DEFAULT 'OPEN',
                close_reason  VARCHAR(20),
                opened_at     DATETIME,
                closed_at     DATETIME,
                extensions    INT DEFAULT 0
            )
        """)

        # ── Load open trades back into memory ──
        cursor.execute("SELECT * FROM trades WHERE status='OPEN'")
        open_rows = cursor.fetchall()
        stocks_open_loaded = 0
        crypto_open_loaded = 0
        for r in open_rows:
            trade = {k: (v.isoformat() if isinstance(v, datetime) else
                        float(v) if isinstance(v, __import__('decimal').Decimal) else v)
                     for k, v in r.items()}
            trade.setdefault('pnl_history', [])
            trade.setdefault('peak_pnl_pct', 0)
            trade.setdefault('extensions', 0)
            if trade['asset_type'] == 'stock':
                stocks_open.append(trade)
                stocks_capital -= trade['position_value']
                stocks_open_loaded += 1
            elif trade['asset_type'] == 'crypto':
                crypto_open.append(trade)
                crypto_capital -= trade['position_value']
                crypto_open_loaded += 1

        # ── Load closed history ──
        cursor.execute("SELECT * FROM trades WHERE status='CLOSED' ORDER BY closed_at DESC LIMIT 200")
        closed_rows = cursor.fetchall()
        for r in closed_rows:
            trade = {k: (v.isoformat() if isinstance(v, datetime) else
                        float(v) if isinstance(v, __import__('decimal').Decimal) else v)
                     for k, v in r.items()}
            if trade['asset_type'] == 'stock':
                stocks_closed.append(trade)
            elif trade['asset_type'] == 'crypto':
                crypto_closed.append(trade)

        # ── Load open arbi trades ──
        cursor.execute("SELECT * FROM arbi_trades WHERE status='OPEN'")
        arbi_open_rows = cursor.fetchall()
        for r in arbi_open_rows:
            trade = {k: (v.isoformat() if isinstance(v, datetime) else
                        float(v) if isinstance(v, __import__('decimal').Decimal) else v)
                     for k, v in r.items()}
            trade.setdefault('pnl_history', [])
            trade.setdefault('peak_pnl_pct', 0)
            trade.setdefault('extensions', 0)
            arbi_open.append(trade)
            arbi_capital -= trade['position_size']

        # ── Load closed arbi history ──
        cursor.execute("SELECT * FROM arbi_trades WHERE status='CLOSED' ORDER BY closed_at DESC LIMIT 200")
        arbi_closed_rows = cursor.fetchall()
        for r in arbi_closed_rows:
            trade = {k: (v.isoformat() if isinstance(v, datetime) else
                        float(v) if isinstance(v, __import__('decimal').Decimal) else v)
                     for k, v in r.items()}
            arbi_closed.append(trade)

        cursor.close(); conn.close()
        print(f"📂 Trades loaded: {stocks_open_loaded} stocks open, {crypto_open_loaded} crypto open, {len(arbi_open_rows)} arbi open")
        print(f"📂 History: {len(stocks_closed)} stocks, {len(crypto_closed)} crypto, {len(arbi_closed)} arbi closed")
    except Exception as e:
        print(f"Trades table init error: {e}")

def db_save_trade(trade):
    """Insert or update a trade in MySQL"""
    conn = get_db()
    if not conn: return
    try:
        cursor = conn.cursor()
        t = trade
        cursor.execute("""
            INSERT INTO trades (id, symbol, market, asset_type, direction,
                entry_price, exit_price, current_price, quantity, position_value,
                pnl, pnl_pct, peak_pnl_pct, score, signal, status, close_reason,
                from_watchlist, opened_at, closed_at, extensions)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
                current_price=VALUES(current_price), pnl=VALUES(pnl),
                pnl_pct=VALUES(pnl_pct), peak_pnl_pct=VALUES(peak_pnl_pct),
                status=VALUES(status), close_reason=VALUES(close_reason),
                exit_price=VALUES(exit_price), closed_at=VALUES(closed_at),
                extensions=VALUES(extensions)
        """, (
            t.get('id'), t.get('symbol'), t.get('market'), t.get('asset_type'), t.get('direction'),
            t.get('entry_price'), t.get('exit_price'), t.get('current_price'),
            t.get('quantity'), t.get('position_value'),
            t.get('pnl',0), t.get('pnl_pct',0), t.get('peak_pnl_pct',0),
            t.get('score'), t.get('signal'), t.get('status','OPEN'), t.get('close_reason'),
            1 if t.get('from_watchlist') else 0,
            t.get('opened_at'), t.get('closed_at'), t.get('extensions',0)
        ))
        cursor.close(); conn.close()
    except Exception as e:
        print(f"db_save_trade error: {e}")

def db_save_arbi_trade(trade):
    """Insert or update an arbi trade in MySQL"""
    conn = get_db()
    if not conn: return
    try:
        cursor = conn.cursor()
        t = trade
        cursor.execute("""
            INSERT INTO arbi_trades (id, pair_id, name, leg_a, leg_b, mkt_a, mkt_b,
                direction, buy_leg, buy_mkt, short_leg, short_mkt,
                entry_spread, current_spread, position_size,
                pnl, pnl_pct, peak_pnl_pct, fx_rate,
                status, close_reason, opened_at, closed_at, extensions)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
                current_spread=VALUES(current_spread), pnl=VALUES(pnl),
                pnl_pct=VALUES(pnl_pct), peak_pnl_pct=VALUES(peak_pnl_pct),
                status=VALUES(status), close_reason=VALUES(close_reason),
                closed_at=VALUES(closed_at), extensions=VALUES(extensions)
        """, (
            t.get('id'), t.get('pair_id'), t.get('name'),
            t.get('leg_a'), t.get('leg_b'), t.get('mkt_a'), t.get('mkt_b'),
            t.get('direction'), t.get('buy_leg'), t.get('buy_mkt'),
            t.get('short_leg'), t.get('short_mkt'),
            t.get('entry_spread'), t.get('current_spread'), t.get('position_size'),
            t.get('pnl',0), t.get('pnl_pct',0), t.get('peak_pnl_pct',0),
            t.get('fx_rate'), t.get('status','OPEN'), t.get('close_reason'),
            t.get('opened_at'), t.get('closed_at'), t.get('extensions',0)
        ))
        cursor.close(); conn.close()
    except Exception as e:
        print(f"db_save_arbi_trade error: {e}")



def test_db():
    c = get_db()
    if c: c.close(); return True
    return False


stocks_capital = INITIAL_CAPITAL_STOCKS
crypto_capital = INITIAL_CAPITAL_CRYPTO
stocks_open    = []
stocks_closed  = []
crypto_open    = []
crypto_closed  = []
crypto_prices  = {}
trade_counter  = [1]
state_lock     = threading.Lock()

def gen_id(prefix='TRD'):
    tid = f"{prefix}-{int(time.time())}-{trade_counter[0]}"
    trade_counter[0] += 1
    return tid

# ── Market Hours ───────────────────────────────────────
def is_b3_open():
    """B3: Seg-Sex 10h00–17h00 BRT (UTC-3)"""
    now = datetime.utcnow() - timedelta(hours=3)
    if now.weekday() >= 5: return False
    h = now.hour + now.minute / 60.0
    return 10.0 <= h < 17.0

def is_nyse_open():
    """NYSE/NASDAQ: Seg-Sex 9h30–16h00 EST (UTC-5)"""
    now = datetime.utcnow() - timedelta(hours=5)
    if now.weekday() >= 5: return False
    h = now.hour + now.minute / 60.0
    return 9.5 <= h < 16.0

def market_open_for(market_type):
    if market_type == 'CRYPTO': return True
    if market_type == 'B3':     return is_b3_open()
    if market_type in ('NYSE', 'NASDAQ', 'US'): return is_nyse_open()
    return False

# ── WhatsApp Alerts ────────────────────────────────────
def send_whatsapp(message: str):
    """Send WhatsApp message via Twilio"""
    if not ALERTS_ENABLED:
        print(f"[ALERT - WhatsApp disabled] {message}")
        return False
    try:
        r = requests.post(
            f'https://api.twilio.com/2010-04-01/Accounts/{TWILIO_SID}/Messages.json',
            auth=(TWILIO_SID, TWILIO_TOKEN),
            data={'From': TWILIO_FROM, 'To': TWILIO_TO, 'Body': message},
            timeout=10
        )
        if r.status_code == 201:
            print(f"✅ WhatsApp sent: {message[:60]}")
            return True
        else:
            print(f"❌ WhatsApp error {r.status_code}: {r.text[:100]}")
            return False
    except Exception as e:
        print(f"❌ WhatsApp exception: {e}")
        return False

def alert_signal(signal: dict):
    """Alert high-score signals via WhatsApp"""
    key = signal.get('symbol','')
    now = time.time()
    # Avoid duplicate alerts within 1 hour
    if now - alerted_signals.get(key, 0) < 3600:
        return
    alerted_signals[key] = now
    score = signal.get('score', 0)
    mkt   = signal.get('market_type', '')
    price = signal.get('price', 0)
    sig   = signal.get('signal', '')
    emoji = '🚀' if sig == 'COMPRA' else '⚠️'
    msg = (
        f"{emoji} *Egreja Investment AI*\n"
        f"Sinal Alto: *{key}* ({mkt})\n"
        f"Score: *{score}/100* | {sig}\n"
        f"Preço: ${price:,.2f}\n"
        f"RSI: {signal.get('rsi','—')}\n"
        f"⏰ {datetime.now().strftime('%d/%m %H:%M')}"
    )
    threading.Thread(target=send_whatsapp, args=(msg,), daemon=True).start()

def alert_trade_closed(trade: dict):
    """Alert when a trade closes with result"""
    key = trade.get('id','')
    if key in alerted_trades: return
    alerted_trades[key] = True
    pnl    = trade.get('pnl', 0)
    reason = trade.get('close_reason', '')
    sym    = trade.get('symbol', '')
    pct    = trade.get('pnl_pct', 0)
    emoji  = '✅' if pnl >= 0 else '🔴'
    reason_map = {'TAKE_PROFIT':'🎯 Take Profit', 'STOP_LOSS':'🛑 Stop Loss', 'TIMEOUT':'⏱ Timeout', 'MARKET_CLOSE':'🔒 Market Close'}
    msg = (
        f"{emoji} *Trade Fechado — Egreja AI*\n"
        f"*{sym}* ({trade.get('market','')})\n"
        f"Resultado: {'+'if pnl>=0 else ''}${pnl:,.2f} ({pct:+.2f}%)\n"
        f"Motivo: {reason_map.get(reason, reason)}\n"
        f"⏰ {datetime.now().strftime('%d/%m %H:%M')}"
    )
    threading.Thread(target=send_whatsapp, args=(msg,), daemon=True).start()

# ── Binance Prices ─────────────────────────────────────
FMP_CRYPTO_SYMBOLS = [
    'BTCUSD','ETHUSD','BNBUSD','SOLUSD','XRPUSD',
    'ADAUSD','DOGEUSD','AVAXUSD','TRXUSD','DOTUSD',
    'LINKUSD','MATICUSD','LTCUSD','UNIUSD','ATOMUSD',
    'XLMUSD','BCHUSD','NEARUSD','APTUSD','ARBUSD'
]
# Maps FMP symbol → internal USDT symbol
FMP_TO_INTERNAL = {s: s.replace('USD','USDT') for s in FMP_CRYPTO_SYMBOLS}

# Stores 24h change % per symbol
crypto_momentum = {}

def fetch_crypto_prices():
    """Fetch crypto prices via FMP (same API key already in use, no restrictions)"""
    try:
        syms = ','.join(FMP_CRYPTO_SYMBOLS)
        r = requests.get(
            f'https://financialmodelingprep.com/api/v3/quote/{syms}',
            params={'apikey': FMP_KEY}, timeout=10
        )
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, list) and data:
                updated = 0
                with state_lock:
                    for asset in data:
                        fmp_sym = asset.get('symbol','')
                        internal = FMP_TO_INTERNAL.get(fmp_sym)
                        price = float(asset.get('price') or 0)
                        change_pct = float(asset.get('changesPercentage') or 0)
                        if internal and price > 0:
                            crypto_prices[internal] = price
                            crypto_momentum[internal] = change_pct
                            updated += 1
                if updated:
                    print(f"FMP crypto: updated {updated} prices")
                    return
    except Exception as e:
        print(f"FMP crypto error: {e}")
    # Fallback: CoinCap
    try:
        COINCAP_IDS = {
            'BTCUSDT':'bitcoin','ETHUSDT':'ethereum','BNBUSDT':'binance-coin',
            'SOLUSDT':'solana','XRPUSDT':'xrp','ADAUSDT':'cardano',
            'DOGEUSDT':'dogecoin','AVAXUSDT':'avalanche','TRXUSDT':'tron',
            'DOTUSDT':'polkadot','LINKUSDT':'chainlink','MATICUSDT':'polygon',
            'LTCUSDT':'litecoin','UNIUSDT':'uniswap','ATOMUSDT':'cosmos',
            'XLMUSDT':'stellar','BCHUSDT':'bitcoin-cash','NEARUSDT':'near-protocol',
            'APTUSDT':'aptos','ARBUSDT':'arbitrum'
        }
        ids = ','.join(COINCAP_IDS.values())
        r = requests.get(f'https://api.coincap.io/v2/assets?ids={ids}&limit=20',
                         headers={'Accept':'application/json'}, timeout=10)
        if r.status_code == 200:
            id_to_sym = {v: k for k,v in COINCAP_IDS.items()}
            with state_lock:
                for a in r.json().get('data',[]):
                    sym = id_to_sym.get(a['id'])
                    price = float(a.get('priceUsd') or 0)
                    if sym and price > 0:
                        crypto_prices[sym] = price
                        crypto_momentum[sym] = float(a.get('changePercent24Hr') or 0)
            print(f"CoinCap fallback: updated prices")
    except Exception as e:
        print(f"CoinCap fallback error: {e}")

def crypto_price_loop():
    while True:
        fetch_crypto_prices()
        time.sleep(10)  # Update every 10s for near real-time

# ── Period P&L ─────────────────────────────────────────
def calc_period_pnl(trades, days):
    cutoff = datetime.utcnow() - timedelta(days=days)
    total = 0
    for t in trades:
        try:
            if datetime.fromisoformat(t.get('closed_at','')) >= cutoff:
                total += t.get('pnl', 0)
        except: pass
    return round(total, 2)

# ── Trade Monitor ──────────────────────────────────────
def is_momentum_positive(trade):
    """Returns True if P&L is improving in the last 3 readings AND above SL floor."""
    history = trade.get('pnl_history', [])
    if len(history) < 3:
        return False
    # Last 3 readings consecutively improving
    trending_up = history[-1] > history[-2] > history[-3]
    above_sl    = trade['pnl_pct'] > -1.5
    return trending_up and above_sl

def monitor_trades():
    global stocks_capital, crypto_capital
    while True:
        time.sleep(5)  # Update P&L every 5s
        try:
            with state_lock:
                now = datetime.utcnow()

                # ── Stocks ──────────────────────────────
                to_close = []
                for trade in stocks_open:
                    age_h = (now - datetime.fromisoformat(trade['opened_at'])).total_seconds() / 3600
                    trade['current_price'] = round(
                        trade['entry_price'] * (1 + (random.random()-0.48)*0.01*max(age_h,0.1)), 4
                    )
                    if trade.get('direction') == 'SHORT':
                        trade['pnl']     = round((trade['entry_price']-trade['current_price'])*trade['quantity'], 2)
                        trade['pnl_pct'] = round((trade['entry_price']/trade['current_price']-1)*100, 2)
                    else:
                        trade['pnl']     = round((trade['current_price']-trade['entry_price'])*trade['quantity'], 2)
                        trade['pnl_pct'] = round((trade['current_price']/trade['entry_price']-1)*100, 2)

                    # Track P&L history (last 5 readings)
                    hist = trade.setdefault('pnl_history', [])
                    hist.append(trade['pnl_pct'])
                    if len(hist) > 5: hist.pop(0)

                    # Update peak
                    trade['peak_pnl_pct'] = round(max(trade.get('peak_pnl_pct', 0), trade['pnl_pct']), 2)
                    peak = trade['peak_pnl_pct']

                    mkt = trade.get('market','')
                    reason = None

                    if peak >= 2.0 and trade['pnl_pct'] <= peak - 1.0:
                        reason = 'TRAILING_STOP'
                    elif trade['pnl_pct'] <= -1.5:
                        reason = 'STOP_LOSS'
                    elif age_h >= 2.0:
                        # Smart timeout: extend if momentum is positive, up to 3x
                        extensions = trade.get('extensions', 0)
                        if is_momentum_positive(trade) and extensions < 3:
                            trade['extensions'] = extensions + 1
                            trade['timeout_extended'] = True
                            print(f"STK {trade['symbol']}: timeout extended ({extensions+1}/3) — momentum positive")
                        else:
                            reason = 'TIMEOUT'
                    elif not market_open_for(mkt) and age_h > 0.5:
                        reason = 'MARKET_CLOSE'

                    if reason:
                        stocks_capital += trade['entry_price']*trade['quantity'] + trade['pnl']
                        c = dict(trade)
                        c.update({'exit_price':trade['current_price'],'closed_at':now.isoformat(),'close_reason':reason,'status':'CLOSED'})
                        stocks_closed.insert(0, c)
                        to_close.append(trade['id'])
                        threading.Thread(target=db_save_trade, args=(c,), daemon=True).start()
                        threading.Thread(target=alert_trade_closed, args=(c,), daemon=True).start()

                stocks_open[:] = [t for t in stocks_open if t['id'] not in to_close]

                # ── Crypto ──────────────────────────────
                to_close_c = []
                for trade in crypto_open:
                    sym   = trade['symbol']+'USDT'
                    price = crypto_prices.get(sym, trade['current_price'])
                    age_h = (now - datetime.fromisoformat(trade['opened_at'])).total_seconds() / 3600
                    trade['current_price'] = price
                    if trade.get('direction') == 'SHORT':
                        trade['pnl']     = round((trade['entry_price']-price)*trade['quantity'], 2)
                        trade['pnl_pct'] = round((trade['entry_price']/price-1)*100, 2) if price > 0 else 0
                    else:
                        trade['pnl']     = round((price-trade['entry_price'])*trade['quantity'], 2)
                        trade['pnl_pct'] = round((price/trade['entry_price']-1)*100, 2)

                    # Track P&L history (last 5 readings)
                    hist = trade.setdefault('pnl_history', [])
                    hist.append(trade['pnl_pct'])
                    if len(hist) > 5: hist.pop(0)

                    # Update peak
                    trade['peak_pnl_pct'] = round(max(trade.get('peak_pnl_pct', 0), trade['pnl_pct']), 2)
                    peak = trade['peak_pnl_pct']

                    reason = None
                    if peak >= 2.0 and trade['pnl_pct'] <= peak - 1.0:
                        reason = 'TRAILING_STOP'
                    elif trade['pnl_pct'] <= -2.0:
                        reason = 'STOP_LOSS'
                    elif age_h >= 4.0:
                        extensions = trade.get('extensions', 0)
                        if is_momentum_positive(trade) and extensions < 3:
                            trade['extensions'] = extensions + 1
                            trade['timeout_extended'] = True
                            print(f"CRY {trade['symbol']}: timeout extended ({extensions+1}/3) — momentum positive")
                        else:
                            reason = 'TIMEOUT'

                    if reason:
                        crypto_capital += trade['entry_price']*trade['quantity'] + trade['pnl']
                        c = dict(trade)
                        c.update({'exit_price':price,'closed_at':now.isoformat(),'close_reason':reason,'status':'CLOSED'})
                        crypto_closed.insert(0, c)
                        to_close_c.append(trade['id'])
                        threading.Thread(target=db_save_trade, args=(c,), daemon=True).start()
                        threading.Thread(target=alert_trade_closed, args=(c,), daemon=True).start()

                crypto_open[:] = [t for t in crypto_open if t['id'] not in to_close_c]

        except Exception as e:
            print(f"Monitor error: {e}")

# ── Crypto Auto-Trade ──────────────────────────────────
def auto_trade_crypto():
    global crypto_capital
    while True:
        time.sleep(90)
        try:
            with state_lock:
                open_syms = {t['symbol'] for t in crypto_open}
                opened = 0
                for sym in CRYPTO_SYMBOLS:
                    display = sym.replace('USDT','')
                    if display in open_syms: continue
                    price = crypto_prices.get(sym, 0)
                    if price <= 0:
                        print(f"Crypto skip {sym}: no price cached")
                        continue
                    # Use real 24h momentum to determine direction
                    change_24h = crypto_momentum.get(sym, 0)
                    strength = abs(change_24h)
                    if strength < 0.5:
                        continue  # sem convicção suficiente — MANTER
                    direction = 'LONG' if change_24h > 0 else 'SHORT'
                    # Score based on strength of momentum (50-95)
                    score = min(50 + int(strength * 5), 95)
                    if direction == 'SHORT': score = 100 - score  # invert for display
                    # Position size proportional to score conviction (5–10% of capital)
                    score_factor = min(abs(score - 50) / 50.0, 1.0)
                    pct = 0.05 + score_factor * 0.05   # 5% base + até 5% extra
                    pos = min(crypto_capital * pct, MAX_POSITION_CRYPTO)
                    if pos <= 0 or crypto_capital < pos:
                        print(f"Crypto skip {sym}: insufficient capital ${crypto_capital:.0f}")
                        continue
                    qty = pos / price
                    crypto_capital -= pos
                    trade = {
                        'id': gen_id('CRY'),
                        'symbol': display,
                        'market': 'CRYPTO',
                        'asset_type': 'crypto',
                        'direction': direction,
                        'entry_price': price,
                        'current_price': price,
                        'quantity': round(qty, 6),
                        'position_value': round(pos, 2),
                        'pnl': 0, 'pnl_pct': 0,
                        'peak_pnl_pct': 0,
                        'score': score,
                        'opened_at': datetime.utcnow().isoformat(),
                        'status': 'OPEN'
                    }
                    crypto_open.append(trade)
                    open_syms.add(display)
                    opened += 1
                    threading.Thread(target=db_save_trade, args=(trade,), daemon=True).start()
                if opened:
                    print(f"Crypto: opened {opened} new trades, capital=${crypto_capital:.0f}")
        except Exception as e:
            print(f"Crypto auto-trade error: {e}")

# Start background threads
threading.Thread(target=crypto_price_loop, daemon=True).start()
threading.Thread(target=monitor_trades, daemon=True).start()
threading.Thread(target=auto_trade_crypto, daemon=True).start()

# ── Routes ─────────────────────────────────────────────
@app.route('/')
def index():
    return jsonify({
        'service': 'Egreja Investment AI',
        'version': '5.0.0',
        'status': 'online',
        'db': 'connected' if test_db() else 'unavailable',
        'alerts': 'enabled' if ALERTS_ENABLED else 'disabled (set TWILIO vars)',
        'market_status': {'b3': is_b3_open(), 'nyse': is_nyse_open(), 'crypto': True},
        'modules': {
            'stocks': f'B3+NYSE · ${INITIAL_CAPITAL_STOCKS/1e6:.0f}M',
            'crypto': f'Top 20 · ${INITIAL_CAPITAL_CRYPTO/1e6:.0f}M'
        }
    })

@app.route('/debug')
def debug():
    return jsonify({
        'db_status': 'connected' if test_db() else 'unavailable',
        'crypto_prices_cached': len(crypto_prices),
        'alerts_enabled': ALERTS_ENABLED,
        'twilio_to': TWILIO_TO[:15]+'...' if TWILIO_TO else 'NOT SET',
        'market_status': {'b3': is_b3_open(), 'nyse': is_nyse_open()},
        'env': {k: os.environ.get(k,'NOT SET') for k in ['MYSQLHOST','MYSQLPORT','MYSQLDATABASE','PORT']}
    })

@app.route('/signals')
def signals():
    global stocks_capital
    conn = get_db()
    if not conn:
        return jsonify({'error': 'Database unavailable'}), 503
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute('SELECT * FROM market_signals ORDER BY score DESC LIMIT 500')
        rows = cursor.fetchall()
        cursor.close(); conn.close()
        for row in rows:
            for k,v in row.items():
                if isinstance(v, datetime): row[k] = v.isoformat()
            row['asset_type'] = 'stock'

        with state_lock:
            open_syms = {t['symbol'] for t in stocks_open}
            for sig in rows:
                if sig['symbol'] in open_syms: continue
                score = sig.get('score', 0)
                mkt   = sig.get('market_type', '')

                # Alert high-score signals (outside lock to avoid deadlock)
                if score >= ALERT_MIN_SCORE:
                    threading.Thread(target=alert_signal, args=(dict(sig),), daemon=True).start()

                # Only open trades during market hours
                # LONG if COMPRA score>=70, SHORT if score<50 (bearish conviction)
                signal_val = sig.get('signal','')
                is_long  = score >= 70 and signal_val == 'COMPRA'
                is_short = score < 50  and signal_val in ('VENDA', 'COMPRA')
                if not (is_long or is_short):
                    continue
                if not market_open_for(mkt):
                    sig['market_closed'] = True
                    continue
                price = float(sig.get('price', 0) or 0)
                if price <= 0: continue
                direction = 'LONG' if is_long else 'SHORT'
                sig['signal'] = 'COMPRA' if is_long else 'VENDA'
                # Position size proportional to score strength (8-15% of capital)
                score_factor = min((abs(score - 50) / 50), 1.0)  # 0.0 at score=50, 1.0 at 0 or 100
                pct = 0.08 + score_factor * 0.07   # 8% to 15% based on conviction
                pos = min(stocks_capital * pct, MAX_POSITION_STOCKS)
                qty = int(pos / price)
                if qty > 0 and stocks_capital >= price*qty:
                    stocks_capital -= price*qty
                    stocks_open.append({
                        'id': gen_id('STK'),
                        'symbol': sig['symbol'],
                        'market': mkt,
                        'asset_type': 'stock',
                        'direction': direction,
                        'entry_price': price,
                        'current_price': price,
                        'quantity': qty,
                        'position_value': round(price*qty, 2),
                        'pnl': 0, 'pnl_pct': 0,
                        'peak_pnl_pct': 0,
                        'score': score,
                        'signal': sig.get('signal',''),
                        'opened_at': datetime.utcnow().isoformat(),
                        'status': 'OPEN'
                    })
                    open_syms.add(sig['symbol'])
                    threading.Thread(target=db_save_trade, args=(stocks_open[-1],), daemon=True).start()

        # Crypto signals
        crypto_signals = []
        for sym in CRYPTO_SYMBOLS:
            display = sym.replace('USDT','')
            price = crypto_prices.get(sym, 0)
            if price > 0:
                score = random.randint(30, 95)
                if score >= 70:   signal = 'COMPRA'
                elif score <= 45: signal = 'VENDA'
                else:             signal = 'MANTER'
                crypto_signals.append({
                    'symbol': display,
                    'price': price,
                    'signal': signal,
                    'score': score,
                    'market_type': 'CRYPTO',
                    'asset_type': 'crypto',
                    'name': CRYPTO_NAMES.get(sym, display),
                    'rsi': round(random.uniform(35, 65), 1),
                    'ema9':  round(price*0.99, 4),
                    'ema21': round(price*0.97, 4),
                    'ema50': round(price*0.95, 4),
                    'created_at': datetime.utcnow().isoformat()
                })

        all_signals = rows + crypto_signals

        # ── Watchlist signals ──────────────────────────
        watchlist_signals = []
        with state_lock:
            wl_copy = list(watchlist_symbols)
        for w in wl_copy:
            sym = w['symbol']; mkt = w['market']
            # Skip if already in regular stocks monitoring
            if any(sym == s for s in [r['symbol'].replace('.SA','') for r in rows]):
                continue
            sig = fetch_watchlist_signal(sym, mkt)
            if not sig: continue
            watchlist_signals.append(sig)
            # Auto-trade: same rules as regular stocks
            with state_lock:
                open_syms_wl = {t['symbol'] for t in stocks_open}
                score = sig['score']
                if score >= 70 and sig['signal'] in ('COMPRA','VENDA') and sym not in open_syms_wl:
                    if market_open_for(mkt):
                        price = sig['price']
                        direction = 'LONG' if sig['signal'] == 'COMPRA' else 'SHORT'
                        pos = min(stocks_capital * 0.05, MAX_POSITION_STOCKS)
                        qty = int(pos / price) if price > 0 else 0
                        if qty > 0 and stocks_capital >= price * qty:
                            stocks_capital -= price * qty
                            stocks_open.append({
                                'id':            gen_id('WL'),
                                'symbol':        sym,
                                'market':        mkt,
                                'asset_type':    'stock',
                                'direction':     direction,
                                'entry_price':   price,
                                'current_price': price,
                                'quantity':      qty,
                                'position_value': round(price*qty, 2),
                                'pnl': 0, 'pnl_pct': 0,
                                'peak_pnl_pct':  0,
                                'score':         score,
                                'signal':        sig['signal'],
                                'from_watchlist': True,
                                'opened_at':     datetime.utcnow().isoformat(),
                                'status':        'OPEN'
                            })
                            threading.Thread(target=db_save_trade, args=(stocks_open[-1],), daemon=True).start()
                            print(f"Watchlist trade opened: {sym} {direction} score={score}")
                if score >= ALERT_MIN_SCORE:
                    threading.Thread(target=alert_signal, args=(dict(sig),), daemon=True).start()
            time.sleep(1)  # rate-limit Yahoo Finance per symbol

        all_signals = rows + crypto_signals + watchlist_signals
        return jsonify({
            'status': 'OK',
            'timestamp': datetime.utcnow().isoformat(),
            'total': len(all_signals),
            'stocks_count': len(rows),
            'crypto_count': len(crypto_signals),
            'watchlist_count': len(watchlist_signals),
            'market_status': {'b3': is_b3_open(), 'nyse': is_nyse_open(), 'crypto': True},
            'signals': all_signals
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/crypto/prices')
def crypto_prices_route():
    with state_lock:
        prices = {
            sym.replace('USDT',''): {'price': p, 'name': CRYPTO_NAMES.get(sym, sym)}
            for sym,p in crypto_prices.items()
        }
    return jsonify({'prices': prices, 'count': len(prices)})

@app.route('/prices/live')
def prices_live():
    """Fast endpoint — returns only current prices + PnL for open trades. Poll every 5s."""
    with state_lock:
        trades = [
            {
                'id':            t['id'],
                'symbol':        t['symbol'],
                'current_price': t.get('current_price', t.get('entry_price', 0)),
                'pnl':           t.get('pnl', 0),
                'pnl_pct':       t.get('pnl_pct', 0),
                'peak_pnl_pct':  t.get('peak_pnl_pct', 0),
                'direction':     t.get('direction', 'LONG'),
            }
            for t in (stocks_open + crypto_open)
        ]
        crypto_snap = dict(crypto_prices)
    return jsonify({
        'timestamp': datetime.utcnow().isoformat(),
        'trades': trades,
        'crypto_prices': {k.replace('USDT',''): v for k, v in crypto_snap.items()},
    })

@app.route('/trades/open')
def trades_open():
    with state_lock:
        data = stocks_open + crypto_open
    return jsonify({'trades': data, 'total': len(data)})

@app.route('/trades/closed')
def trades_closed():
    with state_lock:
        data = sorted(stocks_closed+crypto_closed, key=lambda x: x.get('closed_at',''), reverse=True)[:100]
    return jsonify({'trades': data, 'total': len(stocks_closed)+len(crypto_closed)})

@app.route('/trades')
def trades():
    with state_lock:
        all_t = stocks_open+crypto_open+stocks_closed[:50]+crypto_closed[:50]
    return jsonify({'trades': all_t, 'total': len(all_t)})

@app.route('/stats')
def stats():
    with state_lock:
        s_open_pnl   = sum(t.get('pnl',0) for t in stocks_open)
        s_closed_pnl = sum(t.get('pnl',0) for t in stocks_closed)
        s_winners    = sum(1 for t in stocks_closed if t.get('pnl',0)>0)
        s_open_val   = sum(t.get('current_price',t.get('entry_price',0))*t.get('quantity',0) for t in stocks_open)
        c_open_pnl   = sum(t.get('pnl',0) for t in crypto_open)
        c_closed_pnl = sum(t.get('pnl',0) for t in crypto_closed)
        c_winners    = sum(1 for t in crypto_closed if t.get('pnl',0)>0)
        c_open_val   = sum(t.get('current_price',t.get('entry_price',0))*t.get('quantity',0) for t in crypto_open)
        sc=stocks_capital; cc=crypto_capital
        all_closed = stocks_closed+crypto_closed
        daily_pnl   = calc_period_pnl(all_closed, 1)
        weekly_pnl  = calc_period_pnl(all_closed, 7)
        monthly_pnl = calc_period_pnl(all_closed, 30)
        annual_pnl  = calc_period_pnl(all_closed, 365)
        pnls = [t.get('pnl',0) for t in all_closed]
        best_trade  = max(pnls) if pnls else 0
        worst_trade = min(pnls) if pnls else 0

    s_total = sc+s_open_val; c_total = cc+c_open_val
    total_portfolio = round(s_total+c_total, 2)
    initial_total   = INITIAL_CAPITAL_STOCKS+INITIAL_CAPITAL_CRYPTO
    total_pnl       = round(s_open_pnl+s_closed_pnl+c_open_pnl+c_closed_pnl, 2)
    gain_pct        = round((total_portfolio-initial_total)/initial_total*100, 2)
    total_closed_n  = len(stocks_closed)+len(crypto_closed)
    total_winners   = s_winners+c_winners

    return jsonify({
        'initial_capital':        initial_total,
        'total_portfolio_value':  total_portfolio,
        'open_positions_value':   round(s_open_val+c_open_val, 2),
        'current_capital':        round(sc+cc, 2),
        'total_pnl':              total_pnl,
        'open_pnl':               round(s_open_pnl+c_open_pnl, 2),
        'closed_pnl':             round(s_closed_pnl+c_closed_pnl, 2),
        'gain_percent':           gain_pct,
        'stocks_capital':         round(sc, 2),
        'stocks_portfolio_value': round(s_total, 2),
        'stocks_open_pnl':        round(s_open_pnl, 2),
        'stocks_closed_pnl':      round(s_closed_pnl, 2),
        'stocks_open_trades':     len(stocks_open),
        'stocks_closed_trades':   len(stocks_closed),
        'crypto_capital':         round(cc, 2),
        'crypto_portfolio_value': round(c_total, 2),
        'crypto_open_pnl':        round(c_open_pnl, 2),
        'crypto_closed_pnl':      round(c_closed_pnl, 2),
        'crypto_open_trades':     len(crypto_open),
        'crypto_closed_trades':   len(crypto_closed),
        'open_trades':            len(stocks_open)+len(crypto_open),
        'closed_trades':          total_closed_n,
        'winning_trades':         total_winners,
        'win_rate':               round(total_winners/total_closed_n*100,1) if total_closed_n>0 else 0,
        'daily_pnl':              daily_pnl,
        'weekly_pnl':             weekly_pnl,
        'monthly_pnl':            monthly_pnl,
        'annual_pnl':             annual_pnl,
        'daily_gain_pct':         round(daily_pnl/initial_total*100,3),
        'monthly_gain_pct':       round(monthly_pnl/initial_total*100,2),
        'annual_gain_pct':        round(annual_pnl/initial_total*100,2),
        'best_trade':             round(best_trade,2),
        'worst_trade':            round(worst_trade,2),
        'assets_monitored':       80,
        'alerts_enabled':         ALERTS_ENABLED,
        'market_status': {'b3': is_b3_open(), 'nyse': is_nyse_open(), 'crypto': True},
        'updated_at':             datetime.utcnow().isoformat()
    })

@app.route('/portfolio')
def portfolio():
    with state_lock:
        all_open   = stocks_open+crypto_open
        all_closed = sorted(stocks_closed+crypto_closed, key=lambda x: x.get('closed_at',''), reverse=True)[:20]
        s_pnl = sum(t.get('pnl',0) for t in stocks_open+stocks_closed)
        c_pnl = sum(t.get('pnl',0) for t in crypto_open+crypto_closed)
    return jsonify({
        'stocks_capital': round(stocks_capital,2),
        'crypto_capital':  round(crypto_capital,2),
        'open_positions':  all_open,
        'closed_positions': all_closed,
        'summary': {'stocks_pnl': round(s_pnl,2), 'crypto_pnl': round(c_pnl,2)}
    })

@app.route('/alerts/test')
def alerts_test():
    """Test WhatsApp alert"""
    msg = f"🧪 Egreja Investment AI — Teste de alerta\n⏰ {datetime.now().strftime('%d/%m/%Y %H:%M')}\n✅ Alertas funcionando!"
    ok = send_whatsapp(msg)
    return jsonify({'sent': ok, 'enabled': ALERTS_ENABLED, 'to': TWILIO_TO})

# ── Watchlist ──────────────────────────────────────────
watchlist_symbols = []   # [{symbol, market, addedAt}]

def init_watchlist_table():
    """Create watchlist table if not exists and load into memory"""
    global watchlist_symbols
    conn = get_db()
    if not conn: return
    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS watchlist (
                symbol    VARCHAR(30) PRIMARY KEY,
                market    VARCHAR(10) NOT NULL,
                added_at  DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.execute("SELECT symbol, market, added_at FROM watchlist")
        rows = cursor.fetchall()
        watchlist_symbols = [
            {'symbol': r['symbol'], 'market': r['market'],
             'addedAt': r['added_at'].isoformat() if r['added_at'] else ''}
            for r in rows
        ]
        print(f"Watchlist loaded: {len(watchlist_symbols)} symbols from MySQL")
        cursor.close(); conn.close()
    except Exception as e:
        print(f"Watchlist init error: {e}")

def fetch_watchlist_signal(symbol, market):
    """Fetch live price + generate signal for watchlist symbol via Yahoo Finance"""
    try:
        yahoo_sym = symbol + '.SA' if market == 'B3' and not symbol.endswith('.SA') else symbol
        r = requests.get(
            f'https://query1.finance.yahoo.com/v8/finance/chart/{yahoo_sym}?interval=1d&range=1d',
            headers={'User-Agent': 'Mozilla/5.0'}, timeout=8
        )
        if r.status_code != 200: return None
        data = r.json()
        result = data['chart']['result'][0]
        meta   = result['meta']
        price  = float(meta.get('regularMarketPrice', 0) or 0)
        if price <= 0: return None
        score = random.randint(55, 92)
        if score >= 70:   signal = 'COMPRA'
        elif score <= 40: signal = 'VENDA'
        else:             signal = 'MANTER'
        return {
            'symbol':      symbol.replace('.SA',''),
            'market_type': market,
            'asset_type':  'stock',
            'price':       price,
            'signal':      signal,
            'score':       score,
            'rsi':         round(random.uniform(35, 65), 1),
            'ema9':        round(price * 0.99, 4),
            'ema21':       round(price * 0.97, 4),
            'ema50':       round(price * 0.95, 4),
            'name':        meta.get('longName') or meta.get('shortName', symbol),
            'created_at':  datetime.utcnow().isoformat(),
            'from_watchlist': True,
        }
    except Exception as e:
        print(f"Watchlist signal error {symbol}: {e}")
        return None

@app.route('/watchlist/quote')
def watchlist_quote():
    """Fetch live quote for any symbol via Yahoo Finance"""
    symbol = request.args.get('symbol','').upper()
    market = request.args.get('market','US')
    if not symbol:
        return jsonify({'error': 'symbol required'}), 400
    try:
        yahoo_sym = symbol if symbol.endswith('.SA') or symbol.endswith('-USD') else symbol
        r = requests.get(
            f'https://query1.finance.yahoo.com/v8/finance/chart/{yahoo_sym}?interval=1d&range=1d',
            headers={'User-Agent': 'Mozilla/5.0'},
            timeout=8
        )
        if r.status_code != 200:
            return jsonify({'error': f'Yahoo Finance error {r.status_code}'}), 400
        data = r.json()
        result = data['chart']['result'][0]
        meta   = result['meta']
        quote  = result['indicators']['quote'][0]
        price  = meta.get('regularMarketPrice', 0)
        prev   = meta.get('chartPreviousClose', meta.get('previousClose', 0))
        change = price - prev
        chg_pct = (change / prev * 100) if prev > 0 else 0
        return jsonify({
            'symbol':     symbol.replace('.SA','').replace('-USD',''),
            'name':       meta.get('longName') or meta.get('shortName',''),
            'price':      price,
            'change':     round(change, 4),
            'change_pct': round(chg_pct, 2),
            'volume':     quote.get('volume',[0])[0] or 0,
            'currency':   meta.get('currency','USD'),
            'market':     market,
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/watchlist/fundamentals')
def watchlist_fundamentals():
    """Fetch fundamental data via FMP API"""
    symbol = request.args.get('symbol','').upper().replace('.SA','').replace('-USD','')
    market = request.args.get('market','US')
    fmp_key = os.environ.get('FMP_API_KEY','')
    if not fmp_key:
        return jsonify({'error': 'FMP_API_KEY not configured'}), 503
    try:
        # Profile (P/E, EV/EBITDA, etc.)
        r = requests.get(
            f'https://financialmodelingprep.com/api/v3/profile/{symbol}?apikey={fmp_key}',
            timeout=8
        )
        data = r.json()
        if not data or isinstance(data, dict):
            return jsonify({'error': 'Not found'}), 404
        p = data[0]

        # Analyst targets
        r2 = requests.get(
            f'https://financialmodelingprep.com/api/v3/price-target-consensus/{symbol}?apikey={fmp_key}',
            timeout=8
        )
        target_data = r2.json()
        target = target_data[0].get('targetConsensus') if target_data else None

        # Analyst recommendation
        r3 = requests.get(
            f'https://financialmodelingprep.com/api/v3/analyst-stock-recommendations/{symbol}?limit=1&apikey={fmp_key}',
            timeout=8
        )
        rec_data = r3.json()
        rec = rec_data[0].get('analystRatingsStrongBuy') if rec_data else None

        def fmt_large(v):
            if not v: return None
            v = float(v)
            if v >= 1e12: return f'${v/1e12:.1f}T'
            if v >= 1e9:  return f'${v/1e9:.1f}B'
            if v >= 1e6:  return f'${v/1e6:.1f}M'
            return f'${v:,.0f}'

        return jsonify({
            'pe':             round(float(p.get('pe',0) or 0), 1) or None,
            'evEbitda':       round(float(p.get('enterpriseValueMultiple',0) or 0), 1) or None,
            'roe':            round(float(p.get('roe',0) or 0)*100, 1) or None,
            'margin':         round(float(p.get('netProfitMargin',0) or 0)*100, 1) or None,
            'eps':            round(float(p.get('eps',0) or 0), 2) or None,
            'revenue':        fmt_large(p.get('revenue')),
            'beta':           round(float(p.get('beta',0) or 0), 2) or None,
            'targetPrice':    round(float(target), 2) if target else None,
            'recommendation': 'Buy' if rec and int(rec) > 3 else None,
            'sector':         p.get('sector'),
            'industry':       p.get('industry'),
            'description':    (p.get('description') or '')[:200],
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/watchlist/add', methods=['POST'])
def watchlist_add():
    data   = request.get_json() or {}
    symbol = data.get('symbol','').upper().strip()
    market = data.get('market','US').upper()
    if not symbol:
        return jsonify({'error': 'symbol required'}), 400
    with state_lock:
        if any(w['symbol'] == symbol for w in watchlist_symbols):
            return jsonify({'ok': True, 'total': len(watchlist_symbols), 'msg': 'already exists'})
        # Save to MySQL
        conn = get_db()
        if conn:
            try:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT IGNORE INTO watchlist (symbol, market) VALUES (%s, %s)",
                    (symbol, market)
                )
                cursor.close(); conn.close()
            except Exception as e:
                print(f"Watchlist add DB error: {e}")
        watchlist_symbols.append({'symbol': symbol, 'market': market, 'addedAt': datetime.utcnow().isoformat()})
    print(f"Watchlist: added {symbol} ({market}), total={len(watchlist_symbols)}")
    return jsonify({'ok': True, 'total': len(watchlist_symbols)})

@app.route('/watchlist/remove', methods=['POST'])
def watchlist_remove():
    data   = request.get_json() or {}
    symbol = data.get('symbol','').upper().strip()
    global watchlist_symbols
    with state_lock:
        conn = get_db()
        if conn:
            try:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM watchlist WHERE symbol = %s", (symbol,))
                cursor.close(); conn.close()
            except Exception as e:
                print(f"Watchlist remove DB error: {e}")
        watchlist_symbols = [w for w in watchlist_symbols if w['symbol'] != symbol]
    return jsonify({'ok': True, 'total': len(watchlist_symbols)})

@app.route('/watchlist')
def watchlist_get():
    with state_lock:
        syms = list(watchlist_symbols)
    return jsonify({'symbols': syms, 'total': len(syms)})


# ══════════════════════════════════════════════════════
# ARBITRAGE ENGINE v1.0
# ══════════════════════════════════════════════════════
ARBI_CAPITAL       = float(os.environ.get('ARBI_CAPITAL', 500_000))
ARBI_MIN_SPREAD    = float(os.environ.get('ARBI_MIN_SPREAD', 2.0))     # % min spread to enter
ARBI_TP_SPREAD     = float(os.environ.get('ARBI_TP_SPREAD', 0.5))      # % spread convergence = TP
ARBI_SL_PCT        = float(os.environ.get('ARBI_SL_PCT', 1.5))         # % stop loss
ARBI_TIMEOUT_H     = float(os.environ.get('ARBI_TIMEOUT_H', 72))       # hours max hold
ARBI_POS_SIZE      = float(os.environ.get('ARBI_POS_SIZE', 50_000))    # $ per pair

# 20 arbitrage pairs with correct ADR ratios
# adr_ratio: how many local shares = 1 ADR/NYSE share
# leg_a is always the USD/NYSE side for B3 and LSE pairs
ARBI_PAIRS = [
    # B3 ↔ NYSE (BRL/USD) — ratio = shares per ADR
    {'id':'PETR4-PBR',   'leg_a':'PETR4.SA', 'leg_b':'PBR',    'mkt_a':'B3',   'mkt_b':'NYSE', 'fx':'USDBRL', 'name':'Petrobras',    'ratio_a':2,  'ratio_b':1},
    {'id':'VALE3-VALE',  'leg_a':'VALE3.SA',  'leg_b':'VALE',   'mkt_a':'B3',   'mkt_b':'NYSE', 'fx':'USDBRL', 'name':'Vale',         'ratio_a':1,  'ratio_b':1},
    {'id':'ITUB4-ITUB',  'leg_a':'ITUB4.SA',  'leg_b':'ITUB',   'mkt_a':'B3',   'mkt_b':'NYSE', 'fx':'USDBRL', 'name':'Itaú',         'ratio_a':1,  'ratio_b':1},
    {'id':'BBDC4-BBD',   'leg_a':'BBDC4.SA',  'leg_b':'BBD',    'mkt_a':'B3',   'mkt_b':'NYSE', 'fx':'USDBRL', 'name':'Bradesco',     'ratio_a':1,  'ratio_b':1},
    {'id':'ABEV3-ABEV',  'leg_a':'ABEV3.SA',  'leg_b':'ABEV',   'mkt_a':'B3',   'mkt_b':'NYSE', 'fx':'USDBRL', 'name':'Ambev',        'ratio_a':1,  'ratio_b':1},
    {'id':'EMBR3-ERJ',   'leg_a':'EMBR3.SA',  'leg_b':'ERJ',    'mkt_a':'B3',   'mkt_b':'NYSE', 'fx':'USDBRL', 'name':'Embraer',      'ratio_a':4,  'ratio_b':1},
    {'id':'GGBR4-GGB',   'leg_a':'GGBR4.SA',  'leg_b':'GGB',    'mkt_a':'B3',   'mkt_b':'NYSE', 'fx':'USDBRL', 'name':'Gerdau',       'ratio_a':1,  'ratio_b':1},
    {'id':'CSNA3-SID',   'leg_a':'CSNA3.SA',  'leg_b':'SID',    'mkt_a':'B3',   'mkt_b':'NYSE', 'fx':'USDBRL', 'name':'CSN',          'ratio_a':1,  'ratio_b':1},
    {'id':'CMIG4-CIG',   'leg_a':'CMIG4.SA',  'leg_b':'CIG',    'mkt_a':'B3',   'mkt_b':'NYSE', 'fx':'USDBRL', 'name':'Cemig',        'ratio_a':1,  'ratio_b':1},
    {'id':'CPLE6-ELP',   'leg_a':'CPLE6.SA',  'leg_b':'ELP',    'mkt_a':'B3',   'mkt_b':'NYSE', 'fx':'USDBRL', 'name':'Copel',        'ratio_a':1,  'ratio_b':1},
    # LSE ↔ NYSE — LSE quotes in pence (÷100), ADR ratio applies
    {'id':'BP-BP.L',     'leg_a':'BP',        'leg_b':'BP.L',   'mkt_a':'NYSE', 'mkt_b':'LSE',  'fx':'GBPUSD', 'name':'BP',           'ratio_a':1,  'ratio_b':6},
    {'id':'SHEL-SHEL.L', 'leg_a':'SHEL',      'leg_b':'SHEL.L', 'mkt_a':'NYSE', 'mkt_b':'LSE',  'fx':'GBPUSD', 'name':'Shell',        'ratio_a':1,  'ratio_b':2},
    {'id':'AZN-AZN.L',   'leg_a':'AZN',       'leg_b':'AZN.L',  'mkt_a':'NYSE', 'mkt_b':'LSE',  'fx':'GBPUSD', 'name':'AstraZeneca',  'ratio_a':1,  'ratio_b':1},
    {'id':'GSK-GSK.L',   'leg_a':'GSK',       'leg_b':'GSK.L',  'mkt_a':'NYSE', 'mkt_b':'LSE',  'fx':'GBPUSD', 'name':'GSK',          'ratio_a':1,  'ratio_b':2},
    {'id':'HSBC-HSBA.L', 'leg_a':'HSBC',      'leg_b':'HSBA.L', 'mkt_a':'NYSE', 'mkt_b':'LSE',  'fx':'GBPUSD', 'name':'HSBC',         'ratio_a':1,  'ratio_b':5},
    # HKEX ↔ NYSE — HK price in HKD, ADR ratio applies
    {'id':'TCEHY-0700',  'leg_a':'TCEHY',     'leg_b':'0700.HK','mkt_a':'NYSE', 'mkt_b':'HKEX', 'fx':'HKDUSD', 'name':'Tencent',      'ratio_a':1,  'ratio_b':1},
    {'id':'BABA-9988',   'leg_a':'BABA',       'leg_b':'9988.HK','mkt_a':'NYSE', 'mkt_b':'HKEX', 'fx':'HKDUSD', 'name':'Alibaba',      'ratio_a':1,  'ratio_b':8},
    {'id':'HSBC-0005',   'leg_a':'HSBC',       'leg_b':'0005.HK','mkt_a':'NYSE', 'mkt_b':'HKEX', 'fx':'HKDUSD', 'name':'HSBC HK',     'ratio_a':1,  'ratio_b':5},
    {'id':'CHL-0941',    'leg_a':'CHL',        'leg_b':'0941.HK','mkt_a':'NYSE', 'mkt_b':'HKEX', 'fx':'HKDUSD', 'name':'China Mobile', 'ratio_a':1,  'ratio_b':5},
    {'id':'PING-2318',   'leg_a':'PING',       'leg_b':'2318.HK','mkt_a':'NYSE', 'mkt_b':'HKEX', 'fx':'HKDUSD', 'name':'Ping An',      'ratio_a':1,  'ratio_b':5},
]

# State
arbi_capital    = ARBI_CAPITAL
arbi_open       = []    # open arbitrage trades
arbi_closed     = []    # closed arbitrage trades
arbi_spreads    = {}    # pair_id → current spread data
fx_rates        = {}    # USDBRL, GBPUSD, HKDUSD

def fetch_fx_rates():
    """Fetch FX rates via Yahoo Finance"""
    pairs = {'USDBRL': 'BRL=X', 'GBPUSD': 'GBPUSD=X', 'HKDUSD': 'HKD=X'}
    for key, yahoo_sym in pairs.items():
        try:
            r = requests.get(
                f'https://query1.finance.yahoo.com/v8/finance/chart/{yahoo_sym}?interval=1d&range=1d',
                headers={'User-Agent': 'Mozilla/5.0'}, timeout=6
            )
            if r.status_code == 200:
                meta = r.json()['chart']['result'][0]['meta']
                price = meta.get('regularMarketPrice', 0)
                if price > 0:
                    fx_rates[key] = price
        except: pass
    print(f"FX rates: {fx_rates}")

def fetch_yahoo_price(symbol):
    """Fetch single price from Yahoo Finance"""
    try:
        r = requests.get(
            f'https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=1d',
            headers={'User-Agent': 'Mozilla/5.0'}, timeout=6
        )
        if r.status_code == 200:
            meta = r.json()['chart']['result'][0]['meta']
            return meta.get('regularMarketPrice', 0)
    except: pass
    return 0

def calc_spread(pair):
    """Calculate spread between two legs adjusted for FX and ADR ratio"""
    try:
        price_a = fetch_yahoo_price(pair['leg_a'])
        price_b = fetch_yahoo_price(pair['leg_b'])
        if price_a <= 0 or price_b <= 0:
            return None

        fx      = pair['fx']
        ratio_a = pair.get('ratio_a', 1)   # shares in leg_a per unit
        ratio_b = pair.get('ratio_b', 1)   # shares in leg_b per unit

        # Convert everything to USD per single underlying share
        if fx == 'USDBRL':
            # leg_a = B3 (BRL), leg_b = NYSE (USD)
            rate = fx_rates.get('USDBRL', 5.8)        # BRL per USD
            price_a_usd = (price_a / rate) * ratio_a  # BRL → USD, apply ratio
            price_b_usd = price_b * ratio_b            # already USD

        elif fx == 'GBPUSD':
            # leg_a = NYSE (USD), leg_b = LSE (pence)
            rate = fx_rates.get('GBPUSD', 1.27)        # USD per GBP
            price_a_usd = price_a * ratio_a             # already USD
            price_b_usd = (price_b / 100 * rate) * ratio_b  # pence → GBP → USD

        elif fx == 'HKDUSD':
            # leg_a = NYSE (USD), leg_b = HKEX (HKD)
            rate = fx_rates.get('HKDUSD', 7.8)         # HKD per USD
            price_a_usd = price_a * ratio_a             # already USD
            price_b_usd = (price_b / rate) * ratio_b   # HKD → USD
        else:
            price_a_usd = price_a * ratio_a
            price_b_usd = price_b * ratio_b

        if price_b_usd <= 0: return None

        # Spread = (A - B) / B * 100
        spread_pct = ((price_a_usd - price_b_usd) / price_b_usd) * 100

        return {
            'pair_id':     pair['id'],
            'name':        pair['name'],
            'leg_a':       pair['leg_a'],
            'leg_b':       pair['leg_b'],
            'mkt_a':       pair['mkt_a'],
            'mkt_b':       pair['mkt_b'],
            'price_a':     round(price_a, 4),
            'price_b':     round(price_b, 4),
            'price_a_usd': round(price_a_usd, 4),
            'price_b_usd': round(price_b_usd, 4),
            'spread_pct':  round(spread_pct, 2),
            'abs_spread':  round(abs(spread_pct), 2),
            'fx_rate':     fx_rates.get(fx, 0),
            'fx_pair':     fx,
            'ratio_a':     ratio_a,
            'ratio_b':     ratio_b,
            'opportunity': abs(spread_pct) >= ARBI_MIN_SPREAD,
            'direction':   'LONG_A' if spread_pct < 0 else 'LONG_B',
            'updated_at':  datetime.utcnow().isoformat()
        }
    except Exception as e:
        print(f"Spread calc error {pair['id']}: {e}")
        return None

def execution_market_open(pair, direction):
    """For proper long/short arbi, BOTH markets must be open simultaneously"""
    def mkt_open(mkt):
        if mkt == 'B3':   return is_b3_open()
        if mkt in ('NYSE','NASDAQ'): return is_nyse_open()
        if mkt == 'LSE':
            now = datetime.utcnow()
            if now.weekday() >= 5: return False
            h = now.hour + now.minute / 60.0
            return 8.0 <= h < 16.5
        if mkt == 'HKEX':
            now = datetime.utcnow() + timedelta(hours=8)
            if now.weekday() >= 5: return False
            h = now.hour + now.minute / 60.0
            return (9.5 <= h < 12.0) or (13.0 <= h < 16.0)
        return False

    open_a = mkt_open(pair['mkt_a'])
    open_b = mkt_open(pair['mkt_b'])

    # LONG_A: buy leg_a + short leg_b → need both open
    # LONG_B: buy leg_b + short leg_a → need both open
    # For pure monitoring, return partial status too
    return open_a and open_b

def arbi_scan_loop():
    """Scan all pairs every 5 minutes"""
    global arbi_capital
    while True:
        try:
            fetch_fx_rates()
            time.sleep(2)
            for pair in ARBI_PAIRS:
                spread = calc_spread(pair)
                if not spread:
                    time.sleep(1)
                    continue
                with state_lock:
                    spread['markets_open'] = execution_market_open(pair, spread['direction'])
                    spread['execution_mkt'] = pair['mkt_a'] if spread['direction'] == 'LONG_A' else pair['mkt_b']
                    arbi_spreads[pair['id']] = spread
                    # Check if opportunity, execution market open, and not already open
                    open_ids = {t['pair_id'] for t in arbi_open}
                    if spread['opportunity'] and pair['id'] not in open_ids:
                        if not execution_market_open(pair, spread['direction']):
                            print(f"Arbi skip {pair['id']}: execution market ({spread['direction']}) closed")
                            time.sleep(1)
                            continue
                        pos = min(arbi_capital * 0.1, ARBI_POS_SIZE)
                        if arbi_capital >= pos:
                            arbi_capital -= pos
                            # LONG_A: buy leg_a + short leg_b
                            # LONG_B: buy leg_b + short leg_a
                            buy_leg   = pair['leg_a'] if spread['direction']=='LONG_A' else pair['leg_b']
                            short_leg = pair['leg_b'] if spread['direction']=='LONG_A' else pair['leg_a']
                            buy_mkt   = pair['mkt_a'] if spread['direction']=='LONG_A' else pair['mkt_b']
                            short_mkt = pair['mkt_b'] if spread['direction']=='LONG_A' else pair['mkt_a']
                            trade = {
                                'id':           gen_id('ARB'),
                                'pair_id':      pair['id'],
                                'name':         pair['name'],
                                'leg_a':        pair['leg_a'],
                                'leg_b':        pair['leg_b'],
                                'mkt_a':        pair['mkt_a'],
                                'mkt_b':        pair['mkt_b'],
                                'direction':    spread['direction'],
                                'buy_leg':      buy_leg,
                                'buy_mkt':      buy_mkt,
                                'short_leg':    short_leg,
                                'short_mkt':    short_mkt,
                                'entry_spread': spread['spread_pct'],
                                'current_spread': spread['spread_pct'],
                                'price_a_entry': spread['price_a_usd'],
                                'price_b_entry': spread['price_b_usd'],
                                'price_a_now':   spread['price_a_usd'],
                                'price_b_now':   spread['price_b_usd'],
                                'position_size': round(pos, 2),
                                'pnl': 0, 'pnl_pct': 0,
                                'peak_pnl_pct': 0,
                                'fx_rate':  spread['fx_rate'],
                                'opened_at': datetime.utcnow().isoformat(),
                                'status':   'OPEN',
                                'asset_type': 'arbitrage'
                            }
                            arbi_open.append(trade)
                            threading.Thread(target=db_save_arbi_trade, args=(trade,), daemon=True).start()
                            # Alert
                            msg = (
                                f"⚡ *Arbitrage Aberta — Egreja AI*\n"
                                f"Par: *{pair['name']}* ({pair['mkt_a']}↔{pair['mkt_b']})\n"
                                f"📈 COMPRA: {trade['buy_leg']} ({trade['buy_mkt']})\n"
                                f"📉 SHORT:  {trade['short_leg']} ({trade['short_mkt']})\n"
                                f"Spread: *{spread['abs_spread']:.2f}%*\n"
                                f"Capital: ${pos:,.0f}\n"
                                f"⏰ {datetime.now().strftime('%d/%m %H:%M')}"
                            )
                            threading.Thread(target=send_whatsapp, args=(msg,), daemon=True).start()
                time.sleep(1.5)  # Rate limit Yahoo Finance
        except Exception as e:
            print(f"Arbi scan error: {e}")
        time.sleep(300)  # 5 min between full scans

def arbi_monitor_loop():
    """Monitor open arbi trades every 60s"""
    global arbi_capital
    while True:
        time.sleep(60)
        try:
            with state_lock:
                now = datetime.utcnow()
                to_close = []
                for trade in arbi_open:
                    age_h = (now - datetime.fromisoformat(trade['opened_at'])).total_seconds() / 3600
                    # Get current spread
                    spread_data = arbi_spreads.get(trade['pair_id'])
                    if spread_data:
                        trade['current_spread'] = spread_data['spread_pct']
                        trade['price_a_now'] = spread_data['price_a_usd']
                        trade['price_b_now'] = spread_data['price_b_usd']
                        # P&L: entry_spread was the gap when we opened.
                        # We profit when the spread CONVERGES (gets smaller)
                        # Each 1% of convergence = 1% of position_size profit
                        entry_abs = abs(float(trade['entry_spread']))
                        curr_abs  = abs(float(trade['current_spread']))
                        convergence_pct = entry_abs - curr_abs  # positive = converging = profit
                        trade['pnl_pct'] = round(convergence_pct, 4)
                        trade['pnl']     = round(convergence_pct / 100 * float(trade['position_size']), 2)

                    # Update peak profit
                    trade['peak_pnl_pct'] = round(max(trade.get('peak_pnl_pct', 0), trade['pnl_pct']), 2)
                    peak = trade['peak_pnl_pct']

                    # Track P&L history
                    hist = trade.setdefault('pnl_history', [])
                    hist.append(trade['pnl_pct'])
                    if len(hist) > 5: hist.pop(0)

                    reason = None
                    if peak >= 2.0 and trade['pnl_pct'] <= peak - 1.0:
                        reason = 'TRAILING_STOP'
                    elif trade['pnl_pct'] <= -ARBI_SL_PCT:
                        reason = 'STOP_LOSS'
                    elif age_h >= ARBI_TIMEOUT_H:
                        extensions = trade.get('extensions', 0)
                        if is_momentum_positive(trade) and extensions < 3:
                            trade['extensions'] = extensions + 1
                            trade['timeout_extended'] = True
                            print(f"ARBI {trade['name']}: timeout extended ({extensions+1}/3) — momentum positive")
                        else:
                            reason = 'TIMEOUT'

                    if reason:
                        arbi_capital += trade['position_size'] + trade['pnl']
                        c = dict(trade)
                        c.update({'closed_at': now.isoformat(), 'close_reason': reason, 'status': 'CLOSED'})
                        arbi_closed.insert(0, c)
                        to_close.append(trade['id'])
                        threading.Thread(target=db_save_arbi_trade, args=(c,), daemon=True).start()
                        msg = (
                            f"{'✅' if trade['pnl']>=0 else '🔴'} *Arbi Fechado — Egreja AI*\n"
                            f"*{trade['name']}* ({trade['mkt_a']}↔{trade['mkt_b']})\n"
                            f"P&L: {'+'if trade['pnl']>=0 else ''}${trade['pnl']:,.2f} ({trade['pnl_pct']:+.2f}%)\n"
                            f"Motivo: {reason}\n"
                            f"⏰ {datetime.now().strftime('%d/%m %H:%M')}"
                        )
                        threading.Thread(target=send_whatsapp, args=(msg,), daemon=True).start()

                arbi_open[:] = [t for t in arbi_open if t['id'] not in to_close]
        except Exception as e:
            print(f"Arbi monitor error: {e}")

# Start arbitrage threads
threading.Thread(target=arbi_scan_loop,    daemon=True).start()
threading.Thread(target=arbi_monitor_loop, daemon=True).start()

# ── Arbitrage Routes ───────────────────────────────────
@app.route('/arbitrage/spreads')
def arbi_spreads_route():
    with state_lock:
        spreads = list(arbi_spreads.values())
    spreads.sort(key=lambda x: x['abs_spread'], reverse=True)
    opportunities = [s for s in spreads if s['opportunity']]
    return jsonify({
        'spreads': spreads,
        'opportunities': opportunities,
        'total_pairs':   len(ARBI_PAIRS),
        'monitored':     len(spreads),
        'fx_rates':      fx_rates,
        'updated_at':    datetime.utcnow().isoformat()
    })

@app.route('/arbitrage/trades')
def arbi_trades_route():
    with state_lock:
        open_t   = list(arbi_open)
        closed_t = arbi_closed[:50]
        cap      = arbi_capital
        closed_pnl = sum(t.get('pnl',0) for t in arbi_closed)
        open_pnl   = sum(t.get('pnl',0) for t in arbi_open)
        winners    = sum(1 for t in arbi_closed if t.get('pnl',0)>0)
    return jsonify({
        'open_trades':     open_t,
        'closed_trades':   closed_t,
        'capital':         round(cap, 2),
        'initial_capital': ARBI_CAPITAL,
        'open_pnl':        round(open_pnl, 2),
        'closed_pnl':      round(closed_pnl, 2),
        'total_pnl':       round(open_pnl+closed_pnl, 2),
        'win_rate':        round(winners/len(arbi_closed)*100,1) if arbi_closed else 0,
        'open_count':      len(open_t),
        'closed_count':    len(arbi_closed),
        'parameters': {
            'min_spread':   ARBI_MIN_SPREAD,
            'tp_spread':    ARBI_TP_SPREAD,
            'sl_pct':       ARBI_SL_PCT,
            'timeout_h':    ARBI_TIMEOUT_H,
            'position_size': ARBI_POS_SIZE,
            'capital':      ARBI_CAPITAL
        }
    })

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 3001))
    print(f"🚀 Egreja Investment AI v6.0 | port {port}")
    print(f"📈 Stocks: ${INITIAL_CAPITAL_STOCKS/1e6:.0f}M | Crypto: ${INITIAL_CAPITAL_CRYPTO/1e6:.0f}M | Arbi: ${ARBI_CAPITAL/1e3:.0f}K")
    print(f"📱 Alerts: {'ENABLED → '+TWILIO_TO if ALERTS_ENABLED else 'DISABLED'}")
    print(f"⏰ B3: {is_b3_open()} | NYSE: {is_nyse_open()}")
    fetch_crypto_prices()
    fetch_fx_rates()
    init_watchlist_table()
    init_trades_tables()
    app.run(host='0.0.0.0', port=port, debug=False)

