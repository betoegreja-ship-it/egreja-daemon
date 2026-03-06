#!/usr/bin/env python3
"""
Egreja Investment AI - API Server (Python/Flask)
Substitui o Node.js - roda no mesmo container do daemon
"""

import os
import json
import time
import random
import threading
from datetime import datetime, timedelta
from flask import Flask, jsonify, request
from flask_cors import CORS
import mysql.connector

app = Flask(__name__)
CORS(app)

# ─── Config ─────────────────────────────────────────────────────────
INITIAL_CAPITAL = float(os.environ.get('INITIAL_CAPITAL', 1000000))
MAX_POSITION_SIZE = float(os.environ.get('MAX_POSITION_SIZE', 50000))

# ─── MySQL Connection ────────────────────────────────────────────────
db_config = {
    'host': os.environ.get('MYSQLHOST', 'mysql.railway.internal'),
    'port': int(os.environ.get('MYSQLPORT', 3306)),
    'user': os.environ.get('MYSQLUSER', 'root'),
    'password': os.environ.get('MYSQLPASSWORD', ''),
    'database': os.environ.get('MYSQLDATABASE', 'railway'),
    'autocommit': True,
    'connection_timeout': 10
}

def get_db():
    try:
        conn = mysql.connector.connect(**db_config)
        return conn
    except Exception as e:
        print(f"❌ MySQL error: {e}")
        return None

def test_db():
    conn = get_db()
    if conn:
        conn.close()
        return True
    return False

# ─── Paper Trading State ─────────────────────────────────────────────
capital = INITIAL_CAPITAL
open_trades = []
closed_trades = []
trade_counter = [1]
state_lock = threading.Lock()

def generate_id():
    tid = f"TRD-{int(time.time())}-{trade_counter[0]}"
    trade_counter[0] += 1
    return tid

def monitor_trades():
    """Fecha trades por TP, SL ou Timeout a cada 30s"""
    global capital
    while True:
        time.sleep(30)
        try:
            with state_lock:
                now = datetime.utcnow()
                to_close = []
                for trade in open_trades:
                    opened = datetime.fromisoformat(trade['opened_at'])
                    age_hours = (now - opened).total_seconds() / 3600
                    
                    # Simula variação de preço
                    drift = (random.random() - 0.48) * 0.01
                    trade['current_price'] = round(trade['entry_price'] * (1 + drift * max(age_hours, 0.1)), 4)
                    trade['pnl'] = round((trade['current_price'] - trade['entry_price']) * trade['quantity'], 2)
                    trade['pnl_pct'] = round((trade['current_price'] / trade['entry_price'] - 1) * 100, 2)
                    
                    close_reason = None
                    if trade['pnl_pct'] >= 2.0:
                        close_reason = 'TAKE_PROFIT'
                    elif trade['pnl_pct'] <= -1.5:
                        close_reason = 'STOP_LOSS'
                    elif age_hours >= 2:
                        close_reason = 'TIMEOUT'
                    
                    if close_reason:
                        pnl = round((trade['current_price'] - trade['entry_price']) * trade['quantity'], 2)
                        capital += (trade['entry_price'] * trade['quantity']) + pnl
                        closed_trade = dict(trade)
                        closed_trade.update({
                            'exit_price': trade['current_price'],
                            'closed_at': now.isoformat(),
                            'close_reason': close_reason,
                            'pnl': pnl,
                            'status': 'CLOSED'
                        })
                        closed_trades.insert(0, closed_trade)
                        to_close.append(trade['id'])
                
                for tid in to_close:
                    open_trades[:] = [t for t in open_trades if t['id'] != tid]
        except Exception as e:
            print(f"Monitor error: {e}")

# Inicia monitor em background
threading.Thread(target=monitor_trades, daemon=True).start()

# ─── Routes ──────────────────────────────────────────────────────────

@app.route('/')
def index():
    db_ok = test_db()
    return jsonify({
        'service': 'Egreja Investment AI',
        'version': '3.0.0',
        'status': 'online',
        'db': 'connected' if db_ok else 'unavailable',
        'endpoints': ['/signals', '/trades', '/trades/open', '/trades/closed', '/stats', '/portfolio', '/debug']
    })

@app.route('/debug')
def debug():
    db_ok = test_db()
    return jsonify({
        'db_status': 'connected' if db_ok else 'unavailable',
        'env': {
            'MYSQLHOST': os.environ.get('MYSQLHOST', 'NOT SET'),
            'MYSQLPORT': os.environ.get('MYSQLPORT', 'NOT SET'),
            'MYSQLUSER': os.environ.get('MYSQLUSER', 'NOT SET'),
            'MYSQLDATABASE': os.environ.get('MYSQLDATABASE', 'NOT SET'),
            'PORT': os.environ.get('PORT', 'NOT SET')
        }
    })

@app.route('/signals')
def signals():
    global capital
    conn = get_db()
    if not conn:
        return jsonify({'error': 'Database unavailable'}), 503
    
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute('SELECT * FROM market_signals ORDER BY updated_at DESC, score DESC LIMIT 100')
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        
        # Converte datetime para string
        for row in rows:
            for k, v in row.items():
                if isinstance(v, datetime):
                    row[k] = v.isoformat()
        
        # Auto-abre trades para sinais fortes
        with state_lock:
            for signal in rows:
                already_open = any(t['symbol'] == signal['symbol'] for t in open_trades)
                if not already_open and signal.get('score', 0) >= 70 and signal.get('signal') == 'COMPRA':
                    price = float(signal.get('price', 0) or 0)
                    if price <= 0:
                        continue
                    pos_size = min(capital * 0.05, MAX_POSITION_SIZE)
                    quantity = int(pos_size / price)
                    if quantity > 0 and capital >= price * quantity:
                        capital -= price * quantity
                        open_trades.append({
                            'id': generate_id(),
                            'symbol': signal['symbol'],
                            'market': signal.get('market_type', ''),
                            'direction': 'LONG',
                            'entry_price': price,
                            'current_price': price,
                            'quantity': quantity,
                            'position_value': round(price * quantity, 2),
                            'pnl': 0,
                            'pnl_pct': 0,
                            'score': signal.get('score', 0),
                            'signal': signal.get('signal', ''),
                            'opened_at': datetime.utcnow().isoformat(),
                            'status': 'OPEN'
                        })
        
        return jsonify({
            'status': 'OK',
            'timestamp': datetime.utcnow().isoformat(),
            'total': len(rows),
            'signals': rows
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/trades')
def trades():
    with state_lock:
        all_trades = open_trades + closed_trades[:100]
    return jsonify({'trades': all_trades, 'total': len(all_trades)})

@app.route('/trades/open')
def trades_open():
    with state_lock:
        data = list(open_trades)
    return jsonify({'trades': data, 'total': len(data)})

@app.route('/trades/closed')
def trades_closed():
    with state_lock:
        data = closed_trades[:50]
    return jsonify({'trades': data, 'total': len(closed_trades)})

@app.route('/stats')
def stats():
    with state_lock:
        closed_pnl = sum(t.get('pnl', 0) for t in closed_trades)
        open_pnl = sum(t.get('pnl', 0) for t in open_trades)
        winners = sum(1 for t in closed_trades if t.get('pnl', 0) > 0)
        total_closed = len(closed_trades)
        total_open = len(open_trades)
        cap = capital
    
    return jsonify({
        'initial_capital': INITIAL_CAPITAL,
        'current_capital': round(cap, 2),
        'total_pnl': round(closed_pnl + open_pnl, 2),
        'closed_pnl': round(closed_pnl, 2),
        'open_pnl': round(open_pnl, 2),
        'gain_percent': round((cap + open_pnl - INITIAL_CAPITAL) / INITIAL_CAPITAL * 100, 2),
        'total_trades': total_open + total_closed,
        'open_trades': total_open,
        'closed_trades': total_closed,
        'winning_trades': winners,
        'win_rate': round(winners / total_closed * 100, 1) if total_closed > 0 else 0,
        'assets_monitored': 40,
        'updated_at': datetime.utcnow().isoformat()
    })

@app.route('/portfolio')
def portfolio():
    with state_lock:
        closed_pnl = sum(t.get('pnl', 0) for t in closed_trades)
        open_pnl = sum(t.get('pnl', 0) for t in open_trades)
        cap = capital
        open_pos = list(open_trades)
        closed_pos = closed_trades[:20]
    
    return jsonify({
        'capital': round(cap, 2),
        'open_positions': open_pos,
        'closed_positions': closed_pos,
        'summary': {
            'total_pnl': round(closed_pnl + open_pnl, 2),
            'gain_percent': round((cap + open_pnl - INITIAL_CAPITAL) / INITIAL_CAPITAL * 100, 2)
        }
    })

# ─── Start ────────────────────────────────────────────────────────────
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 3001))
    print(f"🚀 Egreja Investment AI API v3.0 (Python) on port {port}")
    print(f"💾 MySQL: {db_config['host']}:{db_config['port']}")
    print(f"📈 Paper trading ready | Capital: ${INITIAL_CAPITAL:,.0f}")
    app.run(host='0.0.0.0', port=port, debug=False)
