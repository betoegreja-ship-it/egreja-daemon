"""Polygon WebSocket real-time stocks NYSE/NASDAQ.

Conecta em wss://socket.polygon.io/stocks, autentica com POLYGON_API_KEY,
subscribe nos simbolos do universo NYSE/NASDAQ que o sistema opera.

Mantem cache em memoria de preco + bid/ask + timestamp por simbolo.

API publica:
  - get_price(symbol) -> float ou None
  - get_quote(symbol) -> dict {price, bid, ask, ts, age_s} ou None
  - status() -> dict com saude da conexao
  - start_worker(beat_fn) -> thread loop reconnect

Persistente via env POLYGON_WS_ENABLED=true (default).
Auto-reconnect 5s se cair.
"""
import os, json, time, logging, threading
from datetime import datetime
from collections import deque

log = logging.getLogger('egreja.polygon_ws')

POLYGON_API_KEY = os.environ.get('POLYGON_API_KEY', '').strip()
POLYGON_WS_URL = os.environ.get('POLYGON_WS_URL', 'wss://socket.polygon.io/stocks')
POLYGON_WS_ENABLED = os.environ.get('POLYGON_WS_ENABLED', 'true').lower() == 'true'
POLYGON_WS_RECONNECT_S = int(os.environ.get('POLYGON_WS_RECONNECT_S', '10'))
POLYGON_WS_MAX_CONN_BACKOFF_S = int(os.environ.get('POLYGON_WS_MAX_CONN_BACKOFF_S', '900'))

# Cache: symbol -> {price, bid, ask, ts (epoch s), source}
_cache = {}
_cache_lock = threading.Lock()

# Status
_state = {
    'connected': False,
    'connected_since': None,
    'last_message_ts': 0,
    'messages_received': 0,
    'reconnects': 0,
    'subscribed_symbols': [],
    'last_error': None,
    'disabled_until': 0,
}
_state_lock = threading.Lock()

# Universo NYSE/NASDAQ que o sistema opera — pegamos do brain v3 universe
DEFAULT_UNIVERSE = [
    'AAPL', 'MSFT', 'NVDA', 'AMZN', 'GOOGL', 'META', 'TSLA', 'NFLX', 'AMD', 'INTC',
    'CRM', 'ORCL', 'IBM', 'AVGO', 'ADBE', 'CSCO', 'QCOM', 'TXN', 'PYPL', 'UBER',
    'COIN', 'PLTR', 'SNOW', 'SHOP', 'SQ', 'DIS', 'SPOT', 'GS', 'JPM', 'BAC',
    'V', 'MA', 'XOM', 'CVX', 'WMT', 'HD', 'KO', 'PEP', 'MCD', 'NKE',
    'UNH', 'PFE', 'JNJ', 'LLY', 'TSM', 'BABA', 'PDD', 'BBD', 'PBR', 'VALE',
    'ITUB', 'NU', 'IWM', 'QQQ', 'SPY', 'CROX', 'HOOD', 'SMCI', 'NOW',
    'SNDK', 'EPAM', 'CNI', 'VSAT', 'MU',
    # [01-jul-2026] Expansao: 8 mega caps US que faltavam
    'BRK.B', 'COST', 'MRK', 'ABT', 'CAT', 'BA', 'AMAT', 'XLK',
]


def _on_message(ws, message):
    """Processa mensagens do Polygon WS.
    Tipos relevantes: T (trade), Q (quote), AM (1-min aggregate)
    """
    try:
        msgs = json.loads(message)
        if not isinstance(msgs, list): msgs = [msgs]
        for m in msgs:
            ev = m.get('ev')
            if ev == 'T':  # Trade
                sym = m.get('sym')
                price = m.get('p')
                ts_ms = m.get('t')  # ms
                if sym and price:
                    with _cache_lock:
                        c = _cache.setdefault(sym, {})
                        c['price'] = float(price)
                        c['ts'] = ts_ms / 1000 if ts_ms else time.time()
                        c['source'] = 'polygon_ws_trade'
            elif ev == 'Q':  # Quote (bid/ask)
                sym = m.get('sym')
                bid = m.get('bp')
                ask = m.get('ap')
                ts_ms = m.get('t')
                if sym:
                    with _cache_lock:
                        c = _cache.setdefault(sym, {})
                        if bid: c['bid'] = float(bid)
                        if ask: c['ask'] = float(ask)
                        if ts_ms: c['quote_ts'] = ts_ms / 1000
            elif ev == 'status':
                msg = m.get('message', '')
                status = m.get('status', '')
                log.info(f'[polygon-ws] status={status} msg={msg}')
                if status == 'auth_success':
                    with _state_lock:
                        _state['connected'] = True
                        _state['connected_since'] = time.time()
                elif status == 'max_connections' or 'Maximum number of websocket connections exceeded' in msg:
                    disabled_until = time.time() + POLYGON_WS_MAX_CONN_BACKOFF_S
                    with _state_lock:
                        _state['connected'] = False
                        _state['last_error'] = 'max_connections'
                        _state['disabled_until'] = disabled_until
                    log.warning(
                        '[polygon-ws] max_connections: pausando reconexao por %ss',
                        POLYGON_WS_MAX_CONN_BACKOFF_S,
                    )
                    try:
                        ws.close()
                    except Exception:
                        pass
            with _state_lock:
                _state['last_message_ts'] = time.time()
                _state['messages_received'] += 1
    except Exception as e:
        log.debug(f'[polygon-ws] message parse: {e}')


def _on_error(ws, error):
    log.warning(f'[polygon-ws] error: {error}')
    with _state_lock:
        _state['last_error'] = str(error)[:200]
        _state['connected'] = False


def _on_close(ws, close_status_code, close_msg):
    log.warning(f'[polygon-ws] closed: {close_status_code} {close_msg}')
    with _state_lock:
        _state['connected'] = False


def _on_open(ws):
    log.info('[polygon-ws] connected, autenticando...')
    # Autentica
    ws.send(json.dumps({'action': 'auth', 'params': POLYGON_API_KEY}))
    time.sleep(0.5)
    # Subscribe: T.* (trades) + Q.* (quotes) pro universo
    syms_T = ','.join([f'T.{s}' for s in DEFAULT_UNIVERSE])
    syms_Q = ','.join([f'Q.{s}' for s in DEFAULT_UNIVERSE])
    ws.send(json.dumps({'action': 'subscribe', 'params': syms_T}))
    time.sleep(0.3)
    ws.send(json.dumps({'action': 'subscribe', 'params': syms_Q}))
    with _state_lock:
        _state['subscribed_symbols'] = list(DEFAULT_UNIVERSE)
    log.info(f'[polygon-ws] subscribed: {len(DEFAULT_UNIVERSE)} simbolos (T + Q)')


def _run_ws():
    """Worker thread — loop reconnect."""
    try:
        from websocket import WebSocketApp  # websocket-client lib
    except ImportError:
        try:
            # Fallback: tenta com websockets (async lib) — incompativel, avisa
            log.error('[polygon-ws] websocket-client nao instalado. pip install websocket-client')
            return
        except: return

    while True:
        with _state_lock:
            disabled_until = float(_state.get('disabled_until') or 0)
        if disabled_until > time.time():
            sleep_s = min(60, max(1, int(disabled_until - time.time())))
            log.info(f'[polygon-ws] circuit breaker ativo; nova tentativa em {sleep_s}s')
            time.sleep(sleep_s)
            continue
        try:
            log.info(f'[polygon-ws] conectando {POLYGON_WS_URL}...')
            ws = WebSocketApp(POLYGON_WS_URL,
                              on_open=_on_open,
                              on_message=_on_message,
                              on_error=_on_error,
                              on_close=_on_close)
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as e:
            log.error(f'[polygon-ws] crash: {e}')
        with _state_lock:
            _state['connected'] = False
            _state['reconnects'] += 1
            last_error = _state.get('last_error')
            disabled_until = float(_state.get('disabled_until') or 0)
        if disabled_until > time.time() or last_error == 'max_connections':
            sleep_s = min(60, max(1, int(disabled_until - time.time())))
            log.info(f'[polygon-ws] reconexao pausada por limite da conta ({sleep_s}s)')
            time.sleep(sleep_s)
        else:
            log.info(f'[polygon-ws] reconectando em {POLYGON_WS_RECONNECT_S}s...')
            time.sleep(POLYGON_WS_RECONNECT_S)


def start_worker(beat_fn=None):
    """Inicia worker em thread daemon."""
    if not POLYGON_WS_ENABLED:
        log.info('[polygon-ws] disabled via POLYGON_WS_ENABLED=false')
        return None
    if not POLYGON_API_KEY:
        log.warning('[polygon-ws] POLYGON_API_KEY nao setada — skip')
        return None
    t = threading.Thread(target=_run_ws, daemon=True, name='polygon_ws')
    t.start()
    log.info('[polygon-ws] worker thread iniciada')
    return t


# === API publica ===
def get_price(symbol: str, max_age_s: int = 60):
    """Retorna preco real-time do simbolo. None se nao cacheado ou stale."""
    with _cache_lock:
        c = _cache.get(symbol.upper())
    if not c or 'price' not in c: return None
    age = time.time() - c.get('ts', 0)
    if age > max_age_s: return None
    return c['price']


def get_quote(symbol: str):
    """Retorna dict completo {price, bid, ask, ts, age_s, source} ou None."""
    with _cache_lock:
        c = _cache.get(symbol.upper())
    if not c: return None
    age = time.time() - c.get('ts', 0)
    out = dict(c)
    out['symbol'] = symbol.upper()
    out['age_s'] = round(age, 2)
    return out


def status():
    """Snapshot pra endpoint."""
    with _state_lock:
        st = dict(_state)
    with _cache_lock:
        cached = len(_cache)
        sample = {}
        for sym in list(_cache.keys())[:5]:
            c = _cache[sym]
            sample[sym] = {
                'price': c.get('price'),
                'bid': c.get('bid'),
                'ask': c.get('ask'),
                'age_s': round(time.time() - c.get('ts', 0), 2),
            }
    st['cached_symbols'] = cached
    st['sample_quotes'] = sample
    if st.get('connected_since'):
        st['uptime_s'] = round(time.time() - st['connected_since'], 0)
    if st.get('disabled_until'):
        st['disabled_for_s'] = max(0, round(st['disabled_until'] - time.time(), 0))
    return st
