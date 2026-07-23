# -*- coding: utf-8 -*-
"""[23-jul-2026, decisao Beto] ADAPTADOR DE EXECUCAO REAL — Binance Spot.

Primeira peca da migracao paper->real (cripto primeiro: chave API confirmada
na conta Binance Brasil do Beto). Modos, por env EXEC_MODE:

  ghost    (default) NAO chama a Binance. Loga a ordem exata que enviaria e
           simula o fill ao preco corrente com as TAXAS REAIS (real-sim):
           mostra preto no branco o P&L liquido de cada motor antes de
           arriscar 1 real. Zero risco.
  testnet  Envia ordens REAIS para a testnet spot da Binance
           (testnet.binance.vision) — valida assinatura, fills, reconciliacao.
  live     Envia ordens reais para api.binance.com. SO com aprovacao
           explicita do Beto + chave SEM permissao de saque + capital pequeno.

Regras de seguranca (invioladas em qualquer modo):
  - A chave NUNCA pode ter permissao de saque (checagem no boot em live).
  - Teto por ordem: EXEC_MAX_ORDER_USDT (default 200).
  - Teto diario de ordens: EXEC_MAX_ORDERS_DAY (default 60).
  - Kill switch da plataforma bloqueia execucao.
  - Fail-open: qualquer erro NUNCA derruba o paper (que segue soberano).

Tabela exec_orders = trilha completa de tudo que o adaptador fez/faria.
"""
import os, time, hmac, hashlib, json, logging
from datetime import datetime, date
from urllib.parse import urlencode

import requests
import pymysql

log = logging.getLogger('egreja.exec.binance')

def _base_url():
    """Base URL por modo. testnet aceita override por env porque a 'Demo
    Trading' (demo.binance.com) pode usar endpoint diferente da testnet
    classica (testnet.binance.vision)."""
    m = _mode()
    if m == 'testnet':
        return os.environ.get('EXEC_TESTNET_URL', 'https://testnet.binance.vision').rstrip('/')
    if m == 'live':
        return os.environ.get('EXEC_LIVE_URL', 'https://api.binance.com').rstrip('/')
    return None

_day_count = {'d': None, 'n': 0}


def _mode():
    return os.environ.get('EXEC_MODE', 'ghost').lower().strip()


def _env_f(name, default):
    try:
        return float(os.environ.get(name, default))
    except Exception:
        return float(default)


def _conn():
    return pymysql.connect(
        host=os.environ['MYSQLHOST'], user=os.environ['MYSQLUSER'],
        password=os.environ['MYSQLPASSWORD'], database=os.environ['MYSQLDATABASE'],
        port=int(os.environ.get('MYSQLPORT', 3306)), autocommit=True)


def create_tables():
    c = _conn()
    try:
        c.cursor().execute("""CREATE TABLE IF NOT EXISTS exec_orders (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            trade_id VARCHAR(40), symbol VARCHAR(16), side VARCHAR(6),
            event VARCHAR(8), mode VARCHAR(8), status VARCHAR(16),
            qty DECIMAL(20,8), quote_usdt DECIMAL(16,2),
            price_ref DECIMAL(18,8), price_fill DECIMAL(18,8),
            fee_usdt DECIMAL(12,4), binance_order_id VARCHAR(32),
            error VARCHAR(200), resp_json TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_trade (trade_id), INDEX idx_created (created_at)
            ) CHARACTER SET utf8mb4""")
    finally:
        c.close()


def _sign(params: dict) -> str:
    secret = os.environ.get('BINANCE_TRADE_SECRET', '')
    qs = urlencode(params)
    sig = hmac.new(secret.encode(), qs.encode(), hashlib.sha256).hexdigest()
    return qs + '&signature=' + sig


def _api(method, path, params=None, signed=True):
    base = _base_url()
    if not base:
        return None, 'modo ghost nao chama API'
    key = os.environ.get('BINANCE_TRADE_KEY', '')
    if not key:
        return None, 'BINANCE_TRADE_KEY nao configurada'
    params = dict(params or {})
    if signed:
        params['timestamp'] = int(time.time() * 1000)
        params['recvWindow'] = 5000
        body = _sign(params)
    else:
        body = urlencode(params)
    url = f'{base}{path}?{body}'
    try:
        r = requests.request(method, url, headers={'X-MBX-APIKEY': key}, timeout=10)
        if r.status_code == 200:
            return r.json(), None
        return None, f'{r.status_code}: {r.text[:180]}'
    except Exception as e:
        return None, str(e)


def check_key_safety():
    """[live only] Confirma que a chave NAO pode sacar. Aborta live se puder."""
    d, err = _api('GET', '/sapi/v1/account/apiRestrictions')
    if err:
        return False, f'nao consegui verificar restricoes: {err}'
    if d.get('enableWithdrawals'):
        return False, 'CHAVE COM PERMISSAO DE SAQUE — execucao live BLOQUEADA'
    if not d.get('enableSpotAndMarginTrading'):
        return False, 'chave sem permissao de spot trading'
    return True, 'chave segura (sem saque, com spot)'


def _guards_ok(quote_usdt):
    if quote_usdt > _env_f('EXEC_MAX_ORDER_USDT', 200):
        return f'ordem ${quote_usdt:.0f} > teto EXEC_MAX_ORDER_USDT'
    today = date.today()
    if _day_count['d'] != today:
        _day_count['d'] = today
        _day_count['n'] = 0
    if _day_count['n'] >= int(_env_f('EXEC_MAX_ORDERS_DAY', 60)):
        return 'teto diario de ordens atingido'
    return None


_tbl_ready = False

def _record(row):
    global _tbl_ready
    try:
        if not _tbl_ready:
            create_tables()
            _tbl_ready = True
        c = _conn()
        c.cursor().execute("""INSERT INTO exec_orders (trade_id,symbol,side,event,mode,
            status,qty,quote_usdt,price_ref,price_fill,fee_usdt,binance_order_id,error,resp_json)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (row.get('trade_id'), row.get('symbol'), row.get('side'), row.get('event'),
             row.get('mode'), row.get('status'), row.get('qty'), row.get('quote_usdt'),
             row.get('price_ref'), row.get('price_fill'), row.get('fee_usdt'),
             row.get('binance_order_id'), row.get('error'),
             json.dumps(row.get('resp'), default=str)[:2000] if row.get('resp') else None))
        c.close()
    except Exception as e:
        log.debug(f'[EXEC] record: {e}')


def _execute(trade, event, side):
    """Nucleo: ghost simula; testnet/live enviam MARKET order por quoteOrderQty."""
    if os.environ.get('EXEC_ENGINE_ENABLED', 'true').lower() == 'false':
        return
    mode = _mode()
    sym = str(trade.get('symbol', '')).upper()
    pair = sym + 'USDT' if not sym.endswith('USDT') else sym
    price_ref = float(trade.get('current_price') or trade.get('entry_price') or 0)
    # dimensionamento real e INDEPENDENTE do paper: fracao fixa pequena
    quote = min(_env_f('EXEC_ORDER_USDT', 100), _env_f('EXEC_MAX_ORDER_USDT', 200))
    fee_pct = _env_f('EXEC_TAKER_FEE_PCT', 0.075) / 100.0  # BNB discount default
    guard = _guards_ok(quote)
    if guard:
        _record({'trade_id': trade.get('id'), 'symbol': pair, 'side': side, 'event': event,
                 'mode': mode, 'status': 'BLOCKED', 'quote_usdt': quote, 'error': guard})
        log.warning(f'[EXEC-{mode.upper()}] {pair} {side} bloqueada: {guard}')
        return
    if mode == 'ghost':
        fee = quote * fee_pct
        _day_count['n'] += 1
        _record({'trade_id': trade.get('id'), 'symbol': pair, 'side': side, 'event': event,
                 'mode': 'ghost', 'status': 'SIMULATED', 'quote_usdt': quote,
                 'price_ref': price_ref, 'price_fill': price_ref, 'fee_usdt': round(fee, 4)})
        log.info(f'[EXEC-GHOST] {event} {pair} {side} ${quote:.0f} @ ~{price_ref} '
                 f'(fee real-sim ${fee:.3f}) — ordem que SERIA enviada')
        return
    # testnet / live
    if mode == 'live':
        ok, why = check_key_safety()
        if not ok:
            _record({'trade_id': trade.get('id'), 'symbol': pair, 'side': side,
                     'event': event, 'mode': mode, 'status': 'BLOCKED', 'error': why})
            log.error(f'[EXEC-LIVE] BLOQUEADA: {why}')
            return
    d, err = _api('POST', '/api/v3/order', {
        'symbol': pair, 'side': side, 'type': 'MARKET',
        'quoteOrderQty': f'{quote:.2f}'})
    if err:
        _record({'trade_id': trade.get('id'), 'symbol': pair, 'side': side, 'event': event,
                 'mode': mode, 'status': 'ERROR', 'quote_usdt': quote, 'error': err[:200]})
        log.warning(f'[EXEC-{mode.upper()}] {pair} {side} ERRO: {err}')
        return
    fills = d.get('fills') or []
    filled_qty = sum(float(f['qty']) for f in fills) if fills else float(d.get('executedQty') or 0)
    fill_px = (sum(float(f['price']) * float(f['qty']) for f in fills) / filled_qty) if (fills and filled_qty) else None
    fee = sum(float(f.get('commission') or 0) for f in fills)
    _day_count['n'] += 1
    _record({'trade_id': trade.get('id'), 'symbol': pair, 'side': side, 'event': event,
             'mode': mode, 'status': d.get('status', 'FILLED'), 'qty': filled_qty,
             'quote_usdt': quote, 'price_ref': price_ref, 'price_fill': fill_px,
             'fee_usdt': fee, 'binance_order_id': str(d.get('orderId')), 'resp': d})
    log.warning(f'[EXEC-{mode.upper()}] {event} {pair} {side} FILLED qty={filled_qty} '
                f'@ {fill_px} fee={fee} orderId={d.get("orderId")}')


def exec_on_open(trade):
    """Chamado quando o motor cripto ABRE uma trade paper."""
    try:
        side = 'BUY' if str(trade.get('direction', 'LONG')).upper() == 'LONG' else 'SELL'
        # SHORT spot real nao existe sem margem: em ghost/testnet registramos para
        # medir; em live, SHORTs sao pulados (spot-only) ate decisao de margem.
        if side == 'SELL' and _mode() == 'live':
            _record({'trade_id': trade.get('id'), 'symbol': trade.get('symbol'),
                     'side': side, 'event': 'OPEN', 'mode': 'live',
                     'status': 'SKIPPED', 'error': 'SHORT ignorado em spot live'})
            return
        _execute(trade, 'OPEN', side)
    except Exception as e:
        log.debug(f'[EXEC] on_open: {e}')


def exec_on_close(trade):
    """Chamado quando o motor cripto FECHA uma trade paper (lado inverso)."""
    try:
        side = 'SELL' if str(trade.get('direction', 'LONG')).upper() == 'LONG' else 'BUY'
        if side == 'BUY' and _mode() == 'live' and str(trade.get('direction')).upper() == 'SHORT':
            return  # short nunca abriu em live
        _execute(trade, 'CLOSE', side)
    except Exception as e:
        log.debug(f'[EXEC] on_close: {e}')


def summary():
    try:
        c = _conn()
        cur = c.cursor(pymysql.cursors.DictCursor)
        cur.execute("""SELECT mode, event, status, COUNT(*) n, ROUND(SUM(quote_usdt),2) vol,
            ROUND(SUM(fee_usdt),4) fees FROM exec_orders
            WHERE created_at >= CURDATE() - INTERVAL 7 DAY GROUP BY mode, event, status""")
        rows = list(cur.fetchall())
        cur.execute("""SELECT symbol, side, event, mode, status, quote_usdt, price_fill,
            fee_usdt, created_at FROM exec_orders ORDER BY id DESC LIMIT 10""")
        last = list(cur.fetchall())
        c.close()
        return {'mode': _mode(), 'last_7d': rows, 'last_orders': last,
                'order_usdt': _env_f('EXEC_ORDER_USDT', 100),
                'engine_enabled': os.environ.get('EXEC_ENGINE_ENABLED', 'true')}
    except Exception as e:
        return {'error': str(e)}
