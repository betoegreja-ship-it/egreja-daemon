# -*- coding: utf-8 -*-
"""[23-jul-2026, decisao Beto] ADAPTADOR DE EXECUCAO — Interactive Brokers.

Gemeo do binance_exec.py, para NYSE (long E short nativo). Arquitetura:
o core (Railway) NAO fala IB direto — manda a ordem por HTTP para uma PONTE
(ib_bridge) que roda numa VPS junto do IB Gateway logado. Mesmo desenho do
relay Cedro, ao contrario.

  IB_EXEC_MODE:
    ghost   (default) NAO chama a ponte. Loga + simula fill com comissao real
            estimada (IB: ~US$0.005/acao, min US$1). Zero risco.
    paper   Envia para a ponte -> IB Gateway em PORTA 4002 (paper, US$1M virtual).
    live    IB Gateway porta 4001 (real). SO com aprovacao explicita do Beto.

  A CREDENCIAL DA IB nunca passa por aqui: fica no Gateway da VPS. Este modulo
  so conhece a URL da ponte (IB_BRIDGE_URL) e um segredo compartilhado
  (IB_BRIDGE_SECRET) para autenticar as chamadas.

Regras (qualquer modo): teto por ordem IB_EXEC_MAX_USD (2000), teto diario
IB_EXEC_MAX_ORDERS_DAY (80), fail-open total (nunca derruba o paper). Trilha
na MESMA tabela exec_orders (venue='IB').
"""
import os, time, json, logging
from datetime import date

import requests

log = logging.getLogger('egreja.exec.ib')

_day = {'d': None, 'n': 0}


def _mode():
    return os.environ.get('IB_EXEC_MODE', 'ghost').lower().strip()


def _f(name, d):
    try:
        return float(os.environ.get(name, d))
    except Exception:
        return float(d)


def _record(row):
    """Reaproveita a tabela exec_orders do binance_exec (coluna venue)."""
    try:
        from modules.binance_exec import _conn, create_tables
        create_tables()
        c = _conn()
        cur = c.cursor()
        # garante a coluna venue (idempotente)
        try:
            cur.execute("ALTER TABLE exec_orders ADD COLUMN venue VARCHAR(8) DEFAULT 'BINANCE'")
        except Exception:
            pass
        cur.execute("""INSERT INTO exec_orders (trade_id,symbol,side,event,mode,status,
            qty,quote_usdt,price_ref,price_fill,fee_usdt,binance_order_id,error,resp_json,venue)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'IB')""",
            (row.get('trade_id'), row.get('symbol'), row.get('side'), row.get('event'),
             row.get('mode'), row.get('status'), row.get('qty'), row.get('usd'),
             row.get('price_ref'), row.get('price_fill'), row.get('fee'),
             str(row.get('ib_order_id') or ''), row.get('error'),
             json.dumps(row.get('resp'), default=str)[:2000] if row.get('resp') else None))
        c.close()
    except Exception as e:
        log.debug(f'[IB-EXEC] record: {e}')


def _guard(usd):
    if usd > _f('IB_EXEC_MAX_USD', 2000):
        return f'ordem ${usd:.0f} > teto IB_EXEC_MAX_USD'
    today = date.today()
    if _day['d'] != today:
        _day['d'] = today; _day['n'] = 0
    if _day['n'] >= int(_f('IB_EXEC_MAX_ORDERS_DAY', 80)):
        return 'teto diario de ordens IB'
    return None


def _bridge(payload):
    url = os.environ.get('IB_BRIDGE_URL', '').rstrip('/')
    if not url:
        return None, 'IB_BRIDGE_URL nao configurada'
    try:
        r = requests.post(f'{url}/order', json=payload,
                          headers={'X-Bridge-Secret': os.environ.get('IB_BRIDGE_SECRET', '')},
                          timeout=8)
        if r.status_code == 200:
            return r.json(), None
        return None, f'{r.status_code}: {r.text[:180]}'
    except Exception as e:
        return None, str(e)


def _execute(trade, event, action):
    """action = BUY | SELL | SHORT | COVER."""
    if os.environ.get('IB_EXEC_ENGINE_ENABLED', 'true').lower() == 'false':
        return
    mode = _mode()
    sym = str(trade.get('symbol', '')).upper()
    px = float(trade.get('current_price') or trade.get('entry_price') or 0)
    usd = min(_f('IB_EXEC_ORDER_USD', 1000), _f('IB_EXEC_MAX_USD', 2000))
    qty = max(1, int(usd / px)) if px > 0 else 0
    g = _guard(usd)
    if g:
        _record({'trade_id': trade.get('id'), 'symbol': sym, 'side': action, 'event': event,
                 'mode': mode, 'status': 'BLOCKED', 'usd': usd, 'error': g})
        return
    fee = max(_f('IB_MIN_FEE_USD', 1.0), qty * _f('IB_FEE_PER_SHARE', 0.005))
    if mode == 'ghost':
        _day['n'] += 1
        _record({'trade_id': trade.get('id'), 'symbol': sym, 'side': action, 'event': event,
                 'mode': 'ghost', 'status': 'SIMULATED', 'qty': qty, 'usd': usd,
                 'price_ref': px, 'price_fill': px, 'fee': round(fee, 2)})
        log.info(f'[IB-GHOST] {event} {sym} {action} {qty}sh ~${usd:.0f} @ {px} '
                 f'(comissao real-sim ${fee:.2f}) — ordem que SERIA enviada')
        return
    ib_action = 'BUY' if action in ('BUY', 'COVER') else 'SELL'
    d, err = _bridge({'symbol': sym, 'action': ib_action, 'quantity': qty,
                      'mode': mode, 'trade_id': trade.get('id')})
    if err:
        _record({'trade_id': trade.get('id'), 'symbol': sym, 'side': action, 'event': event,
                 'mode': mode, 'status': 'ERROR', 'qty': qty, 'usd': usd, 'error': err[:200]})
        log.warning(f'[IB-{mode.upper()}] {sym} {action} ERRO: {err}')
        return
    _day['n'] += 1
    _record({'trade_id': trade.get('id'), 'symbol': sym, 'side': action, 'event': event,
             'mode': mode, 'status': d.get('status', 'SENT'), 'qty': d.get('filled', qty),
             'usd': usd, 'price_ref': px, 'price_fill': d.get('avg_price'),
             'fee': d.get('commission'), 'ib_order_id': d.get('order_id'), 'resp': d})
    log.warning(f'[IB-{mode.upper()}] {event} {sym} {action} -> {d.get("status")} '
                f'filled={d.get("filled")} @ {d.get("avg_price")} id={d.get("order_id")}')


def exec_on_open(trade):
    """LONG -> BUY ; SHORT -> SHORT (venda a descoberto NATIVA na IB)."""
    try:
        d = str(trade.get('direction', 'LONG')).upper()
        _execute(trade, 'OPEN', 'BUY' if d == 'LONG' else 'SHORT')
    except Exception as e:
        log.debug(f'[IB-EXEC] on_open: {e}')


def exec_on_close(trade):
    """Fecha LONG -> SELL ; fecha SHORT -> COVER (recompra)."""
    try:
        d = str(trade.get('direction', 'LONG')).upper()
        _execute(trade, 'CLOSE', 'SELL' if d == 'LONG' else 'COVER')
    except Exception as e:
        log.debug(f'[IB-EXEC] on_close: {e}')


def bridge_health():
    url = os.environ.get('IB_BRIDGE_URL', '').rstrip('/')
    if not url:
        return {'ok': False, 'error': 'IB_BRIDGE_URL nao configurada', 'mode': _mode()}
    try:
        r = requests.get(f'{url}/health',
                         headers={'X-Bridge-Secret': os.environ.get('IB_BRIDGE_SECRET', '')},
                         timeout=6)
        return {'ok': r.status_code == 200, 'mode': _mode(), 'bridge': r.json() if r.status_code == 200 else r.text[:150]}
    except Exception as e:
        return {'ok': False, 'mode': _mode(), 'error': str(e)}
