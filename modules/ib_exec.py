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

Regras (qualquer modo): teto de LIQUIDEZ por ordem IB_EXEC_MAX_USD (150000; espelha
o notional real do book ate esse teto — nunca o fill fantasma de US$1M/perna), teto diario
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
        # garante colunas idempotentes (venue + custo de execucao Tier1 25-jul)
        for _col in ("venue VARCHAR(8) DEFAULT 'BINANCE'", "shortfall_bps DECIMAL(10,3)",
                     "total_cost_bps DECIMAL(10,3)"):
            try: cur.execute(f"ALTER TABLE exec_orders ADD COLUMN {_col}")
            except Exception: pass
        try:
            from modules.exec_metrics import implementation_shortfall_bps, total_cost_bps
            _sf = implementation_shortfall_bps(row.get('side'), row.get('price_ref'), row.get('price_fill'))
            _tc = total_cost_bps(_sf, row.get('fee'), row.get('usd'))
        except Exception:
            _sf = _tc = None
        cur.execute("""INSERT INTO exec_orders (trade_id,symbol,side,event,mode,status,
            qty,quote_usdt,price_ref,price_fill,fee_usdt,binance_order_id,error,resp_json,venue,
            shortfall_bps,total_cost_bps)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'IB',%s,%s)""",
            (row.get('trade_id'), row.get('symbol'), row.get('side'), row.get('event'),
             row.get('mode'), row.get('status'), row.get('qty'), row.get('usd'),
             row.get('price_ref'), row.get('price_fill'), row.get('fee'),
             str(row.get('ib_order_id') or ''), row.get('error'),
             json.dumps(row.get('resp'), default=str)[:2000] if row.get('resp') else None,
             _sf, _tc))
        c.close()
    except Exception as e:
        log.debug(f'[IB-EXEC] record: {e}')


def _guard(usd):
    if usd > _f('IB_EXEC_MAX_USD', 150000):
        return f'ordem ${usd:.0f} > teto IB_EXEC_MAX_USD'
    today = date.today()
    if _day['d'] != today:
        _day['d'] = today; _day['n'] = 0
    if _day['n'] >= int(_f('IB_EXEC_MAX_ORDERS_DAY', 80)):
        return 'teto diario de ordens IB'
    return None


# [24-jul, decisao Beto] mapa mercado interno -> (bolsa IB, moeda). Sufixo do
# ticker tambem resolve. B3 NAO esta aqui (nao e alcancavel via IB -> ProfitDLL).
_IB_EXCH = {
    'NYSE': ('SMART', 'USD'), 'NASDAQ': ('SMART', 'USD'), 'US': ('SMART', 'USD'),
    'LSE': ('LSE', 'GBP'), 'XETRA': ('IBIS', 'EUR'),
    'EURONEXT': ('SMART', 'EUR'), 'TSX': ('TSE', 'CAD'), 'TSXV': ('VENTURE', 'CAD'),
}
_SUFFIX_EXCH = {
    '.L': ('LSE', 'GBP'), '.DE': ('IBIS', 'EUR'), '.AS': ('AEB', 'EUR'),
    '.PA': ('SBF', 'EUR'), '.MI': ('BVME', 'EUR'), '.SW': ('EBS', 'CHF'),
    '.TO': ('TSE', 'CAD'), '.V': ('VENTURE', 'CAD'),
}


def ib_reachable(mkt, leg=''):
    """True se a perna e negociavel na IB (nao e B3)."""
    m = str(mkt or '').upper()
    if m == 'B3':
        return False
    if m in _IB_EXCH:
        return True
    return any(str(leg).upper().endswith(s) for s in _SUFFIX_EXCH)


def _resolve_leg(leg, mkt):
    """Retorna (symbol_ib, exchange, currency) para uma perna cross-listada."""
    leg = str(leg).upper()
    for suf, (ex, cur) in _SUFFIX_EXCH.items():
        if leg.endswith(suf.upper()):
            return leg[:-len(suf)], ex, cur
    ex, cur = _IB_EXCH.get(str(mkt or '').upper(), ('SMART', 'USD'))
    return leg, ex, cur


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


# ═══ [28-jul-2026] COTACAO via bridge IB (real-time, mesma fonte das duas pernas) ═══
# Mapa sufixo Yahoo -> (exchange IB, moeda). US sem sufixo -> SMART/USD.
_YH2IB = {
    '.L':  ('LSE',  'GBP'), '.AS': ('AEB',  'EUR'), '.PA': ('SBF',  'EUR'),
    '.DE': ('IBIS', 'EUR'), '.MC': ('BM',   'EUR'), '.IR': ('ISED', 'EUR'),
    '.TO': ('TSE',  'CAD'), '.MI': ('BVME', 'EUR'), '.SW': ('EBS',  'CHF'),
}

def yahoo_to_ib(sym):
    """'BP.L' -> ('BP','LSE','GBP'); 'AAPL' -> ('AAPL','SMART','USD')."""
    s = str(sym or '').upper()
    for suf, (exch, curr) in _YH2IB.items():
        if s.endswith(suf):
            return s[:-len(suf)], exch, curr
    return s, 'SMART', 'USD'

def bridge_quote(instruments, md_type=1, timeout=15):
    """Cotacao snapshot de N papeis via bridge IB. instruments: lista de
    {symbol,exchange,currency} OU strings estilo Yahoo ('BP.L').
    Retorna (dict{sym_original: row}, err)."""
    url = os.environ.get('IB_BRIDGE_URL', '').rstrip('/')
    if not url:
        return None, 'IB_BRIDGE_URL nao configurada'
    payload = []
    orig = []
    for it in instruments:
        if isinstance(it, str):
            sym, exch, curr = yahoo_to_ib(it)
            payload.append({'symbol': sym, 'exchange': exch, 'currency': curr})
            orig.append(it)
        else:
            payload.append(it); orig.append(it.get('symbol'))
    try:
        r = requests.post(f'{url}/quote', json={'instruments': payload, 'md_type': md_type},
                          headers={'X-Bridge-Secret': os.environ.get('IB_BRIDGE_SECRET', '')},
                          timeout=timeout)
        if r.status_code != 200:
            return None, f'{r.status_code}: {r.text[:180]}'
        rows = (r.json() or {}).get('quotes', [])
        out = {orig[i]: rows[i] for i in range(min(len(orig), len(rows)))}
        return out, None
    except Exception as e:
        return None, str(e)


def _execute(trade, event, action):
    """action = BUY | SELL | SHORT | COVER."""
    if os.environ.get('IB_EXEC_ENGINE_ENABLED', 'true').lower() == 'false':
        return
    mode = _mode()
    sym = str(trade.get('symbol', '')).upper()
    px = float(trade.get('current_price') or trade.get('entry_price') or 0)
    # [decisao Beto] espelha o notional REAL da posicao do book (nao ticket de US$1k),
    # com TETO DE LIQUIDEZ por ordem (IB_EXEC_MAX_USD) — nunca o fill impossivel de US$1M.
    _real = float(trade.get('position_value') or trade.get('position_size') or 0)
    usd = min(_real if _real > 0 else _f('IB_EXEC_ORDER_USD', 1000), _f('IB_EXEC_MAX_USD', 150000))
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


def exec_arbi(pair, direction, price_a, price_b, event, ref_id=None):
    """[24-jul, decisao Beto] Executa as DUAS pernas de um par Arbi cross-listado
    na IB (long + short reais). So dispara se AMBAS as pernas sao alcancaveis na
    IB; par com perna B3 e pulado (espera ProfitDLL). event = OPEN | CLOSE.
    ref_id: id do trade fantasma, para amarrar exec_orders ao book."""
    try:
        # [DIAG 28-jul] grava o motivo de pular (Arbi nunca executou no IB — 0 registros).
        _tid = ref_id or pair.get('id')
        _sym = f"{pair.get('leg_a')}/{pair.get('leg_b')}"
        def _skip(reason):
            _record({'trade_id': _tid, 'symbol': _sym, 'side': '-', 'event': f'ARBI_{event}',
                     'mode': _mode(), 'status': 'BLOCKED', 'error': f'DIAG:{reason}'})
        if os.environ.get('IB_ARBI_ENABLED', 'true').lower() == 'false':
            _skip('IB_ARBI_ENABLED=false'); return
        la, lb = pair.get('leg_a'), pair.get('leg_b')
        ma, mb = pair.get('mkt_a'), pair.get('mkt_b')
        _ra = ib_reachable(ma, la); _rb = ib_reachable(mb, lb)
        if not (_ra and _rb):
            # so registra se PELO MENOS uma perna for alcancavel (evita ruido dos pares B3-B3);
            # o caso interessante e "quase da" (uma perna IB, outra nao).
            if _ra or _rb:
                _skip(f'nao_reachable a={ma}:{_ra} b={mb}:{_rb}')
            return  # perna B3 -> nao executavel na IB
        mode = _mode()
        ra = float(pair.get('ratio_a') or 1) or 1.0
        rb = float(pair.get('ratio_b') or 1) or 1.0
        pa = float(price_a or 0)
        if pa <= 0:
            _skip(f'price0 pa={price_a}'); return
        # [decisao Beto] espelha o notional real da perna, com TETO DE LIQUIDEZ.
        # O $1M/perna do book e inexequivel em ADR iliquido (auditoria forense) —
        # por isso o teto IB_EXEC_MAX_USD limita, evitando o fill fantasma.
        _arbi_real = float(pair.get('position_size') or pair.get('leg_notional') or 0)
        leg_usd = min(_arbi_real if _arbi_real > 0 else _f('IB_ARBI_LEG_USD', 50000), _f('IB_EXEC_MAX_USD', 150000))
        qa = max(1, int(leg_usd / pa))
        qb = max(1, int(qa * rb / ra))
        if direction == 'LONG_A':
            act_a, act_b = 'BUY', 'SELL'
        else:  # LONG_B
            act_a, act_b = 'SELL', 'BUY'
        if event == 'CLOSE':
            act_a = 'SELL' if act_a == 'BUY' else 'BUY'
            act_b = 'SELL' if act_b == 'BUY' else 'BUY'
        g = _guard(leg_usd)
        if g:
            _record({'trade_id': _tid, 'symbol': f'{la}/{lb}', 'side': f'{act_a}/{act_b}',
                     'event': f'ARBI_{event}', 'mode': mode, 'status': 'BLOCKED', 'usd': leg_usd, 'error': g})
            return
        for leg, mkt, qty, act in ((la, ma, qa, act_a), (lb, mb, qb, act_b)):
            sym_ib, exch, cur = _resolve_leg(leg, mkt)
            if mode == 'ghost':
                _record({'trade_id': _tid, 'symbol': sym_ib, 'side': act,
                         'event': f'ARBI_{event}', 'mode': 'ghost', 'status': 'SIMULATED', 'qty': qty})
                log.info(f'[IB-GHOST-ARBI] {event} {pair.get("id")} {sym_ib}({exch}) {act} {qty}sh')
                continue
            d, err = _bridge({'symbol': sym_ib, 'action': act, 'quantity': qty,
                              'exchange': exch, 'currency': cur, 'mode': mode})
            if err:
                _record({'trade_id': _tid, 'symbol': sym_ib, 'side': act,
                         'event': f'ARBI_{event}', 'mode': mode, 'status': 'ERROR', 'qty': qty, 'error': err[:200]})
                log.warning(f'[IB-ARBI] {sym_ib}({exch}) {act} ERRO: {err}')
                continue
            _record({'trade_id': _tid, 'symbol': sym_ib, 'side': act, 'event': f'ARBI_{event}',
                     'mode': mode, 'status': d.get('status'), 'qty': d.get('filled', qty),
                     'price_fill': d.get('avg_price'), 'fee': d.get('commission'),
                     'ib_order_id': d.get('order_id'), 'resp': d})
            log.warning(f'[IB-ARBI] {event} {pair.get("id")} {sym_ib}({exch}) {act} {qty} -> '
                        f'{d.get("status")} @ {d.get("avg_price")}')
        _day['n'] += 1
    except Exception as e:
        log.debug(f'[IB-ARBI] exec {e}')


def _has_open_exec(tid):
    """True se ja houve um USPAIRS_OPEN executado (nao ERROR/BLOCKED) para este
    trade_id. Evita disparar CLOSE de posicoes legadas sem perna real no IB."""
    if not tid:
        return False
    try:
        from modules.binance_exec import _conn
        c = _conn()
        cur = c.cursor()
        cur.execute("SELECT COUNT(*) FROM exec_orders WHERE venue='IB' AND event='USPAIRS_OPEN' "
                    "AND trade_id=%s AND status NOT IN ('ERROR','BLOCKED')", (str(tid),))
        n = cur.fetchone()[0]
        c.close()
        return n > 0
    except Exception as e:
        log.debug(f'[IB-USPAIRS] _has_open_exec: {e}')
        return False  # em duvida, NAO fecha (fail-safe: evita ordem naked)


def exec_uspairs(pair, direction, price_a, price_b, notional_a, notional_b, event, ref_id=None):
    """[24-jul, decisao Beto] Espelha um trade do motor US Pairs (shadow) no IB
    paper. As DUAS pernas sao acoes US (SMART/USD), sempre IB-reachable. O motor
    decide pos-fechamento (candle diario), entao usamos Market-on-Open (tif=OPG):
    a ordem enche na abertura seguinte -> slippage real, execucao honesta.

    direction: SHORT_A (vende A, compra B) | LONG_A (compra A, vende B).
    event: OPEN | CLOSE (no CLOSE as pernas invertem).
    O book shadow (uspairs_shadow_trades) NAO e tocado — isto e ledger paralelo."""
    try:
        if os.environ.get('IB_USPAIRS_ENABLED', 'true').lower() == 'false':
            return
        _tid = ref_id or pair
        try:
            a, b = str(pair).split('-')
        except Exception:
            return
        pa = float(price_a or 0)
        pb = float(price_b or 0)
        if pa <= 0 or pb <= 0:
            return
        # [decisao Beto] espelha o notional real, MAS capado pelo teto de liquidez
        # ANTES de calcular a quantidade (senao o qty sairia do notional cheio).
        _cap = _f('IB_EXEC_MAX_USD', 150000)
        na = min(float(notional_a or 0), _cap)
        nb = min(float(notional_b or 0), _cap)
        qa = max(1, int(round(na / pa)))
        qb = max(1, int(round(nb / pb)))
        if str(direction).upper() == 'SHORT_A':
            act_a, act_b = 'SELL', 'BUY'
        else:  # LONG_A
            act_a, act_b = 'BUY', 'SELL'
        if str(event).upper() == 'CLOSE':
            # trava: so fecha no IB se ESTE trade teve um OPEN executado antes.
            # posicoes abertas no shadow antes do hook existir NAO tem perna IB —
            # fechar geraria ordem naked. _has_open_exec evita isso.
            if not _has_open_exec(_tid):
                log.info(f'[IB-USPAIRS] CLOSE {pair} ignorado: sem OPEN no IB (trade legado)')
                return
            act_a = 'SELL' if act_a == 'BUY' else 'BUY'
            act_b = 'SELL' if act_b == 'BUY' else 'BUY'
        mode = _mode()
        tif = os.environ.get('IB_USPAIRS_TIF', 'OPG').upper()
        # teto por perna: reaproveita IB_EXEC_MAX_USD
        for leg_sym, qty, act, notion in ((a, qa, act_a, na), (b, qb, act_b, nb)):
            g = _guard(min(notion, _f('IB_EXEC_MAX_USD', 150000)))
            if g:
                _record({'trade_id': _tid, 'symbol': leg_sym, 'side': act,
                         'event': f'USPAIRS_{event}', 'mode': mode, 'status': 'BLOCKED',
                         'usd': notion, 'error': g})
                continue
            if mode == 'ghost':
                _day['n'] += 1
                _record({'trade_id': _tid, 'symbol': leg_sym, 'side': act,
                         'event': f'USPAIRS_{event}', 'mode': 'ghost', 'status': 'SIMULATED',
                         'qty': qty, 'usd': notion, 'price_ref': pa if leg_sym == a else pb})
                log.info(f'[IB-GHOST-USPAIRS] {event} {pair} {leg_sym} {act} {qty}sh (tif={tif})')
                continue
            d, err = _bridge({'symbol': leg_sym, 'action': act, 'quantity': qty,
                              'exchange': 'SMART', 'currency': 'USD', 'tif': tif,
                              'mode': mode, 'trade_id': _tid})
            if err:
                _record({'trade_id': _tid, 'symbol': leg_sym, 'side': act,
                         'event': f'USPAIRS_{event}', 'mode': mode, 'status': 'ERROR',
                         'qty': qty, 'usd': notion, 'error': err[:200]})
                log.warning(f'[IB-USPAIRS] {pair} {leg_sym} {act} ERRO: {err}')
                continue
            _day['n'] += 1
            _record({'trade_id': _tid, 'symbol': leg_sym, 'side': act, 'event': f'USPAIRS_{event}',
                     'mode': mode, 'status': d.get('status', 'SENT'), 'qty': d.get('filled', qty),
                     'usd': notion, 'price_ref': pa if leg_sym == a else pb,
                     'price_fill': d.get('avg_price'), 'fee': d.get('commission'),
                     'ib_order_id': d.get('order_id'), 'resp': d})
            log.warning(f'[IB-USPAIRS] {event} {pair} {leg_sym} {act} {qty} (tif={tif}) -> '
                        f'{d.get("status")} @ {d.get("avg_price")} id={d.get("order_id")}')
    except Exception as e:
        log.debug(f'[IB-USPAIRS] exec {e}')


def exec_longleg(sym, mkt, action, qty, tag, price_ref=None):
    """[decisao Beto] Perna long da 'limonada' como book proprio (tag LL-, event LONGLEG).
    action = BUY (abre) | SELL (fecha). Single-leg, so em bolsa IB-reachable (nao-B3).
    Retorna (fill_price, status). Fail-open: erro NUNCA derruba nada."""
    try:
        if os.environ.get('IB_LONGLEG_ENABLED', 'true').lower() == 'false':
            return None, 'DISABLED'
        if os.environ.get('IB_EXEC_ENGINE_ENABLED', 'true').lower() == 'false':
            return None, 'ENGINE_OFF'
        if not ib_reachable(mkt, sym):
            return None, 'NAO_IB'
        qty = int(qty or 0)
        if qty <= 0:
            return None, 'QTY0'
        mode = _mode()
        sym_ib, exch, cur = _resolve_leg(sym, mkt)
        usd = (float(price_ref) * qty) if price_ref else 0
        g = _guard(usd) if usd else None
        if g:
            _record({'trade_id': tag, 'symbol': sym_ib, 'side': action, 'event': 'LONGLEG',
                     'mode': mode, 'status': 'BLOCKED', 'qty': qty, 'usd': usd, 'error': g})
            return None, 'BLOCKED'
        if mode == 'ghost':
            _day['n'] += 1
            _record({'trade_id': tag, 'symbol': sym_ib, 'side': action, 'event': 'LONGLEG',
                     'mode': 'ghost', 'status': 'SIMULATED', 'qty': qty, 'usd': usd,
                     'price_ref': price_ref, 'price_fill': price_ref})
            return (float(price_ref) if price_ref else None), 'SIMULATED'
        d, err = _bridge({'symbol': sym_ib, 'action': action, 'quantity': qty,
                          'exchange': exch, 'currency': cur, 'mode': mode, 'trade_id': tag})
        if err:
            _record({'trade_id': tag, 'symbol': sym_ib, 'side': action, 'event': 'LONGLEG',
                     'mode': mode, 'status': 'ERROR', 'qty': qty, 'usd': usd, 'error': err[:200]})
            return None, 'ERROR'
        _day['n'] += 1
        _record({'trade_id': tag, 'symbol': sym_ib, 'side': action, 'event': 'LONGLEG',
                 'mode': mode, 'status': d.get('status', 'SENT'), 'qty': d.get('filled', qty),
                 'usd': usd, 'price_ref': price_ref, 'price_fill': d.get('avg_price'),
                 'fee': d.get('commission'), 'ib_order_id': d.get('order_id'), 'resp': d})
        return (d.get('avg_price') or price_ref), d.get('status', 'SENT')
    except Exception as e:
        log.debug(f'[IB-LONGLEG] {sym} {action}: {e}')
        return None, 'EXC'


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
