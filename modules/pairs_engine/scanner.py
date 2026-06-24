"""Scanner principal do pairs engine.

Roda em loop: pega quotes, calcula z-score, identifica oportunidades.
Quando |z| > z_entry e correlacao > 0.5: abre trade paper.
Quando |z| < z_exit: fecha trade aberta.
Quando |z| > z_stop: stop loss.
"""
import os
import time
import logging
import math
from datetime import datetime
from threading import Lock
from typing import Dict, Optional, List

from .config import PAIRS_CONFIG, get_pair, all_symbols
from .data_fetcher import fetch_pair_history, fetch_pair_quotes_bulk
from .zscore import calc_spread_series, calc_zscore_stats, calc_hedge_ratio, calc_correlation

log = logging.getLogger('egreja.pairs')

# Estado em memoria (similar ao arbi_open/arbi_closed)
pairs_open: List[Dict] = []
pairs_closed: List[Dict] = []
pairs_state_lock = Lock()
pairs_spreads: Dict[str, Dict] = {}  # cache spread/z atual por pair_id
pairs_history_cache: Dict[str, Dict] = {}  # cache historico por simbolo (refresh 1x/dia)

# Capital paper para pairs (separado do arbi)
PAIRS_CAPITAL = float(os.environ.get('PAIRS_CAPITAL', 1_000_000.0))  # R$ 1MM default
PAIRS_MAX_POSITIONS = int(os.environ.get('PAIRS_MAX_POSITIONS', 5))  # max 5 trades simultaneas
PAIRS_SCAN_INTERVAL_S = int(os.environ.get('PAIRS_SCAN_INTERVAL_S', 30))  # scan a cada 30s
PAIRS_MIN_CORRELATION = float(os.environ.get('PAIRS_MIN_CORRELATION', 0.5))


def _gen_trade_id():
    """Gera ID unico para trade pairs."""
    import uuid
    return f"PAIR-{uuid.uuid4().hex[:12]}"


def _refresh_history(symbol: str, force: bool = False) -> List[Dict]:
    """Cache de historico (24h TTL)."""
    now = time.time()
    cached = pairs_history_cache.get(symbol)
    if cached and not force and (now - cached.get('ts', 0)) < 86400:
        return cached.get('data', [])
    data = fetch_pair_history(symbol, days=70)  # 70 pra dar margem ao window 60
    pairs_history_cache[symbol] = {'ts': now, 'data': data}
    return data


def calc_pair_signal(pair_config: Dict, quotes: Dict[str, Dict]) -> Optional[Dict]:
    """Calcula sinal para um par especifico.

    Args:
        pair_config: dict com config do par (id, leg_a, leg_b, z_entry, etc)
        quotes: dict com quotes atuais por symbol

    Returns:
        Dict com z-score + recomendacao, ou None se dados insuficientes.
    """
    leg_a = pair_config['leg_a']
    leg_b = pair_config['leg_b']
    qa = quotes.get(leg_a)
    qb = quotes.get(leg_b)
    if not qa or not qb:
        return None

    # Pegar historico cacheado
    hist_a = _refresh_history(leg_a)
    hist_b = _refresh_history(leg_b)
    if len(hist_a) < 60 or len(hist_b) < 60:
        return None

    # Alinhar series por data (intersecao)
    dates_a = {h['date']: h['close'] for h in hist_a}
    dates_b = {h['date']: h['close'] for h in hist_b}
    common = sorted(set(dates_a.keys()) & set(dates_b.keys()))
    if len(common) < 60:
        return None
    prices_a = [dates_a[d] for d in common]
    prices_b = [dates_b[d] for d in common]

    # Adicionar quote atual ao final (para z atual ser refletido em tempo real)
    prices_a_now = prices_a + [qa['price']]
    prices_b_now = prices_b + [qb['price']]

    # Calcular spread series e z-score
    pair_type = pair_config.get('pair_type', 'SECTORIAL')
    method = 'log_ratio'  # default
    if pair_type == 'CLASSES':
        method = 'pct_diff'  # melhor pra ON/PN
    spread_series = calc_spread_series(prices_a_now, prices_b_now, method=method)
    stats = calc_zscore_stats(spread_series, window=60)
    if stats.get('z') is None:
        return None

    # Hedge ratio dinamico
    hedge = calc_hedge_ratio(prices_a_now, prices_b_now, window=60) or pair_config.get('beta_a_to_b', 1.0)

    # Correlacao
    corr = calc_correlation(prices_a_now, prices_b_now, window=60) or 0

    # Decisao
    z = stats['z']
    z_entry = pair_config.get('z_entry', 2.0)
    z_exit = pair_config.get('z_exit', 0.4)
    z_stop = pair_config.get('z_stop', 3.5)

    action = 'HOLD'
    direction = None
    if abs(z) > z_stop:
        action = 'AVOID'  # regime shift possivel, nao entrar
    elif abs(z) >= z_entry:
        action = 'ENTRY'
        # Se z > 0: spread acima da media => A esta caro relativo a B
        #   => Short A, Long B
        # Se z < 0: spread abaixo da media => A esta barato relativo a B
        #   => Long A, Short B
        direction = 'SHORT_A' if z > 0 else 'LONG_A'
    elif abs(z) < z_exit:
        action = 'CONVERGED'

    return {
        'pair_id': pair_config['id'],
        'name': pair_config['name'],
        'leg_a': leg_a, 'leg_b': leg_b,
        'price_a': qa['price'], 'price_b': qb['price'],
        'bid_a': qa.get('bid', qa['price']), 'ask_a': qa.get('ask', qa['price']),
        'bid_b': qb.get('bid', qb['price']), 'ask_b': qb.get('ask', qb['price']),
        'spread_method': method,
        'spread_current': stats['current'],
        'spread_mean_60d': stats['mean'],
        'spread_stdev_60d': stats['stdev'],
        'z_score': z,
        'z_entry_threshold': z_entry,
        'z_exit_threshold': z_exit,
        'z_stop_threshold': z_stop,
        'hedge_ratio': hedge,
        'correlation_60d': corr,
        'action': action,
        'direction': direction,
        'pair_type': pair_type,
        'enabled_by_correlation': corr >= PAIRS_MIN_CORRELATION,
        'timestamp': datetime.utcnow().isoformat(),
    }


def open_pair_trade(signal: Dict, beat_fn=None, audit_fn=None, enqueue_fn=None):
    """Abre uma trade pairs em PAPER mode.

    Args:
        signal: output de calc_pair_signal com action='ENTRY'
    """
    if signal.get('action') != 'ENTRY':
        return None
    if not signal.get('enabled_by_correlation'):
        log.info(f'[PAIRS] {signal["pair_id"]}: correlation {signal["correlation_60d"]:.2f} below '
                 f'{PAIRS_MIN_CORRELATION:.2f} — entry SKIPPED')
        return None

    with pairs_state_lock:
        # Nao abrir 2 trades no mesmo par
        if any(t['pair_id'] == signal['pair_id'] for t in pairs_open):
            return None
        if len(pairs_open) >= PAIRS_MAX_POSITIONS:
            log.info(f'[PAIRS] max positions ({PAIRS_MAX_POSITIONS}) reached — skip {signal["pair_id"]}')
            return None

        # Position size = capital / max_positions
        pos_size = round(PAIRS_CAPITAL / PAIRS_MAX_POSITIONS, 2)
        hedge_ratio = signal.get('hedge_ratio', 1.0)
        # Qty leg_a = capital_a / price_a
        # Qty leg_b = qty_a * hedge_ratio  (na quantidade que faz a perna B financeiramente equilibrada)
        qty_a = int(pos_size / 2 / max(signal['price_a'], 0.01))
        qty_b = int(qty_a * abs(hedge_ratio))

        trade_id = _gen_trade_id()
        now_iso = datetime.utcnow().isoformat()
        trade = {
            'id': trade_id,
            'pair_id': signal['pair_id'],
            'name': signal['name'],
            'leg_a': signal['leg_a'], 'leg_b': signal['leg_b'],
            'direction': signal['direction'],   # SHORT_A ou LONG_A
            'pair_type': signal['pair_type'],
            'entry_z': signal['z_score'],
            'entry_spread': signal['spread_current'],
            'entry_spread_mean': signal['spread_mean_60d'],
            'entry_spread_stdev': signal['spread_stdev_60d'],
            'hedge_ratio': hedge_ratio,
            'correlation_60d': signal['correlation_60d'],
            'price_a_entry': signal['price_a'],
            'price_b_entry': signal['price_b'],
            'qty_a': qty_a, 'qty_b': qty_b,
            'position_size': pos_size,
            'opened_at': now_iso,
            'status': 'OPEN',
            'asset_type': 'pairs',
            'mode': 'paper',
            'current_z': signal['z_score'],
            'pnl': 0, 'pnl_pct': 0,
            'peak_pnl_pct': 0,
        }
        pairs_open.append(trade)

    log.info(f'[PAIRS] OPEN {trade_id} {signal["pair_id"]} dir={signal["direction"]} '
             f'z={signal["z_score"]:+.2f} corr={signal["correlation_60d"]:.2f} '
             f'qty_a={qty_a} qty_b={qty_b}')
    if audit_fn:
        try: audit_fn('PAIRS_OPENED', {'id': trade_id, 'pair_id': signal['pair_id'],
                                        'z': signal['z_score'], 'direction': signal['direction']})
        except Exception: pass
    return trade


def evaluate_pair_trade_exit(trade: Dict, signal: Dict, audit_fn=None) -> Optional[str]:
    """Avalia se trade aberta deve ser fechada.

    Returns:
        'CONVERGED' / 'STOP_LOSS' / None
    """
    z_now = signal.get('z_score')
    if z_now is None:
        return None
    entry_z = trade['entry_z']
    z_exit = signal['z_exit_threshold']
    z_stop = signal['z_stop_threshold']

    # Converged: z cruzou pra perto de zero
    if abs(z_now) <= z_exit:
        return 'CONVERGED'

    # Stop: z foi MAIS LONGE da media (regime shift)
    if abs(z_now) > z_stop:
        return 'STOP_LOSS'

    # Se z mudou de sinal (passou pelo zero), tambem converged
    if (entry_z > 0 and z_now < 0) or (entry_z < 0 and z_now > 0):
        return 'CONVERGED'

    return None


def close_pair_trade(trade: Dict, signal: Dict, reason: str, audit_fn=None):
    """Fecha trade pairs e calcula PnL (paper)."""
    with pairs_state_lock:
        if trade not in pairs_open:
            return
        # PnL paper: depende da direcao
        # Se SHORT_A (z entrou positivo, spread acima da media):
        #   pnl_a = -(price_a_exit - price_a_entry) * qty_a   (short)
        #   pnl_b = (price_b_exit - price_b_entry) * qty_b    (long)
        # Se LONG_A (z entrou negativo):
        #   pnl_a = (price_a_exit - price_a_entry) * qty_a    (long)
        #   pnl_b = -(price_b_exit - price_b_entry) * qty_b   (short)
        price_a_exit = signal['price_a']
        price_b_exit = signal['price_b']
        qty_a = trade['qty_a']
        qty_b = trade['qty_b']
        if trade['direction'] == 'SHORT_A':
            pnl_a = -(price_a_exit - trade['price_a_entry']) * qty_a
            pnl_b =  (price_b_exit - trade['price_b_entry']) * qty_b
        else:  # LONG_A
            pnl_a =  (price_a_exit - trade['price_a_entry']) * qty_a
            pnl_b = -(price_b_exit - trade['price_b_entry']) * qty_b
        pnl = round(pnl_a + pnl_b, 2)
        pnl_pct = round(100 * pnl / max(trade['position_size'], 1), 4)

        trade['price_a_exit'] = price_a_exit
        trade['price_b_exit'] = price_b_exit
        trade['exit_z'] = signal['z_score']
        trade['exit_spread'] = signal['spread_current']
        trade['close_reason'] = reason
        trade['closed_at'] = datetime.utcnow().isoformat()
        trade['pnl'] = pnl
        trade['pnl_pct'] = pnl_pct
        trade['status'] = 'CLOSED'
        pairs_open.remove(trade)
        pairs_closed.append(trade)

    log.info(f'[PAIRS] CLOSE {trade["id"]} {trade["pair_id"]} reason={reason} '
             f'pnl=${pnl:+,.2f} ({pnl_pct:+.3f}%) entry_z={trade["entry_z"]:+.2f} exit_z={signal["z_score"]:+.2f}')
    if audit_fn:
        try: audit_fn('PAIRS_CLOSED', {'id': trade['id'], 'pair_id': trade['pair_id'],
                                        'reason': reason, 'pnl': pnl, 'pnl_pct': pnl_pct})
        except Exception: pass


def pairs_scan_loop(beat_fn=None, audit_fn=None, enqueue_fn=None):
    """Loop principal — scan a cada PAIRS_SCAN_INTERVAL_S segundos."""
    log.info(f'[PAIRS] scanner iniciando | capital_paper=R${PAIRS_CAPITAL:,.0f} '
             f'max_positions={PAIRS_MAX_POSITIONS} interval={PAIRS_SCAN_INTERVAL_S}s')
    while True:
        if beat_fn: beat_fn('pairs_scan_loop')
        try:
            # Coletar todos os simbolos necessarios
            syms = list(all_symbols())
            quotes = fetch_pair_quotes_bulk(syms)
            if not quotes:
                time.sleep(PAIRS_SCAN_INTERVAL_S)
                continue

            for pcfg in PAIRS_CONFIG:
                if not pcfg.get('enabled', True):
                    continue
                if beat_fn: beat_fn('pairs_scan_loop')
                signal = calc_pair_signal(pcfg, quotes)
                if not signal:
                    continue
                pairs_spreads[pcfg['id']] = signal  # cache

                # Existe trade aberta neste par?
                open_trade = None
                with pairs_state_lock:
                    for t in pairs_open:
                        if t['pair_id'] == pcfg['id']:
                            open_trade = t
                            break

                if open_trade:
                    exit_reason = evaluate_pair_trade_exit(open_trade, signal, audit_fn=audit_fn)
                    if exit_reason:
                        close_pair_trade(open_trade, signal, exit_reason, audit_fn=audit_fn)
                else:
                    if signal['action'] == 'ENTRY':
                        open_pair_trade(signal, audit_fn=audit_fn, enqueue_fn=enqueue_fn)

        except Exception as e:
            log.error(f'[PAIRS] scan loop erro: {e}')
            import traceback; traceback.print_exc()
        time.sleep(PAIRS_SCAN_INTERVAL_S)
