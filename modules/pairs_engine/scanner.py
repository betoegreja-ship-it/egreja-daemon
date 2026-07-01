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

from .config import PAIRS_CONFIG, get_pair, all_symbols, pairs_by_tier
from .data_fetcher import fetch_pair_history, fetch_pair_quotes_bulk
from .zscore import calc_spread_series, calc_zscore_stats, calc_hedge_ratio, calc_correlation
from . import persistence as _persist
from . import learning as _learning

log = logging.getLogger('egreja.pairs')

# Estado em memoria (similar ao arbi_open/arbi_closed)
pairs_open: List[Dict] = []
pairs_closed: List[Dict] = []
pairs_state_lock = Lock()
pairs_spreads: Dict[str, Dict] = {}  # cache spread/z atual por pair_id
pairs_history_cache: Dict[str, Dict] = {}  # cache historico por simbolo (refresh 1x/dia)

# Capital paper para pairs (separado do arbi)
PAIRS_CAPITAL = float(os.environ.get('PAIRS_CAPITAL', 1_000_000.0))  # R$ 1MM default
# [01-jul-2026] Sizing dinamico: aloca ate esgotar PAIRS_CAPITAL, respeitando
# minimo por trade. Antes: 5 posicoes fixas de R$200k. Agora: min R$100k/trade,
# max ceil (PAIRS_CAPITAL / PAIRS_MIN_INVESTMENT) posicoes simultaneas.
PAIRS_MIN_INVESTMENT = float(os.environ.get('PAIRS_MIN_INVESTMENT', 100_000.0))  # min R$100k/trade
PAIRS_MAX_POSITIONS = int(os.environ.get('PAIRS_MAX_POSITIONS', 10))  # ceiling de posicoes (safety)
PAIRS_SCAN_INTERVAL_S = int(os.environ.get('PAIRS_SCAN_INTERVAL_S', 30))  # scan a cada 30s
PAIRS_MIN_CORRELATION = float(os.environ.get('PAIRS_MIN_CORRELATION', 0.5))
# [01-jul-2026] SWAP: se capital cheio e sinal novo tem |z| bem melhor,
# fecha a trade aberta mais fraca (menor |current_z|) e abre a nova.
PAIRS_SWAP_ENABLED = os.environ.get('PAIRS_SWAP_ENABLED', 'true').lower() == 'true'
PAIRS_SWAP_Z_MARGIN = float(os.environ.get('PAIRS_SWAP_Z_MARGIN', 0.5))  # |z_new| >= |z_worst| + margin


def _deployed_capital() -> float:
    """Soma o position_size das trades abertas (usar dentro do lock)."""
    return sum(float(t.get('position_size', 0) or 0) for t in pairs_open)


def _available_capital() -> float:
    """Capital ainda nao alocado (usar dentro do lock)."""
    return max(PAIRS_CAPITAL - _deployed_capital(), 0.0)


def _worst_open_trade() -> Optional[Dict]:
    """Trade aberta com menor |current_z| (candidata a ser trocada)."""
    if not pairs_open:
        return None
    def _key(t):
        z = t.get('current_z', t.get('entry_z', 0)) or 0
        return abs(z)
    return min(pairs_open, key=_key)


def _gen_trade_id():
    """Gera ID unico para trade pairs."""
    import uuid
    return f"PAIR-{uuid.uuid4().hex[:12]}"


PAIRS_HISTORY_DAYS = int(os.environ.get('PAIRS_HISTORY_DAYS', 250))  # ~1y de pregoes
PAIRS_HISTORY_TTL_S = int(os.environ.get('PAIRS_HISTORY_TTL_S', 14400))  # 4h cache

def _refresh_history(symbol: str, force: bool = False) -> List[Dict]:
    """Cache de historico (4h TTL). Source-of-truth: pairs_history_daily MySQL.

    Fluxo:
    1. Memoria (TTL 4h) — mais rapido
    2. MySQL pairs_history_daily — fonte persistente
    3. BRAPI+Cedro fusion — apenas se DB tem pouca data
    """
    now = time.time()
    cached = pairs_history_cache.get(symbol)
    if cached and not force and (now - cached.get('ts', 0)) < PAIRS_HISTORY_TTL_S:
        return cached.get('data', [])

    # Tentar DB primeiro (mais denso, ja consolidado)
    db_data = _persist.load_history_from_db(symbol, days=PAIRS_HISTORY_DAYS)
    if len(db_data) >= 80:  # janela 60 + margem
        pairs_history_cache[symbol] = {'ts': now, 'data': db_data}
        return db_data

    # Sem DB suficiente — fetch fresh + persiste
    log.info(f'[PAIRS] {symbol}: DB tem {len(db_data)} bars, fetch fresh...')
    data = fetch_pair_history(symbol, days=PAIRS_HISTORY_DAYS)
    if data:
        n = _persist.bulk_upsert_daily_bars(symbol, data, source=data[0].get('source', 'brapi'))
        log.info(f'[PAIRS] {symbol}: persisted {n} bars no MySQL')
    # Merge: DB + fresh, dedupe por date
    merged = {b['date']: b for b in db_data}
    for b in data: merged[b['date']] = b
    final = sorted(merged.values(), key=lambda x: x['date'])
    pairs_history_cache[symbol] = {'ts': now, 'data': final}
    return final


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

    swap_victim = None  # trade que sera fechada para dar lugar (se SWAP)
    with pairs_state_lock:
        # Nao abrir 2 trades no mesmo par
        if any(t['pair_id'] == signal['pair_id'] for t in pairs_open):
            return None

        # [25-jun-2026] WATCH tier: nao opera, so observa
        cfg = next((p for p in PAIRS_CONFIG if p['id'] == signal['pair_id']), {})
        tier = cfg.get('tier', 'A')
        if tier == 'WATCH':
            log.info(f'[PAIRS] {signal["pair_id"]} tier=WATCH — skip entry (apenas observacao)')
            return None

        # [01-jul-2026] Sizing dinamico: pos_size = capital disponivel, floor R$100k.
        # Se disponivel < min, tenta SWAP (fecha trade aberta mais fraca).
        available = _available_capital()
        pos_size = max(min(available, PAIRS_MIN_INVESTMENT), PAIRS_MIN_INVESTMENT)
        # tier B: sizing reduzido a 60% do min (respeitando piso de investimento)
        if tier == 'B':
            pos_size = max(pos_size * 0.6, PAIRS_MIN_INVESTMENT * 0.6)

        # Ceiling de posicoes (safety) + capital deployed check
        cap_full = (len(pairs_open) >= PAIRS_MAX_POSITIONS) or (available < PAIRS_MIN_INVESTMENT)
        if cap_full:
            # Tenta SWAP: se novo |z| >= worst |z| + margin, fecha worst e abre novo
            if PAIRS_SWAP_ENABLED:
                worst = _worst_open_trade()
                z_new = abs(signal.get('z_score', 0) or 0)
                z_worst = abs((worst or {}).get('current_z', (worst or {}).get('entry_z', 0)) or 0) if worst else 0
                if worst and z_new >= z_worst + PAIRS_SWAP_Z_MARGIN:
                    swap_victim = worst
                    log.info(f'[PAIRS] SWAP candidato: fechar {worst["pair_id"]} (|z|={z_worst:.2f}) '
                             f'para abrir {signal["pair_id"]} (|z|={z_new:.2f}, margin={PAIRS_SWAP_Z_MARGIN})')
                else:
                    log.info(f'[PAIRS] capital cheio ({_deployed_capital():,.0f}/{PAIRS_CAPITAL:,.0f}) '
                             f'e SWAP nao aplicavel (|z_new|={z_new:.2f} vs |z_worst|={z_worst:.2f}) '
                             f'— skip {signal["pair_id"]}')
                    return None
            else:
                log.info(f'[PAIRS] capital cheio ({_deployed_capital():,.0f}/{PAIRS_CAPITAL:,.0f}) '
                         f'— skip {signal["pair_id"]}')
                return None
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
             f'qty_a={qty_a} qty_b={qty_b} pos_size=R${pos_size:,.0f}')
    # PERSIST: nunca perder esta trade
    try: _persist.persist_trade_open(trade)
    except Exception as e: log.warning(f'[PAIRS] persist OPEN failed: {e}')
    if audit_fn:
        try: audit_fn('PAIRS_OPENED', {'id': trade_id, 'pair_id': signal['pair_id'],
                                        'z': signal['z_score'], 'direction': signal['direction'],
                                        'pos_size': pos_size,
                                        'swap_of': (swap_victim or {}).get('id')})
        except Exception: pass
    # [01-jul-2026] SWAP: fecha a trade fraca APOS abrir a nova (paper mode).
    # Usa signal atual da victima (que ja pode ser calculado independente),
    # aqui vamos usar o proprio signal atual como referencia de preco (aprox).
    if swap_victim is not None:
        try:
            # Reconstroi um signal minimo com precos correntes da victima
            # (se nao tiver, cai pra usar entry como exit -> pnl 0)
            v_pair_id = swap_victim.get('pair_id')
            v_snap = pairs_spreads.get(v_pair_id) or {}
            v_signal = {
                'z_score': v_snap.get('z_score', swap_victim.get('current_z', swap_victim.get('entry_z', 0))),
                'spread_current': v_snap.get('spread_current', swap_victim.get('entry_spread', 0)),
                'price_a': v_snap.get('price_a', swap_victim.get('price_a_entry')),
                'price_b': v_snap.get('price_b', swap_victim.get('price_b_entry')),
            }
            close_pair_trade(swap_victim, v_signal, reason='SWAPPED_OUT', audit_fn=audit_fn)
        except Exception as e:
            log.warning(f'[PAIRS] SWAP close failed pair={swap_victim.get("pair_id")}: {e}')
    return trade


PAIRS_MAX_HOLD_DAYS = 15  # [26-jun-2026] protege contra pares que perderam cointegracao


def evaluate_pair_trade_exit(trade: Dict, signal: Dict, audit_fn=None) -> Optional[str]:
    """Avalia se trade aberta deve ser fechada.

    Returns:
        'CONVERGED' / 'STOP_LOSS' / 'TIMEOUT' / None
    """
    z_now = signal.get('z_score')
    if z_now is None:
        return None
    entry_z = trade['entry_z']
    z_exit = signal['z_exit_threshold']
    z_stop = signal['z_stop_threshold']

    # [26-jun-2026] Timeout: fecha apos N dias mesmo sem convergir.
    # Half-life tipica de pares cointegrados B3 = 5-15d. Se passou disso
    # sem voltar a media, provavelmente perdeu cointegracao (regime shift
    # silencioso) — melhor sair com PnL atual do que ficar travado.
    try:
        opened_at = trade.get('opened_at')
        if opened_at:
            if isinstance(opened_at, str):
                # ISO format with optional 'Z' or '+00:00'
                ts = opened_at.replace('Z', '').split('+')[0]
                opened_dt = datetime.fromisoformat(ts)
            else:
                opened_dt = opened_at
            days_open = (datetime.utcnow() - opened_dt).days
            if days_open >= PAIRS_MAX_HOLD_DAYS:
                return 'TIMEOUT'
    except Exception as e:
        log.debug(f'[pairs] timeout check failed: {e}')

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
    # PERSIST close + pattern stats — historia desta trade preservada pra sempre
    try: _persist.persist_trade_close(trade)
    except Exception as e: log.warning(f'[PAIRS] persist CLOSE failed: {e}')
    try: _persist.update_pattern_stats(trade)
    except Exception as e: log.debug(f'[PAIRS] update pattern_stats: {e}')
    if audit_fn:
        try: audit_fn('PAIRS_CLOSED', {'id': trade['id'], 'pair_id': trade['pair_id'],
                                        'reason': reason, 'pnl': pnl, 'pnl_pct': pnl_pct})
        except Exception: pass


def pairs_scan_loop(beat_fn=None, audit_fn=None, enqueue_fn=None):
    """Loop principal — scan a cada PAIRS_SCAN_INTERVAL_S segundos.

    Persiste TODO signal (mesmo HOLD) em pairs_signals. Toda trade open/close
    em pairs_trades. Cada hora roda aggregate_hourly_stats.
    """
    log.info(f'[PAIRS] scanner iniciando | capital_paper=R${PAIRS_CAPITAL:,.0f} '
             f'max_positions={PAIRS_MAX_POSITIONS} interval={PAIRS_SCAN_INTERVAL_S}s')
    # Garante schema MySQL (idempotente)
    try:
        from . import schema as _schema
        from .persistence import _get_conn
        c = _get_conn()
        if c:
            _schema.create_pairs_tables(c)
            try: c.close()
            except: pass
    except Exception as e:
        log.warning(f'[PAIRS] schema init falhou: {e}')

    # [25-jun-2026 CRITICO] Restaurar trades OPEN do MySQL
    # Sem isso, cada deploy zerava pairs_open em memoria e o scanner
    # reabria trades novas no mesmo par, com novo entry_z e novo opened_at
    # — o usuario via os parametros mudando a cada redeploy.
    try:
        restored = _persist.load_open_trades_from_db()
        if restored:
            with pairs_state_lock:
                pairs_open.extend(restored)
            log.info(f'[PAIRS] RESTAURADAS {len(restored)} trades OPEN do MySQL: '
                     f'{[t["pair_id"] for t in restored]}')
    except Exception as e:
        log.error(f'[PAIRS] falha ao restaurar open trades: {e}')

    # Signal persist sample rate (1 = todos, 2 = a cada 2 scans, etc)
    # Default 4 = ~uma persist por minuto por par (scan 30s × 4 = 2min)
    signal_persist_every = int(os.environ.get('PAIRS_SIGNAL_PERSIST_EVERY', 4))
    scan_count = 0
    last_hourly_agg = 0.0

    while True:
        if beat_fn: beat_fn('pairs_scan_loop')
        try:
            syms = list(all_symbols())
            quotes = fetch_pair_quotes_bulk(syms)
            if not quotes:
                time.sleep(PAIRS_SCAN_INTERVAL_S)
                continue

            scan_count += 1
            persist_signals_this_scan = (scan_count % signal_persist_every == 0)

            for pcfg in PAIRS_CONFIG:
                if not pcfg.get('enabled', True):
                    continue
                if beat_fn: beat_fn('pairs_scan_loop')
                signal = calc_pair_signal(pcfg, quotes)
                if not signal:
                    continue
                pairs_spreads[pcfg['id']] = signal

                # PERSIST signal periodicamente (todo signal vai pro DB pra learning)
                # Sempre persiste se for ENTRY/CONVERGED/AVOID (eventos importantes)
                if persist_signals_this_scan or signal.get('action') in ('ENTRY','CONVERGED','AVOID'):
                    try: _persist.persist_signal(signal)
                    except Exception as e: log.debug(f'[PAIRS] persist_signal: {e}')

                # [25-jun-2026 auto-learning] Snapshot continuo (throttled 2min) + event detection
                try: _learning.snapshot_persist(signal)
                except Exception as e: log.debug(f'[learning] snapshot {e}')
                try: _learning.detect_events(signal)
                except Exception as e: log.debug(f'[learning] events {e}')

                open_trade = None
                with pairs_state_lock:
                    for t in pairs_open:
                        if t['pair_id'] == pcfg['id']:
                            open_trade = t
                            break

                if open_trade:
                    # [25-jun-2026] Atualizar PnL e z_score LIVE em trade aberta
                    # Antes: pnl ficava 0 ate a trade fechar (display mostrava +R$0)
                    try:
                        price_a_now = signal['price_a']
                        price_b_now = signal['price_b']
                        qty_a = open_trade['qty_a']
                        qty_b = open_trade['qty_b']
                        if open_trade['direction'] == 'SHORT_A':
                            pnl_a = -(price_a_now - open_trade['price_a_entry']) * qty_a
                            pnl_b =  (price_b_now - open_trade['price_b_entry']) * qty_b
                        else:  # LONG_A
                            pnl_a =  (price_a_now - open_trade['price_a_entry']) * qty_a
                            pnl_b = -(price_b_now - open_trade['price_b_entry']) * qty_b
                        pnl_now = pnl_a + pnl_b
                        pnl_pct_now = 100 * pnl_now / max(open_trade['position_size'], 1)
                        open_trade['pnl'] = round(pnl_now, 2)
                        open_trade['pnl_pct'] = round(pnl_pct_now, 4)
                        open_trade['current_z'] = signal['z_score']
                        open_trade['current_spread'] = signal.get('spread_current')
                        # Peak tracking
                        peak = float(open_trade.get('peak_pnl_pct', 0) or 0)
                        if pnl_pct_now > peak:
                            open_trade['peak_pnl_pct'] = round(pnl_pct_now, 4)
                    except Exception as e:
                        log.debug(f'[PAIRS] live update {open_trade.get("id")}: {e}')
                    exit_reason = evaluate_pair_trade_exit(open_trade, signal, audit_fn=audit_fn)
                    if exit_reason:
                        close_pair_trade(open_trade, signal, exit_reason, audit_fn=audit_fn)
                else:
                    if signal['action'] == 'ENTRY':
                        open_pair_trade(signal, audit_fn=audit_fn, enqueue_fn=enqueue_fn)

            # Hourly aggregate (1x por hora)
            now_ts = time.time()
            if now_ts - last_hourly_agg > 3600:
                try:
                    _persist.aggregate_hourly_stats()
                    last_hourly_agg = now_ts
                except Exception as e:
                    log.debug(f'[PAIRS] hourly aggregate: {e}')

        except Exception as e:
            log.error(f'[PAIRS] scan loop erro: {e}')
            import traceback; traceback.print_exc()
        time.sleep(PAIRS_SCAN_INTERVAL_S)
