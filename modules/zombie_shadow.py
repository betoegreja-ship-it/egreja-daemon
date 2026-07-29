"""
═══════════════════════════════════════════════════════════════════════════
ZOMBIE_CUT SHADOW — NYSE  [29-jul-2026, consenso GPT+Grok+Kimi]
═══════════════════════════════════════════════════════════════════════════
Observador PASSIVO: nao fecha nada, nao envia ordem. Para cada trade NYSE
aberta, avalia em tempo real as regras de corte e REGISTRA o "teria cortado",
com o P&L no momento do corte. Quando a trade fecha de verdade, reconcilia:
  delta = pnl_no_corte - pnl_final   (>0 = o corte teria poupado dinheiro)

Regras avaliadas (v1 congelada, recalibra so com dados novos):
  ZC3    : vida >= 3h  E pnl <= -0.5%  E pico historico < +0.5%
  ZC4    : vida >= 4h  E pnl <= 0%     E pico historico < +0.5%
  MIDDAY : (estudo Kimi) aberta antes de 11h ET, sem pico >= +0.5% ate 13h30 ET

Criterios de promocao CONGELADOS (Kimi, definidos ANTES do teste):
  - ressurreicao <= 1 a cada 10 cortes (final voltou a > +1%)
  - economia projetada >= 60% do backtest (>= +US$6k/45d corridos)
  - kill-switch: ressurreicao > 20% em qualquer semana -> suspende e recalibra
Validacao minima: 10-15 pregoes (Grok) / >=30 acionamentos (GPT).

Extra (pedido GPT): snapshot de EQUITY INTRADIARIA do book NYSE a cada 15min
(realizado do dia + nao-realizado das abertas) -> curva peak-to-close honesta.
"""
import os, time, logging
from datetime import datetime, timezone

log = logging.getLogger('egreja.zombie.shadow')

ZS_ENABLED = os.environ.get('ZOMBIE_SHADOW_ENABLED', 'true').lower() != 'false'
PEAK_GUARD = float(os.environ.get('ZOMBIE_PEAK_GUARD', 0.5))
ZC3_H, ZC3_PNL = 3.0, -0.5
ZC4_H, ZC4_PNL = 4.0, 0.0
LOOP_S = int(os.environ.get('ZOMBIE_SHADOW_LOOP_S', 240))
EQUITY_EVERY_S = 900

_peaks = {}        # trade_id -> max pnl_pct visto por este observador
_flagged = {}      # trade_id -> set(rules ja registradas)
_last_equity = 0


def _conn():
    try:
        from modules.pairs_engine.persistence import _get_conn
        return _get_conn()
    except Exception as e:
        log.debug(f'[ZOMBIE] conn: {e}')
        return None


def _ensure_tables(cur):
    cur.execute("""CREATE TABLE IF NOT EXISTS zombie_shadow_events (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        trade_id VARCHAR(64), symbol VARCHAR(12), rule VARCHAR(8),
        cut_at DATETIME, age_h DECIMAL(8,2), pnl_pct_cut DECIMAL(10,4),
        pnl_usd_cut DECIMAL(14,2), peak_at_cut DECIMAL(10,4),
        status VARCHAR(10) DEFAULT 'PENDING',
        final_pnl DECIMAL(14,2), final_pnl_pct DECIMAL(10,4),
        delta_usd DECIMAL(14,2), resurrected TINYINT,
        resolved_at DATETIME, UNIQUE KEY uq_trade_rule (trade_id, rule))""")
    cur.execute("""CREATE TABLE IF NOT EXISTS nyse_equity_curve (
        id BIGINT AUTO_INCREMENT PRIMARY KEY, ts DATETIME,
        realized_today_usd DECIMAL(14,2), unrealized_usd DECIMAL(14,2),
        equity_usd DECIMAL(14,2), n_open INT, INDEX ix_ts (ts))""")


def _px(sym):
    try:
        import api_server
        v = api_server.stock_prices.get(str(sym).upper())
        if isinstance(v, dict):
            v = v.get('price') or v.get('last') or v.get('close')
        v = float(v)
        return v if v > 0 else None
    except Exception:
        return None


def _et_hour(now_utc):
    return (now_utc.hour - 4) % 24 + now_utc.minute / 60.0


def _cycle():
    now = datetime.now(timezone.utc)
    if now.weekday() >= 5: return
    h_et = _et_hour(now)
    if not (9.5 <= h_et <= 16.2): return
    c = _conn()
    if not c: return
    try:
        cur = c.cursor(); _ensure_tables(cur)
        cur.execute("""SELECT id, symbol, direction, entry_price, position_value,
                       opened_at, COALESCE(peak_pnl_pct,0)
                       FROM trades WHERE market='NYSE' AND status='OPEN'""")
        rows = cur.fetchall()
        unreal_tot = 0.0
        for tid, sym, direction, entry, posval, oat, db_peak in rows:
            entry = float(entry or 0); posval = float(posval or 0)
            if entry <= 0: continue
            px = _px(sym)
            if not px: continue
            sign = 1 if str(direction).upper() in ('LONG', 'BUY') else -1
            pnl_pct = sign * (px / entry - 1) * 100
            pnl_usd = pnl_pct / 100 * posval
            unreal_tot += pnl_usd
            peak = max(_peaks.get(tid, float(db_peak or 0)), pnl_pct)
            _peaks[tid] = peak
            age_h = (now.replace(tzinfo=None) - oat).total_seconds() / 3600 if oat else 0
            flags = _flagged.setdefault(tid, set())

            def _hit(rule):
                if rule in flags: return
                flags.add(rule)
                try:
                    cur.execute("""INSERT IGNORE INTO zombie_shadow_events
                        (trade_id, symbol, rule, cut_at, age_h, pnl_pct_cut,
                         pnl_usd_cut, peak_at_cut, status)
                        VALUES (%s,%s,%s,NOW(),%s,%s,%s,%s,'PENDING')""",
                        (str(tid), sym, rule, round(age_h, 2), round(pnl_pct, 4),
                         round(pnl_usd, 2), round(peak, 4)))
                    log.info(f'[ZOMBIE-SHADOW] {rule} teria cortado {sym} '
                             f'(vida {age_h:.1f}h, pnl {pnl_pct:+.2f}%, pico {peak:+.2f}%)')
                except Exception as e:
                    log.debug(f'[ZOMBIE] insert: {e}')

            if peak < PEAK_GUARD:
                if age_h >= ZC3_H and pnl_pct <= ZC3_PNL: _hit('ZC3')
                if age_h >= ZC4_H and pnl_pct <= ZC4_PNL: _hit('ZC4')
                # MIDDAY (Kimi): aberta antes de 11h ET, sem +0.5% ate 13h30 ET
                if oat is not None and h_et >= 13.5:
                    o_et = _et_hour(oat.replace(tzinfo=timezone.utc))
                    if o_et < 11.0: _hit('MIDDAY')

        # reconciliacao: eventos PENDING de trades ja fechadas
        cur.execute("""SELECT e.id, e.trade_id, e.pnl_usd_cut FROM zombie_shadow_events e
                       JOIN trades t ON t.id = e.trade_id
                       WHERE e.status='PENDING' AND t.status='CLOSED' LIMIT 200""")
        pend = cur.fetchall()
        for eid, tid, pnl_cut in pend:
            cur.execute("SELECT pnl, pnl_pct FROM trades WHERE id=%s", (tid,))
            r = cur.fetchone()
            if not r: continue
            fpnl, fpct = float(r[0] or 0), float(r[1] or 0)
            delta = float(pnl_cut or 0) - fpnl
            cur.execute("""UPDATE zombie_shadow_events SET status='RESOLVED',
                final_pnl=%s, final_pnl_pct=%s, delta_usd=%s, resurrected=%s,
                resolved_at=NOW() WHERE id=%s""",
                (fpnl, fpct, round(delta, 2), 1 if fpct > 1.0 else 0, eid))
            _peaks.pop(tid, None); _flagged.pop(tid, None)

        # equity intradiaria (pedido GPT) a cada 15min
        global _last_equity
        if time.time() - _last_equity >= EQUITY_EVERY_S:
            _last_equity = time.time()
            cur.execute("""SELECT COALESCE(SUM(pnl),0) FROM trades
                           WHERE market='NYSE' AND status='CLOSED' AND DATE(closed_at)=CURDATE()""")
            realized = float(cur.fetchone()[0] or 0)
            cur.execute("""INSERT INTO nyse_equity_curve
                (ts, realized_today_usd, unrealized_usd, equity_usd, n_open)
                VALUES (NOW(),%s,%s,%s,%s)""",
                (round(realized, 2), round(unreal_tot, 2),
                 round(realized + unreal_tot, 2), len(rows)))
        c.commit(); cur.close(); c.close()
    except Exception as e:
        log.debug(f'[ZOMBIE] cycle: {e}')
        try: c.close()
        except Exception: pass


def zombie_shadow_loop(beat_fn=None):
    if not ZS_ENABLED:
        log.info('[ZOMBIE-SHADOW] desabilitado via env'); return
    log.info('[ZOMBIE-SHADOW] observador iniciado (ZC3/ZC4/MIDDAY, passivo, so loga)')
    time.sleep(150)
    while True:
        try:
            if beat_fn:
                try: beat_fn('zombie_shadow_loop')
                except Exception: pass
            _cycle()
        except Exception as e:
            log.error(f'[ZOMBIE-SHADOW] loop: {e}')
        time.sleep(LOOP_S)


def summary():
    out = {'enabled': ZS_ENABLED,
           'criterios_congelados': {'ressurreicao_max': '10% (1 em 10)',
                                    'economia_min': '>=60% do backtest (US$6k/45d)',
                                    'kill_switch': 'ressurreicao >20% em qualquer semana'},
           'rules': {}}
    c = _conn()
    if not c: return out
    try:
        cur = c.cursor(); _ensure_tables(cur)
        cur.execute("""SELECT rule, COUNT(*),
            SUM(status='RESOLVED'), SUM(CASE WHEN status='RESOLVED' THEN delta_usd ELSE 0 END),
            SUM(CASE WHEN status='RESOLVED' AND resurrected=1 THEN 1 ELSE 0 END)
            FROM zombie_shadow_events GROUP BY rule""")
        for rule, n, res, delta, resu in cur.fetchall():
            res = int(res or 0)
            out['rules'][rule] = {
                'acionamentos': int(n or 0), 'resolvidos': res,
                'economia_usd': round(float(delta or 0), 2),
                'ressurreicoes': int(resu or 0),
                'taxa_ressurreicao_pct': round(100 * int(resu or 0) / res, 1) if res else None}
        cur.execute("""SELECT ts, realized_today_usd, unrealized_usd, equity_usd, n_open
                       FROM nyse_equity_curve WHERE DATE(ts)=CURDATE() ORDER BY ts""")
        out['equity_hoje'] = [{'ts': str(r[0]), 'realizado': float(r[1]), 'aberto': float(r[2]),
                               'equity': float(r[3]), 'n_open': int(r[4])} for r in cur.fetchall()]
        cur.close(); c.commit(); c.close()
    except Exception as e:
        log.debug(f'[ZOMBIE] summary: {e}')
        try: c.close()
        except Exception: pass
    return out
