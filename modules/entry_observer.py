"""
═══════════════════════════════════════════════════════════════════════════
ENTRY OBSERVER — instrumentacao por trade  [30-jul-2026, consenso 3 consultores]
═══════════════════════════════════════════════════════════════════════════
Grava, para CADA trade de acoes aberta (B3/NYSE), o snapshot completo do
contexto de decisao que ate hoje era proxy:

  1. PULSE LOG (pedido unanime): estado do Market Pulse, breadth, indice,
     fonte, alinhamento do lado — acaba a era da "acuracia por proxy".
  2. GATE B3 (shadow): regime estrutural IBOV + breadth + "teria bloqueado".
  3. FLAGS P1 por regra (resolve a ressalva de atribuicao do GPT): cada
     regra proposta vira uma COLUNA independente — curfew 15h, short
     restrito, MIXED, MANTER->SHORT NYSE. Efeito individual E incremental
     mensuravel por SQL, sem 13 books paralelos.
  4. BREAKEVEN AUDIT: no disparo do BREAKEVEN_PROTECT, grava pico, pnl no
     gatilho, piso usado e preco — a anatomia do vazamento de −R$22,7k/−US$9,2k.

NADA aqui muda decisao. Insercao fail-open. Configuracao versionada.
"""
import os, json, logging
from datetime import datetime, timezone

log = logging.getLogger('egreja.entryobs')

PARAMS_VERSION = 'obs_2026-07-30_v1'
_tables_ok = {'v': False}


def _conn():
    try:
        from modules.pairs_engine.persistence import _get_conn
        return _get_conn()
    except Exception as e:
        log.debug(f'[ENTRY-OBS] conn: {e}')
        return None


def _ensure(cur):
    if _tables_ok['v']:
        return
    cur.execute("""CREATE TABLE IF NOT EXISTS entry_observer_log (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        trade_id VARCHAR(64) UNIQUE, symbol VARCHAR(16), market VARCHAR(8),
        direction VARCHAR(6), signal_orig VARCHAR(10), score INT,
        regime_v2 VARCHAR(12),
        pulse_state VARCHAR(10), pulse_detail VARCHAR(160),
        breadth_up DECIMAL(6,2), breadth_dn DECIMAL(6,2),
        index_chg_pct DECIMAL(8,3), aligned TINYINT NULL,
        conversion_reason VARCHAR(30),
        gate_estrutural VARCHAR(12), gate_final VARCHAR(12),
        gate_ibov_close DECIMAL(12,0), gate_ibov_ema50 DECIMAL(12,0),
        gate_ibov_ret20d DECIMAL(8,2), gate_would_block TINYINT NULL,
        gate_block_reason VARCHAR(120),
        flag_curfew TINYINT DEFAULT 0, flag_mixed TINYINT DEFAULT 0,
        flag_short_restrito TINYINT DEFAULT 0,
        flag_neutral_short_nyse TINYINT DEFAULT 0,
        params_version VARCHAR(30), ts DATETIME,
        INDEX ix_mkt (market), INDEX ix_ts (ts))""")
    cur.execute("""CREATE TABLE IF NOT EXISTS breakeven_audit (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        trade_id VARCHAR(64), symbol VARCHAR(16), market VARCHAR(10),
        asset_type VARCHAR(10),
        peak_pnl_pct DECIMAL(9,4), pnl_at_trigger DECIMAL(9,4),
        floor_used DECIMAL(8,3), trigger_cfg DECIMAL(8,3),
        step_active TINYINT, price_at_trigger DECIMAL(20,8),
        devolvido_pp DECIMAL(9,4), ts DATETIME,
        final_pnl DECIMAL(14,2) NULL, final_pnl_pct DECIMAL(9,4) NULL,
        INDEX ix_trade (trade_id), INDEX ix_ts (ts))""")
    _tables_ok['v'] = True


def log_entry(trade_id, symbol, market, direction, signal_orig, score,
              regime_v2, pulse=None, gate=None, opened_at_utc=None):
    """Snapshot completo na abertura. Fail-open total."""
    try:
        c = _conn()
        if not c:
            return
        cur = c.cursor()
        _ensure(cur)
        mkt = (market or '').upper()
        d = (direction or '').upper()
        sig_o = (signal_orig or '').upper()
        reg = (regime_v2 or '').upper()
        pu = pulse or {}
        p_state = (pu.get('state') or 'UNKNOWN').upper()
        # alinhamento do lado com a mare (NULL se pulse neutro/desconhecido)
        aligned = None
        if p_state == 'RISK_ON':
            aligned = 1 if d == 'LONG' else 0
        elif p_state == 'RISK_OFF':
            aligned = 1 if d == 'SHORT' else 0
        # motivo de conversao (derivado sinal original vs direcao final)
        if sig_o == 'MANTER' and d in ('LONG', 'SHORT'):
            conv = f'TAILWIND_{d}'
        elif (sig_o == 'COMPRA' and d == 'SHORT') or (sig_o == 'VENDA' and d == 'LONG'):
            conv = 'FLIP'
        else:
            conv = 'DIRECT'
        now = opened_at_utc or datetime.now(timezone.utc).replace(tzinfo=None)
        # flags P1 (avaliadas SEMPRE, aplicadas NUNCA)
        # curfew ancorado no dado VALIDADO (Kimi/banco): entradas >=18h UTC
        # (15h BRT) = 115 trades, -R$10.290, WR 27%. Env B3_CURFEW_UTC ajusta.
        h, m = now.hour, now.minute
        _cf = os.environ.get('B3_CURFEW_UTC', '18:00')
        try:
            _cfh, _cfm = int(_cf.split(':')[0]), int(_cf.split(':')[1])
        except Exception:
            _cfh, _cfm = 18, 0
        flag_curfew = 1 if (mkt == 'B3' and (h > _cfh or (h == _cfh and m >= _cfm))) else 0
        flag_mixed = 1 if (mkt == 'B3' and reg == 'MIXED') else 0
        flag_short_r = 0
        if mkt == 'B3' and d == 'SHORT':
            ok = (reg == 'RANGING' and sig_o == 'VENDA' and aligned == 1)
            flag_short_r = 0 if ok else 1
        flag_nns = 1 if (mkt == 'NYSE' and sig_o == 'MANTER' and d == 'SHORT') else 0
        g = gate or {}
        wb, wb_reason = (None, '')
        if g:
            try:
                from modules.b3_regime_gate import would_block
                wb_b, wb_reason = would_block(g, d, score, sig_o)
                wb = 1 if wb_b else 0
            except Exception:
                pass
        cur.execute("""INSERT IGNORE INTO entry_observer_log
            (trade_id,symbol,market,direction,signal_orig,score,regime_v2,
             pulse_state,pulse_detail,breadth_up,breadth_dn,index_chg_pct,
             aligned,conversion_reason,gate_estrutural,gate_final,
             gate_ibov_close,gate_ibov_ema50,gate_ibov_ret20d,
             gate_would_block,gate_block_reason,
             flag_curfew,flag_mixed,flag_short_restrito,flag_neutral_short_nyse,
             params_version,ts)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,%s)""",
            (str(trade_id), symbol, mkt, d, sig_o, int(score or 0), reg or None,
             p_state, str(pu.get('detail') or '')[:158],
             pu.get('breadth_up_pct'), pu.get('breadth_dn_pct'),
             pu.get('index_chg_pct') if pu.get('index_chg_pct') is not None
             else pu.get('avg_change_pct'),
             aligned, conv,
             g.get('estrutural'), g.get('final'), g.get('ibov_close'),
             g.get('ibov_ema50'), g.get('ibov_ret20d'), wb, wb_reason[:118],
             flag_curfew, flag_mixed, flag_short_r, flag_nns,
             PARAMS_VERSION, now))
        c.commit()
        cur.close()
        c.close()
    except Exception as e:
        log.debug(f'[ENTRY-OBS] log_entry: {e}')


def log_breakeven(trade_id, symbol, market, asset_type, peak, pnl_at_trigger,
                  floor_used, trigger_cfg, step_active, price):
    """Anatomia do BREAKEVEN_PROTECT no momento do disparo (auditoria GPT)."""
    try:
        c = _conn()
        if not c:
            return
        cur = c.cursor()
        _ensure(cur)
        cur.execute("""INSERT INTO breakeven_audit
            (trade_id,symbol,market,asset_type,peak_pnl_pct,pnl_at_trigger,
             floor_used,trigger_cfg,step_active,price_at_trigger,devolvido_pp,ts)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())""",
            (str(trade_id), symbol, market, asset_type,
             round(float(peak or 0), 4), round(float(pnl_at_trigger or 0), 4),
             float(floor_used), float(trigger_cfg), 1 if step_active else 0,
             price, round(float(peak or 0) - float(pnl_at_trigger or 0), 4)))
        c.commit()
        cur.close()
        c.close()
    except Exception as e:
        log.debug(f'[ENTRY-OBS] log_breakeven: {e}')


def summary():
    """Resumo para /debug/entry-observer: contagens por flag + gate + breakeven."""
    out = {'params_version': PARAMS_VERSION}
    c = _conn()
    if not c:
        return out
    try:
        cur = c.cursor()
        _ensure(cur)
        cur.execute("""SELECT market, COUNT(*),
            SUM(aligned=1), SUM(aligned=0),
            SUM(flag_curfew), SUM(flag_mixed), SUM(flag_short_restrito),
            SUM(flag_neutral_short_nyse), SUM(gate_would_block=1)
            FROM entry_observer_log GROUP BY market""")
        out['por_mercado'] = [
            {'market': r[0], 'trades': int(r[1] or 0),
             'alinhadas': int(r[2] or 0), 'desalinhadas': int(r[3] or 0),
             'flag_curfew': int(r[4] or 0), 'flag_mixed': int(r[5] or 0),
             'flag_short_restrito': int(r[6] or 0),
             'flag_neutral_short_nyse': int(r[7] or 0),
             'gate_teria_bloqueado': int(r[8] or 0)} for r in cur.fetchall()]
        # contrafactual das flags: P&L real das trades que cada regra teria evitado
        out['contrafactual_flags'] = {}
        for col in ('flag_curfew', 'flag_mixed', 'flag_short_restrito',
                    'flag_neutral_short_nyse', 'gate_would_block'):
            cur.execute(f"""SELECT e.market, COUNT(*), COALESCE(SUM(t.pnl),0)
                FROM entry_observer_log e JOIN trades t ON t.id=e.trade_id
                WHERE e.{col}=1 AND t.status='CLOSED' GROUP BY e.market""")
            out['contrafactual_flags'][col] = [
                {'market': r[0], 'n': int(r[1]), 'pnl_evitado': round(float(r[2]), 2)}
                for r in cur.fetchall()]
        cur.execute("""SELECT market, COUNT(*), ROUND(AVG(peak_pnl_pct),3),
            ROUND(AVG(pnl_at_trigger),3), ROUND(AVG(devolvido_pp),3)
            FROM breakeven_audit GROUP BY market""")
        out['breakeven_audit'] = [
            {'market': r[0], 'disparos': int(r[1]), 'pico_medio': float(r[2] or 0),
             'pnl_no_gatilho': float(r[3] or 0), 'devolvido_pp_medio': float(r[4] or 0)}
            for r in cur.fetchall()]
        cur.close()
        c.commit()
        c.close()
    except Exception as e:
        log.debug(f'[ENTRY-OBS] summary: {e}')
        try:
            c.close()
        except Exception:
            pass
    return out
