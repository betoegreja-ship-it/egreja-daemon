# -*- coding: utf-8 -*-
"""[08-ago-2026, decisao Beto] SIZING POR ATR — contrafactual em SOMBRA.

Restricao explicita do fundador, e a razao de este modulo existir assim:

  "se implementar o sizing atr conforme sua sugestao nao vai atrapalhar a b3
   inverse? deixe a b3 exatamente como esta sem mudanca alguma de entrada e
   saida, em sombra para a inverse dar certo! o mesmo com short de nyse."

O experimento do espelho invertido (INV-/INVG-) so funciona se o book real
continuar identico. Mudar o tamanho das posicoes mudaria o P&L do espelho e
destruiria a comparacao. Entao o sizing por ATR NAO altera nenhuma ordem:
ele apenas RECALCULA, depois que a trade fecha, quanto teria dado se o
tamanho tivesse sido definido pela volatilidade em vez de por valor fixo.

A CONTA
  Hoje: toda posicao tem o mesmo valor financeiro. Uma acao que oscila 6% ao
  dia e uma que oscila 1% carregam o MESMO dinheiro — logo, riscos seis vezes
  diferentes. O risco por trade e acidental.

  Sizing por ATR: cada posicao carrega o mesmo RISCO, nao o mesmo valor.
      notional_atr = notional_base x (ATR_alvo / ATR_do_ativo)
  limitado entre ATR_SIZING_MIN_MULT e ATR_SIZING_MAX_MULT para nao criar
  posicao gigante num ativo parado nem posicao insignificante num agitado.

  P&L contrafactual = pnl_pct x notional_atr

O que se aprende: se a soma do contrafactual bater a soma real, o tamanho fixo
esta jogando dinheiro fora — e ai vale a pena discutir promover. Se nao bater,
morreu a ideia e nao custou uma ordem.

Envs:
  ATR_SIZING_SHADOW_ENABLED (true)  ATR_SIZING_TARGET_PCT (2.0)
  ATR_SIZING_MIN_MULT (0.4)         ATR_SIZING_MAX_MULT (2.0)
"""
import os
import logging

log = logging.getLogger('egreja.atr.sizing.shadow')

_ready = {'v': False}


def _f(name, default):
    try:
        return float(os.environ.get(name, default))
    except Exception:
        return float(default)


def enabled():
    return os.environ.get('ATR_SIZING_SHADOW_ENABLED', 'true').lower() == 'true'


def create_tables(get_db):
    if _ready['v']:
        return True
    conn = None
    try:
        conn = get_db()
        if not conn:
            return False
        c = conn.cursor()
        c.execute("""CREATE TABLE IF NOT EXISTS atr_sizing_shadow (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            trade_id VARCHAR(64) NOT NULL,
            symbol VARCHAR(24), market VARCHAR(16), strategy VARCHAR(32),
            direction VARCHAR(8), close_reason VARCHAR(48),
            atr_pct DOUBLE, atr_alvo_pct DOUBLE, mult DOUBLE,
            notional_real DOUBLE, notional_atr DOUBLE,
            pnl_pct DOUBLE, pnl_real DOUBLE, pnl_atr DOUBLE, delta DOUBLE,
            opened_at DATETIME NULL, closed_at DATETIME NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uq_trade (trade_id),
            INDEX ix_sym (symbol), INDEX ix_closed (closed_at)
        ) CHARACTER SET utf8mb4""")
        conn.commit(); c.close()
        _ready['v'] = True
        return True
    except Exception as e:
        log.error(f'[ATR-SIZING] schema: {e}')
        return False
    finally:
        try:
            if conn: conn.close()
        except Exception:
            pass


def on_close(get_db, trade, pnl_pct, close_reason=None):
    """Registra o contrafactual de uma trade que acabou de fechar.

    NAO envia ordem, NAO altera a trade, NAO toca capital. So escreve numa
    tabela propria. Fail-open em qualquer erro: shadow nunca derruba producao.
    """
    if not enabled():
        return
    try:
        if not create_tables(get_db):
            return
        atr = float(trade.get('_atr_pct') or trade.get('atr_pct') or 0)
        if atr <= 0:
            return  # sem ATR nao ha contrafactual honesto — melhor nao inventar
        notional = float(trade.get('position_value')
                         or trade.get('reserved_capital_at_entry') or 0)
        if notional <= 0:
            return
        ppct = float(pnl_pct if pnl_pct is not None else (trade.get('pnl_pct') or 0))
        alvo = _f('ATR_SIZING_TARGET_PCT', 2.0)
        mult = max(_f('ATR_SIZING_MIN_MULT', 0.4),
                   min(_f('ATR_SIZING_MAX_MULT', 2.0), alvo / atr))
        n_atr = notional * mult
        pnl_real = notional * ppct / 100.0
        pnl_atr = n_atr * ppct / 100.0
        conn = get_db()
        if not conn:
            return
        c = conn.cursor()
        c.execute("""INSERT IGNORE INTO atr_sizing_shadow
            (trade_id,symbol,market,strategy,direction,close_reason,
             atr_pct,atr_alvo_pct,mult,notional_real,notional_atr,
             pnl_pct,pnl_real,pnl_atr,delta,opened_at,closed_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (trade.get('id'), trade.get('symbol'), trade.get('market'),
             trade.get('strategy'), trade.get('direction'), close_reason,
             round(atr, 4), alvo, round(mult, 4), round(notional, 2),
             round(n_atr, 2), round(ppct, 4), round(pnl_real, 2),
             round(pnl_atr, 2), round(pnl_atr - pnl_real, 2),
             trade.get('opened_at'), trade.get('closed_at')))
        conn.commit(); c.close(); conn.close()
    except Exception as e:
        log.error(f'[ATR-SIZING] on_close {trade.get("id")}: {e}')


def backfill(get_db, limite=2000):
    """Preenche o contrafactual das trades JA fechadas, sem esperar semanas.

    O ATR de entrada existe em score_log_v4.atr_pct_entry desde 24/jul; o valor
    da posicao e o retorno estao em `trades`. Da para reconstruir 1.188 trades
    hoje. Roda uma vez no boot; o UNIQUE em trade_id torna a operacao idempotente.
    """
    if not enabled():
        return 0
    conn = None
    try:
        if not create_tables(get_db):
            return 0
        alvo = _f('ATR_SIZING_TARGET_PCT', 2.0)
        lo, hi = _f('ATR_SIZING_MIN_MULT', 0.4), _f('ATR_SIZING_MAX_MULT', 2.0)
        conn = get_db()
        if not conn:
            return 0
        c = conn.cursor()
        c.execute(f"""
            INSERT IGNORE INTO atr_sizing_shadow
              (trade_id,symbol,market,strategy,direction,close_reason,
               atr_pct,atr_alvo_pct,mult,notional_real,notional_atr,
               pnl_pct,pnl_real,pnl_atr,delta,opened_at,closed_at)
            SELECT t.id, t.symbol, t.market, t.strategy, t.direction, t.close_reason,
                   s.atr_pct_entry, {alvo},
                   GREATEST({lo}, LEAST({hi}, {alvo} / s.atr_pct_entry)) AS m,
                   t.position_value,
                   t.position_value * GREATEST({lo}, LEAST({hi}, {alvo} / s.atr_pct_entry)),
                   t.pnl_pct,
                   t.position_value * t.pnl_pct / 100,
                   t.position_value * GREATEST({lo}, LEAST({hi}, {alvo} / s.atr_pct_entry))
                     * t.pnl_pct / 100,
                   t.position_value * t.pnl_pct / 100
                     * (GREATEST({lo}, LEAST({hi}, {alvo} / s.atr_pct_entry)) - 1),
                   t.opened_at, t.closed_at
            FROM score_log_v4 s
            JOIN trades t ON s.trade_id = t.id
            WHERE s.atr_pct_entry > 0 AND t.status = 'CLOSED'
              AND t.position_value > 0 AND t.pnl_pct IS NOT NULL
            LIMIT %s""", (limite,))
        n = c.rowcount or 0
        conn.commit(); c.close()
        if n:
            log.info(f'[ATR-SIZING] backfill: {n} trades historicas reconstruidas')
        return n
    except Exception as e:
        log.error(f'[ATR-SIZING] backfill: {e}')
        return 0
    finally:
        try:
            if conn: conn.close()
        except Exception:
            pass


def summary(get_db, dias=30):
    """Veredito do contrafactual. Compara os DOIS tamanhos sobre as MESMAS trades."""
    conn = None
    try:
        if not create_tables(get_db):
            return {'erro': 'schema'}
        conn = get_db()
        c = conn.cursor(dictionary=True)
        base = ("FROM atr_sizing_shadow WHERE closed_at > NOW() - INTERVAL %s DAY")
        c.execute(f"""SELECT COUNT(*) n, SUM(pnl_real) real_total, SUM(pnl_atr) atr_total,
                             SUM(delta) delta_total, AVG(mult) mult_medio,
                             SUM(pnl_real>0) wins, AVG(atr_pct) atr_medio,
                             SUM(notional_real) not_real, SUM(notional_atr) not_atr
                      {base}""", (dias,))
        g = c.fetchone() or {}
        c.execute(f"""SELECT market, COUNT(*) n, SUM(pnl_real) real_total,
                             SUM(pnl_atr) atr_total, SUM(delta) delta_total
                      {base} GROUP BY market ORDER BY delta_total DESC""", (dias,))
        por_mercado = c.fetchall()
        c.execute(f"""SELECT symbol, COUNT(*) n, AVG(atr_pct) atr, AVG(mult) mult,
                             SUM(delta) delta_total
                      {base} GROUP BY symbol HAVING n >= 3
                      ORDER BY delta_total DESC LIMIT 15""", (dias,))
        melhores = c.fetchall()
        c.close()
        n = int(g.get('n') or 0)
        real = float(g.get('real_total') or 0)
        atrv = float(g.get('atr_total') or 0)
        return {
            'dias': dias, 'n_trades': n,
            'pnl_tamanho_fixo': round(real, 2),
            'pnl_tamanho_por_atr': round(atrv, 2),
            'diferenca': round(atrv - real, 2),
            'diferenca_pct': round((atrv / real - 1) * 100, 2) if real else None,
            'capital_usado_fixo': round(float(g.get('not_real') or 0), 2),
            'capital_usado_atr': round(float(g.get('not_atr') or 0), 2),
            'mult_medio': round(float(g.get('mult_medio') or 0), 3),
            'atr_medio_pct': round(float(g.get('atr_medio') or 0), 3),
            'wr_pct': round(100 * float(g.get('wins') or 0) / n, 1) if n else None,
            'por_mercado': por_mercado, 'maiores_ganhos_do_atr': melhores,
            'nota': ('Contrafactual puro: nenhuma ordem foi alterada. As trades sao as '
                     'mesmas; muda so o tamanho. Comparar tambem o CAPITAL usado — se o '
                     'ATR ganhou mais usando mais capital, nao ganhou nada.'),
        }
    except Exception as e:
        log.error(f'[ATR-SIZING] summary: {e}')
        return {'erro': str(e)}
    finally:
        try:
            if conn: conn.close()
        except Exception:
            pass
