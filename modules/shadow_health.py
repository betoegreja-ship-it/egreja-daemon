# -*- coding: utf-8 -*-
"""
═══════════════════════════════════════════════════════════════════════════
VIGIA DE SAUDE DOS SHADOWS  [08-ago-2026, decisao Beto — "item zero"]
═══════════════════════════════════════════════════════════════════════════
Nasceu de TRES falhas silenciosas na mesma semana, nenhuma detectada por
alarme — todas descobertas porque alguem foi olhar:

  1. executor IB mudo por 2 SEMANAS: coluna event era varchar(8) e 'ARBI_OPEN'
     tem 9 caracteres; todo INSERT falhava e o erro estava em log.debug.
  2. tres threads registradas que NUNCA subiram (pairs_scan_loop, pairs_v2_loop,
     pairs_formation_loop) — o watchdog so vigia thread que nasceu.
  3. 152 mil opinioes de advisor com a coluna de resultado NULA: o pipeline de
     resolucao nunca rodou.

Principio: nao basta vigiar o que esta vivo. E preciso vigiar o que DEVERIA
estar vivo e nao esta, e o que DEVERIA crescer e parou.

Tres verificacoes:
  A) THREADS  — esperadas (declaradas aqui) vs presentes no registro de threads
  B) TABELAS  — cada tabela de estrategia/estudo tem um SLA de frescor; se a
                ultima linha for mais velha que o SLA em horario habil, grita
  C) PIPELINES— colunas que deveriam ser preenchidas depois (resolucao de
                advisor, fechamento de trade) e estao sistematicamente nulas

NAO toca em nada: so le e reporta. Endpoint /shadow/saude.
"""
import os
import logging
from datetime import datetime, timedelta, timezone

log = logging.getLogger('egreja.shadow.health')

# ── A) threads que DEVEM existir quando o modulo correspondente esta ligado ──
# (nome_da_thread, env_que_liga, default_ligado, descricao)
THREADS_ESPERADAS = [
    ('crypto_rv_shadow_loop',     'CRYPTO_RV_SHADOW_ENABLED',     True,  'Crypto RV'),
    ('crossasset_rv_shadow_loop', 'CROSSASSET_RV_SHADOW_ENABLED', True,  'Cross-asset RV'),
    ('uspairs_shadow_loop',       'USPAIRS_SHADOW_ENABLED',       True,  'US Pairs'),
    ('inverse_shadow_loop',       'INVERSE_SHADOW_ENABLED',       True,  'Espelho invertido'),
    ('zombie_shadow_loop',        'ZOMBIE_SHADOW_ENABLED',        True,  'ZombieCut'),
    ('adranchor_loop',            'ADRANCHOR_ENABLED',            True,  'ADRANCHOR'),
    ('arbix_shadow_loop',         'ARBIX_SHADOW_ENABLED',         True,  'ARBIX'),
    ('funding_arb_loop',          'FUNDING_ARB_ENABLED',          True,  'Funding arb'),
    ('pairs_scan_loop',           'PAIRS_ENABLED',                True,  'Pares B3 (scan)'),
    ('pairs_v2_loop',             'PAIRS_V2_ENABLED',             True,  'Pares B3 v2'),
    ('pairs_formation_loop',      'PAIRS_FORMATION_ENABLED',      True,  'Pares B3 (formacao)'),
    ('stocks_intel_loop',         'STOCKS_INTEL_ENABLED',         True,  'Stocks intel'),
]

# ── B) tabelas e o SLA de frescor (horas). None = so conta, nao cobra prazo ──
# (tabela, coluna_de_tempo, sla_horas, so_em_pregao, descricao)
TABELAS_VIGIADAS = [
    ('arbi_trades',                 'created_at', 24,  True,  'Arbi — trades'),
    ('trades',                      'created_at', 6,   True,  'Direcional — trades'),
    ('crypto_rv_shadow_snapshots',  'created_at', 8,   False, 'Crypto RV — snapshots'),
    ('crypto_rv_shadow_trades',     'created_at', 240, False, 'Crypto RV — trades'),
    ('crossasset_rv_shadow_snapshots', 'created_at', 30, True, 'Cross-asset — snapshots'),
    ('uspairs_shadow_snapshots',    'created_at', 30,  True,  'US Pairs — snapshots'),
    ('inverse_shadow_trades',       'created_at', 12,  True,  'Espelho invertido'),
    ('longleg_harvest',             'created_at', 72,  True,  'Limonada (perna long)'),
    ('score_log_v4',                'created_at', 12,  True,  'Score V4'),
    ('brain_shadow_entry_advisor',  'created_at', 12,  True,  'Advisor de entrada'),
    ('brain_shadow_exit_advisor',   'created_at', 6,   True,  'Advisor de saida'),
    ('spread_audit',                'created_at', 24,  True,  'Auditoria bid/ask'),
    ('exec_orders',                 'created_at', 48,  True,  'Executor IB'),
    ('pairs_trades',                'created_at', None, True, 'Pares B3 — trades'),
    ('adranchor_trades',            'created_at', None, True, 'ADRANCHOR — trades'),
    ('arbix_shadow_trades',         'created_at', None, True, 'ARBIX — trades'),
    ('funding_arb_shadow',          'created_at', None, False, 'Funding arb — trades'),
    ('lh_trades',                   'created_at', None, True, 'Long Horizon — trades'),
    ('zombie_shadow_events',        'cut_at',     None, True, 'ZombieCut — eventos'),
]

# ── C) pipelines de resolucao: coluna que deveria deixar de ser NULA ──
# (tabela, coluna_alvo, condicao_de_elegibilidade, alerta_se_pct_nulo_acima, descricao)
PIPELINES = [
    ('brain_shadow_entry_advisor', 'actual_pnl',
     "trade_id IS NOT NULL AND created_at < DATE_SUB(NOW(), INTERVAL 2 DAY)", 90,
     'Advisor de entrada: resultado nunca vinculado'),
    ('brain_shadow_exit_advisor', 'final_pnl',
     "trade_id IS NOT NULL AND created_at < DATE_SUB(NOW(), INTERVAL 2 DAY)", 90,
     'Advisor de saida: resultado nunca vinculado'),
    ('longleg_harvest', 'dir_exit_ret_pct',
     "opened_at < DATE_SUB(NOW(), INTERVAL 3 DAY)", 50,
     'Limonada: saida direcional nao calculada'),
    ('score_log_v4', 'pnl_real',
     "created_at < DATE_SUB(NOW(), INTERVAL 2 DAY)", 30,
     'Score V4: resultado nao preenchido'),
]


def _ligado(env, default):
    v = os.environ.get(env)
    if v is None:
        return default
    return v.lower() not in ('false', '0', 'no')


def _em_pregao():
    """Heuristica simples: dia util e entre 12h e 21h UTC (cobre B3 e NYSE)."""
    ag = datetime.now(timezone.utc)
    return ag.weekday() < 5 and 12 <= ag.hour <= 21


def checar(get_db, thread_registry=None):
    """Roda as tres verificacoes. Retorna dict com achados e severidade.
    thread_registry: dict {nome: {...}} do registro vivo de threads."""
    ag = datetime.now(timezone.utc)
    pregao = _em_pregao()
    achados = []
    resumo = {'ok': 0, 'aviso': 0, 'critico': 0}

    # ── A) threads ──
    vivas = set((thread_registry or {}).keys())
    threads_rel = []
    for nome, env, dflt, desc in THREADS_ESPERADAS:
        deveria = _ligado(env, dflt)
        viva = nome in vivas
        hb = None
        if viva and isinstance(thread_registry.get(nome), dict):
            hb = thread_registry[nome].get('hb_age_s')
        if deveria and not viva:
            sev = 'critico'
            msg = f'thread NAO SUBIU (esperada e ausente do registro)'
        elif deveria and hb is not None and hb > 900:
            sev = 'aviso'
            msg = f'heartbeat velho: {hb:.0f}s'
        elif not deveria:
            sev = 'ok'
            msg = 'desligada por env (esperado)'
        else:
            sev = 'ok'
            msg = f'viva (hb {hb:.0f}s)' if hb is not None else 'viva'
        resumo[sev] += 1
        threads_rel.append({'thread': nome, 'estrategia': desc, 'deveria_rodar': deveria,
                            'viva': viva, 'hb_age_s': hb, 'severidade': sev, 'nota': msg})
        if sev != 'ok':
            achados.append(f'[{sev.upper()}] {desc}: {msg}')

    # ── B) tabelas ──
    tabelas_rel = []
    conn = None
    try:
        conn = get_db()
        cur = conn.cursor()
        for tab, col, sla, so_pregao, desc in TABELAS_VIGIADAS:
            try:
                cur.execute(f"SELECT COUNT(*), MAX(`{col}`) FROM `{tab}`")
                n, ultimo = cur.fetchone()
            except Exception as e:
                tabelas_rel.append({'tabela': tab, 'estrategia': desc, 'erro': str(e)[:80],
                                    'severidade': 'aviso'})
                resumo['aviso'] += 1
                achados.append(f'[AVISO] {desc}: tabela inacessivel ({str(e)[:50]})')
                continue
            idade_h = None
            if ultimo:
                try:
                    if isinstance(ultimo, str):
                        ultimo = datetime.fromisoformat(ultimo.replace('Z', ''))
                    if ultimo.tzinfo is None:
                        ultimo = ultimo.replace(tzinfo=timezone.utc)
                    idade_h = (ag - ultimo).total_seconds() / 3600
                except Exception:
                    pass
            sev = 'ok'
            nota = f'{n} linhas'
            if n == 0:
                sev = 'aviso'
                nota = 'VAZIA — nunca produziu'
            elif sla is not None and idade_h is not None:
                if idade_h > sla and (pregao or not so_pregao):
                    sev = 'critico'
                    nota = f'PAROU DE CRESCER: ultima linha ha {idade_h:.1f}h (SLA {sla}h)'
                elif idade_h > sla * 0.7 and (pregao or not so_pregao):
                    sev = 'aviso'
                    nota = f'ficando velha: {idade_h:.1f}h (SLA {sla}h)'
                else:
                    nota = f'{n} linhas, ultima ha {idade_h:.1f}h'
            resumo[sev] += 1
            tabelas_rel.append({'tabela': tab, 'estrategia': desc, 'linhas': n,
                                'idade_ultima_h': round(idade_h, 2) if idade_h is not None else None,
                                'sla_h': sla, 'severidade': sev, 'nota': nota})
            if sev != 'ok':
                achados.append(f'[{sev.upper()}] {desc}: {nota}')

        # ── C) pipelines ──
        pipes_rel = []
        for tab, col, cond, limite_pct, desc in PIPELINES:
            try:
                cur.execute(f"SELECT COUNT(*), SUM(`{col}` IS NULL) FROM `{tab}` WHERE {cond}")
                tot, nulos = cur.fetchone()
            except Exception as e:
                pipes_rel.append({'pipeline': desc, 'erro': str(e)[:80], 'severidade': 'aviso'})
                resumo['aviso'] += 1
                continue
            if not tot:
                pipes_rel.append({'pipeline': desc, 'elegiveis': 0, 'severidade': 'ok',
                                  'nota': 'nada elegivel ainda'})
                resumo['ok'] += 1
                continue
            pct = 100.0 * float(nulos or 0) / float(tot)
            sev = 'critico' if pct >= limite_pct else ('aviso' if pct >= limite_pct * 0.6 else 'ok')
            nota = (f'{pct:.0f}% sem resultado ({nulos} de {tot} elegiveis)')
            resumo[sev] += 1
            pipes_rel.append({'pipeline': desc, 'tabela': tab, 'coluna': col,
                              'elegiveis': tot, 'sem_resultado': nulos,
                              'pct_nulo': round(pct, 1), 'limite_pct': limite_pct,
                              'severidade': sev, 'nota': nota})
            if sev != 'ok':
                achados.append(f'[{sev.upper()}] {desc}: {nota}')
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass

    estado = ('CRITICO' if resumo['critico'] else
              ('ATENCAO' if resumo['aviso'] else 'SAUDAVEL'))
    return {
        'estado_geral': estado,
        'verificado_em': ag.isoformat(timespec='seconds'),
        'em_pregao': pregao,
        'resumo': resumo,
        'achados': achados,
        'threads': threads_rel,
        'tabelas': tabelas_rel,
        'pipelines': pipes_rel,
        'nota': ('Vigia passivo: so le e reporta. Criado apos 3 falhas silenciosas '
                 'na semana de 04-08/ago (executor mudo por schema, 3 threads que '
                 'nunca subiram, 152 mil advisors nao resolvidos).'),
    }
