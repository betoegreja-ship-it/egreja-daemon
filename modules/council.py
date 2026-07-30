"""
═══════════════════════════════════════════════════════════════════════════
COUNCIL — painel de consultores automatizado  [30-jul-2026, decisao Beto]
═══════════════════════════════════════════════════════════════════════════
Uma pergunta -> dispara em PARALELO para os 4 consultores (GPT, Grok, Gemini,
Kimi) via OpenRouter (UMA chave, uma fatura) -> coleta as 4 respostas -> grava
no banco (registro auditavel) -> devolve tudo junto. Fim do copia-e-cola.

FERRAMENTA, nao estrategia: nao toca em nenhuma decisao de trade. Dormente ate
COUNCIL_ENABLED=true E OPENROUTER_API_KEY setada.

CONTROLE DE CUSTO (preocupacao do Beto — API e por token, separado da assinatura):
  - persona enxuta (~1k tokens), NAO manda dossies inteiros a cada chamada;
  - max_tokens por resposta (COUNCIL_MAX_TOKENS, default 1200) capa a saida;
  - grava tokens usados por chamada (usage do OpenRouter) — custo real visivel;
  - teto de rodada: aborta se estimativa passar COUNCIL_MAX_ROUND_TOKENS.

Endpoints (protegidos por API key — custam dinheiro, nunca publicos):
  POST /council/ask     {"pergunta": "...", "dados": "opcional: tabela/numeros"}
  GET  /council/history ultimas rodadas
"""
import os
import json
import time
import logging
import threading
from datetime import datetime

import requests

log = logging.getLogger('egreja.council')

ENABLED = os.environ.get('COUNCIL_ENABLED', 'false').lower() == 'true'
OR_KEY = os.environ.get('OPENROUTER_API_KEY', '')
OR_URL = 'https://openrouter.ai/api/v1/chat/completions'
MAX_TOKENS = int(os.environ.get('COUNCIL_MAX_TOKENS', 1200))
MAX_ROUND_TOKENS = int(os.environ.get('COUNCIL_MAX_ROUND_TOKENS', 60000))
TIMEOUT_S = int(os.environ.get('COUNCIL_TIMEOUT_S', 90))

# ── PROVEDORES ────────────────────────────────────────────────────────────
# Cada consultor resolve sua rota em tempo de chamada:
#  - se tiver chave DIRETA propria no env (ex.: KIMI_API_KEY), usa o provedor
#    dele (compativel com OpenAI: base_url + key + model);
#  - senao, cai no OpenRouter (uma chave p/ todos).
# Assim da p/ comecar so com a chave do Kimi que o Beto ja tem, e adicionar
# os outros depois (OpenRouter OU chave direta de cada um).
def _resolve(name):
    """Retorna (base_url, api_key, model, provider) para um consultor."""
    direct = {
        'Kimi':   ('KIMI_API_KEY',   os.environ.get('KIMI_BASE_URL', 'https://api.moonshot.ai/v1'),
                   os.environ.get('KIMI_MODEL', 'kimi-k2-0711-preview')),
        'GPT':    ('OPENAI_API_KEY', os.environ.get('OPENAI_BASE_URL', 'https://api.openai.com/v1'),
                   os.environ.get('OPENAI_MODEL', 'gpt-5')),
        'Grok':   ('XAI_API_KEY',    os.environ.get('XAI_BASE_URL', 'https://api.x.ai/v1'),
                   os.environ.get('XAI_MODEL', 'grok-4')),
        'Gemini': ('GEMINI_API_KEY', os.environ.get('GEMINI_BASE_URL',
                   'https://generativelanguage.googleapis.com/v1beta/openai'),
                   os.environ.get('GEMINI_MODEL', 'gemini-2.5-pro')),
    }
    keyenv, base, model = direct[name]
    kv = os.environ.get(keyenv, '')
    if kv:
        return base.rstrip('/') + '/chat/completions', kv, model, f'direct:{keyenv}'
    # fallback OpenRouter (slugs overridaveis; confirmar em openrouter.ai/models)
    or_models = {
        'GPT':    os.environ.get('COUNCIL_MODEL_GPT',    'openai/gpt-5'),
        'Grok':   os.environ.get('COUNCIL_MODEL_GROK',   'x-ai/grok-4'),
        'Gemini': os.environ.get('COUNCIL_MODEL_GEMINI', 'google/gemini-2.5-pro'),
        'Kimi':   os.environ.get('COUNCIL_MODEL_KIMI',   'moonshotai/kimi-k2'),
    }
    return OR_URL, OR_KEY, or_models[name], 'openrouter'

CONSULTANTS = ['GPT', 'Grok', 'Gemini', 'Kimi']

# Persona compartilhada + fatos-nucleo da casa (enxuto de proposito p/ custo).
# Contexto especifico da pergunta vai no campo "dados" da chamada.
PERSONA = (
    "Voce e um consultor senior do painel da Egreja Investment AI — plataforma "
    "autonoma de paper trading (B3, NYSE, cripto, arbitragem cross-listed) rumo a "
    "uma captacao de R$ 15M via crowdfunding (CVM 88). Filosofia: honestidade "
    "radical de dados; nada vai a capital real sem sobreviver liquido de custos, "
    "em sombra, com criterios congelados. Fatos-nucleo (jul/2026, paper, bruto): "
    "NYSE virou apos 21/jul (WR 45->54%, +US$19,3k) pelo gate de regime QQQ; B3 "
    "piorou (+R$28k->-R$17k) por falta de gate de indice; o edge esta no "
    "ALINHAMENTO COM A MARE (Market Pulse), nao no score de 13 indicadores "
    "(correlacao ~zero); ~75% do lucro bruto vem do TRAILING_STOP; custo "
    "round-trip B3 day trade ~0,05% ja supera o edge bruto ~0,046%/trade. "
    "Seja cetico, traga numeros e formulas, diga se sobra edge liquido, e "
    "recomende sempre validacao em sombra com criterios congelados. Responda em "
    "portugues, direto, sem bajulacao."
)

_table_ok = {'v': False}


def _conn():
    try:
        import api_server
        return api_server.get_db()
    except Exception as e:
        log.debug(f'[COUNCIL] conn: {e}')
        return None


def _ensure(cur):
    if _table_ok['v']:
        return
    cur.execute("""CREATE TABLE IF NOT EXISTS council_log (
        id BIGINT AUTO_INCREMENT PRIMARY KEY, round_id VARCHAR(32),
        consultant VARCHAR(20), model VARCHAR(60), pergunta TEXT,
        resposta MEDIUMTEXT, tokens_in INT, tokens_out INT,
        latency_ms INT, erro VARCHAR(200), ts DATETIME,
        INDEX ix_round (round_id), INDEX ix_ts (ts))""")
    _table_ok['v'] = True


def _available():
    """Consultores que tem rota valida (chave direta OU OpenRouter setado)."""
    out = []
    for nm in CONSULTANTS:
        _, key, _, _ = _resolve(nm)
        if key:
            out.append(nm)
    return out


def _ask_one(name, pergunta, dados):
    """Chama um consultor (provedor direto ou OpenRouter). Retorna dict."""
    t0 = time.time()
    base_url, api_key, model, provider = _resolve(name)
    user = pergunta if not dados else f"{pergunta}\n\n--- DADOS PARA ESTA ANALISE ---\n{dados}"
    try:
        r = requests.post(base_url, timeout=TIMEOUT_S,
            headers={'Authorization': f'Bearer {api_key}',
                     'HTTP-Referer': 'https://egreja.net',
                     'X-Title': 'Egreja Council'},
            json={'model': model, 'max_tokens': MAX_TOKENS,
                  'messages': [{'role': 'system', 'content': PERSONA},
                               {'role': 'user', 'content': user}]})
        lat = int((time.time() - t0) * 1000)
        if r.status_code != 200:
            return {'consultant': name, 'model': model, 'erro': f'HTTP {r.status_code}: {r.text[:160]}',
                    'resposta': None, 'tokens_in': 0, 'tokens_out': 0, 'latency_ms': lat}
        j = r.json()
        msg = (((j.get('choices') or [{}])[0]).get('message') or {}).get('content', '')
        usage = j.get('usage') or {}
        return {'consultant': name, 'model': model, 'resposta': msg, 'erro': None,
                'tokens_in': int(usage.get('prompt_tokens') or 0),
                'tokens_out': int(usage.get('completion_tokens') or 0),
                'latency_ms': lat}
    except Exception as e:
        return {'consultant': name, 'model': model, 'erro': str(e)[:180],
                'resposta': None, 'tokens_in': 0, 'tokens_out': 0,
                'latency_ms': int((time.time() - t0) * 1000)}


def ask_council(pergunta, dados=None, only=None):
    """Dispara a pergunta para todos os consultores em paralelo.
    only: lista opcional de nomes p/ subconjunto (ex.: ['Gemini','Kimi'])."""
    if not ENABLED:
        return {'error': 'council desabilitado (COUNCIL_ENABLED != true)'}
    avail = _available()
    targets = [n for n in avail if (only is None or n in only)]
    if not targets:
        return {'error': 'nenhum consultor com chave configurada (KIMI_API_KEY, '
                         'OPENAI_API_KEY, XAI_API_KEY, GEMINI_API_KEY ou OPENROUTER_API_KEY)'}
    # teto de rodada (estimativa grosseira: 4 chars/token do input compartilhado)
    est_in = (len(PERSONA) + len(pergunta) + len(dados or '')) // 4 * len(targets)
    est_out = MAX_TOKENS * len(targets)
    if est_in + est_out > MAX_ROUND_TOKENS:
        return {'error': f'estimativa {est_in+est_out} tokens > teto {MAX_ROUND_TOKENS} '
                         f'(reduza os dados ou suba COUNCIL_MAX_ROUND_TOKENS)'}
    round_id = datetime.utcnow().strftime('%Y%m%d%H%M%S')
    results = {}
    threads = []

    def _run(nm):
        results[nm] = _ask_one(nm, pergunta, dados)

    for nm in targets:
        t = threading.Thread(target=_run, args=(nm,))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()

    # persistir (registro auditavel — cultura da casa)
    try:
        c = _conn()
        if c:
            cur = c.cursor()
            _ensure(cur)
            for nm, r in results.items():
                cur.execute("""INSERT INTO council_log (round_id, consultant, model,
                    pergunta, resposta, tokens_in, tokens_out, latency_ms, erro, ts)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())""",
                    (round_id, nm, r['model'], pergunta[:4000], r.get('resposta'),
                     r['tokens_in'], r['tokens_out'], r['latency_ms'], r.get('erro')))
            c.commit()
            cur.close()
            c.close()
    except Exception as e:
        log.debug(f'[COUNCIL] persist: {e}')

    tot_in = sum(r['tokens_in'] for r in results.values())
    tot_out = sum(r['tokens_out'] for r in results.values())
    return {'round_id': round_id, 'pergunta': pergunta,
            'respostas': results,
            'uso': {'tokens_in': tot_in, 'tokens_out': tot_out,
                    'nota': 'custo real = tokens x preco do modelo no OpenRouter; ver openrouter.ai/activity'}}


def history(limit=20):
    out = {'enabled': ENABLED, 'consultores_disponiveis': _available(),
           'rotas': {n: {'provider': _resolve(n)[3], 'model': _resolve(n)[2]} for n in CONSULTANTS},
           'max_tokens_resposta': MAX_TOKENS, 'teto_rodada_tokens': MAX_ROUND_TOKENS}
    c = _conn()
    if not c:
        return out
    try:
        cur = c.cursor()
        _ensure(cur)
        cur.execute("""SELECT round_id, consultant, LEFT(pergunta,80) q,
            tokens_in, tokens_out, latency_ms, erro, ts FROM council_log
            ORDER BY id DESC LIMIT %s""", (limit,))
        out['ultimas'] = [
            {'round_id': r[0], 'consultant': r[1], 'pergunta': r[2],
             'tokens_in': r[3], 'tokens_out': r[4], 'latency_ms': r[5],
             'erro': r[6], 'ts': str(r[7])} for r in cur.fetchall()]
        cur.close()
        c.close()
    except Exception as e:
        log.debug(f'[COUNCIL] history: {e}')
    return out
