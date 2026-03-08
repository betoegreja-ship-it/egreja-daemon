#!/usr/bin/env python3
"""
Egreja Investment AI — API Server v10.7.0
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
v10.6.4 → v10.7.0: Polling inteligente + 6 cirurgias (dedup stocks+crypto, dead code, klines unificado, signal_id real)

FONTES DE DADOS (em ordem de prioridade)
  Stocks US / NYSE:  Polygon.io REST (candles + snapshot) → FMP → Yahoo
  Stocks B3:         brapi.dev (especializado) → Polygon → FMP → Yahoo
  Crypto:            Binance REST público (allTickers bulk) → FMP → Yahoo
  FX (USDBRL etc.):  frankfurter.app (ECB, free) → Yahoo
  Arbi legs:         camada unificada _fetch_arbi_price() (Binance/Polygon/brapi/FMP/Yahoo)

  Env vars novas:
    POLYGON_API_KEY  — obrigatório para stocks US com qualidade máxima
    BRAPI_TOKEN      — recomendado para B3 (sem token: modo free com rate limit)
  Env vars mantidas:
    FMP_API_KEY      — fallback secundário (ainda útil)

FEATURES ENRIQUECIDAS  [v10.4]
  atr_bucket    — ATR como % do preço (VERY_LOW/LOW/NORMAL/HIGH/EXTREME)
                  calculado com high/low reais quando disponíveis (Polygon, brapi, Binance klines)
                  distingue ativo em compressão de ativo em expansão de volatilidade

  volume_bucket — ratio volume_hoje / média_20d (VERY_LOW/LOW/NORMAL/HIGH/SURGE)
                  confirma ou invalida o movimento; volume fraco = sinal suspeito
                  disponível via Polygon, brapi e Binance klines

  weekday       — agora faz parte do make_feature_hash()
                  segunda-feira (gap open) e sexta-feira (liquidez reduzida) têm
                  padrões distintos que o learning vai capturar naturalmente

SCORE COMPOSTO CRYPTO  [v10.4]
  Substitui: score = 50 + int(abs(change_24h) * 5)   ← ignorava volume e ATR
  Novo: _crypto_composite_score() — 4 fatores ponderados:
    40% change_pct_24h  — força direcional (capped ±15%)
    30% volume_ratio    — volume USDT hoje vs média 20d (confirma movimento)
    20% range_position  — posição do preço no high/low do dia (direcionalidade intraday)
    10% liquidez        — n_trades normalizado (evita altcoins ilíquidas)
  Klines Binance são cacheadas por 1 hora por símbolo (sem impacto em rate limit)

DEDUPLICAÇÃO CRYPTO  [v10.4, melhoria sobre v10.3.4]
  Substituiu: ms_key = f"CRY:{sym}:{direction}:{score}:{int(price)}"
              ↑ instável em altcoins com preço < 1 USDT
  Novo:       ms_key = f"CRY:{sym}:{direction}:{int(time.time()/90)}"
              chave muda a cada janela de 90s — exatamente o ciclo do loop
              sem falsos positivos por variação de centavo em DOGE/XRP etc.

Herdado e preservado da v10.3.4 (F1..F5) e ancestrais.
"""







import decimal   # [v10.7] movido do interior de funções para o nível de módulo
import os, time, queue, json, uuid, threading, itertools, requests, logging, hashlib, math
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
from flask import Flask, jsonify, request
from flask_cors import CORS
import mysql.connector

logging.basicConfig(level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S')
log = logging.getLogger('egreja')

app = Flask(__name__)
CORS(app)

# ═══════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════
ENV = os.environ.get('ENV', 'dev').lower()

# [C-3] Single-process enforcement
GUNICORN_WORKERS = int(os.environ.get('WEB_CONCURRENCY', os.environ.get('GUNICORN_WORKERS', 1)))
if GUNICORN_WORKERS > 1:
    raise RuntimeError(
        f'[C-3] This system uses in-process state (global lists + threads). '
        f'Running with {GUNICORN_WORKERS} workers would create parallel universes. '
        f'Set WEB_CONCURRENCY=1 or GUNICORN_WORKERS=1.')

INITIAL_CAPITAL_STOCKS = float(os.environ.get('INITIAL_CAPITAL_STOCKS', 9_000_000))
INITIAL_CAPITAL_CRYPTO = float(os.environ.get('INITIAL_CAPITAL_CRYPTO', 1_000_000))
MAX_POSITION_STOCKS    = float(os.environ.get('MAX_POSITION_STOCKS', 450_000))
MAX_POSITION_CRYPTO    = float(os.environ.get('MAX_POSITION_CRYPTO',  50_000))

FMP_API_KEY      = os.environ.get('FMP_API_KEY', '')        # mantido como fallback terciário
POLYGON_API_KEY  = os.environ.get('POLYGON_API_KEY', '')    # primário para stocks US/NYSE
BRAPI_TOKEN      = os.environ.get('BRAPI_TOKEN', '')        # primário para stocks B3
# Binance: endpoints públicos sem key | frankfurter.app: BCE, free, sem key
API_SECRET_KEY = os.environ.get('API_SECRET_KEY', '')

if ENV == 'production' and not API_SECRET_KEY:
    raise RuntimeError('[P0-3] API_SECRET_KEY is REQUIRED in production.')
if not POLYGON_API_KEY:
    log.warning('POLYGON_API_KEY not set — stocks US/NYSE usarão fallback FMP→Yahoo')
if not BRAPI_TOKEN:
    log.info('BRAPI_TOKEN não configurado — B3 usará mapa ADR/Polygon como proxy')

# [v10.5-1] Mapa explícito B3 ticker → ADR no NYSE/NASDAQ para Polygon como fallback.
# Apenas os ADRs mais líquidos e com cobertura confiável no Polygon.
B3_TO_ADR = {
    'PETR4': 'PBR',   'PETR3': 'PBR-A',
    'VALE3': 'VALE',
    'ITUB4': 'ITUB',  'ITUB3': 'ITUB',
    'BBDC4': 'BBD',   'BBDC3': 'BBD',
    'ABEV3': 'ABEV',
    'EMBR3': 'ERJ',
    'PBR':   'PBR',   # já é ADR — passthrough
    'VALE':  'VALE',
}
# ADRs têm preço em USD; converter para BRL usando fx_rates['USDBRL']
B3_ADR_SYMBOLS = set(B3_TO_ADR.values())
if not FMP_API_KEY and not POLYGON_API_KEY:
    log.warning('Nenhuma API key configurada — usando Yahoo Finance (não recomendado em produção)')

PUBLIC_ROUTES = {'/', '/health', '/degraded'}

TWILIO_SID     = os.environ.get('TWILIO_ACCOUNT_SID', '')
TWILIO_TOKEN   = os.environ.get('TWILIO_AUTH_TOKEN', '')
TWILIO_FROM    = os.environ.get('TWILIO_WHATSAPP_FROM', 'whatsapp:+14155238886')
TWILIO_TO      = os.environ.get('TWILIO_WHATSAPP_TO', '')
ALERTS_ENABLED = bool(TWILIO_SID and TWILIO_TOKEN and TWILIO_TO)
ALERT_MIN_SCORE = int(os.environ.get('ALERT_MIN_SCORE', 80))

MAX_CAPITAL_PCT_STOCKS   = float(os.environ.get('MAX_CAPITAL_PCT_STOCKS', 90.0))
MAX_CAPITAL_PCT_CRYPTO   = float(os.environ.get('MAX_CAPITAL_PCT_CRYPTO', 90.0))
MAX_POSITIONS_STOCKS     = int(os.environ.get('MAX_POSITIONS_STOCKS', 15))
MAX_POSITIONS_CRYPTO     = int(os.environ.get('MAX_POSITIONS_CRYPTO', 10))

# Arbitragem — livro segregado
ARBI_CAPITAL         = float(os.environ.get('ARBI_CAPITAL', 500_000))
ARBI_MIN_SPREAD      = float(os.environ.get('ARBI_MIN_SPREAD', 2.0))
ARBI_TP_SPREAD       = float(os.environ.get('ARBI_TP_SPREAD', 0.5))
ARBI_SL_PCT          = float(os.environ.get('ARBI_SL_PCT', 1.5))
ARBI_TIMEOUT_H       = float(os.environ.get('ARBI_TIMEOUT_H', 72))
ARBI_POS_SIZE        = float(os.environ.get('ARBI_POS_SIZE', 50_000))
ARBI_MAX_POSITIONS   = int(os.environ.get('ARBI_MAX_POSITIONS', 8))
ARBI_MAX_DAILY_LOSS  = float(os.environ.get('ARBI_MAX_DAILY_LOSS_PCT', 1.5))
ARBI_KILL_SWITCH     = False

# Risco global
MAX_OPEN_POSITIONS      = int(os.environ.get('MAX_OPEN_POSITIONS', 25))
MAX_DAILY_DRAWDOWN_PCT  = float(os.environ.get('MAX_DAILY_DRAWDOWN_PCT', 2.0))
MAX_WEEKLY_DRAWDOWN_PCT = float(os.environ.get('MAX_WEEKLY_DRAWDOWN_PCT', 5.0))
MAX_POSITION_SAME_MKT   = int(os.environ.get('MAX_POSITION_SAME_MKT', 10))
MAX_SAME_SYMBOL         = int(os.environ.get('MAX_SAME_SYMBOL', 1))
MAX_RISK_PER_TRADE_PCT  = float(os.environ.get('MAX_RISK_PER_TRADE_PCT', 1.5))
RISK_KILL_SWITCH        = False

SIGNAL_MAX_AGE_MIN  = int(os.environ.get('SIGNAL_MAX_AGE_MIN', 30))
SYMBOL_COOLDOWN_SEC = int(os.environ.get('SYMBOL_COOLDOWN_SEC', 300))

# [V9-2] Limites de proteção da fila crítica
URGENT_QUEUE_WARN = int(os.environ.get('URGENT_QUEUE_WARN', 1000))
URGENT_QUEUE_CRIT = int(os.environ.get('URGENT_QUEUE_CRIT', 5000))
_queue_alert_last = 0   # throttle de alerta da fila

# [C-1] Heartbeat timeout POR THREAD (segundos)
THREAD_HEARTBEAT_TIMEOUT = {
    'stock_price_loop':       420,
    'crypto_price_loop':      60,
    'monitor_trades':         30,
    'auto_trade_crypto':      200,
    'stock_execution_worker': 150,
    'arbi_scan_loop':         400,
    'arbi_monitor_loop':      120,
    'snapshot_loop':          400,
    'persistence_worker':     30,
    'alert_worker':           30,
    'watchdog':               60,
    'shadow_evaluator_loop':  1200,   # [FIX-5] 20 min timeout
}
DEFAULT_HB_TIMEOUT = int(os.environ.get('DEFAULT_HB_TIMEOUT', 120))
WATCHDOG_RESET_STABLE_H = float(os.environ.get('WATCHDOG_RESET_STABLE_H', 6.0))

# ── Learning Engine config ────────────────────────────────────────
LEARNING_VERSION       = '10.7.0'
LEARNING_MIN_SAMPLES   = int(os.environ.get('LEARNING_MIN_SAMPLES', 10))   # amostras mínimas para confiar no histórico
LEARNING_EWMA_ALPHA    = float(os.environ.get('LEARNING_EWMA_ALPHA', 0.15)) # recência (0=ignore histórico, 1=só recente)
RISK_MULT_MIN          = float(os.environ.get('RISK_MULT_MIN', 0.50))       # [L-9] multiplicador mínimo conservador
RISK_MULT_MAX          = float(os.environ.get('RISK_MULT_MAX', 1.15))       # [L-9] multiplicador máximo
SHADOW_TRACK_REASONS   = {'confidence_low','market_closed','risk_blocked','symbol_open','kill_switch','cooldown','capital'}
LEARNING_ENABLED       = os.environ.get('LEARNING_ENABLED', 'true').lower() != 'false'

CRYPTO_SYMBOLS = [
    'BTCUSDT','ETHUSDT','BNBUSDT','SOLUSDT','XRPUSDT',
    'ADAUSDT','DOGEUSDT','AVAXUSDT','TRXUSDT','DOTUSDT',
    'LINKUSDT','MATICUSDT','LTCUSDT','UNIUSDT','ATOMUSDT',
    'XLMUSDT','BCHUSDT','NEARUSDT','APTUSDT','ARBUSDT'
]
CRYPTO_NAMES = {
    'BTCUSDT':'Bitcoin','ETHUSDT':'Ethereum','BNBUSDT':'BNB','SOLUSDT':'Solana',
    'XRPUSDT':'XRP','ADAUSDT':'Cardano','DOGEUSDT':'Dogecoin','AVAXUSDT':'Avalanche',
    'TRXUSDT':'TRON','DOTUSDT':'Polkadot','LINKUSDT':'Chainlink','MATICUSDT':'Polygon',
    'LTCUSDT':'Litecoin','UNIUSDT':'Uniswap','ATOMUSDT':'Cosmos','XLMUSDT':'Stellar',
    'BCHUSDT':'Bitcoin Cash','NEARUSDT':'NEAR','APTUSDT':'Aptos','ARBUSDT':'Arbitrum'
}

# ═══════════════════════════════════════════════════════════════
# MYSQL
# ═══════════════════════════════════════════════════════════════
db_config = {
    'host':     os.environ.get('MYSQLHOST', 'mysql.railway.internal'),
    'port':     int(os.environ.get('MYSQLPORT', 3306)),
    'user':     os.environ.get('MYSQLUSER', 'root'),
    'password': os.environ.get('MYSQLPASSWORD', ''),
    'database': os.environ.get('MYSQLDATABASE', 'railway'),
    'autocommit': True, 'connection_timeout': 10
}

# [v10.7-Fix1] Connection pool — elimina overhead de connect/disconnect por operação.
# Pool de 10 conexões: suficiente para persistence_worker + shadow_evaluator + workers simultâneos.
# Sem pool: cada get_db() abre uma TCP connection nova (~5-50ms), Railway tem limite baixo.
_db_pool = None
_db_pool_lock = threading.Lock()

def _get_pool():
    """Inicializa o pool na primeira chamada (lazy) e retorna a instância."""
    global _db_pool
    if _db_pool is not None:
        return _db_pool
    with _db_pool_lock:
        if _db_pool is not None:
            return _db_pool
        try:
            from mysql.connector.pooling import MySQLConnectionPool
            pool_cfg = dict(db_config)
            pool_cfg.pop('autocommit', None)   # pooling não aceita autocommit no config
            pool_cfg.pop('connection_timeout', None)
            _db_pool = MySQLConnectionPool(
                pool_name='egreja', pool_size=20,
                autocommit=True, connection_timeout=10,
                **pool_cfg)
            log.info('[v10.7] MySQL connection pool inicializado (size=10)')
        except Exception as e:
            log.error(f'MySQL pool init: {e}')
    return _db_pool

def get_db():
    """Retorna uma conexão do pool. Caller é responsável por chamar .close()
    (que devolve a conexão ao pool, não fecha a TCP connection).
    Em caso de falha no pool, faz fallback para conexão direta.
    """
    pool = _get_pool()
    if pool:
        try:
            return pool.get_connection()
        except Exception as e:
            log.warning(f'Pool get_connection: {e} — tentando conexão direta')
    # Fallback direto (ex.: pool esgotado ou erro de inicialização)
    try:
        return mysql.connector.connect(**db_config)
    except Exception as e:
        log.error(f'MySQL fallback connect: {e}')
        return None

def test_db():
    c = get_db()
    if c: c.close(); return True
    return False

# ═══════════════════════════════════════════════════════════════
# STATE + LOCKS
# ═══════════════════════════════════════════════════════════════
stocks_capital = INITIAL_CAPITAL_STOCKS
crypto_capital = INITIAL_CAPITAL_CRYPTO
arbi_capital   = ARBI_CAPITAL

stocks_open  = []; stocks_closed  = []
crypto_open  = []; crypto_closed  = []
arbi_open    = []; arbi_closed    = []

# [v10.7-Fix3] Cap em listas de trades fechados.
# Sem cap, após 6 meses com 20-30 trades/dia = 3.000-5.000 entradas em memória.
# check_risk() itera s_closed+c_closed em cada sinal → O(n) no caminho crítico.
# 500 entradas cobre >7 dias de histórico para drawdown (janela máxima = 7d).
MAX_CLOSED_HISTORY = int(os.environ.get('MAX_CLOSED_HISTORY', 500))

state_lock       = threading.Lock()
orders_lock      = threading.Lock()
audit_lock       = threading.Lock()
dq_lock          = threading.Lock()
degraded_lock    = threading.Lock()   # [V91-5] protege DEGRADED_MODE
learning_lock    = threading.Lock()   # [L-3] protege caches de learning em memória

stock_prices    = {}
crypto_prices   = {}
crypto_momentum = {}
crypto_tickers  = {}   # [v10.4] dados extras Binance: high_24h, low_24h, vol_quote, n_trades
market_regime   = {'mode':'UNKNOWN','volatility':'NORMAL','avg_change_pct':0,'updated_at':''}
arbi_spreads    = {}
fx_rates        = {}

symbol_cooldown = {}
alerted_signals = {}
alerted_trades  = {}

orders_log  = []
audit_log   = []
data_quality= {}

# ── [L-3/L-4] Caches de learning em memória (protegidos por learning_lock) ──
# Espelham as tabelas pattern_stats e factor_stats para evitar I/O no caminho crítico
pattern_stats_cache: dict = {}   # feature_hash → stats dict
factor_stats_cache:  dict = {}   # (factor_type, factor_value) → stats dict
signal_events_count: int  = 0
last_learning_update: str = ''
learning_errors:     int  = 0
LEARNING_DEGRADED:   bool = False

# [P0-2] Deduplicação: rastreia IDs de market_signals já processados nesta sessão.
# Formato: {market_signal_db_id: signal_event_id}
# LRU manual: descarta metade quando ultrapassa MAX_PROCESSED_SIGNALS_CACHE.
# Evita que o mesmo sinal de origem gere vários signal_events e shadow_decisions
# enquanto permanecer dentro da janela SIGNAL_MAX_AGE_MIN.
MAX_PROCESSED_SIGNALS_CACHE = 2000
processed_signal_ids: dict = {}   # market_signal_id → signal_event_id

thread_health        = {}
thread_fns           = {}
thread_restart_count = {}
thread_last_restart  = {}
thread_heartbeat     = {}

def gen_id(prefix='TRD'):
    return f"{prefix}-{uuid.uuid4().hex[:12]}"

def beat(name):
    thread_heartbeat[name] = time.time()

# ═══════════════════════════════════════════════════════════════
# [V9-3] DEGRADED MODE
# ═══════════════════════════════════════════════════════════════
DEGRADED_MODE = {
    'active':     False,
    'reasons':    [],
    'since':      None,
    'queue_size': 0,
}

def _check_degraded():
    """[V9-3][V91-5] Atualiza estado degradado com lock próprio."""
    reasons = []
    qsize = urgent_queue.qsize()

    if qsize >= URGENT_QUEUE_CRIT:
        reasons.append(f'QUEUE_CRITICAL:{qsize}')
    elif qsize >= URGENT_QUEUE_WARN:
        reasons.append(f'QUEUE_HIGH:{qsize}')

    with dq_lock:
        dq_snap = list(data_quality.values())
    if dq_snap:
        stale_n = sum(1 for s in dq_snap if s.get('stale'))
        if stale_n / len(dq_snap) > 0.5:
            reasons.append(f'FEED_STALE:{stale_n}/{len(dq_snap)}')

    now = time.time()
    recent_restarts = sum(
        1 for name in thread_restart_count
        if thread_restart_count.get(name, 0) > 0
        and (now - thread_last_restart.get(name, 0)) < 6 * 3600
    )
    if recent_restarts > 0:
        reasons.append(f'THREAD_RESTARTS:{recent_restarts}')

    if RISK_KILL_SWITCH:
        reasons.append('KILL_SWITCH_ACTIVE')
    if ARBI_KILL_SWITCH:
        reasons.append('ARBI_KILL_SWITCH_ACTIVE')

    active = len(reasons) > 0

    with degraded_lock:   # [V91-5]
        was_active = DEGRADED_MODE['active']
        if active and not was_active:
            DEGRADED_MODE['since'] = datetime.utcnow().isoformat()
        elif not active:
            DEGRADED_MODE['since'] = None
        DEGRADED_MODE['active']     = active
        DEGRADED_MODE['reasons']    = reasons
        DEGRADED_MODE['queue_size'] = qsize

def _read_degraded():
    """[V91-5] Leitura thread-safe de DEGRADED_MODE."""
    with degraded_lock:
        return dict(DEGRADED_MODE)

# ═══════════════════════════════════════════════════════════════
# [BUG-1] QUEUES — PriorityQueue com (priority, seq, item)
#   Sem counter monotônico, dois dicts com mesma priority causam
#   TypeError: '<' not supported between instances of 'dict' and 'dict'
# ═══════════════════════════════════════════════════════════════
_urgent_seq   = itertools.count()      # [BUG-1] contador monotônico — nunca compara dicts
persist_queue = queue.Queue()          # mantido para compat, não usado diretamente
alert_queue   = queue.Queue(maxsize=500)
urgent_queue  = queue.PriorityQueue() # (priority, seq, item) — sem risco de comparar dicts

PERSIST_PRIORITY = {
    'trade':                1,
    'order':                1,
    'audit':                2,
    'arbi':                 1,
    'cooldown':             3,
    'snapshot':             4,
    'signal_event':         3,
    'signal_attribution':   2,   # [FIX-2] vínculo trade↔sinal — prioridade alta
    'signal_outcome':       2,
    'pattern_stats':        4,
    'factor_stats':         4,
    'shadow_decision':      5,
}

def enqueue_persist(kind, data=None, **kwargs):
    """[BUG-1] Enfileira para persistência. Nunca descarta. Nunca compara dicts."""
    item = {'kind': kind}
    if data is not None: item['data'] = data
    item.update(kwargs)
    priority = PERSIST_PRIORITY.get(kind, 5)
    # [BUG-1] seq garante ordem FIFO dentro de mesma prioridade e NUNCA compara dicts
    urgent_queue.put((priority, next(_urgent_seq), item))

def send_whatsapp(message):
    try:
        alert_queue.put_nowait({'kind': 'whatsapp', 'message': message})
    except queue.Full:
        log.warning(f'alert_queue full — alert dropped: {message[:60]}')

def _send_whatsapp_direct(message):
    if not ALERTS_ENABLED:
        log.info(f'[ALERT disabled] {message[:80]}'); return False
    try:
        r = requests.post(
            f'https://api.twilio.com/2010-04-01/Accounts/{TWILIO_SID}/Messages.json',
            auth=(TWILIO_SID, TWILIO_TOKEN),
            data={'From': TWILIO_FROM, 'To': TWILIO_TO, 'Body': message}, timeout=10)
        return r.status_code == 201
    except Exception as e:
        log.error(f'WhatsApp direct: {e}'); return False

def persistence_worker():
    """[BUG-1] Desempacota (priority, seq, item) — nunca mais TypeError."""
    global _queue_alert_last
    while True:
        beat('persistence_worker')
        try:
            # [BUG-1] agora são 3 elementos
            priority, seq, task = urgent_queue.get(timeout=5)
            kind = task.get('kind')
            if kind == 'trade':          _db_save_trade(task['data'])
            elif kind == 'arbi':         _db_save_arbi_trade(task['data'])
            elif kind == 'audit':        _db_insert_audit(task['data'])
            elif kind == 'order':        _db_save_order(task['data'])
            elif kind == 'snapshot':     _db_save_snapshot(task['data'])
            elif kind == 'cooldown':     _db_save_cooldown(task['symbol'], task['ts'])
            elif kind == 'signal_event':       _db_save_signal_event(task['data'])
            elif kind == 'signal_attribution': _db_update_signal_attribution(task['data'])
            elif kind == 'signal_outcome':     _db_update_signal_outcome(task['data'])
            elif kind == 'pattern_stats':      _db_upsert_pattern_stats(task['data'])
            elif kind == 'factor_stats':       _db_upsert_factor_stats(task['data'])
            elif kind == 'shadow_decision':    _db_save_shadow_decision(task['data'])
            urgent_queue.task_done()

            # [V9-2] Monitorar crescimento da fila após cada processamento
            qsize = urgent_queue.qsize()
            now   = time.time()
            if qsize >= URGENT_QUEUE_CRIT and now - _queue_alert_last > 300:
                _queue_alert_last = now
                log.critical(f'[V9-2] urgent_queue CRÍTICA: {qsize} itens — DB pode estar lento/travado')
                send_whatsapp(f'CRÍTICO: fila de persistência com {qsize} itens. Verificar banco de dados.')
            elif qsize >= URGENT_QUEUE_WARN:
                log.warning(f'[V9-2] urgent_queue alta: {qsize} itens')

        except queue.Empty:
            pass
        except Exception as e:
            log.error(f'persistence_worker: {e}')

def alert_worker():
    while True:
        beat('alert_worker')
        try:
            task = alert_queue.get(timeout=5)
            if task.get('kind') == 'whatsapp':
                _send_whatsapp_direct(task['message'])
            alert_queue.task_done()
        except queue.Empty:
            pass
        except Exception as e:
            log.error(f'alert_worker: {e}')

# ═══════════════════════════════════════════════════════════════
# AUTH MIDDLEWARE
# ═══════════════════════════════════════════════════════════════
@app.before_request
def auth_check():
    if request.method == 'OPTIONS':
        return None
    if not API_SECRET_KEY:
        return None
    if request.path in PUBLIC_ROUTES or request.path.startswith('/health'):
        return None
    key = request.headers.get('X-API-Key', '').strip()
    if key != API_SECRET_KEY:
        log.warning(f'Unauthorized: {request.remote_addr} {request.path}')
        return jsonify({'error': 'Unauthorized — X-API-Key required'}), 401

def require_auth(f):
    """[FIX-1] Decorador de documentação — autenticação real feita pelo before_request.
    Mantido para clareza semântica: marcar explicitamente rotas que exigem auth."""
    from functools import wraps
    @wraps(f)
    def decorated(*args, **kwargs):
        return f(*args, **kwargs)
    return decorated

# ═══════════════════════════════════════════════════════════════
# AUDIT
# ═══════════════════════════════════════════════════════════════
def audit(event, data):
    entry = {'timestamp': datetime.utcnow().isoformat(), 'event': event, **data}
    with audit_lock:
        audit_log.append(entry)
        if len(audit_log) > 1000: audit_log.pop(0)
    log.info(f'[AUDIT] {event} | {data}')
    enqueue_persist('audit', entry)

def _db_insert_audit(entry):
    conn = get_db()
    if not conn: return
    try:
        cursor    = conn.cursor()
        event     = entry.get('event', '')
        entity_id = str(entry.get('id') or entry.get('pair') or entry.get('symbol', ''))
        payload   = json.dumps({k:v for k,v in entry.items() if k not in ('event','timestamp')})
        cursor.execute(
            "INSERT INTO audit_events (event_type, entity_type, entity_id, payload_json) "
            "VALUES (%s, %s, %s, %s)",
            (event, event.split('_')[0].lower(), entity_id, payload))
        conn.commit(); cursor.close(); conn.close()
    except Exception as e:
        log.error(f'_db_insert_audit: {e}')

# ═══════════════════════════════════════════════════════════════
# CALENDÁRIO DE FERIADOS 2025-2027
# ═══════════════════════════════════════════════════════════════
_NYSE_HOLIDAYS = {
    date(2025,1,1), date(2025,1,20), date(2025,2,17), date(2025,4,18),
    date(2025,5,26), date(2025,6,19), date(2025,7,4), date(2025,9,1),
    date(2025,11,27), date(2025,12,25),
    date(2026,1,1), date(2026,1,19), date(2026,2,16), date(2026,4,3),
    date(2026,5,25), date(2026,6,19), date(2026,7,3), date(2026,9,7),
    date(2026,11,26), date(2026,12,25),
    date(2027,1,1), date(2027,1,18), date(2027,2,15), date(2027,3,26),
    date(2027,5,31), date(2027,6,18), date(2027,7,5), date(2027,9,6),
    date(2027,11,25), date(2027,12,24),
}
_B3_HOLIDAYS = {
    date(2025,1,1), date(2025,3,3), date(2025,3,4), date(2025,4,18),
    date(2025,4,21), date(2025,5,1), date(2025,6,19), date(2025,9,7),
    date(2025,10,12), date(2025,11,2), date(2025,11,15), date(2025,11,20), date(2025,12,25),
    date(2026,1,1), date(2026,2,16), date(2026,2,17), date(2026,4,3),
    date(2026,4,21), date(2026,5,1), date(2026,6,4), date(2026,9,7),
    date(2026,10,12), date(2026,11,2), date(2026,11,15), date(2026,11,20), date(2026,12,25),
    date(2027,1,1), date(2027,2,8), date(2027,2,9), date(2027,3,26),
    date(2027,4,21), date(2027,5,1), date(2027,5,27), date(2027,9,7),
    date(2027,10,12), date(2027,11,2), date(2027,11,15), date(2027,11,20), date(2027,12,25),
}
_LSE_HOLIDAYS = {
    date(2025,1,1), date(2025,4,18), date(2025,4,21), date(2025,5,5),
    date(2025,5,26), date(2025,8,25), date(2025,12,25), date(2025,12,26),
    date(2026,1,1), date(2026,4,3), date(2026,4,6), date(2026,5,4),
    date(2026,5,25), date(2026,8,31), date(2026,12,25), date(2026,12,28),
    date(2027,1,1), date(2027,3,26), date(2027,3,29), date(2027,5,3),
    date(2027,5,31), date(2027,8,30), date(2027,12,27), date(2027,12,28),
}
_HKEX_HOLIDAYS = {
    date(2025,1,1), date(2025,1,29), date(2025,1,30), date(2025,1,31),
    date(2025,4,4), date(2025,4,18), date(2025,4,21), date(2025,5,1),
    date(2025,5,5), date(2025,6,2), date(2025,7,1), date(2025,10,1),
    date(2025,10,2), date(2025,10,7), date(2025,12,25), date(2025,12,26),
    date(2026,1,1), date(2026,2,17), date(2026,2,18), date(2026,2,19),
    date(2026,4,3), date(2026,4,6), date(2026,4,7), date(2026,5,1),
    date(2026,5,25), date(2026,6,19), date(2026,7,1), date(2026,10,1),
    date(2026,10,2), date(2026,12,25),
}

TZ_SAO_PAULO = ZoneInfo('America/Sao_Paulo')
TZ_NEW_YORK  = ZoneInfo('America/New_York')
TZ_LONDON    = ZoneInfo('Europe/London')
TZ_HK        = ZoneInfo('Asia/Hong_Kong')

def is_b3_open():
    now = datetime.now(TZ_SAO_PAULO)
    if now.weekday()>=5 or now.date() in _B3_HOLIDAYS: return False
    h = now.hour + now.minute/60.0; return 10.0<=h<17.0

def is_nyse_open():
    now = datetime.now(TZ_NEW_YORK)
    if now.weekday()>=5 or now.date() in _NYSE_HOLIDAYS: return False
    h = now.hour + now.minute/60.0; return 9.5<=h<16.0

def is_lse_open():
    now = datetime.now(TZ_LONDON)
    if now.weekday()>=5 or now.date() in _LSE_HOLIDAYS: return False
    h = now.hour + now.minute/60.0; return 8.0<=h<16.5

def is_hkex_open():
    now = datetime.now(TZ_HK)
    if now.weekday()>=5 or now.date() in _HKEX_HOLIDAYS: return False
    h = now.hour + now.minute/60.0; return (9.5<=h<12.0) or (13.0<=h<16.0)

def market_open_for(mkt):
    if mkt=='CRYPTO':                return True
    if mkt=='B3':                    return is_b3_open()
    if mkt in ('NYSE','NASDAQ','US'):return is_nyse_open()
    if mkt=='LSE':                   return is_lse_open()
    if mkt=='HKEX':                  return is_hkex_open()
    return False

# ═══════════════════════════════════════════════════════════════
# RISK ENGINE
# ═══════════════════════════════════════════════════════════════
def check_risk(symbol, market_type, position_value, strategy='stocks'):
    global RISK_KILL_SWITCH
    if RISK_KILL_SWITCH: return False, 'KILL_SWITCH_ACTIVE', 0

    with state_lock:
        all_open = stocks_open + crypto_open
        s_open   = list(stocks_open); c_open = list(crypto_open)
        s_closed = list(stocks_closed); c_closed = list(crypto_closed)
        sc = stocks_capital; cc = crypto_capital

    if len(all_open) >= MAX_OPEN_POSITIONS:
        return False, f'MAX_OPEN_POSITIONS ({len(all_open)}/{MAX_OPEN_POSITIONS})', 0

    if strategy == 'stocks':
        sc_count = sum(1 for t in s_open if t.get('asset_type')=='stock')
        if sc_count >= MAX_POSITIONS_STOCKS:
            return False, f'MAX_POSITIONS_STOCKS ({sc_count}/{MAX_POSITIONS_STOCKS})', 0
        committed = sum(t.get('position_value',0) for t in s_open)
        if committed+position_value > INITIAL_CAPITAL_STOCKS*MAX_CAPITAL_PCT_STOCKS/100:
            return False, 'STOCKS_CAPITAL_LIMIT', 0
        free_cap = sc; max_pos = MAX_POSITION_STOCKS
    elif strategy == 'crypto':
        cc_count = sum(1 for t in c_open if t.get('asset_type')=='crypto')
        if cc_count >= MAX_POSITIONS_CRYPTO:
            return False, f'MAX_POSITIONS_CRYPTO ({cc_count}/{MAX_POSITIONS_CRYPTO})', 0
        committed = sum(t.get('position_value',0) for t in c_open)
        if committed+position_value > INITIAL_CAPITAL_CRYPTO*MAX_CAPITAL_PCT_CRYPTO/100:
            return False, 'CRYPTO_CAPITAL_LIMIT', 0
        free_cap = cc; max_pos = MAX_POSITION_CRYPTO
    else:
        free_cap = sc; max_pos = MAX_POSITION_STOCKS

    if sum(1 for t in all_open if t.get('symbol')==symbol) >= MAX_SAME_SYMBOL:
        return False, f'SYMBOL_ALREADY_OPEN ({symbol})', 0
    if time.time()-symbol_cooldown.get(symbol,0) < SYMBOL_COOLDOWN_SEC:
        secs = int(SYMBOL_COOLDOWN_SEC-(time.time()-symbol_cooldown.get(symbol,0)))
        return False, f'SYMBOL_COOLDOWN (+{secs}s)', 0
    if sum(1 for t in all_open if t.get('market')==market_type) >= MAX_POSITION_SAME_MKT:
        return False, f'MAX_POSITION_SAME_MKT ({market_type})', 0
    if position_value > free_cap:
        return False, f'INSUFFICIENT_CAPITAL (free=${free_cap:.0f})', 0

    total_cap = INITIAL_CAPITAL_STOCKS+INITIAL_CAPITAL_CRYPTO
    max_risk  = total_cap*MAX_RISK_PER_TRADE_PCT/100
    approved  = min(position_value, max_risk, max_pos, free_cap)

    # [v10.7-Fix3+Fix6] closed lists com cap MAX_CLOSED_HISTORY=500 → drawdown O(500) no pior caso.
    # 500 entradas cobre >7 dias de trades a 20/dia — janela máxima de drawdown = 7 dias.
    cutoff_d = (datetime.utcnow()-timedelta(days=1)).isoformat()
    daily_loss = sum(t.get('pnl',0) for t in s_closed+c_closed
        if t.get('closed_at','')>=cutoff_d and t.get('pnl',0)<0)
    dd_d = abs(daily_loss)/total_cap*100
    if dd_d >= MAX_DAILY_DRAWDOWN_PCT:
        _trigger_kill_switch(dd_d,'daily'); return False,f'DAILY_DRAWDOWN ({dd_d:.2f}%)',0

    cutoff_w = (datetime.utcnow()-timedelta(days=7)).isoformat()
    weekly_loss = sum(t.get('pnl',0) for t in s_closed+c_closed
        if t.get('closed_at','')>=cutoff_w and t.get('pnl',0)<0)
    dd_w = abs(weekly_loss)/total_cap*100
    if dd_w >= MAX_WEEKLY_DRAWDOWN_PCT:
        _trigger_kill_switch(dd_w,'weekly'); return False,f'WEEKLY_DRAWDOWN ({dd_w:.2f}%)',0

    return True, 'OK', round(approved, 2)

def check_risk_arbi(pair_id, position_value):
    global ARBI_KILL_SWITCH
    if ARBI_KILL_SWITCH: return False, 'ARBI_KILL_SWITCH', 0
    with state_lock:
        open_count=len(arbi_open); cap=arbi_capital; a_closed=list(arbi_closed)
    if open_count >= ARBI_MAX_POSITIONS:
        return False, f'ARBI_MAX_POSITIONS ({open_count}/{ARBI_MAX_POSITIONS})', 0
    if any(t.get('pair_id')==pair_id for t in arbi_open):
        return False, f'ARBI_PAIR_OPEN ({pair_id})', 0
    if position_value > cap:
        return False, f'ARBI_INSUFFICIENT_CAPITAL ({cap:.0f})', 0
    cutoff=(datetime.utcnow()-timedelta(days=1)).isoformat()
    daily_loss=sum(t.get('pnl',0) for t in a_closed
        if t.get('closed_at','')>=cutoff and t.get('pnl',0)<0)
    dd=abs(daily_loss)/ARBI_CAPITAL*100
    if dd>=ARBI_MAX_DAILY_LOSS:
        ARBI_KILL_SWITCH=True; send_whatsapp(f'ARBI KILL SWITCH: drawdown {dd:.2f}%')
        return False, f'ARBI_DAILY_DRAWDOWN ({dd:.2f}%)', 0
    return True, 'OK', round(min(position_value, ARBI_POS_SIZE, cap), 2)

def _trigger_kill_switch(dd_pct, period):
    global RISK_KILL_SWITCH
    RISK_KILL_SWITCH = True
    audit('KILL_SWITCH_ACTIVATED',{'drawdown_pct':round(dd_pct,2),'period':period})
    send_whatsapp(f'KILL SWITCH ATIVADO — drawdown {period}: {dd_pct:.2f}%')

def _second_validation(symbol, market_type, strategy):
    """Segunda validação leve DENTRO do state_lock"""
    global RISK_KILL_SWITCH
    if RISK_KILL_SWITCH: return False, 'KILL_SWITCH'
    all_open = stocks_open+crypto_open
    if len(all_open) >= MAX_OPEN_POSITIONS: return False,'MAX_OPEN_POSITIONS'
    if any(t.get('symbol')==symbol for t in all_open): return False,'SYMBOL_DUPLICATE'
    if time.time()-symbol_cooldown.get(symbol,0)<SYMBOL_COOLDOWN_SEC: return False,'COOLDOWN'
    if sum(1 for t in all_open if t.get('market')==market_type)>=MAX_POSITION_SAME_MKT:
        return False,'MAX_SAME_MKT'
    if strategy=='stocks':
        if len(stocks_open)>=MAX_POSITIONS_STOCKS: return False,'MAX_POSITIONS_STOCKS'
        if stocks_capital<=0: return False,'NO_CAPITAL_STOCKS'
    elif strategy=='crypto':
        if len(crypto_open)>=MAX_POSITIONS_CRYPTO: return False,'MAX_POSITIONS_CRYPTO'
        if crypto_capital<=0: return False,'NO_CAPITAL_CRYPTO'
    return True,'OK'

def alert_signal(signal):
    key=signal.get('symbol',''); now=time.time()
    if now-alerted_signals.get(key,0)<3600: return
    alerted_signals[key]=now
    send_whatsapp(f"Egreja AI | {key} ({signal.get('market_type','')}) Score:{signal.get('score',0)}/100 {signal.get('signal','')} ${signal.get('price',0):,.2f}")

def alert_trade_closed(trade):
    key=trade.get('id','')
    if key in alerted_trades: return
    alerted_trades[key]=True
    pnl=trade.get('pnl',0); result='OK' if pnl>=0 else 'LOSS'
    send_whatsapp(f"Trade {result} | {trade.get('symbol','')} | {trade.get('close_reason','')} | {'+'if pnl>=0 else ''}{pnl:,.2f} ({trade.get('pnl_pct',0):+.2f}%)")

# ═══════════════════════════════════════════════════════════════
# ORDERS
# ═══════════════════════════════════════════════════════════════
def create_order(trade_id, symbol, side, order_type, qty, price, strategy='stocks', notes='',
                 order_id_override=None):
    """[V91-1] Aceita order_id_override para que trade e ordem compartilhem ID pré-gerado."""
    order = {
        'order_id':   order_id_override or gen_id('ORD'), 'trade_id': trade_id,
        'symbol':     symbol, 'side': side, 'order_type': order_type,
        'qty': qty, 'limit_price': price, 'stop_price': None, 'strategy': strategy,
        'status': 'NEW', 'status_history': [{'status':'NEW','ts':datetime.utcnow().isoformat()}],
        'sent_at': None, 'filled_at': None, 'fill_price': None,
        'fill_qty': 0, 'slippage': 0, 'fee': 0, 'notes': notes,
        'created_at': datetime.utcnow().isoformat(), 'updated_at': datetime.utcnow().isoformat(),
    }
    with orders_lock:
        orders_log.append(order)
        if len(orders_log) > 2000: orders_log.pop(0)
    enqueue_persist('order', order)
    return order

def update_order_status(order, new_status, fill_price=None, fill_qty=None):
    """[V91-4] Protegido por orders_lock — evita leitura de estado intermediário."""
    with orders_lock:
        order['status'] = new_status; order['updated_at'] = datetime.utcnow().isoformat()
        order['status_history'].append({'status':new_status,'ts':order['updated_at']})
        if new_status=='SENT': order['sent_at']=order['updated_at']
        if new_status in ('FILLED','PARTIALLY_FILLED') and fill_price:
            order['fill_price']=fill_price; order['fill_qty']=fill_qty or order['qty']
            order['filled_at']=order['updated_at']
            order['slippage']=round(abs(fill_price-order['limit_price'])/order['limit_price']*100,4)
    enqueue_persist('order', order)
    return order

def _db_save_order(order):
    conn=get_db()
    if not conn: return
    try:
        cursor=conn.cursor()
        # [V91-2] status_history_json persiste a trilha completa da máquina de estados
        status_history_json = json.dumps(order.get('status_history', []))
        cursor.execute("""INSERT INTO orders (
            order_id,trade_id,symbol,side,order_type,qty,limit_price,stop_price,
            strategy,status,fill_price,fill_qty,slippage,fee,notes,
            sent_at,filled_at,created_at,updated_at,status_history_json)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE status=VALUES(status),fill_price=VALUES(fill_price),
            fill_qty=VALUES(fill_qty),slippage=VALUES(slippage),
            filled_at=VALUES(filled_at),updated_at=VALUES(updated_at),
            status_history_json=VALUES(status_history_json)""",
            (order.get('order_id'),order.get('trade_id'),order.get('symbol'),
             order.get('side'),order.get('order_type'),order.get('qty'),
             order.get('limit_price'),order.get('stop_price'),order.get('strategy'),
             order.get('status'),order.get('fill_price'),order.get('fill_qty'),
             order.get('slippage',0),order.get('fee',0),order.get('notes',''),
             order.get('sent_at'),order.get('filled_at'),
             order.get('created_at'),order.get('updated_at'),
             status_history_json))
        conn.commit(); cursor.close(); conn.close()
    except Exception as e: log.error(f'db_save_order: {e}')

# ═══════════════════════════════════════════════════════════════
# PORTFOLIO SNAPSHOT
# ═══════════════════════════════════════════════════════════════
def take_portfolio_snapshot():
    with state_lock:
        snap = {
            'timestamp':        datetime.utcnow().isoformat(),
            'stocks_capital':   round(stocks_capital,2),
            'crypto_capital':   round(crypto_capital,2),
            'arbi_capital':     round(arbi_capital,2),
            'stocks_open_pnl':  round(sum(t.get('pnl',0) for t in stocks_open),2),
            'crypto_open_pnl':  round(sum(t.get('pnl',0) for t in crypto_open),2),
            'arbi_open_pnl':    round(sum(t.get('pnl',0) for t in arbi_open),2),
            'total_open_pnl':   round(sum(t.get('pnl',0) for t in stocks_open+crypto_open+arbi_open),2),
            'open_positions':   len(stocks_open)+len(crypto_open),
            'arbi_positions':   len(arbi_open),
            'kill_switch':      int(RISK_KILL_SWITCH),
            'arbi_kill_switch': int(ARBI_KILL_SWITCH),
            'market_regime':    market_regime.get('mode','UNKNOWN'),
        }
    enqueue_persist('snapshot', snap)

def _db_save_snapshot(snap):
    conn=get_db()
    if not conn: return
    try:
        cursor=conn.cursor()
        cursor.execute("""INSERT INTO portfolio_snapshots (
            ts,stocks_capital,crypto_capital,arbi_capital,
            stocks_open_pnl,crypto_open_pnl,arbi_open_pnl,total_open_pnl,
            open_positions,arbi_positions,kill_switch,arbi_kill_switch,market_regime)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (snap['timestamp'],snap['stocks_capital'],snap['crypto_capital'],snap['arbi_capital'],
             snap['stocks_open_pnl'],snap['crypto_open_pnl'],snap['arbi_open_pnl'],
             snap['total_open_pnl'],snap['open_positions'],snap['arbi_positions'],
             snap['kill_switch'],snap['arbi_kill_switch'],snap['market_regime']))
        conn.commit(); cursor.close(); conn.close()
    except Exception as e: log.error(f'db_save_snapshot: {e}')

def snapshot_loop():
    while True:
        beat('snapshot_loop')
        time.sleep(300)
        beat('snapshot_loop')
        try:
            take_portfolio_snapshot()
        except Exception as e: log.error(f'snapshot_loop: {e}')

# ═══════════════════════════════════════════════════════════════
# QUALIDADE DO DADO
# ═══════════════════════════════════════════════════════════════
def record_data_quality(symbol, source, latency_ms, price_valid):
    score = 100
    if not price_valid:     score -= 50
    if latency_ms > 30_000: score -= 20
    if latency_ms > 60_000: score -= 30
    with dq_lock:
        data_quality[symbol] = {
            'symbol': symbol, 'source': source,
            'fetch_at': datetime.utcnow().isoformat(),
            'latency_ms': round(latency_ms,1),
            'quality': max(0,score), 'stale': latency_ms>60_000,
            'price_valid': price_valid,
        }

# ═══════════════════════════════════════════════════════════════
# [L-1] FEATURE ENGINEERING — extração determinística e bucketing
# ═══════════════════════════════════════════════════════════════

def _score_bucket(score: float) -> str:
    if score <= 29:   return 'VERY_LOW'
    if score <= 49:   return 'LOW'
    if score <= 69:   return 'NEUTRAL'
    if score <= 84:   return 'HIGH'
    return 'VERY_HIGH'

def _rsi_bucket(rsi: float) -> str:
    if rsi < 30:    return 'OVERSOLD'
    if rsi < 45:    return 'WEAK'
    if rsi < 55:    return 'NEUTRAL'
    if rsi < 70:    return 'STRONG'
    return 'OVERBOUGHT'

def _ema_alignment(ema9: float, ema21: float, ema50: float, price: float) -> str:
    """Alinhamento das EMAs em relação ao preço e entre si."""
    if price > ema9 > ema21 > ema50:  return 'BULLISH_STACK'
    if price < ema9 < ema21 < ema50:  return 'BEARISH_STACK'
    if ema9 > ema21:                   return 'BULLISH_CROSS'
    if ema9 < ema21:                   return 'BEARISH_CROSS'
    return 'MIXED'

def _change_pct_bucket(change_pct: float) -> str:
    a = abs(change_pct)
    if a < 0.5:   return 'FLAT'
    if a < 1.5:   return 'SMALL'
    if a < 3.0:   return 'MEDIUM'
    if a < 6.0:   return 'LARGE'
    return 'EXTREME'

def _volatility_bucket(regime_volatility: str) -> str:
    return regime_volatility or 'NORMAL'

def _time_bucket(dt: datetime) -> str:
    h = dt.hour
    if h < 6:    return 'OVERNIGHT'
    if h < 10:   return 'PRE_MARKET'
    if h < 12:   return 'MORNING'
    if h < 14:   return 'MIDDAY'
    if h < 17:   return 'AFTERNOON'
    if h < 20:   return 'EVENING'
    return 'NIGHT'

def _data_quality_bucket(dq_score: float) -> str:
    if dq_score >= 90: return 'HIGH'
    if dq_score >= 60: return 'MEDIUM'
    return 'LOW'

def _atr_bucket(atr_pct: float) -> str:
    """[v10.4] ATR como % do preço — volatility real, não só regime de crypto."""
    if atr_pct <= 0:    return 'UNKNOWN'
    if atr_pct < 0.5:   return 'VERY_LOW'
    if atr_pct < 1.5:   return 'LOW'
    if atr_pct < 3.0:   return 'NORMAL'
    if atr_pct < 6.0:   return 'HIGH'
    return 'EXTREME'

def _volume_bucket(vol_ratio: float) -> str:
    """[v10.4] Ratio volume_atual / volume_médio_20d.
    >1.5 = volume acima da média (confirma movimento); <0.7 = volume fraco."""
    if vol_ratio <= 0:   return 'UNKNOWN'
    if vol_ratio < 0.5:  return 'VERY_LOW'
    if vol_ratio < 0.8:  return 'LOW'
    if vol_ratio < 1.3:  return 'NORMAL'
    if vol_ratio < 2.0:  return 'HIGH'
    return 'SURGE'

def _calc_atr(closes: list, highs: list = None, lows: list = None, period: int = 14) -> float:
    """[v10.4] ATR simplificado. Se highs/lows não disponíveis, usa desvio de closes."""
    if len(closes) < 2: return 0.0
    if highs and lows and len(highs) == len(closes):
        trs = []
        for i in range(1, min(period + 1, len(closes))):
            hl = highs[i] - lows[i]
            hc = abs(highs[i] - closes[i-1])
            lc = abs(lows[i] - closes[i-1])
            trs.append(max(hl, hc, lc))
        return sum(trs) / len(trs) if trs else 0.0
    # Fallback: desvio médio absoluto dos closes
    n = min(period, len(closes))
    diffs = [abs(closes[i] - closes[i-1]) for i in range(1, n + 1)]
    return sum(diffs) / len(diffs) if diffs else 0.0

def extract_features(sig: dict, regime: dict, dq_score: float, now: datetime) -> dict:
    """[L-1][v10.4] Extrai features canônicas de um sinal para learning.
    Inclui atr_bucket e volume_bucket para espaço de padrões mais discriminativo.
    """
    score     = float(sig.get('score', 50) or 50)
    rsi       = float(sig.get('rsi', 50) or 50)
    ema9      = float(sig.get('ema9', 0) or 0)
    ema21     = float(sig.get('ema21', 0) or 0)
    ema50     = float(sig.get('ema50', 0) or 0)
    price     = float(sig.get('price', 0) or 0)
    change    = float(sig.get('change_pct', sig.get('change_24h', 0)) or 0)
    direction = 'LONG' if sig.get('signal') == 'COMPRA' else ('SHORT' if sig.get('signal') == 'VENDA' else 'NEUTRAL')
    asset_t   = sig.get('asset_type', 'stock')
    mkt       = sig.get('market_type', 'NYSE')

    # [v10.4] ATR e volume — vindos do price_dict ou do sig_enriched
    atr_pct    = float(sig.get('atr_pct', 0) or 0)
    vol_ratio  = float(sig.get('volume_ratio', 0) or 0)

    return {
        'score_bucket':     _score_bucket(score),
        'rsi_bucket':       _rsi_bucket(rsi),
        'ema_alignment':    _ema_alignment(ema9, ema21, ema50, price),
        'change_pct_bucket':_change_pct_bucket(change),
        'volatility_bucket':_volatility_bucket(regime.get('volatility', 'NORMAL')),
        'regime_mode':      regime.get('mode', 'UNKNOWN'),
        'time_bucket':      _time_bucket(now),
        'weekday':          now.weekday(),   # 0=segunda
        'asset_type':       asset_t,
        'market_type':      mkt,
        'direction':        direction,
        'dq_bucket':        _data_quality_bucket(dq_score),
        'atr_bucket':       _atr_bucket(atr_pct),       # [v10.4] volatility real por ativo
        'volume_bucket':    _volume_bucket(vol_ratio),  # [v10.4] confirmação por volume
    }

def make_feature_hash(features: dict) -> str:
    """[L-1][v10.4] Hash canônico determinístico — espaço ampliado com atr, volume e weekday.
    weekday distingue comportamento segunda-feira (gap open) de quarta/quinta (fluxo normal).
    """
    canonical = '|'.join([
        features.get('score_bucket', ''),
        features.get('rsi_bucket', ''),
        features.get('ema_alignment', ''),
        features.get('volatility_bucket', ''),
        features.get('regime_mode', ''),
        features.get('time_bucket', ''),
        features.get('asset_type', ''),
        features.get('direction', ''),
        features.get('atr_bucket', ''),       # [v10.4]
        features.get('volume_bucket', ''),    # [v10.4]
        str(features.get('weekday', '')),     # [v10.4]
    ])
    return hashlib.md5(canonical.encode()).hexdigest()[:16]

def get_dq_score(symbol: str) -> float:
    """Retorna data quality score do símbolo ou 50 se desconhecido."""
    with dq_lock:
        dq = data_quality.get(symbol.upper(), {})
    return float(dq.get('quality', 50))

# ═══════════════════════════════════════════════════════════════
# [L-3/L-4] PATTERN & FACTOR STATS — estruturas e helpers
# ═══════════════════════════════════════════════════════════════

def _empty_pattern_stats(feature_hash: str) -> dict:
    return {
        'feature_hash': feature_hash,
        'total_samples': 0, 'wins': 0, 'losses': 0, 'flat_count': 0,
        'avg_pnl': 0.0, 'avg_pnl_pct': 0.0,
        'ewma_pnl_pct': 0.0, 'ewma_hit_rate': 0.5,
        'expectancy': 0.0, 'downside_score': 0.0,
        'max_loss_seen': 0.0, 'confidence_weight': 0.0,
        'last_seen_at': '', 'updated_at': '',
    }

def _empty_factor_stats(factor_type: str, factor_value: str) -> dict:
    return {
        'factor_type': factor_type, 'factor_value': factor_value,
        'total_samples': 0, 'wins': 0, 'losses': 0,
        'avg_pnl_pct': 0.0, 'ewma_pnl_pct': 0.0,
        'expectancy': 0.0, 'downside_score': 0.0,
        'confidence_weight': 0.0,
        'last_seen_at': '', 'updated_at': '',
    }

def _update_ewma(current: float, new_value: float, alpha: float) -> float:
    return alpha * new_value + (1 - alpha) * current

def _calc_confidence_weight(total_samples: int, ewma_hit_rate: float,
                             expectancy: float, downside_score: float) -> float:
    """[L-3] Peso de confiança: aumenta com amostras, penaliza downside."""
    # Fator de amostras: sobe suavemente até N>=30
    sample_factor = min(total_samples / max(LEARNING_MIN_SAMPLES * 3, 30), 1.0)
    # Fator de hit_rate normalizado (0.5 = neutro)
    hit_factor    = max(0.0, (ewma_hit_rate - 0.5) * 2)
    # Fator de expectancy (normalizado para [-1, 1])
    exp_factor    = max(-1.0, min(1.0, expectancy / 3.0))
    # Penalidade de downside
    down_penalty  = min(downside_score / 5.0, 1.0)

    raw = sample_factor * (0.4 + 0.3 * hit_factor + 0.3 * exp_factor) - 0.2 * down_penalty
    return max(-1.0, min(1.0, round(raw, 4)))

def update_pattern_stats(feature_hash: str, pnl: float, pnl_pct: float) -> dict:
    """[L-3] Atualiza pattern_stats em memória de forma incremental."""
    global last_learning_update
    alpha = LEARNING_EWMA_ALPHA
    now_s = datetime.utcnow().isoformat()
    with learning_lock:
        s = pattern_stats_cache.get(feature_hash) or _empty_pattern_stats(feature_hash)
        s['total_samples'] += 1
        if pnl_pct > 0.1:    s['wins'] += 1
        elif pnl_pct < -0.1: s['losses'] += 1
        else:                  s['flat_count'] += 1

        n = s['total_samples']
        # Média simples incremental (Welford)
        s['avg_pnl']     += (pnl - s['avg_pnl']) / n
        s['avg_pnl_pct'] += (pnl_pct - s['avg_pnl_pct']) / n
        # EWMA para recência
        s['ewma_pnl_pct']  = _update_ewma(s['ewma_pnl_pct'],  pnl_pct, alpha)
        hit = 1.0 if pnl_pct > 0.1 else 0.0
        s['ewma_hit_rate'] = _update_ewma(s['ewma_hit_rate'], hit, alpha)
        # Expectancy = win_rate * avg_win - loss_rate * avg_loss (simplificado)
        wins   = s['wins']; losses = s['losses']
        s['expectancy'] = round(s['ewma_hit_rate'] * max(s['avg_pnl_pct'], 0)
                                - (1 - s['ewma_hit_rate']) * abs(min(s['avg_pnl_pct'], 0)), 4)
        # Downside: frequência de perdas grandes
        if pnl_pct < s['max_loss_seen']: s['max_loss_seen'] = round(pnl_pct, 4)
        loss_rate = losses / n if n > 0 else 0
        s['downside_score'] = round(loss_rate * abs(min(s['avg_pnl_pct'], 0)) * 10, 4)
        s['confidence_weight'] = _calc_confidence_weight(
            n, s['ewma_hit_rate'], s['expectancy'], s['downside_score'])
        s['last_seen_at'] = now_s; s['updated_at'] = now_s
        pattern_stats_cache[feature_hash] = s
        last_learning_update = now_s
    return dict(s)

def update_factor_stats(features: dict, pnl: float, pnl_pct: float):
    """[L-4] Atualiza factor_stats incrementalmente para cada fator do sinal."""
    alpha   = LEARNING_EWMA_ALPHA
    now_s   = datetime.utcnow().isoformat()
    factors = [
        ('score_bucket',      features.get('score_bucket', '')),
        ('rsi_bucket',        features.get('rsi_bucket', '')),
        ('ema_alignment',     features.get('ema_alignment', '')),
        ('volatility_bucket', features.get('volatility_bucket', '')),
        ('regime_mode',       features.get('regime_mode', '')),
        ('time_bucket',       features.get('time_bucket', '')),
        ('weekday',           str(features.get('weekday', ''))),
        ('asset_type',        features.get('asset_type', '')),
        ('market_type',       features.get('market_type', '')),
        ('direction',         features.get('direction', '')),
        ('dq_bucket',         features.get('dq_bucket', '')),
        ('atr_bucket',        features.get('atr_bucket', '')),      # [v10.4]
        ('volume_bucket',     features.get('volume_bucket', '')),   # [v10.4]
    ]
    with learning_lock:
        for ftype, fval in factors:
            if not fval: continue
            key = (ftype, fval)
            s   = factor_stats_cache.get(key) or _empty_factor_stats(ftype, fval)
            s['total_samples'] += 1
            n = s['total_samples']
            if pnl_pct > 0.1:    s['wins'] += 1
            elif pnl_pct < -0.1: s['losses'] += 1
            s['avg_pnl_pct']   += (pnl_pct - s['avg_pnl_pct']) / n
            s['ewma_pnl_pct']   = _update_ewma(s['ewma_pnl_pct'], pnl_pct, alpha)
            hit = 1.0 if pnl_pct > 0.1 else 0.0
            hit_rate = _update_ewma(s.get('_ewma_hit', 0.5), hit, alpha)
            s['_ewma_hit'] = hit_rate
            s['expectancy'] = round(hit_rate * max(s['avg_pnl_pct'], 0)
                                    - (1 - hit_rate) * abs(min(s['avg_pnl_pct'], 0)), 4)
            loss_rate = s['losses'] / n if n > 0 else 0
            s['downside_score'] = round(loss_rate * abs(min(s['avg_pnl_pct'], 0)) * 10, 4)
            s['confidence_weight'] = _calc_confidence_weight(
                n, hit_rate, s['expectancy'], s['downside_score'])
            s['last_seen_at'] = now_s; s['updated_at'] = now_s
            factor_stats_cache[key] = s

# ═══════════════════════════════════════════════════════════════
# [L-5] CONFIDENCE ENGINE — aprendizado explicável
# ═══════════════════════════════════════════════════════════════

def calc_learning_confidence(sig: dict, features: dict, feature_hash: str) -> dict:
    """
    [L-5][P0-2] Calcula learning_confidence para um sinal.
    Retorna dict com breakdown completo — nada de caixa-preta.

    IMPORTANTE: base normaliza pela FORÇA do sinal, não pelo valor bruto.
    Score 85 (compra forte) e score 15 (venda forte) têm mesma força base = 0.70.
    Evita viés estrutural contra shorts.
    """
    if not LEARNING_ENABLED:
        direction = features.get('direction', 'LONG') if features else 'LONG'
        return _neutral_confidence(sig.get('score', 50), direction)

    raw_score   = float(sig.get('score', 50) or 50)
    dq_score    = float(features.get('_dq_score', 50))
    regime_mode = features.get('regime_mode', 'UNKNOWN')
    direction   = features.get('direction', 'LONG')

    # ── [P0-2] Base: força relativa ao lado do sinal ──────────────
    # score 50 = neutro → força 0; score 100 ou 0 → força máxima
    # LONG:  score alto é bom   (ex. 85 → força = (85-50)/50 = 0.70)
    # SHORT: score baixo é bom  (ex. 15 → força = (50-15)/50 = 0.70)
    if direction == 'SHORT':
        signal_strength = (50 - raw_score) / 50.0   # scores baixos = curto forte
    else:
        signal_strength = (raw_score - 50) / 50.0   # scores altos = longo forte
    # Normalizar para [0, 1] — força negativa tratada como neutro (50%)
    base = max(0.0, min(1.0, 0.5 + signal_strength * 0.5))

    # ── Histórico do padrão ───────────────────────────────────
    with learning_lock:
        ps = dict(pattern_stats_cache.get(feature_hash, {}))
    p_samples = ps.get('total_samples', 0)
    p_cw      = ps.get('confidence_weight', 0.0)
    p_exp     = ps.get('expectancy', 0.0)

    # Shrinkage: peso do padrão cresce com amostras
    p_weight  = min(p_samples / max(LEARNING_MIN_SAMPLES * 3, 30), 1.0)
    pattern_score = 0.5 + 0.5 * p_cw   # mapeia [-1,1] → [0,1]

    # ── Fatores individuais ───────────────────────────────────
    relevant = ['score_bucket','rsi_bucket','ema_alignment','regime_mode','direction']
    factor_scores = []
    with learning_lock:
        for ftype in relevant:
            fval = features.get(ftype, '')
            if not fval: continue
            fs = factor_stats_cache.get((ftype, fval), {})
            if fs.get('total_samples', 0) >= 5:
                factor_scores.append(0.5 + 0.5 * fs.get('confidence_weight', 0.0))
    factor_score = (sum(factor_scores) / len(factor_scores)) if factor_scores else 0.5

    # ── Ajuste de qualidade do dado ───────────────────────────
    dq_adj = (dq_score / 100.0 - 0.5) * 0.2   # ±0.1 no máximo

    # ── Ajuste de regime ─────────────────────────────────────
    regime_adj = 0.0
    if regime_mode == 'HIGH_VOL':   regime_adj = -0.08
    elif regime_mode == 'TRENDING': regime_adj =  0.04

    # ── Penalidade por amostra pequena ───────────────────────
    sample_penalty = max(0.0, 0.15 * (1 - p_weight))

    # ── Composição final ─────────────────────────────────────
    # Peso: base 40%, padrão 30% (ajustado por shrinkage), fatores 20%, ajustes 10%
    if p_weight > 0:
        blended = (0.40 * base
                   + 0.30 * (p_weight * pattern_score + (1 - p_weight) * base)
                   + 0.20 * factor_score
                   + 0.10 * base)    # fallback parcial
    else:
        blended = 0.65 * base + 0.35 * factor_score

    final_raw   = blended + dq_adj + regime_adj - sample_penalty
    final_conf  = max(0.0, min(1.0, final_raw))
    final_score = round(final_conf * 100, 1)

    band = ('HIGH'   if final_score >= 65 else
            'MEDIUM' if final_score >= 40 else 'LOW')

    return {
        'final_confidence': final_score,
        'confidence_band':  band,
        'base_score':       round(base * 100, 1),
        'pattern_score':    round(pattern_score * 100, 1) if p_weight > 0 else None,
        'pattern_samples':  p_samples,
        'factor_score':     round(factor_score * 100, 1),
        'data_quality_adj': round(dq_adj * 100, 1),
        'regime_adj':       round(regime_adj * 100, 1),
        'sample_penalty':   round(sample_penalty * 100, 1),
        'feature_hash':     feature_hash,
    }

def _neutral_confidence(raw_score: float, direction: str = 'LONG') -> dict:
    """[P0-2][P6] Fallback — normaliza pela força do lado do sinal.
    Banda é calculada dinamicamente (não fixo MEDIUM).
    """
    if direction == 'SHORT':
        strength = max(0.0, (50 - raw_score) / 50.0)
    else:
        strength = max(0.0, (raw_score - 50) / 50.0)
    final = round(50 + strength * 50, 1)
    # [P6] Banda dinâmica: não travar em MEDIUM quando confiança for alta/baixa
    if   final >= 70: band = 'HIGH'
    elif final <= 40: band = 'LOW'
    else:             band = 'MEDIUM'
    return {
        'final_confidence': final,
        'confidence_band':  band,
        'base_score':       round(final, 1),
        'pattern_score':    None, 'pattern_samples': 0,
        'factor_score':     50.0, 'data_quality_adj': 0.0,
        'regime_adj':       0.0,  'sample_penalty':   0.0,
        'feature_hash':     '',
    }

# ═══════════════════════════════════════════════════════════════
# [L-6] INSIGHT ENGINE — explicação humano-legível
# ═══════════════════════════════════════════════════════════════

def generate_insight(sig: dict, features: dict, feature_hash: str, conf: dict) -> str:
    """[L-6] Gera insight_summary explicável para o sinal."""
    if not LEARNING_ENABLED or LEARNING_DEGRADED:
        return f"Score bruto: {sig.get('score', 50)}/100. Learning indisponível."

    parts = []
    band   = conf.get('confidence_band', 'MEDIUM')
    fc     = conf.get('final_confidence', 50)
    p_samp = conf.get('pattern_samples', 0)

    # Avaliação geral
    if band == 'HIGH':
        parts.append(f"Alta confiança ({fc:.0f}/100)")
    elif band == 'MEDIUM':
        parts.append(f"Confiança média ({fc:.0f}/100)")
    else:
        parts.append(f"Baixa confiança ({fc:.0f}/100)")

    # Histórico do padrão
    with learning_lock:
        ps = dict(pattern_stats_cache.get(feature_hash, {}))
    if p_samp >= LEARNING_MIN_SAMPLES:
        wr    = round(ps.get('ewma_hit_rate', 0.5) * 100)
        exp   = ps.get('expectancy', 0.0)
        sign  = "positiva" if exp >= 0 else "negativa"
        parts.append(f"padrão semelhante teve win rate de {wr}% em {p_samp} amostras (expectancy {sign})")
    elif p_samp > 0:
        parts.append(f"baixa amostra ({p_samp} trades); confiança reduzida apesar do score")
    else:
        parts.append("sem histórico para este padrão ainda")

    # Fatores positivos e negativos
    regime = features.get('regime_mode', '')
    if regime == 'HIGH_VOL':
        parts.append("atenção: regime HIGH_VOL reduz confiança histórica")
    elif regime == 'TRENDING':
        parts.append("regime TRENDING favorável para este setup")

    dq_adj = conf.get('data_quality_adj', 0)
    if dq_adj < -5:
        parts.append("qualidade do dado fraca — sinal com cautela")

    ema = features.get('ema_alignment', '')
    if ema in ('BULLISH_STACK', 'BEARISH_STACK'):
        with learning_lock:
            ema_fs = factor_stats_cache.get(('ema_alignment', ema), {})
        if ema_fs.get('total_samples', 0) >= 5 and ema_fs.get('confidence_weight', 0) > 0.1:
            parts.append(f"{ema} historicamente favorável neste contexto")

    sp = conf.get('sample_penalty', 0)
    if sp > 8:
        parts.append(f"penalização por amostra insuficiente ({sp:.0f}pts)")

    return '. '.join(parts) + '.'

def get_risk_multiplier(conf: dict) -> float:
    """[L-9] Multiplica size do position de forma conservadora."""
    band = conf.get('confidence_band', 'MEDIUM')
    if band == 'HIGH':   mult = RISK_MULT_MAX
    elif band == 'LOW':  mult = RISK_MULT_MIN
    else:                mult = 1.0
    return round(max(RISK_MULT_MIN, min(RISK_MULT_MAX, mult)), 3)

def get_top_factors(n_best: int = 5, n_worst: int = 5) -> dict:
    """[L-6] Retorna fatores com melhor e pior performance histórica."""
    with learning_lock:
        items = [(k, dict(v)) for k, v in factor_stats_cache.items()
                 if v.get('total_samples', 0) >= LEARNING_MIN_SAMPLES]
    items.sort(key=lambda x: x[1].get('confidence_weight', 0), reverse=True)
    def _fmt(entry):
        k, v = entry
        return {'factor_type': k[0], 'factor_value': k[1],
                'samples': v['total_samples'], 'cw': round(v['confidence_weight'], 3),
                'expectancy': round(v.get('expectancy', 0), 4),
                'ewma_pnl_pct': round(v.get('ewma_pnl_pct', 0), 4)}
    return {
        'top_positive': [_fmt(i) for i in items[:n_best]],
        'top_negative': [_fmt(i) for i in reversed(items[-n_worst:]) if items],
    }

# ═══════════════════════════════════════════════════════════════
# [L-2] SIGNAL MEMORY — snapshot de cada sinal no DB
# ═══════════════════════════════════════════════════════════════

def record_signal_event(sig: dict, features: dict, feature_hash: str,
                         conf: dict, insight: str,
                         trade_id: str = None, order_id: str = None,
                         source_type: str = 'stock_signal_db',
                         existing_signal_id: str = None,
                         origin_signal_key: str = None) -> str:
    """[L-2][FIX-2][S2] Registra evento de sinal. Retorna signal_id.
    origin_signal_key: chave de origem do registro em market_signals (para dedup persistida).
    Se existing_signal_id for passado, faz UPDATE ao invés de INSERT.
    """
    global signal_events_count, LEARNING_DEGRADED
    if not LEARNING_ENABLED: return ''
    try:
        signal_id  = existing_signal_id or gen_id('SIG')
        dq_score   = features.get('_dq_score', 50)
        payload    = {k: v for k, v in sig.items()
                      if k not in ('payload_json',) and not isinstance(v, (list, dict))}
        payload.update(features)

        event = {
            'signal_id':               signal_id,
            'feature_hash':            feature_hash,
            'symbol':                  sig.get('symbol', ''),
            'asset_type':              sig.get('asset_type', 'stock'),
            'market_type':             sig.get('market_type', ''),
            'signal':                  sig.get('signal', ''),
            'raw_score':               float(sig.get('score', 50) or 50),
            'learning_confidence':     conf.get('final_confidence', 50),
            'confidence_band':         conf.get('confidence_band', 'MEDIUM'),
            'price':                   float(sig.get('price', 0) or 0),
            'signal_created_at':       datetime.utcnow().isoformat(),
            'market_regime_mode':      features.get('regime_mode', ''),
            'market_regime_volatility':features.get('volatility_bucket', ''),
            'market_open':             bool(sig.get('market_open', False)),
            'trade_open':              bool(sig.get('trade_open', False)),
            'rsi':                     float(sig.get('rsi', 50) or 50),
            'ema9':                    float(sig.get('ema9', 0) or 0),
            'ema21':                   float(sig.get('ema21', 0) or 0),
            'ema50':                   float(sig.get('ema50', 0) or 0),
            'rsi_bucket':              features.get('rsi_bucket', ''),
            'score_bucket':            features.get('score_bucket', ''),
            'change_pct_bucket':       features.get('change_pct_bucket', ''),
            'ema_alignment':           features.get('ema_alignment', ''),
            'volatility_bucket':       features.get('volatility_bucket', ''),
            'weekday':                 features.get('weekday', 0),
            'time_bucket':             features.get('time_bucket', ''),
            'data_quality_score':      dq_score,
            'source_type':             source_type,
            'payload_json':            json.dumps(payload, default=str),
            'insight_summary':         insight,
            'learning_version':        LEARNING_VERSION,
            'origin_signal_key':       origin_signal_key,   # [S2] chave de dedup persistida
            'trade_id':                trade_id,
            'order_id':                order_id,
            'outcome_status':          None,
            'outcome_pnl':             None,
            'outcome_pnl_pct':         None,
            'outcome_close_reason':    None,
            'updated_at':              datetime.utcnow().isoformat(),
        }
        # [v10.3.2-P0-1] Se for reavaliação (existing_signal_id vem do cache), gravar SÍNCRONO
        # para garantir que o signal_id retornado é o real do banco — usado em seguida pelo
        # update_signal_attribution() para vincular trade_id/order_id corretamente.
        if existing_signal_id:
            confirmed_id = _db_save_signal_event(event)
            LEARNING_DEGRADED = False
            if not confirmed_id:
                # [v10.3.3-F4] Banco falhou ou oscilou — logar explicitamente.
                # Retornar existing_signal_id (o tentado) para não deixar trade sem referência,
                # mas sinalizar que a atribuição pode estar inconsistente.
                log.warning(f'record_signal_event: banco não confirmou signal_id {existing_signal_id} '
                            f'(origin_key={origin_signal_key}). Atribuição pode estar inconsistente.')
                return existing_signal_id
            return confirmed_id
        enqueue_persist('signal_event', event)
        # [P0-3] Não incrementar aqui — o contador é gerenciado por _db_save_signal_event
        # via ROW_COUNT, que distingue insert real de ON DUPLICATE KEY UPDATE.
        LEARNING_DEGRADED = False
        return signal_id
    except Exception as e:
        log.error(f'record_signal_event: {e}')
        return ''

def update_signal_attribution(signal_id: str, trade_id: str, order_id: str):
    """[FIX-2] Vincula trade_id/order_id ao signal_event existente imediatamente após abertura."""
    if not LEARNING_ENABLED or not signal_id: return
    try:
        update = {
            'signal_id': signal_id,
            'trade_id':  trade_id,
            'order_id':  order_id,
            'updated_at':datetime.utcnow().isoformat(),
        }
        enqueue_persist('signal_attribution', update)
    except Exception as e:
        log.error(f'update_signal_attribution: {e}')

def update_signal_outcome(signal_id: str, trade_id: str, order_id: str,
                           pnl: float, pnl_pct: float, close_reason: str):
    """[L-7] Vincula outcome de trade ao evento de sinal original."""
    if not LEARNING_ENABLED or not signal_id: return
    try:
        update = {
            'signal_id':           signal_id,
            'trade_id':            trade_id,
            'order_id':            order_id,
            'outcome_status':      'WIN' if pnl_pct > 0.1 else ('LOSS' if pnl_pct < -0.1 else 'FLAT'),
            'outcome_pnl':         round(pnl, 4),
            'outcome_pnl_pct':     round(pnl_pct, 4),
            'outcome_close_reason':close_reason,
            'updated_at':          datetime.utcnow().isoformat(),
        }
        enqueue_persist('signal_outcome', update)
    except Exception as e:
        log.error(f'update_signal_outcome: {e}')

# ═══════════════════════════════════════════════════════════════
# [L-8] SHADOW LEARNING — decisões hipotéticas
# ═══════════════════════════════════════════════════════════════

def record_shadow_decision(signal_id: str, sig: dict, reason: str):
    """[L-8] Registra sinal observado mas não executado."""
    if not LEARNING_ENABLED: return
    try:
        shadow = {
            'shadow_id':         gen_id('SHD'),
            'signal_id':         signal_id,
            'symbol':            sig.get('symbol', ''),
            'signal':            sig.get('signal', ''),
            'price_at_signal':   float(sig.get('price', 0) or 0),
            'not_executed_reason':reason,
            'hypothetical_entry':float(sig.get('price', 0) or 0),
            'evaluation_status': 'PENDING',
            'created_at':        datetime.utcnow().isoformat(),
            'payload_json':      json.dumps({'score': sig.get('score'), 'reason': reason}, default=str),
        }
        enqueue_persist('shadow_decision', shadow)
    except Exception as e:
        log.error(f'record_shadow_decision: {e}')

# ═══════════════════════════════════════════════════════════════
# [L-2/L-3] PERSIST HELPERS para learning tables
# ═══════════════════════════════════════════════════════════════

def _db_save_signal_event(event: dict):
    """[L-2][P0-3] Persiste signal_event.
    ROW_COUNT() = 1 → insert real → incrementa signal_events_count.
    ROW_COUNT() = 2 → ON DUPLICATE KEY UPDATE → não incrementa (já contado).
    """
    global signal_events_count
    conn = get_db()
    if not conn: return
    try:
        c = conn.cursor()
        c.execute("""INSERT INTO signal_events (
            signal_id, feature_hash, symbol, asset_type, market_type, `signal`, raw_score,
            learning_confidence, confidence_band, price, signal_created_at,
            market_regime_mode, market_regime_volatility, market_open, trade_open,
            rsi, ema9, ema21, ema50, rsi_bucket, score_bucket, change_pct_bucket,
            ema_alignment, volatility_bucket, weekday, time_bucket, data_quality_score,
            source_type, payload_json, insight_summary, learning_version,
            origin_signal_key,
            trade_id, order_id, outcome_status, outcome_pnl, outcome_pnl_pct,
            outcome_close_reason, updated_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
                trade_id=COALESCE(VALUES(trade_id), trade_id),
                order_id=COALESCE(VALUES(order_id), order_id),
                learning_confidence=VALUES(learning_confidence),
                confidence_band=VALUES(confidence_band),
                insight_summary=VALUES(insight_summary),
                payload_json=VALUES(payload_json),
                updated_at=VALUES(updated_at)""",
            (event['signal_id'], event['feature_hash'], event['symbol'],
             event['asset_type'], event['market_type'], event['signal'],
             event['raw_score'], event['learning_confidence'], event['confidence_band'],
             event['price'], event['signal_created_at'], event['market_regime_mode'],
             event['market_regime_volatility'], event['market_open'], event['trade_open'],
             event['rsi'], event['ema9'], event['ema21'], event['ema50'],
             event['rsi_bucket'], event['score_bucket'], event['change_pct_bucket'],
             event['ema_alignment'], event['volatility_bucket'], event['weekday'],
             event['time_bucket'], event['data_quality_score'], event['source_type'],
             event['payload_json'], event['insight_summary'], event['learning_version'],
             event.get('origin_signal_key'),
             event['trade_id'], event['order_id'], event['outcome_status'],
             event['outcome_pnl'], event['outcome_pnl_pct'],
             event['outcome_close_reason'], event['updated_at']))
        # [P0-3] ROW_COUNT=1 → insert real; ROW_COUNT=2 → duplicate key update (signal_id PK)
        # ROW_COUNT=0 pode acontecer quando o ON DUPLICATE KEY não altera nenhuma coluna (valores iguais)
        row_count = c.rowcount
        # [v10.3.2-P0-1] Se houve conflito por origin_signal_key (UNIQUE), o sinal_id real
        # pode ser diferente do que foi tentado inserir. Buscar o ID real do banco.
        real_signal_id = event['signal_id']
        # [v10.3.3-F1] cursor com dictionary=True para acessar row['signal_id'] sem KeyError/TypeError
        if row_count != 1 and event.get('origin_signal_key'):
            try:
                c2 = conn.cursor(dictionary=True)
                c2.execute("SELECT signal_id FROM signal_events WHERE origin_signal_key=%s LIMIT 1",
                           (event['origin_signal_key'],))
                row = c2.fetchone()
                c2.close()
                if row:
                    real_signal_id = row['signal_id']
            except Exception: pass
        conn.commit(); c.close(); conn.close()
        if row_count == 1:
            with learning_lock:
                signal_events_count += 1
        return real_signal_id
    except Exception as e:
        log.error(f'_db_save_signal_event: {e}')

def _db_update_signal_attribution(upd: dict):
    """[FIX-2] Vincula trade_id/order_id ao signal_event existente."""
    conn = get_db()
    if not conn: return
    try:
        c = conn.cursor()
        c.execute("""UPDATE signal_events
                     SET trade_id=%s, order_id=%s, updated_at=%s
                     WHERE signal_id=%s""",
                  (upd['trade_id'], upd['order_id'], upd['updated_at'], upd['signal_id']))
        conn.commit(); c.close(); conn.close()
    except Exception as e:
        log.error(f'_db_update_signal_attribution: {e}')

def _db_update_signal_outcome(upd: dict):
    conn = get_db()
    if not conn: return
    try:
        c = conn.cursor()
        c.execute("""UPDATE signal_events SET
            trade_id=%s, order_id=%s, outcome_status=%s, outcome_pnl=%s,
            outcome_pnl_pct=%s, outcome_close_reason=%s, updated_at=%s
            WHERE signal_id=%s""",
            (upd['trade_id'], upd['order_id'], upd['outcome_status'],
             upd['outcome_pnl'], upd['outcome_pnl_pct'],
             upd['outcome_close_reason'], upd['updated_at'],
             upd['signal_id']))
        conn.commit(); c.close(); conn.close()
    except Exception as e:
        log.error(f'_db_update_signal_outcome: {e}')

def _db_upsert_pattern_stats(s: dict):
    conn = get_db()
    if not conn: return
    try:
        c = conn.cursor()
        c.execute("""INSERT INTO pattern_stats (
            feature_hash, total_samples, wins, losses, flat_count,
            avg_pnl, avg_pnl_pct, ewma_pnl_pct, ewma_hit_rate,
            expectancy, downside_score, max_loss_seen, confidence_weight,
            last_seen_at, updated_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
            total_samples=VALUES(total_samples), wins=VALUES(wins),
            losses=VALUES(losses), flat_count=VALUES(flat_count),
            avg_pnl=VALUES(avg_pnl), avg_pnl_pct=VALUES(avg_pnl_pct),
            ewma_pnl_pct=VALUES(ewma_pnl_pct), ewma_hit_rate=VALUES(ewma_hit_rate),
            expectancy=VALUES(expectancy), downside_score=VALUES(downside_score),
            max_loss_seen=VALUES(max_loss_seen), confidence_weight=VALUES(confidence_weight),
            last_seen_at=VALUES(last_seen_at), updated_at=VALUES(updated_at)""",
            (s['feature_hash'], s['total_samples'], s['wins'], s['losses'], s['flat_count'],
             s['avg_pnl'], s['avg_pnl_pct'], s['ewma_pnl_pct'], s['ewma_hit_rate'],
             s['expectancy'], s['downside_score'], s['max_loss_seen'], s['confidence_weight'],
             s['last_seen_at'], s['updated_at']))
        conn.commit(); c.close(); conn.close()
    except Exception as e:
        log.error(f'_db_upsert_pattern_stats: {e}')

def _db_upsert_factor_stats(s: dict):
    conn = get_db()
    if not conn: return
    try:
        c = conn.cursor()
        c.execute("""INSERT INTO factor_stats (
            factor_type, factor_value, total_samples, wins, losses,
            avg_pnl_pct, ewma_pnl_pct, expectancy, downside_score,
            confidence_weight, last_seen_at, updated_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
            total_samples=VALUES(total_samples), wins=VALUES(wins),
            losses=VALUES(losses), avg_pnl_pct=VALUES(avg_pnl_pct),
            ewma_pnl_pct=VALUES(ewma_pnl_pct), expectancy=VALUES(expectancy),
            downside_score=VALUES(downside_score), confidence_weight=VALUES(confidence_weight),
            last_seen_at=VALUES(last_seen_at), updated_at=VALUES(updated_at)""",
            (s['factor_type'], s['factor_value'], s['total_samples'], s['wins'], s['losses'],
             s['avg_pnl_pct'], s['ewma_pnl_pct'], s['expectancy'], s['downside_score'],
             s['confidence_weight'], s['last_seen_at'], s['updated_at']))
        conn.commit(); c.close(); conn.close()
    except Exception as e:
        log.error(f'_db_upsert_factor_stats: {e}')

def _db_save_shadow_decision(shadow: dict):
    conn = get_db()
    if not conn: return
    try:
        c = conn.cursor()
        c.execute("""INSERT IGNORE INTO shadow_decisions (
            shadow_id, signal_id, symbol, `signal`, price_at_signal,
            not_executed_reason, hypothetical_entry, evaluation_status,
            created_at, payload_json)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (shadow['shadow_id'], shadow['signal_id'], shadow['symbol'],
             shadow['signal'], shadow['price_at_signal'],
             shadow['not_executed_reason'], shadow['hypothetical_entry'],
             shadow['evaluation_status'], shadow['created_at'], shadow['payload_json']))
        c.close()
    except Exception as e:
        log.error(f'_db_save_shadow_decision: {e}')
    finally:
        conn.close()   # [v10.7] sempre devolve ao pool

def _db_log_learning_audit(event_type: str, entity_id: str, payload: dict):
    conn = get_db()
    if not conn: return
    try:
        c = conn.cursor()
        c.execute("INSERT INTO learning_audit (event_type, entity_id, payload_json) VALUES (%s,%s,%s)",
                  (event_type, entity_id, json.dumps(payload, default=str)))
        conn.commit(); c.close(); conn.close()
    except Exception as e:
        log.error(f'_db_log_learning_audit: {e}')

# ═══════════════════════════════════════════════════════════════
# [L-3] LEARNING UPDATE — chamado quando trade fecha
# ═══════════════════════════════════════════════════════════════

def process_trade_outcome(trade: dict):
    """[L-7][FIX-4] Processa fechamento de trade e atualiza aprendizado.
    Se _features não estiver em memória (pós-restart), reconstrói do features_json salvo no banco.
    """
    global LEARNING_DEGRADED, learning_errors
    if not LEARNING_ENABLED: return
    try:
        pnl       = trade.get('pnl', 0)
        pnl_pct   = trade.get('pnl_pct', 0)
        sig_id    = trade.get('signal_id', '')
        feat_hash = trade.get('feature_hash', '')

        # [FIX-4] Reconstituir features — preferência: memória → features_json do trade → None
        features = trade.get('_features')
        if not features and trade.get('features_json'):
            try:
                features = json.loads(trade['features_json'])
                log.debug(f'process_trade_outcome: features reconstituídas do features_json ({trade.get("id")})')
            except Exception as e:
                log.warning(f'process_trade_outcome: falha ao parse features_json: {e}')
                features = None

        if feat_hash:
            ps = update_pattern_stats(feat_hash, pnl, pnl_pct)
            enqueue_persist('pattern_stats', ps)

        if features:
            update_factor_stats(features, pnl, pnl_pct)
            # [v10.5-4] Persistir TODOS os fatores — antes só 6 eram salvos no banco.
            # atr_bucket, volume_bucket, time_bucket, weekday, asset_type, market_type, dq_bucket
            # agora persistem junto. Shadow learning e restart usam cache completo.
            ALL_FACTOR_KEYS = [
                'score_bucket', 'rsi_bucket', 'ema_alignment',
                'volatility_bucket', 'regime_mode', 'direction',
                'atr_bucket', 'volume_bucket',          # [v10.5-4]
                'time_bucket', 'weekday',               # [v10.5-4]
                'asset_type', 'market_type', 'dq_bucket',  # [v10.5-4]
            ]
            with learning_lock:
                for ftype in ALL_FACTOR_KEYS:
                    fval = str(features.get(ftype, ''))
                    if fval:
                        fs_copy = dict(factor_stats_cache.get((ftype, fval), {}))
                        if fs_copy:
                            enqueue_persist('factor_stats', fs_copy)
        elif feat_hash:
            log.debug(f'process_trade_outcome: sem features para {trade.get("id")} — só pattern_stats atualizado')

        if sig_id:
            update_signal_outcome(sig_id, trade.get('id',''), trade.get('order_id',''),
                                   pnl, pnl_pct, trade.get('close_reason',''))

        LEARNING_DEGRADED = False
        learning_errors = max(0, learning_errors - 1)   # decrementa ao sucesso
    except Exception as e:
        log.error(f'process_trade_outcome: {e}')
        learning_errors += 1
        if learning_errors >= 5:
            LEARNING_DEGRADED = True
            log.warning('Learning engine em modo degradado após 5 erros consecutivos')

# ═══════════════════════════════════════════════════════════════
# [L-3] INIT LEARNING — carrega stats do banco no startup
# ═══════════════════════════════════════════════════════════════

def init_learning_cache():
    """[L-3] Carrega pattern_stats e factor_stats do banco para memória."""
    global signal_events_count, last_learning_update, learning_errors, LEARNING_DEGRADED
    if not LEARNING_ENABLED: return
    conn = get_db()
    if not conn: return
    try:
        c = conn.cursor(dictionary=True)

        # pattern_stats
        c.execute("SELECT * FROM pattern_stats")
        with learning_lock:
            for r in c.fetchall():
                ps = {k: float(v) if isinstance(v, decimal.Decimal) else
                         (v.isoformat() if isinstance(v, datetime) else v)
                      for k, v in r.items()}
                pattern_stats_cache[ps['feature_hash']] = ps

        # factor_stats
        c.execute("SELECT * FROM factor_stats")
        with learning_lock:
            for r in c.fetchall():
                fs = {k: float(v) if isinstance(v, decimal.Decimal) else
                         (v.isoformat() if isinstance(v, datetime) else v)
                      for k, v in r.items()}
                key = (fs['factor_type'], fs['factor_value'])
                factor_stats_cache[key] = fs

        # Contagem de signal_events
        c.execute("SELECT COUNT(*) as n FROM signal_events")
        row = c.fetchone()
        signal_events_count = row['n'] if row else 0

        c.close(); conn.close()
        learning_errors = 0; LEARNING_DEGRADED = False
        log.info(f'Learning cache: {len(pattern_stats_cache)} padrões | '
                 f'{len(factor_stats_cache)} fatores | {signal_events_count} signal_events')
    except Exception as e:
        log.error(f'init_learning_cache: {e}')
        LEARNING_DEGRADED = True

# ═══════════════════════════════════════════════════════════════
# [FIX-5] SHADOW EVALUATOR LOOP — avalia decisões PENDING
# ═══════════════════════════════════════════════════════════════
SHADOW_EVAL_WINDOW_MIN = int(os.environ.get('SHADOW_EVAL_WINDOW_MIN', 60))   # minutos até avaliar

def shadow_evaluator_loop():
    """[FIX-5] Avalia shadow_decisions PENDING após janela configurável.
    Busca o preço atual do ativo, calcula hypothetical_pnl e fecha a decisão.
    Também atualiza aprendizado shadow (pattern_stats com peso reduzido).
    """
    while True:
        beat('shadow_evaluator_loop')
        time.sleep(600)   # verifica a cada 10 minutos
        beat('shadow_evaluator_loop')
        if not LEARNING_ENABLED or LEARNING_DEGRADED: continue
        try:
            conn = get_db()
            if not conn: continue
            cutoff = (datetime.utcnow() - timedelta(minutes=SHADOW_EVAL_WINDOW_MIN)).strftime('%Y-%m-%d %H:%M:%S')
            c = conn.cursor(dictionary=True)
            c.execute("""SELECT * FROM shadow_decisions
                         WHERE evaluation_status='PENDING'
                         AND created_at <= %s
                         LIMIT 50""", (cutoff,))
            pending = c.fetchall(); c.close(); conn.close()
            if not pending: continue

            evaluated = 0
            for dec in pending:
                # [v10.7-Fix4] Uma única conexão por decision, fechada em finally.
                # Antes: conn2 + conn3 = até 3 conexões simultâneas × 50 decisions = 150 conexões.
                # Agora: 1 conexão reaproveitada, fechamento garantido mesmo em erro.
                dec_conn = get_db()
                if not dec_conn: continue
                try:
                    sym   = dec.get('symbol', '')
                    sig   = dec.get('signal', 'COMPRA')
                    entry = float(dec.get('hypothetical_entry', 0) or 0)
                    if entry <= 0: continue

                    # Preço atual — lê de memória, sem I/O
                    current_price = None
                    p = stock_prices.get(sym + '.SA') or stock_prices.get(sym)
                    if p: current_price = p.get('price')
                    if not current_price:
                        crypto_sym = sym + 'USDT'
                        if crypto_sym in crypto_prices:
                            current_price = crypto_prices[crypto_sym]
                    if not current_price or current_price <= 0: continue

                    # PnL hipotético — [P4] coerente para long e short
                    if sig == 'COMPRA':
                        hyp_pnl_pct = (current_price - entry) / entry * 100
                        hyp_pnl     = round(current_price - entry, 4)
                    else:
                        hyp_pnl_pct = (entry - current_price) / entry * 100
                        hyp_pnl     = round(entry - current_price, 4)

                    status = 'WIN' if hyp_pnl_pct > 0.1 else ('LOSS' if hyp_pnl_pct < -0.1 else 'FLAT')

                    # UPDATE shadow_decision
                    cx = dec_conn.cursor()
                    cx.execute("""UPDATE shadow_decisions SET
                        hypothetical_exit=%s, hypothetical_pnl=%s,
                        hypothetical_pnl_pct=%s, evaluation_status=%s,
                        evaluated_at=%s WHERE shadow_id=%s""",
                        (current_price, round(hyp_pnl, 4), round(hyp_pnl_pct, 4),
                         status, datetime.utcnow().isoformat(), dec['shadow_id']))
                    cx.close()

                    # Buscar feature_hash via signal_events — mesma conexão
                    try:
                        cx2 = dec_conn.cursor(dictionary=True)
                        cx2.execute("SELECT feature_hash, payload_json FROM signal_events WHERE signal_id=%s",
                                    (dec.get('signal_id'),))
                        se_row = cx2.fetchone(); cx2.close()
                        if se_row and se_row.get('feature_hash'):
                            fhash = se_row['feature_hash']
                            shadow_pnl_pct = round(hyp_pnl_pct * 0.5, 4)
                            shadow_pnl     = round(hyp_pnl * 0.5, 4)
                            ps = update_pattern_stats(fhash, shadow_pnl, shadow_pnl_pct)
                            enqueue_persist('pattern_stats', ps)
                            try:
                                payload = json.loads(se_row.get('payload_json') or '{}')
                                shadow_features = {
                                    'score_bucket':     payload.get('score_bucket',''),
                                    'rsi_bucket':       payload.get('rsi_bucket',''),
                                    'ema_alignment':    payload.get('ema_alignment',''),
                                    'volatility_bucket':payload.get('volatility_bucket',''),
                                    'regime_mode':      payload.get('regime_mode',''),
                                    'direction':        payload.get('direction',''),
                                    'time_bucket':      payload.get('time_bucket',''),
                                    'weekday':          str(payload.get('weekday','')),
                                    'asset_type':       payload.get('asset_type',''),
                                    'market_type':      payload.get('market_type',''),
                                    'dq_bucket':        payload.get('dq_bucket',''),
                                }
                                if any(shadow_features.values()):
                                    update_factor_stats(shadow_features, shadow_pnl, shadow_pnl_pct)
                                    alpha_keys = list(shadow_features.keys())
                                    with learning_lock:
                                        for ftype in alpha_keys:
                                            fval = shadow_features.get(ftype, '')
                                            if fval:
                                                fs_copy = dict(factor_stats_cache.get((ftype, fval), {}))
                                                if fs_copy:
                                                    enqueue_persist('factor_stats', fs_copy)
                            except Exception as ef:
                                log.debug(f'shadow factor_stats {dec.get("shadow_id")}: {ef}')
                            _db_log_learning_audit('SHADOW_OUTCOME', dec['shadow_id'], {
                                'feature_hash': fhash, 'status': status,
                                'hyp_pnl_pct': round(hyp_pnl_pct, 4),
                                'shadow_weight': 0.5,
                            })
                    except Exception as e2:
                        log.debug(f'shadow learning update {dec.get("shadow_id")}: {e2}')

                    evaluated += 1
                except Exception as e:
                    log.debug(f'shadow_evaluator item {dec.get("shadow_id")}: {e}')
                finally:
                    dec_conn.close()   # [v10.7-Fix4] sempre devolve ao pool

            if evaluated:
                log.info(f'Shadow evaluator: {evaluated} decisões avaliadas')
        except Exception as e:
            log.error(f'shadow_evaluator_loop: {e}')

# ═══════════════════════════════════════════════════════════════
# Registrar handlers no persistence_worker para novos tipos
# ═══════════════════════════════════════════════════════════════
# (feito inline no persistence_worker existente via extend)

# Antes de init_all_tables:
# ═══════════════════════════════════════════════════════════════
def init_all_tables():
    conn=get_db()
    if not conn: log.error('init_all_tables: no DB'); return
    try:
        cursor=conn.cursor()
        cursor.execute("""CREATE TABLE IF NOT EXISTS trades (
            id VARCHAR(40) PRIMARY KEY, symbol VARCHAR(20), market VARCHAR(10),
            asset_type VARCHAR(15), direction VARCHAR(5),
            entry_price DECIMAL(18,6), exit_price DECIMAL(18,6), current_price DECIMAL(18,6),
            quantity DECIMAL(20,6), position_value DECIMAL(18,2),
            pnl DECIMAL(18,2) DEFAULT 0, pnl_pct DECIMAL(10,4) DEFAULT 0,
            peak_pnl_pct DECIMAL(10,4) DEFAULT 0, score INT, `signal` VARCHAR(10),
            status VARCHAR(10) DEFAULT 'OPEN', close_reason VARCHAR(20),
            from_watchlist TINYINT(1) DEFAULT 0, order_id VARCHAR(40),
            opened_at DATETIME, closed_at DATETIME, extensions INT DEFAULT 0,
            signal_id VARCHAR(40) NULL, feature_hash VARCHAR(20) NULL,
            learning_confidence DECIMAL(6,2) NULL, insight_summary TEXT NULL,
            learning_version VARCHAR(10) NULL, features_json LONGTEXT NULL)""")
        cursor.execute("""CREATE TABLE IF NOT EXISTS arbi_trades (
            id VARCHAR(40) PRIMARY KEY, pair_id VARCHAR(40), name VARCHAR(40),
            leg_a VARCHAR(20), leg_b VARCHAR(20), mkt_a VARCHAR(10), mkt_b VARCHAR(10),
            direction VARCHAR(10), buy_leg VARCHAR(20), buy_mkt VARCHAR(10),
            short_leg VARCHAR(20), short_mkt VARCHAR(10),
            entry_spread DECIMAL(10,4), current_spread DECIMAL(10,4), position_size DECIMAL(18,2),
            pnl DECIMAL(18,2) DEFAULT 0, pnl_pct DECIMAL(10,4) DEFAULT 0,
            peak_pnl_pct DECIMAL(10,4) DEFAULT 0, fx_rate DECIMAL(10,4),
            status VARCHAR(10) DEFAULT 'OPEN', close_reason VARCHAR(20),
            opened_at DATETIME, closed_at DATETIME, extensions INT DEFAULT 0)""")
        cursor.execute("""CREATE TABLE IF NOT EXISTS audit_events (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            event_type VARCHAR(50), entity_type VARCHAR(30), entity_id VARCHAR(50),
            payload_json TEXT, created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_created (created_at), INDEX idx_event (event_type))""")
        cursor.execute("""CREATE TABLE IF NOT EXISTS watchlist (
            symbol VARCHAR(30) PRIMARY KEY, market VARCHAR(10) NOT NULL,
            added_at DATETIME DEFAULT CURRENT_TIMESTAMP)""")
        cursor.execute("""CREATE TABLE IF NOT EXISTS orders (
            order_id VARCHAR(40) PRIMARY KEY, trade_id VARCHAR(40),
            symbol VARCHAR(20), side VARCHAR(5), order_type VARCHAR(10),
            qty DECIMAL(20,6), limit_price DECIMAL(18,6), stop_price DECIMAL(18,6),
            strategy VARCHAR(20), status VARCHAR(20) DEFAULT 'NEW',
            fill_price DECIMAL(18,6), fill_qty DECIMAL(20,6),
            slippage DECIMAL(10,4) DEFAULT 0, fee DECIMAL(10,4) DEFAULT 0,
            notes VARCHAR(200), sent_at DATETIME, filled_at DATETIME,
            status_history_json TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_symbol (symbol), INDEX idx_status (status))""")
        cursor.execute("""CREATE TABLE IF NOT EXISTS portfolio_snapshots (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            ts DATETIME, stocks_capital DECIMAL(18,2), crypto_capital DECIMAL(18,2),
            arbi_capital DECIMAL(18,2), stocks_open_pnl DECIMAL(18,2),
            crypto_open_pnl DECIMAL(18,2), arbi_open_pnl DECIMAL(18,2),
            total_open_pnl DECIMAL(18,2), open_positions INT, arbi_positions INT,
            kill_switch TINYINT(1), arbi_kill_switch TINYINT(1), market_regime VARCHAR(20),
            INDEX idx_ts (ts))""")
        cursor.execute("""CREATE TABLE IF NOT EXISTS symbol_cooldowns (
            symbol VARCHAR(30) PRIMARY KEY, last_close_at DATETIME,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP)""")

        # ── [L-2] Signal Events ───────────────────────────────────────────
        cursor.execute("""CREATE TABLE IF NOT EXISTS signal_events (
            signal_id VARCHAR(40) PRIMARY KEY,
            feature_hash VARCHAR(20), symbol VARCHAR(20), asset_type VARCHAR(15),
            market_type VARCHAR(10), `signal` VARCHAR(10), raw_score DECIMAL(6,2),
            learning_confidence DECIMAL(6,2), confidence_band VARCHAR(10),
            price DECIMAL(18,6), signal_created_at DATETIME,
            market_regime_mode VARCHAR(20), market_regime_volatility VARCHAR(10),
            market_open TINYINT(1), trade_open TINYINT(1),
            rsi DECIMAL(6,2), ema9 DECIMAL(18,6), ema21 DECIMAL(18,6), ema50 DECIMAL(18,6),
            rsi_bucket VARCHAR(15), score_bucket VARCHAR(15), change_pct_bucket VARCHAR(10),
            ema_alignment VARCHAR(20), volatility_bucket VARCHAR(10),
            weekday TINYINT, time_bucket VARCHAR(15), data_quality_score DECIMAL(5,2),
            source_type VARCHAR(30), payload_json TEXT, insight_summary TEXT,
            learning_version VARCHAR(10),
            trade_id VARCHAR(40) NULL, order_id VARCHAR(40) NULL,
            outcome_status VARCHAR(10) NULL, outcome_pnl DECIMAL(18,4) NULL,
            outcome_pnl_pct DECIMAL(10,4) NULL, outcome_close_reason VARCHAR(20) NULL,
            origin_signal_key VARCHAR(120) NULL,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_sig_symbol (symbol), INDEX idx_sig_hash (feature_hash),
            INDEX idx_sig_created (signal_created_at),
            UNIQUE KEY uq_origin_signal_key (origin_signal_key))""")

        # ── [L-3] Pattern Stats ───────────────────────────────────────────
        cursor.execute("""CREATE TABLE IF NOT EXISTS pattern_stats (
            feature_hash VARCHAR(20) PRIMARY KEY,
            total_samples INT DEFAULT 0, wins INT DEFAULT 0,
            losses INT DEFAULT 0, flat_count INT DEFAULT 0,
            avg_pnl DECIMAL(18,4) DEFAULT 0, avg_pnl_pct DECIMAL(10,4) DEFAULT 0,
            ewma_pnl_pct DECIMAL(10,4) DEFAULT 0, ewma_hit_rate DECIMAL(6,4) DEFAULT 0.5,
            expectancy DECIMAL(10,4) DEFAULT 0, downside_score DECIMAL(10,4) DEFAULT 0,
            max_loss_seen DECIMAL(10,4) DEFAULT 0, confidence_weight DECIMAL(6,4) DEFAULT 0,
            last_seen_at DATETIME, updated_at DATETIME DEFAULT CURRENT_TIMESTAMP)""")

        # ── [L-4] Factor Stats ────────────────────────────────────────────
        cursor.execute("""CREATE TABLE IF NOT EXISTS factor_stats (
            factor_type VARCHAR(30), factor_value VARCHAR(30),
            total_samples INT DEFAULT 0, wins INT DEFAULT 0, losses INT DEFAULT 0,
            avg_pnl_pct DECIMAL(10,4) DEFAULT 0, ewma_pnl_pct DECIMAL(10,4) DEFAULT 0,
            expectancy DECIMAL(10,4) DEFAULT 0, downside_score DECIMAL(10,4) DEFAULT 0,
            confidence_weight DECIMAL(6,4) DEFAULT 0,
            last_seen_at DATETIME, updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (factor_type, factor_value))""")

        # ── [L-8] Shadow Decisions ────────────────────────────────────────
        cursor.execute("""CREATE TABLE IF NOT EXISTS shadow_decisions (
            shadow_id VARCHAR(40) PRIMARY KEY,
            signal_id VARCHAR(40), symbol VARCHAR(20), `signal` VARCHAR(10),
            price_at_signal DECIMAL(18,6), not_executed_reason VARCHAR(30),
            hypothetical_entry DECIMAL(18,6), hypothetical_exit DECIMAL(18,6) NULL,
            hypothetical_pnl DECIMAL(18,4) NULL, hypothetical_pnl_pct DECIMAL(10,4) NULL,
            evaluation_status VARCHAR(15) DEFAULT 'PENDING',
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            evaluated_at DATETIME NULL, payload_json TEXT,
            INDEX idx_shd_signal (signal_id), INDEX idx_shd_symbol (symbol))""")

        # ── Learning Audit ────────────────────────────────────────────────
        cursor.execute("""CREATE TABLE IF NOT EXISTS learning_audit (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            event_type VARCHAR(50), entity_id VARCHAR(50),
            payload_json TEXT, created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_la_event (event_type))""")

        # ── Migração: adicionar colunas de learning nas tabelas existentes ─
        for col_sql in [
            "ALTER TABLE trades ADD COLUMN IF NOT EXISTS signal_id           VARCHAR(40)  NULL",
            "ALTER TABLE trades ADD COLUMN IF NOT EXISTS feature_hash        VARCHAR(20)  NULL",
            "ALTER TABLE trades ADD COLUMN IF NOT EXISTS learning_confidence DECIMAL(6,2) NULL",
            "ALTER TABLE trades ADD COLUMN IF NOT EXISTS insight_summary     TEXT         NULL",
            "ALTER TABLE trades ADD COLUMN IF NOT EXISTS learning_version    VARCHAR(10)  NULL",
            "ALTER TABLE trades ADD COLUMN IF NOT EXISTS features_json       LONGTEXT     NULL",  # [P0-1]
            # [S2] Deduplicação persistida — chave de origem do sinal de mercado
            "ALTER TABLE signal_events ADD COLUMN IF NOT EXISTS origin_signal_key VARCHAR(120) NULL",
        ]:
            try: cursor.execute(col_sql)
            except Exception as e:
                if 'Duplicate column' not in str(e): log.debug(f'Migration: {e}')
        # [S2] Índice UNIQUE para origin_signal_key em bancos existentes
        # Ignora erro se já existir (Duplicate key name)
        try:
            cursor.execute("""ALTER TABLE signal_events
                ADD UNIQUE KEY uq_origin_signal_key (origin_signal_key)""")
        except Exception as e:
            if 'Duplicate key name' not in str(e) and 'already exists' not in str(e).lower():
                log.debug(f'Migration uq_origin: {e}')
        conn.commit(); cursor.close(); conn.close()
        log.info('All tables created/verified')
    except Exception as e: log.error(f'init_all_tables: {e}')

def _row_to_trade(r):
    t = {}
    for k,v in r.items():
        if isinstance(v,datetime): t[k]=v.isoformat()
        elif isinstance(v,decimal.Decimal): t[k]=float(v)
        else: t[k]=v
    t.setdefault('pnl_history',[]); t.setdefault('peak_pnl_pct',0); t.setdefault('extensions',0)
    return t

def init_trades_tables():
    global stocks_capital, crypto_capital, arbi_capital
    conn=get_db()
    if not conn: return
    try:
        cursor=conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM trades WHERE status='OPEN'")
        for r in cursor.fetchall():
            t=_row_to_trade(r)
            # [FIX-4] Restaurar _features para factor learning funcionar após restart
            if t.get('features_json'):
                try: t['_features'] = json.loads(t['features_json'])
                except: pass
            if t['asset_type']=='stock': stocks_open.append(t); stocks_capital-=t['position_value']
            elif t['asset_type']=='crypto': crypto_open.append(t); crypto_capital-=t['position_value']
        cursor.execute("SELECT * FROM trades WHERE status='CLOSED' ORDER BY closed_at DESC LIMIT 200")
        for r in cursor.fetchall():
            t=_row_to_trade(r)
            if t['asset_type']=='stock': stocks_closed.append(t)
            elif t['asset_type']=='crypto': crypto_closed.append(t)
        cursor.execute("SELECT * FROM arbi_trades WHERE status='OPEN'")
        for r in cursor.fetchall():
            t=_row_to_trade(r); arbi_open.append(t); arbi_capital-=t['position_size']
        cursor.execute("SELECT * FROM arbi_trades WHERE status='CLOSED' ORDER BY closed_at DESC LIMIT 200")
        for r in cursor.fetchall(): arbi_closed.append(_row_to_trade(r))
        cursor.execute("SELECT symbol, last_close_at FROM symbol_cooldowns")
        for r in cursor.fetchall():
            if r.get('last_close_at'):
                symbol_cooldown[r['symbol']]=r['last_close_at'].timestamp()
        cursor.execute("SELECT * FROM orders ORDER BY created_at DESC LIMIT 500")
        with orders_lock:
            for r in cursor.fetchall():
                row = _row_to_trade(r)
                # [V91-2] Restaurar status_history do JSON salvo no banco
                if row.get('status_history_json'):
                    try: row['status_history'] = json.loads(row['status_history_json'])
                    except: row.setdefault('status_history', [])
                else:
                    row.setdefault('status_history', [])
                orders_log.append(row)
        cursor.execute("SELECT * FROM audit_events ORDER BY created_at DESC LIMIT 200")
        with audit_lock:
            for r in cursor.fetchall():
                try:
                    payload=json.loads(r.get('payload_json') or '{}')
                    entry={'timestamp':r['created_at'].isoformat() if r.get('created_at') else '',
                           'event':r.get('event_type','')}
                    entry.update(payload); audit_log.append(entry)
                except: pass
        cursor.close(); conn.close()
        log.info(f'Loaded: {len(stocks_open)}s/{len(crypto_open)}c/{len(arbi_open)}a open | '
                 f'{len(orders_log)} orders | {len(audit_log)} audit | {len(symbol_cooldown)} cooldowns')
    except Exception as e: log.error(f'init_trades_tables: {e}')

def _db_save_trade(trade):
    conn=get_db()
    if not conn: return
    try:
        cursor=conn.cursor(); t=trade
        # [FIX-4] features_json serializado para sobreviver restart
        features_json = None
        if t.get('_features'):
            try: features_json = json.dumps({k: v for k, v in t['_features'].items()
                                              if not k.startswith('_')}, default=str)
            except: pass
        cursor.execute("""INSERT INTO trades (id,symbol,market,asset_type,direction,
            entry_price,exit_price,current_price,quantity,position_value,
            pnl,pnl_pct,peak_pnl_pct,score,`signal`,status,close_reason,
            from_watchlist,order_id,opened_at,closed_at,extensions,
            signal_id,feature_hash,learning_confidence,insight_summary,learning_version,features_json)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE current_price=VALUES(current_price),pnl=VALUES(pnl),
            pnl_pct=VALUES(pnl_pct),peak_pnl_pct=VALUES(peak_pnl_pct),
            status=VALUES(status),close_reason=VALUES(close_reason),
            exit_price=VALUES(exit_price),closed_at=VALUES(closed_at),extensions=VALUES(extensions)""",
            (t.get('id'),t.get('symbol'),t.get('market'),t.get('asset_type'),t.get('direction'),
             t.get('entry_price'),t.get('exit_price'),t.get('current_price'),
             t.get('quantity'),t.get('position_value'),t.get('pnl',0),t.get('pnl_pct',0),
             t.get('peak_pnl_pct',0),t.get('score'),t.get('signal'),t.get('status','OPEN'),
             t.get('close_reason'),1 if t.get('from_watchlist') else 0,
             t.get('order_id'),t.get('opened_at'),t.get('closed_at'),t.get('extensions',0),
             t.get('signal_id'),t.get('feature_hash'),t.get('learning_confidence'),
             t.get('insight_summary'),t.get('learning_version'),features_json))
        conn.commit(); cursor.close(); conn.close()
    except Exception as e: log.error(f'db_save_trade: {e}')

def _db_save_arbi_trade(trade):
    conn=get_db()
    if not conn: return
    try:
        cursor=conn.cursor(); t=trade
        cursor.execute("""INSERT INTO arbi_trades (id,pair_id,name,leg_a,leg_b,mkt_a,mkt_b,
            direction,buy_leg,buy_mkt,short_leg,short_mkt,entry_spread,current_spread,
            position_size,pnl,pnl_pct,peak_pnl_pct,fx_rate,status,close_reason,opened_at,closed_at,extensions)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE current_spread=VALUES(current_spread),pnl=VALUES(pnl),
            pnl_pct=VALUES(pnl_pct),peak_pnl_pct=VALUES(peak_pnl_pct),
            status=VALUES(status),close_reason=VALUES(close_reason),
            closed_at=VALUES(closed_at),extensions=VALUES(extensions)""",
            (t.get('id'),t.get('pair_id'),t.get('name'),t.get('leg_a'),t.get('leg_b'),
             t.get('mkt_a'),t.get('mkt_b'),t.get('direction'),t.get('buy_leg'),t.get('buy_mkt'),
             t.get('short_leg'),t.get('short_mkt'),t.get('entry_spread'),t.get('current_spread'),
             t.get('position_size'),t.get('pnl',0),t.get('pnl_pct',0),t.get('peak_pnl_pct',0),
             t.get('fx_rate'),t.get('status','OPEN'),t.get('close_reason'),
             t.get('opened_at'),t.get('closed_at'),t.get('extensions',0)))
        conn.commit(); cursor.close(); conn.close()
    except Exception as e: log.error(f'db_save_arbi_trade: {e}')

def _db_save_cooldown(symbol, ts):
    conn=get_db()
    if not conn: return
    try:
        cursor=conn.cursor()
        dt=datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
        cursor.execute("INSERT INTO symbol_cooldowns (symbol,last_close_at) VALUES (%s,%s) "
                       "ON DUPLICATE KEY UPDATE last_close_at=%s,updated_at=NOW()",(symbol,dt,dt))
        conn.commit(); cursor.close(); conn.close()
    except Exception as e: log.error(f'db_save_cooldown: {e}')

# ═══════════════════════════════════════════════════════════════
# STOCK PRICE FEED — range=3mo para indicadores reais
# ═══════════════════════════════════════════════════════════════
STOCK_SYMBOLS_B3 = [
    'PETR4.SA','VALE3.SA','ITUB4.SA','BBDC4.SA','ABEV3.SA','WEGE3.SA',
    'RENT3.SA','LREN3.SA','SUZB3.SA','GGBR4.SA','EMBR3.SA','CSNA3.SA',
    'CMIG4.SA','CPLE6.SA','BBAS3.SA','VIVT3.SA','SBSP3.SA','CSAN3.SA',
    'GOAU4.SA','USIM5.SA','BPAC11.SA','RADL3.SA','PRIO3.SA','RAIZ4.SA',
    'BRFS3.SA','MRFG3.SA','JBSS3.SA','EGIE3.SA','CMIN3.SA','AESB3.SA'
]
STOCK_SYMBOLS_US = [
    'AAPL','MSFT','NVDA','AMZN','GOOGL','META','TSLA','NFLX','AMD','INTC',
    'JPM','BAC','GS','MS','V','MA','JNJ','PFE','UNH','XOM','CVX','COP',
    'DIS','UBER','LYFT','SPOT','COIN','SPY','QQQ','IWM'
]
ALL_STOCK_SYMBOLS = STOCK_SYMBOLS_B3 + STOCK_SYMBOLS_US

def _ema(closes, period):
    if len(closes) < period: return closes[-1] if closes else 0
    k=2.0/(period+1); ema=closes[0]
    for c in closes[1:]: ema=c*k+ema*(1-k)
    return ema

def _rsi(closes, period=14):
    if len(closes) < period+1: return 50.0
    gains=[]; losses=[]
    for i in range(1,period+1):
        d=closes[-period+i]-closes[-period+i-1]
        gains.append(d if d>0 else 0); losses.append(abs(d) if d<0 else 0)
    ag=sum(gains)/period; al=sum(losses)/period
    if al==0: return 100.0
    return round(100-100/(1+ag/al),1)

# [v10.5-5] Cache de candles/indicadores para não refetchar histórico a cada loop.
# Preço snapshot: sempre fresco. Candles/EMA/RSI/ATR/Volume: cache de CANDLES_CACHE_MIN minutos.
CANDLES_CACHE_MIN  = int(os.environ.get('CANDLES_CACHE_MIN', 10))
_candles_cache: dict = {}   # sym → {'data': result_dict, 'ts': float}
_candles_lock = threading.Lock()

def _get_cached_candles(sym: str, ttl_min: int = None) -> dict:
    """Retorna candles do cache se frescos, None caso contrário.
    ttl_min: TTL customizado. None usa CANDLES_CACHE_MIN (padrão 10min).
    Klines diários de crypto usam ttl_min=60 para não sobrecarregar Binance.
    """
    ttl = (ttl_min if ttl_min is not None else CANDLES_CACHE_MIN) * 60
    with _candles_lock:
        entry = _candles_cache.get(sym)
    if entry and (time.time() - entry['ts']) < ttl:
        return entry['data']
    return None

def _set_cached_candles(sym: str, data: dict):
    with _candles_lock:
        _candles_cache[sym] = {'data': data, 'ts': time.time()}
def _fetch_polygon_stock(ticker: str) -> tuple:
    """[v10.4][v10.5-5] Polygon.io: snapshot de preço sempre fresco.
    Candles históricos (EMA/RSI/ATR/Volume) buscados só se cache > CANDLES_CACHE_MIN min.
    Reduz chamadas de API de ~4/min para ~1/10min por símbolo.
    """
    t0 = time.time()
    try:
        # Snapshot para preço atual — sempre fresco
        r = requests.get(
            f'https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers/{ticker}',
            params={'apiKey': POLYGON_API_KEY}, timeout=8)
        lat = (time.time() - t0) * 1000
        if r.status_code != 200: return None, lat
        snap = r.json().get('ticker', {})
        day  = snap.get('day', {}); prev_day = snap.get('prevDay', {})
        price = float(day.get('c') or snap.get('lastTrade', {}).get('p') or 0)
        prev  = float(prev_day.get('c') or 0)
        if price <= 0: return None, lat

        market = 'NYSE' if not ticker.endswith('.SA') else 'B3'

        # [v10.5-5] Tentar cache de candles primeiro
        cached = _get_cached_candles(f'polygon:{ticker}')
        if cached:
            result = dict(cached)
            result['price']      = price
            result['prev']       = prev
            result['change_pct'] = round((price / prev - 1) * 100, 2) if prev > 0 else 0
            result['updated_at'] = datetime.utcnow().isoformat()
            result['source']     = 'Polygon-snapshot'
            vol_today = float(day.get('v') or 0)
            if vol_today > 0 and cached.get('_avg_vol20', 0) > 0:
                result['volume_ratio'] = round(vol_today / cached['_avg_vol20'], 3)
            record_data_quality(ticker.replace('.SA', ''), 'Polygon', lat, True)
            return result, lat

        # Cache frio: buscar candles históricos
        end_date   = datetime.utcnow().strftime('%Y-%m-%d')
        start_date = (datetime.utcnow() - timedelta(days=90)).strftime('%Y-%m-%d')
        rc = requests.get(
            f'https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start_date}/{end_date}',
            params={'apiKey': POLYGON_API_KEY, 'adjusted': 'true', 'sort': 'asc', 'limit': 90},
            timeout=8)
        closes = []; highs = []; lows = []; volumes = []
        if rc.status_code == 200:
            bars = rc.json().get('results', [])
            closes  = [b['c'] for b in bars if b.get('c')]
            highs   = [b['h'] for b in bars if b.get('h')]
            lows    = [b['l'] for b in bars if b.get('l')]
            volumes = [b['v'] for b in bars if b.get('v')]

        n = len(closes)
        ema9  = _ema(closes, 9)  if n >= 9  else price
        ema21 = _ema(closes, 21) if n >= 21 else price
        ema50 = _ema(closes, 50) if n >= 50 else price
        rsi   = _rsi(closes)     if n >= 15 else 50.0
        atr   = _calc_atr(closes, highs, lows, 14) if n >= 15 else 0.0
        atr_pct = round((atr / price) * 100, 3) if price > 0 and atr > 0 else 0.0
        vol_today = float(day.get('v') or volumes[-1] if volumes else 0)
        avg_vol20 = sum(volumes[-20:]) / len(volumes[-20:]) if len(volumes) >= 20 else 0
        vol_ratio = round(vol_today / avg_vol20, 3) if avg_vol20 > 0 else 0.0

        result = {
            'price': price, 'prev': prev,
            'change_pct': round((price / prev - 1) * 100, 2) if prev > 0 else 0,
            'ema9': round(ema9, 4), 'ema21': round(ema21, 4), 'ema50': round(ema50, 4),
            'rsi': round(rsi, 1), 'atr_pct': atr_pct, 'volume_ratio': vol_ratio,
            'ema9_real': n >= 9, 'ema21_real': n >= 21, 'ema50_real': n >= 50, 'rsi_real': n >= 15,
            'candles_available': n, 'market': market,
            '_avg_vol20': avg_vol20,   # guardado no cache para atualizar vol_ratio no snapshot
            'source': 'Polygon', 'updated_at': datetime.utcnow().isoformat()
        }
        _set_cached_candles(f'polygon:{ticker}', result)
        record_data_quality(ticker.replace('.SA', ''), 'Polygon', lat, True)
        return result, lat
    except Exception as e:
        lat = (time.time() - t0) * 1000
        log.debug(f'Polygon {ticker}: {e}')
        record_data_quality(ticker.replace('.SA', ''), 'Polygon', lat, False)
        return None, lat

def _fetch_brapi_stock(ticker: str) -> tuple:
    """[v10.6-P0-1] Wrapper fino sobre _fetch_brapi_batch para retrocompatibilidade.
    Chamado por _fetch_single_stock() e _fetch_arbi_price() quando BRAPI_TOKEN existe.
    Retorna (result_dict | None, latency_ms).
    """
    t0 = time.time()
    res = _fetch_brapi_batch([ticker])
    lat = (time.time() - t0) * 1000
    data = res.get(ticker)
    return (data, lat) if data else (None, lat)


def _fetch_brapi_batch(tickers: list) -> dict:
    """[v10.6-P1] Busca até 20 ativos B3 em uma única chamada brapi.
    Retorna dict {ticker: result_dict}.

    Isso reduz chamadas de brapi de N req/loop para ceil(N/20) req/loop,
    e de ~2.5M/mês para ~130k/mês com candles cacheados por CANDLES_CACHE_MIN.
    """
    if not tickers or not BRAPI_TOKEN:
        return {}
    results = {}
    # Separar os que precisam de histórico dos que só precisam de snapshot
    cold = [t for t in tickers if _get_cached_candles(f'brapi:{t}') is None]
    warm = [t for t in tickers if t not in cold]

    headers = {'Authorization': f'Bearer {BRAPI_TOKEN}'}

    # ── Warm: batch snapshot, sem histórico ─────────────────────────────────
    for i in range(0, len(warm), 20):
        chunk = warm[i:i+20]
        t0 = time.time()
        try:
            r = requests.get(
                f'https://brapi.dev/api/quote/{",".join(chunk)}',
                headers=headers, timeout=8)
            lat = (time.time() - t0) * 1000
            if r.status_code != 200: continue
            for q in r.json().get('results', []):
                sym   = q.get('symbol', '').replace('.SA', '')
                price = float(q.get('regularMarketPrice') or 0)
                prev  = float(q.get('regularMarketPreviousClose') or 0)
                if price <= 0: continue
                cached = _get_cached_candles(f'brapi:{sym}')
                if cached:
                    entry = dict(cached)
                    entry['price']      = price
                    entry['prev']       = prev
                    entry['change_pct'] = round((price / prev - 1) * 100, 2) if prev > 0 else 0
                    entry['updated_at'] = datetime.utcnow().isoformat()
                    entry['source']     = 'brapi-batch-snapshot'
                    results[sym] = entry
                    record_data_quality(sym, 'brapi', lat, True)
        except Exception as e:
            log.warning(f'brapi batch snapshot chunk {chunk}: {e}')

    # ── Cold: batch com histórico, chunks de 10 (range=3mo é mais pesado) ────
    for i in range(0, len(cold), 10):
        chunk = cold[i:i+10]
        t0 = time.time()
        try:
            r = requests.get(
                f'https://brapi.dev/api/quote/{",".join(chunk)}',
                params={'range': '3mo', 'interval': '1d', 'fundamental': 'false'},
                headers=headers, timeout=12)
            lat = (time.time() - t0) * 1000
            if r.status_code != 200: continue
            for q in r.json().get('results', []):
                sym   = q.get('symbol', '').replace('.SA', '')
                price = float(q.get('regularMarketPrice') or 0)
                prev  = float(q.get('regularMarketPreviousClose') or 0)
                if price <= 0: continue

                hist  = q.get('historicalDataPrice', [])
                closes  = [c['close']  for c in hist if c.get('close')]
                highs   = [c['high']   for c in hist if c.get('high')]
                lows    = [c['low']    for c in hist if c.get('low')]
                volumes = [c['volume'] for c in hist if c.get('volume')]
                n = len(closes)
                ema9  = _ema(closes, 9)  if n >= 9  else price
                ema21 = _ema(closes, 21) if n >= 21 else price
                ema50 = _ema(closes, 50) if n >= 50 else price
                rsi   = _rsi(closes)     if n >= 15 else 50.0
                atr   = _calc_atr(closes, highs, lows, 14) if n >= 15 else 0.0
                atr_pct   = round((atr / price) * 100, 3) if price > 0 and atr > 0 else 0.0
                vol_today = float(q.get('regularMarketVolume') or 0)
                avg_vol20 = sum(volumes[-20:]) / len(volumes[-20:]) if len(volumes) >= 20 else 0
                vol_ratio = round(vol_today / avg_vol20, 3) if avg_vol20 > 0 else 0.0

                entry = {
                    'price': price, 'prev': prev,
                    'change_pct': round((price / prev - 1) * 100, 2) if prev > 0 else 0,
                    'ema9': round(ema9, 4), 'ema21': round(ema21, 4), 'ema50': round(ema50, 4),
                    'rsi': round(rsi, 1), 'atr_pct': atr_pct, 'volume_ratio': vol_ratio,
                    'ema9_real': n >= 9, 'ema21_real': n >= 21,
                    'ema50_real': n >= 50, 'rsi_real': n >= 15,
                    'candles_available': n, 'market': 'B3',
                    'source': 'brapi-batch-cold', 'updated_at': datetime.utcnow().isoformat()
                }
                _set_cached_candles(f'brapi:{sym}', entry)
                results[sym] = entry
                record_data_quality(sym, 'brapi', lat, True)
        except Exception as e:
            log.warning(f'brapi batch cold chunk {chunk}: {e}')

    return results


def _fetch_single_stock(sym: str) -> tuple:
    """[v10.4] Camada de dados: Polygon (US) → brapi (B3) → FMP → Yahoo.
    Sempre retorna atr_pct e volume_ratio quando disponível.
    """
    is_b3 = sym.endswith('.SA') or any(sym == s.replace('.SA','') for s in STOCK_SYMBOLS_B3)
    display = sym.replace('.SA', '')

    # 1. brapi para B3
    if is_b3 and BRAPI_TOKEN:
        result, lat = _fetch_brapi_stock(display)
        if result: return result, lat

    # 2. Polygon para US (e ADR de B3 quando brapi indisponível)
    if POLYGON_API_KEY:
        if not is_b3:
            result, lat = _fetch_polygon_stock(display)
            if result: return result, lat
        else:
            # [v10.5-1] ADR map real — não tentar ticker B3 diretamente no Polygon
            adr_sym = B3_TO_ADR.get(display)
            if adr_sym:
                result, lat = _fetch_polygon_stock(adr_sym)
                if result and result.get('price', 0) > 0:
                    # Converter preço USD → BRL usando fx_rates
                    usd_brl = fx_rates.get('USDBRL', 5.8)
                    price_brl = round(result['price'] * usd_brl, 2)
                    result['price'] = price_brl
                    result['prev']  = round(result.get('prev', 0) * usd_brl, 2)
                    result['ema9']  = round(result.get('ema9', 0) * usd_brl, 4)
                    result['ema21'] = round(result.get('ema21', 0) * usd_brl, 4)
                    result['ema50'] = round(result.get('ema50', 0) * usd_brl, 4)
                    result['market'] = 'B3'
                    result['source'] = f'Polygon-ADR({adr_sym})'
                    return result, lat
            # Sem ADR mapeado: não tentar Polygon com ticker B3 — vai retornar 404

    # 3. FMP fallback
    if FMP_API_KEY:
        try:
            t0 = time.time()
            r = requests.get(
                f'https://financialmodelingprep.com/api/v3/quote/{display}',
                params={'apikey': FMP_API_KEY}, timeout=8)
            lat = (time.time() - t0) * 1000
            if r.status_code == 200:
                d = r.json()
                if d and isinstance(d, list):
                    q = d[0]; price = float(q.get('price') or 0); prev = float(q.get('previousClose') or 0)
                    if price > 0:
                        result = {
                            'price': price, 'prev': prev,
                            'change_pct': round(float(q.get('changesPercentage') or 0), 2),
                            'ema9': price, 'ema21': price, 'ema50': price,
                            'rsi': 50.0, 'atr_pct': 0.0, 'volume_ratio': 0.0,
                            'ema9_real': False, 'ema21_real': False, 'ema50_real': False, 'rsi_real': False,
                            'candles_available': 0, 'market': 'B3' if is_b3 else 'NYSE',
                            'source': 'FMP', 'updated_at': datetime.utcnow().isoformat()
                        }
                        record_data_quality(display, 'FMP', lat, True)
                        return result, lat
        except Exception as e:
            log.debug(f'FMP fallback {display}: {e}')

    # 4. Yahoo Finance último recurso
    t0 = time.time()
    try:
        r = requests.get(
            f'https://query1.finance.yahoo.com/v8/finance/chart/{sym}?interval=1d&range=3mo',
            headers={'User-Agent': 'Mozilla/5.0'}, timeout=8)
        lat = (time.time() - t0) * 1000
        if r.status_code != 200: return None, lat
        data   = r.json()['chart']['result'][0]
        meta   = data['meta']
        price  = float(meta.get('regularMarketPrice') or 0)
        prev   = float(meta.get('chartPreviousClose') or 0)
        if price <= 0: return None, lat
        closes = [c for c in data.get('indicators', {}).get('quote', [{}])[0].get('close', []) if c]
        n = len(closes)
        ema9  = _ema(closes, 9)  if n >= 9  else price
        ema21 = _ema(closes, 21) if n >= 21 else price
        ema50 = _ema(closes, 50) if n >= 50 else price
        rsi   = _rsi(closes)     if n >= 15 else 50.0
        atr   = _calc_atr(closes, [], [], 14)
        atr_pct = round((atr / price) * 100, 3) if price > 0 and atr > 0 else 0.0
        result = {
            'price': price, 'prev': prev,
            'change_pct': round((price / prev - 1) * 100, 2) if prev > 0 else 0,
            'ema9': round(ema9, 4), 'ema21': round(ema21, 4), 'ema50': round(ema50, 4),
            'rsi': round(rsi, 1), 'atr_pct': atr_pct, 'volume_ratio': 0.0,
            'ema9_real': n >= 9, 'ema21_real': n >= 21, 'ema50_real': n >= 50, 'rsi_real': n >= 15,
            'candles_available': n, 'market': 'B3' if sym.endswith('.SA') else 'NYSE',
            'source': 'Yahoo', 'updated_at': datetime.utcnow().isoformat()
        }
        record_data_quality(display, 'Yahoo-3mo', lat, True)
        return result, lat
    except Exception as e:
        lat = (time.time() - t0) * 1000
        log.debug(f'Yahoo fallback {sym}: {e}')
        record_data_quality(display, 'Yahoo-3mo', lat, False)
        return None, lat

def fetch_stock_prices():
    """[v10.6-P2+P3] Polling com cadência inteligente por tipo de ativo e status do pregão.

    Regras de cadência reais (implementadas):
    ┌─────────────────────────┬──────────────────┬──────────────────────────┐
    │ Situação                │ B3               │ US/NYSE                  │
    ├─────────────────────────┼──────────────────┼──────────────────────────┤
    │ Posição aberta          │ ~30s (todo loop) │ ~30s (todo loop)         │
    │ Watchlist, pregão aberto│ ~60s (throttle)  │ ~30s (todo loop)         │
    │ Fora do pregão          │ 1x/30min         │ 1x/30min                 │
    └─────────────────────────┴──────────────────┴──────────────────────────┘

    Conta real de requisições com 30 B3 + 30 US:
    - Posições B3 (todo loop a 30s, 7h/dia × 22d): 2 batches × 30s = ~29.000/mês brapi max
      (em geral bem menos — apenas os ativos em posição, não todos os 30)
    - Watchlist B3 (60s throttle, 7h/dia × 22d):   2 batches × 60s = ~14.500/mês brapi max
    - Fora do pregão:                               ~800/mês brapi
    - Total máximo brapi: ~45.000/mês — 9% do plano Pro (500k).
    """
    with state_lock:
        open_syms_all = {t['symbol'] for t in stocks_open}

    b3_open   = is_b3_open()
    nyse_open = is_nyse_open()

    b3_symbols_display  = [s.replace('.SA', '') for s in STOCK_SYMBOLS_B3]
    us_symbols          = list(STOCK_SYMBOLS_US)

    # ── B3 ───────────────────────────────────────────────────────────────────
    b3_open_positions = [s for s in b3_symbols_display if s in open_syms_all]
    b3_watchlist      = [s for s in b3_symbols_display if s not in open_syms_all]

    now_ts = time.time()
    B3_OFF_HOURS_INTERVAL  = 30 * 60   # 30 min fora do pregão
    B3_WATCHLIST_PREGAO_IV = 60        # [v10.6-P1-3] 60s entre updates da watchlist durante pregão

    if BRAPI_TOKEN:
        # Posições abertas B3: todo loop durante pregão
        if b3_open_positions and b3_open:
            batch_result = _fetch_brapi_batch(b3_open_positions)
            with state_lock:
                for sym, data in batch_result.items():
                    stock_prices[sym] = data

        if b3_watchlist:
            last_wl_ts = getattr(fetch_stock_prices, '_last_b3_watchlist_ts', 0)
            if b3_open:
                # [v10.6-P1-3] Throttle real: watchlist só a cada 60s mesmo com pregão aberto
                should_update_watchlist = (now_ts - last_wl_ts) >= B3_WATCHLIST_PREGAO_IV
            else:
                should_update_watchlist = (now_ts - last_wl_ts) >= B3_OFF_HOURS_INTERVAL

            if should_update_watchlist:
                batch_result = _fetch_brapi_batch(b3_watchlist)
                with state_lock:
                    for sym, data in batch_result.items():
                        stock_prices[sym] = data
                fetch_stock_prices._last_b3_watchlist_ts = now_ts

        # Posições abertas B3 fora do pregão: 1x/30min para monitorar gap de abertura
        if b3_open_positions and not b3_open:
            last_b3_pos = getattr(fetch_stock_prices, '_last_b3_pos_offhours_ts', 0)
            if now_ts - last_b3_pos > B3_OFF_HOURS_INTERVAL:
                batch_result = _fetch_brapi_batch(b3_open_positions)
                with state_lock:
                    for sym, data in batch_result.items():
                        stock_prices[sym] = data
                fetch_stock_prices._last_b3_pos_offhours_ts = now_ts

    else:
        # Sem brapi: fallback individual (Polygon ADR → FMP → Yahoo)
        if b3_open or (now_ts - getattr(fetch_stock_prices, '_last_b3_fallback_ts', 0) > B3_OFF_HOURS_INTERVAL):
            for sym in b3_symbols_display:
                result, _ = _fetch_single_stock(sym)
                if result:
                    with state_lock: stock_prices[sym] = result
                time.sleep(0.3)
            fetch_stock_prices._last_b3_fallback_ts = now_ts

    # ── US/NYSE ──────────────────────────────────────────────────────────────
    # Posições abertas: sempre atualizar (podem estar em extended hours)
    us_open_positions = [s for s in us_symbols if s in open_syms_all]
    us_watchlist      = [s for s in us_symbols if s not in open_syms_all]

    US_OFF_HOURS_INTERVAL = 30 * 60

    for sym in us_open_positions:
        result, _ = _fetch_single_stock(sym)
        if result:
            with state_lock: stock_prices[sym] = result
        time.sleep(0.15)

    # Watchlist US: normal no pregão, 1x/30min fora
    if nyse_open or (now_ts - getattr(fetch_stock_prices, '_last_us_watchlist_ts', 0) > US_OFF_HOURS_INTERVAL):
        for sym in us_watchlist:
            if sym in open_syms_all: continue
            result, _ = _fetch_single_stock(sym)
            if result:
                with state_lock: stock_prices[sym] = result
            time.sleep(0.20)
        fetch_stock_prices._last_us_watchlist_ts = now_ts


def stock_price_loop():
    """[v10.6-P3] Loop com sleep adaptativo: mais curto no pregão, mais longo fora.
    [v10.6-P0-2] Beat incremental a cada 60s durante sleep fora do pregão para
    não disparar watchdog (timeout=420s). Sleep total fora do pregão: 300s = 5 beats de 60s.
    """
    while True:
        beat('stock_price_loop')
        try:
            fetch_stock_prices()
        except Exception as e:
            log.error(f'stock_price_loop: {e}')
        beat('stock_price_loop')
        # [v10.6-P3] Cadência adaptativa: 30s durante pregão, 5min fora
        if is_b3_open() or is_nyse_open():
            time.sleep(30)
        else:
            # [v10.6-P0-2] Sleep fragmentado: 5×60s com beat intermediário
            # Watchdog timeout = 420s; fragmentos de 60s garantem < 420s entre beats
            for _ in range(5):
                time.sleep(60)
                beat('stock_price_loop')

# ═══════════════════════════════════════════════════════════════
# CRYPTO PRICES — v10.4: Binance REST primário + score composto
# ═══════════════════════════════════════════════════════════════
FMP_CRYPTO_SYMBOLS=[s.replace('USDT','USD') for s in CRYPTO_SYMBOLS]
FMP_TO_INTERNAL={s:s.replace('USD','USDT') for s in FMP_CRYPTO_SYMBOLS}

def _fetch_binance_ticker(symbol: str) -> dict:
    """[v10.4] Binance 24h ticker — preço, volume, change_pct, high, low.
    Endpoint público, sem API key. Latência típica < 80ms.
    """
    try:
        r = requests.get(
            f'https://api.binance.com/api/v3/ticker/24hr',
            params={'symbol': symbol}, timeout=6)
        if r.status_code != 200: return {}
        d = r.json()
        return {
            'price':      float(d.get('lastPrice') or 0),
            'prev':       float(d.get('prevClosePrice') or 0),
            'change_pct': float(d.get('priceChangePercent') or 0),
            'high_24h':   float(d.get('highPrice') or 0),
            'low_24h':    float(d.get('lowPrice') or 0),
            'vol_24h':    float(d.get('volume') or 0),       # volume em base coin
            'vol_quote':  float(d.get('quoteVolume') or 0),  # volume em USDT
            'n_trades':   int(d.get('count') or 0),
        }
    except Exception as e:
        log.debug(f'Binance ticker {symbol}: {e}')
        return {}

def _fetch_binance_klines(symbol: str, period: int = 20) -> dict:
    """[v10.4][v10.5-2] Binance klines diárias para ATR e volume médio.
    Usa b[7] (quoteAssetVolume, em USDT) — compatível com vol_quote do allTickers.
    b[5] é volume em moeda base (BTC, ETH…) — não comparável com quoteVolume.
    """
    try:
        r = requests.get(
            'https://api.binance.com/api/v3/klines',
            params={'symbol': symbol, 'interval': '1d', 'limit': period + 2},
            timeout=6)
        if r.status_code != 200: return {}
        bars = r.json()
        closes  = [float(b[4]) for b in bars]   # close
        highs   = [float(b[2]) for b in bars]   # high
        lows    = [float(b[3]) for b in bars]   # low
        volumes = [float(b[7]) for b in bars]   # [v10.5-2] quoteAssetVolume (USDT) — era b[5] (base)
        return {'closes': closes, 'highs': highs, 'lows': lows, 'volumes': volumes}
    except Exception as e:
        log.debug(f'Binance klines {symbol}: {e}')
        return {}

def _crypto_composite_score(ticker: dict, klines: dict, direction: str) -> int:
    """[v10.4] Score composto multi-fator para crypto.
    Substitui 'score = 50 + int(abs(change_24h)*5)' que ignorava volume e ATR.

    Fatores (todos normalizados para 0-100, depois ponderados):
    - change_pct_24h   (40%): força do movimento
    - volume_ratio     (30%): volume hoje vs média 20d — confirma movimento
    - atr_position     (20%): preço vs range do dia (high/low) — direcionalidade
    - momentum_quality (10%): número de trades normalizado — liquidez
    """
    change  = ticker.get('change_pct', 0)
    high_24 = ticker.get('high_24h', 0)
    low_24  = ticker.get('low_24h', 0)
    price   = ticker.get('price', 0)
    vol_24  = ticker.get('vol_quote', 0)
    n_tr    = ticker.get('n_trades', 0)

    closes  = klines.get('closes', [])
    highs_k = klines.get('highs', [])
    lows_k  = klines.get('lows', [])
    vols_k  = klines.get('volumes', [])

    # Fator 1: change_pct (capped em ±15%)
    change_capped = max(-15.0, min(15.0, change))
    change_factor = (change_capped + 15) / 30 * 100  # 0-100

    # Fator 2: volume ratio vs média 20d
    avg_vol20 = sum(vols_k[-20:]) / len(vols_k[-20:]) if len(vols_k) >= 20 else 0
    vol_ratio = vol_24 / avg_vol20 if avg_vol20 > 0 else 1.0
    # 0.3→0 | 1.0→50 | 2.0→75 | 4.0→100
    vol_factor = min(100, (vol_ratio / 4.0) * 100)

    # Fator 3: posição no range do dia (0=low, 100=high)
    day_range = high_24 - low_24
    if day_range > 0 and price > 0:
        range_pos = ((price - low_24) / day_range) * 100
    else:
        range_pos = 50.0

    # Fator 4: liquidez (n_trades normalizado — >100k = max)
    liq_factor = min(100, (n_tr / 100_000) * 100) if n_tr > 0 else 50.0

    # Combinar com pesos
    raw = (0.40 * change_factor + 0.30 * vol_factor +
           0.20 * range_pos     + 0.10 * liq_factor)
    composite = max(5, min(95, int(raw)))

    # Para SHORT: inverter (score baixo = sinal de venda forte)
    if direction == 'SHORT':
        composite = 100 - composite
    return composite

def fetch_crypto_prices():
    """[v10.4] Binance REST primário → FMP fallback → Yahoo último recurso."""
    fetched_via_binance = False

    # 1. Binance — endpoint público, sem rate limit em bulk para poucos símbolos
    if True:  # sempre tenta Binance
        try:
            t0 = time.time()
            # allTickers em uma chamada só para eficiência
            r_all = requests.get('https://api.binance.com/api/v3/ticker/24hr', timeout=8)
            lat_bulk = (time.time() - t0) * 1000
            if r_all.status_code == 200:
                all_tickers = {d['symbol']: d for d in r_all.json()}
                for sym in CRYPTO_SYMBOLS:
                    t_data = all_tickers.get(sym, {})
                    if not t_data: continue
                    price   = float(t_data.get('lastPrice') or 0)
                    change  = float(t_data.get('priceChangePercent') or 0)
                    if price <= 0: continue
                    with state_lock:
                        crypto_prices[sym] = price
                        crypto_momentum[sym] = round(change, 3)
                        # Guardar dados extras para score composto
                        crypto_tickers[sym] = {
                            'price': price, 'change_pct': change,
                            'high_24h': float(t_data.get('highPrice') or 0),
                            'low_24h':  float(t_data.get('lowPrice') or 0),
                            'vol_quote': float(t_data.get('quoteVolume') or 0),
                            'n_trades': int(t_data.get('count') or 0),
                        }
                    record_data_quality(sym.replace('USDT',''), 'Binance', lat_bulk, True)
                fetched_via_binance = True

                # [v10.6-P1-5] Enriquecer crypto_tickers com atr_pct e vol_ratio reais via klines
                # Feito APÓS o bulk para não bloquear a atualização de preço.
                # Usa cache _candles_cache para evitar excesso de chamadas.
                for sym in CRYPTO_SYMBOLS:
                    cached_klines = _get_cached_candles(f'klines:{sym}', ttl_min=60)  # [v10.6.3-Fix2]
                    if cached_klines is None:
                        klines = _fetch_binance_klines(sym, 22)
                        if klines:
                            _set_cached_candles(f'klines:{sym}', klines)
                    else:
                        klines = cached_klines

                    if not klines:
                        continue

                    closes  = klines.get('closes', [])
                    highs_k = klines.get('highs', [])
                    lows_k  = klines.get('lows', [])
                    vols_k  = klines.get('volumes', [])
                    n = len(closes)

                    with state_lock:
                        tk = crypto_tickers.get(sym, {})
                        price_tk = tk.get('price', 0)

                    if price_tk <= 0 or n < 2:
                        continue

                    atr     = _calc_atr(closes, highs_k, lows_k, 14) if n >= 15 else 0.0
                    atr_pct = round((atr / price_tk) * 100, 3) if atr > 0 else 0.0
                    avg_vol  = sum(vols_k[-20:]) / len(vols_k[-20:]) if len(vols_k) >= 20 else 0
                    vol_24h  = tk.get('vol_quote', 0)
                    vol_ratio = round(vol_24h / avg_vol, 3) if avg_vol > 0 else 0.0

                    with state_lock:
                        if sym in crypto_tickers:
                            crypto_tickers[sym]['atr_pct']   = atr_pct
                            crypto_tickers[sym]['vol_ratio']  = vol_ratio
            else:
                log.warning(f'Binance allTickers HTTP {r_all.status_code}')
        except Exception as e:
            log.warning(f'Binance bulk fetch: {e}')

    if fetched_via_binance: return

    # 2. FMP fallback
    if FMP_API_KEY:
        try:
            t0 = time.time()
            r = requests.get(
                f'https://financialmodelingprep.com/api/v3/quote/{",".join(FMP_CRYPTO_SYMBOLS)}',
                params={'apikey': FMP_API_KEY}, timeout=10)
            lat = (time.time() - t0) * 1000
            if r.status_code == 200:
                data = r.json()
                if isinstance(data, list) and data:
                    with state_lock:
                        for a in data:
                            internal = FMP_TO_INTERNAL.get(a.get('symbol', ''))
                            price = float(a.get('price') or 0); chg = float(a.get('changesPercentage') or 0)
                            if internal and price > 0:
                                crypto_prices[internal] = price; crypto_momentum[internal] = chg
                                record_data_quality(internal.replace('USDT', ''), 'FMP', lat, True)
                    return
        except Exception as e: log.warning(f'FMP crypto: {e}')

    # 3. Yahoo último recurso
    try:
        for sym in CRYPTO_SYMBOLS:
            t0 = time.time(); display = sym.replace('USDT', '') + '-USD'
            r = requests.get(
                f'https://query1.finance.yahoo.com/v8/finance/chart/{display}?interval=1d&range=1d',
                headers={'User-Agent': 'Mozilla/5.0'}, timeout=5)
            lat = (time.time() - t0) * 1000
            if r.status_code == 200:
                meta = r.json()['chart']['result'][0]['meta']
                price = float(meta.get('regularMarketPrice') or 0)
                prev  = float(meta.get('chartPreviousClose') or 0)
                if price > 0:
                    with state_lock:
                        crypto_prices[sym] = price
                        crypto_momentum[sym] = round((price / prev - 1) * 100, 2) if prev > 0 else 0
                    record_data_quality(sym.replace('USDT', ''), 'Yahoo', lat, True)
            time.sleep(0.3)
    except Exception as e: log.error(f'Yahoo crypto: {e}')

def crypto_price_loop():
    while True:
        beat('crypto_price_loop')
        try: fetch_crypto_prices(); _update_market_regime()
        except Exception as e: log.error(f'crypto_price_loop: {e}')
        time.sleep(10)

def _update_market_regime():
    global market_regime
    with state_lock: mom=dict(crypto_momentum)
    if not mom: return
    vals=list(mom.values()); n=len(vals)
    trending=sum(1 for v in vals if abs(v)>2.0); high_vol=sum(1 for v in vals if abs(v)>5.0)
    mode='HIGH_VOL' if high_vol/n>0.4 else ('TRENDING' if trending/n>0.6 else 'RANGING')
    avg=sum(abs(v) for v in vals)/n
    vol='HIGH' if avg>4 else ('LOW' if avg<1 else 'NORMAL')
    market_regime={'mode':mode,'volatility':vol,'avg_change_pct':round(avg,2),'updated_at':datetime.utcnow().isoformat()}

def calc_period_pnl(trades, days):
    cutoff=(datetime.utcnow()-timedelta(days=days)).isoformat()
    return round(sum(t.get('pnl',0) for t in trades if t.get('closed_at','')>=cutoff),2)

def is_momentum_positive(trade):
    h=trade.get('pnl_history',[]); return len(h)>=3 and h[-1]>h[-2]>h[-3] and trade['pnl_pct']>-1.5

# ═══════════════════════════════════════════════════════════════
# FX RATES
# ═══════════════════════════════════════════════════════════════
def fetch_fx_rates():
    """[v10.4] frankfurter.app primário (ECB data, free, sem key, sem limite) → Yahoo fallback.
    frankfurter.app é mantido pelo Frankfurter open-source project, dados do Banco Central Europeu.
    USDBRL, GBPUSD, HKDUSD. Atualizado a cada ciclo do arbi_scan_loop (~6min).
    """
    try:
        # frankfurter.app: base USD, retorna quantas unidades de cada moeda = 1 USD
        r = requests.get(
            'https://api.frankfurter.app/latest',
            params={'from': 'USD', 'to': 'BRL,GBP,HKD'}, timeout=8)
        if r.status_code == 200:
            rates = r.json().get('rates', {})
            if rates.get('BRL', 0) > 0:
                fx_rates['USDBRL'] = round(rates['BRL'], 4)
            if rates.get('GBP', 0) > 0:
                # frankfurter retorna USD→GBP (ex: 0.79); queremos GBPUSD (ex: 1.27)
                fx_rates['GBPUSD'] = round(1.0 / rates['GBP'], 4)
            if rates.get('HKD', 0) > 0:
                fx_rates['HKDUSD'] = round(rates['HKD'], 4)
            log.info(f'FX (frankfurter.app/ECB): {fx_rates}')
            return
    except Exception as e:
        log.warning(f'frankfurter.app: {e}')
    # Yahoo fallback
    pairs = {'USDBRL': 'BRL=X', 'GBPUSD': 'GBPUSD=X', 'HKDUSD': 'HKD=X'}
    for key, sym in pairs.items():
        try:
            r = requests.get(
                f'https://query1.finance.yahoo.com/v8/finance/chart/{sym}?interval=1d&range=1d',
                headers={'User-Agent': 'Mozilla/5.0'}, timeout=6)
            if r.status_code == 200:
                price = r.json()['chart']['result'][0]['meta'].get('regularMarketPrice', 0)
                if price > 0: fx_rates[key] = price
        except: pass
    log.info(f'FX (Yahoo fallback): {fx_rates}')

# ═══════════════════════════════════════════════════════════════
# MONITOR TRADES
# ═══════════════════════════════════════════════════════════════
def monitor_trades():
    global stocks_capital, crypto_capital
    while True:
        beat('monitor_trades')
        time.sleep(5)
        try:
            closed_stocks=[]; closed_cryptos=[]
            with state_lock:
                now=datetime.utcnow(); to_close=[]
                for trade in stocks_open:
                    sym=trade['symbol']; pd=stock_prices.get(sym)
                    price=pd['price'] if pd else trade.get('current_price',trade['entry_price'])
                    trade['current_price']=price
                    age_h=(now-datetime.fromisoformat(trade['opened_at'])).total_seconds()/3600
                    if trade.get('direction')=='SHORT':
                        trade['pnl']=round((trade['entry_price']-price)*trade['quantity'],2)
                        trade['pnl_pct']=round((trade['entry_price']/price-1)*100,2) if price>0 else 0
                    else:
                        trade['pnl']=round((price-trade['entry_price'])*trade['quantity'],2)
                        trade['pnl_pct']=round((price/trade['entry_price']-1)*100,2)
                    h=trade.setdefault('pnl_history',[]); h.append(trade['pnl_pct'])
                    if len(h)>5: h.pop(0)
                    trade['peak_pnl_pct']=round(max(trade.get('peak_pnl_pct',0),trade['pnl_pct']),2)
                    peak=trade['peak_pnl_pct']; mkt=trade.get('market',''); reason=None
                    if peak>=2.0 and trade['pnl_pct']<=peak-1.0:   reason='TRAILING_STOP'
                    elif trade['pnl_pct']<=-1.5:                    reason='STOP_LOSS'
                    elif age_h>=2.0:
                        ext=trade.get('extensions',0)
                        if is_momentum_positive(trade) and ext<3: trade['extensions']=ext+1
                        else:                                      reason='TIMEOUT'
                    elif not market_open_for(mkt) and age_h>0.5:   reason='MARKET_CLOSE'
                    if reason:
                        # [v10.7-Fix2] Devolução de capital correta para LONG e SHORT:
                        # Debitado na abertura: position_value = entry_price * qty
                        # Retornado no fechamento: position_value + pnl
                        #   LONG:  pnl = (exit - entry) * qty  → retorna exit_price * qty   ✓
                        #   SHORT: pnl = (entry - exit) * qty  → retorna collateral + ganho ✓
                        # NÃO usar exit_price * qty para SHORT (seria capital incorreto)
                        stocks_capital += trade['position_value'] + trade['pnl']
                        symbol_cooldown[sym]=time.time()
                        c=dict(trade); c.update({'exit_price':price,'closed_at':now.isoformat(),'close_reason':reason,'status':'CLOSED'})
                        stocks_closed.insert(0,c)
                        if len(stocks_closed) > MAX_CLOSED_HISTORY: stocks_closed.pop()   # [v10.7-Fix3]
                        to_close.append(trade['id']); closed_stocks.append(c)
                stocks_open[:] = [t for t in stocks_open if t['id'] not in to_close]

                to_close_c=[]
                for trade in crypto_open:
                    sym=trade['symbol']+'USDT'; price=crypto_prices.get(sym,trade['current_price'])
                    age_h=(now-datetime.fromisoformat(trade['opened_at'])).total_seconds()/3600
                    trade['current_price']=price
                    if trade.get('direction')=='SHORT':
                        trade['pnl']=round((trade['entry_price']-price)*trade['quantity'],2)
                        trade['pnl_pct']=round((trade['entry_price']/price-1)*100,2) if price>0 else 0
                    else:
                        trade['pnl']=round((price-trade['entry_price'])*trade['quantity'],2)
                        trade['pnl_pct']=round((price/trade['entry_price']-1)*100,2)
                    h=trade.setdefault('pnl_history',[]); h.append(trade['pnl_pct'])
                    if len(h)>5: h.pop(0)
                    trade['peak_pnl_pct']=round(max(trade.get('peak_pnl_pct',0),trade['pnl_pct']),2)
                    peak=trade['peak_pnl_pct']; reason=None
                    if peak>=2.0 and trade['pnl_pct']<=peak-1.0:   reason='TRAILING_STOP'
                    elif trade['pnl_pct']<=-2.0:                    reason='STOP_LOSS'
                    elif age_h>=4.0:
                        ext=trade.get('extensions',0)
                        if is_momentum_positive(trade) and ext<3: trade['extensions']=ext+1
                        else:                                      reason='TIMEOUT'
                    if reason:
                        # [v10.7-Fix2] position_value + pnl — correto para LONG e SHORT
                        crypto_capital += trade['position_value'] + trade['pnl']
                        symbol_cooldown[trade['symbol']]=time.time()
                        c=dict(trade); c.update({'exit_price':price,'closed_at':now.isoformat(),'close_reason':reason,'status':'CLOSED'})
                        crypto_closed.insert(0,c)
                        if len(crypto_closed) > MAX_CLOSED_HISTORY: crypto_closed.pop()   # [v10.7-Fix3]
                        to_close_c.append(trade['id']); closed_cryptos.append(c)
                crypto_open[:] = [t for t in crypto_open if t['id'] not in to_close_c]

            for c in closed_stocks:
                audit('TRADE_CLOSED',{'id':c['id'],'symbol':c['symbol'],'pnl':c['pnl'],'reason':c['close_reason']})
                enqueue_persist('trade',c)
                enqueue_persist('cooldown',symbol=c['symbol'],ts=symbol_cooldown.get(c['symbol'],time.time()))
                alert_trade_closed(c)
                # [L-7] Aprender com o resultado do trade
                process_trade_outcome(c)
            for c in closed_cryptos:
                audit('TRADE_CLOSED',{'id':c['id'],'symbol':c['symbol'],'pnl':c['pnl'],'reason':c['close_reason']})
                enqueue_persist('trade',c)
                enqueue_persist('cooldown',symbol=c['symbol'],ts=symbol_cooldown.get(c['symbol'],time.time()))
                alert_trade_closed(c)
                # [L-7] Aprender com o resultado do trade
                process_trade_outcome(c)
        except Exception as e: log.error(f'monitor_trades: {e}')

# ═══════════════════════════════════════════════════════════════
# [V9-1] STOCK EXECUTION WORKER — create_order FORA do state_lock
# ═══════════════════════════════════════════════════════════════
def stock_execution_worker():
    global stocks_capital
    while True:
        beat('stock_execution_worker')
        time.sleep(60)
        beat('stock_execution_worker')
        try:
            conn=get_db()
            if not conn: continue
            cursor=conn.cursor(dictionary=True)
            cutoff=(datetime.utcnow()-timedelta(minutes=SIGNAL_MAX_AGE_MIN)).strftime('%Y-%m-%d %H:%M:%S')
            cursor.execute('SELECT * FROM market_signals WHERE created_at>=%s ORDER BY score DESC LIMIT 200',(cutoff,))
            rows=cursor.fetchall(); cursor.close(); conn.close()

            for sig in rows:
                for k,v in sig.items():
                    if isinstance(v,datetime): sig[k]=v.isoformat()
                score=sig.get('score',0); mkt=sig.get('market_type','')
                signal_val=sig.get('signal',''); sym=sig.get('symbol','')
                pd=stock_prices.get(sym); price=pd['price'] if pd else float(sig.get('price',0) or 0)
                if price<=0: continue
                is_long=score>=70 and signal_val=='COMPRA'
                is_short=score<=30 and signal_val=='VENDA'
                if not (is_long or is_short): continue

                # ── Deduplicação + política de re-avaliação ─────────────────────────────────
                # Motivos PERMANENTES: nunca re-avaliar dentro da mesma janela de sinal.
                # [v10.3.2-P0-3] 'executed' agora é permanente — evita reprocessar sinal que já virou trade.
                PERMANENT_REASONS = {'kill_switch', 'symbol_duplicate', 'executed'}
                # Motivos TEMPORÁRIOS: re-avaliar se o contexto que causou o bloqueio mudou.
                # Mapeamento reason → função de checagem (True = ainda bloqueado).
                ms_key = str(sig.get('id') or f"{sym}:{score}:{sig.get('created_at','')}")
                origin_key = ms_key[:120]
                with learning_lock:
                    cached = processed_signal_ids.get(ms_key)

                # [v10.3.2-P0-1] _sig_pre_id preservado do cache; gen_id() SÓ para sinal novo.
                # A linha que sobrescrevia o valor do cache foi removida.
                if cached:
                    if cached['reason'] in PERMANENT_REASONS:
                        continue
                    reason_was = cached['reason']
                    _sig_pre_id = cached['sig_id']   # reusar ID existente — não gerar novo
                    # Verificar se o contexto ainda bloqueia
                    # Mapeamento cobre TODAS as strings reais devolvidas por check_risk()
                    still_blocked = False
                    if reason_was == 'market_closed':
                        still_blocked = not market_open_for(mkt)
                    elif reason_was in ('SYMBOL_COOLDOWN', 'cooldown', 'COOLDOWN'):
                        # [v10.3.2-P0-2] float timestamp + SYMBOL_COOLDOWN_SEC
                        still_blocked = (time.time() - symbol_cooldown.get(sym, 0)) < SYMBOL_COOLDOWN_SEC
                    elif reason_was in ('INSUFFICIENT_CAPITAL', 'capital',
                                        'STOCKS_CAPITAL_LIMIT', 'CRYPTO_CAPITAL_LIMIT', 'NO_CAPITAL_CRYPTO'):
                        # [v10.3.4-F3] Replica a lógica REAL do check_risk():
                        # STOCKS_CAPITAL_LIMIT: committed + desired > INITIAL * MAX_CAPITAL_PCT/100
                        # Não basta olhar stocks_capital livre — precisa checar capital comprometido.
                        if reason_was in ('STOCKS_CAPITAL_LIMIT', 'capital', 'INSUFFICIENT_CAPITAL'):
                            committed_s = sum(t.get('position_value', 0) for t in stocks_open)
                            score_factor_tmp = min(abs(score - 50) / 50.0, 1.0)
                            conf_tmp = calc_learning_confidence(
                                {'symbol': sym, 'asset_type': 'stock', 'market_type': mkt, 'score': score},
                                {}, '')
                            rm_tmp = get_risk_multiplier(conf_tmp)
                            desired_tmp = min(stocks_capital * (0.08 + score_factor_tmp * 0.07) * rm_tmp,
                                              MAX_POSITION_STOCKS)
                            cap_limit = INITIAL_CAPITAL_STOCKS * MAX_CAPITAL_PCT_STOCKS / 100
                            still_blocked = (committed_s + desired_tmp) > cap_limit
                        else:
                            # [v10.5-6] CRYPTO_CAPITAL_LIMIT no stock_execution_worker:
                            # approved_size não existe neste escopo (é variável do auto_trade_crypto).
                            # Usar desired_pos calculado para a posição corrente como proxy.
                            committed_c = sum(t.get('position_value', 0) for t in crypto_open)
                            cap_limit_c = INITIAL_CAPITAL_CRYPTO * MAX_CAPITAL_PCT_CRYPTO / 100
                            score_factor_c = min(abs(score - 50) / 50.0, 1.0)
                            desired_c = min(crypto_capital * (0.05 + score_factor_c * 0.05),
                                            MAX_POSITION_CRYPTO)
                            still_blocked = (committed_c + desired_c) > cap_limit_c
                    elif reason_was.startswith('MAX_OPEN_POSITIONS'):
                        # [v10.3.3-F2] Bloqueado por limite global — checar se abrimos menos
                        still_blocked = len(stocks_open) + len(crypto_open) >= MAX_OPEN_POSITIONS
                    elif reason_was.startswith('MAX_POSITIONS_STOCKS'):
                        still_blocked = len(stocks_open) >= MAX_POSITIONS_STOCKS
                    elif reason_was.startswith('MAX_POSITIONS_CRYPTO'):
                        still_blocked = len(crypto_open) >= MAX_POSITIONS_CRYPTO
                    elif reason_was.startswith('MAX_POSITION_SAME_MKT'):
                        # [v10.3.4-F4] Constante sempre existe — definida no topo do arquivo
                        mkt_count = sum(1 for t in stocks_open if t.get('market') == mkt)
                        still_blocked = mkt_count >= MAX_POSITION_SAME_MKT
                    elif reason_was.startswith('SYMBOL_ALREADY_OPEN'):
                        still_blocked = sym in {t['symbol'] for t in stocks_open + crypto_open}
                    # [v10.3.4-F5] DRAWDOWN ativa kill_switch internamente — tratar como permanente
                    # já no primeiro evento (mesmo que RISK_KILL_SWITCH ainda não fosse True no split()[0])
                    elif reason_was in ('KILL_SWITCH_ACTIVE', 'KILL_SWITCH', 'ARBI_KILL_SWITCH') \
                            or reason_was.startswith(('DAILY_DRAWDOWN', 'WEEKLY_DRAWDOWN')):
                        still_blocked = True
                    else:
                        # Motivo desconhecido ou temporário genérico → tentar de novo
                        still_blocked = False
                    if still_blocked:
                        continue
                    # Contexto mudou — reavaliação usando o signal_id já existente no banco
                else:
                    _sig_pre_id = gen_id('SIG')   # sinal novo: gerar ID agora
                    # Registrar no cache com LRU
                    with learning_lock:
                        if len(processed_signal_ids) >= MAX_PROCESSED_SIGNALS_CACHE:
                            keys_to_drop = list(processed_signal_ids.keys())[:MAX_PROCESSED_SIGNALS_CACHE // 2]
                            for k in keys_to_drop: del processed_signal_ids[k]
                        processed_signal_ids[ms_key] = {'sig_id': _sig_pre_id, 'reason': 'processing'}

                direction='LONG' if is_long else 'SHORT'
                score_factor=min(abs(score-50)/50.0,1.0)

                # [L-1/L-5] Extrair features e calcular confidence para TODOS os sinais acionáveis
                now_dt   = datetime.utcnow()
                dq_score = get_dq_score(sym)
                mkt_open = market_open_for(mkt)
                price_dict = stock_prices.get(sym, {})
                sig_enriched = dict(sig)
                sig_enriched.update({
                    'price':        price,
                    'asset_type':   'stock',
                    'market_open':  mkt_open,
                    'trade_open':   sym in {t['symbol'] for t in stocks_open},
                    'atr_pct':      price_dict.get('atr_pct', 0.0),       # [v10.4]
                    'volume_ratio': price_dict.get('volume_ratio', 0.0),   # [v10.4]
                })
                features = extract_features(sig_enriched, dict(market_regime), dq_score, now_dt)
                features['_dq_score'] = dq_score
                feat_hash = make_feature_hash(features)
                conf      = calc_learning_confidence(sig_enriched, features, feat_hash)
                insight   = generate_insight(sig_enriched, features, feat_hash, conf)
                risk_mult = get_risk_multiplier(conf)

                # Filtros de execução — gravar signal_event + shadow antes de qualquer continue/break
                # [v10.6.3-Fix1] _confirmed_sig_id: começa com _sig_pre_id e é atualizado para o ID
                # real que o banco confirma via ON DUPLICATE KEY em record_signal_event().
                # Sem isso, o cache pode guardar o ID tentado em vez do ID persistido, causando
                # shadow_decisions ligados ao ID errado — simétrico ao fix de crypto em v10.6.2.
                _confirmed_sig_id = _sig_pre_id

                def _cache_reason(reason: str):
                    with learning_lock:
                        processed_signal_ids[ms_key] = {'sig_id': _confirmed_sig_id, 'reason': reason}

                if not mkt_open:
                    _confirmed_sig_id = record_signal_event(sig_enriched, features, feat_hash, conf, insight,
                                        source_type='stock_signal_db',
                                        existing_signal_id=_sig_pre_id,
                                        origin_signal_key=origin_key)
                    record_shadow_decision(_confirmed_sig_id, sig_enriched, 'market_closed')
                    _cache_reason('market_closed')
                    continue

                desired_pos=min(stocks_capital*(0.08+score_factor*0.07)*risk_mult, MAX_POSITION_STOCKS)
                risk_ok,risk_reason,approved_size=check_risk(sym,mkt,desired_pos,'stocks')
                if not risk_ok:
                    # [v10.3.4-F1] Preservar o motivo REAL do bloqueio, não colapsar em 'risk_blocked'
                    real_reason = risk_reason.split()[0] if risk_reason else 'risk_blocked'
                    # [v10.3.4-F5] DAILY/WEEKLY_DRAWDOWN dispara kill_switch internamente —
                    # tratar como permanente já no primeiro evento, sem esperar KILL_SWITCH_ACTIVE
                    is_permanent_risk = ('KILL_SWITCH' in risk_reason
                                         or risk_reason.startswith(('DAILY_DRAWDOWN', 'WEEKLY_DRAWDOWN')))
                    _confirmed_sig_id = record_signal_event(sig_enriched, features, feat_hash, conf, insight,
                                        source_type='stock_signal_db',
                                        existing_signal_id=_sig_pre_id,
                                        origin_signal_key=origin_key)
                    record_shadow_decision(_confirmed_sig_id, sig_enriched, real_reason)
                    _cache_reason('kill_switch' if is_permanent_risk else real_reason)
                    log.info(f'Risk-1 {sym}: {risk_reason}')
                    if is_permanent_risk: break
                    continue
                qty=int(approved_size/price)
                if qty<=0: continue

                # [V91-1] Gerar IDs ANTES do lock — trade já nasce com identidade formal
                # [L-2] Registrar signal_event com intenção de executar
                trade = None; pre_trade_id = gen_id('STK'); pre_order_id = gen_id('ORD')
                order_side = 'BUY' if direction=='LONG' else 'SELL'
                # [v10.3.2-P0-1] signal_id = retorno real do banco (via ON DUPLICATE KEY, pode ser o antigo)
                # [v10.6.3-Fix1] Atualizar _confirmed_sig_id para que _cache_reason use o ID correto
                signal_id  = record_signal_event(
                    sig_enriched, features, feat_hash, conf, insight,
                    source_type='stock_signal_db',
                    existing_signal_id=_sig_pre_id,
                    origin_signal_key=origin_key)
                _confirmed_sig_id = signal_id
                _cache_reason('executed')

                with state_lock:
                    ok2,reason2=_second_validation(sym,mkt,'stocks')
                    if ok2 and stocks_capital>=price*qty:
                        stocks_capital -= price*qty
                        # [V91-1] order_id já está no trade dentro do lock
                        trade = {
                            'id':pre_trade_id,'symbol':sym,'market':mkt,'asset_type':'stock',
                            'direction':direction,'entry_price':price,'current_price':price,
                            'quantity':qty,'position_value':round(price*qty,2),
                            'pnl':0,'pnl_pct':0,'peak_pnl_pct':0,'score':score,
                            'signal':signal_val,'order_id':pre_order_id,
                            'opened_at':datetime.utcnow().isoformat(),'status':'OPEN',
                            # [L-7] campos de attribution
                            'signal_id':           signal_id,
                            'feature_hash':        feat_hash,
                            'learning_confidence': conf.get('final_confidence'),
                            'insight_summary':     insight,
                            'learning_version':    LEARNING_VERSION,
                            '_features':           features,
                        }
                        stocks_open.append(trade)
                    else:
                        log.info(f'Risk-2 {sym}: {reason2 if not ok2 else "insufficient_capital"}')
                        # [L-8] Shadow: registrar sinal bloqueado no segundo nível
                        block_reason2 = reason2 if not ok2 else 'capital'
                        record_shadow_decision(signal_id, sig_enriched, block_reason2)
                        # [S3] symbol_duplicate é permanente para esta janela
                        if 'DUPLICATE' in (reason2 or '').upper():
                            with learning_lock:
                                processed_signal_ids[ms_key] = {'sig_id': signal_id, 'reason': 'symbol_duplicate'}
                        else:
                            with learning_lock:
                                processed_signal_ids[ms_key] = {'sig_id': signal_id, 'reason': block_reason2}

                if trade is None: continue

                # [FIX-2] Vincular trade_id e order_id ao signal_event imediatamente
                update_signal_attribution(signal_id, pre_trade_id, pre_order_id)

                # [V91-1] Fora do lock: criar ordem com o ID já definido, depois atualizar status
                order = create_order(pre_trade_id, sym, order_side, 'MARKET', qty, price, 'stocks',
                                     order_id_override=pre_order_id)
                update_order_status(order,'VALIDATED')
                update_order_status(order,'SENT')
                update_order_status(order,'FILLED',price,qty)

                audit('TRADE_OPENED',{'id':pre_trade_id,'symbol':sym,'direction':direction,'score':score,'pos':round(price*qty)})
                enqueue_persist('trade',trade)
                if score>=ALERT_MIN_SCORE: alert_signal(dict(sig))
                log.info(f'STK {sym} {direction} qty={qty} score={score}')
        except Exception as e: log.error(f'stock_execution_worker: {e}')

# ═══════════════════════════════════════════════════════════════
# [V9-1] CRYPTO AUTO-TRADE — create_order FORA do state_lock
# ═══════════════════════════════════════════════════════════════
def auto_trade_crypto():
    global crypto_capital
    while True:
        beat('auto_trade_crypto')
        time.sleep(90)
        beat('auto_trade_crypto')
        try:
            if market_regime.get('mode')=='HIGH_VOL':
                log.info('Crypto paused: HIGH_VOL'); continue
            for sym in CRYPTO_SYMBOLS:
                display=sym.replace('USDT',''); price=crypto_prices.get(sym,0)
                change_24h=crypto_momentum.get(sym,0)
                if price<=0 or abs(change_24h)<0.5: continue
                direction='LONG' if change_24h>0 else 'SHORT'

                # [v10.4] Score composto multi-fator (substitui change_24h * 5)
                ticker_data = crypto_tickers.get(sym, {})
                if ticker_data:
                    # [v10.6.2-Fix4] Cache unificado: usa _candles_cache com TTL=60min (klines diários).
                    # Elimina o segundo cache privado auto_trade_crypto._klines_cache — fonte única.
                    kline_cache_key = f'klines:{sym}'
                    klines_data = _get_cached_candles(kline_cache_key, ttl_min=60) or {}
                    if not klines_data:
                        klines_data = _fetch_binance_klines(sym, 22)
                        if klines_data:
                            _set_cached_candles(kline_cache_key, klines_data)
                    score = _crypto_composite_score(ticker_data, klines_data, direction)
                    # ATR pct e volume_ratio vindos dos klines
                    closes_k = klines_data.get('closes', [])
                    highs_k  = klines_data.get('highs', [])
                    lows_k   = klines_data.get('lows', [])
                    vols_k   = klines_data.get('volumes', [])
                    atr_c    = _calc_atr(closes_k, highs_k, lows_k, 14) if len(closes_k) >= 15 else 0.0
                    atr_pct_c = round((atr_c / price) * 100, 3) if price > 0 and atr_c > 0 else 0.0
                    avg_vol20_c = sum(vols_k[-20:]) / len(vols_k[-20:]) if len(vols_k) >= 20 else 0
                    vol_ratio_c = round(ticker_data.get('vol_quote', 0) / avg_vol20_c, 3) if avg_vol20_c > 0 else 0.0
                else:
                    # Fallback sem dados Binance
                    score = min(50 + int(abs(change_24h) * 5), 95)
                    if direction == 'SHORT': score = 100 - score
                    atr_pct_c = 0.0; vol_ratio_c = 0.0

                score_factor=min(abs(score-50)/50.0,1.0)

                # [v10.4-F2-dedup] Chave por janela de tempo de 90s — não por preço (instável em altcoins)
                time_window = int(time.time() / 90)   # muda a cada ciclo do loop
                ms_key_c = f"CRY:{display}:{direction}:{time_window}"
                origin_key_c = ms_key_c[:120]
                with learning_lock:
                    cached_c = processed_signal_ids.get(ms_key_c)

                if cached_c and cached_c['reason'] in ('executed', 'kill_switch'):
                    continue

                _sig_pre_id_c = cached_c['sig_id'] if cached_c else gen_id('SIG')
                if not cached_c:
                    with learning_lock:
                        if len(processed_signal_ids) >= MAX_PROCESSED_SIGNALS_CACHE:
                            keys_to_drop = list(processed_signal_ids.keys())[:MAX_PROCESSED_SIGNALS_CACHE // 2]
                            for k in keys_to_drop: del processed_signal_ids[k]
                        processed_signal_ids[ms_key_c] = {'sig_id': _sig_pre_id_c, 'reason': 'processing'}

                # [FIX-3][v10.4] Calcular features com ATR e volume_ratio
                now_dt_c   = datetime.utcnow()
                dq_score_c = get_dq_score(display)
                sig_enriched_c = {
                    'symbol': display, 'asset_type': 'crypto', 'market_type': 'CRYPTO',
                    'signal': 'COMPRA' if direction == 'LONG' else 'VENDA',
                    'score': score, 'price': price, 'rsi': 50,
                    'atr_pct': atr_pct_c,         # [v10.4]
                    'volume_ratio': vol_ratio_c,   # [v10.4]
                }
                features_c  = extract_features(sig_enriched_c, dict(market_regime), dq_score_c, now_dt_c)
                features_c['_dq_score'] = dq_score_c
                feat_hash_c = make_feature_hash(features_c)
                conf_c      = calc_learning_confidence(sig_enriched_c, features_c, feat_hash_c)
                insight_c   = generate_insight(sig_enriched_c, features_c, feat_hash_c, conf_c)
                risk_mult_c = get_risk_multiplier(conf_c)

                desired_pos=min(crypto_capital*(0.05+score_factor*0.05)*risk_mult_c,MAX_POSITION_CRYPTO)
                risk_ok,risk_reason,approved_size=check_risk(display,'CRYPTO',desired_pos,'crypto')

                if not risk_ok:
                    # [v10.3.3-F3] Motivo real preservado
                    real_reason_c = risk_reason.split()[0] if risk_reason else 'risk_blocked'
                    is_perm_c = 'KILL_SWITCH' in risk_reason or 'DRAWDOWN' in risk_reason
                    # [v10.6.4] Capturar ID real confirmado pelo banco — mesmo padrão do fix de stocks.
                    # record_signal_event pode retornar ID diferente de _sig_pre_id_c por ON DUPLICATE KEY.
                    confirmed_sig_id_c = record_signal_event(
                        sig_enriched_c, features_c, feat_hash_c, conf_c, insight_c,
                        source_type='crypto_derived',
                        existing_signal_id=_sig_pre_id_c,
                        origin_signal_key=origin_key_c)
                    record_shadow_decision(confirmed_sig_id_c, sig_enriched_c,
                                           'kill_switch' if is_perm_c else real_reason_c)
                    with learning_lock:
                        processed_signal_ids[ms_key_c] = {
                            'sig_id': confirmed_sig_id_c,
                            'reason': 'kill_switch' if is_perm_c else real_reason_c}
                    if is_perm_c: break
                    continue
                if approved_size<=0: continue

                # [V91-1] Gerar IDs ANTES do lock
                pre_trade_id = gen_id('CRY'); pre_order_id = gen_id('ORD')
                order_side   = 'BUY' if direction=='LONG' else 'SELL'
                trade = None; qty = 0
                # [v10.3.4-F1] existing_signal_id → síncrono → sig_id_c confirmado antes do attribution
                sig_id_c = record_signal_event(
                    sig_enriched_c, features_c, feat_hash_c, conf_c, insight_c,
                    source_type='crypto_derived',
                    existing_signal_id=_sig_pre_id_c,
                    origin_signal_key=origin_key_c)
                with learning_lock:
                    processed_signal_ids[ms_key_c] = {'sig_id': sig_id_c, 'reason': 'executed'}

                with state_lock:
                    ok2,reason2=_second_validation(display,'CRYPTO','crypto')
                    if ok2 and crypto_capital>=approved_size:
                        qty=approved_size/price; crypto_capital-=approved_size
                        trade={
                            'id':pre_trade_id,'symbol':display,'market':'CRYPTO','asset_type':'crypto',
                            'direction':direction,'entry_price':price,'current_price':price,
                            'quantity':round(qty,6),'position_value':round(approved_size,2),
                            'pnl':0,'pnl_pct':0,'peak_pnl_pct':0,'score':score,
                            'signal':'COMPRA' if direction=='LONG' else 'VENDA',
                            'order_id':pre_order_id,
                            'opened_at':datetime.utcnow().isoformat(),'status':'OPEN',
                            'signal_id':           sig_id_c,
                            'feature_hash':        feat_hash_c,
                            'learning_confidence': conf_c.get('final_confidence'),
                            'insight_summary':     insight_c,
                            'learning_version':    LEARNING_VERSION,
                            '_features':           features_c,
                        }
                        crypto_open.append(trade)
                    else:
                        # [v10.6.2-Fix1] 2ª validação falhou — sobrescrever 'executed' com o motivo real.
                        # Sem isso, o sinal fica marcado como 'executed' no dedup mesmo sem abrir trade,
                        # impedindo reavaliação futura. Padrão simétrico ao bloco stocks (linhas 3285-3290).
                        _c_block2 = reason2 if not ok2 else 'capital'
                        log.info(f'Crypto Risk-2 {display}: {_c_block2}')
                        record_shadow_decision(sig_id_c, sig_enriched_c, _c_block2)
                        is_perm_c = 'DUPLICATE' in (_c_block2 or '').upper()
                        with learning_lock:
                            processed_signal_ids[ms_key_c] = {
                                'sig_id': sig_id_c,
                                'reason': 'symbol_duplicate' if is_perm_c else _c_block2
                            }

                if trade is None: continue

                # [FIX-2] Vincular trade_id e order_id ao signal_event imediatamente
                update_signal_attribution(sig_id_c, pre_trade_id, pre_order_id)

                # [V91-1] Fora do lock: criar ordem com ID pré-definido
                order=create_order(pre_trade_id,display,order_side,'MARKET',round(qty,6),price,'crypto',
                                   order_id_override=pre_order_id)
                update_order_status(order,'VALIDATED')
                update_order_status(order,'SENT')
                update_order_status(order,'FILLED',price,round(qty,6))

                audit('TRADE_OPENED',{'id':pre_trade_id,'symbol':display,'direction':direction,'score':score})
                enqueue_persist('trade',trade)
        except Exception as e: log.error(f'auto_trade_crypto: {e}')

# ═══════════════════════════════════════════════════════════════
# ARBI ENGINE
# ═══════════════════════════════════════════════════════════════
ARBI_PAIRS = [
    {'id':'PETR4-PBR',   'leg_a':'PETR4.SA','leg_b':'PBR',    'mkt_a':'B3',  'mkt_b':'NYSE','fx':'USDBRL','name':'Petrobras',   'ratio_a':2,'ratio_b':1},
    {'id':'VALE3-VALE',  'leg_a':'VALE3.SA', 'leg_b':'VALE',   'mkt_a':'B3',  'mkt_b':'NYSE','fx':'USDBRL','name':'Vale',        'ratio_a':1,'ratio_b':1},
    {'id':'ITUB4-ITUB',  'leg_a':'ITUB4.SA', 'leg_b':'ITUB',   'mkt_a':'B3',  'mkt_b':'NYSE','fx':'USDBRL','name':'Itaú',        'ratio_a':1,'ratio_b':1},
    {'id':'BBDC4-BBD',   'leg_a':'BBDC4.SA', 'leg_b':'BBD',    'mkt_a':'B3',  'mkt_b':'NYSE','fx':'USDBRL','name':'Bradesco',    'ratio_a':1,'ratio_b':1},
    {'id':'ABEV3-ABEV',  'leg_a':'ABEV3.SA', 'leg_b':'ABEV',   'mkt_a':'B3',  'mkt_b':'NYSE','fx':'USDBRL','name':'Ambev',       'ratio_a':1,'ratio_b':1},
    {'id':'EMBR3-ERJ',   'leg_a':'EMBR3.SA', 'leg_b':'ERJ',    'mkt_a':'B3',  'mkt_b':'NYSE','fx':'USDBRL','name':'Embraer',     'ratio_a':4,'ratio_b':1},
    {'id':'GGBR4-GGB',   'leg_a':'GGBR4.SA', 'leg_b':'GGB',    'mkt_a':'B3',  'mkt_b':'NYSE','fx':'USDBRL','name':'Gerdau',      'ratio_a':1,'ratio_b':1},
    {'id':'CSNA3-SID',   'leg_a':'CSNA3.SA', 'leg_b':'SID',    'mkt_a':'B3',  'mkt_b':'NYSE','fx':'USDBRL','name':'CSN',         'ratio_a':1,'ratio_b':1},
    {'id':'CMIG4-CIG',   'leg_a':'CMIG4.SA', 'leg_b':'CIG',    'mkt_a':'B3',  'mkt_b':'NYSE','fx':'USDBRL','name':'Cemig',       'ratio_a':1,'ratio_b':1},
    {'id':'CPLE6-ELP',   'leg_a':'CPLE6.SA', 'leg_b':'ELP',    'mkt_a':'B3',  'mkt_b':'NYSE','fx':'USDBRL','name':'Copel',       'ratio_a':1,'ratio_b':1},
    {'id':'BP-BP.L',     'leg_a':'BP',       'leg_b':'BP.L',   'mkt_a':'NYSE','mkt_b':'LSE', 'fx':'GBPUSD','name':'BP',          'ratio_a':1,'ratio_b':6},
    {'id':'SHEL-SHEL.L', 'leg_a':'SHEL',     'leg_b':'SHEL.L', 'mkt_a':'NYSE','mkt_b':'LSE', 'fx':'GBPUSD','name':'Shell',       'ratio_a':1,'ratio_b':2},
    {'id':'AZN-AZN.L',   'leg_a':'AZN',      'leg_b':'AZN.L',  'mkt_a':'NYSE','mkt_b':'LSE', 'fx':'GBPUSD','name':'AstraZeneca', 'ratio_a':1,'ratio_b':1},
    {'id':'GSK-GSK.L',   'leg_a':'GSK',      'leg_b':'GSK.L',  'mkt_a':'NYSE','mkt_b':'LSE', 'fx':'GBPUSD','name':'GSK',         'ratio_a':1,'ratio_b':2},
    {'id':'HSBC-HSBA.L', 'leg_a':'HSBC',     'leg_b':'HSBA.L', 'mkt_a':'NYSE','mkt_b':'LSE', 'fx':'GBPUSD','name':'HSBC',        'ratio_a':1,'ratio_b':5},
    {'id':'TCEHY-0700',  'leg_a':'TCEHY',    'leg_b':'0700.HK','mkt_a':'NYSE','mkt_b':'HKEX','fx':'HKDUSD','name':'Tencent',     'ratio_a':1,'ratio_b':1},
    {'id':'BABA-9988',   'leg_a':'BABA',     'leg_b':'9988.HK','mkt_a':'NYSE','mkt_b':'HKEX','fx':'HKDUSD','name':'Alibaba',     'ratio_a':1,'ratio_b':8},
    {'id':'HSBC-0005',   'leg_a':'HSBC',     'leg_b':'0005.HK','mkt_a':'NYSE','mkt_b':'HKEX','fx':'HKDUSD','name':'HSBC HK',    'ratio_a':1,'ratio_b':5},
    {'id':'CHL-0941',    'leg_a':'CHL',      'leg_b':'0941.HK','mkt_a':'NYSE','mkt_b':'HKEX','fx':'HKDUSD','name':'China Mobile','ratio_a':1,'ratio_b':5},
    {'id':'PING-2318',   'leg_a':'PING',     'leg_b':'2318.HK','mkt_a':'NYSE','mkt_b':'HKEX','fx':'HKDUSD','name':'Ping An',    'ratio_a':1,'ratio_b':5},
]

def _fetch_arbi_price(symbol: str) -> float:
    """[v10.4][v10.6-P4] Preço para arbitragem com ADR fallback para legs B3.
    Cadência: Binance (crypto) → Polygon (US + ADR de B3) → brapi (B3) → FMP → Yahoo.
    """
    display = symbol.replace('.SA', '')
    is_b3_sym = symbol.endswith('.SA') or display in {s.replace('.SA','') for s in STOCK_SYMBOLS_B3}

    # Binance para crypto
    if symbol.endswith('USDT') or symbol in CRYPTO_SYMBOLS:
        try:
            r = requests.get('https://api.binance.com/api/v3/ticker/price',
                             params={'symbol': symbol}, timeout=5)
            if r.status_code == 200:
                p = float(r.json().get('price', 0))
                if p > 0: return p
        except: pass

    # brapi primário para B3 (snapshot, usa cache)
    if is_b3_sym and BRAPI_TOKEN:
        result, _ = _fetch_brapi_stock(display)
        if result and result.get('price', 0) > 0:
            return result['price']

    # [v10.6-P4] Para B3 sem brapi: tentar ADR via Polygon com conversão USD→BRL
    if is_b3_sym and POLYGON_API_KEY:
        adr_sym = B3_TO_ADR.get(display)
        if adr_sym:
            result, _ = _fetch_polygon_stock(adr_sym)
            if result and result.get('price', 0) > 0:
                usd_brl = fx_rates.get('USDBRL', 5.8)
                return round(result['price'] * usd_brl, 2)

    # Polygon para equity US direto
    if not is_b3_sym and POLYGON_API_KEY:
        result, _ = _fetch_polygon_stock(display)
        if result and result.get('price', 0) > 0:
            return result['price']

    # FMP fallback universal
    if FMP_API_KEY:
        try:
            r = requests.get(
                f'https://financialmodelingprep.com/api/v3/quote/{display}',
                params={'apikey': FMP_API_KEY}, timeout=6)
            if r.status_code == 200:
                d = r.json()
                if d and isinstance(d, list):
                    p = float(d[0].get('price') or 0)
                    if p > 0: return p
        except: pass

    # Yahoo último recurso
    try:
        r = requests.get(
            f'https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=1d',
            headers={'User-Agent': 'Mozilla/5.0'}, timeout=6)
        if r.status_code == 200:
            return r.json()['chart']['result'][0]['meta'].get('regularMarketPrice', 0)
    except: pass
    return 0

def calc_spread(pair):
    try:
        pa_raw=_fetch_arbi_price(pair['leg_a']); pb_raw=_fetch_arbi_price(pair['leg_b'])
        if pa_raw<=0 or pb_raw<=0: return None
        fx=pair['fx']; ra=pair.get('ratio_a',1); rb=pair.get('ratio_b',1)
        if fx=='USDBRL':   rate=fx_rates.get('USDBRL',5.8); pa=(pa_raw/rate)*ra; pb=pb_raw*rb
        elif fx=='GBPUSD': rate=fx_rates.get('GBPUSD',1.27); pa=pa_raw*ra; pb=(pb_raw/100*rate)*rb
        elif fx=='HKDUSD': rate=fx_rates.get('HKDUSD',7.8); pa=pa_raw*ra; pb=(pb_raw/rate)*rb
        else:              pa=pa_raw*ra; pb=pb_raw*rb
        if pb<=0: return None
        spread_pct=((pa-pb)/pb)*100
        return {'pair_id':pair['id'],'name':pair['name'],'leg_a':pair['leg_a'],'leg_b':pair['leg_b'],
            'mkt_a':pair['mkt_a'],'mkt_b':pair['mkt_b'],'price_a':round(pa_raw,4),'price_b':round(pb_raw,4),
            'price_a_usd':round(pa,4),'price_b_usd':round(pb,4),'spread_pct':round(spread_pct,2),
            'abs_spread':round(abs(spread_pct),2),'fx_rate':fx_rates.get(fx,0),'fx_pair':fx,
            'ratio_a':ra,'ratio_b':rb,'opportunity':abs(spread_pct)>=ARBI_MIN_SPREAD,
            'direction':'LONG_A' if spread_pct<0 else 'LONG_B',
            'markets_open':market_open_for(pair['mkt_a']) and market_open_for(pair['mkt_b']),
            'updated_at':datetime.utcnow().isoformat()}
    except Exception as e: log.error(f'Spread {pair["id"]}: {e}'); return None

def arbi_scan_loop():
    global arbi_capital
    while True:
        beat('arbi_scan_loop')
        try:
            fetch_fx_rates()
            for pair in ARBI_PAIRS:
                beat('arbi_scan_loop')
                spread=calc_spread(pair)
                if not spread:
                    time.sleep(1); continue

                with state_lock: arbi_spreads[pair['id']]=spread

                if not spread['opportunity'] or not spread['markets_open']:
                    time.sleep(1.5); continue

                risk_ok,risk_reason,approved_size=check_risk_arbi(pair['id'],ARBI_POS_SIZE)
                if not risk_ok:
                    if 'KILL_SWITCH' in risk_reason: break
                    time.sleep(1.5); continue

                bl=pair['leg_a'] if spread['direction']=='LONG_A' else pair['leg_b']
                sl=pair['leg_b'] if spread['direction']=='LONG_A' else pair['leg_a']
                bm=pair['mkt_a'] if spread['direction']=='LONG_A' else pair['mkt_b']
                sm=pair['mkt_b'] if spread['direction']=='LONG_A' else pair['mkt_a']
                trade_id=gen_id('ARB'); opened=False; pos=0

                with state_lock:
                    if any(t['pair_id']==pair['id'] for t in arbi_open): pass
                    elif not (market_open_for(pair['mkt_a']) and market_open_for(pair['mkt_b'])): pass
                    elif approved_size<=0 or arbi_capital<=0: pass
                    else:
                        pos=min(approved_size,arbi_capital); arbi_capital-=pos
                        trade={'id':trade_id,'pair_id':pair['id'],'name':pair['name'],
                            'leg_a':pair['leg_a'],'leg_b':pair['leg_b'],
                            'mkt_a':pair['mkt_a'],'mkt_b':pair['mkt_b'],
                            'direction':spread['direction'],'buy_leg':bl,'buy_mkt':bm,
                            'short_leg':sl,'short_mkt':sm,'entry_spread':spread['spread_pct'],
                            'current_spread':spread['spread_pct'],'position_size':round(pos,2),
                            'pnl':0,'pnl_pct':0,'peak_pnl_pct':0,'fx_rate':spread['fx_rate'],
                            'opened_at':datetime.utcnow().isoformat(),'status':'OPEN','asset_type':'arbitrage'}
                        arbi_open.append(trade); opened=True

                if opened:
                    audit('ARBI_OPENED',{'id':trade_id,'pair':pair['id'],'spread':spread['abs_spread']})
                    enqueue_persist('arbi',trade)
                    send_whatsapp(f"ARBI: {pair['name']} spread {spread['abs_spread']:.2f}% ${pos:,.0f}")

                time.sleep(1.5)
        except Exception as e: log.error(f'arbi_scan: {e}')

        beat('arbi_scan_loop')
        time.sleep(300)
        beat('arbi_scan_loop')

def arbi_monitor_loop():
    global arbi_capital
    while True:
        beat('arbi_monitor_loop')
        time.sleep(60)
        try:
            closed_trades=[]
            with state_lock:
                now=datetime.utcnow(); to_close=[]
                for trade in arbi_open:
                    age_h=(now-datetime.fromisoformat(trade['opened_at'])).total_seconds()/3600
                    sd=arbi_spreads.get(trade['pair_id'])
                    if sd:
                        trade['current_spread']=sd['spread_pct']
                        ea=abs(float(trade['entry_spread'])); ca=abs(float(trade['current_spread']))
                        trade['pnl_pct']=round(ea-ca,4)
                        trade['pnl']=round(trade['pnl_pct']/100*float(trade['position_size']),2)
                    trade['peak_pnl_pct']=round(max(trade.get('peak_pnl_pct',0),trade['pnl_pct']),2)
                    peak=trade['peak_pnl_pct']
                    h=trade.setdefault('pnl_history',[]); h.append(trade['pnl_pct'])
                    if len(h)>5: h.pop(0)
                    reason=None
                    if abs(trade.get('current_spread',99))<=ARBI_TP_SPREAD:  reason='TAKE_PROFIT'
                    elif peak>=2.0 and trade['pnl_pct']<=peak-1.0:           reason='TRAILING_STOP'
                    elif trade['pnl_pct']<=-ARBI_SL_PCT:                     reason='STOP_LOSS'
                    elif age_h>=ARBI_TIMEOUT_H:
                        ext=trade.get('extensions',0)
                        if is_momentum_positive(trade) and ext<3: trade['extensions']=ext+1
                        else: reason='TIMEOUT'
                    if reason:
                        arbi_capital+=trade['position_size']+trade['pnl']
                        c=dict(trade); c.update({'closed_at':now.isoformat(),'close_reason':reason,'status':'CLOSED'})
                        arbi_closed.insert(0,c)
                        if len(arbi_closed) > MAX_CLOSED_HISTORY: arbi_closed.pop()   # [v10.7-Fix3]
                        to_close.append(trade['id']); closed_trades.append(c)
                arbi_open[:] = [t for t in arbi_open if t['id'] not in to_close]

            for c in closed_trades:
                audit('ARBI_CLOSED',{'id':c['id'],'pair':c['pair_id'],'pnl':c['pnl'],'reason':c['close_reason']})
                enqueue_persist('arbi',c)
        except Exception as e: log.error(f'arbi_monitor: {e}')

# ═══════════════════════════════════════════════════════════════
# [C-1] WATCHDOG — timeout por thread + [V9-3] _check_degraded
# ═══════════════════════════════════════════════════════════════
def watchdog():
    while True:
        beat('watchdog')
        time.sleep(30)
        now=time.time()

        # [V9-3] Atualizar modo degradado a cada ciclo do watchdog
        try: _check_degraded()
        except Exception as e: log.error(f'watchdog _check_degraded: {e}')

        # [V91-3] Alerta de fila crítica direto no watchdog — não depende do persistence_worker
        qsize = urgent_queue.qsize()
        if qsize >= URGENT_QUEUE_CRIT:
            global _queue_alert_last
            if now - _queue_alert_last > 300:
                _queue_alert_last = now
                log.critical(f'[V91-3] WATCHDOG: urgent_queue CRÍTICA {qsize} itens — DB pode estar travado')
                send_whatsapp(f'CRÍTICO (watchdog): fila de persistência com {qsize} itens. Verificar banco.')

        for name, t in list(thread_health.items()):
            if name=='watchdog': continue
            alive   = t.is_alive()
            hb      = thread_heartbeat.get(name, now)
            timeout = THREAD_HEARTBEAT_TIMEOUT.get(name, DEFAULT_HB_TIMEOUT)
            hb_ok   = (now-hb) < timeout

            if alive and hb_ok:
                last_restart=thread_last_restart.get(name,0)
                if (thread_restart_count.get(name,0)>0 and last_restart>0 and
                        (now-last_restart)/3600 >= WATCHDOG_RESET_STABLE_H):
                    old=thread_restart_count[name]; thread_restart_count[name]=0
                    log.info(f'WATCHDOG: {name} stable {WATCHDOG_RESET_STABLE_H}h — reset count (was {old})')
                continue

            problem='DEAD' if not alive else f'FROZEN (no beat for {now-hb:.0f}s, timeout={timeout}s)'
            count=thread_restart_count.get(name,0)
            log.error(f'WATCHDOG: {name} {problem} (restart #{count+1})')

            if count>=3:
                log.critical(f'WATCHDOG: {name} failed 3x — activating kill switch')
                global RISK_KILL_SWITCH
                RISK_KILL_SWITCH=True
                send_whatsapp(f'CRITICO: thread {name} falhou 3x ({problem}). Kill switch ativado.')
                thread_restart_count[name]=0
                continue

            fn=thread_fns.get(name)
            if fn:
                try:
                    new_t=threading.Thread(target=fn,daemon=True); new_t.start()
                    thread_health[name]=new_t
                    thread_restart_count[name]=count+1
                    thread_last_restart[name]=now
                    thread_heartbeat[name]=now
                    log.warning(f'WATCHDOG: {name} restarted (attempt {count+1})')
                    send_whatsapp(f'ALERTA: {name} ({problem}) reiniciada (tentativa {count+1})')
                except Exception as e: log.error(f'WATCHDOG restart {name}: {e}')

def start_background_threads():
    defs = {
        'stock_price_loop':       stock_price_loop,
        'crypto_price_loop':      crypto_price_loop,
        'monitor_trades':         monitor_trades,
        'auto_trade_crypto':      auto_trade_crypto,
        'stock_execution_worker': stock_execution_worker,
        'arbi_scan_loop':         arbi_scan_loop,
        'arbi_monitor_loop':      arbi_monitor_loop,
        'snapshot_loop':          snapshot_loop,
        'persistence_worker':     persistence_worker,
        'alert_worker':           alert_worker,
        'watchdog':               watchdog,
        'shadow_evaluator_loop':  shadow_evaluator_loop,   # [FIX-5]
    }
    now=time.time()
    for name,fn in defs.items():
        thread_fns[name]=fn; thread_restart_count[name]=0
        thread_last_restart[name]=0; thread_heartbeat[name]=now
        t=threading.Thread(target=fn,daemon=True); t.start()
        thread_health[name]=t
        log.info(f'Thread started: {name} (hb_timeout={THREAD_HEARTBEAT_TIMEOUT.get(name,DEFAULT_HB_TIMEOUT)}s)')

# ═══════════════════════════════════════════════════════════════
# WATCHLIST
# ═══════════════════════════════════════════════════════════════
watchlist_symbols=[]
watchlist_lock=threading.Lock()

def init_watchlist_table():
    global watchlist_symbols
    conn=get_db()
    if not conn: return
    try:
        cursor=conn.cursor(dictionary=True)
        cursor.execute("SELECT symbol, market, added_at FROM watchlist")
        watchlist_symbols=[{'symbol':r['symbol'],'market':r['market'],
            'addedAt':r['added_at'].isoformat() if r['added_at'] else ''} for r in cursor.fetchall()]
        cursor.close(); conn.close()
        log.info(f'Watchlist: {len(watchlist_symbols)} loaded')
    except Exception as e: log.error(f'Watchlist init: {e}')

@app.route('/watchlist/quote')
def watchlist_quote():
    symbol=request.args.get('symbol','').upper().strip()
    market=request.args.get('market','US').upper()
    if not symbol: return jsonify({'error':'symbol required'}),400
    cached=stock_prices.get(symbol)
    if cached and cached.get('price',0)>0:
        return jsonify({'symbol':symbol,'name':symbol,'price':cached['price'],'change':0,
            'change_pct':cached.get('change_pct',0),'rsi':cached.get('rsi',50),
            'ema9':cached.get('ema9',0),'ema21':cached.get('ema21',0),
            'ema9_real':cached.get('ema9_real',False),'ema50_real':cached.get('ema50_real',False),
            'candles':cached.get('candles_available',0),
            'atr_pct':cached.get('atr_pct',0.0),             # [v10.4]
            'volume_ratio':cached.get('volume_ratio',0.0),   # [v10.4]
            'source':cached.get('source','cache'),
            'currency':'BRL' if market=='B3' else 'USD','market':market})
    # [v10.4] Usar camada unificada Polygon→brapi→FMP→Yahoo
    try:
        sym_fetch = symbol+'.SA' if market=='B3' else symbol
        result, _ = _fetch_single_stock(sym_fetch)
        if result and result.get('price',0)>0:
            price=result['price']; prev=result.get('prev',0)
            return jsonify({'symbol':symbol,'name':symbol,
                'price':price,'change':round(price-prev,4) if prev>0 else 0,
                'change_pct':result.get('change_pct',0),
                'rsi':result.get('rsi',50),'ema9':result.get('ema9',0),'ema21':result.get('ema21',0),
                'ema9_real':result.get('ema9_real',False),'ema50_real':result.get('ema50_real',False),
                'candles':result.get('candles_available',0),
                'atr_pct':result.get('atr_pct',0.0),
                'volume_ratio':result.get('volume_ratio',0.0),
                'source':result.get('source','live'),
                'currency':'BRL' if market=='B3' else 'USD','market':market})
        return jsonify({'error':'price unavailable'}),400
    except Exception as e: return jsonify({'error':str(e)}),500

@app.route('/watchlist/add', methods=['POST'])
def watchlist_add():
    data=request.get_json() or {}
    symbol=data.get('symbol','').upper().strip(); market=data.get('market','US').upper()
    if not symbol: return jsonify({'error':'symbol required'}),400
    with watchlist_lock:
        if any(w['symbol']==symbol for w in watchlist_symbols):
            return jsonify({'ok':True,'total':len(watchlist_symbols),'msg':'already exists'})
        conn=get_db()
        if conn:
            try:
                cursor=conn.cursor()
                cursor.execute("INSERT IGNORE INTO watchlist (symbol,market) VALUES (%s,%s)",(symbol,market))
                conn.commit(); cursor.close(); conn.close()
            except Exception as e: log.error(f'Watchlist add DB: {e}')
        watchlist_symbols.append({'symbol':symbol,'market':market,'addedAt':datetime.utcnow().isoformat()})
    return jsonify({'ok':True,'total':len(watchlist_symbols)})

@app.route('/watchlist/remove', methods=['POST'])
def watchlist_remove():
    global watchlist_symbols
    data=request.get_json() or {}; symbol=data.get('symbol','').upper().strip()
    with watchlist_lock:
        conn=get_db()
        if conn:
            try:
                cursor=conn.cursor()
                cursor.execute("DELETE FROM watchlist WHERE symbol=%s",(symbol,))
                conn.commit(); cursor.close(); conn.close()
            except Exception as e: log.error(f'Watchlist remove DB: {e}')
        watchlist_symbols=[w for w in watchlist_symbols if w['symbol']!=symbol]
    return jsonify({'ok':True,'total':len(watchlist_symbols)})

@app.route('/watchlist')
def watchlist_get():
    with watchlist_lock: syms=list(watchlist_symbols)
    return jsonify({'symbols':syms,'total':len(syms)})

# ═══════════════════════════════════════════════════════════════
# ROUTES
# ═══════════════════════════════════════════════════════════════
@app.route('/health')
def health():
    with state_lock: open_count=len(stocks_open)+len(crypto_open)
    now=time.time()
    hb_status={}
    for k,t in thread_health.items():
        timeout=THREAD_HEARTBEAT_TIMEOUT.get(k,DEFAULT_HB_TIMEOUT)
        hb_age=round(now-thread_heartbeat.get(k,now),1)
        hb_status[k]={'alive':t.is_alive(),'hb_age_s':hb_age,'timeout_s':timeout,
            'frozen': hb_age>timeout,'restarts':thread_restart_count.get(k,0)}
    return jsonify({
        'status':'ok','db':'connected' if test_db() else 'unavailable',
        'open_trades':open_count,'kill_switch':RISK_KILL_SWITCH,'arbi_kill_switch':ARBI_KILL_SWITCH,
        'market_regime':market_regime,'alerts':ALERTS_ENABLED,
        'stock_prices_cached':len(stock_prices),'crypto_prices_cached':len(crypto_prices),
        'persist_queue_size':urgent_queue.qsize(),
        'persist_queue_warn':URGENT_QUEUE_WARN,'persist_queue_crit':URGENT_QUEUE_CRIT,
        'alert_queue_size':alert_queue.qsize(),
        'degraded': _read_degraded(),   # [V91-5]
        'learning_degraded': LEARNING_DEGRADED,   # [L-10]
        'threads':hb_status,'timestamp':datetime.utcnow().isoformat()
    })

@app.route('/')
def index():
    return jsonify({
        'service':'Egreja Investment AI','version':'10.7.0','status':'online',
        'kill_switch':RISK_KILL_SWITCH,'arbi_kill_switch':ARBI_KILL_SWITCH,
        'market_regime':market_regime.get('mode','UNKNOWN'),
        'market_status':{'b3':is_b3_open(),'nyse':is_nyse_open(),'lse':is_lse_open(),'hkex':is_hkex_open(),'crypto':True},
        'deploy_mode':'single-process',
        'degraded': _read_degraded()['active'],   # [V91-5] flag rápida
    })

@app.route('/degraded')
def degraded_route():
    """[V9-3][V91-5] Estado degradado do sistema — público."""
    return jsonify({
        **_read_degraded(),
        'learning_degraded':   LEARNING_DEGRADED,   # [L-10]
        'learning_errors':     learning_errors,
        'queue_warn_threshold': URGENT_QUEUE_WARN,
        'queue_crit_threshold': URGENT_QUEUE_CRIT,
        'timestamp': datetime.utcnow().isoformat(),
    })

@app.route('/debug')
def debug():
    now=time.time()
    return jsonify({
        'db_status':'connected' if test_db() else 'unavailable',
        'stock_prices_cached':len(stock_prices),'crypto_prices_cached':len(crypto_prices),
        'alerts_enabled':ALERTS_ENABLED,'kill_switch':RISK_KILL_SWITCH,'arbi_kill_switch':ARBI_KILL_SWITCH,
        'market_regime':market_regime,
        'degraded': _read_degraded(),   # [V91-5]
        'risk_limits':{'max_open':MAX_OPEN_POSITIONS,'max_daily_dd_pct':MAX_DAILY_DRAWDOWN_PCT,
            'max_weekly_dd_pct':MAX_WEEKLY_DRAWDOWN_PCT,'max_risk_per_trade_pct':MAX_RISK_PER_TRADE_PCT,
            'signal_max_age_min':SIGNAL_MAX_AGE_MIN,'cooldown_sec':SYMBOL_COOLDOWN_SEC,
            'max_positions_stocks':MAX_POSITIONS_STOCKS,'max_positions_crypto':MAX_POSITIONS_CRYPTO},
        'arbi_limits':{'max_positions':ARBI_MAX_POSITIONS,'min_spread':ARBI_MIN_SPREAD,
            'tp_spread':ARBI_TP_SPREAD,'sl_pct':ARBI_SL_PCT,'timeout_h':ARBI_TIMEOUT_H},
        'queue_limits':{'warn':URGENT_QUEUE_WARN,'crit':URGENT_QUEUE_CRIT,
            'current':urgent_queue.qsize()},
        'market_status':{'b3':is_b3_open(),'nyse':is_nyse_open(),'lse':is_lse_open(),'hkex':is_hkex_open()},
        'threads':{k:{'alive':t.is_alive(),'hb_age_s':round(now-thread_heartbeat.get(k,now),1),
            'timeout_s':THREAD_HEARTBEAT_TIMEOUT.get(k,DEFAULT_HB_TIMEOUT),
            'restarts':thread_restart_count.get(k,0)}
            for k,t in thread_health.items()},
        'env':{k:os.environ.get(k,'NOT SET') for k in ['MYSQLHOST','MYSQLPORT','MYSQLDATABASE','PORT','ENV','WEB_CONCURRENCY']}
    })

@app.route('/signals')
def signals():
    conn=get_db()
    if not conn: return jsonify({'error':'Database unavailable'}),503
    try:
        cursor=conn.cursor(dictionary=True)
        cutoff=(datetime.utcnow()-timedelta(minutes=SIGNAL_MAX_AGE_MIN)).strftime('%Y-%m-%d %H:%M:%S')
        cursor.execute('SELECT * FROM market_signals WHERE created_at>=%s ORDER BY score DESC LIMIT 500',(cutoff,))
        rows=cursor.fetchall(); cursor.close(); conn.close()
        for row in rows:
            for k,v in row.items():
                if isinstance(v,datetime): row[k]=v.isoformat()
            row['asset_type']='stock'
        with state_lock:
            open_stock_syms  = {t['symbol'] for t in stocks_open}
            open_crypto_syms = {t['symbol'] for t in crypto_open}
        for sig in rows:
            sig['trade_open']=sig['symbol'] in open_stock_syms
            sig['market_open']=market_open_for(sig.get('market_type',''))
            cached=stock_prices.get(sig['symbol'])
            if cached:
                sig['price']=cached['price']; sig['rsi']=cached.get('rsi',sig.get('rsi',50))
                sig['ema9']=cached.get('ema9',sig.get('ema9',0)); sig['ema21']=cached.get('ema21',sig.get('ema21',0))
                sig['ema50']=cached.get('ema50',sig.get('ema50',0))
                sig['ema50_real']=cached.get('ema50_real',False); sig['rsi_real']=cached.get('rsi_real',False)
        crypto_signals=[]
        for sym in CRYPTO_SYMBOLS:
            display=sym.replace('USDT',''); price=crypto_prices.get(sym,0)
            if price<=0: continue
            change_24h=crypto_momentum.get(sym,0); strength=abs(change_24h)
            if strength < 0.5:
                score = 50; signal = 'MANTER'
            else:
                direction_str = 'LONG' if change_24h > 0 else 'SHORT'
                # [v10.5-3] Usar _crypto_composite_score real — mesmo motor da execução
                ticker_data = crypto_tickers.get(sym, {})
                kline_cache_key = f'klines:{sym}'
                # [v10.6.2-Fix4] Mesma fonte única de klines — _candles_cache TTL=60min
                klines_data = _get_cached_candles(kline_cache_key, ttl_min=60) or {}
                if ticker_data and klines_data:
                    score = _crypto_composite_score(ticker_data, klines_data, direction_str)
                else:
                    # fallback se klines ainda não carregadas (startup)
                    base  = min(50 + int(strength * 5), 95)
                    score = base if change_24h > 0 else (100 - base)
                signal = 'COMPRA' if score >= 70 else ('VENDA' if score <= 30 else 'MANTER')

            crypto_signals.append({
                'symbol':display,'price':price,'signal':signal,'score':score,
                'market_type':'CRYPTO','asset_type':'crypto',
                'name':CRYPTO_NAMES.get(sym,display),'rsi':round(max(10,min(90,50+change_24h*3)),1),
                'change_24h':round(change_24h,2),'ema50_real':False,'rsi_real':False,
                'atr_pct':   crypto_tickers.get(sym,{}).get('atr_pct', 0.0),      # [v10.5-3]
                'vol_ratio': crypto_tickers.get(sym,{}).get('vol_ratio', 0.0),     # [v10.5-3]
                'created_at':datetime.utcnow().isoformat(),'trade_open':display in open_crypto_syms
            })
        all_signals=rows+crypto_signals
        return jsonify({'status':'OK','timestamp':datetime.utcnow().isoformat(),
            'total':len(all_signals),'stocks_count':len(rows),'crypto_count':len(crypto_signals),
            'market_status':{'b3':is_b3_open(),'nyse':is_nyse_open(),'crypto':True},
            'market_regime':market_regime,'signals':all_signals})
    except Exception as e: return jsonify({'error':str(e)}),500

@app.route('/prices/live')
def prices_live():
    with state_lock:
        trades=[{'id':t['id'],'symbol':t['symbol'],
            'current_price':t.get('current_price',t.get('entry_price',0)),
            'pnl':t.get('pnl',0),'pnl_pct':t.get('pnl_pct',0),
            'peak_pnl_pct':t.get('peak_pnl_pct',0),'direction':t.get('direction','LONG')}
            for t in stocks_open+crypto_open]
        crypto_snap={k.replace('USDT',''):v for k,v in crypto_prices.items()}
    return jsonify({'timestamp':datetime.utcnow().isoformat(),'trades':trades,'crypto_prices':crypto_snap})


@app.route('/prices/crypto')
def prices_crypto():
    with state_lock:
        result = {}
        for sym, data in crypto_tickers.items():
            clean = sym.replace('USDT','')
            result[clean] = {
                'price': data.get('price', 0),
                'change_24h': data.get('change_pct', 0),
                'high_24h': data.get('high_24h', 0),
                'low_24h': data.get('low_24h', 0),
            }
        trades = [{'id':t['id'],'symbol':t['symbol'],
            'current_price':t.get('current_price',t.get('entry_price',0)),
            'pnl':t.get('pnl',0),'pnl_pct':t.get('pnl_pct',0),
            'direction':t.get('direction','LONG')}
            for t in crypto_open]
    return jsonify({'prices': result, 'trades': trades, 'ts': datetime.utcnow().isoformat()})

@app.route('/trades/open')
def trades_open():
    with state_lock: data=stocks_open+crypto_open
    return jsonify({'trades':data,'total':len(data)})

@app.route('/trades/closed')
def trades_closed():
    with state_lock:
        data=sorted(stocks_closed+crypto_closed,key=lambda x:x.get('closed_at',''),reverse=True)[:100]
    return jsonify({'trades':data,'total':len(stocks_closed)+len(crypto_closed)})

@app.route('/trades')
def trades():
    with state_lock: all_t=stocks_open+crypto_open+stocks_closed[:50]+crypto_closed[:50]
    return jsonify({'trades':all_t,'total':len(all_t)})

@app.route('/stats')
def stats():
    with state_lock:
        s_op=sum(t.get('pnl',0) for t in stocks_open); s_cl=sum(t.get('pnl',0) for t in stocks_closed)
        s_win=sum(1 for t in stocks_closed if t.get('pnl',0)>0)
        s_val=sum(t.get('current_price',t.get('entry_price',0))*t.get('quantity',0) for t in stocks_open)
        c_op=sum(t.get('pnl',0) for t in crypto_open); c_cl=sum(t.get('pnl',0) for t in crypto_closed)
        c_win=sum(1 for t in crypto_closed if t.get('pnl',0)>0)
        c_val=sum(t.get('current_price',t.get('entry_price',0))*t.get('quantity',0) for t in crypto_open)
        a_op=sum(t.get('pnl',0) for t in arbi_open); a_cl=sum(t.get('pnl',0) for t in arbi_closed)
        a_win=sum(1 for t in arbi_closed if t.get('pnl',0)>0)
        sc=stocks_capital; cc=crypto_capital; ac=arbi_capital
        all_cl=stocks_closed+crypto_closed; pnls=[t.get('pnl',0) for t in all_cl]
        d_pnl=calc_period_pnl(all_cl,1); w_pnl=calc_period_pnl(all_cl,7)
        m_pnl=calc_period_pnl(all_cl,30); y_pnl=calc_period_pnl(all_cl,365)
    st=sc+s_val; ct=cc+c_val
    # [V9-4] core_portfolio_value = stocks+crypto apenas (arbi é segregado, não entra)
    core_total=round(st+ct,2); arbi_total=round(ac,2)
    initial_global=INITIAL_CAPITAL_STOCKS+INITIAL_CAPITAL_CRYPTO
    total_cl_n=len(stocks_closed)+len(crypto_closed); total_win=s_win+c_win
    return jsonify({
        # ─── GLOBAL (stocks + crypto) — arbi NÃO entra aqui ────
        'initial_capital':initial_global,
        'core_portfolio_value':core_total,        # [V9-4] nome claro: só stocks+crypto
        'total_portfolio_value':core_total,        # alias backward-compat
        'open_positions_value':round(s_val+c_val,2),'current_capital':round(sc+cc,2),
        'total_pnl':round(s_op+s_cl+c_op+c_cl,2),
        'open_pnl':round(s_op+c_op,2),'closed_pnl':round(s_cl+c_cl,2),
        'gain_percent':round((core_total-initial_global)/initial_global*100,2),
        'open_trades':len(stocks_open)+len(crypto_open),
        'closed_trades':total_cl_n,'winning_trades':total_win,
        'win_rate':round(total_win/total_cl_n*100,1) if total_cl_n>0 else 0,
        'daily_pnl':d_pnl,'weekly_pnl':w_pnl,'monthly_pnl':m_pnl,'annual_pnl':y_pnl,
        'daily_gain_pct':round(d_pnl/initial_global*100,3),
        'monthly_gain_pct':round(m_pnl/initial_global*100,2),
        'annual_gain_pct':round(y_pnl/initial_global*100,2),
        'best_trade':round(max(pnls),2) if pnls else 0,'worst_trade':round(min(pnls),2) if pnls else 0,
        # ─── STOCKS ─────────────────────────────────────────────
        'stocks_capital':round(sc,2),'stocks_portfolio_value':round(st,2),
        'stocks_open_pnl':round(s_op,2),'stocks_closed_pnl':round(s_cl,2),
        'stocks_open_trades':len(stocks_open),'stocks_closed_trades':len(stocks_closed),
        # ─── CRYPTO ─────────────────────────────────────────────
        'crypto_capital':round(cc,2),'crypto_portfolio_value':round(ct,2),
        'crypto_open_pnl':round(c_op,2),'crypto_closed_pnl':round(c_cl,2),
        'crypto_open_trades':len(crypto_open),'crypto_closed_trades':len(crypto_closed),
        # ─── ARBI (SEGREGADO) ───────────────────────────────────
        'arbi_book': {
            'segregated': True,
            'note': 'Arbi capital is separate — not included in core_portfolio_value',
            'capital': round(ac,2), 'initial_capital': ARBI_CAPITAL,
            'portfolio_value': arbi_total,
            'open_pnl': round(a_op,2), 'closed_pnl': round(a_cl,2),
            'total_pnl': round(a_op+a_cl,2),
            'gain_percent': round((arbi_total-ARBI_CAPITAL)/ARBI_CAPITAL*100,2),
            'open_trades': len(arbi_open), 'closed_trades': len(arbi_closed),
            'winning_trades': a_win,
            'win_rate': round(a_win/len(arbi_closed)*100,1) if arbi_closed else 0,
            'kill_switch': ARBI_KILL_SWITCH,
        },
        'assets_monitored':len(ALL_STOCK_SYMBOLS)+len(CRYPTO_SYMBOLS),
        'kill_switch':RISK_KILL_SWITCH,'market_regime':market_regime,
        'alerts_enabled':ALERTS_ENABLED,
        'market_status':{'b3':is_b3_open(),'nyse':is_nyse_open(),'crypto':True},
        'updated_at':datetime.utcnow().isoformat()
    })

@app.route('/audit')
def audit_route():
    # [V9-4] cached_recent_only: true — deixa explícito que é cache parcial (últimos 200 do DB + runtime)
    with audit_lock: data=list(reversed(audit_log))[:100]
    return jsonify({'events':data,'total':len(audit_log),
        'cached_recent_only': True,
        'note': 'In-memory cache (last ~200 from DB + runtime). Full history in audit_events table.'})

@app.route('/risk')
def risk_status():
    with state_lock:
        open_c=len(stocks_open)+len(crypto_open)
        d=calc_period_pnl(stocks_closed+crypto_closed,1)
        w=calc_period_pnl(stocks_closed+crypto_closed,7)
    total_cap=INITIAL_CAPITAL_STOCKS+INITIAL_CAPITAL_CRYPTO
    return jsonify({
        'kill_switch':RISK_KILL_SWITCH,'arbi_kill_switch':ARBI_KILL_SWITCH,
        'limits':{'max_open':MAX_OPEN_POSITIONS,'max_same_symbol':MAX_SAME_SYMBOL,
            'max_daily_dd_pct':MAX_DAILY_DRAWDOWN_PCT,'max_weekly_dd_pct':MAX_WEEKLY_DRAWDOWN_PCT,
            'max_risk_per_trade_pct':MAX_RISK_PER_TRADE_PCT,
            'signal_max_age_min':SIGNAL_MAX_AGE_MIN,'cooldown_sec':SYMBOL_COOLDOWN_SEC},
        'current':{'open_positions':open_c,
            'daily_pnl':d,'daily_dd_pct':round(abs(min(d,0))/total_cap*100,3),
            'weekly_pnl':w,'weekly_dd_pct':round(abs(min(w,0))/total_cap*100,3)},
        'arbi_book':{'capital':arbi_capital,'open':len(arbi_open),
            'max_positions':ARBI_MAX_POSITIONS,'kill_switch':ARBI_KILL_SWITCH,
            'note':'segregated book — own risk limits, separate kill switch'}
    })

@app.route('/risk/reset_kill_switch', methods=['POST'])
def reset_kill_switch():
    global RISK_KILL_SWITCH
    data=request.get_json() or {}
    if data.get('confirm')!='RESET': return jsonify({'error':'Send {"confirm":"RESET"}'}),400
    RISK_KILL_SWITCH=False; audit('KILL_SWITCH_RESET',{'by':'manual_api'})
    return jsonify({'ok':True,'kill_switch':False})

@app.route('/risk/reset_arbi_kill_switch', methods=['POST'])
def reset_arbi_kill_switch():
    global ARBI_KILL_SWITCH
    data=request.get_json() or {}
    if data.get('confirm')!='RESET': return jsonify({'error':'Send {"confirm":"RESET"}'}),400
    ARBI_KILL_SWITCH=False; audit('ARBI_KILL_SWITCH_RESET',{'by':'manual_api'})
    return jsonify({'ok':True,'arbi_kill_switch':False})

@app.route('/alerts/test')
def alerts_test():
    ok=_send_whatsapp_direct(f"Egreja AI v10.7.0 test {datetime.now().strftime('%d/%m %H:%M')}")
    return jsonify({'sent':ok,'enabled':ALERTS_ENABLED})

@app.route('/arbitrage/spreads')
def arbi_spreads_route():
    with state_lock: spreads=list(arbi_spreads.values())
    spreads.sort(key=lambda x:x['abs_spread'],reverse=True)
    return jsonify({'spreads':spreads,'opportunities':[s for s in spreads if s['opportunity']],
        'total_pairs':len(ARBI_PAIRS),'monitored':len(spreads),'fx_rates':fx_rates,
        'arbi_kill_switch':ARBI_KILL_SWITCH,'updated_at':datetime.utcnow().isoformat()})

@app.route('/arbitrage/trades')
def arbi_trades_route():
    with state_lock:
        open_t=list(arbi_open); closed_t=arbi_closed[:50]; cap=arbi_capital
        c_pnl=sum(t.get('pnl',0) for t in arbi_closed); o_pnl=sum(t.get('pnl',0) for t in arbi_open)
        winners=sum(1 for t in arbi_closed if t.get('pnl',0)>0)
    return jsonify({'open_trades':open_t,'closed_trades':closed_t,'capital':round(cap,2),
        'initial_capital':ARBI_CAPITAL,'open_pnl':round(o_pnl,2),
        'closed_pnl':round(c_pnl,2),'total_pnl':round(o_pnl+c_pnl,2),
        'win_rate':round(winners/len(arbi_closed)*100,1) if arbi_closed else 0,
        'open_count':len(open_t),'closed_count':len(arbi_closed),'kill_switch':ARBI_KILL_SWITCH,
        'book':'SEGREGATED — own risk limits, separate capital',
        'parameters':{'min_spread':ARBI_MIN_SPREAD,'tp_spread':ARBI_TP_SPREAD,
            'sl_pct':ARBI_SL_PCT,'timeout_h':ARBI_TIMEOUT_H,
            'position_size':ARBI_POS_SIZE,'max_positions':ARBI_MAX_POSITIONS}})

@app.route('/orders')
def orders_route():
    limit=min(int(request.args.get('limit',50)),500)
    status=request.args.get('status','')
    with orders_lock: data=list(reversed(orders_log))
    filtered=[o for o in data if not status or o.get('status')==status]
    # [V9-4] cached_recent_only: deixa explícito que é cache parcial
    return jsonify({'orders':filtered[:limit],'total':len(orders_log),
        'cached_recent_only': True,
        'note': 'In-memory cache (last ~500 from DB + runtime). Full history in orders table.'})

@app.route('/portfolio/snapshots')
def portfolio_snapshots():
    conn=get_db()
    if not conn: return jsonify({'error':'DB unavailable'}),503
    try:
        limit=min(int(request.args.get('limit',100)),1000)
        cursor=conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM portfolio_snapshots ORDER BY ts DESC LIMIT %s",(limit,))
        rows=cursor.fetchall(); cursor.close(); conn.close()
        for r in rows:
            for k,v in r.items():
                if isinstance(v,datetime): r[k]=v.isoformat()
        return jsonify({'snapshots':rows,'total':len(rows)})
    except Exception as e: return jsonify({'error':str(e)}),500

@app.route('/data/quality')
def data_quality_route():
    with dq_lock: dq=dict(data_quality)
    stale=[s for s in dq.values() if s.get('stale')]
    low_quality=[s for s in dq.values() if s.get('quality',100)<60]
    return jsonify({'symbols':list(dq.values()),'total':len(dq),
        'stale_count':len(stale),'low_quality_count':len(low_quality),
        'stale_symbols':[s['symbol'] for s in stale],
        'timestamp':datetime.utcnow().isoformat()})

# ═══════════════════════════════════════════════════════════════
# [L] ENDPOINTS DE LEARNING & INSIGHT ENGINE
# ═══════════════════════════════════════════════════════════════

@app.route('/learning/status')
@require_auth
def learning_status():
    """[L][FIX-6] Status geral do Learning Engine com métricas de calibração."""
    with learning_lock:
        n_patterns = len(pattern_stats_cache)
        n_factors  = len(factor_stats_cache)
        patterns_above_min = sum(1 for p in pattern_stats_cache.values()
                                 if p.get('total_samples', 0) >= LEARNING_MIN_SAMPLES)

    # [FIX-6] Calcular métricas de calibração a partir do banco (últimos 500 sinais)
    calib = {'avg_confidence_winners': None, 'avg_confidence_losers': None,
             'confidence_band_stats': {}, 'total_attributed': 0}
    try:
        conn = get_db()
        if conn:
            c = conn.cursor(dictionary=True)
            c.execute("""SELECT outcome_status, confidence_band,
                                AVG(learning_confidence) as avg_conf,
                                COUNT(*) as n
                         FROM signal_events
                         WHERE outcome_status IS NOT NULL
                         AND learning_confidence IS NOT NULL
                         GROUP BY outcome_status, confidence_band
                         LIMIT 100""")
            rows = c.fetchall()
            wins_conf  = []; losses_conf = []; band_agg: dict = {}
            for r in rows:
                status = r.get('outcome_status', ''); band = r.get('confidence_band', '')
                avg_c  = float(r.get('avg_conf') or 0); n = int(r.get('n', 0))
                if status == 'WIN':   wins_conf.append((avg_c, n))
                if status == 'LOSS':  losses_conf.append((avg_c, n))
                if band not in band_agg: band_agg[band] = {'wins':0,'losses':0,'flat':0,'total':0}
                band_agg[band][status.lower() if status in ('WIN','LOSS','FLAT') else 'flat'] += n
                band_agg[band]['total'] += n
            # Média ponderada
            def _wavg(lst):
                if not lst: return None
                total_n = sum(n for _, n in lst)
                return round(sum(c * n for c, n in lst) / total_n, 1) if total_n else None
            calib['avg_confidence_winners'] = _wavg(wins_conf)
            calib['avg_confidence_losers']  = _wavg(losses_conf)
            # Win rate por banda
            for band, agg in band_agg.items():
                t = agg['total']
                calib['confidence_band_stats'][band] = {
                    'total': t,
                    'win_rate': round(agg['wins'] / t * 100, 1) if t else None,
                    'wins': agg['wins'], 'losses': agg['losses'],
                }
            c.execute("SELECT COUNT(*) as n FROM signal_events WHERE trade_id IS NOT NULL")
            row = c.fetchone(); calib['total_attributed'] = row['n'] if row else 0
            c.close(); conn.close()
    except Exception as e:
        log.debug(f'learning_status calibration: {e}')

    return jsonify({
        'learning_version':           LEARNING_VERSION,
        'enabled':                    LEARNING_ENABLED,
        'degraded':                   LEARNING_DEGRADED,
        'learning_errors':            learning_errors,
        'total_signal_events':        signal_events_count,
        'total_patterns':             n_patterns,
        'total_factor_rows':          n_factors,
        'patterns_above_min_samples': patterns_above_min,
        'last_learning_update':       last_learning_update,
        'min_samples_threshold':      LEARNING_MIN_SAMPLES,
        'ewma_alpha':                 LEARNING_EWMA_ALPHA,
        'risk_mult_range':            [RISK_MULT_MIN, RISK_MULT_MAX],
        'shadow_eval_window_min':     SHADOW_EVAL_WINDOW_MIN,
        # [FIX-6] calibração
        'calibration':                calib,
        'timestamp':                  datetime.utcnow().isoformat(),
    })

@app.route('/learning/patterns')
@require_auth
def learning_patterns():
    """[L-3][FIX-7] Padrões com filtros funcionando e métricas reais."""
    min_samp   = int(request.args.get('min_samples', LEARNING_MIN_SAMPLES))
    sort_by    = request.args.get('sort_by', 'confidence_weight')
    limit      = int(request.args.get('limit', 50))
    # Filtros por símbolo/asset_type/market_type cruzam com signal_events no banco
    symbol     = request.args.get('symbol', '').upper()
    asset_type = request.args.get('asset_type', '')
    market_type= request.args.get('market_type', '')

    with learning_lock:
        rows = [dict(v) for v in pattern_stats_cache.values()
                if v.get('total_samples', 0) >= min_samp]
    for r in rows: r.pop('_ewma_hit', None)

    # Se filtros contextuais foram pedidos, cruzar com signal_events no banco
    if symbol or asset_type or market_type:
        try:
            conn = get_db()
            if conn:
                c = conn.cursor(dictionary=True)
                where = ["1=1"]
                params = []
                if symbol:      where.append("symbol=%s");      params.append(symbol)
                if asset_type:  where.append("asset_type=%s");  params.append(asset_type)
                if market_type: where.append("market_type=%s"); params.append(market_type)
                c.execute(f"SELECT DISTINCT feature_hash FROM signal_events WHERE {' AND '.join(where)}", params)
                valid_hashes = {r2['feature_hash'] for r2 in c.fetchall()}
                c.close(); conn.close()
                rows = [r for r in rows if r.get('feature_hash') in valid_hashes]
        except Exception as e:
            log.debug(f'learning_patterns filter: {e}')

    # Ordenação segura
    if sort_by not in ('confidence_weight','total_samples','expectancy','ewma_pnl_pct','wins','losses'):
        sort_by = 'confidence_weight'
    rows.sort(key=lambda x: x.get(sort_by, 0), reverse=True)

    return jsonify({
        'patterns':    rows[:limit],
        'total_count': len(rows),
        'min_samples': min_samp,
        'sort_by':     sort_by,
        'filters':     {'symbol': symbol, 'asset_type': asset_type, 'market_type': market_type},
        'timestamp':   datetime.utcnow().isoformat(),
    })

@app.route('/learning/factors')
@require_auth
def learning_factors():
    """[L-4] Lista fatores com melhor e pior performance histórica."""
    factor_type = request.args.get('factor_type', '')
    min_samp    = int(request.args.get('min_samples', 5))

    with learning_lock:
        rows = [dict(v) for k, v in factor_stats_cache.items()
                if v.get('total_samples', 0) >= min_samp
                and (not factor_type or v.get('factor_type') == factor_type)]

    for r in rows:
        r.pop('_ewma_hit', None)

    rows.sort(key=lambda x: x.get('confidence_weight', 0), reverse=True)
    top    = rows[:20]
    bottom = sorted(rows, key=lambda x: x.get('confidence_weight', 0))[:10]

    return jsonify({
        'top_factors':    top,
        'bottom_factors': bottom,
        'total_count':    len(rows),
        'timestamp':      datetime.utcnow().isoformat(),
    })

@app.route('/learning/insights')
@require_auth
def learning_insights():
    """[L-6] Insights do sistema baseados no histórico."""
    factors   = get_top_factors(n_best=10, n_worst=5)
    with learning_lock:
        # Padrões com alta confiança mas poucos dados
        fragile = [dict(v) for v in pattern_stats_cache.values()
                   if v.get('confidence_weight', 0) > 0.3
                   and v.get('total_samples', 0) < LEARNING_MIN_SAMPLES * 2]
        # Padrões deteriorando: ewma_pnl recente pior que avg
        deteriorating = [dict(v) for v in pattern_stats_cache.values()
                         if v.get('ewma_pnl_pct', 0) < v.get('avg_pnl_pct', 0) - 0.5
                         and v.get('total_samples', 0) >= LEARNING_MIN_SAMPLES]
        # Top padrões
        top_patterns = sorted(pattern_stats_cache.values(),
                               key=lambda x: x.get('confidence_weight', 0), reverse=True)[:5]

    return jsonify({
        'top_positive_factors':  factors['top_positive'],
        'top_negative_factors':  factors['top_negative'],
        'fragile_patterns':      fragile[:10],
        'deteriorating_patterns':deteriorating[:10],
        'top_patterns':          [dict(p) for p in top_patterns],
        'total_signal_events':   signal_events_count,
        'learning_degraded':     LEARNING_DEGRADED,
        'timestamp':             datetime.utcnow().isoformat(),
    })

@app.route('/signals/enriched')
@require_auth
def signals_enriched():
    """[L-5/L-6] Sinais enriquecidos com learning_confidence e insight."""
    conn = get_db()
    if not conn:
        return jsonify({'error': 'DB unavailable'}), 503
    try:
        cursor = conn.cursor(dictionary=True)
        cutoff = (datetime.utcnow() - timedelta(minutes=SIGNAL_MAX_AGE_MIN)).strftime('%Y-%m-%d %H:%M:%S')
        cursor.execute('SELECT * FROM market_signals WHERE created_at>=%s ORDER BY score DESC LIMIT 50', (cutoff,))
        raw_signals = cursor.fetchall(); cursor.close(); conn.close()
        for r in raw_signals:
            for k, v in r.items():
                if isinstance(v, datetime): r[k] = v.isoformat()
            r['asset_type'] = 'stock'
            cached = stock_prices.get(r['symbol'])
            if cached:
                r['price']  = cached['price']
                r['rsi']    = cached.get('rsi', r.get('rsi', 50))
                r['ema9']   = cached.get('ema9', r.get('ema9', 0))
                r['ema21']  = cached.get('ema21', r.get('ema21', 0))
                r['ema50']  = cached.get('ema50', r.get('ema50', 0))
    except Exception as e:
        return jsonify({'error': str(e)}), 500

    result = []
    now_dt = datetime.utcnow()
    factors = get_top_factors(n_best=3, n_worst=2)

    for sig in raw_signals:
        try:
            sym       = sig.get('symbol', '')
            dq_score  = get_dq_score(sym)
            sig_e     = dict(sig)
            features  = extract_features(sig_e, dict(market_regime), dq_score, now_dt)
            features['_dq_score'] = dq_score
            feat_hash = make_feature_hash(features)
            conf      = calc_learning_confidence(sig_e, features, feat_hash)
            insight   = generate_insight(sig_e, features, feat_hash, conf)
            risk_mult = get_risk_multiplier(conf)

            result.append({
                'symbol':               sym,
                'signal':               sig.get('signal'),
                'raw_score':            sig.get('score'),
                'price':                sig.get('price'),
                'learning_confidence':  conf.get('final_confidence'),
                'confidence_band':      conf.get('confidence_band'),
                'pattern_samples':      conf.get('pattern_samples', 0),
                'insight_summary':      insight,
                'top_positive_factors': factors['top_positive'][:3],
                'top_negative_factors': factors['top_negative'][:2],
                'recommended_risk_multiplier': risk_mult,
                'recommended_action':  ('OPERAR' if conf.get('confidence_band') == 'HIGH' else
                                        'CAUTELA' if conf.get('confidence_band') == 'MEDIUM' else
                                        'EVITAR'),
                'feature_hash':         feat_hash,
                'confidence_breakdown': conf,
            })
        except Exception as e:
            log.debug(f'signals_enriched {sig.get("symbol")}: {e}')

    return jsonify({
        'signals':   result,
        'count':     len(result),
        'timestamp': datetime.utcnow().isoformat(),
        'cached_recent_only': True,
    })

@app.route('/shadow/status')
@require_auth
def shadow_status():
    """[L-8][FIX-7] Resumo do shadow learning com métricas reais."""
    conn = get_db()
    if not conn:
        return jsonify({'error': 'DB unavailable', 'timestamp': datetime.utcnow().isoformat()})
    try:
        c = conn.cursor(dictionary=True)
        c.execute("SELECT COUNT(*) as total FROM shadow_decisions")
        total = (c.fetchone() or {}).get('total', 0)
        c.execute("SELECT COUNT(*) as pending FROM shadow_decisions WHERE evaluation_status='PENDING'")
        pending = (c.fetchone() or {}).get('pending', 0)
        c.execute("""SELECT evaluation_status, COUNT(*) as n
                     FROM shadow_decisions GROUP BY evaluation_status""")
        by_status = {r['evaluation_status']: r['n'] for r in c.fetchall()}
        c.execute("""SELECT not_executed_reason, COUNT(*) as n
                     FROM shadow_decisions GROUP BY not_executed_reason ORDER BY n DESC LIMIT 10""")
        by_reason = c.fetchall()
        # Shadow win rate (avaliadas)
        evaluated = total - pending
        wins = by_status.get('WIN', 0); losses = by_status.get('LOSS', 0)
        shadow_win_rate = round(wins / evaluated * 100, 1) if evaluated > 0 else None
        # Média de pnl_pct hipotético
        c.execute("""SELECT AVG(hypothetical_pnl_pct) as avg_pnl
                     FROM shadow_decisions WHERE evaluation_status != 'PENDING'""")
        avg_row = c.fetchone()
        avg_hyp_pnl = round(float(avg_row['avg_pnl']), 4) if avg_row and avg_row['avg_pnl'] else None
        c.close(); conn.close()
        return jsonify({
            'total_shadow_decisions': total,
            'pending_evaluation':     pending,
            'evaluated':              evaluated,
            'shadow_win_rate_pct':    shadow_win_rate,
            'avg_hypothetical_pnl_pct': avg_hyp_pnl,
            'by_status':              by_status,
            'by_reason':              by_reason,
            'eval_window_min':        SHADOW_EVAL_WINDOW_MIN,
            'learning_enabled':       LEARNING_ENABLED,
            'timestamp':              datetime.utcnow().isoformat(),
        })
    except Exception as e:
        return jsonify({'error': str(e), 'timestamp': datetime.utcnow().isoformat()})

# Adicionar rotas de learning a PUBLIC_ROUTES (somente status básico)
PUBLIC_ROUTES.add('/learning/status')

# ═══════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════
if __name__ == '__main__':
    port=int(os.environ.get('PORT',3001))
    log.info(f'━━━ Egreja Investment AI v10.7.0 | {ENV.upper()} | port {port} | single-process ━━━')
    log.info(f'FMP: {"SET" if FMP_API_KEY else "NOT SET"} | Auth: {"ENABLED" if API_SECRET_KEY else "DISABLED (dev)"} | Alerts: {"ON" if ALERTS_ENABLED else "OFF"}')
    log.info(f'Stocks ${INITIAL_CAPITAL_STOCKS/1e6:.0f}M | Crypto ${INITIAL_CAPITAL_CRYPTO/1e6:.0f}M | Arbi ${ARBI_CAPITAL/1e3:.0f}K (SEGREGATED)')
    log.info(f'Queue thresholds: WARN={URGENT_QUEUE_WARN} / CRIT={URGENT_QUEUE_CRIT}')

    log.info('Init...')
    init_all_tables()
    fetch_fx_rates()          # [v10.6-P1-4] FX carregado ANTES de stock — ADR usa USDBRL
    fetch_crypto_prices()
    fetch_stock_prices()
    init_watchlist_table()
    init_trades_tables()
    init_learning_cache()   # [L-3] carrega histórico de aprendizado em memória
    _update_market_regime()
    take_portfolio_snapshot()
    _check_degraded()
    log.info('Init complete.')

    start_background_threads()
    # Single-process: use gunicorn com --workers=1 em produção
    # gunicorn -w 1 -b 0.0.0.0:$PORT api_server:app
    app.run(host='0.0.0.0', port=port, debug=False)
