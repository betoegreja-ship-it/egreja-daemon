"""
Brain Advisor V4 — Módulo comum

Funções compartilhadas entre Entry Advisor e Exit Advisor.

RESTRIÇÃO ABSOLUTA:
- Derivativos e arbitragem NUNCA passam por IA.
- should_bypass_ai() é o gate único. Qualquer chamada ao advisor
  SEMPRE consulta esta função primeiro.

REGRA CRYPTO INTOCÁVEL:
- Trailing stop de crypto usa TRAILING_PEAK_CRYPTO e TRAILING_DROP_CRYPTO
  do motor V3. Valores atuais Railway: PEAK=0.4 DROP=0.1.
- O Exit Advisor NUNCA pode fechar crypto lucrativa antes do trailing.
- Só pode TIGHTEN_STOP ou CLOSE em caso de risco extremo (news, regime).
"""
from __future__ import annotations
import os
import time
import threading
from typing import Dict, Any, Optional, Tuple


# ─── FLAGS DE CONTROLE ────────────────────────────────────────────────
# Todas lidas do ambiente — Railway muda sem deploy de código

def advisor_entry_enabled() -> bool:
    return os.environ.get('ADVISOR_ENTRY_ENABLED', 'false').lower() == 'true'

def advisor_entry_shadow() -> bool:
    """True = calcula mas NÃO influencia decisão (só grava shadow)."""
    return os.environ.get('ADVISOR_ENTRY_SHADOW', 'true').lower() == 'true'

def advisor_exit_enabled() -> bool:
    return os.environ.get('ADVISOR_EXIT_ENABLED', 'false').lower() == 'true'

def advisor_exit_shadow() -> bool:
    return os.environ.get('ADVISOR_EXIT_SHADOW', 'true').lower() == 'true'

def advisor_entry_actions() -> set:
    """Ações ativas do Entry Advisor (em não-shadow).
    Default conservador: só reduce e boost. BLOCK liga depois."""
    raw = os.environ.get('ADVISOR_ENTRY_ACTIONS', 'reduce,boost')
    return {a.strip().lower() for a in raw.split(',') if a.strip()}

def advisor_exit_actions() -> set:
    """Ações ativas do Exit Advisor. Default: tighten_stop + reduce.
    CLOSE liga depois pra não fechar trades indevidamente."""
    raw = os.environ.get('ADVISOR_EXIT_ACTIONS', 'tighten_stop,reduce')
    return {a.strip().lower() for a in raw.split(',') if a.strip()}

def advisor_stocks_enabled() -> bool:
    return os.environ.get('ADVISOR_STOCKS_ENABLED', 'true').lower() == 'true'

def advisor_crypto_enabled() -> bool:
    return os.environ.get('ADVISOR_CRYPTO_ENABLED', 'true').lower() == 'true'


# ─── ISOLAMENTO ABSOLUTO DE DERIVATIVOS ──────────────────────────────

DERIVATIVE_ASSET_TYPES = frozenset({'derivative', 'derivatives', 'deriv', 'option', 'future'})
DERIVATIVE_STRATEGIES = frozenset({
    'arbi', 'derivatives', 'pcp', 'fst', 'roll_arb', 'etf_basket',
    'skew_arb', 'interlisted', 'interlisted_hedged', 'dividend_arb',
    'vol_arb', 'ibov_basis', 'di_calendar'
})

def should_bypass_ai(asset_type: Optional[str], strategy: Optional[str]) -> bool:
    """Gate único de isolamento.

    Retorna True se a trade/decisão deve FAZER BYPASS do advisor.
    Chamado no início de TODAS as entradas públicas do advisor.

    Regras:
    - asset_type em DERIVATIVE_ASSET_TYPES → bypass
    - strategy em DERIVATIVE_STRATEGIES → bypass
    - asset_type desconhecido/None → bypass (segurança)
    """
    if not asset_type:
        return True
    at = str(asset_type).strip().lower()
    if at in DERIVATIVE_ASSET_TYPES:
        return True
    if at not in ('stock', 'crypto'):
        # asset_type desconhecido → NUNCA processar (fail-safe)
        return True
    if strategy:
        st = str(strategy).strip().lower()
        if st in DERIVATIVE_STRATEGIES:
            return True
    # Check se asset_type habilitado
    if at == 'stock' and not advisor_stocks_enabled():
        return True
    if at == 'crypto' and not advisor_crypto_enabled():
        return True
    return False


# ─── CACHE TTL ────────────────────────────────────────────────────────

class _AdvisorCache:
    """Cache TTL thread-safe usado pelos votos (similarity, regime).
    Evita milhares de SQL queries redundantes."""

    def __init__(self, ttl_sec: int = 90):
        self._data: Dict[str, Tuple[float, Any]] = {}
        self._lock = threading.Lock()
        self.ttl = ttl_sec

    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            if key in self._data:
                ts, val = self._data[key]
                if time.time() - ts < self.ttl:
                    return val
                del self._data[key]
        return None

    def set(self, key: str, val: Any):
        with self._lock:
            self._data[key] = (time.time(), val)

    def clear(self):
        with self._lock:
            self._data.clear()

_cache = _AdvisorCache(ttl_sec=90)

def get_cache() -> _AdvisorCache:
    return _cache


# ─── PESOS E THRESHOLDS ───────────────────────────────────────────────
# Lidos do ambiente pra calibração posterior via Railway

def entry_weights() -> Dict[str, float]:
    """Pesos iniciais conforme prompt. Somam 1.0."""
    return {
        'similarity': float(os.environ.get('ADVISOR_W_SIMILARITY', '0.30')),
        'risk':       float(os.environ.get('ADVISOR_W_RISK',       '0.20')),
        'regime':     float(os.environ.get('ADVISOR_W_REGIME',     '0.20')),
        'calendar':   float(os.environ.get('ADVISOR_W_CALENDAR',   '0.10')),
        'news':       float(os.environ.get('ADVISOR_W_NEWS',       '0.20')),
    }

def exit_weights() -> Dict[str, float]:
    return {
        'pnl_protection':       float(os.environ.get('ADVISOR_EW_PNL',      '0.25')),
        'regime_deterioration': float(os.environ.get('ADVISOR_EW_REGIME',   '0.25')),
        'news_exit':            float(os.environ.get('ADVISOR_EW_NEWS',     '0.20')),
        'time_decay':           float(os.environ.get('ADVISOR_EW_TIME',     '0.10')),
        'risk_exit':            float(os.environ.get('ADVISOR_EW_RISK',     '0.20')),
    }

# Thresholds de decisão Entry
ENTRY_BLOCK_MAX  = 0.30   # aggregate < 0.30 → BLOCK
ENTRY_REDUCE_MAX = 0.50   # 0.30-0.50 → REDUCE
ENTRY_PASS_MAX   = 0.75   # 0.50-0.75 → PASS / >0.75 → BOOST

# Thresholds de decisão Exit
EXIT_CLOSE_MIN   = 0.80   # > 0.80 → CLOSE
EXIT_REDUCE_MIN  = 0.65   # 0.65-0.80 → REDUCE/TIGHTEN_STOP
EXIT_ATTN_MIN    = 0.45   # 0.45-0.65 → HOLD com atenção / < 0.45 → HOLD normal


# ─── VOTO DEFAULT ─────────────────────────────────────────────────────
# Quando um voto falha (DB indisponível, dados insuficientes), retorna neutro.
DEFAULT_NEUTRAL_VOTE = 0.50


# ─── CRYPTO RULE INTOCÁVEL ────────────────────────────────────────────

def crypto_trailing_peak() -> float:
    """Valor atual do peak ativação trailing crypto.
    Default do código: 1.5. Railway atual: 0.4."""
    return float(os.environ.get('TRAILING_PEAK_CRYPTO', '1.5'))

def crypto_trailing_drop() -> float:
    return float(os.environ.get('TRAILING_DROP_CRYPTO', '0.7'))

def is_crypto_in_trailing_protection(pnl_pct: float, peak_pnl_pct: float) -> bool:
    """Retorna True se crypto já está em zona protegida pelo trailing do motor.
    Usado pelo Exit Advisor para NUNCA propor CLOSE nessa zona.

    Lógica: se peak já atingiu PEAK_CRYPTO, o motor V3 está cuidando do exit.
    Exit Advisor só pode TIGHTEN_STOP — e mesmo assim com parcimônia.
    """
    peak_threshold = crypto_trailing_peak()
    return peak_pnl_pct >= peak_threshold

