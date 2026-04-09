"""
[v10.26] Trading Configuration Module
Extracts pure constants and configuration from api_server.py.
No mutable state, no Flask app references.
"""

import os
from datetime import datetime

# ═══════════════════════════════════════════════════════════════
# CAPITAL & POSITION LIMITS
# ═══════════════════════════════════════════════════════════════
INITIAL_CAPITAL_STOCKS = float(os.environ.get('INITIAL_CAPITAL_STOCKS', 9_000_000))
INITIAL_CAPITAL_CRYPTO = float(os.environ.get('INITIAL_CAPITAL_CRYPTO', 1_000_000))
MAX_POSITION_STOCKS    = float(os.environ.get('MAX_POSITION_STOCKS', 350_000))  # [v10.14] proporcional ao capital atual
MAX_POSITION_CRYPTO    = float(os.environ.get('MAX_POSITION_CRYPTO', 500_000))  # [v10.25] máximo global

# [v10.14] Posição máxima por símbolo — BTC e ETH são as âncoras de capital
CRYPTO_MAX_POSITION_BY_SYM = {
    'ETHUSDT':  float(os.environ.get('MAX_POS_ETH',  500_000)),  # [v10.24.4] 33% — melhor histórico WR 55%
    'BTCUSDT':  float(os.environ.get('MAX_POS_BTC',  300_000)),  # 20% — referência de mercado
    'ARBUSDT':  float(os.environ.get('MAX_POS_ARB',  200_000)),  # [v10.24.4] 13% — segundo melhor P&L
    'NEARUSDT': float(os.environ.get('MAX_POS_NEAR', 200_000)),  # 13% — terceiro WR 55%
    'BNBUSDT':  float(os.environ.get('MAX_POS_BNB',  200_000)),  # [v10.24.4] 13% — exchange coin estável
}

MAX_CAPITAL_PCT_STOCKS   = float(os.environ.get('MAX_CAPITAL_PCT_STOCKS', 100.0))  # [v10.14] 100% do capital
MAX_CAPITAL_PCT_CRYPTO   = float(os.environ.get('MAX_CAPITAL_PCT_CRYPTO', 100.0))  # [v10.14] 100% do capital
MAX_POSITIONS_STOCKS     = 60  # [v10.14] 60 posições simultâneas (env var ignorada)
MAX_POSITIONS_CRYPTO     = int(os.environ.get('MAX_POSITIONS_CRYPTO', 15))  # [v10.26] expanded universe
MAX_POSITIONS_NYSE       = int(os.environ.get('MAX_POSITIONS_NYSE', 10))

# ═══════════════════════════════════════════════════════════════
# API KEYS & AUTHENTICATION
# ═══════════════════════════════════════════════════════════════
FMP_API_KEY      = os.environ.get('FMP_API_KEY', '')        # fallback terciário
POLYGON_API_KEY  = os.environ.get('POLYGON_API_KEY', '')    # primário para stocks US/NYSE
BRAPI_TOKEN      = os.environ.get('BRAPI_TOKEN', '')        # primário para stocks B3
API_SECRET_KEY   = os.environ.get('API_SECRET_KEY', '')

ENV = os.environ.get('ENV', 'dev').lower()

# ═══════════════════════════════════════════════════════════════
# B3 → ADR MAPPING
# ═══════════════════════════════════════════════════════════════
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

# ═══════════════════════════════════════════════════════════════
# TRADING RULES & TIMEOUTS
# ═══════════════════════════════════════════════════════════════
PUBLIC_ROUTES = {'/', '/health', '/degraded', '/sync/export', '/sync/import'}

MAX_OPEN_POSITIONS      = 75  # [v10.26] 60 stocks + 15 crypto
MAX_DAILY_DRAWDOWN_PCT  = float(os.environ.get('MAX_DAILY_DRAWDOWN_PCT', 2.0))
MAX_WEEKLY_DRAWDOWN_PCT = float(os.environ.get('MAX_WEEKLY_DRAWDOWN_PCT', 5.0))
MAX_POSITION_SAME_MKT   = int(os.environ.get('MAX_POSITION_SAME_MKT', 10))
MAX_SAME_SYMBOL         = int(os.environ.get('MAX_SAME_SYMBOL', 1))
MAX_RISK_PER_TRADE_PCT  = float(os.environ.get('MAX_RISK_PER_TRADE_PCT', 1.5))
RISK_KILL_SWITCH        = False

# Settings ajustaveis em runtime (via /settings POST)
KILL_SWITCH_USD          = float(os.environ.get('KILL_SWITCH_USD', 30000))
STOCK_TP_PCT             = float(os.environ.get('STOCK_TP_PCT', 2.0))
STOCK_SL_PCT             = float(os.environ.get('STOCK_SL_PCT', 2.0))
TRAILING_FLOOR_PCT       = float(os.environ.get('TRAILING_FLOOR_PCT', 0.3))
TRAILING_TRIGGER_PCT     = float(os.environ.get('TRAILING_TRIGGER_PCT', 1.5))
TIMEOUT_B3_H             = float(os.environ.get('TIMEOUT_B3_H', 48))    # [v10.26] increased from 5h
TIMEOUT_CRYPTO_H         = float(os.environ.get('TIMEOUT_CRYPTO_H', 48))
TIMEOUT_NYSE_H           = float(os.environ.get('TIMEOUT_NYSE_H', 48))   # [v10.26] increased from 7h
MIN_SCORE_AUTO           = int(os.environ.get('MIN_SCORE_AUTO', 70))
MIN_SCORE_AUTO_CRYPTO    = int(os.environ.get('MIN_SCORE_AUTO_CRYPTO', 75))  # [v10.26] raised from 55 — crypto entries need higher quality
DEFAULT_POSITION_SIZE    = float(os.environ.get('DEFAULT_POSITION_SIZE', 100000))

SIGNAL_MAX_AGE_MIN  = int(os.environ.get('SIGNAL_MAX_AGE_MIN', 30))
SYMBOL_COOLDOWN_SEC = int(os.environ.get('SYMBOL_COOLDOWN_SEC', 300))

# [V9-2] Limites de proteção da fila crítica
URGENT_QUEUE_WARN = int(os.environ.get('URGENT_QUEUE_WARN', 1000))
URGENT_QUEUE_CRIT = int(os.environ.get('URGENT_QUEUE_CRIT', 5000))

# ═══════════════════════════════════════════════════════════════
# ARBITRAGE CONFIG
# ═══════════════════════════════════════════════════════════════
ARBI_CAPITAL         = float(os.environ.get('ARBI_CAPITAL', 4_500_000))  # [v10.9] increased
ARBI_MIN_SPREAD      = float(os.environ.get('ARBI_MIN_SPREAD', 0.8))
ARBI_MAX_SPREAD      = float(os.environ.get('ARBI_MAX_SPREAD', 15.0))  # [v10.9]
ARBI_TP_SPREAD       = float(os.environ.get('ARBI_TP_SPREAD',  0.20))   # [v10.14]
ARBI_SL_PCT          = float(os.environ.get('ARBI_SL_PCT',    0.80))   # [v10.14]
ARBI_TIMEOUT_H       = float(os.environ.get('ARBI_TIMEOUT_H',  48))    # [v10.14]
ARBI_POS_SIZE        = float(os.environ.get('ARBI_POS_SIZE', 50_000))
ARBI_MAX_POSITIONS   = int(os.environ.get('ARBI_MAX_POSITIONS', 8))
ARBI_MAX_DAILY_LOSS  = float(os.environ.get('ARBI_MAX_DAILY_LOSS_PCT', 1.5))
ARBI_KILL_SWITCH     = False

# ═══════════════════════════════════════════════════════════════
# LEARNING ENGINE CONFIG
# ═══════════════════════════════════════════════════════════════
LEARNING_VERSION       = '10.17.0'
LEARNING_MIN_SAMPLES   = int(os.environ.get('LEARNING_MIN_SAMPLES', 10))   # mínimo de amostras
LEARNING_EWMA_ALPHA    = float(os.environ.get('LEARNING_EWMA_ALPHA', 0.15)) # recência
RISK_MULT_MIN          = float(os.environ.get('RISK_MULT_MIN', 0.30))       # [L-9][v10.15]
RISK_MULT_MAX          = float(os.environ.get('RISK_MULT_MAX', 1.50))       # [L-9][v10.15]
LEARNING_DEAD_ZONE_LOW  = float(os.environ.get('LEARNING_DEAD_ZONE_LOW',  58.0))  # [v10.24]
LEARNING_DEAD_ZONE_HIGH = float(os.environ.get('LEARNING_DEAD_ZONE_HIGH', 63.0))  # [v10.24]
SHADOW_TRACK_REASONS   = {'confidence_low','market_closed','risk_blocked','symbol_open','kill_switch','cooldown','capital'}

# ── [v10.16] Daily drawdown per strategy ──────────────────────────────────
DAILY_DD_STOCKS_PCT   = float(os.environ.get('DAILY_DD_STOCKS_PCT', 1.5))
DAILY_DD_CRYPTO_PCT   = float(os.environ.get('DAILY_DD_CRYPTO_PCT', 2.0))

# ── [v10.16] Auto-blacklist ──────────────────────────────────
BLACKLIST_MIN_TRADES    = int(os.environ.get('BLACKLIST_MIN_TRADES', 20))
BLACKLIST_MAX_AVG_PNL   = float(os.environ.get('BLACKLIST_MAX_AVG_PNL', -40))
BLACKLIST_MAX_WR        = float(os.environ.get('BLACKLIST_MAX_WR', 42))
BLACKLIST_REVIEW_H      = float(os.environ.get('BLACKLIST_REVIEW_H', 24))

# ── [v10.16] ATR-based adaptive stop-loss ─────────────────────────────────
ATR_SL_MULTIPLIER_STOCK  = float(os.environ.get('ATR_SL_MULTIPLIER_STOCK', 2.5))
ATR_SL_MULTIPLIER_CRYPTO = float(os.environ.get('ATR_SL_MULTIPLIER_CRYPTO', 3.0))  # [v10.26] wider stops for crypto volatility
ATR_SL_MIN_PCT           = float(os.environ.get('ATR_SL_MIN_PCT', 0.8))
ATR_SL_MAX_PCT           = float(os.environ.get('ATR_SL_MAX_PCT', 5.5))  # [v10.26] allow wider crypto stops

# ── [v10.16] Inactivity alert ─────────────────────────────────────────────
INACTIVITY_ALERT_H_STOCKS = float(os.environ.get('INACTIVITY_ALERT_H_STOCKS', 4))
INACTIVITY_ALERT_H_CRYPTO = float(os.environ.get('INACTIVITY_ALERT_H_CRYPTO', 8))

# ── [v10.17] Flat Exit ──────────────────────────────────────────────────
FLAT_EXIT_MIN_AGE_MIN     = float(os.environ.get('FLAT_EXIT_MIN_AGE_MIN', 45))
FLAT_EXIT_MAX_VARIATION   = float(os.environ.get('FLAT_EXIT_MAX_VARIATION', 0.30))

# ── [v10.17] Trailing stop triggers ────────────────────────
TRAILING_PEAK_STOCKS      = float(os.environ.get('TRAILING_PEAK_STOCKS', 1.0))
TRAILING_DROP_STOCKS      = float(os.environ.get('TRAILING_DROP_STOCKS', 0.4))
TRAILING_PEAK_CRYPTO      = float(os.environ.get('TRAILING_PEAK_CRYPTO', 1.5))
TRAILING_DROP_CRYPTO      = float(os.environ.get('TRAILING_DROP_CRYPTO', 0.7))

# ── [v10.17] Directional exposure limit ───────────────────────────────────
MAX_DIRECTIONAL_PCT       = float(os.environ.get('MAX_DIRECTIONAL_PCT', 70))
MAX_DIRECTIONAL_PCT_CRYPTO = float(os.environ.get('MAX_DIRECTIONAL_PCT_CRYPTO', 70))   # [v10.26] force diversification

# ── [v10.17] Dynamic timeout ─────────────────────────────────────────────
DYNAMIC_TIMEOUT_ENABLED   = os.environ.get('DYNAMIC_TIMEOUT_ENABLED', 'true').lower() != 'false'
DYNAMIC_TIMEOUT_MULT      = float(os.environ.get('DYNAMIC_TIMEOUT_MULT', 1.3))
DYNAMIC_TIMEOUT_MIN_H     = float(os.environ.get('DYNAMIC_TIMEOUT_MIN_H', 1.5))
DYNAMIC_TIMEOUT_MAX_H     = float(os.environ.get('DYNAMIC_TIMEOUT_MAX_H', 48.0))  # [v10.26] no more early timeout kills

# ── [v10.18] Calibration persistence ─────────────────────────────────────
CALIBRATION_PERSIST_INTERVAL = int(os.environ.get('CALIBRATION_PERSIST_INTERVAL', 300))

# ── [v10.18] Reconciliation ─────────────────────────────────────────────
RECONCILIATION_INTERVAL_S    = int(os.environ.get('RECONCILIATION_INTERVAL_S', 600))
RECONCILIATION_ALERT_PCT     = float(os.environ.get('RECONCILIATION_ALERT_PCT', 2.0))

# ── [v10.18] Crypto conviction filter ───────────────────────────────────
CRYPTO_MIN_CONVICTION        = float(os.environ.get('CRYPTO_MIN_CONVICTION', 70))  # [v10.26] raised from 52
CRYPTO_MIN_HOLD_MIN          = float(os.environ.get('CRYPTO_MIN_HOLD_MIN', 15))

LEARNING_ENABLED       = os.environ.get('LEARNING_ENABLED', 'true').lower() != 'false'

# ═══════════════════════════════════════════════════════════════
# CRYPTO SYMBOLS
# ═══════════════════════════════════════════════════════════════
CRYPTO_SYMBOLS = [
    # [v10.26] Reativado universo completo — agora protegido por min_score=75, conviction=70,
    #          ATR SL 3.0x, max_directional 70%, e detecção de inversão de mercado
    'BTCUSDT',   # referência de mercado
    'ETHUSDT',   # melhor histórico
    'BNBUSDT',   # exchange coin estável
    'SOLUSDT',   # alta liquidez
    'XRPUSDT',   # alta liquidez
    'ADAUSDT',   # top 10
    'DOGEUSDT',  # meme coin, alta vol
    'AVAXUSDT',  # L1
    'DOTUSDT',   # L0
    'LINKUSDT',  # oracle líder
    'MATICUSDT', # L2
    'LTCUSDT',   # legacy
    'UNIUSDT',   # DeFi líder
    'ATOMUSDT',  # interchain
    'XLMUSDT',   # payments
    'NEARUSDT',  # bom histórico
    'APTUSDT',   # L1 novo
    'ARBUSDT',   # bom histórico
    'TRXUSDT',   # stablecoin chain
]

CRYPTO_NAMES = {
    'BTCUSDT':'Bitcoin','ETHUSDT':'Ethereum','BNBUSDT':'BNB','SOLUSDT':'Solana',
    'XRPUSDT':'XRP','ADAUSDT':'Cardano','DOGEUSDT':'Dogecoin','AVAXUSDT':'Avalanche',
    'TRXUSDT':'TRON','DOTUSDT':'Polkadot','LINKUSDT':'Chainlink','MATICUSDT':'Polygon',
    'LTCUSDT':'Litecoin','UNIUSDT':'Uniswap','ATOMUSDT':'Cosmos','XLMUSDT':'Stellar',
    'BCHUSDT':'Bitcoin Cash','NEARUSDT':'NEAR','APTUSDT':'Aptos','ARBUSDT':'Arbitrum'
}

# ═══════════════════════════════════════════════════════════════
# ALERTS & NOTIFICATIONS
# ═══════════════════════════════════════════════════════════════
TWILIO_SID     = os.environ.get('TWILIO_ACCOUNT_SID', '')
TWILIO_TOKEN   = os.environ.get('TWILIO_AUTH_TOKEN', '')
TWILIO_FROM    = os.environ.get('TWILIO_WHATSAPP_FROM', 'whatsapp:+14155238886')
TWILIO_TO      = os.environ.get('TWILIO_WHATSAPP_TO', '')
ALERTS_ENABLED = bool(TWILIO_SID and TWILIO_TOKEN and TWILIO_TO)
ALERT_MIN_SCORE = int(os.environ.get('ALERT_MIN_SCORE', 80))

# ═══════════════════════════════════════════════════════════════
# HEARTBEAT TIMEOUTS
# ═══════════════════════════════════════════════════════════════
# [C-1] Heartbeat timeout POR THREAD (segundos)
THREAD_HEARTBEAT_TIMEOUT = {
    'stock_price_loop':       600,   # [v10.9] 10min
    'crypto_price_loop':      120,   # [v10.9] 2min
    'monitor_trades':         60,    # [v10.9] 60s
    'auto_trade_crypto':      300,   # [v10.9] 5min
    'stock_execution_worker': 300,   # [v10.9] 5min
    'arbi_scan_loop':         600,
    'arbi_monitor_loop':      180,
    'snapshot_loop':          600,
    'persistence_worker':     60,    # [v10.9] 60s
    'alert_worker':           60,
    'watchdog':               90,
    'shadow_evaluator_loop':  1800,  # 30 min
    'network_sync_loop':      150,   # [v10.9] 2.5min
    'report_scheduler':       150,
    # [v10.25] Derivatives scan loops
    'pcp_scan_loop': 300,
    'fst_scan_loop': 300,
    'roll_arb_scan_loop': 300,
    'etf_basket_scan_loop': 300,
    'skew_arb_scan_loop': 300,
    'interlisted_scan_loop': 300,
    'dividend_arb_scan_loop': 600,
    'vol_arb_scan_loop': 300,
}
DEFAULT_HB_TIMEOUT = int(os.environ.get('DEFAULT_HB_TIMEOUT', 120))
WATCHDOG_RESET_STABLE_H = float(os.environ.get('WATCHDOG_RESET_STABLE_H', 6.0))

# ═══════════════════════════════════════════════════════════════
# BROKERAGE FEES
# ═══════════════════════════════════════════════════════════════
# [v10.14] BROKERAGE FEE SIMULATION — taxas reais por mercado
# Deduzidas automaticamente no fechamento de cada trade
# P&L BRUTO inalterado (usado para lógica de trading)
# P&L LÍQUIDO = gross - fee (usado para reporting e aprendizado)

BINANCE_VIP_TIER   = int(os.environ.get('BINANCE_VIP_TIER', 3))
USE_BNB_DISCOUNT   = bool(os.environ.get('USE_BNB_DISCOUNT', 'true').lower() == 'true')
BROKER             = 'BTG'   # B3, NYSE, Arbi via BTG | Crypto via Binance

# Tabela maker/taker Binance por VIP tier (valores por LADO, sem BNB)
_BINANCE_FEES = {0:(0.0010,0.0010), 1:(0.0009,0.0010), 2:(0.0008,0.0010),
                 3:(0.00042,0.0006), 4:(0.0002,0.0004), 5:(0.00012,0.0003)}

def _binance_rt() -> float:
    """Calculate Binance round-trip fee based on VIP tier and BNB discount."""
    m, t = _BINANCE_FEES.get(BINANCE_VIP_TIER, (0.001, 0.001))
    if USE_BNB_DISCOUNT:
        m, t = m * 0.75, t * 0.75
    return round(m + t, 6)   # round trip = maker+taker (compra taker + venda taker)

FEES = {
    'B3':    0.00030,   # BTG Day Trade: ZERO corretagem + emolumentos B3
    'NYSE':  0.00020,   # BTG US: ~0.020% rt spread+SEC
    'CRYPTO': _binance_rt(),  # Binance VIP3+BNB = 0.0765% rt
    'ARBI':  0.00010,   # BTG Day Trade: ZERO corretagem + emolumentos ~0.010% rt
}

def calc_fee(position_value: float, market: str, asset_type: str = 'stock') -> float:
    """[v10.14] Calcula taxa estimada de corretagem para uma operação round-trip.
    FEES já incorpora BNB discount via _binance_rt() — não precisa de FEES_BNB separado.
    """
    pv = abs(float(position_value or 0))
    if asset_type == 'stock':
        rate = FEES.get(market, FEES['NYSE'])
    elif asset_type == 'crypto':
        rate = FEES['CRYPTO']   # já calculado com VIP tier + BNB por _binance_rt()
    else:                       # arbi — BTG Day Trade: emolumentos B3 ~0.010% rt
        rate = FEES['ARBI']  # 0.010%
    return round(pv * rate, 2)

def apply_fee_to_trade(trade: dict) -> dict:
    """
    [v10.14] Calcula e registra a taxa estimada de corretagem.
    IMPORTANTE: pnl e pnl_pct NÃO são alterados — permanecem como bruto.
    O sistema interno (capital, learning, WR, SL, TP) usa sempre o bruto.
    Campos adicionados para exibição no frontend:
      - trade['pnl_gross']    = cópia do pnl bruto (igual a pnl)
      - trade['fee_estimated'] = taxa calculada
      - trade['pnl_net']      = pnl - fee (só para display)
      - trade['pnl_net_pct']  = pnl_net / position_value × 100
    """
    if trade.get('_fee_applied'):
        return trade
    pv    = float(trade.get('position_value', 0) or trade.get('position_size', 0) or 0)
    mkt   = trade.get('market', 'NYSE')
    atype = trade.get('asset_type', 'stock')
    # Para arbi: usar total_cost_estimated que já tem fee+slippage+fx_cost
    if atype in ('arbitrage', 'arbi') or mkt == 'ARBI':
        fee = float(trade.get('total_cost_estimated', 0) or 0)
        if fee == 0:  # fallback para trades antigas sem o campo
            fee = calc_fee(pv, mkt, atype)
    else:
        fee = calc_fee(pv, mkt, atype)
    gross = float(trade.get('pnl', 0) or 0)
    net   = round(gross - fee, 2)
    net_pct = round(net / pv * 100, 4) if pv > 0 else 0
    # NUNCA sobrescreve pnl ou pnl_pct — lógica interna usa bruto
    trade['pnl_gross']     = gross   # igual a pnl (bruto)
    trade['fee_estimated'] = fee
    trade['pnl_net']       = net     # líquido = só para display
    trade['pnl_net_pct']   = net_pct
    trade['_fee_applied']  = True
    return trade

# ═══════════════════════════════════════════════════════════════
# [v10.26] BATCH ENTRY LIMITER
# ═══════════════════════════════════════════════════════════════
MAX_ENTRIES_PER_MINUTE_STOCKS  = int(os.environ.get('MAX_ENTRIES_PER_MINUTE_STOCKS', 5))
MAX_ENTRIES_PER_MINUTE_CRYPTO  = int(os.environ.get('MAX_ENTRIES_PER_MINUTE_CRYPTO', 3))

# ═══════════════════════════════════════════════════════════════
# [v10.26] MARKET REVERSAL DETECTION
# ═══════════════════════════════════════════════════════════════
# RSI + EMA reversal
REVERSAL_RSI_OB              = float(os.environ.get('REVERSAL_RSI_OB', 72))       # overbought threshold
REVERSAL_RSI_OS              = float(os.environ.get('REVERSAL_RSI_OS', 28))       # oversold threshold
REVERSAL_EMA_FAST            = int(os.environ.get('REVERSAL_EMA_FAST', 9))
REVERSAL_EMA_SLOW            = int(os.environ.get('REVERSAL_EMA_SLOW', 21))

# MACD divergence
REVERSAL_MACD_FAST           = int(os.environ.get('REVERSAL_MACD_FAST', 12))
REVERSAL_MACD_SLOW           = int(os.environ.get('REVERSAL_MACD_SLOW', 26))
REVERSAL_MACD_SIGNAL         = int(os.environ.get('REVERSAL_MACD_SIGNAL', 9))

# Volume spike
REVERSAL_VOLUME_SPIKE_MULT   = float(os.environ.get('REVERSAL_VOLUME_SPIKE_MULT', 2.0))  # 2x avg volume = spike
REVERSAL_VOLUME_LOOKBACK     = int(os.environ.get('REVERSAL_VOLUME_LOOKBACK', 20))

# Regime change sensitivity
REVERSAL_REGIME_WINDOW       = int(os.environ.get('REVERSAL_REGIME_WINDOW', 14))
REVERSAL_MIN_SIGNALS         = int(os.environ.get('REVERSAL_MIN_SIGNALS', 2))  # need 2+ indicators agreeing

# Action on reversal detection
REVERSAL_BLOCK_COUNTER_TREND = os.environ.get('REVERSAL_BLOCK_COUNTER_TREND', 'true').lower() != 'false'
REVERSAL_CLOSE_LOSING        = os.environ.get('REVERSAL_CLOSE_LOSING', 'true').lower() != 'false'

# ═══════════════════════════════════════════════════════════════
# [v10.26] POLYGON/OPLAB/BRAPI CONFIRMATION FILTER
# ═══════════════════════════════════════════════════════════════
POLYGON_CONFIRM_ENABLED      = os.environ.get('POLYGON_CONFIRM_ENABLED', 'true').lower() != 'false'
OPLAB_CONFIRM_ENABLED        = os.environ.get('OPLAB_CONFIRM_ENABLED', 'true').lower() != 'false'
BRAPI_CONFIRM_ENABLED        = os.environ.get('BRAPI_CONFIRM_ENABLED', 'true').lower() != 'false'
CONFIRM_TIMEOUT_S            = float(os.environ.get('CONFIRM_TIMEOUT_S', 3.0))  # max wait for API response
CONFIRM_MIN_AGREEMENT        = int(os.environ.get('CONFIRM_MIN_AGREEMENT', 1))   # 1 source agrees = pass
