"""
═══════════════════════════════════════════════════════════════════════════
B3 REGIME GATE — SHADOW  [30-jul-2026, consenso GPT+Grok+Kimi, decisao Beto]
═══════════════════════════════════════════════════════════════════════════
Freio de regime estrutural para a mesa direcional B3, equivalente funcional
ao NYSE-REGIME-SOFT (QQQ), com o desenho de DUAS CONDICOES que os tres
consultores convergiram:
  (a) tendencia do IBOV:  close vs EMA50  +  retorno 20 pregoes
  (b) confirmacao do breadth do universo B3 (co-testemunha — resolve a
      concentracao Vale/Petro/bancos do indice)

100% SHADOW: este modulo NAO bloqueia nada. Ele calcula o regime e o
entry_observer registra, por trade real aberta, o "teria bloqueado" —
o resultado real da trade vira o contrafactual perfeito.

Motivacao (dados julho/2026, verificados no banco):
  - lado desalinhado da mare: −R$ 43,8 mil no mes (B3)
  - acerto direcional B3 caiu de 57% para 43% apos 21/jul
  - NYSE com gate equivalente: 51% -> 70% de acerto direcional

Regime final (tatico) combina estrutural + pulse do dia + breadth:
  STRONG_DOWN : estrutural DOWN + pulse RISK_OFF + breadth < 40%
  DOWN        : estrutural DOWN (sem confirmacao dupla)
  STRONG_UP   : estrutural UP + pulse RISK_ON + breadth > 62%
  UP          : estrutural UP
  NEUTRAL     : resto (inclui falha de dado — fail-open, nunca trava)

Envs: B3_GATE_BREADTH_DOWN (40) | B3_GATE_BREADTH_UP (62)
      B3_GATE_LONG_MIN_SCORE_DOWN (75) | B3_GATE_LONG_MIN_SCORE_STRONG (90)
"""
import os, time, logging
import requests

log = logging.getLogger('egreja.b3gate')

_cache = {'ts': 0, 'data': None}
_CACHE_S = 3600  # estrutural e diario; 1h de cache basta


def _ema(values, span):
    if not values:
        return None
    k = 2.0 / (span + 1)
    e = float(values[0])
    for v in values[1:]:
        e = float(v) * k + e * (1 - k)
    return e


def get_structural():
    """Regime estrutural do IBOV (diario, cache 1h).
    Retorna dict {regime, close, ema50, ret20d_pct, fonte} — fail-open NEUTRAL."""
    now = time.time()
    if _cache['data'] and now - _cache['ts'] < _CACHE_S:
        return _cache['data']
    out = {'regime': 'NEUTRAL', 'close': None, 'ema50': None,
           'ret20d_pct': None, 'fonte': 'indisponivel'}
    try:
        tok = os.environ.get('BRAPI_TOKEN', '')
        pr = {'range': '6mo', 'interval': '1d'}
        if tok:
            pr['token'] = tok
        rs = requests.get('https://brapi.dev/api/quote/%5EBVSP', params=pr, timeout=8)
        res = ((rs.json() or {}).get('results') or [{}])[0] if rs.status_code == 200 else {}
        candles = res.get('historicalDataPrice') or []
        closes = [float(c['close']) for c in candles if c.get('close')]
        if len(closes) >= 55:
            close = closes[-1]
            ema50 = _ema(closes[-120:], 50)
            ret20 = (close / closes[-21] - 1.0) * 100.0 if len(closes) >= 21 else None
            reg = 'NEUTRAL'
            if ema50 and ret20 is not None:
                if close < ema50 and ret20 < 0:
                    reg = 'DOWN'
                elif close > ema50 and ret20 > 0:
                    reg = 'UP'
            out = {'regime': reg, 'close': round(close, 0),
                   'ema50': round(ema50, 0) if ema50 else None,
                   'ret20d_pct': round(ret20, 2) if ret20 is not None else None,
                   'fonte': 'brapi ^BVSP 1d'}
            log.info(f"[B3-GATE] estrutural={reg} close={out['close']} "
                     f"ema50={out['ema50']} ret20d={out['ret20d_pct']}%")
    except Exception as e:
        log.debug(f'[B3-GATE] estrutural: {e}')
    _cache['ts'] = now
    _cache['data'] = out
    return out


def evaluate(pulse_state=None, breadth_up_pct=None):
    """Combina estrutural + pulse + breadth -> regime final tatico.
    Retorna dict completo para o entry_observer logar. NAO bloqueia nada."""
    st = get_structural()
    b_dn = float(os.environ.get('B3_GATE_BREADTH_DOWN', 40))
    b_up = float(os.environ.get('B3_GATE_BREADTH_UP', 62))
    estr = st['regime']
    pu = (pulse_state or 'NEUTRAL').upper()
    br = float(breadth_up_pct) if breadth_up_pct is not None else None

    final = 'NEUTRAL'
    if estr == 'DOWN':
        final = 'STRONG_DOWN' if (pu == 'RISK_OFF' and br is not None and br < b_dn) else 'DOWN'
    elif estr == 'UP':
        final = 'STRONG_UP' if (pu == 'RISK_ON' and br is not None and br > b_up) else 'UP'

    return {'estrutural': estr, 'final': final,
            'ibov_close': st['close'], 'ibov_ema50': st['ema50'],
            'ibov_ret20d': st['ret20d_pct'], 'breadth_up': br,
            'pulse_state': pu, 'fonte': st['fonte']}


def would_block(gate, direction, score, signal_orig):
    """Dado o regime final e a trade proposta, o gate TERIA bloqueado?
    (regras do consenso — avaliadas em shadow, nunca aplicadas)
    Retorna (bool, motivo)."""
    try:
        d = (direction or '').upper()
        s = float(score or 50)
        f = gate.get('final', 'NEUTRAL')
        min_down = float(os.environ.get('B3_GATE_LONG_MIN_SCORE_DOWN', 75))
        min_strong = float(os.environ.get('B3_GATE_LONG_MIN_SCORE_STRONG', 90))
        if d == 'LONG':
            if f == 'STRONG_DOWN' and s < min_strong:
                return True, f'LONG em STRONG_DOWN (score {s:.0f} < {min_strong:.0f})'
            if f == 'DOWN' and s < min_down:
                return True, f'LONG em DOWN (score {s:.0f} < {min_down:.0f})'
        elif d == 'SHORT':
            if f == 'STRONG_UP':
                return True, 'SHORT em STRONG_UP'
            # conversao MANTER->SHORT so permitida em DOWN/STRONG_DOWN (consenso)
            if str(signal_orig or '').upper() == 'MANTER' and f not in ('DOWN', 'STRONG_DOWN'):
                return True, f'conversao MANTER->SHORT fora de DOWN (regime {f})'
        return False, ''
    except Exception as e:
        return False, f'erro fail-open: {e}'
