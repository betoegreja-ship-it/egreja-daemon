"""
═══════════════════════════════════════════════════════════════════════════
PROFIT BRIDGE — ProfitDLL (Nelogica) atras de HTTP  [30-jul-2026]
═══════════════════════════════════════════════════════════════════════════
Mesma arquitetura do ib_bridge que ja funciona: a DLL roda NESTA maquina
Windows (PC do simulador, Ryzen 5600X/Win11 Pro) e o core no Railway
conversa por HTTP via Cloudflare Tunnel. Credenciais vivem SO aqui.

REQUISITOS NA PASTA (C:\\egreja\\profit_bridge):
  - profit_bridge.py           (este arquivo)
  - profit_dll.py              (copiar de ProfitDLL/Exemplo Python)
  - profitTypes.py             (copiar de ProfitDLL/Exemplo Python)
  - ProfitDLL.dll              (copiar de ProfitDLL/DLLs/Win64)
  - config.ini                 (criar conforme guia — chave/usuario/senha)

MODOS (config.ini [bridge] orders_mode):
  ghost  : ordens NAO enviadas — so logadas (default do trial, semana 1)
  live   : ordens reais via SendOrder (semana 2+, lote minimo, aprovacao Beto)

Endpoints (todos exigem header X-API-Key):
  GET  /health              estados de conexao (login/market/broker/ativacao)
  POST /subscribe           {"symbol":"PETR4","bolsa":"B"}
  GET  /quote/<symbol>      ultimo tick do cache
  GET  /quotes              cache inteiro
  POST /order               {"symbol","bolsa","side","qty","price","type"}
  GET  /orders              trilha local de ordens do bridge
"""
import os
import json
import time
import logging
import threading
import configparser
from datetime import datetime

from flask import Flask, request, jsonify

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger('profit_bridge')

HERE = os.path.dirname(os.path.abspath(__file__))
cfg = configparser.ConfigParser()
cfg.read(os.path.join(HERE, 'config.ini'), encoding='utf-8')

PROFIT_KEY = cfg.get('profit', 'activation_key', fallback='')
PROFIT_USER = cfg.get('profit', 'user', fallback='')
PROFIT_PASS = cfg.get('profit', 'password', fallback='')
API_KEY = cfg.get('bridge', 'api_key', fallback='troque-esta-chave')
PORT = cfg.getint('bridge', 'port', fallback=6001)
ORDERS_MODE = cfg.get('bridge', 'orders_mode', fallback='ghost').lower()
DLL_PATH = cfg.get('profit', 'dll_path',
                   fallback=os.path.join(HERE, 'ProfitDLL.dll'))

app = Flask(__name__)

# ─── estado global ───────────────────────────────────────────────────────
STATE = {'login': False, 'market': False, 'broker': False, 'ativacao': False,
         'boot_ts': time.time(), 'last_state_msg': '', 'dll_loaded': False}
QUOTES = {}          # symbol -> {price, qty, ts, n_ticks}
ORDERS_LOG = []      # trilha local (ghost e live)
_subscribed = set()
_lock = threading.Lock()
profit_dll = None


def _now():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


# ─── inicializacao da DLL (thread propria; callbacks do exemplo oficial) ─
def dll_thread():
    global profit_dll
    try:
        from ctypes import (WINFUNCTYPE, byref, c_int, c_size_t, c_uint,
                            c_wchar_p, cast)
        from profit_dll import initializeDll
        from profitTypes import (TConnectorAssetIdentifierSafe,
                                 TConnectorTrade)
        NL_OK = 0
        profit_dll = initializeDll(DLL_PATH)
        STATE['dll_loaded'] = True
        log.info(f'DLL carregada: {DLL_PATH}')

        @WINFUNCTYPE(None, c_int, c_int)
        def stateCallback(nType, nResult):
            # mapa do exemplo oficial: 0=login 1=broker 2=market 3=ativacao
            if nType == 0:
                STATE['login'] = (nResult == 0)
            elif nType == 1:
                STATE['broker'] = (nResult == 5)
            elif nType == 2:
                STATE['market'] = (nResult == 4)
            elif nType == 3:
                STATE['ativacao'] = (nResult == 0)
            STATE['last_state_msg'] = f'{_now()} type={nType} result={nResult}'
            log.info(f'[STATE] type={nType} result={nResult} -> {STATE}')

        @WINFUNCTYPE(None, TConnectorAssetIdentifierSafe, c_size_t, c_uint)
        def tradeCallback(assetSafe, pTrade, flags):
            try:
                ticker = cast(assetSafe.Ticker, c_wchar_p).value or ''
                trade = TConnectorTrade(Version=0)
                if profit_dll.TranslateTrade(pTrade, byref(trade)) == NL_OK:
                    with _lock:
                        q = QUOTES.setdefault(ticker, {'n_ticks': 0})
                        q['price'] = float(trade.Price)
                        q['qty'] = int(trade.Quantity)
                        q['ts'] = time.time()
                        q['n_ticks'] += 1
            except Exception as e:
                log.debug(f'tradeCallback: {e}')

        # login completo (market data + roteamento). A assinatura exata dos
        # 12 callbacks segue o Exemplo Python (main.py linha ~753); os nao
        # usados vao como None — ajustar no dia 1 do trial se a versao da
        # DLL divergir.
        result = profit_dll.DLLInitializeLogin(
            c_wchar_p(PROFIT_KEY), c_wchar_p(PROFIT_USER),
            c_wchar_p(PROFIT_PASS),
            stateCallback, None, None, None, None, None, None, None,
            None, None)
        log.info(f'DLLInitializeLogin: {result}')
        profit_dll.SetTradeCallbackV2(tradeCallback)

        # manter a thread viva (callbacks chegam pela DLL)
        while True:
            time.sleep(5)
    except Exception as e:
        STATE['last_state_msg'] = f'ERRO init: {e}'
        log.error(f'dll_thread: {e}', exc_info=True)


# ─── auth ────────────────────────────────────────────────────────────────
@app.before_request
def _auth():
    if request.headers.get('X-API-Key') != API_KEY:
        return jsonify({'error': 'unauthorized'}), 401


# ─── endpoints ───────────────────────────────────────────────────────────
@app.route('/health')
def health():
    with _lock:
        return jsonify({
            'ok': STATE['login'] and STATE['market'] and STATE['ativacao'],
            'state': {k: STATE[k] for k in
                      ('login', 'market', 'broker', 'ativacao', 'dll_loaded')},
            'last_state_msg': STATE['last_state_msg'],
            'orders_mode': ORDERS_MODE,
            'uptime_s': round(time.time() - STATE['boot_ts'], 1),
            'subscribed': sorted(_subscribed),
            'quotes_cached': len(QUOTES),
        })


@app.route('/subscribe', methods=['POST'])
def subscribe():
    from ctypes import c_wchar_p
    d = request.get_json(force=True)
    sym = (d.get('symbol') or '').upper()
    bolsa = d.get('bolsa') or 'B'   # B = Bovespa; F = BMF (DOL/WDO/WIN)
    if not sym or profit_dll is None:
        return jsonify({'error': 'symbol obrigatorio / dll nao carregada'}), 400
    r = profit_dll.SubscribeTicker(c_wchar_p(sym), c_wchar_p(bolsa))
    _subscribed.add(f'{sym}:{bolsa}')
    return jsonify({'symbol': sym, 'bolsa': bolsa, 'result': int(r)})


@app.route('/quote/<symbol>')
def quote(symbol):
    with _lock:
        q = dict(QUOTES.get(symbol.upper()) or {})
    if not q:
        return jsonify({'error': 'sem tick — ja fez /subscribe?'}), 404
    return jsonify({'symbol': symbol.upper(), **q,
                    'age_s': round(time.time() - q['ts'], 2)})


@app.route('/quotes')
def quotes():
    with _lock:
        out = {s: {**q, 'age_s': round(time.time() - q['ts'], 2)}
               for s, q in QUOTES.items()}
    return jsonify(out)


@app.route('/order', methods=['POST'])
def order():
    d = request.get_json(force=True)
    entry = {'ts': _now(), 'mode': ORDERS_MODE, **d}
    if ORDERS_MODE != 'live':
        entry['status'] = 'GHOST (nao enviada)'
        ORDERS_LOG.append(entry)
        log.info(f'[ORDER-GHOST] {json.dumps(d, ensure_ascii=False)}')
        return jsonify(entry)
    # live: montar TConnectorSendOrder conforme Exemplo Python (semana 2 do
    # trial — habilitar so com aprovacao explicita do Beto, lote minimo)
    entry['status'] = 'LIVE_NAO_IMPLEMENTADO_AINDA'
    ORDERS_LOG.append(entry)
    return jsonify(entry), 501


@app.route('/orders')
def orders():
    return jsonify(ORDERS_LOG[-100:])


if __name__ == '__main__':
    t = threading.Thread(target=dll_thread, daemon=True)
    t.start()
    log.info(f'profit_bridge na porta {PORT} (orders_mode={ORDERS_MODE})')
    app.run(host='127.0.0.1', port=PORT)
