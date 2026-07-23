# -*- coding: utf-8 -*-
"""[23-jul-2026] PONTE IB — roda na VPS, ao lado do IB Gateway logado.

Recebe ordens do core (Railway) autenticadas por segredo compartilhado e as
coloca na IB via ib_insync. NUNCA conhece as credenciais da IB — o Gateway
(container separado) e quem loga; esta ponte so fala socket local com ele.

Endpoints:
  GET  /health   -> estado da conexao IB + conta
  POST /order    -> {symbol, action(BUY/SELL), quantity, mode, trade_id}

Env:
  BRIDGE_SECRET   segredo compartilhado (== IB_BRIDGE_SECRET no core)
  IB_HOST 127.0.0.1 | IB_PORT 4002(paper)/4001(live) | IB_CLIENT_ID 7
  BRIDGE_PORT 8088
"""
import os, logging
from flask import Flask, request, jsonify
from ib_insync import IB, Stock, MarketOrder

logging.basicConfig(level=logging.INFO)
log = logging.getLogger('ib-bridge')
app = Flask(__name__)

SECRET = os.environ.get('BRIDGE_SECRET', '')
IB_HOST = os.environ.get('IB_HOST', '127.0.0.1')
IB_PORT = int(os.environ.get('IB_PORT', 4002))
IB_CID = int(os.environ.get('IB_CLIENT_ID', 7))
_ib = IB()


def _ensure():
    if not _ib.isConnected():
        _ib.connect(IB_HOST, IB_PORT, clientId=IB_CID, timeout=8)
    return _ib.isConnected()


def _auth():
    return SECRET and request.headers.get('X-Bridge-Secret', '') == SECRET


@app.route('/health')
def health():
    if not _auth():
        return jsonify({'error': 'unauthorized'}), 401
    try:
        conn = _ensure()
        acct = {}
        if conn:
            for v in _ib.accountValues():
                if v.tag in ('NetLiquidation', 'BuyingPower', 'AvailableFunds') and v.currency == 'USD':
                    acct[v.tag] = float(v.value)
        return jsonify({'ib_connected': conn, 'port': IB_PORT,
                        'mode': 'paper' if IB_PORT == 4002 else 'live', 'account': acct})
    except Exception as e:
        return jsonify({'ib_connected': False, 'error': str(e)}), 500


@app.route('/order', methods=['POST'])
def order():
    if not _auth():
        return jsonify({'error': 'unauthorized'}), 401
    b = request.get_json(silent=True) or {}
    sym = str(b.get('symbol', '')).upper()
    action = str(b.get('action', '')).upper()
    qty = int(b.get('quantity', 0))
    if not sym or action not in ('BUY', 'SELL') or qty <= 0:
        return jsonify({'error': 'payload invalido'}), 400
    try:
        if not _ensure():
            return jsonify({'error': 'IB desconectado'}), 503
        contract = Stock(sym, 'SMART', 'USD')
        _ib.qualifyContracts(contract)
        trade = _ib.placeOrder(contract, MarketOrder(action, qty))
        for _ in range(20):
            _ib.sleep(0.25)
            if trade.orderStatus.status in ('Filled', 'Cancelled', 'ApiCancelled', 'Inactive'):
                break
        st = trade.orderStatus
        comm = 0.0
        try:
            comm = sum(f.commissionReport.commission for f in trade.fills if f.commissionReport)
        except Exception:
            pass
        return jsonify({'status': st.status, 'order_id': trade.order.orderId,
                        'filled': st.filled, 'avg_price': st.avgFillPrice,
                        'commission': round(comm, 2)})
    except Exception as e:
        log.warning(f'order {sym} {action} {qty}: {e}')
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('BRIDGE_PORT', 8088)))
