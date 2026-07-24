# -*- coding: utf-8 -*-
"""[23-jul-2026 v2] PONTE IB — thread dedicada dona do event loop asyncio.

Corrige o erro 'no current event loop': ib_insync exige um loop asyncio, e o
Flask multi-thread nao o tem nas threads de request. Solucao: uma thread de
fundo roda o loop pra sempre e e a UNICA dona da conexao IB; as requests do
Flask despacham corrotinas pra ela via run_coroutine_threadsafe.
"""
import os, time, asyncio, threading, logging
from flask import Flask, request, jsonify
from ib_insync import IB, Stock, MarketOrder

logging.basicConfig(level=logging.INFO)
log = logging.getLogger('ib-bridge')
app = Flask(__name__)

SECRET = os.environ.get('BRIDGE_SECRET', '')
IB_HOST = os.environ.get('IB_HOST', 'ib-gateway')
IB_PORT = int(os.environ.get('IB_PORT', 4002))
IB_CID = int(os.environ.get('IB_CLIENT_ID', 7))

_loop = asyncio.new_event_loop()
_ib = None


def _loop_thread():
    global _ib
    asyncio.set_event_loop(_loop)
    _ib = IB()
    _loop.run_forever()


threading.Thread(target=_loop_thread, daemon=True).start()
while _ib is None:
    time.sleep(0.05)


def _call(coro, timeout=25):
    return asyncio.run_coroutine_threadsafe(coro, _loop).result(timeout=timeout)


async def _connect():
    if not _ib.isConnected():
        await _ib.connectAsync(IB_HOST, IB_PORT, clientId=IB_CID, timeout=8)
    return _ib.isConnected()


def _auth():
    return SECRET and request.headers.get('X-Bridge-Secret', '') == SECRET


@app.route('/health')
def health():
    if not _auth():
        return jsonify({'error': 'unauthorized'}), 401
    async def _h():
        conn = await _connect()
        acct = {}
        if conn:
            for v in _ib.accountValues():
                if v.tag in ('NetLiquidation', 'BuyingPower', 'AvailableFunds') and v.currency == 'USD':
                    acct[v.tag] = float(v.value)
        return {'ib_connected': conn, 'port': IB_PORT,
                'mode': 'paper' if IB_PORT in (4002, 4004) else 'live', 'account': acct}
    try:
        return jsonify(_call(_h(), timeout=15))
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
    exch = str(b.get('exchange') or 'SMART')       # [24-jul] multi-bolsa (LSE/IBIS/AEB/SBF/TSE)
    curr = str(b.get('currency') or 'USD')
    tif = str(b.get('tif') or 'DAY').upper()        # [24-jul] OPG = Market-on-Open (US Pairs pos-close)
    if not sym or action not in ('BUY', 'SELL') or qty <= 0:
        return jsonify({'error': 'payload invalido'}), 400
    async def _o():
        if not await _connect():
            return None
        contract = Stock(sym, exch, curr)
        q = await _ib.qualifyContractsAsync(contract)
        if not q:
            raise RuntimeError(f'contrato nao qualificado: {sym}/{exch}/{curr}')
        mo = MarketOrder(action, qty)
        if tif == 'OPG':
            mo.tif = 'OPG'   # Market-on-Open: enche na abertura seguinte, nao agora
        trade = _ib.placeOrder(contract, mo)
        if tif == 'OPG':
            # nao espera fill (pregao fechado); confirma que foi aceita e retorna
            for _ in range(12):
                await asyncio.sleep(0.25)
                if trade.orderStatus.status in ('Submitted', 'PreSubmitted', 'Filled',
                                                'Cancelled', 'ApiCancelled', 'Inactive'):
                    break
        else:
            for _ in range(24):
                await asyncio.sleep(0.25)
                if trade.orderStatus.status in ('Filled', 'Cancelled', 'ApiCancelled', 'Inactive'):
                    break
        st = trade.orderStatus
        comm = 0.0
        try:
            comm = sum(f.commissionReport.commission for f in trade.fills if f.commissionReport)
        except Exception:
            pass
        return {'status': st.status, 'order_id': trade.order.orderId,
                'filled': st.filled, 'avg_price': st.avgFillPrice, 'commission': round(comm, 2)}
    try:
        r = _call(_o(), timeout=25)
        if r is None:
            return jsonify({'error': 'IB desconectado'}), 503
        return jsonify(r)
    except Exception as e:
        log.warning(f'order {sym} {action} {qty}: {e}')
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('BRIDGE_PORT', 8088)), threaded=True)
