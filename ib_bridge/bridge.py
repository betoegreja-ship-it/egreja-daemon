# -*- coding: utf-8 -*-
"""[23-jul-2026 v2] PONTE IB — thread dedicada dona do event loop asyncio.

Corrige o erro 'no current event loop': ib_insync exige um loop asyncio, e o
Flask multi-thread nao o tem nas threads de request. Solucao: uma thread de
fundo roda o loop pra sempre e e a UNICA dona da conexao IB; as requests do
Flask despacham corrotinas pra ela via run_coroutine_threadsafe.
"""
import os, time, asyncio, threading, logging, math
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


@app.route('/quote', methods=['POST'])
def quote():
    """[28-jul] Cotacao snapshot de N instrumentos, mesma fonte/relogio (IB).
    Payload: {"instruments":[{"symbol","exchange","currency"}...], "md_type":1}
      md_type: 1=live, 2=frozen, 3=delayed, 4=delayed-frozen.
    Resposta por papel: price/bid/ask, md_type efetivo, error (ex.: 354 sem sub)."""
    if not _auth():
        return jsonify({'error': 'unauthorized'}), 401
    b = request.get_json(silent=True) or {}
    items = b.get('instruments') or []
    if not items:
        return jsonify({'error': 'sem instruments'}), 400
    md_type = int(b.get('md_type', 1))

    async def _q():
        if not await _connect():
            return None
        try:
            _ib.reqMarketDataType(md_type)
        except Exception:
            pass
        errs = {}
        def _on_err(*a):  # (reqId, code, msg, contract) em versoes do ib_insync
            try:
                code = a[1]; msg = a[2]; contract = a[3] if len(a) > 3 else None
                if code in (354, 10089, 10090, 10091, 10167, 10197, 162, 200):
                    sym = getattr(contract, 'symbol', '?') if contract else '?'
                    errs[sym] = f'{code}:{str(msg)[:70]}'
            except Exception:
                pass
        _ib.errorEvent += _on_err

        def _v(x):
            if x is None: return None
            try:
                x = float(x)
                return None if math.isnan(x) else x
            except Exception:
                return None

        out = []
        try:
            contracts, meta = [], []
            for it in items:
                sym = str(it.get('symbol', '')).upper()
                exch = str(it.get('exchange') or 'SMART')
                curr = str(it.get('currency') or 'USD')
                contracts.append(Stock(sym, exch, curr)); meta.append((sym, exch, curr))
            await _ib.qualifyContractsAsync(*contracts)
            qual = [c for c in contracts if c.conId]
            tickers = await _ib.reqTickersAsync(*qual, timeout=8) if qual else []
            tk = {t.contract.conId: t for t in tickers}
            for c, (sym, exch, curr) in zip(contracts, meta):
                row = {'symbol': sym, 'exchange': exch, 'currency': curr,
                       'price': None, 'bid': None, 'ask': None, 'md_type': None,
                       'ts': int(time.time()), 'error': errs.get(sym)}
                if not c.conId:
                    row['error'] = row['error'] or 'contrato_nao_qualificado'
                    out.append(row); continue
                t = tk.get(c.conId)
                if t:
                    bid, ask, last, close = _v(t.bid), _v(t.ask), _v(t.last), _v(t.close)
                    try: mp = _v(t.marketPrice())
                    except Exception: mp = None
                    row['bid'], row['ask'] = bid, ask
                    row['price'] = last or mp or ((bid + ask) / 2 if bid and ask else None) or close
                    row['md_type'] = getattr(t, 'marketDataType', None)
                out.append(row)
        finally:
            try: _ib.errorEvent -= _on_err
            except Exception: pass
        return {'quotes': out, 'md_type_req': md_type}

    try:
        r = _call(_q(), timeout=20)
        if r is None:
            return jsonify({'error': 'IB desconectado'}), 503
        return jsonify(r)
    except Exception as e:
        log.warning(f'quote: {e}')
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('BRIDGE_PORT', 8088)), threaded=True)
