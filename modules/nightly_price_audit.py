"""
[NIGHT-AUDIT 15-jul-2026, aprovado Beto] Auditoria noturna de precos.

Todo dia (NIGHT_AUDIT_HOUR_UTC, default 04:10 UTC = 01:10 BRT), cada trade
fechada nas ultimas 26h tem entry E exit conferidos contra o candle REAL de
1 minuto da bolsa:
  - crypto -> data-api.binance.vision (espelho publico, sem geo-block)
  - NYSE   -> Polygon 1m (adjusted=false)
  - B3     -> brapi 1m (cobre os ultimos dias — suficiente para D-1)

Desvio >= NIGHT_AUDIT_DEV_PCT (0.5%) = fantasma: anula via /ops/void-trade
(mesma trilha auditavel das varreduras de 15/jul) e alerta via WhatsApp.

Motivacao: as varreduras de 15/jul acharam ~4.200 trades com preco falso
(~30% da era do fallback FMP/Yahoo), $892k de perdas falsas e ~$580k de
ganhos falsos. Este worker garante que fantasma novo morre em horas.

Envs: NIGHT_AUDIT_ENABLED (true), NIGHT_AUDIT_HOUR_UTC (4),
NIGHT_AUDIT_DEV_PCT (0.5), NIGHT_AUDIT_AUTOVOID (true).
"""
import json
import os
import re
import time
import urllib.request
from datetime import datetime, timezone, timedelta

_last_run = {'ts': None, 'checked': 0, 'ghosts': 0, 'voided': 0, 'no_data': 0, 'detail': []}


def _is_b3(sym):
    return bool(re.match(r'^[A-Z]{4}[0-9]+$', sym or ''))


def _fetch_json(url, timeout=15, headers=None):
    req = urllib.request.Request(url, headers=headers or {})
    return json.loads(urllib.request.urlopen(req, timeout=timeout).read())


def _minute_bars(sym, atype, day, cache, log):
    key = (sym, atype, day)
    if key in cache:
        return cache[key]
    bars = {}
    try:
        if atype == 'crypto':
            d0 = int(datetime.strptime(day, '%Y-%m-%d').replace(tzinfo=timezone.utc).timestamp() * 1000)
            for st in (0, 60000000):
                u = (f'https://data-api.binance.vision/api/v3/klines?symbol={sym}USDT'
                     f'&interval=1m&startTime={d0 + st}&limit=1000')
                for c in _fetch_json(u):
                    hm = datetime.fromtimestamp(c[0] / 1000, tz=timezone.utc).strftime('%H:%M')
                    bars[hm] = (float(c[3]), float(c[2]))
        elif _is_b3(sym):
            tok = os.environ.get('BRAPI_TOKEN', '')
            u = f'https://brapi.dev/api/quote/{sym}?range=5d&interval=1m' + (f'&token={tok}' if tok else '')
            d = _fetch_json(u, 20)
            for c in (d.get('results') or [{}])[0].get('historicalDataPrice', []):
                dt_ = datetime.fromtimestamp(c['date'], tz=timezone.utc)
                if dt_.strftime('%Y-%m-%d') == day and c.get('low') and c.get('high'):
                    bars[dt_.strftime('%H:%M')] = (float(c['low']), float(c['high']))
        else:
            pk = os.environ.get('POLYGON_API_KEY', '')
            u = (f'https://api.polygon.io/v2/aggs/ticker/{sym}/range/1/minute/{day}/{day}'
                 f'?adjusted=false&limit=1000&apiKey={pk}')
            d = _fetch_json(u, 20)
            for c in d.get('results') or []:
                hm = datetime.fromtimestamp(c['t'] / 1000, tz=timezone.utc).strftime('%H:%M')
                bars[hm] = (float(c['l']), float(c['h']))
        time.sleep(0.12)
    except Exception as e:
        log.debug(f'[NIGHT-AUDIT] barras {sym}/{day}: {e}')
    cache[key] = bars
    return bars


def _dev_leg(sym, atype, px, ts, cache, log):
    bars = _minute_bars(sym, atype, ts.strftime('%Y-%m-%d'), cache, log)
    win = []
    for off in (-2, -1, 0, 1, 2):
        hm = (ts + timedelta(minutes=off)).strftime('%H:%M')
        if hm in bars:
            win.append(bars[hm])
    if not win:
        return None
    lo = min(w[0] for w in win)
    hi = max(w[1] for w in win)
    if lo <= px <= hi:
        return 0.0
    return (px / hi - 1) * 100 if px > hi else (px / lo - 1) * 100


def run_nightly_audit(ctx):
    log = ctx['log']
    get_closed = ctx['get_closed_snapshot']
    api_key = ctx['api_key']
    port = ctx.get('port', 3001)
    send_whatsapp = ctx.get('send_whatsapp')
    dev_lim = float(os.environ.get('NIGHT_AUDIT_DEV_PCT', 0.5))
    autovoid = os.environ.get('NIGHT_AUDIT_AUTOVOID', 'true').lower() != 'false'

    cutoff = datetime.utcnow() - timedelta(hours=26)
    trades = [t for t in get_closed()
              if t.get('close_reason') not in ('VOIDED', 'CORRUPTED_DATA_FIXED', 'MANUAL_ORPHAN')
              and t.get('entry_price') and t.get('exit_price')
              and str(t.get('closed_at', '')) >= cutoff.strftime('%Y-%m-%dT%H:%M:%S')]
    log.info(f'[NIGHT-AUDIT] iniciando: {len(trades)} trades das ultimas 26h')

    cache = {}
    ghosts = []
    checked = no_data = 0
    for t in trades:
        sym = t['symbol']
        atype = t.get('asset_type', 'stock')
        try:
            ts_e = datetime.fromisoformat(str(t['opened_at'])[:19]).replace(tzinfo=timezone.utc)
            ts_x = datetime.fromisoformat(str(t['closed_at'])[:19]).replace(tzinfo=timezone.utc)
            ed = _dev_leg(sym, atype, float(t['entry_price']), ts_e, cache, log)
            xd = _dev_leg(sym, atype, float(t['exit_price']), ts_x, cache, log)
        except Exception:
            ed = xd = None
        if ed is None and xd is None:
            no_data += 1
            continue
        checked += 1
        mx = max(abs(ed or 0), abs(xd or 0))
        if mx >= dev_lim:
            ghosts.append((t['id'], sym, float(t.get('pnl_net') or t.get('pnl') or 0), round(mx, 2)))

    voided = 0
    for tid, sym, pnl, mx in ghosts:
        log.warning(f'[NIGHT-AUDIT] FANTASMA: {sym} {tid} pnl=${pnl:,.0f} dev={mx}%')
        if autovoid:
            try:
                req = urllib.request.Request(
                    f'http://127.0.0.1:{port}/ops/void-trade/{tid}', method='POST',
                    headers={'X-API-Key': api_key, 'Content-Type': 'application/json'},
                    data=json.dumps({'reason': f'auditoria noturna: preco fora do candle 1m real (dev {mx}%)'}).encode())
                json.loads(urllib.request.urlopen(req, timeout=25).read())
                voided += 1
                time.sleep(0.3)
            except Exception as e:
                log.error(f'[NIGHT-AUDIT] void {tid}: {e}')

    _last_run.update({'ts': datetime.utcnow().isoformat(), 'checked': checked,
                      'ghosts': len(ghosts), 'voided': voided, 'no_data': no_data,
                      'detail': [{'id': g[0], 'symbol': g[1], 'pnl': g[2], 'dev': g[3]} for g in ghosts[:50]]})
    msg = (f'AUDITORIA NOTURNA: {checked} trades verificadas | '
           f'{len(ghosts)} fantasmas | {voided} anuladas | {no_data} sem dados')
    log.info(f'[NIGHT-AUDIT] {msg}')
    if ghosts and send_whatsapp:
        try:
            top = ', '.join(f'{g[1]} ${g[2]:,.0f} ({g[3]}%)' for g in ghosts[:5])
            send_whatsapp(f'{msg}\nPiores: {top}')
        except Exception:
            pass
    return _last_run


def nightly_audit_loop(ctx):
    log = ctx['log']
    beat = ctx['beat']
    if os.environ.get('NIGHT_AUDIT_ENABLED', 'true').lower() == 'false':
        log.info('[NIGHT-AUDIT] desabilitado via env')
        while True:
            beat('nightly_audit_loop')
            time.sleep(60)
    log.info('[NIGHT-AUDIT] worker iniciado')
    while True:
        beat('nightly_audit_loop')
        now = datetime.utcnow()
        target = now.replace(hour=int(os.environ.get('NIGHT_AUDIT_HOUR_UTC', 4)),
                             minute=10, second=0, microsecond=0)
        if target <= now:
            target += timedelta(days=1)
        wait = (target - now).total_seconds()
        log.info(f'[NIGHT-AUDIT] proxima execucao em {wait/3600:.1f}h ({target.isoformat()}Z)')
        while wait > 0:
            time.sleep(min(60, wait))
            wait -= 60
            beat('nightly_audit_loop')
        try:
            run_nightly_audit(ctx)
        except Exception as e:
            log.error(f'[NIGHT-AUDIT] falha na execucao: {e}')


def get_last_run():
    return dict(_last_run)
