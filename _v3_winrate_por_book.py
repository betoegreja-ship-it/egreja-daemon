#!/usr/bin/env python3
"""V3 win rate por book (B3 / NYSE / Crypto). Roda quando a rede TCP voltar.
   uso: python3 _v3_winrate_por_book.py            (all-time)
        python3 _v3_winrate_por_book.py 2026-03-08 2026-03-25   (janela)
   Classificacao B3 vs NYSE estritamente separada. Arbi excluido (nao usa V3)."""
import subprocess, json, re, os, sys, urllib.request
from collections import defaultdict

BASE = "https://diligent-spirit-production.up.railway.app"
REPO = os.path.dirname(os.path.abspath(__file__))

# key sem imprimir
g = subprocess.run(['grep','-rohI','--include=*.py','X-API-Key:[ ]*[A-Za-z0-9_-]\\+', REPO],
                   capture_output=True, text=True).stdout
keys = set(re.findall(r'X-API-Key:\s*([A-Za-z0-9_-]+)', g))
API_KEY = sorted(keys, key=len, reverse=True)[0] if keys else os.environ.get('API_KEY','')

def get(path):
    req = urllib.request.Request(BASE+path, headers={'X-API-Key': API_KEY})
    with urllib.request.urlopen(req, timeout=90) as r:
        return json.loads(r.read().decode())

raw = get('/trades/closed')
trades = raw if isinstance(raw, list) else next((v for v in raw.values() if isinstance(v, list)), [])

# janela de datas client-side (payload e cumulativo)
def fld(t, *names):
    for n in names:
        if n in t and t[n] is not None: return t[n]
    return None
if len(sys.argv) == 3:
    lo, hi = sys.argv[1], sys.argv[2]
    trades = [t for t in trades if (str(fld(t,'closed_at','exit_time','close_time','timestamp','date') or ''))[:10] >= lo
              and (str(fld(t,'closed_at','exit_time','close_time','timestamp','date') or ''))[:10] <= hi]

CRYPTO = {'BTC','ETH','SOL','BNB','XRP','ADA','DOGE','AVAX','LINK','APT','ARB','MATIC',
          'DOT','LTC','UNI','ATOM','NEAR','OP','SUI','TON','TRX','BCH','FIL','INJ'}
def book(t):
    # prefere campo explicito se existir
    for f in ('book','market_type','asset_class','exchange','market','category'):
        v = str(fld(t, f) or '').upper()
        if 'ARB' in v or 'ARBI' in v: return 'ARBI'
        if 'CRYPTO' in v or 'CRIPTO' in v: return 'CRYPTO'
        if 'NYSE' in v or 'NASDAQ' in v or 'USA' in v: return 'NYSE'
        if 'B3' in v or 'BVMF' in v or 'BOVESPA' in v: return 'B3'
    s = str(fld(t,'symbol','ticker','asset','pair') or '').upper()
    base = re.split(r'[/\-]', s)[0].replace('USDT','').replace('USD','')
    if fld(t,'is_arbitrage','arbi') or 'ARB' in str(fld(t,'strategy') or '').upper(): return 'ARBI'
    if any(c in s for c in ('USDT','USD','/','-')) or base in CRYPTO: return 'CRYPTO'
    if s.endswith('.SA') or re.search(r'[A-Z]{4}\d{1,2}$', s): return 'B3'   # PETR4, VALE3
    if re.fullmatch(r'[A-Z]{1,5}', s): return 'NYSE'                        # AAPL, PBR(ADR)
    return 'OUTRO'

def pnl(t):
    v = fld(t,'pnl','profit','realized_pnl','pnl_usd','result','net_pnl')
    try: return float(v)
    except: return 0.0

agg = defaultdict(lambda: {'n':0,'win':0,'pnl':0.0})
for t in trades:
    b = book(t)
    a = agg[b]; a['n'] += 1; a['pnl'] += pnl(t)
    if pnl(t) > 0: a['win'] += 1

print(f"\nTotal trades: {len(trades)}")
print(f"{'BOOK':8s} {'N':>6s} {'WIN%':>7s} {'P&L total':>14s} {'P&L medio':>11s}")
for b in ('B3','NYSE','CRYPTO','ARBI','OUTRO'):
    a = agg.get(b)
    if not a or a['n']==0: continue
    wr = 100*a['win']/a['n']
    print(f"{b:8s} {a['n']:6d} {wr:6.1f}% {a['pnl']:13,.0f} {a['pnl']/a['n']:10,.0f}")
print("\nNota: V3 = B3+NYSE+CRYPTO (arbi nao usa o score engine).")
