"""[v10.31] CedroSocketProvider — Cedro Crystal real-time streaming via TCP/Telnet socket.

Protocol: datafeed1.cedrotech.com:81 (Cedro Crystal Socket API).
After TCP connect: send 3 CRLF-terminated lines: software_key (blank) / username / password.
On "You are connected" banner, the session is live. Commands:
  SQT <asset>     → subscribe quote (streaming T:SYMBOL:idx:val:idx:val...!)
  USQ <asset>     → unsubscribe quote
  BQT <asset>     → subscribe book (streaming B:SYMBOL:...)
  UBQ <asset>     → unsubscribe book

Messages terminate with "!". A single TCP segment may carry multiple messages or a partial
message — we buffer until we see "!" and parse each complete chunk.

Cedro SQT field indexes (see API Socket PDF §SQT). We expose price + full analysis fields:
price, prev_close, day_high, day_low, day_open, week_high, week_low, month_high, month_low,
year_high, year_low, volume_day, avg_vol_20d, market_cap, sector, subsector, segment,
variation_pct, prev_day_close, last_trade_time, trading_phase, market_type, etc.

This provider maintains a single persistent socket in a background thread; subscriptions
are reference-counted so callers can safely re-subscribe. Auto-reconnects on drop.

Usage:
    from modules.cedro_socket_provider import get_cedro
    cedro = get_cedro()  # singleton
    q = cedro.get_quote('PETR4')      # dict with price + all analysis fields
    cedro.subscribe(['VALE3', 'ITUB4'])  # pre-warm cache
    cedro.get_price('PETR4')           # shortcut, just the last price

Environment variables:
    CEDRO_SOCKET_HOST   (default: datafeed1.cedrotech.com)
    CEDRO_SOCKET_HOST_BACKUP (default: datafeed2.cedrotech.com)
    CEDRO_SOCKET_PORT   (default: 81)
    CEDRO_USER          (required)
    CEDRO_PASSWORD      (required)
    CEDRO_SOFTWARE_KEY  (optional, usually blank)
"""

import os
import socket
import threading
import time
import logging
from datetime import datetime
from typing import Optional, Iterable

log = logging.getLogger('egreja.cedro')


# ── [v10.43] Brapi fallback helpers ──────────────────────────────────────
# Some tickers (observed: EMBR3) are silently rejected by the Cedro feed even
# though the socket subscription is accepted. They show trading_phase='F' + price=0
# while B3 is actively trading. When detected, we fetch from Brapi as fallback.
_BRAPI_TOKEN = os.environ.get('BRAPI_TOKEN', '')
_BRAPI_CACHE: dict = {}  # symbol -> (timestamp, quote_dict)
_BRAPI_CACHE_TTL = 60  # seconds


def _is_b3_market_hours() -> bool:
    """Checa se é horário de pregão B3 (10h-18h BRT = 13h-21h UTC, seg-sex).
    Aproximação simples (ignora DST transitions, mas funcional para a detecção)."""
    try:
        now = datetime.utcnow()
        if now.weekday() >= 5:
            return False
        return 13 <= now.hour <= 21
    except Exception:
        return False


def _brapi_fallback(symbol):
    """Fetch a quote from Brapi.dev as fallback for stuck Cedro tickers. Cached 60s."""
    if not _BRAPI_TOKEN:
        return None
    try:
        import requests
    except ImportError:
        return None
    now_ts = time.time()
    if symbol in _BRAPI_CACHE:
        ts, cached = _BRAPI_CACHE[symbol]
        if now_ts - ts < _BRAPI_CACHE_TTL:
            return cached
    try:
        r = requests.get(
            f'https://brapi.dev/api/quote/{symbol}',
            params={'token': _BRAPI_TOKEN},
            timeout=5,
        )
        if r.status_code != 200:
            return None
        data = r.json()
        results = data.get('results') or []
        if not results:
            return None
        row = results[0]
        price = row.get('regularMarketPrice')
        if not price or price <= 0:
            return None
        q = {
            'price': float(price),
            'prev_close': row.get('regularMarketPreviousClose'),
            'day_high': row.get('regularMarketDayHigh'),
            'day_low': row.get('regularMarketDayLow'),
            'day_open': row.get('regularMarketOpen'),
            'volume_qty': row.get('regularMarketVolume'),
            'updated_at': row.get('regularMarketTime'),
        }
        _BRAPI_CACHE[symbol] = (now_ts, q)
        return q
    except Exception as e:
        log.debug(f'brapi_fallback({symbol}) failed: {e}')
        return None


# SQT field index → canonical name. Covers what we need for full analysis.
# See Cedro API Socket PDF for the complete list.
SQT_FIELDS = {
    0:  'last_mod_time',       # HHMMSS
    1:  'last_mod_date',       # YYYYMMDD
    2:  'price',               # last trade price
    3:  'best_bid',            # melhor oferta compra
    4:  'best_ask',            # melhor oferta venda
    5:  'last_trade_time',     # HHMMSS
    6:  'current_trade_qty',
    7:  'last_trade_qty',
    8:  'trades_count',
    9:  'volume_qty',          # quantidade acumulada
    10: 'volume_financial',    # volume $ acumulado
    11: 'day_high',
    12: 'day_low',
    13: 'prev_close',          # fechamento dia anterior
    14: 'day_open',
    15: 'best_bid_time',
    16: 'best_ask_time',
    17: 'bid_vol_acc',
    18: 'ask_vol_acc',
    19: 'best_bid_vol',
    20: 'best_ask_vol',
    21: 'variation_pct',
    36: 'week_close',
    37: 'month_close',
    38: 'year_close',
    39: 'prev_day_open',
    40: 'prev_day_high',
    41: 'prev_day_low',
    42: 'avg_price',
    43: 'vh_daily',
    44: 'market_code',         # 1=Bovespa 3=BMF 4=Indices 30=Bitcoin 10=Nyse 12=Nasdaq
    45: 'asset_type',          # 1=Spot 2=Option 3=Index 4=Commodity 7=Future 13=ETF
    46: 'standard_lot',
    47: 'description',         # DOL, INDICE BOVESPA
    48: 'classification_name',
    49: 'quote_form',
    50: 'intraday_date_forces',
    51: 'last_trade_date_forces',
    52: 'short_description',
    53: 'cancelled_trade_id',
    54: 'last_trade_date',     # YYYYMMDD
    64: 'expiry_date',         # options
    65: 'expired',
    66: 'total_papers',
    67: 'instrument_status',   # 101=Normal 102=Auction 105=Suspended 118=Frozen
    72: 'option_type',         # A/E
    74: 'option_direction',    # P=Put C=Call
    81: 'underlying_symbol',   # for options
    82: 'theoretical_open',
    83: 'theoretical_qty',
    84: 'asset_status',        # 0=Normal 1=Frozen 2=Suspended 3=Auction 4=Inhibited
    85: 'strike_price',
    86: 'diff_vs_previous',
    87: 'previous_date',
    88: 'trading_phase',       # P/A/PN/N/E/R/NE/F/NO/T
    89: 'prev_day_avg',
    90: 'margin_interval_btc',
    94: 'avg_vol_20d',
    95: 'market_cap',
    96: 'market_type',         # RT/D/EOD
    97: 'var_week_pct',
    98: 'var_month_pct',
    99: 'var_year_pct',
    100: 'open_contracts',
    101: 'business_days_to_expiry',
    102: 'days_to_expiry',
    103: 'day_settlement',
    104: 'prev_settlement',
    105: 'security_id',
    106: 'tick_direction',
    107: 'tunnel_upper_hard',
    108: 'tunnel_lower_hard',
    109: 'trading_phase_fix',
    110: 'tick_size',
    111: 'min_trade_volume',
    112: 'min_price_increment',
    115: 'unique_instrument_id',
    116: 'currency',
    117: 'security_type',       # FUT, SPOT, OPT, SOPT, FOPT, DTERM
    118: 'security_sub_type',
    119: 'product_id',
    120: 'expiry_month_year',
    121: 'strike_price_2',
    122: 'strike_currency',
    123: 'contract_multiplier',
    125: 'last_tradeable_time',
    126: 'group_indicator',
    127: 'current_rate_adj',
    128: 'prev_rate_adj',
    134: 'vol_hour_var_vs_20d',
    135: 'vol_to_hour_var_vs_20d',
    136: 'sector_code',
    137: 'subsector_code',
    138: 'segment_code',
    140: 'reference_price',
    146: 'var_prev_adj_pct',
    147: 'diff_prev_adj',
    154: 'expire_date',
    155: 'week_low',
    156: 'week_high',
    157: 'month_low',
    158: 'month_high',
    159: 'year_low',
    160: 'year_high',
}

# Fields that should be parsed as float
_FLOAT_FIELDS = {2, 3, 4, 10, 11, 12, 13, 14, 17, 18, 21, 36, 37, 38, 39, 40, 41, 42, 43,
                 82, 85, 86, 89, 90, 94, 95, 97, 98, 99, 103, 104, 107, 108, 112, 121, 123,
                 127, 128, 134, 135, 140, 146, 147, 155, 156, 157, 158, 159, 160}


class CedroSocketProvider:
    """Single persistent socket connection + streaming quote cache."""

    def __init__(self,
                 host: Optional[str] = None,
                 port: Optional[int] = None,
                 user: Optional[str] = None,
                 password: Optional[str] = None,
                 software_key: str = ''):
        self.host = host or os.environ.get('CEDRO_SOCKET_HOST', 'datafeed1.cedrotech.com')
        self.host_backup = os.environ.get('CEDRO_SOCKET_HOST_BACKUP', 'datafeed2.cedrotech.com')
        self.port = port or int(os.environ.get('CEDRO_SOCKET_PORT', '81'))
        self.user = user or os.environ.get('CEDRO_USER', '')
        self.password = password or os.environ.get('CEDRO_PASSWORD', '')
        self.software_key = software_key or os.environ.get('CEDRO_SOFTWARE_KEY', '')

        self._sock: Optional[socket.socket] = None
        self._connected = threading.Event()
        self._stop = threading.Event()
        self._send_lock = threading.Lock()

        self._cache: dict = {}            # {symbol_upper: {field_name: value, ...}}
        self._cache_lock = threading.Lock()
        self._subs: dict = {}             # {symbol_upper: refcount}
        self._subs_lock = threading.Lock()

        self._last_connect_ts: float = 0
        self._last_msg_ts: float = 0
        self._msg_count: int = 0
        self._reconnect_count: int = 0

        self._reader_thread: Optional[threading.Thread] = None
        self._started = False

        self.enabled = bool(self.user and self.password)
        if not self.enabled:
            log.warning('[cedro-socket] disabled — CEDRO_USER/CEDRO_PASSWORD not set')

    # ─── Public API ─────────────────────────────────────────────────────

    def start(self):
        """Start background reader thread (idempotent)."""
        if self._started or not self.enabled:
            return
        self._started = True
        self._stop.clear()
        self._reader_thread = threading.Thread(target=self._run, daemon=True, name='cedro-socket')
        self._reader_thread.start()
        log.info(f'[cedro-socket] provider started host={self.host}:{self.port} user={self.user}')

    def stop(self):
        self._stop.set()
        try:
            if self._sock:
                self._sock.close()
        except Exception:
            pass
        self._started = False

    def subscribe(self, symbols: Iterable[str]):
        """Add symbols to subscription set. Will re-send SQT on next connect cycle."""
        if not self.enabled:
            return
        new = []
        with self._subs_lock:
            for s in symbols:
                k = s.strip().upper()
                if not k: continue
                if k not in self._subs:
                    self._subs[k] = 1
                    new.append(k)
                else:
                    self._subs[k] += 1
        if self._connected.is_set() and new:
            for k in new:
                self._send_command(f'SQT {k.lower()}')

    def unsubscribe(self, symbols: Iterable[str]):
        if not self.enabled:
            return
        gone = []
        with self._subs_lock:
            for s in symbols:
                k = s.strip().upper()
                if k in self._subs:
                    self._subs[k] -= 1
                    if self._subs[k] <= 0:
                        del self._subs[k]
                        gone.append(k)
        if self._connected.is_set() and gone:
            for k in gone:
                self._send_command(f'USQ {k.lower()}')

    def get_options_chain(self, underlying: str, max_wait_ms: int = 3000,
                          max_symbols: int = 300) -> list:
        """
        Resolve and subscribe the live B3 option chain for `underlying` via
        Cedro Crystal socket. Since Crystal has no GOP-style list command,
        we discover option tickers via BRAPI's `available` endpoint and bulk
        SQT them through the already-open socket.

        Returns a list of dicts with: symbol, underlying, strike, expiry,
        option_type ('C'/'P'), bid, ask, last, volume, oi.
        """
        if not self.enabled:
            return []
        root = underlying.strip().upper()[:4]  # PETR4 → PETR
        if not root:
            return []

        # [v10.34c] Pass live spot so synthetic discovery centers the strike
        # band around current price instead of the 50.0 default fallback.
        _spot = 0.0
        try:
            _sq = self._get_cached(underlying.strip().upper())
            if _sq and _sq.get('price'):
                _spot = float(_sq['price'])
        except Exception:
            pass

        syms = self._discover_option_symbols(root, spot=_spot)
        if not syms:
            return []
        if len(syms) > max_symbols:
            # [v10.34e] Sort by proximity to spot so calls AND puts survive
            # truncation (alphabetical sort used to drop M-X puts entirely).
            import re as _re2
            def _dist(sym: str) -> float:
                m = _re2.match(rf'^{root}[A-X](\d+)$', sym)
                if not m or _spot <= 0:
                    return 0.0
                try:
                    return abs(float(int(m.group(1))) - _spot)
                except Exception:
                    return 0.0
            syms = sorted(syms, key=_dist)[:max_symbols]

        # ─── Bulk SQT via persistent socket ────────────────────────────────
        self.subscribe(syms)

        # ─── Wait briefly for quotes to populate ───────────────────────────
        import time as _t
        deadline = _t.time() + (max_wait_ms / 1000.0)
        while _t.time() < deadline:
            with self._cache_lock:
                populated = sum(
                    1 for s in syms
                    if self._cache.get(s) and self._cache[s].get('price') is not None
                )
            if populated >= len(syms) * 0.5:
                break
            _t.sleep(0.05)

        # ─── Build chain from cache ────────────────────────────────────────
        chain = []
        with self._cache_lock:
            for s in syms:
                q = self._cache.get(s)
                if not q:
                    continue
                if q.get('asset_type') not in (2, 16, 17, None):
                    # 2=Opção, 16=Opção sobre à vista, 17=Opção sobre futuro
                    # Skip if explicitly non-option
                    if q.get('asset_type') is not None:
                        continue
                direction = q.get('option_direction')  # 'C' or 'P'
                if direction not in ('C', 'P'):
                    # Infer from B3 naming: letter 5 of symbol
                    if len(s) >= 5 and s[4] in 'ABCDEFGHIJKL':
                        direction = 'C'
                    elif len(s) >= 5 and s[4] in 'MNOPQRSTUVWX':
                        direction = 'P'
                    else:
                        continue
                strike = q.get('strike_price') or q.get('strike_price_2') or 0.0
                expiry = q.get('expiry_date')
                exp_iso = None
                if expiry:
                    exp_s = str(expiry).strip()
                    if len(exp_s) == 8 and exp_s.isdigit():
                        exp_iso = f'{exp_s[:4]}-{exp_s[4:6]}-{exp_s[6:8]}'

                # [v10.34d] Infer missing strike/expiry from B3 ticker convention.
                # Cedro SQT sometimes doesn't echo fields 64/85 for low-liquidity
                # series, but the ticker itself encodes both.
                try:
                    if (not strike or float(strike) <= 0) and len(s) >= 6:
                        _code = ''.join(ch for ch in s[5:] if ch.isdigit())
                        if _code:
                            _k = int(_code)
                            # Heuristic: codes 10-999 in "reais" for most stocks;
                            # for penny stocks we store as int*0.1. Keep 1:1 for now.
                            strike = float(_k)
                    if not exp_iso and len(s) >= 5:
                        _letter = s[4].upper()
                        _month_map = {
                            'A': 1, 'B': 2, 'C': 3, 'D': 4, 'E': 5, 'F': 6,
                            'G': 7, 'H': 8, 'I': 9, 'J': 10, 'K': 11, 'L': 12,
                            'M': 1, 'N': 2, 'O': 3, 'P': 4, 'Q': 5, 'R': 6,
                            'S': 7, 'T': 8, 'U': 9, 'V': 10, 'W': 11, 'X': 12,
                        }
                        _m = _month_map.get(_letter)
                        if _m:
                            import datetime as _dtm
                            _today = _dtm.date.today()
                            _y = _today.year if _m >= _today.month else _today.year + 1
                            # 3rd Monday of month = monthly expiry
                            _first = _dtm.date(_y, _m, 1)
                            _offset = (0 - _first.weekday()) % 7  # days to first Mon
                            _third_mon = _first + _dtm.timedelta(days=_offset + 14)
                            exp_iso = _third_mon.isoformat()
                except Exception:
                    pass
                chain.append({
                    'symbol': s,
                    'underlying': underlying.upper(),
                    'strike': float(strike or 0.0),
                    'expiry': exp_iso,
                    'option_type': direction,
                    'bid': float(q.get('best_bid') or 0.0),
                    'ask': float(q.get('best_ask') or 0.0),
                    'last': float(q.get('price') or 0.0),
                    'volume': int(q.get('volume_qty') or 0),
                    'oi': int(q.get('open_contracts') or 0),
                })
        return chain

    # ─── Options discovery cache ───────────────────────────────────────────
    _opt_disc_cache: dict = {}   # root → (ts, symbols_list)
    _opt_disc_ttl: int = 600     # [v10.34] 10-min cache (DB queries are cheap but limit noise)

    def _discover_option_symbols(self, root: str, spot: float = 0.0) -> list:
        """
        [v10.34c] Generate B3 option tickers synthetically per B3 convention.

        B3 series letters:  A-L = call months Jan-Dec,  M-X = put months Jan-Dec.
        Ticker format:       {ROOT4}{LETTER}{STRIKE_CODE}
                             e.g. PETRD48 = PETR4 April call strike 48
                                  PETRP48 = PETR4 April put  strike 48

        We fan out 3 nearest monthly expiries × {call, put} × 13 strikes
        (spot ± ~30% in integer steps). Cedro's socket will silently return
        no data for tickers that don't exist, while real listings populate
        the cache within ~3s. Cached 10 min per root.

        Strike step heuristic:
          - spot < 10  → step 0.50 (converted to code as int(strike*10))
          - 10 ≤ spot < 30 → step 1
          - spot ≥ 30 → step 2

        DB + BRAPI are merged as a secondary source to catch any exotic
        listings (e.g. weekly series, non-standard strikes).
        """
        import time as _t, re as _re, datetime as _dt
        now = _t.time()
        hit = self._opt_disc_cache.get(root)
        if hit and now - hit[0] < self._opt_disc_ttl:
            return hit[1]

        # B3 truncates root to 4 chars (PETR4 → PETR, VALE3 → VALE, BOVA11 → BOVA).
        root4 = root.strip().upper()[:4]
        pattern = _re.compile(rf'^{root4}[A-X]\d{{1,4}}$')
        symbols: set = set()

        # ── (1) Synthetic generator — PRIMARY source
        #        Use current date to pick next 3 monthly series.
        call_letters = 'ABCDEFGHIJKL'   # Jan–Dec
        put_letters  = 'MNOPQRSTUVWX'   # Jan–Dec
        today = _dt.date.today()
        months_to_scan = []
        for delta in range(0, 3):
            m = today.month + delta
            y = today.year + (m - 1) // 12
            m = ((m - 1) % 12) + 1
            months_to_scan.append((y, m))

        # Determine strike step / strike code encoding.
        # Fall back to coarse step if spot is unknown.
        if spot <= 0:
            try:
                sq = self._get_cached(f'{root4}4') or self._get_cached(f'{root4}3') or self._get_cached(f'{root4}11')
                if sq and sq.get('price'):
                    spot = float(sq['price'])
            except Exception:
                pass
        if spot <= 0:
            spot = 50.0  # neutral default — produces wide band; harmless misses
        # [v10.34f] Finer grid so skew/fst/roll strategies get 3+ matching
        # strikes per expiry. B3 lists strikes at step 1 for most blue chips.
        if spot < 10:
            step = 0.5
            code = lambda s: str(int(round(s * 10)))   # 5.00 → "50"
        elif spot < 150:
            step = 1.0
            code = lambda s: str(int(round(s)))
        else:
            step = 2.0
            code = lambda s: str(int(round(s)))

        # Narrower band (±20%) to stay well inside 300-ticker cap with step 1.
        lo = max(step, spot * 0.8)
        hi = spot * 1.2
        # Snap to step grid
        k = lo - (lo % step)
        strikes = []
        while k <= hi + 1e-9:
            if k > 0:
                strikes.append(round(k, 2))
            k += step

        for (y, m) in months_to_scan:
            cl = call_letters[m - 1]
            pl = put_letters[m - 1]
            for k in strikes:
                symbols.add(f'{root4}{cl}{code(k)}')
                symbols.add(f'{root4}{pl}{code(k)}')

        # ── (2) DB supplement — pick up any non-standard listings seen historically
        try:
            from modules.database import get_db as _get_db
            _conn = _get_db()
            if _conn is not None:
                for _tbl in ('strategy_trade_legs', 'strategy_opportunities_log'):
                    try:
                        _cur = _conn.cursor()
                        _cur.execute(
                            f"SELECT DISTINCT symbol FROM {_tbl} "
                            f"WHERE symbol REGEXP %s "
                            f"AND timestamp >= DATE_SUB(NOW(), INTERVAL 90 DAY) "
                            f"LIMIT 500",
                            (rf'^{root4}[A-X][0-9]{{1,4}}$',)
                        )
                        for (sym,) in _cur.fetchall():
                            su = str(sym).upper()
                            if pattern.match(su):
                                symbols.add(su)
                        _cur.close()
                    except Exception as _sql_e:
                        log.debug(f'[cedro-socket] {_tbl} query failed for {root4}: {_sql_e}')
                try:
                    _conn.close()
                except Exception:
                    pass
        except Exception as _db_e:
            log.debug(f'[cedro-socket] DB supplement unavailable for {root4}: {_db_e}')

        opts = sorted(symbols)
        log.info(f'[cedro-socket] generated {len(opts)} option symbols for {root4} '
                 f'(spot={spot:.2f}, step={step}, months={months_to_scan}, first={opts[:3]})')
        self._opt_disc_cache[root] = (now, opts)
        return opts

    def get_quote(self, symbol: str, wait_ms: int = 1500) -> Optional[dict]:
        """Return full cached quote dict for symbol. Auto-subscribes if missing, waits briefly.

        [v10.43] FALLBACK: If Cedro reports trading_phase='F' with price=0 but B3 is trading,
        tries Brapi. Covers known cases (e.g. EMBR3) where the Cedro feed silently blocks
        the subscription despite accepting the SQT command.
        """
        if not self.enabled:
            return None
        k = symbol.strip().upper()
        q = self._get_cached(k)
        if q and q.get('price'):
            return q

        # ── [v10.43] Fast path: known-stuck ticker (cached with trading_phase=F) ──
        # Skip the 1.5s wait loop if we already know Cedro won't deliver.
        if (q is not None and not q.get('price')
                and q.get('trading_phase') == 'F' and _is_b3_market_hours()):
            brapi_q = _brapi_fallback(k)
            if brapi_q:
                q = dict(q)
                q['price'] = brapi_q['price']
                q['prev_close'] = brapi_q.get('prev_close') or q.get('prev_close')
                q['day_high'] = brapi_q.get('day_high') or q.get('day_high')
                q['day_low'] = brapi_q.get('day_low') or q.get('day_low')
                q['source'] = 'brapi-fallback'
                q['_fallback_reason'] = 'cedro_trading_phase_F_fastpath'
                q['_updated_at'] = brapi_q.get('updated_at', datetime.utcnow().isoformat())
                log.info(f'[cedro] Brapi fast-fallback for {k}: price={brapi_q["price"]}')
                return q

        # Not cached or waiting for snapshot — subscribe and wait
        self.subscribe([k])
        deadline = time.time() + wait_ms / 1000.0
        while time.time() < deadline:
            q = self._get_cached(k)
            if q and q.get('price'):
                return q
            time.sleep(0.05)
        q = self._get_cached(k)  # maybe partial

        # ── [v10.43] Slow path fallback (after wait loop timed out) ───────────
        if (q is not None and not q.get('price')
                and q.get('trading_phase') == 'F' and _is_b3_market_hours()):
            brapi_q = _brapi_fallback(k)
            if brapi_q:
                q = dict(q)
                q['price'] = brapi_q['price']
                q['prev_close'] = brapi_q.get('prev_close') or q.get('prev_close')
                q['day_high'] = brapi_q.get('day_high') or q.get('day_high')
                q['day_low'] = brapi_q.get('day_low') or q.get('day_low')
                q['source'] = 'brapi-fallback'
                q['_fallback_reason'] = 'cedro_trading_phase_F_slowpath'
                q['_updated_at'] = brapi_q.get('updated_at', datetime.utcnow().isoformat())
                log.warning(f'[cedro] Brapi slow-fallback for {k}: price={brapi_q["price"]}')
        return q

    def get_price(self, symbol: str, wait_ms: int = 1500) -> Optional[float]:
        q = self.get_quote(symbol, wait_ms=wait_ms)
        if q and q.get('price'):
            try:
                return float(q['price'])
            except (TypeError, ValueError):
                return None
        return None

    def get_batch(self, symbols: Iterable[str], wait_ms: int = 2000) -> dict:
        """Fetch multiple quotes — subscribes missing, waits once, returns dict."""
        if not self.enabled:
            return {}
        syms = [s.strip().upper() for s in symbols if s.strip()]
        missing = [k for k in syms if not self._get_cached(k, fresh_only=False)]
        if missing:
            self.subscribe(missing)
            deadline = time.time() + wait_ms / 1000.0
            while time.time() < deadline:
                if all(self._get_cached(k) for k in missing):
                    break
                time.sleep(0.05)
        out = {}
        for k in syms:
            q = self._get_cached(k)
            if q:
                out[k] = q
        return out

    def get_analysis(self, symbol: str) -> dict:
        """Expose all analysis-oriented fields in a convenient structure for dashboards / brain."""
        q = self.get_quote(symbol) or {}
        price = q.get('price') or 0
        prev = q.get('prev_close') or 0
        return {
            'symbol': symbol.upper(),
            'price': price,
            'prev_close': prev,
            'change_pct': q.get('variation_pct'),
            'day_open': q.get('day_open'),
            'day_high': q.get('day_high'),
            'day_low': q.get('day_low'),
            'week_high': q.get('week_high'),
            'week_low': q.get('week_low'),
            'month_high': q.get('month_high'),
            'month_low': q.get('month_low'),
            'year_high': q.get('year_high'),
            'year_low': q.get('year_low'),
            'var_week_pct': q.get('var_week_pct'),
            'var_month_pct': q.get('var_month_pct'),
            'var_year_pct': q.get('var_year_pct'),
            'volume_qty': q.get('volume_qty'),
            'volume_financial': q.get('volume_financial'),
            'avg_vol_20d': q.get('avg_vol_20d'),
            'trades_count': q.get('trades_count'),
            'best_bid': q.get('best_bid'),
            'best_ask': q.get('best_ask'),
            'best_bid_vol': q.get('best_bid_vol'),
            'best_ask_vol': q.get('best_ask_vol'),
            'market_cap': q.get('market_cap'),
            'sector_code': q.get('sector_code'),
            'subsector_code': q.get('subsector_code'),
            'segment_code': q.get('segment_code'),
            'classification_name': q.get('classification_name'),
            'description': q.get('description'),
            'short_description': q.get('short_description'),
            'asset_type': q.get('asset_type'),
            'market_code': q.get('market_code'),
            'security_type': q.get('security_type'),
            'currency': q.get('currency'),
            'trading_phase': q.get('trading_phase'),
            'instrument_status': q.get('instrument_status'),
            'asset_status': q.get('asset_status'),
            'last_trade_time': q.get('last_trade_time'),
            'last_trade_date': q.get('last_trade_date'),
            'updated_at': q.get('_updated_at'),
            'source': 'cedro-socket',
        }

    def healthcheck(self) -> dict:
        return {
            'enabled': self.enabled,
            'connected': self._connected.is_set(),
            'host': self.host,
            'port': self.port,
            'user': self.user,
            'subscriptions': len(self._subs),
            'cached_symbols': len(self._cache),
            'msg_count': self._msg_count,
            'reconnects': self._reconnect_count,
            'last_msg_age_s': round(time.time() - self._last_msg_ts, 2) if self._last_msg_ts else None,
            'uptime_s': round(time.time() - self._last_connect_ts, 1) if self._last_connect_ts else 0,
        }

    # ─── Internals ──────────────────────────────────────────────────────

    def _get_cached(self, k: str, fresh_only=True) -> Optional[dict]:
        with self._cache_lock:
            v = self._cache.get(k)
            if not v:
                return None
            return dict(v)

    def _send_command(self, cmd: str) -> bool:
        with self._send_lock:
            if not self._sock:
                return False
            try:
                self._sock.sendall((cmd + '\r\n').encode())
                return True
            except Exception as e:
                log.warning(f'[cedro-socket] send fail "{cmd}": {e}')
                return False

    def _connect_and_login(self, host: str) -> bool:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(15)
            s.connect((host, self.port))
            # Drain banner ("Welcome to Cedro Crystal / Username:")
            buf = b''
            s.settimeout(5)
            try:
                while True:
                    d = s.recv(4096)
                    if not d: break
                    buf += d
                    if b'Username' in buf or b'Software' in buf: break
            except socket.timeout:
                pass
            s.sendall((self.software_key + '\r\n').encode())
            time.sleep(0.3)
            s.sendall((self.user + '\r\n').encode())
            time.sleep(0.3)
            s.sendall((self.password + '\r\n').encode())
            # Wait for "You are connected"
            buf2 = b''
            s.settimeout(7)
            t_end = time.time() + 7
            while time.time() < t_end:
                try:
                    d = s.recv(4096)
                    if not d: break
                    buf2 += d
                    if b'You are connected' in buf2:
                        break
                    if b'Invalid' in buf2 or b'invalid' in buf2 or b'Software key not found' in buf2:
                        log.error(f'[cedro-socket] login rejected: {buf2[:200]!r}')
                        s.close()
                        return False
                except socket.timeout:
                    break
            if b'You are connected' not in buf2:
                log.warning(f'[cedro-socket] no login confirm: {buf2[:200]!r}')
                s.close()
                return False
            self._sock = s
            self._connected.set()
            self._last_connect_ts = time.time()
            log.info(f'[cedro-socket] logged in to {host}:{self.port}')
            return True
        except Exception as e:
            log.warning(f'[cedro-socket] connect {host}:{self.port} failed: {e}')
            try: s.close()
            except: pass
            return False

    def _resubscribe_all(self):
        with self._subs_lock:
            syms = list(self._subs.keys())
        for k in syms:
            self._send_command(f'SQT {k.lower()}')

    def _parse_message(self, raw: str):
        """Parse a single message (without trailing '!'). T:<sym>:<hhmmss>:<idx>:<val>:<idx>:<val>..."""
        if not raw or len(raw) < 3:
            return
        # Only handle quote messages (T:) for now
        if not raw.startswith('T:'):
            return
        try:
            parts = raw.split(':')
            # parts[0]='T', [1]=symbol, [2]=hhmmss_header, then pairs idx:val
            if len(parts) < 4: return
            sym = parts[1].upper()
            header_time = parts[2]
            update: dict = {'_hdr_time': header_time, '_updated_at': datetime.utcnow().isoformat()}
            i = 3
            while i + 1 < len(parts):
                try:
                    idx = int(parts[i])
                except ValueError:
                    i += 1
                    continue
                val = parts[i + 1]
                name = SQT_FIELDS.get(idx)
                if name:
                    if idx in _FLOAT_FIELDS:
                        try: val = float(val)
                        except (ValueError, TypeError): val = None
                    elif idx in (6, 7, 8, 9, 17, 18, 19, 20, 46, 66, 83, 100, 101, 102, 110,
                                 111, 113, 114, 115, 119, 130, 136, 137, 138):
                        try: val = int(val)
                        except (ValueError, TypeError): val = None
                    update[name] = val
                i += 2
            with self._cache_lock:
                cur = self._cache.get(sym, {})
                cur.update(update)
                self._cache[sym] = cur
            self._last_msg_ts = time.time()
            self._msg_count += 1
        except Exception as e:
            log.debug(f'[cedro-socket] parse error: {e} raw={raw[:200]!r}')

    def _run(self):
        """Main reader loop: connect → login → subscribe → read stream → reconnect on drop."""
        backoff = 1.0
        hosts = [self.host, self.host_backup]
        host_idx = 0
        buf = b''
        while not self._stop.is_set():
            host = hosts[host_idx % len(hosts)]
            if not self._connect_and_login(host):
                host_idx += 1
                self._reconnect_count += 1
                time.sleep(min(backoff, 30))
                backoff = min(backoff * 1.6, 30)
                continue
            backoff = 1.0
            # Re-subscribe everything
            self._resubscribe_all()
            buf = b''
            self._sock.settimeout(15)  # reading loop timeout
            while not self._stop.is_set():
                try:
                    d = self._sock.recv(16384)
                    if not d:
                        log.warning('[cedro-socket] EOF, will reconnect')
                        break
                    buf += d
                    while b'!' in buf:
                        msg, buf = buf.split(b'!', 1)
                        try:
                            text = msg.decode('latin-1', errors='replace').strip('\r\n\t ')
                        except Exception:
                            continue
                        self._parse_message(text)
                except socket.timeout:
                    # periodic keepalive ping could go here; for now just loop
                    continue
                except Exception as e:
                    log.warning(f'[cedro-socket] read error: {e}')
                    break
            self._connected.clear()
            try: self._sock.close()
            except: pass
            self._sock = None
            self._reconnect_count += 1


# ─── Singleton helper ───────────────────────────────────────────────────

_instance: Optional[CedroSocketProvider] = None
_instance_lock = threading.Lock()


def get_cedro() -> CedroSocketProvider:
    global _instance
    with _instance_lock:
        if _instance is None:
            _instance = CedroSocketProvider()
            _instance.start()
        return _instance


def is_enabled() -> bool:
    return bool(os.environ.get('CEDRO_USER') and os.environ.get('CEDRO_PASSWORD'))
