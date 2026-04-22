"""Teste E2E: simula DB e chama advisor completo."""
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
os.environ['ADVISOR_ENTRY_ENABLED'] = 'true'
os.environ['ADVISOR_EXIT_ENABLED'] = 'true'
os.environ.pop('POLYGON_API_KEY', None)
os.environ.pop('ADVISOR_STOCKS_ENABLED', None)
os.environ.pop('ADVISOR_CRYPTO_ENABLED', None)

from modules.unified_brain.advisor_entry import evaluate_entry
from modules.unified_brain.advisor_exit import evaluate_exit
from modules.unified_brain.advisor_common import get_cache


class _SmartCursor:
    """Cursor que inspeciona SQL e retorna dado apropriado."""
    def __init__(self, db_state):
        self._db = db_state
        self._last_query = ''
    def execute(self, query, params=None):
        self._last_query = query.strip().upper()
    def fetchall(self):
        # similarity_vote espera: {'pnl', 'pnl_pct'}
        if 'SELECT PNL_PCT' in self._last_query or 'SELECT PNL,' in self._last_query:
            return self._db.get('similarity_rows', [])
        if 'PEAK_PNL_PCT' in self._last_query:
            return self._db.get('similarity_rows', [])
        return []
    def fetchone(self):
        if 'COUNT(*)' in self._last_query and 'STOP_LOSS' in self._last_query:
            return self._db.get('risk_row', {'n_closed': 0, 'n_stops': 0, 'day_pnl': 0,
                                              'n_wins': 0, 'n_losses': 0})
        if 'COUNT(*)' in self._last_query and 'REGIME_V2' in self._last_query:
            return self._db.get('regime_row', {'n': 5, 'wins': 3, 'avg_pnl_pct': 0.2})
        if 'COUNT(*)' in self._last_query:
            return self._db.get('count_row', {'n': 5, 'wins': 3})
        return None
    def close(self): pass


class _SmartConn:
    def __init__(self, db_state):
        self._db = db_state
    def cursor(self, dictionary=False):
        return _SmartCursor(self._db)
    def close(self): pass
    def commit(self): pass


def make_db(rows_sim=None, risk_row=None, regime_row=None):
    state = {
        'similarity_rows': rows_sim or [],
        'risk_row': risk_row,
        'regime_row': regime_row,
    }
    return lambda: _SmartConn(state)


class _L:
    def debug(self,*a): pass
    def info(self,*a): pass
    def warning(self,*a): pass
    def error(self,*a): print('[TEST_ERR]', *a)


def _reset():
    get_cache().clear()


def test_entry_normal_stock():
    _reset()
    db_fn = make_db(
        rows_sim=[{'pnl': 100, 'pnl_pct': 0.5} if i%2==0 else {'pnl': -80, 'pnl_pct': -0.3} for i in range(10)],
        risk_row={'n_closed': 10, 'n_stops': 1, 'day_pnl': 500, 'n_wins': 5, 'n_losses': 5},
        regime_row={'n': 10, 'wins': 5, 'avg_pnl_pct': 0.1},
    )
    d = evaluate_entry(db_fn, _L(),
                       symbol='PETR4', asset_type='stock',
                       score_v3=72, regime_v3='TRENDING',
                       direction='LONG', hour_of_day=11, weekday=2)
    assert d['bypassed'] is False, f'unexpected bypass: {d}'
    assert d['action'] in ('pass', 'reduce', 'boost', 'block')
    assert len(d['votes']) == 5
    print(f"  ✓ stock: action={d['action']} agg={d['aggregate_score']:.2f} votes={d['votes']}")


def test_entry_bad_day_reduces():
    _reset()
    db_fn = make_db(
        rows_sim=[{'pnl': -100, 'pnl_pct': -0.5} for _ in range(10)],
        risk_row={'n_closed': 15, 'n_stops': 8, 'day_pnl': -15000,
                  'n_wins': 3, 'n_losses': 12},
        regime_row={'n': 10, 'wins': 2, 'avg_pnl_pct': -0.8},
    )
    d = evaluate_entry(db_fn, _L(),
                       symbol='PETR4', asset_type='stock',
                       score_v3=65, regime_v3='CHOPPY',
                       direction='LONG', hour_of_day=11, weekday=2)
    # Dia ruim + histórico ruim = deveria BLOCK ou REDUCE
    assert d['action'] in ('block', 'reduce'), f'bad day should veto/reduce: {d}'
    print(f"  ✓ bad day: action={d['action']} agg={d['aggregate_score']:.2f}")


def test_entry_derivative_bypass():
    _reset()
    db_fn = make_db()
    d = evaluate_entry(db_fn, _L(),
                       symbol='PETR4', asset_type='derivative',
                       strategy='pcp', score_v3=80)
    assert d['bypassed'] is True
    assert d['action'] == 'pass'
    assert d['approve'] is True
    print(f'  ✓ derivative bypass OK')


def test_exit_crypto_in_trailing_never_close():
    _reset()
    os.environ['TRAILING_PEAK_CRYPTO'] = '0.4'
    db_fn = make_db(
        rows_sim=[{'pnl': 100, 'pnl_pct': 0.6} for _ in range(10)],
        risk_row={'n_closed': 10, 'n_stops': 0, 'day_pnl': 500,
                  'n_wins': 7, 'n_losses': 3},
    )
    d = evaluate_exit(db_fn, _L(),
                      trade_id='t1', symbol='BTCUSDT', asset_type='crypto',
                      current_pnl_pct=0.35, peak_pnl_pct=0.8,
                      holding_minutes=120,
                      score_v3_entry=80, score_v3_current=30,
                      regime_v3_entry='TRENDING', regime_v3_current='CHOPPY')
    # Mesmo com tudo sinalizando saída, NUNCA CLOSE para crypto em trailing
    assert d['action'] != 'close', f'VIOLATION: {d}'
    print(f"  ✓ crypto trailing INTOCÁVEL: action={d['action']} agg={d['aggregate_score']:.2f}")


def test_exit_stock_deep_drawdown():
    _reset()
    db_fn = make_db(
        risk_row={'n_closed': 5, 'n_stops': 0, 'day_pnl': 100, 'n_wins': 3, 'n_losses': 2},
    )
    # Trade que atingiu 3% e agora em 0.5% (perdeu 83% do peak)
    d = evaluate_exit(db_fn, _L(),
                      trade_id='t2', symbol='PETR4', asset_type='stock',
                      current_pnl_pct=0.5, peak_pnl_pct=3.0,
                      holding_minutes=180,
                      score_v3_entry=75, score_v3_current=50,
                      regime_v3_entry='TRENDING', regime_v3_current='CHOPPY')
    # drawdown profundo + regime piorou → alta prob de close/reduce
    print(f"  ✓ deep drawdown stock: action={d['action']} agg={d['aggregate_score']:.2f}")


if __name__ == '__main__':
    print("=== E2E Tests ===")
    test_entry_normal_stock()
    test_entry_bad_day_reduces()
    test_entry_derivative_bypass()
    test_exit_crypto_in_trailing_never_close()
    test_exit_stock_deep_drawdown()
    print('\n✅ Todos E2E tests passaram')
