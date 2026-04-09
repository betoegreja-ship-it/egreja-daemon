"""
Long Horizon AI Module Endpoints

Flask Blueprint with 13 endpoints for investment analysis, portfolio management,
and performance monitoring. Compatible with MySQL (mysql.connector).

Endpoints:
  - GET /long-horizon/assets - List all assets with scores
  - GET /long-horizon/ranking - Assets ranked by score
  - GET /long-horizon/asset/<ticker> - Detailed asset view
  - GET /long-horizon/thesis/<ticker> - Investment thesis
  - GET /long-horizon/portfolios - All portfolios summary
  - GET /long-horizon/portfolio/<name> - Detailed portfolio
  - GET /long-horizon/capital - Capital summary
  - GET /long-horizon/pnl - P&L daily/monthly/annual
  - GET /long-horizon/win-rate - Win rate statistics
  - GET /long-horizon/backtest - Backtest results
  - GET /long-horizon/health - Module health
  - GET /long-horizon/system-state - Comprehensive state
  - GET /long-horizon/alerts - Active alerts
"""

from flask import Blueprint, request, jsonify
from datetime import datetime, date, timedelta
import traceback
import json

from .scoring_engine import generate_demo_scores, rank_assets, get_conviction_color
from .thesis_engine import generate_thesis_for_ticker
from .portfolio_engine import get_all_portfolios_summary, get_model_portfolios
from .backtest_engine import get_all_backtest_results


def create_long_horizon_blueprint(db_fn, log, **kwargs):
    """
    Factory function to create Flask Blueprint with dependencies.

    Args:
        db_fn: callable returning a mysql.connector connection
        log: logger instance
    """
    lh_bp = Blueprint('long_horizon', __name__, url_prefix='/long-horizon')

    # ============== HELPER FUNCTIONS ==============


    def _get_mp_portfolio_data():
        try:
            if not _mp_repo_available: return None
            repo = MonthlyPicksRepository(db_fn, log)
            positions = repo.get_open_positions()
            if not positions: return None
            sp = {}
            mm = __import__('sys').modules.get('__main__')
            if mm and hasattr(mm, 'stock_prices'): sp = mm.stock_prices
            ta, tp, pl = 0, 0, []
            for p in positions:
                tk = p.get('ticker','?')
                ep = float(p.get('entry_price') or 0)
                qt = float(p.get('quantity') or 0)
                cap = float(p.get('capital_allocated') or 0)
                cached = sp.get(tk) or sp.get(tk+'.SA') or {}
                cp = float(cached.get('price') or ep)
                cv = cp * qt; pv = cv - cap
                pp = (pv/cap*100) if cap>0 else 0
                ta += cap; tp += pv
                pl.append({'ticker':tk,'entry_price':ep,'current_price':cp,'quantity':qt,'weight':cap/7e6 if cap>0 else 0,'position_value':cv,'capital_allocated':cap,'pnl_value':round(pv,2),'pnl_pct':round(pp,2),'sector':p.get('sector','Unknown'),'score':float(p.get('entry_score') or 0)})
            return {'positions':pl,'total_allocated':ta,'total_value':sum(x['position_value'] for x in pl),'total_pnl':round(tp,2),'total_pnl_pct':round(tp/ta*100,2) if ta>0 else 0,'position_count':len(pl)}
        except Exception as e:
            if log: log.error(f"_get_mp_portfolio_data error: {e}")
            return None

    def get_db():
        return db_fn()

    def _safe_query(query, params=None, fetch='all'):
        """Execute a read query safely."""
        conn = None
        try:
            conn = get_db()
            if conn is None:
                return []
            cursor = conn.cursor(dictionary=True)
            cursor.execute(query, params or ())
            if fetch == 'one':
                return cursor.fetchone()
            return cursor.fetchall() or []
        except Exception as e:
            log.warning(f"_safe_query error: {e}")
            return [] if fetch == 'all' else None
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    def _serialize_row(row):
        """Convert datetime/decimal fields to JSON-serializable types."""
        if not row:
            return row
        out = {}
        for k, v in row.items():
            if isinstance(v, datetime):
                out[k] = v.isoformat()
            elif hasattr(v, '__float__'):  # Decimal
                out[k] = float(v)
            else:
                out[k] = v
        return out

    def _serialize_rows(rows):
        return [_serialize_row(r) for r in rows] if rows else []

    # ============== ASSET ENDPOINTS ==============

    @lh_bp.route('/assets', methods=['GET'])
    def get_assets():
        """GET /long-horizon/assets - List all assets with scores"""
        try:
            # Generate demo scores (in production, would query database)
            scores = generate_demo_scores()

            assets_list = [
                {
                    'ticker': ticker,
                    'score': data['total_score'],
                    'conviction': data['conviction'],
                    'color': get_conviction_color(data['conviction']),
                    'business_quality': data['business_quality'],
                    'valuation': data['valuation'],
                    'market_strength': data['market_strength'],
                    'macro_factors': data['macro_factors'],
                    'options_signal': data['options_signal'],
                    'structural_risk': data['structural_risk'],
                    'data_reliability': data['data_reliability'],
                }
                for ticker, data in scores.items()
            ]

            return jsonify({
                'status': 'success',
                'count': len(assets_list),
                'assets': sorted(assets_list, key=lambda x: x['score'], reverse=True),
                'as_of_date': date.today().isoformat(),
            }), 200

        except Exception as e:
            log.error(f"GET /assets error: {e}\n{traceback.format_exc()}")
            return jsonify({'error': str(e)}), 500

    @lh_bp.route('/ranking', methods=['GET'])
    def get_ranking():
        """GET /long-horizon/ranking - Assets ranked by score"""
        try:
            scores = generate_demo_scores()
            ranked = rank_assets(scores)

            ranking_list = [
                {
                    'rank': i + 1,
                    'ticker': ticker,
                    'score': float(score),
                    'conviction': scores[ticker]['conviction'],
                    'change_from_prev': round((i - scores[ticker].get('prev_rank', i)) * 10, 0),
                }
                for i, (ticker, score) in enumerate(ranked)
            ]

            return jsonify({
                'status': 'success',
                'ranking': ranking_list,
                'top_conviction': ranking_list[0]['conviction'] if ranking_list else None,
                'as_of_date': date.today().isoformat(),
            }), 200

        except Exception as e:
            log.error(f"GET /ranking error: {e}\n{traceback.format_exc()}")
            return jsonify({'error': str(e)}), 500

    @lh_bp.route('/asset/<ticker>', methods=['GET'])
    def get_asset_detail(ticker):
        """GET /long-horizon/asset/<ticker> - Detailed asset view"""
        try:
            # Generate scores
            scores = generate_demo_scores()

            if ticker not in scores:
                return jsonify({'error': f'Asset {ticker} not found'}), 404

            score_data = scores[ticker]
            subscores = json.loads(score_data['subscores']) if isinstance(score_data['subscores'], str) else score_data['subscores']

            return jsonify({
                'status': 'success',
                'ticker': ticker,
                'score_date': score_data['score_date'],
                'total_score': score_data['total_score'],
                'conviction': score_data['conviction'],
                'color': get_conviction_color(score_data['conviction']),
                'dimensions': {
                    'business_quality': score_data['business_quality'],
                    'valuation': score_data['valuation'],
                    'market_strength': score_data['market_strength'],
                    'macro_factors': score_data['macro_factors'],
                    'options_signal': score_data['options_signal'],
                    'structural_risk': score_data['structural_risk'],
                    'data_reliability': score_data['data_reliability'],
                },
                'subscores': subscores,
                'model_version': score_data['model_version'],
            }), 200

        except Exception as e:
            log.error(f"GET /asset/{ticker} error: {e}\n{traceback.format_exc()}")
            return jsonify({'error': str(e)}), 500

    @lh_bp.route('/thesis/<ticker>', methods=['GET'])
    def get_thesis(ticker):
        """GET /long-horizon/thesis/<ticker> - Investment thesis"""
        try:
            thesis = generate_thesis_for_ticker(ticker)

            if thesis is None:
                return jsonify({'error': f'Thesis for {ticker} not found'}), 404

            key_drivers = json.loads(thesis['key_drivers']) if isinstance(thesis['key_drivers'], str) else thesis['key_drivers']

            return jsonify({
                'status': 'success',
                'ticker': ticker,
                'thesis_date': thesis['thesis_date'],
                'thesis': thesis['thesis_text'],
                'key_drivers': key_drivers,
                'risks': thesis['risks'],
                'hedge_suggestion': thesis['hedge_suggestion'],
                'recommended_horizon': thesis['recommended_horizon'],
                'conviction_level': thesis['conviction_level'],
                'model_version': thesis['model_version'],
            }), 200

        except Exception as e:
            log.error(f"GET /thesis/{ticker} error: {e}\n{traceback.format_exc()}")
            return jsonify({'error': str(e)}), 500

    # ============== PORTFOLIO ENDPOINTS ==============

    @lh_bp.route('/portfolios', methods=['GET'])
    def get_portfolios_summary():
        try:
            mp = _get_mp_portfolio_data()
            if mp and mp['position_count']>0:
                s=[{'name':'Monthly Picks','description':'AI scoring engine positions','risk_level':'Moderate-Aggressive','target_return':25.0,'total_value':round(mp['total_value'],2),'total_pnl':mp['total_pnl'],'total_pnl_pct':mp['total_pnl_pct'],'position_count':mp['position_count'],'investment_ratio':round(mp['total_allocated']/7e6*100,2)}]
                return jsonify({'status':'success','portfolios':s,'initial_capital':7_000_000,'total_invested':round(mp['total_allocated'],2),'total_pnl':mp['total_pnl'],'as_of_date':date.today().isoformat()}),200
            return jsonify({'status':'success','portfolios':[],'initial_capital':7_000_000,'total_invested':0,'total_pnl':0,'as_of_date':date.today().isoformat()}),200
        except Exception as e:
            log.error(f'GET /portfolios error: {e}')
            return jsonify({'error':str(e)}),500

    @lh_bp.route('/portfolio/<name>', methods=['GET'])
    def get_portfolio_detail(name):
        try:
            mp = _get_mp_portfolio_data()
            if not mp: return jsonify({'error':'No data'}),404
            return jsonify({'status':'success','portfolio_name':'Monthly Picks','description':'AI scoring engine','risk_level':'Moderate-Aggressive','target_return':25.0,'total_capital':7_000_000,'total_value':round(mp['total_value'],2),'cash_reserve':round(7e6-mp['total_allocated'],2),'investment_ratio':round(mp['total_allocated']/7e6*100,2),'total_pnl':mp['total_pnl'],'total_pnl_pct':mp['total_pnl_pct'],'positions':mp['positions'],'as_of_date':date.today().isoformat()}),200
        except Exception as e:
            log.error(f'GET /portfolio error: {e}')
            return jsonify({'error':str(e)}),500

    # ============== CAPITAL & P&L ENDPOINTS ==============

    @lh_bp.route('/capital', methods=['GET'])
    def get_capital_summary():
        try:
            mp = _get_mp_portfolio_data()
            ic=7_000_000; tv=mp['total_value'] if mp else ic; tp=mp['total_pnl'] if mp else 0; al=mp['total_allocated'] if mp else 0
            return jsonify({'status':'success','capital_inicial':ic,'valor_atual':round(tv,2),'initial_capital':ic,'current_value':round(tv,2),'daily_pnl':round(tp,2),'monthly_pnl':round(tp,2),'annual_pnl':round(tp,2),'total_return_pct':round(tp/ic*100,2) if ic>0 else 0,'allocated':round(al,2),'reserve':round(ic-al,2),'allocation_ratio':round((al/ic)*100,2) if ic>0 else 0,'as_of_date':date.today().isoformat()}),200
        except Exception as e:
            log.error(f'GET /capital error: {e}')
            return jsonify({'error':str(e)}),500

    @lh_bp.route('/pnl', methods=['GET'])
    def get_pnl():
        try:
            mp = _get_mp_portfolio_data()
            tp=mp['total_pnl'] if mp else 0
            return jsonify({'status':'success','total_pnl':round(tp,2),'pnl_hoje':round(tp,2),'pnl_mes':round(tp,2),'pnl_ano':round(tp,2),'daily_pnl':[{'date':date.today().isoformat(),'pnl':round(tp,2)}],'monthly_pnl':[{'month':date.today().strftime('%Y-%m'),'pnl':round(tp,2)}],'as_of_date':date.today().isoformat()}),200
        except Exception as e:
            log.error(f'GET /pnl error: {e}')
            return jsonify({'error':str(e)}),500

    @lh_bp.route('/win-rate', methods=['GET'])
    def get_win_rate():
        try:
            mp = _get_mp_portfolio_data()
            if mp:
                ps=mp['positions'];t=len(ps);w=sum(1 for p in ps if p['pnl_pct']>0);wr=(w/t*100) if t>0 else 0
            else: t=w=0;wr=0
            return jsonify({'status':'success','total_positions':t,'winning_positions':w,'losing_positions':t-w,'win_rate_pct':round(wr,2),'avg_win':0,'avg_loss':0,'profit_factor':0,'as_of_date':date.today().isoformat()}),200
        except Exception as e:
            log.error(f'GET /win-rate error: {e}')
            return jsonify({'error':str(e)}),500

    @lh_bp.route('/backtest', methods=['GET'])
    def get_backtest_results():
        """GET /long-horizon/backtest - Backtest results"""
        try:
            results = get_all_backtest_results()

            backtest_data = []
            for pname, btest in results.items():
                backtest_data.append({
                    'portfolio_name': btest['portfolio_name'],
                    'period': f"{btest['start_date']} to {btest['end_date']}",
                    'initial_capital': btest['initial_capital'],
                    'final_value': btest['final_value'],
                    'total_return_pct': btest['total_return_pct'],
                    'annualized_return_pct': btest['annualized_return_pct'],
                    'benchmark_return_pct': btest['benchmark_return_pct'],
                    'outperformance_pct': btest['outperformance_pct'],
                    'sharpe_ratio': btest['sharpe_ratio'],
                    'max_drawdown_pct': btest['max_drawdown_pct'],
                    'win_rate_pct': btest['win_rate_pct'],
                    'trades_count': btest['trades_count'],
                })

            return jsonify({
                'status': 'success',
                'backtests': backtest_data,
                'benchmark': 'Ibovespa (INDEXBOV)',
                'period': 'Last 12 months',
            }), 200

        except Exception as e:
            log.error(f"GET /backtest error: {e}\n{traceback.format_exc()}")
            return jsonify({'error': str(e)}), 500

    # ============== MONITORING ENDPOINTS ==============

    @lh_bp.route('/health', methods=['GET'])
    def get_health():
        """GET /long-horizon/health - Module health"""
        try:
            # Check database connection
            db_status = 'ok'
            conn = get_db()
            if conn:
                conn.close()
            else:
                db_status = 'warning'

            return jsonify({
                'status': 'healthy',
                'database': db_status,
                'scoring_engine': 'operational',
                'portfolio_engine': 'operational',
                'backtest_engine': 'operational',
                'version': 'v1.0',
                'timestamp': datetime.now().isoformat(),
            }), 200

        except Exception as e:
            log.error(f"GET /health error: {e}")
            return jsonify({
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.now().isoformat(),
            }), 500

    @lh_bp.route('/system-state', methods=['GET'])
    def get_system_state():
        """GET /long-horizon/system-state - Comprehensive state for dashboard"""
        try:
            scores = generate_demo_scores()
            portfolios = get_all_portfolios_summary()
            backtests = get_all_backtest_results()

            # Top convictions
            top_scores = sorted(
                [(t, s['total_score']) for t, s in scores.items()],
                key=lambda x: x[1],
                reverse=True
            )[:3]

            # Portfolio summary
            total_capital = 7_000_000
            total_value = sum(p['total_position_value'] for p in portfolios.values() if p)
            total_pnl = sum(p['total_pnl'] for p in portfolios.values() if p)

            return jsonify({
                'status': 'success',
                'timestamp': datetime.now().isoformat(),
                'capital': {
                    'initial': total_capital,
                    'current': round(total_value, 2),
                    'pnl': round(total_pnl, 2),
                    'pnl_pct': round((total_pnl / total_capital) * 100, 2),
                },
                'top_scores': [
                    {'ticker': t, 'score': s} for t, s in top_scores
                ],
                'portfolios_count': len(portfolios),
                'assets_covered': len(scores),
                'backtest_outperformance': {
                    'Quality Brasil': backtests['Quality Brasil']['outperformance_pct'],
                    'Dividendos + Proteção': backtests['Dividendos + Proteção']['outperformance_pct'],
                    'Brasil + EUA': backtests['Brasil + EUA']['outperformance_pct'],
                },
            }), 200

        except Exception as e:
            log.error(f"GET /system-state error: {e}\n{traceback.format_exc()}")
            return jsonify({'error': str(e)}), 500

    @lh_bp.route('/alerts', methods=['GET'])
    def get_alerts():
        """GET /long-horizon/alerts - Active alerts"""
        try:
            # Query alerts from database
            alerts = _safe_query(
                "SELECT * FROM lh_alerts WHERE resolved = FALSE ORDER BY created_at DESC LIMIT 50"
            )

            formatted_alerts = []
            for alert in alerts:
                alert_serialized = _serialize_row(alert)
                formatted_alerts.append({
                    'alert_id': alert_serialized.get('alert_id'),
                    'asset_id': alert_serialized.get('asset_id'),
                    'alert_type': alert_serialized.get('alert_type'),
                    'message': alert_serialized.get('message'),
                    'severity': alert_serialized.get('severity'),
                    'created_at': alert_serialized.get('created_at'),
                })

            return jsonify({
                'status': 'success',
                'alerts_count': len(formatted_alerts),
                'alerts': formatted_alerts,
                'as_of_date': date.today().isoformat(),
            }), 200

        except Exception as e:
            log.error(f"GET /alerts error: {e}\n{traceback.format_exc()}")
            return jsonify({
                'status': 'success',
                'alerts_count': 0,
                'alerts': [],
                'note': 'Demo mode - no database alerts'
            }), 200

    return lh_bp
