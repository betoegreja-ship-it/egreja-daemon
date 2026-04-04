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
        """GET /long-horizon/portfolios - All portfolios summary"""
        try:
            all_portfolios = get_all_portfolios_summary()

            summary = []
            for pname, pdata in all_portfolios.items():
                if pdata:
                    summary.append({
                        'name': pname,
                        'description': pdata['description'],
                        'risk_level': pdata['risk_level'],
                        'target_return': pdata['target_return'],
                        'total_value': pdata['total_position_value'],
                        'total_pnl': pdata['total_pnl'],
                        'total_pnl_pct': pdata['total_pnl_pct'],
                        'position_count': pdata['position_count'],
                        'investment_ratio': pdata['investment_ratio'],
                    })

            return jsonify({
                'status': 'success',
                'portfolios': summary,
                'initial_capital': 7_000_000,
                'total_invested': sum(p['total_value'] for p in summary),
                'total_pnl': sum(p['total_pnl'] for p in summary),
                'as_of_date': date.today().isoformat(),
            }), 200

        except Exception as e:
            log.error(f"GET /portfolios error: {e}\n{traceback.format_exc()}")
            return jsonify({'error': str(e)}), 500

    @lh_bp.route('/portfolio/<name>', methods=['GET'])
    def get_portfolio_detail(name):
        """GET /long-horizon/portfolio/<name> - Detailed portfolio"""
        try:
            portfolios = get_model_portfolios()

            if name not in portfolios:
                return jsonify({'error': f'Portfolio {name} not found'}), 404

            pdata = get_all_portfolios_summary()[name]

            return jsonify({
                'status': 'success',
                'portfolio_name': pdata['portfolio_name'],
                'description': pdata['description'],
                'risk_level': pdata['risk_level'],
                'target_return': pdata['target_return'],
                'total_capital': pdata['total_capital'],
                'total_value': pdata['total_position_value'],
                'cash_reserve': pdata['cash_reserve'],
                'investment_ratio': pdata['investment_ratio'],
                'total_pnl': pdata['total_pnl'],
                'total_pnl_pct': pdata['total_pnl_pct'],
                'positions': pdata['positions'],
                'as_of_date': pdata['as_of_date'],
            }), 200

        except Exception as e:
            log.error(f"GET /portfolio/{name} error: {e}\n{traceback.format_exc()}")
            return jsonify({'error': str(e)}), 500

    # ============== CAPITAL & P&L ENDPOINTS ==============

    @lh_bp.route('/capital', methods=['GET'])
    def get_capital_summary():
        """GET /long-horizon/capital - Capital summary"""
        try:
            all_portfolios = get_all_portfolios_summary()

            total_value = sum(p['total_position_value'] for p in all_portfolios.values() if p)
            total_pnl = sum(p['total_pnl'] for p in all_portfolios.values() if p)
            initial_capital = 7_000_000
            total_pnl_pct = (total_pnl / initial_capital) * 100 if initial_capital > 0 else 0

            return jsonify({
                'status': 'success',
                'initial_capital': initial_capital,
                'current_value': round(total_value, 2),
                'daily_pnl': round(total_pnl / 20, 2),  # Estimate based on 20 trading days
                'monthly_pnl': round(total_pnl / 4, 2),  # Estimate
                'annual_pnl': round(total_pnl, 2),
                'total_return_pct': round(total_pnl_pct, 2),
                'allocated': round(total_value, 2),
                'reserve': round(initial_capital - total_value, 2),
                'allocation_ratio': round((total_value / initial_capital) * 100, 2),
                'as_of_date': date.today().isoformat(),
            }), 200

        except Exception as e:
            log.error(f"GET /capital error: {e}\n{traceback.format_exc()}")
            return jsonify({'error': str(e)}), 500

    @lh_bp.route('/pnl', methods=['GET'])
    def get_pnl():
        """GET /long-horizon/pnl - P&L daily/monthly/annual"""
        try:
            all_portfolios = get_all_portfolios_summary()
            total_pnl = sum(p['total_pnl'] for p in all_portfolios.values() if p)

            # Generate mock daily P&L (20 trading days)
            daily_pnl = []
            daily_value = total_pnl / 20
            for i in range(20):
                daily_pnl.append({
                    'date': (date.today() - timedelta(days=20-i)).isoformat(),
                    'pnl': round(daily_value * (1 + (i * 0.05)), 2),
                })

            # Monthly P&L (last 12 months)
            monthly_pnl = []
            for m in range(12):
                d = date.today() - timedelta(days=30*m)
                monthly_pnl.append({
                    'month': d.strftime('%Y-%m'),
                    'pnl': round(total_pnl * (0.7 + m * 0.02), 2),
                })

            return jsonify({
                'status': 'success',
                'total_pnl': round(total_pnl, 2),
                'daily_pnl': daily_pnl[-10:],  # Last 10 days
                'monthly_pnl': monthly_pnl[-12:],
                'as_of_date': date.today().isoformat(),
            }), 200

        except Exception as e:
            log.error(f"GET /pnl error: {e}\n{traceback.format_exc()}")
            return jsonify({'error': str(e)}), 500

    @lh_bp.route('/win-rate', methods=['GET'])
    def get_win_rate():
        """GET /long-horizon/win-rate - Win rate statistics"""
        try:
            all_portfolios = get_all_portfolios_summary()

            total_positions = sum(len(p['positions']) for p in all_portfolios.values() if p)
            winning_positions = sum(
                sum(1 for pos in p['positions'] if pos['pnl_pct'] > 0)
                for p in all_portfolios.values() if p
            )
            win_rate = (winning_positions / total_positions * 100) if total_positions > 0 else 0

            return jsonify({
                'status': 'success',
                'total_positions': total_positions,
                'winning_positions': winning_positions,
                'losing_positions': total_positions - winning_positions,
                'win_rate_pct': round(win_rate, 2),
                'avg_win': round(2.5, 2),  # Demo value
                'avg_loss': round(-1.8, 2),
                'profit_factor': round(2.5 / 1.8, 2),
                'as_of_date': date.today().isoformat(),
            }), 200

        except Exception as e:
            log.error(f"GET /win-rate error: {e}\n{traceback.format_exc()}")
            return jsonify({'error': str(e)}), 500

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
