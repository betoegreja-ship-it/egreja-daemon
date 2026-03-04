#!/usr/bin/env python3
"""
Sofia IA API REST - Endpoints para integração com dashboard
"""

from flask import Flask, jsonify, request
from flask_cors import CORS
import json
import os
from datetime import datetime
import logging

from sofia_regenerative_ai import SofiaRegenerativeAI
from sofia_trade_executor import SofiaTradeExecutor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

# Inicializa Sofia
sofia = SofiaRegenerativeAI()
executor = SofiaTradeExecutor()

@app.route('/api/sofia/health', methods=['GET'])
def health():
    """Health check"""
    return jsonify({
        'status': 'ok',
        'timestamp': datetime.now().isoformat(),
        'sofia_version': '1.0.0'
    })

@app.route('/api/sofia/analyze', methods=['POST'])
def analyze():
    """Analisa mercado e retorna recomendações"""
    try:
        data = request.json
        market_data = data.get('market_data', {})
        
        if not market_data:
            # Se não receber dados, busca da Binance
            market_data = executor.fetch_market_data()
        
        # Sofia analisa
        analysis = sofia.analyze_market(market_data)
        
        return jsonify({
            'status': 'success',
            'analysis': analysis,
            'timestamp': datetime.now().isoformat()
        })
    
    except Exception as e:
        logger.error(f"Erro na análise: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/sofia/metrics', methods=['GET'])
def metrics():
    """Retorna métricas de desempenho de Sofia"""
    try:
        metrics = {}
        
        for symbol, data in sofia.accuracy_metrics.items():
            metrics[symbol] = {
                'total_trades': data['total'],
                'correct_predictions': data['correct'],
                'accuracy': round(data['accuracy'], 2),
                'win_rate': round(data['win_rate'], 2),
                'avg_profit': round(data['avg_profit'], 2)
            }
        
        return jsonify({
            'status': 'success',
            'metrics': metrics,
            'overall_accuracy': round(sofia._get_overall_accuracy(), 2),
            'timestamp': datetime.now().isoformat()
        })
    
    except Exception as e:
        logger.error(f"Erro ao obter métricas: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/sofia/learning-history', methods=['GET'])
def learning_history():
    """Retorna histórico de aprendizado"""
    try:
        symbol = request.args.get('symbol', None)
        
        if symbol:
            history = sofia.learning_history.get(symbol, {})
            return jsonify({
                'status': 'success',
                'symbol': symbol,
                'history': history,
                'timestamp': datetime.now().isoformat()
            })
        else:
            return jsonify({
                'status': 'success',
                'history': sofia.learning_history,
                'timestamp': datetime.now().isoformat()
            })
    
    except Exception as e:
        logger.error(f"Erro ao obter histórico: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/sofia/trades', methods=['GET'])
def trades():
    """Retorna histórico de trades"""
    try:
        limit = request.args.get('limit', 50, type=int)
        symbol = request.args.get('symbol', None)
        
        trades_list = sofia.trades_executed
        
        if symbol:
            trades_list = [t for t in trades_list if t['symbol'] == symbol]
        
        # Retorna últimos N trades
        trades_list = trades_list[-limit:]
        
        return jsonify({
            'status': 'success',
            'trades': trades_list,
            'total': len(trades_list),
            'timestamp': datetime.now().isoformat()
        })
    
    except Exception as e:
        logger.error(f"Erro ao obter trades: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/sofia/daily-summary', methods=['GET'])
def daily_summary():
    """Retorna resumo do dia"""
    try:
        summary = sofia.get_daily_summary()
        
        return jsonify({
            'status': 'success',
            'summary': summary,
            'timestamp': datetime.now().isoformat()
        })
    
    except Exception as e:
        logger.error(f"Erro ao obter resumo: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/sofia/execute-cycle', methods=['POST'])
def execute_cycle():
    """Executa ciclo de trading"""
    try:
        executor.execute_daily_trading_cycle()
        summary = sofia.get_daily_summary()
        
        return jsonify({
            'status': 'success',
            'message': 'Ciclo de trading executado',
            'summary': summary,
            'timestamp': datetime.now().isoformat()
        })
    
    except Exception as e:
        logger.error(f"Erro ao executar ciclo: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/sofia/recommendations', methods=['GET'])
def recommendations():
    """Retorna recomendações atuais"""
    try:
        # Busca dados de mercado
        market_data = executor.fetch_market_data()
        
        # Sofia analisa
        analysis = sofia.analyze_market(market_data)
        
        # Ordena por confiança
        sorted_recs = sorted(
            analysis.items(),
            key=lambda x: x[1]['confidence'],
            reverse=True
        )
        
        recommendations_list = []
        for symbol, rec in sorted_recs:
            recommendations_list.append({
                'symbol': symbol,
                'recommendation': rec['recommendation'],
                'confidence': round(rec['confidence'], 1),
                'reasoning': rec['reasoning'],
                'profit_target': round(rec['profit_target'], 2),
                'stop_loss': round(rec['stop_loss'], 2),
                'current_price': round(rec['current_price'], 2),
                'accuracy': round(rec['accuracy'], 1),
                'win_rate': round(rec['win_rate'], 1)
            })
        
        return jsonify({
            'status': 'success',
            'recommendations': recommendations_list,
            'timestamp': datetime.now().isoformat()
        })
    
    except Exception as e:
        logger.error(f"Erro ao obter recomendações: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/sofia/stats', methods=['GET'])
def stats():
    """Retorna estatísticas gerais"""
    try:
        today_trades = sofia.get_daily_summary()
        
        stats_data = {
            'total_trades_all_time': len(sofia.trades_executed),
            'total_trades_today': today_trades['total_trades'],
            'total_pnl_today': round(today_trades['total_pnl'], 2),
            'win_rate_today': round(today_trades['win_rate'], 1),
            'overall_accuracy': round(sofia._get_overall_accuracy(), 1),
            'symbols_monitored': len(sofia.accuracy_metrics),
            'best_symbol': max(
                sofia.accuracy_metrics.items(),
                key=lambda x: x[1]['accuracy']
            )[0] if sofia.accuracy_metrics else None,
            'best_accuracy': round(
                max([m['accuracy'] for m in sofia.accuracy_metrics.values()], default=0), 1
            )
        }
        
        return jsonify({
            'status': 'success',
            'stats': stats_data,
            'timestamp': datetime.now().isoformat()
        })
    
    except Exception as e:
        logger.error(f"Erro ao obter estatísticas: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

if __name__ == '__main__':
    logger.info("🚀 Sofia IA API iniciada na porta 5000")
    app.run(debug=False, host='0.0.0.0', port=5000)
