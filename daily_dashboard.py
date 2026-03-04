#!/usr/bin/env python3
"""
Daily Dashboard - Dashboard de Resultados Diários e P&L
Visualização de performance, trades executados e métricas em tempo real
"""

import os
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List
import pandas as pd
import numpy as np

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DailyDashboard:
    """Dashboard de resultados diários"""
    
    def __init__(self):
        """Inicializar dashboard"""
        self.trades_dir = "logs/trades"
        self.alerts_dir = "logs/alerts"
        self.dashboard_dir = "data/dashboard"
        
        os.makedirs(self.dashboard_dir, exist_ok=True)
        
        logger.info("✅ Daily Dashboard Inicializado")
    
    def load_daily_trades(self, date: str = None) -> List[Dict]:
        """Carregar trades do dia"""
        if date is None:
            date = datetime.now().strftime('%Y%m%d')
        
        trades = []
        
        try:
            # Procurar arquivos de ciclo do dia
            for filename in os.listdir(self.trades_dir):
                if filename.startswith(f"cycle_{date}"):
                    filepath = os.path.join(self.trades_dir, filename)
                    with open(filepath, 'r') as f:
                        cycle_data = json.load(f)
                        trades.append(cycle_data)
        
        except Exception as e:
            logger.error(f"❌ Erro ao carregar trades: {e}")
        
        return trades
    
    def load_daily_alerts(self, date: str = None) -> List[Dict]:
        """Carregar alertas do dia"""
        if date is None:
            date = datetime.now().strftime('%Y%m%d')
        
        alerts = []
        
        try:
            alert_file = os.path.join(self.alerts_dir, f"alerts_{date}.json")
            if os.path.exists(alert_file):
                with open(alert_file, 'r') as f:
                    alerts = json.load(f)
        
        except Exception as e:
            logger.error(f"❌ Erro ao carregar alertas: {e}")
        
        return alerts
    
    def calculate_daily_metrics(self, trades: List[Dict]) -> Dict:
        """Calcular métricas diárias"""
        if not trades:
            return self._empty_metrics()
        
        # Extrair dados de todos os ciclos
        all_closed_trades = []
        total_opportunities = 0
        total_executed = 0
        
        for cycle in trades:
            total_opportunities += cycle.get('opportunities_found', 0)
            total_executed += cycle.get('trades_executed', 0)
        
        # Calcular P&L
        daily_summary = trades[-1].get('daily_summary', {}) if trades else {}
        
        metrics = {
            'date': datetime.now().strftime('%Y-%m-%d'),
            'total_opportunities': total_opportunities,
            'total_trades_executed': total_executed,
            'total_trades_closed': sum(t.get('trades_closed', 0) for t in trades),
            'total_pnl': daily_summary.get('total_pnl', 0),
            'total_pnl_pct': daily_summary.get('total_pnl_pct', 0),
            'win_trades': daily_summary.get('win_trades', 0),
            'loss_trades': daily_summary.get('loss_trades', 0),
            'win_rate': daily_summary.get('win_rate', 0),
            'open_positions': daily_summary.get('open_positions', 0),
            'daily_loss': daily_summary.get('daily_loss', 0),
            'remaining_capital': daily_summary.get('remaining_capital', 0),
            'cycles_completed': len(trades)
        }
        
        return metrics
    
    def _empty_metrics(self) -> Dict:
        """Retornar métricas vazias"""
        return {
            'date': datetime.now().strftime('%Y-%m-%d'),
            'total_opportunities': 0,
            'total_trades_executed': 0,
            'total_trades_closed': 0,
            'total_pnl': 0,
            'total_pnl_pct': 0,
            'win_trades': 0,
            'loss_trades': 0,
            'win_rate': 0,
            'open_positions': 0,
            'daily_loss': 0,
            'remaining_capital': 1000000,
            'cycles_completed': 0
        }
    
    def generate_html_dashboard(self, metrics: Dict, alerts: List[Dict]) -> str:
        """Gerar HTML do dashboard"""
        
        # Cores baseadas em performance
        pnl_color = '#4caf50' if metrics['total_pnl'] >= 0 else '#f44336'
        win_rate_color = '#4caf50' if metrics['win_rate'] >= 50 else '#ff9800'
        
        html = f"""
        <!DOCTYPE html>
        <html lang="pt-BR">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>ArbitrageAI - Dashboard Diário</title>
            <style>
                * {{
                    margin: 0;
                    padding: 0;
                    box-sizing: border-box;
                }}
                
                body {{
                    font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    color: #333;
                    padding: 20px;
                    min-height: 100vh;
                }}
                
                .container {{
                    max-width: 1400px;
                    margin: 0 auto;
                }}
                
                .header {{
                    background: white;
                    padding: 30px;
                    border-radius: 10px;
                    margin-bottom: 30px;
                    box-shadow: 0 10px 30px rgba(0,0,0,0.2);
                }}
                
                .header h1 {{
                    color: #667eea;
                    margin-bottom: 10px;
                    font-size: 2.5em;
                }}
                
                .header p {{
                    color: #666;
                    font-size: 1.1em;
                }}
                
                .metrics-grid {{
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
                    gap: 20px;
                    margin-bottom: 30px;
                }}
                
                .metric-card {{
                    background: white;
                    padding: 25px;
                    border-radius: 10px;
                    box-shadow: 0 10px 30px rgba(0,0,0,0.2);
                    border-left: 5px solid #667eea;
                }}
                
                .metric-card.positive {{
                    border-left-color: #4caf50;
                }}
                
                .metric-card.negative {{
                    border-left-color: #f44336;
                }}
                
                .metric-label {{
                    color: #999;
                    font-size: 0.9em;
                    text-transform: uppercase;
                    letter-spacing: 1px;
                    margin-bottom: 10px;
                }}
                
                .metric-value {{
                    font-size: 2em;
                    font-weight: bold;
                    color: #333;
                }}
                
                .metric-value.positive {{
                    color: #4caf50;
                }}
                
                .metric-value.negative {{
                    color: #f44336;
                }}
                
                .metric-subtext {{
                    color: #999;
                    font-size: 0.85em;
                    margin-top: 5px;
                }}
                
                .section {{
                    background: white;
                    padding: 30px;
                    border-radius: 10px;
                    margin-bottom: 30px;
                    box-shadow: 0 10px 30px rgba(0,0,0,0.2);
                }}
                
                .section h2 {{
                    color: #667eea;
                    margin-bottom: 20px;
                    font-size: 1.5em;
                    border-bottom: 2px solid #667eea;
                    padding-bottom: 10px;
                }}
                
                .stats-table {{
                    width: 100%;
                    border-collapse: collapse;
                }}
                
                .stats-table th {{
                    background: #f5f5f5;
                    padding: 12px;
                    text-align: left;
                    font-weight: 600;
                    color: #667eea;
                }}
                
                .stats-table td {{
                    padding: 12px;
                    border-bottom: 1px solid #eee;
                }}
                
                .stats-table tr:hover {{
                    background: #f9f9f9;
                }}
                
                .alert-item {{
                    padding: 15px;
                    margin-bottom: 10px;
                    border-radius: 5px;
                    border-left: 4px solid #667eea;
                }}
                
                .alert-item.opportunity {{
                    background: #e8f5e9;
                    border-left-color: #4caf50;
                }}
                
                .alert-item.trade {{
                    background: #e3f2fd;
                    border-left-color: #2196f3;
                }}
                
                .alert-item.error {{
                    background: #ffebee;
                    border-left-color: #f44336;
                }}
                
                .alert-time {{
                    color: #999;
                    font-size: 0.85em;
                }}
                
                .footer {{
                    background: white;
                    padding: 20px;
                    border-radius: 10px;
                    text-align: center;
                    color: #999;
                    font-size: 0.9em;
                }}
                
                .status-badge {{
                    display: inline-block;
                    padding: 5px 10px;
                    border-radius: 20px;
                    font-size: 0.85em;
                    font-weight: 600;
                }}
                
                .status-active {{
                    background: #4caf50;
                    color: white;
                }}
                
                .status-idle {{
                    background: #ff9800;
                    color: white;
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>🚀 ArbitrageAI - Dashboard Diário</h1>
                    <p>Resultados de Trading em Tempo Real | {metrics['date']}</p>
                </div>
                
                <div class="metrics-grid">
                    <div class="metric-card {'positive' if metrics['total_pnl'] >= 0 else 'negative'}">
                        <div class="metric-label">P&L Diário</div>
                        <div class="metric-value {'positive' if metrics['total_pnl'] >= 0 else 'negative'}">
                            ${metrics['total_pnl']:,.2f}
                        </div>
                        <div class="metric-subtext">
                            {metrics['total_pnl_pct']:.2f}% do capital
                        </div>
                    </div>
                    
                    <div class="metric-card">
                        <div class="metric-label">Taxa de Acerto</div>
                        <div class="metric-value {'positive' if metrics['win_rate'] >= 50 else 'negative'}">
                            {metrics['win_rate']:.1f}%
                        </div>
                        <div class="metric-subtext">
                            {metrics['win_trades']}W / {metrics['loss_trades']}L
                        </div>
                    </div>
                    
                    <div class="metric-card">
                        <div class="metric-label">Oportunidades</div>
                        <div class="metric-value">{metrics['total_opportunities']}</div>
                        <div class="metric-subtext">
                            {metrics['total_trades_executed']} executadas
                        </div>
                    </div>
                    
                    <div class="metric-card">
                        <div class="metric-label">Capital Restante</div>
                        <div class="metric-value">
                            ${metrics['remaining_capital']:,.0f}
                        </div>
                        <div class="metric-subtext">
                            {metrics['open_positions']} posições abertas
                        </div>
                    </div>
                </div>
                
                <div class="section">
                    <h2>📊 Resumo de Performance</h2>
                    <table class="stats-table">
                        <tr>
                            <th>Métrica</th>
                            <th>Valor</th>
                        </tr>
                        <tr>
                            <td>Ciclos Completados</td>
                            <td><strong>{metrics['cycles_completed']}</strong></td>
                        </tr>
                        <tr>
                            <td>Total de Oportunidades</td>
                            <td><strong>{metrics['total_opportunities']}</strong></td>
                        </tr>
                        <tr>
                            <td>Trades Executados</td>
                            <td><strong>{metrics['total_trades_executed']}</strong></td>
                        </tr>
                        <tr>
                            <td>Trades Fechados</td>
                            <td><strong>{metrics['total_trades_closed']}</strong></td>
                        </tr>
                        <tr>
                            <td>Trades Vencedores</td>
                            <td><strong style="color: #4caf50;">{metrics['win_trades']}</strong></td>
                        </tr>
                        <tr>
                            <td>Trades Perdedores</td>
                            <td><strong style="color: #f44336;">{metrics['loss_trades']}</strong></td>
                        </tr>
                        <tr>
                            <td>Perda Diária</td>
                            <td><strong style="color: #f44336;">${metrics['daily_loss']:,.2f}</strong></td>
                        </tr>
                    </table>
                </div>
                
                <div class="section">
                    <h2>🔔 Alertas Recentes</h2>
                    <div>
        """
        
        # Adicionar alertas
        if alerts:
            for alert in alerts[-10:]:  # Últimos 10 alertas
                alert_type = alert.get('type', 'UNKNOWN')
                timestamp = alert.get('timestamp', '')
                
                if alert_type == 'OPPORTUNITY':
                    html += f"""
                    <div class="alert-item opportunity">
                        <strong>🎯 Oportunidade: {alert.get('symbol')}</strong>
                        <div class="alert-time">{timestamp}</div>
                        <p>Sinal: {alert.get('signal')} | Confiança: {alert.get('confidence', 0):.1%}</p>
                    </div>
                    """
                
                elif alert_type == 'TRADE_EXECUTED':
                    html += f"""
                    <div class="alert-item trade">
                        <strong>✅ Trade Executado: {alert.get('symbol')}</strong>
                        <div class="alert-time">{timestamp}</div>
                        <p>{alert.get('side')} | Preço: ${alert.get('entry_price', 0):,.2f} | Qtd: {alert.get('quantity', 0):.4f}</p>
                    </div>
                    """
                
                elif alert_type == 'TRADE_CLOSED':
                    pnl = alert.get('pnl', 0)
                    pnl_pct = alert.get('pnl_pct', 0)
                    html += f"""
                    <div class="alert-item trade">
                        <strong>✅ Trade Fechado: {alert.get('symbol')}</strong>
                        <div class="alert-time">{timestamp}</div>
                        <p>P&L: ${pnl:,.2f} ({pnl_pct:.2f}%) | Razão: {alert.get('reason')}</p>
                    </div>
                    """
        else:
            html += '<p style="color: #999;">Nenhum alerta ainda</p>'
        
        html += """
                    </div>
                </div>
                
                <div class="footer">
                    <p>ArbitrageAI v2 | Sistema de Trading Automático Profissional</p>
                    <p>Última atualização: """ + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + """</p>
                </div>
            </div>
        </body>
        </html>
        """
        
        return html
    
    def save_dashboard_html(self, html: str, date: str = None) -> str:
        """Salvar dashboard em HTML"""
        if date is None:
            date = datetime.now().strftime('%Y%m%d')
        
        filepath = os.path.join(self.dashboard_dir, f"dashboard_{date}.html")
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(html)
        
        logger.info(f"✅ Dashboard salvo: {filepath}")
        return filepath
    
    def save_metrics_json(self, metrics: Dict, date: str = None) -> str:
        """Salvar métricas em JSON"""
        if date is None:
            date = datetime.now().strftime('%Y%m%d')
        
        filepath = os.path.join(self.dashboard_dir, f"metrics_{date}.json")
        
        with open(filepath, 'w') as f:
            json.dump(metrics, f, indent=2, default=str)
        
        logger.info(f"✅ Métricas salvas: {filepath}")
        return filepath
    
    def generate_daily_report(self, date: str = None) -> Dict:
        """Gerar relatório diário completo"""
        if date is None:
            date = datetime.now().strftime('%Y%m%d')
        
        logger.info("\n" + "="*70)
        logger.info("📊 GERANDO RELATÓRIO DIÁRIO")
        logger.info("="*70)
        
        # Carregar dados
        trades = self.load_daily_trades(date)
        alerts = self.load_daily_alerts(date)
        
        # Calcular métricas
        metrics = self.calculate_daily_metrics(trades)
        
        # Gerar HTML
        html = self.generate_html_dashboard(metrics, alerts)
        
        # Salvar arquivos
        html_file = self.save_dashboard_html(html, date)
        json_file = self.save_metrics_json(metrics, date)
        
        # Log do relatório
        logger.info(f"\n📊 RESUMO DO DIA {date}")
        logger.info(f"P&L: ${metrics['total_pnl']:,.2f} ({metrics['total_pnl_pct']:.2f}%)")
        logger.info(f"Taxa de Acerto: {metrics['win_rate']:.1f}%")
        logger.info(f"Oportunidades: {metrics['total_opportunities']}")
        logger.info(f"Trades Executados: {metrics['total_trades_executed']}")
        logger.info(f"Capital Restante: ${metrics['remaining_capital']:,.0f}")
        logger.info("="*70 + "\n")
        
        return {
            'metrics': metrics,
            'html_file': html_file,
            'json_file': json_file,
            'alerts_count': len(alerts)
        }


def main():
    """Testar dashboard"""
    
    dashboard = DailyDashboard()
    
    # Gerar relatório diário
    report = dashboard.generate_daily_report()
    
    print("\n✅ DASHBOARD GERADO COM SUCESSO!")
    print(f"HTML: {report['html_file']}")
    print(f"JSON: {report['json_file']}")


if __name__ == "__main__":
    main()
