#!/usr/bin/env python3
"""
Report Generator - Gerador de Relatórios Profissionais de Trading
Análise detalhada de P&L, métricas e histórico de trades
"""

import os
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List
import numpy as np
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TradingReportGenerator:
    """Gerador de relatórios profissionais de trading"""
    
    def __init__(self, capital_inicial: float = 1000000):
        """Inicializar gerador de relatórios"""
        self.capital_inicial = capital_inicial
        self.capital_atual = capital_inicial
        self.trades = []
        self.ciclos = []
        
        self.reports_dir = "data/reports"
        os.makedirs(self.reports_dir, exist_ok=True)
        
        logger.info("✅ Report Generator Inicializado")
    
    def add_trade(self, symbol: str, side: str, entry_price: float, 
                  exit_price: float, quantity: float, reason: str = "MANUAL") -> Dict:
        """Adicionar trade ao histórico"""
        
        entry_amount = entry_price * quantity
        exit_amount = exit_price * quantity
        pnl = exit_amount - entry_amount
        pnl_pct = (pnl / entry_amount) * 100
        
        trade = {
            'id': len(self.trades) + 1,
            'symbol': symbol,
            'side': side,
            'entry_price': entry_price,
            'exit_price': exit_price,
            'quantity': quantity,
            'entry_amount': entry_amount,
            'exit_amount': exit_amount,
            'pnl': pnl,
            'pnl_pct': pnl_pct,
            'reason': reason,
            'timestamp': datetime.now().isoformat()
        }
        
        self.trades.append(trade)
        self.capital_atual += pnl
        
        return trade
    
    def add_ciclo(self, ciclo_data: Dict) -> None:
        """Adicionar ciclo ao histórico"""
        self.ciclos.append(ciclo_data)
    
    def calculate_metrics(self) -> Dict:
        """Calcular métricas profissionais"""
        
        if not self.trades:
            return self._empty_metrics()
        
        df = pd.DataFrame(self.trades)
        
        # Métricas básicas
        total_pnl = df['pnl'].sum()
        total_pnl_pct = (total_pnl / self.capital_inicial) * 100
        
        win_trades = len(df[df['pnl'] > 0])
        loss_trades = len(df[df['pnl'] < 0])
        break_even = len(df[df['pnl'] == 0])
        
        win_rate = (win_trades / len(df) * 100) if len(df) > 0 else 0
        
        # Ganhos e perdas
        winning_trades = df[df['pnl'] > 0]['pnl']
        losing_trades = df[df['pnl'] < 0]['pnl']
        
        avg_win = winning_trades.mean() if len(winning_trades) > 0 else 0
        avg_loss = losing_trades.mean() if len(losing_trades) > 0 else 0
        
        max_win = winning_trades.max() if len(winning_trades) > 0 else 0
        max_loss = losing_trades.min() if len(losing_trades) > 0 else 0
        
        # Profit Factor
        gross_profit = winning_trades.sum() if len(winning_trades) > 0 else 0
        gross_loss = abs(losing_trades.sum()) if len(losing_trades) > 0 else 0
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else 0
        
        # Expectativa matemática
        expectancy = (win_rate / 100 * avg_win) + ((1 - win_rate / 100) * avg_loss)
        
        # Drawdown
        cumulative_pnl = df['pnl'].cumsum()
        running_max = cumulative_pnl.expanding().max()
        drawdown = cumulative_pnl - running_max
        max_drawdown = drawdown.min()
        max_drawdown_pct = (max_drawdown / self.capital_inicial) * 100
        
        # Sharpe Ratio
        returns = df['pnl_pct'].values
        sharpe_ratio = (returns.mean() / returns.std()) * np.sqrt(252) if returns.std() > 0 else 0
        
        # Sortino Ratio
        downside_returns = returns[returns < 0]
        sortino_ratio = (returns.mean() / downside_returns.std()) * np.sqrt(252) if len(downside_returns) > 0 and downside_returns.std() > 0 else 0
        
        # Calmar Ratio
        annual_return = total_pnl_pct * (252 / len(df)) if len(df) > 0 else 0
        calmar_ratio = annual_return / abs(max_drawdown_pct) if max_drawdown_pct != 0 else 0
        
        metrics = {
            'capital_inicial': self.capital_inicial,
            'capital_final': self.capital_atual,
            'total_pnl': total_pnl,
            'total_pnl_pct': total_pnl_pct,
            'total_trades': len(df),
            'win_trades': int(win_trades),
            'loss_trades': int(loss_trades),
            'break_even': int(break_even),
            'win_rate': win_rate,
            'avg_win': avg_win,
            'avg_loss': avg_loss,
            'max_win': max_win,
            'max_loss': max_loss,
            'profit_factor': profit_factor,
            'expectancy': expectancy,
            'max_drawdown': max_drawdown,
            'max_drawdown_pct': max_drawdown_pct,
            'sharpe_ratio': sharpe_ratio,
            'sortino_ratio': sortino_ratio,
            'calmar_ratio': calmar_ratio,
            'avg_trade_size': df['entry_amount'].mean(),
            'largest_trade': df['entry_amount'].max(),
            'smallest_trade': df['entry_amount'].min()
        }
        
        return metrics
    
    def _empty_metrics(self) -> Dict:
        """Retornar métricas vazias"""
        return {
            'capital_inicial': self.capital_inicial,
            'capital_final': self.capital_inicial,
            'total_pnl': 0,
            'total_pnl_pct': 0,
            'total_trades': 0,
            'win_trades': 0,
            'loss_trades': 0,
            'break_even': 0,
            'win_rate': 0,
            'avg_win': 0,
            'avg_loss': 0,
            'max_win': 0,
            'max_loss': 0,
            'profit_factor': 0,
            'expectancy': 0,
            'max_drawdown': 0,
            'max_drawdown_pct': 0,
            'sharpe_ratio': 0,
            'sortino_ratio': 0,
            'calmar_ratio': 0,
            'avg_trade_size': 0,
            'largest_trade': 0,
            'smallest_trade': 0
        }
    
    def generate_html_report(self, metrics: Dict) -> str:
        """Gerar relatório em HTML"""
        
        pnl_color = '#4caf50' if metrics['total_pnl'] >= 0 else '#f44336'
        win_rate_color = '#4caf50' if metrics['win_rate'] >= 50 else '#ff9800'
        
        html = f"""
        <!DOCTYPE html>
        <html lang="pt-BR">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Relatório de Trading - ArbitrageAI</title>
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
                    padding: 40px 20px;
                    min-height: 100vh;
                }}
                
                .container {{
                    max-width: 1600px;
                    margin: 0 auto;
                }}
                
                .header {{
                    background: white;
                    padding: 40px;
                    border-radius: 10px;
                    margin-bottom: 30px;
                    box-shadow: 0 10px 30px rgba(0,0,0,0.2);
                    border-left: 5px solid #667eea;
                }}
                
                .header h1 {{
                    color: #667eea;
                    margin-bottom: 10px;
                    font-size: 2.8em;
                }}
                
                .header p {{
                    color: #666;
                    font-size: 1.1em;
                    margin: 5px 0;
                }}
                
                .metrics-grid {{
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
                    gap: 20px;
                    margin-bottom: 30px;
                }}
                
                .metric-card {{
                    background: white;
                    padding: 30px;
                    border-radius: 10px;
                    box-shadow: 0 10px 30px rgba(0,0,0,0.2);
                    border-left: 5px solid #667eea;
                    transition: transform 0.2s;
                }}
                
                .metric-card:hover {{
                    transform: translateY(-5px);
                }}
                
                .metric-card.positive {{
                    border-left-color: #4caf50;
                }}
                
                .metric-card.negative {{
                    border-left-color: #f44336;
                }}
                
                .metric-label {{
                    color: #999;
                    font-size: 0.85em;
                    text-transform: uppercase;
                    letter-spacing: 1px;
                    margin-bottom: 12px;
                    font-weight: 600;
                }}
                
                .metric-value {{
                    font-size: 2.2em;
                    font-weight: bold;
                    color: #333;
                    margin-bottom: 5px;
                }}
                
                .metric-value.positive {{
                    color: #4caf50;
                }}
                
                .metric-value.negative {{
                    color: #f44336;
                }}
                
                .metric-subtext {{
                    color: #999;
                    font-size: 0.9em;
                }}
                
                .section {{
                    background: white;
                    padding: 40px;
                    border-radius: 10px;
                    margin-bottom: 30px;
                    box-shadow: 0 10px 30px rgba(0,0,0,0.2);
                }}
                
                .section h2 {{
                    color: #667eea;
                    margin-bottom: 25px;
                    font-size: 1.8em;
                    border-bottom: 3px solid #667eea;
                    padding-bottom: 15px;
                }}
                
                .metrics-table {{
                    width: 100%;
                    border-collapse: collapse;
                    margin-bottom: 30px;
                }}
                
                .metrics-table th {{
                    background: #f5f5f5;
                    padding: 15px;
                    text-align: left;
                    font-weight: 600;
                    color: #667eea;
                    border-bottom: 2px solid #667eea;
                }}
                
                .metrics-table td {{
                    padding: 15px;
                    border-bottom: 1px solid #eee;
                }}
                
                .metrics-table tr:hover {{
                    background: #f9f9f9;
                }}
                
                .metrics-table .label {{
                    font-weight: 600;
                    color: #333;
                }}
                
                .metrics-table .value {{
                    text-align: right;
                    font-weight: 500;
                }}
                
                .trades-table {{
                    width: 100%;
                    border-collapse: collapse;
                    font-size: 0.95em;
                }}
                
                .trades-table th {{
                    background: #f5f5f5;
                    padding: 12px;
                    text-align: left;
                    font-weight: 600;
                    color: #667eea;
                    border-bottom: 2px solid #667eea;
                }}
                
                .trades-table td {{
                    padding: 12px;
                    border-bottom: 1px solid #eee;
                }}
                
                .trades-table tr:hover {{
                    background: #f9f9f9;
                }}
                
                .trade-win {{
                    color: #4caf50;
                    font-weight: 600;
                }}
                
                .trade-loss {{
                    color: #f44336;
                    font-weight: 600;
                }}
                
                .summary-box {{
                    background: #f9f9f9;
                    padding: 20px;
                    border-radius: 5px;
                    margin: 20px 0;
                    border-left: 4px solid #667eea;
                }}
                
                .summary-box h3 {{
                    color: #667eea;
                    margin-bottom: 10px;
                }}
                
                .summary-box p {{
                    color: #666;
                    line-height: 1.6;
                    margin: 5px 0;
                }}
                
                .footer {{
                    background: white;
                    padding: 30px;
                    border-radius: 10px;
                    text-align: center;
                    color: #999;
                    font-size: 0.9em;
                    box-shadow: 0 10px 30px rgba(0,0,0,0.2);
                }}
                
                .badge {{
                    display: inline-block;
                    padding: 8px 15px;
                    border-radius: 20px;
                    font-size: 0.85em;
                    font-weight: 600;
                    margin: 5px;
                }}
                
                .badge-success {{
                    background: #4caf50;
                    color: white;
                }}
                
                .badge-danger {{
                    background: #f44336;
                    color: white;
                }}
                
                .badge-info {{
                    background: #2196f3;
                    color: white;
                }}
                
                .page-break {{
                    page-break-after: always;
                    margin: 40px 0;
                    border-top: 2px dashed #ddd;
                    padding-top: 40px;
                }}
                
                @media print {{
                    body {{
                        background: white;
                    }}
                    .container {{
                        max-width: 100%;
                    }}
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>📊 Relatório de Trading - ArbitrageAI v2</h1>
                    <p><strong>Data:</strong> {datetime.now().strftime('%d de %B de %Y às %H:%M:%S')}</p>
                    <p><strong>Capital Inicial:</strong> ${metrics['capital_inicial']:,.2f}</p>
                    <p><strong>Capital Final:</strong> ${metrics['capital_final']:,.2f}</p>
                </div>
                
                <div class="metrics-grid">
                    <div class="metric-card {'positive' if metrics['total_pnl'] >= 0 else 'negative'}">
                        <div class="metric-label">P&L Total</div>
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
                        <div class="metric-label">Sharpe Ratio</div>
                        <div class="metric-value">{metrics['sharpe_ratio']:.2f}</div>
                        <div class="metric-subtext">Retorno ajustado ao risco</div>
                    </div>
                    
                    <div class="metric-card">
                        <div class="metric-label">Max Drawdown</div>
                        <div class="metric-value negative">
                            {metrics['max_drawdown_pct']:.2f}%
                        </div>
                        <div class="metric-subtext">
                            ${metrics['max_drawdown']:,.2f}
                        </div>
                    </div>
                    
                    <div class="metric-card">
                        <div class="metric-label">Profit Factor</div>
                        <div class="metric-value">{metrics['profit_factor']:.2f}</div>
                        <div class="metric-subtext">Ganho / Perda</div>
                    </div>
                    
                    <div class="metric-card">
                        <div class="metric-label">Total de Trades</div>
                        <div class="metric-value">{metrics['total_trades']}</div>
                        <div class="metric-subtext">Operações executadas</div>
                    </div>
                </div>
                
                <div class="section">
                    <h2>📈 Métricas Detalhadas</h2>
                    <table class="metrics-table">
                        <tr>
                            <td class="label">Capital Inicial</td>
                            <td class="value">${metrics['capital_inicial']:,.2f}</td>
                        </tr>
                        <tr>
                            <td class="label">Capital Final</td>
                            <td class="value">${metrics['capital_final']:,.2f}</td>
                        </tr>
                        <tr>
                            <td class="label">P&L Total</td>
                            <td class="value {'trade-win' if metrics['total_pnl'] >= 0 else 'trade-loss'}">
                                ${metrics['total_pnl']:,.2f}
                            </td>
                        </tr>
                        <tr>
                            <td class="label">Retorno (%)</td>
                            <td class="value {'trade-win' if metrics['total_pnl_pct'] >= 0 else 'trade-loss'}">
                                {metrics['total_pnl_pct']:.2f}%
                            </td>
                        </tr>
                        <tr>
                            <td class="label">Total de Trades</td>
                            <td class="value">{metrics['total_trades']}</td>
                        </tr>
                        <tr>
                            <td class="label">Trades Vencedores</td>
                            <td class="value trade-win">{metrics['win_trades']}</td>
                        </tr>
                        <tr>
                            <td class="label">Trades Perdedores</td>
                            <td class="value trade-loss">{metrics['loss_trades']}</td>
                        </tr>
                        <tr>
                            <td class="label">Break Even</td>
                            <td class="value">{metrics['break_even']}</td>
                        </tr>
                        <tr>
                            <td class="label">Taxa de Acerto</td>
                            <td class="value">{metrics['win_rate']:.2f}%</td>
                        </tr>
                        <tr>
                            <td class="label">Ganho Médio</td>
                            <td class="value trade-win">${metrics['avg_win']:,.2f}</td>
                        </tr>
                        <tr>
                            <td class="label">Perda Média</td>
                            <td class="value trade-loss">${metrics['avg_loss']:,.2f}</td>
                        </tr>
                        <tr>
                            <td class="label">Maior Ganho</td>
                            <td class="value trade-win">${metrics['max_win']:,.2f}</td>
                        </tr>
                        <tr>
                            <td class="label">Maior Perda</td>
                            <td class="value trade-loss">${metrics['max_loss']:,.2f}</td>
                        </tr>
                        <tr>
                            <td class="label">Profit Factor</td>
                            <td class="value">{metrics['profit_factor']:.2f}</td>
                        </tr>
                        <tr>
                            <td class="label">Expectância Matemática</td>
                            <td class="value">${metrics['expectancy']:,.2f}</td>
                        </tr>
                        <tr>
                            <td class="label">Max Drawdown</td>
                            <td class="value trade-loss">{metrics['max_drawdown_pct']:.2f}%</td>
                        </tr>
                        <tr>
                            <td class="label">Sharpe Ratio</td>
                            <td class="value">{metrics['sharpe_ratio']:.2f}</td>
                        </tr>
                        <tr>
                            <td class="label">Sortino Ratio</td>
                            <td class="value">{metrics['sortino_ratio']:.2f}</td>
                        </tr>
                        <tr>
                            <td class="label">Calmar Ratio</td>
                            <td class="value">{metrics['calmar_ratio']:.2f}</td>
                        </tr>
                        <tr>
                            <td class="label">Tamanho Médio de Trade</td>
                            <td class="value">${metrics['avg_trade_size']:,.2f}</td>
                        </tr>
                        <tr>
                            <td class="label">Maior Trade</td>
                            <td class="value">${metrics['largest_trade']:,.2f}</td>
                        </tr>
                        <tr>
                            <td class="label">Menor Trade</td>
                            <td class="value">${metrics['smallest_trade']:,.2f}</td>
                        </tr>
                    </table>
                </div>
                
                <div class="section">
                    <h2>📝 Resumo Executivo</h2>
                    <div class="summary-box">
                        <h3>Performance Geral</h3>
                        <p>O sistema executou <strong>{metrics['total_trades']} trades</strong> com um P&L total de <strong>${metrics['total_pnl']:,.2f}</strong> ({metrics['total_pnl_pct']:.2f}% de retorno).</p>
                        <p>A taxa de acerto foi de <strong>{metrics['win_rate']:.1f}%</strong>, com <strong>{metrics['win_trades']} trades vencedores</strong> e <strong>{metrics['loss_trades']} trades perdedores</strong>.</p>
                    </div>
                    
                    <div class="summary-box">
                        <h3>Métricas de Risco</h3>
                        <p>O máximo drawdown foi de <strong>{metrics['max_drawdown_pct']:.2f}%</strong> (${metrics['max_drawdown']:,.2f}), indicando a maior redução de capital durante o período.</p>
                        <p>O Sharpe Ratio de <strong>{metrics['sharpe_ratio']:.2f}</strong> indica o retorno ajustado ao risco, enquanto o Profit Factor de <strong>{metrics['profit_factor']:.2f}</strong> mostra a relação entre ganhos e perdas.</p>
                    </div>
                    
                    <div class="summary-box">
                        <h3>Recomendações</h3>
                        <p>✅ Continuar monitorando a performance do sistema</p>
                        <p>✅ Revisar os parâmetros de risco regularmente</p>
                        <p>✅ Manter o limite de perda diária de 5%</p>
                        <p>✅ Escalar gradualmente o capital conforme ganho de confiança</p>
                    </div>
                </div>
                
                <div class="footer">
                    <p>ArbitrageAI v2 | Sistema de Trading Automático Profissional</p>
                    <p>Relatório gerado em {datetime.now().strftime('%d/%m/%Y às %H:%M:%S')}</p>
                    <p>© 2026 - Todos os direitos reservados</p>
                </div>
            </div>
        </body>
        </html>
        """
        
        return html
    
    def save_report(self, metrics: Dict, trades: List[Dict], filename: str = None) -> str:
        """Salvar relatório em HTML"""
        
        if filename is None:
            filename = f"relatorio_trading_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
        
        filepath = os.path.join(self.reports_dir, filename)
        
        html = self.generate_html_report(metrics)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(html)
        
        logger.info(f"✅ Relatório salvo: {filepath}")
        return filepath
    
    def save_json_report(self, metrics: Dict, trades: List[Dict], filename: str = None) -> str:
        """Salvar relatório em JSON"""
        
        if filename is None:
            filename = f"relatorio_trading_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        filepath = os.path.join(self.reports_dir, filename)
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'metrics': metrics,
            'trades': trades,
            'ciclos': self.ciclos
        }
        
        with open(filepath, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        logger.info(f"✅ Relatório JSON salvo: {filepath}")
        return filepath
    
    def print_summary(self, metrics: Dict) -> None:
        """Imprimir resumo no console"""
        
        logger.info("\n" + "="*80)
        logger.info("📊 RELATÓRIO DE TRADING - RESUMO EXECUTIVO")
        logger.info("="*80)
        logger.info(f"\n💰 CAPITAL")
        logger.info(f"   Inicial: ${metrics['capital_inicial']:,.2f}")
        logger.info(f"   Final:   ${metrics['capital_final']:,.2f}")
        logger.info(f"   P&L:     ${metrics['total_pnl']:,.2f} ({metrics['total_pnl_pct']:.2f}%)")
        
        logger.info(f"\n📈 PERFORMANCE")
        logger.info(f"   Total de Trades:     {metrics['total_trades']}")
        logger.info(f"   Trades Vencedores:   {metrics['win_trades']} ✅")
        logger.info(f"   Trades Perdedores:   {metrics['loss_trades']} ❌")
        logger.info(f"   Break Even:          {metrics['break_even']}")
        logger.info(f"   Taxa de Acerto:      {metrics['win_rate']:.2f}%")
        
        logger.info(f"\n💵 GANHOS E PERDAS")
        logger.info(f"   Ganho Médio:         ${metrics['avg_win']:,.2f}")
        logger.info(f"   Perda Média:         ${metrics['avg_loss']:,.2f}")
        logger.info(f"   Maior Ganho:         ${metrics['max_win']:,.2f}")
        logger.info(f"   Maior Perda:         ${metrics['max_loss']:,.2f}")
        logger.info(f"   Profit Factor:       {metrics['profit_factor']:.2f}")
        
        logger.info(f"\n📊 MÉTRICAS AVANÇADAS")
        logger.info(f"   Sharpe Ratio:        {metrics['sharpe_ratio']:.2f}")
        logger.info(f"   Sortino Ratio:       {metrics['sortino_ratio']:.2f}")
        logger.info(f"   Calmar Ratio:        {metrics['calmar_ratio']:.2f}")
        logger.info(f"   Max Drawdown:        {metrics['max_drawdown_pct']:.2f}%")
        logger.info(f"   Expectância:         ${metrics['expectancy']:,.2f}")
        
        logger.info(f"\n📦 TAMANHO DE TRADES")
        logger.info(f"   Tamanho Médio:       ${metrics['avg_trade_size']:,.2f}")
        logger.info(f"   Maior Trade:         ${metrics['largest_trade']:,.2f}")
        logger.info(f"   Menor Trade:         ${metrics['smallest_trade']:,.2f}")
        
        logger.info("\n" + "="*80 + "\n")


def main():
    """Executar gerador de relatórios"""
    
    generator = TradingReportGenerator(capital_inicial=1000000)
    
    # Simular trades
    logger.info("\n" + "🚀"*40)
    logger.info("SIMULANDO TRADES EM TEMPO REAL")
    logger.info("🚀"*40)
    
    trades_simulados = [
        ('BTCUSDT', 'BUY', 45000, 46000, 0.5, 'TAKE_PROFIT'),
        ('ETHUSDT', 'BUY', 2500, 2400, 1.0, 'STOP_LOSS'),
        ('BNBUSDT', 'SELL', 600, 590, 2.0, 'TAKE_PROFIT'),
        ('ADAUSDT', 'BUY', 0.5, 0.52, 10000, 'TAKE_PROFIT'),
        ('XRPUSDT', 'BUY', 2.0, 1.95, 5000, 'STOP_LOSS'),
        ('BTCUSDT', 'SELL', 46000, 45500, 0.3, 'TAKE_PROFIT'),
    ]
    
    for symbol, side, entry, exit_p, qty, reason in trades_simulados:
        trade = generator.add_trade(symbol, side, entry, exit_p, qty, reason)
        emoji = "✅" if trade['pnl'] > 0 else "❌"
        logger.info(f"{emoji} {symbol} | {side} | P&L: ${trade['pnl']:,.2f} ({trade['pnl_pct']:.2f}%)")
    
    # Calcular métricas
    metrics = generator.calculate_metrics()
    
    # Imprimir resumo
    generator.print_summary(metrics)
    
    # Salvar relatórios
    html_file = generator.save_report(metrics, generator.trades)
    json_file = generator.save_json_report(metrics, generator.trades)
    
    logger.info(f"✅ Relatório HTML: {html_file}")
    logger.info(f"✅ Relatório JSON: {json_file}")
    
    print("\n✅ RELATÓRIO GERADO COM SUCESSO!")


if __name__ == "__main__":
    main()
