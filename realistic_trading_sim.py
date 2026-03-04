#!/usr/bin/env python3
"""
Realistic Trading Simulator - Simulador Realista com Cotações Reais
Operações de 2 horas com lucro alvo 2-3% e saída antecipada
"""

import os
import json
import logging
from datetime import datetime, timedelta
import requests
import numpy as np
from typing import Dict, List, Tuple

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class RealisticTradingSimulator:
    """Simulador realista com dados reais de cotação"""
    
    def __init__(self, capital_inicial: float = 1000000):
        """Inicializar simulador"""
        self.capital_inicial = capital_inicial
        self.capital_atual = capital_inicial
        self.trades = []
        self.operacoes_realizadas = 0
        
        # Parâmetros de trading
        self.lucro_alvo = 0.025  # 2.5% de lucro
        self.stop_loss = -0.02   # -2% de perda
        self.max_duracao = 2     # 2 horas máximo
        self.max_por_operacao = capital_inicial * 0.30  # 30% máximo
        
        # Símbolos para trading
        self.symbols = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'ADAUSDT', 'XRPUSDT']
        
        logger.info("✅ Realistic Trading Simulator Inicializado")
        logger.info(f"   Capital: ${capital_inicial:,.2f}")
        logger.info(f"   Lucro Alvo: {self.lucro_alvo*100:.1f}%")
        logger.info(f"   Stop Loss: {self.stop_loss*100:.1f}%")
        logger.info(f"   Duração Máxima: {self.max_duracao}h")
    
    def fetch_real_prices(self, symbol: str) -> Dict:
        """Buscar cotações reais da Binance"""
        try:
            # Usar API pública da Binance (sem autenticação)
            url = f"https://api.binance.com/api/v3/ticker/24hr?symbol={symbol}"
            response = requests.get(url, timeout=5)
            
            if response.status_code == 200:
                data = response.json()
                return {
                    'symbol': symbol,
                    'price': float(data['lastPrice']),
                    'high_24h': float(data['highPrice']),
                    'low_24h': float(data['lowPrice']),
                    'volume': float(data['volume']),
                    'timestamp': datetime.now().isoformat()
                }
        except Exception as e:
            logger.warning(f"⚠️ Erro ao buscar preço de {symbol}: {e}")
        
        return None
    
    def fetch_historical_prices(self, symbol: str, hours: int = 24) -> List[Dict]:
        """Buscar histórico de preços dos últimas horas"""
        try:
            # Usar klines (candlestick) da Binance
            url = f"https://api.binance.com/api/v3/klines"
            params = {
                'symbol': symbol,
                'interval': '1h',
                'limit': hours
            }
            response = requests.get(url, params=params, timeout=5)
            
            if response.status_code == 200:
                klines = response.json()
                prices = []
                for kline in klines:
                    prices.append({
                        'timestamp': datetime.fromtimestamp(kline[0]/1000).isoformat(),
                        'open': float(kline[1]),
                        'high': float(kline[2]),
                        'low': float(kline[3]),
                        'close': float(kline[4]),
                        'volume': float(kline[7])
                    })
                return prices
        except Exception as e:
            logger.warning(f"⚠️ Erro ao buscar histórico de {symbol}: {e}")
        
        return []
    
    def simulate_trade(self, symbol: str, entry_price: float) -> Dict:
        """Simular uma operação realista de 2 horas"""
        
        # Calcular preços alvo
        lucro_alvo_price = entry_price * (1 + self.lucro_alvo)
        stop_loss_price = entry_price * (1 + self.stop_loss)
        
        # Quantidade baseada no capital
        position_value = min(
            np.random.uniform(self.max_por_operacao * 0.1, self.max_por_operacao * 0.5),
            self.capital_atual * 0.30
        )
        quantity = position_value / entry_price
        
        # Simular movimento de preço realista durante 2 horas
        # Usar movimento browniano geométrico
        dt = 1/12  # 5 minutos em horas
        steps = 24  # 24 passos de 5 minutos = 2 horas
        
        prices = [entry_price]
        volatility = 0.01  # 1% volatilidade por hora
        
        for _ in range(steps):
            # Movimento aleatório com tendência leve
            trend = np.random.normal(0, volatility/np.sqrt(steps))
            noise = np.random.normal(0, volatility/np.sqrt(steps))
            
            new_price = prices[-1] * (1 + trend + noise)
            prices.append(new_price)
            
            # Verificar se atingiu lucro alvo
            if new_price >= lucro_alvo_price:
                exit_price = lucro_alvo_price
                exit_reason = "LUCRO_ALVO_ATINGIDO"
                duracao_horas = (_ + 1) / 12  # Converter passos para horas
                break
            
            # Verificar se atingiu stop loss
            if new_price <= stop_loss_price:
                exit_price = stop_loss_price
                exit_reason = "STOP_LOSS"
                duracao_horas = (_ + 1) / 12
                break
        else:
            # Se não atingiu nem lucro nem stop em 2 horas, sair no preço final
            exit_price = prices[-1]
            exit_reason = "TIMEOUT_2H"
            duracao_horas = 2.0
        
        # Calcular P&L
        pnl = (exit_price - entry_price) * quantity
        pnl_pct = ((exit_price - entry_price) / entry_price) * 100
        
        # Atualizar capital
        self.capital_atual += pnl
        
        trade = {
            'id': len(self.trades) + 1,
            'symbol': symbol,
            'entry_price': entry_price,
            'exit_price': exit_price,
            'quantity': quantity,
            'position_value': entry_price * quantity,
            'pnl': pnl,
            'pnl_pct': pnl_pct,
            'duracao_horas': duracao_horas,
            'razao_saida': exit_reason,
            'timestamp': datetime.now().isoformat()
        }
        
        self.trades.append(trade)
        self.operacoes_realizadas += 1
        
        return trade
    
    def run_realistic_simulation(self, num_operacoes: int = 20) -> Dict:
        """Executar simulação realista"""
        
        logger.info("\n\n" + "🚀"*40)
        logger.info("SIMULAÇÃO REALISTA DE TRADING")
        logger.info("Com Cotações Reais da Binance Brasil")
        logger.info("🚀"*40)
        
        logger.info(f"\n📊 Buscando cotações reais da Binance...")
        
        # Buscar preços reais
        precos_reais = {}
        for symbol in self.symbols:
            price_data = self.fetch_real_prices(symbol)
            if price_data:
                precos_reais[symbol] = price_data['price']
                logger.info(f"   {symbol}: ${price_data['price']:,.2f}")
        
        if not precos_reais:
            logger.error("❌ Não foi possível buscar cotações reais. Usando preços simulados.")
            precos_reais = {
                'BTCUSDT': 45000,
                'ETHUSDT': 2500,
                'BNBUSDT': 600,
                'ADAUSDT': 0.5,
                'XRPUSDT': 2.0
            }
        
        logger.info(f"\n🔄 Executando {num_operacoes} operações realistas...")
        logger.info("   Cada operação: máximo 2 horas, lucro alvo 2-3%, saída antecipada")
        
        total_pnl = 0
        
        for i in range(num_operacoes):
            symbol = self.symbols[i % len(self.symbols)]
            entry_price = precos_reais[symbol]
            
            # Simular pequena variação no preço de entrada
            entry_price *= (1 + np.random.uniform(-0.005, 0.005))
            
            trade = self.simulate_trade(symbol, entry_price)
            total_pnl += trade['pnl']
            
            emoji = "✅" if trade['pnl'] > 0 else "❌"
            logger.info(f"{emoji} Op #{trade['id']:2d} | {symbol} | P&L: ${trade['pnl']:>10,.2f} ({trade['pnl_pct']:>6.2f}%) | {trade['razao_saida']} | {trade['duracao_horas']:.1f}h")
        
        # Calcular métricas finais
        metrics = self._calculate_metrics()
        
        logger.info("\n\n" + "="*80)
        logger.info("📊 RESUMO FINAL - SIMULAÇÃO REALISTA")
        logger.info("="*80)
        logger.info(f"\n💰 CAPITAL")
        logger.info(f"   Inicial:  ${self.capital_inicial:,.2f}")
        logger.info(f"   Final:    ${self.capital_atual:,.2f}")
        logger.info(f"   P&L:      ${total_pnl:,.2f}")
        logger.info(f"   Retorno:  {(total_pnl/self.capital_inicial)*100:.2f}%")
        
        logger.info(f"\n📈 PERFORMANCE")
        logger.info(f"   Total de Operações: {metrics['total_trades']}")
        logger.info(f"   Operações Lucrativas: {metrics['win_trades']} ✅")
        logger.info(f"   Operações com Prejuízo: {metrics['loss_trades']} ❌")
        logger.info(f"   Taxa de Acerto: {metrics['win_rate']:.1f}%")
        
        logger.info(f"\n💵 GANHOS E PERDAS")
        logger.info(f"   Ganho Médio: ${metrics['avg_win']:,.2f}")
        logger.info(f"   Perda Média: ${metrics['avg_loss']:,.2f}")
        logger.info(f"   Maior Ganho: ${metrics['max_win']:,.2f}")
        logger.info(f"   Maior Perda: ${metrics['max_loss']:,.2f}")
        
        logger.info(f"\n⏱️  DURAÇÃO DAS OPERAÇÕES")
        logger.info(f"   Duração Média: {metrics['avg_duracao']:.2f}h")
        logger.info(f"   Maior Duração: {metrics['max_duracao']:.2f}h")
        
        logger.info("="*80 + "\n")
        
        return {
            'metrics': metrics,
            'trades': self.trades,
            'total_pnl': total_pnl,
            'precos_reais': precos_reais
        }
    
    def _calculate_metrics(self) -> Dict:
        """Calcular métricas detalhadas"""
        
        if not self.trades:
            return {}
        
        trades_df = self.trades
        
        pnls = [t['pnl'] for t in trades_df]
        pnls_pct = [t['pnl_pct'] for t in trades_df]
        duracoes = [t['duracao_horas'] for t in trades_df]
        
        win_trades = len([t for t in trades_df if t['pnl'] > 0])
        loss_trades = len([t for t in trades_df if t['pnl'] < 0])
        
        winning_pnls = [t['pnl'] for t in trades_df if t['pnl'] > 0]
        losing_pnls = [t['pnl'] for t in trades_df if t['pnl'] < 0]
        
        return {
            'total_trades': len(trades_df),
            'win_trades': win_trades,
            'loss_trades': loss_trades,
            'win_rate': (win_trades / len(trades_df) * 100) if trades_df else 0,
            'total_pnl': sum(pnls),
            'total_pnl_pct': (sum(pnls) / self.capital_inicial) * 100,
            'avg_win': np.mean(winning_pnls) if winning_pnls else 0,
            'avg_loss': np.mean(losing_pnls) if losing_pnls else 0,
            'max_win': max(winning_pnls) if winning_pnls else 0,
            'max_loss': min(losing_pnls) if losing_pnls else 0,
            'avg_duracao': np.mean(duracoes),
            'max_duracao': max(duracoes),
            'min_duracao': min(duracoes),
            'capital_final': self.capital_atual
        }
    
    def save_report(self, result: Dict) -> str:
        """Salvar relatório em JSON"""
        
        os.makedirs('data/reports', exist_ok=True)
        
        filename = f"data/reports/relatorio_realista_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'tipo': 'SIMULACAO_REALISTA_COM_COTACOES_REAIS',
            'capital_inicial': self.capital_inicial,
            'capital_final': result['metrics']['capital_final'],
            'metrics': result['metrics'],
            'trades': result['trades'],
            'precos_reais_entrada': result['precos_reais'],
            'parametros': {
                'lucro_alvo_pct': self.lucro_alvo * 100,
                'stop_loss_pct': self.stop_loss * 100,
                'duracao_maxima_horas': self.max_duracao,
                'max_por_operacao_pct': 30
            }
        }
        
        with open(filename, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        logger.info(f"✅ Relatório salvo: {filename}")
        return filename


def main():
    """Executar simulação realista"""
    
    simulator = RealisticTradingSimulator(capital_inicial=1000000)
    result = simulator.run_realistic_simulation(num_operacoes=20)
    
    # Salvar relatório
    report_file = simulator.save_report(result)
    
    print("\n✅ SIMULAÇÃO REALISTA CONCLUÍDA!")
    print(f"\nResumo Final:")
    print(f"  Capital Inicial: ${simulator.capital_inicial:,.2f}")
    print(f"  Capital Final: ${result['metrics']['capital_final']:,.2f}")
    print(f"  P&L Total: ${result['total_pnl']:,.2f}")
    print(f"  Retorno: {result['metrics']['total_pnl_pct']:.2f}%")
    print(f"  Taxa de Acerto: {result['metrics']['win_rate']:.1f}%")
    print(f"\nRelatório: {report_file}")


if __name__ == "__main__":
    main()
