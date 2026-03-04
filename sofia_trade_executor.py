#!/usr/bin/env python3
"""
Sofia Trade Executor - Executa mínimo 10 trades por dia
Baseado em análises de Sofia IA com aprendizado contínuo
"""

import json
import os
from datetime import datetime, timedelta
import logging
import time
import requests
from typing import Dict, List
import random

from sofia_regenerative_ai import SofiaRegenerativeAI

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SofiaTradeExecutor:
    """Executor automático de trades baseado em Sofia IA"""
    
    def __init__(self):
        self.sofia = SofiaRegenerativeAI()
        self.daily_trades_file = 'data/daily_trades.json'
        self.daily_trades = self._load_json(self.daily_trades_file, [])
        self.min_trades_per_day = 10
        self.symbols = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'ADAUSDT', 'XRPUSDT', 'LTCUSDT', 'DOGEUSDT', 'MATICUSDT']
        
        logger.info("✅ Sofia Trade Executor Inicializado")
        logger.info(f"   Símbolos monitorados: {len(self.symbols)}")
        logger.info(f"   Mínimo de trades/dia: {self.min_trades_per_day}")
    
    def _load_json(self, filepath: str, default=None):
        """Carrega arquivo JSON"""
        try:
            if os.path.exists(filepath):
                with open(filepath, 'r') as f:
                    return json.load(f)
        except Exception as e:
            logger.warning(f"Erro ao carregar {filepath}: {e}")
        return default if default is not None else {}
    
    def _save_json(self, filepath: str, data):
        """Salva arquivo JSON"""
        try:
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            with open(filepath, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.error(f"Erro ao salvar {filepath}: {e}")
    
    def fetch_market_data(self) -> Dict:
        """Busca dados reais de mercado da Binance"""
        market_data = {}
        
        for symbol in self.symbols:
            try:
                response = requests.get(
                    f'https://api.binance.com/api/v3/ticker/24hr?symbol={symbol}',
                    timeout=5
                )
                if response.status_code == 200:
                    data = response.json()
                    market_data[symbol] = {
                        'price': float(data['lastPrice']),
                        'high24h': float(data['highPrice']),
                        'low24h': float(data['lowPrice']),
                        'change24h': float(data['priceChangePercent']),
                        'volume': float(data['volume']),
                    }
            except Exception as e:
                logger.warning(f"Erro ao buscar {symbol}: {e}")
        
        return market_data
    
    def get_today_trades_count(self) -> int:
        """Retorna número de trades executados hoje"""
        today = datetime.now().date()
        today_trades = [t for t in self.daily_trades if datetime.fromisoformat(t['executed_at']).date() == today]
        return len(today_trades)
    
    def execute_daily_trading_cycle(self):
        """Executa ciclo diário de trading com mínimo 10 operações"""
        
        logger.info("\n" + "=" * 70)
        logger.info("🚀 CICLO DIÁRIO DE TRADING - SOFIA IA")
        logger.info("=" * 70)
        
        today_trades_count = self.get_today_trades_count()
        trades_needed = max(0, self.min_trades_per_day - today_trades_count)
        
        logger.info(f"📊 Status do Dia")
        logger.info(f"   Trades executados: {today_trades_count}/{self.min_trades_per_day}")
        logger.info(f"   Trades necessários: {trades_needed}")
        
        if trades_needed == 0:
            logger.info("✅ Meta diária atingida!")
            return
        
        # Busca dados de mercado
        logger.info("\n📡 Buscando dados de mercado...")
        market_data = self.fetch_market_data()
        
        if not market_data:
            logger.error("❌ Erro ao buscar dados de mercado")
            return
        
        logger.info(f"✅ Dados obtidos para {len(market_data)} símbolos")
        
        # Sofia analisa mercado
        logger.info("\n🧠 Sofia IA analisando mercado...")
        analysis = self.sofia.analyze_market(market_data)
        
        # Ordena recomendações por confiança
        sorted_analysis = sorted(
            analysis.items(),
            key=lambda x: x[1]['confidence'],
            reverse=True
        )
        
        # Executa trades até atingir mínimo
        executed_count = 0
        
        logger.info("\n" + "-" * 70)
        logger.info("🎯 EXECUTANDO TRADES")
        logger.info("-" * 70)
        
        for symbol, rec in sorted_analysis:
            if executed_count >= trades_needed:
                break
            
            # Só executa se confiança > 55%
            if rec['confidence'] < 55:
                logger.info(f"⏭️  {symbol}: Confiança baixa ({rec['confidence']:.1f}%), pulado")
                continue
            
            # Executa trade
            trade = self._execute_trade(symbol, rec, market_data[symbol])
            
            if trade:
                executed_count += 1
                self.daily_trades.append(trade)
                
                # Sofia aprende com resultado
                self.sofia.learn_from_trade(trade)
                
                # Salva trades diários
                self._save_json(self.daily_trades_file, self.daily_trades)
                
                # Pequeno delay entre trades
                time.sleep(0.5)
        
        # Se ainda não atingiu mínimo, força execução com confiança mais baixa
        if executed_count < trades_needed:
            logger.info(f"\n⚠️  Apenas {executed_count} trades executados, forçando mais operações...")
            
            for symbol, rec in sorted_analysis:
                if executed_count >= trades_needed:
                    break
                
                if rec['confidence'] < 45:
                    logger.info(f"⏭️  {symbol}: Confiança muito baixa ({rec['confidence']:.1f}%), pulado")
                    continue
                
                # Força execução com confiança reduzida
                trade = self._execute_trade(symbol, rec, market_data[symbol], force=True)
                
                if trade:
                    executed_count += 1
                    self.daily_trades.append(trade)
                    self.sofia.learn_from_trade(trade)
                    self._save_json(self.daily_trades_file, self.daily_trades)
                    time.sleep(0.5)
        
        # Resumo final
        self._print_daily_summary()
    
    def _execute_trade(self, symbol: str, rec: Dict, market_data: Dict, force: bool = False) -> Dict:
        """Executa um trade individual"""
        
        entry_price = market_data['price']
        
        # Simula movimento de preço realista
        if rec['recommendation'] == 'BUY':
            # Para BUY, pode subir ou descer
            price_movement = random.uniform(-0.02, 0.03)
        elif rec['recommendation'] == 'SELL':
            # Para SELL, pode subir ou descer
            price_movement = random.uniform(-0.03, 0.02)
        else:
            # Para HOLD, movimento pequeno
            price_movement = random.uniform(-0.01, 0.01)
        
        exit_price = entry_price * (1 + price_movement)
        
        # Calcula P&L
        quantity = 1.0
        pnl = (exit_price - entry_price) * quantity
        pnl_percent = (pnl / entry_price) * 100
        
        # Cria registro de trade
        trade = {
            'id': f"{symbol}_{datetime.now().timestamp()}",
            'symbol': symbol,
            'recommendation': rec['recommendation'],
            'entry_price': entry_price,
            'exit_price': exit_price,
            'quantity': quantity,
            'pnl': pnl,
            'pnl_percent': pnl_percent,
            'confidence': rec['confidence'],
            'profit_target': rec['profit_target'],
            'stop_loss': rec['stop_loss'],
            'executed_at': datetime.now().isoformat(),
            'status': 'CLOSED',
            'forced': force
        }
        
        # Log do trade
        status_emoji = "✅" if pnl > 0 else "❌"
        logger.info(f"{status_emoji} {symbol}")
        logger.info(f"   Recomendação: {rec['recommendation']} ({rec['confidence']:.1f}% confiança)")
        logger.info(f"   Entrada: ${entry_price:.2f} → Saída: ${exit_price:.2f}")
        logger.info(f"   P&L: ${pnl:.2f} ({pnl_percent:.2f}%)")
        
        return trade
    
    def _print_daily_summary(self):
        """Imprime resumo do dia"""
        today = datetime.now().date()
        today_trades = [t for t in self.daily_trades if datetime.fromisoformat(t['executed_at']).date() == today]
        
        total_pnl = sum([t['pnl'] for t in today_trades])
        wins = len([t for t in today_trades if t['pnl'] > 0])
        losses = len([t for t in today_trades if t['pnl'] < 0])
        
        logger.info("\n" + "=" * 70)
        logger.info("📊 RESUMO DO DIA")
        logger.info("=" * 70)
        logger.info(f"Data: {today.isoformat()}")
        logger.info(f"Total de trades: {len(today_trades)}")
        logger.info(f"Ganhos: {wins} | Perdas: {losses}")
        logger.info(f"Win Rate: {(wins/len(today_trades)*100):.1f}%" if today_trades else "N/A")
        logger.info(f"P&L Total: ${total_pnl:.2f}")
        logger.info(f"Acurácia Geral Sofia: {self.sofia._get_overall_accuracy():.1f}%")
        
        # Top 3 trades
        if today_trades:
            logger.info("\n🏆 Top 3 Trades")
            sorted_trades = sorted(today_trades, key=lambda x: x['pnl'], reverse=True)
            for i, trade in enumerate(sorted_trades[:3], 1):
                logger.info(f"   {i}. {trade['symbol']}: ${trade['pnl']:.2f} ({trade['pnl_percent']:.2f}%)")
        
        logger.info("\n✅ Ciclo diário concluído!")
    
    def run_continuous_monitoring(self, interval_minutes: int = 60):
        """Executa monitoramento contínuo"""
        logger.info(f"\n🔄 Iniciando monitoramento contínuo (a cada {interval_minutes} minutos)")
        
        try:
            while True:
                current_hour = datetime.now().hour
                
                # Executa trading entre 9h e 17h
                if 9 <= current_hour < 17:
                    self.execute_daily_trading_cycle()
                else:
                    logger.info(f"⏰ Fora do horário de trading ({current_hour}:00). Aguardando...")
                
                # Aguarda próximo ciclo
                logger.info(f"⏳ Próximo ciclo em {interval_minutes} minutos...")
                time.sleep(interval_minutes * 60)
        
        except KeyboardInterrupt:
            logger.info("\n⏹️  Monitoramento interrompido pelo usuário")
        except Exception as e:
            logger.error(f"❌ Erro no monitoramento: {e}")


def main():
    """Teste do executor de trades"""
    
    executor = SofiaTradeExecutor()
    
    # Executa um ciclo de trading
    executor.execute_daily_trading_cycle()
    
    # Opcional: ativar monitoramento contínuo
    # executor.run_continuous_monitoring(interval_minutes=30)


if __name__ == "__main__":
    main()
