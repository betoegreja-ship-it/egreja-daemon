#!/usr/bin/env python3
"""
ArbitrageAI v2 - Sistema Autônomo e Auto-Regenerativo
Aprendizado contínuo, simulações diárias e auto-melhoria
"""

import os
import json
import logging
from datetime import datetime, timedelta
import numpy as np
from binance_brasil_client import BinanceBrasilClient
from professional_strategies import ProfessionalStrategies
from backtesting_engine import BacktestingEngine

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class AutonomousTradingSystem:
    """Sistema autônomo de trading com aprendizado contínuo"""
    
    def __init__(self, api_key, api_secret):
        """Inicializar sistema autônomo"""
        self.api_key = api_key
        self.api_secret = api_secret
        self.binance = BinanceBrasilClient(api_key, api_secret)
        self.strategies = ProfessionalStrategies()
        self.backtester = BacktestingEngine()
        
        # Diretórios
        self.logs_dir = "logs/autonomous"
        self.data_dir = "data/autonomous"
        self.models_dir = "models/autonomous"
        
        os.makedirs(self.logs_dir, exist_ok=True)
        os.makedirs(self.data_dir, exist_ok=True)
        os.makedirs(self.models_dir, exist_ok=True)
        
        # Carregar histórico de aprendizado
        self.learning_history = self._load_learning_history()
        self.strategy_performance = self._load_strategy_performance()
        
        logger.info("✅ Sistema Autônomo Inicializado")
    
    def _load_learning_history(self):
        """Carregar histórico de aprendizado"""
        history_file = f"{self.data_dir}/learning_history.json"
        if os.path.exists(history_file):
            with open(history_file, 'r') as f:
                return json.load(f)
        return {
            'created_at': datetime.now().isoformat(),
            'simulations': [],
            'improvements': [],
            'best_strategies': []
        }
    
    def _load_strategy_performance(self):
        """Carregar performance de estratégias"""
        perf_file = f"{self.data_dir}/strategy_performance.json"
        if os.path.exists(perf_file):
            with open(perf_file, 'r') as f:
                return json.load(f)
        return {}
    
    def _save_learning_history(self):
        """Salvar histórico de aprendizado"""
        history_file = f"{self.data_dir}/learning_history.json"
        with open(history_file, 'w') as f:
            json.dump(self.learning_history, f, indent=2, default=str)
    
    def _save_strategy_performance(self):
        """Salvar performance de estratégias"""
        perf_file = f"{self.data_dir}/strategy_performance.json"
        with open(perf_file, 'w') as f:
            json.dump(self.strategy_performance, f, indent=2, default=str)
    
    def run_real_time_test(self):
        """Executar teste em tempo real com dados reais"""
        logger.info("\n" + "="*70)
        logger.info("🚀 TESTE EM TEMPO REAL - Dados Reais da Binance")
        logger.info("="*70)
        
        test_result = {
            'timestamp': datetime.now().isoformat(),
            'type': 'real_time',
            'symbols': [],
            'signals': [],
            'performance': {}
        }
        
        # Símbolos para testar
        symbols = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'ADAUSDT', 'XRPUSDT']
        
        for symbol in symbols:
            logger.info(f"\n📊 Analisando {symbol}...")
            
            try:
                # Obter dados reais
                klines = self.binance.get_klines(symbol, '1h', limit=100)
                if not klines:
                    continue
                
                # Preparar dados
                closes = np.array([float(k[4]) for k in klines])
                volumes = np.array([float(k[7]) for k in klines])
                
                # Executar estratégias
                signal = self.strategies.mean_reversion_strategy(closes, volumes)
                
                if signal:
                    logger.info(f"   ✅ Sinal: {signal['direction']} | Confiança: {signal['confidence']:.1%}")
                    test_result['signals'].append({
                        'symbol': symbol,
                        'signal': signal['direction'],
                        'confidence': signal['confidence'],
                        'price': closes[-1]
                    })
                else:
                    logger.info(f"   ⏸️  Sem sinal")
                
                test_result['symbols'].append(symbol)
                
            except Exception as e:
                logger.error(f"   ❌ Erro: {e}")
        
        # Salvar resultado
        test_file = f"{self.logs_dir}/real_time_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(test_file, 'w') as f:
            json.dump(test_result, f, indent=2, default=str)
        
        logger.info(f"\n✅ Teste em tempo real concluído")
        logger.info(f"   Sinais gerados: {len(test_result['signals'])}")
        logger.info(f"   Arquivo: {test_file}")
        
        return test_result
    
    def run_daily_simulation(self):
        """Executar simulação diária para aprendizado"""
        logger.info("\n" + "="*70)
        logger.info("🧠 SIMULAÇÃO DIÁRIA - Aprendizado Contínuo")
        logger.info("="*70)
        
        simulation = {
            'timestamp': datetime.now().isoformat(),
            'type': 'daily_simulation',
            'backtests': [],
            'improvements': [],
            'new_strategies': []
        }
        
        # Símbolos para backtest
        symbols = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT']
        
        for symbol in symbols:
            logger.info(f"\n📈 Backtesting {symbol}...")
            
            try:
                # Obter dados históricos (últimos 30 dias)
                klines = self.binance.get_klines(symbol, '1d', limit=30)
                if not klines:
                    continue
                
                # Preparar dados
                closes = np.array([float(k[4]) for k in klines])
                
                # Executar backtest
                result = self.backtester.backtest_mean_reversion(closes)
                
                logger.info(f"   Retorno: {result['total_return']:.2%}")
                logger.info(f"   Sharpe Ratio: {result['sharpe_ratio']:.2f}")
                logger.info(f"   Win Rate: {result['win_rate']:.1%}")
                
                simulation['backtests'].append({
                    'symbol': symbol,
                    'return': result['total_return'],
                    'sharpe': result['sharpe_ratio'],
                    'win_rate': result['win_rate']
                })
                
            except Exception as e:
                logger.error(f"   ❌ Erro: {e}")
        
        # Analisar melhorias
        improvements = self._analyze_improvements(simulation)
        simulation['improvements'] = improvements
        
        # Gerar novas estratégias
        new_strategies = self._generate_new_strategies(simulation)
        simulation['new_strategies'] = new_strategies
        
        # Salvar simulação
        sim_file = f"{self.logs_dir}/daily_simulation_{datetime.now().strftime('%Y%m%d')}.json"
        with open(sim_file, 'w') as f:
            json.dump(simulation, f, indent=2, default=str)
        
        # Atualizar histórico
        self.learning_history['simulations'].append(simulation)
        self._save_learning_history()
        
        logger.info(f"\n✅ Simulação diária concluída")
        logger.info(f"   Melhorias identificadas: {len(improvements)}")
        logger.info(f"   Novas estratégias: {len(new_strategies)}")
        
        return simulation
    
    def _analyze_improvements(self, simulation):
        """Analisar melhorias identificadas"""
        improvements = []
        
        for backtest in simulation['backtests']:
            # Comparar com performance anterior
            symbol = backtest['symbol']
            if symbol in self.strategy_performance:
                prev_return = self.strategy_performance[symbol].get('return', 0)
                if backtest['return'] > prev_return:
                    improvements.append({
                        'symbol': symbol,
                        'improvement': backtest['return'] - prev_return,
                        'new_return': backtest['return'],
                        'prev_return': prev_return
                    })
                    logger.info(f"   ✅ Melhoria em {symbol}: +{(backtest['return'] - prev_return):.2%}")
            
            # Atualizar performance
            self.strategy_performance[symbol] = backtest
        
        self._save_strategy_performance()
        return improvements
    
    def _generate_new_strategies(self, simulation):
        """Gerar novas estratégias baseadas em aprendizado"""
        new_strategies = []
        
        # Analisar padrões de sucesso
        successful_backtests = [b for b in simulation['backtests'] if b['return'] > 0.01]
        
        if successful_backtests:
            # Estratégia 1: Aumentar confiança em símbolos com bom desempenho
            for backtest in successful_backtests:
                new_strategies.append({
                    'name': f"Enhanced Mean Reversion - {backtest['symbol']}",
                    'type': 'mean_reversion',
                    'symbol': backtest['symbol'],
                    'confidence_threshold': 0.75,
                    'expected_return': backtest['return'] * 1.1  # Esperar 10% de melhoria
                })
            
            # Estratégia 2: Pairs Trading com símbolos correlacionados
            if len(successful_backtests) >= 2:
                new_strategies.append({
                    'name': 'Pairs Trading - Top Performers',
                    'type': 'pairs_trading',
                    'symbols': [b['symbol'] for b in successful_backtests[:2]],
                    'correlation_threshold': 0.8,
                    'expected_return': 0.05
                })
        
        logger.info(f"   Novas estratégias geradas: {len(new_strategies)}")
        for strategy in new_strategies:
            logger.info(f"      - {strategy['name']}")
        
        return new_strategies
    
    def auto_regenerate(self):
        """Auto-regeneração do sistema com melhorias"""
        logger.info("\n" + "="*70)
        logger.info("🔄 AUTO-REGENERAÇÃO - Melhoria Contínua")
        logger.info("="*70)
        
        regeneration = {
            'timestamp': datetime.now().isoformat(),
            'optimizations': [],
            'new_parameters': {},
            'status': 'success'
        }
        
        # 1. Otimizar parâmetros baseado em histórico
        logger.info("\n🔧 Otimizando parâmetros...")
        
        avg_return = 0.01
        avg_sharpe = 1.0
        
        if self.strategy_performance:
            avg_return = np.mean([s.get('return', 0) for s in self.strategy_performance.values()])
            avg_sharpe = np.mean([s.get('sharpe', 0) for s in self.strategy_performance.values()])
            
            regeneration['optimizations'].append({
                'type': 'parameter_optimization',
                'avg_return': avg_return,
                'avg_sharpe': avg_sharpe,
                'status': 'completed'
            })
            
            logger.info(f"   Retorno médio: {avg_return:.2%}")
            logger.info(f"   Sharpe médio: {avg_sharpe:.2f}")
        
        # 2. Gerar novos parâmetros
        logger.info("\n📊 Gerando novos parâmetros...")
        
        regeneration['new_parameters'] = {
            'rsi_threshold': 30 if avg_return > 0.01 else 25,
            'z_score_threshold': 2.0 if avg_sharpe > 1.0 else 2.5,
            'confidence_threshold': 0.75 if avg_return > 0.01 else 0.70,
            'position_size': 0.02 if avg_return > 0.01 else 0.015
        }
        
        logger.info(f"   Novos parâmetros: {regeneration['new_parameters']}")
        
        # 3. Salvar regeneração
        regen_file = f"{self.models_dir}/regeneration_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(regen_file, 'w') as f:
            json.dump(regeneration, f, indent=2, default=str)
        
        logger.info(f"\n✅ Auto-regeneração concluída")
        
        return regeneration
    
    def run_full_cycle(self):
        """Executar ciclo completo: teste real + simulação + regeneração"""
        logger.info("\n\n" + "🌟"*35)
        logger.info("CICLO COMPLETO - ArbitrageAI v2 Autônomo")
        logger.info("🌟"*35)
        
        cycle_result = {
            'timestamp': datetime.now().isoformat(),
            'real_time_test': self.run_real_time_test(),
            'daily_simulation': self.run_daily_simulation(),
            'auto_regeneration': self.auto_regenerate()
        }
        
        # Salvar ciclo completo
        cycle_file = f"{self.logs_dir}/full_cycle_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(cycle_file, 'w') as f:
            json.dump(cycle_result, f, indent=2, default=str)
        
        logger.info("\n" + "✅"*35)
        logger.info("CICLO COMPLETO CONCLUÍDO COM SUCESSO!")
        logger.info("✅"*35 + "\n")
        
        return cycle_result


def main():
    """Executar sistema autônomo"""
    
    api_key = os.getenv('BINANCE_API_KEY')
    api_secret = os.getenv('BINANCE_API_SECRET')
    
    if not api_key or not api_secret:
        logger.error("❌ Chaves de API não encontradas")
        return
    
    # Criar sistema
    system = AutonomousTradingSystem(api_key, api_secret)
    
    # Executar ciclo completo
    result = system.run_full_cycle()
    
    print("\n" + "="*70)
    print("📊 RESUMO DO CICLO")
    print("="*70)
    print(f"Sinais em tempo real: {len(result['real_time_test']['signals'])}")
    print(f"Backtests executados: {len(result['daily_simulation']['backtests'])}")
    print(f"Melhorias identificadas: {len(result['daily_simulation']['improvements'])}")
    print(f"Novas estratégias: {len(result['daily_simulation']['new_strategies'])}")
    print("="*70 + "\n")


if __name__ == "__main__":
    main()
