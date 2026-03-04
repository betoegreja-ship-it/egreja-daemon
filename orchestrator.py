#!/usr/bin/env python3.11
"""
ArbitrageAI v2 - Orquestrador Automático
Executa análises diárias, gera sinais e atualiza dashboard
"""

import sys
sys.path.insert(0, '/home/ubuntu/arbitrage-dashboard')

import json
import subprocess
from datetime import datetime
import logging
import numpy as np

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/home/ubuntu/arbitrage-dashboard/orchestrator.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ArbitrageOrchestrator:
    """Orquestrador de execução automática"""
    
    def __init__(self):
        self.project_dir = '/home/ubuntu/arbitrage-dashboard'
        self.public_dir = f'{self.project_dir}/client/public'
        
    def run_crypto_analysis(self):
        """Executar análise de crypto pairs"""
        logger.info("🚀 Iniciando análise de Crypto Pairs Trading...")
        try:
            result = subprocess.run(
                ['python3.11', f'{self.project_dir}/crypto_pairs_strategy.py'],
                capture_output=True,
                text=True,
                timeout=120
            )
            if result.returncode == 0:
                logger.info("✓ Análise de crypto concluída")
                return True
            else:
                logger.error(f"✗ Erro na análise de crypto: {result.stderr}")
                return False
        except Exception as e:
            logger.error(f"✗ Exceção na análise de crypto: {e}")
            return False
    
    def run_backtesting(self):
        """Executar backtesting profissional"""
        logger.info("📊 Iniciando backtesting...")
        try:
            result = subprocess.run(
                ['python3.11', f'{self.project_dir}/advanced_backtesting.py'],
                capture_output=True,
                text=True,
                timeout=300
            )
            if result.returncode == 0:
                logger.info("✓ Backtesting concluído")
                return True
            else:
                logger.error(f"✗ Erro no backtesting: {result.stderr}")
                return False
        except Exception as e:
            logger.error(f"✗ Exceção no backtesting: {e}")
            return False
    
    def generate_dashboard_data(self):
        """Gerar dados consolidados para dashboard"""
        logger.info("📈 Gerando dados para dashboard...")
        try:
            result = subprocess.run(
                ['python3.11', f'{self.project_dir}/generate_dashboard_data.py'],
                capture_output=True,
                text=True,
                timeout=120
            )
            if result.returncode == 0:
                logger.info("✓ Dados do dashboard gerados")
                return True
            else:
                logger.error(f"✗ Erro ao gerar dados: {result.stderr}")
                return False
        except Exception as e:
            logger.error(f"✗ Exceção ao gerar dados: {e}")
            return False
    
    def create_daily_report(self):
        """Criar relatório diário"""
        logger.info("📄 Criando relatório diário...")
        
        try:
            # Ler sinais e resultados
            with open(f'{self.public_dir}/crypto-signal.json', 'r') as f:
                crypto_signal = json.load(f)
            
            with open(f'{self.public_dir}/backtest-results.json', 'r') as f:
                backtest_results = json.load(f)
            
            # Criar relatório
            report = {
                'timestamp': datetime.now().isoformat(),
                'date': datetime.now().strftime('%Y-%m-%d'),
                'crypto_signal': crypto_signal,
                'backtest_summary': {
                    'total_results': len(backtest_results.get('results', [])),
                    'avg_return': np.mean([r.get('total_return', 0) for r in backtest_results.get('results', [])]),
                    'avg_sharpe': np.mean([r.get('sharpe_ratio', 0) for r in backtest_results.get('results', [])]),
                    'best_strategy': max(backtest_results.get('results', []), key=lambda x: x.get('total_return', 0)) if backtest_results.get('results') else None
                }
            }
            
            # Salvar relatório
            report_file = f'{self.public_dir}/daily-report-{datetime.now().strftime("%Y-%m-%d")}.json'
            with open(report_file, 'w') as f:
                json.dump(report, f, indent=2)
            
            logger.info(f"✓ Relatório diário criado: {report_file}")
            return report
            
        except Exception as e:
            logger.error(f"✗ Erro ao criar relatório: {e}")
            return None
    
    def run_daily_analysis(self):
        """Executar análise completa diária"""
        logger.info("\n" + "="*70)
        logger.info("ARBITRAGEAI V2 - ANÁLISE DIÁRIA AUTOMÁTICA")
        logger.info(f"Timestamp: {datetime.now().isoformat()}")
        logger.info("="*70)
        
        # Executar análises
        steps = [
            ("Análise de Crypto", self.run_crypto_analysis),
            ("Backtesting", self.run_backtesting),
            ("Dados do Dashboard", self.generate_dashboard_data),
        ]
        
        results = {}
        for step_name, step_func in steps:
            success = step_func()
            results[step_name] = "✓ Sucesso" if success else "✗ Falha"
            logger.info(f"{results[step_name]}: {step_name}")
        
        # Criar relatório
        report = self.create_daily_report()
        
        # Resumo final
        logger.info("\n" + "="*70)
        logger.info("RESUMO DA EXECUÇÃO")
        logger.info("="*70)
        for step, result in results.items():
            logger.info(f"{result}: {step}")
        
        if report:
            logger.info(f"\n📊 Melhor Estratégia: {report['backtest_summary']['best_strategy']}")
            logger.info(f"📈 Retorno Médio: {report['backtest_summary']['avg_return']*100:.2f}%")
            logger.info(f"📊 Sharpe Médio: {report['backtest_summary']['avg_sharpe']:.2f}")
        
        logger.info("="*70)
        logger.info("✅ Análise diária concluída!")
        logger.info("="*70 + "\n")
        
        return results

def main():
    orchestrator = ArbitrageOrchestrator()
    results = orchestrator.run_daily_analysis()

if __name__ == "__main__":
    main()
