#!/usr/bin/env python3
"""
Sofia Scheduler - Executa ciclos de trading automaticamente
Usa APScheduler para executar Sofia Trade Executor a cada hora
"""

import logging
from datetime import datetime, time
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import atexit

from sofia_trade_executor import SofiaTradeExecutor

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SofiaScheduler:
    """Scheduler automático para Sofia IA"""
    
    def __init__(self):
        self.scheduler = BackgroundScheduler()
        self.executor = SofiaTradeExecutor()
        self.is_running = False
        
        logger.info("✅ Sofia Scheduler Inicializado")
    
    def schedule_trading_cycles(self):
        """Agenda ciclos de trading automáticos"""
        
        # Executa a cada hora durante horário de trading (9h-17h)
        # Segunda a sexta
        self.scheduler.add_job(
            func=self._execute_trading_cycle,
            trigger=CronTrigger(
                hour='9-17',  # 9h até 17h
                minute=0,     # No início de cada hora
                day_of_week='0-4'  # Segunda a sexta
            ),
            id='sofia_hourly_trading',
            name='Sofia Hourly Trading Cycle',
            replace_existing=True
        )
        
        logger.info("✅ Ciclos horários agendados (9h-17h, seg-sex)")
        
        # Executa análise a cada 30 minutos (mesmo fora do horário de trading)
        self.scheduler.add_job(
            func=self._execute_analysis,
            trigger=CronTrigger(minute='*/30'),
            id='sofia_analysis',
            name='Sofia Market Analysis',
            replace_existing=True
        )
        
        logger.info("✅ Análises agendadas a cada 30 minutos")
        
        # Relatório diário às 18h
        self.scheduler.add_job(
            func=self._daily_report,
            trigger=CronTrigger(hour=18, minute=0),
            id='sofia_daily_report',
            name='Sofia Daily Report',
            replace_existing=True
        )
        
        logger.info("✅ Relatório diário agendado para 18h")
    
    def _execute_trading_cycle(self):
        """Executa ciclo de trading"""
        try:
            logger.info("\n" + "=" * 70)
            logger.info("🚀 CICLO DE TRADING AUTOMÁTICO INICIADO")
            logger.info("=" * 70)
            
            self.executor.execute_daily_trading_cycle()
            
            logger.info("✅ Ciclo de trading concluído com sucesso")
        
        except Exception as e:
            logger.error(f"❌ Erro no ciclo de trading: {e}")
    
    def _execute_analysis(self):
        """Executa análise de mercado"""
        try:
            logger.info("\n📊 Análise de mercado automática iniciada...")
            
            market_data = self.executor.fetch_market_data()
            analysis = self.executor.sofia.analyze_market(market_data)
            
            # Log resumido
            buy_count = len([a for a in analysis.values() if a['recommendation'] == 'BUY'])
            sell_count = len([a for a in analysis.values() if a['recommendation'] == 'SELL'])
            
            logger.info(f"✅ Análise concluída: {buy_count} BUY, {sell_count} SELL")
        
        except Exception as e:
            logger.error(f"❌ Erro na análise: {e}")
    
    def _daily_report(self):
        """Gera relatório diário"""
        try:
            logger.info("\n" + "=" * 70)
            logger.info("📊 RELATÓRIO DIÁRIO DE SOFIA")
            logger.info("=" * 70)
            
            summary = self.executor.sofia.get_daily_summary()
            
            logger.info(f"Data: {summary['date']}")
            logger.info(f"Total de trades: {summary['total_trades']}")
            logger.info(f"P&L Total: ${summary['total_pnl']:.2f}")
            logger.info(f"Win Rate: {summary['win_rate']:.1f}%")
            logger.info(f"Acurácia Geral: {self.executor.sofia._get_overall_accuracy():.1f}%")
            
            logger.info("✅ Relatório gerado")
        
        except Exception as e:
            logger.error(f"❌ Erro ao gerar relatório: {e}")
    
    def start(self):
        """Inicia o scheduler"""
        if self.is_running:
            logger.warning("⚠️  Scheduler já está rodando")
            return
        
        try:
            self.schedule_trading_cycles()
            self.scheduler.start()
            self.is_running = True
            
            logger.info("\n" + "=" * 70)
            logger.info("🎯 SOFIA SCHEDULER ATIVO")
            logger.info("=" * 70)
            logger.info("Horário de trading: 9h-17h (seg-sex)")
            logger.info("Ciclos: A cada hora")
            logger.info("Análises: A cada 30 minutos")
            logger.info("Relatório: 18h diariamente")
            logger.info("\nPressione Ctrl+C para parar")
            
            # Mantém o scheduler rodando
            atexit.register(self.shutdown)
        
        except Exception as e:
            logger.error(f"❌ Erro ao iniciar scheduler: {e}")
    
    def shutdown(self):
        """Para o scheduler"""
        if self.is_running:
            self.scheduler.shutdown()
            self.is_running = False
            logger.info("\n✅ Sofia Scheduler parado")


def main():
    """Inicia o scheduler"""
    scheduler = SofiaScheduler()
    scheduler.start()
    
    # Mantém rodando
    try:
        while True:
            pass
    except KeyboardInterrupt:
        scheduler.shutdown()


if __name__ == "__main__":
    main()
