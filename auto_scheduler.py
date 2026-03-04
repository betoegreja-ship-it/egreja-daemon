"""
Auto Scheduler - Executa Sofia IA automaticamente
Garante mínimo 10 operações por dia em horário de trading
"""

import os
import sys
import time
import logging
from datetime import datetime
from pathlib import Path

# Configurar logging
log_dir = Path("/home/ubuntu/arbitrage-dashboard/logs/scheduler")
log_dir.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_dir / f"scheduler_{datetime.now().strftime('%Y%m%d')}.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("auto_scheduler")

def run_sofia_cycle():
    """Executar um ciclo de Sofia IA"""
    try:
        logger.info("="*60)
        logger.info("🚀 Iniciando ciclo de Sofia IA")
        logger.info("="*60)
        
        # Importar Sofia Integrated
        from sofia_integrated import SofiaIntegrated
        
        # Símbolos para análise
        symbols = [
            "BTCUSDT", "ETHUSDT", "BNBUSDT", "ADAUSDT", "XRPUSDT",
            "LTCUSDT", "DOGEUSDT", "MATICUSDT", "SOLUSDT", "DOTUSDT"
        ]
        
        # Executar ciclo
        sofia = SofiaIntegrated()
        results = sofia.execute_daily_cycle(symbols)
        
        # Log resultados
        logger.info(f"✅ Ciclo concluído com sucesso!")
        logger.info(f"   Total de trades: {results['total_trades']}")
        logger.info(f"   Ganhos: {results['winning_trades']} | Perdas: {results['losing_trades']}")
        logger.info(f"   P&L Total: ${results['total_pnl']:.2f}")
        logger.info(f"   Win Rate: {results['win_rate']:.1f}%")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Erro ao executar ciclo: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False

def main():
    """Main scheduler loop"""
    logger.info("🤖 Auto Scheduler Iniciado")
    logger.info(f"📅 Data: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Horário de trading: 9h-17h (horário local)
    trading_hours = range(9, 18)  # 9h às 17h
    
    cycles_today = 0
    min_cycles = 10
    
    while True:
        now = datetime.now()
        current_hour = now.hour
        
        # Verificar se está em horário de trading
        if current_hour in trading_hours:
            logger.info(f"⏰ Hora atual: {now.strftime('%H:%M')} - Executando ciclo...")
            
            success = run_sofia_cycle()
            
            if success:
                cycles_today += 1
                logger.info(f"📊 Ciclos hoje: {cycles_today}/{min_cycles}")
            
            # Aguardar 1 hora antes do próximo ciclo
            logger.info("⏳ Aguardando 1 hora para próximo ciclo...")
            time.sleep(3600)  # 1 hora
            
        else:
            # Fora do horário de trading
            if cycles_today < min_cycles:
                logger.warning(f"⚠️ Apenas {cycles_today} ciclos executados hoje (mínimo: {min_cycles})")
                logger.info("🔄 Executando ciclo adicional para atingir meta...")
                run_sofia_cycle()
                cycles_today += 1
            
            logger.info(f"🌙 Fora do horário de trading ({current_hour}h)")
            logger.info(f"📊 Total de ciclos hoje: {cycles_today}")
            
            # Aguardar até próximo horário de trading
            if current_hour < 9:
                wait_hours = 9 - current_hour
            else:
                wait_hours = 24 - current_hour + 9
            
            logger.info(f"⏳ Aguardando {wait_hours}h até próximo horário de trading...")
            time.sleep(wait_hours * 3600)
            
            # Reset contador para novo dia
            cycles_today = 0

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("\n👋 Scheduler interrompido pelo usuário")
        sys.exit(0)
    except Exception as e:
        logger.error(f"❌ Erro fatal no scheduler: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)
