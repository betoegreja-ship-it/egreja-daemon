#!/usr/bin/env python3
"""
Advanced Cron Jobs Manager
Múltiplas tarefas automáticas com APScheduler
"""

import os
import logging
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import mysql.connector
import requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class AdvancedScheduler:
    def __init__(self):
        self.scheduler = BackgroundScheduler(daemon=True)
        self.db_config = {
            'host': os.environ.get('MYSQLHOST', 'localhost'),
            'user': os.environ.get('MYSQLUSER', 'root'),
            'password': os.environ.get('MYSQLPASSWORD', ''),
            'database': os.environ.get('MYSQLDATABASE', 'railway'),
            'port': int(os.environ.get('MYSQLPORT', '3306')),
        }
    
    def get_db(self):
        """Conecta ao banco"""
        try:
            return mysql.connector.connect(**self.db_config)
        except Exception as e:
            logger.error(f"❌ Erro ao conectar MySQL: {e}")
            return None
    
    # ============================================
    # TASK 1: Daily Report at 08:00
    # ============================================
    def daily_report_task(self):
        """Gera e envia relatório PDF diário"""
        try:
            logger.info("📊 [08:00] Gerando relatório diário...")
            
            # Importar gerador
            from pdf_report_generator import ReportGenerator
            from daily_report_scheduler import DailyReportScheduler
            
            scheduler = DailyReportScheduler()
            scheduler.generate_and_send_report()
            
            logger.info("✅ Relatório enviado!")
        except Exception as e:
            logger.error(f"❌ Erro ao gerar relatório: {e}")
    
    # ============================================
    # TASK 2: Weekly Performance Summary (Monday 09:00)
    # ============================================
    def weekly_summary_task(self):
        """Resumo semanal de performance"""
        try:
            logger.info("📈 [Segunda 09:00] Gerando resumo semanal...")
            
            conn = self.get_db()
            if not conn:
                return
            
            cursor = conn.cursor(dictionary=True)
            
            # Buscar sinais da última semana
            week_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
            cursor.execute(f"""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN score > 80 THEN 1 ELSE 0 END) as buy_signals,
                    SUM(CASE WHEN score < 20 THEN 1 ELSE 0 END) as sell_signals,
                    AVG(score) as avg_score,
                    MAX(score) as max_score,
                    MIN(score) as min_score
                FROM market_signals
                WHERE created_at >= '{week_ago}'
            """)
            
            stats = cursor.fetchone()
            cursor.close()
            conn.close()
            
            summary = f"""
            📊 RESUMO SEMANAL - {datetime.now().strftime('%d/%m/%Y')}
            
            Total de Sinais: {stats['total']}
            🟢 Sinais de Compra: {stats['buy_signals']}
            🔴 Sinais de Venda: {stats['sell_signals']}
            
            Score Médio: {stats['avg_score']:.1f}/100
            Score Máximo: {stats['max_score']}/100
            Score Mínimo: {stats['min_score']}/100
            """
            
            # Enviar via Telegram
            self.send_telegram_message(summary)
            logger.info("✅ Resumo semanal enviado!")
        
        except Exception as e:
            logger.error(f"❌ Erro ao gerar resumo semanal: {e}")
    
    # ============================================
    # TASK 3: Database Backup (Daily at 03:00)
    # ============================================
    def backup_database_task(self):
        """Backup automático do MySQL"""
        try:
            logger.info("💾 [03:00] Fazendo backup do banco...")
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_dir = '/tmp/egreja_backups'
            os.makedirs(backup_dir, exist_ok=True)
            
            backup_file = f"{backup_dir}/backup_{timestamp}.sql"
            
            # Comando mysqldump
            host = os.environ.get('MYSQLHOST', 'localhost')
            user = os.environ.get('MYSQLUSER', 'root')
            password = os.environ.get('MYSQLPASSWORD', '')
            database = os.environ.get('MYSQLDATABASE', 'railway')
            
            if password:
                cmd = f"mysqldump -h {host} -u {user} -p{password} {database} > {backup_file}"
            else:
                cmd = f"mysqldump -h {host} -u {user} {database} > {backup_file}"
            
            os.system(cmd)
            
            # Verificar tamanho
            size = os.path.getsize(backup_file) / (1024*1024)  # MB
            logger.info(f"✅ Backup criado: {backup_file} ({size:.2f} MB)")
            
            # Manter apenas últimas 7 cópias
            self.cleanup_old_backups(backup_dir, keep=7)
        
        except Exception as e:
            logger.error(f"❌ Erro ao fazer backup: {e}")
    
    def cleanup_old_backups(self, backup_dir, keep=7):
        """Remove backups antigos"""
        try:
            files = sorted(os.listdir(backup_dir))
            if len(files) > keep:
                for old_file in files[:-keep]:
                    os.remove(os.path.join(backup_dir, old_file))
                    logger.info(f"🗑️ Removido backup antigo: {old_file}")
        except Exception as e:
            logger.error(f"❌ Erro ao limpar backups: {e}")
    
    # ============================================
    # TASK 4: Health Check (Every 30 min)
    # ============================================
    def health_check_task(self):
        """Verifica saúde do sistema a cada 30 min"""
        try:
            logger.info("🏥 Verificando saúde do sistema...")
            
            # Check Database
            conn = self.get_db()
            if conn:
                conn.close()
                db_status = "✅ OK"
            else:
                db_status = "❌ ERRO"
            
            # Check API
            try:
                api_url = "/signals"
                response = requests.get(api_url, timeout=5)
                api_status = "✅ OK" if response.status_code == 200 else f"❌ {response.status_code}"
            except:
                api_status = "❌ TIMEOUT"
            
            # Check Website
            try:
                response = requests.get("https://www.egreja.com", timeout=5)
                web_status = "✅ OK" if response.status_code == 200 else f"❌ {response.status_code}"
            except:
                web_status = "❌ TIMEOUT"
            
            logger.info(f"Database: {db_status} | API: {api_status} | Website: {web_status}")
            
            # Alert if any down
            if "❌" in [db_status, api_status, web_status]:
                self.send_telegram_message(f"""
                🚨 ALERTA DE SAÚDE DO SISTEMA
                
                Database: {db_status}
                API: {api_status}
                Website: {web_status}
                
                Verifique imediatamente!
                """)
        
        except Exception as e:
            logger.error(f"❌ Erro no health check: {e}")
    
    # ============================================
    # TASK 5: Clean Old Logs (Daily at 23:00)
    # ============================================
    def cleanup_logs_task(self):
        """Limpa logs antigos (> 30 dias)"""
        try:
            logger.info("🧹 Limpando logs antigos...")
            
            log_dir = '/tmp/egreja_logs'
            if os.path.exists(log_dir):
                cutoff_time = datetime.now() - timedelta(days=30)
                
                for filename in os.listdir(log_dir):
                    filepath = os.path.join(log_dir, filename)
                    if os.path.isfile(filepath):
                        mod_time = datetime.fromtimestamp(os.path.getmtime(filepath))
                        if mod_time < cutoff_time:
                            os.remove(filepath)
                            logger.info(f"🗑️ Log removido: {filename}")
            
            logger.info("✅ Limpeza de logs concluída!")
        
        except Exception as e:
            logger.error(f"❌ Erro ao limpar logs: {e}")
    
    # ============================================
    # TASK 6: Performance Analysis (Sunday 18:00)
    # ============================================
    def performance_analysis_task(self):
        """Análise de performance semanal"""
        try:
            logger.info("📊 [Domingo 18:00] Analisando performance...")
            
            from backtesting_engine import BacktestingEngine
            engine = BacktestingEngine()
            metrics = engine.generate_report(days=7)
            
            if metrics:
                summary = f"""
                📊 ANÁLISE DE PERFORMANCE (Última Semana)
                
                Win Rate: {metrics['win_rate']*100:.1f}%
                Profit Factor: {metrics['profit_factor']:.2f}x
                Max Drawdown: {metrics['max_drawdown']*100:.2f}%
                Total Trades: {metrics['total_trades']}
                
                P&L Total: {metrics['total_pnl']*100:+.2f}%
                """
                
                self.send_telegram_message(summary)
                logger.info("✅ Análise enviada!")
        
        except Exception as e:
            logger.error(f"❌ Erro na análise: {e}")
    
    # ============================================
    # Telegram Helper
    # ============================================
    def send_telegram_message(self, message):
        """Envia mensagem via Telegram"""
        try:
            token = os.getenv('TELEGRAM_BOT_TOKEN', '')
            chat_id = os.getenv('TELEGRAM_CHAT_ID', '')
            
            if not token or not chat_id:
                return
            
            url = f"https://api.telegram.org/bot{token}/sendMessage"
            data = {
                "chat_id": chat_id,
                "text": message,
                "parse_mode": "HTML"
            }
            
            requests.post(url, json=data)
        except Exception as e:
            logger.error(f"❌ Erro ao enviar Telegram: {e}")
    
    # ============================================
    # Start Scheduler
    # ============================================
    def start(self):
        """Inicia o scheduler com todas as tarefas"""
        
        logger.info("🚀 Iniciando Advanced Cron Jobs Scheduler...\n")
        
        # Task 1: Daily Report (08:00 GMT-3)
        self.scheduler.add_job(
            self.daily_report_task,
            CronTrigger(hour=8, minute=0, timezone='America/Sao_Paulo'),
            id='daily_report',
            name='📊 Daily Report at 08:00',
            replace_existing=True
        )
        logger.info("✅ Task 1: Daily Report (08:00) agendada")
        
        # Task 2: Weekly Summary (Monday 09:00)
        self.scheduler.add_job(
            self.weekly_summary_task,
            CronTrigger(day_of_week=0, hour=9, minute=0, timezone='America/Sao_Paulo'),
            id='weekly_summary',
            name='📈 Weekly Summary (Monday 09:00)',
            replace_existing=True
        )
        logger.info("✅ Task 2: Weekly Summary (Segunda 09:00) agendada")
        
        # Task 3: Database Backup (03:00)
        self.scheduler.add_job(
            self.backup_database_task,
            CronTrigger(hour=3, minute=0, timezone='America/Sao_Paulo'),
            id='db_backup',
            name='💾 Database Backup at 03:00',
            replace_existing=True
        )
        logger.info("✅ Task 3: Database Backup (03:00) agendada")
        
        # Task 4: Health Check (Every 30 min)
        self.scheduler.add_job(
            self.health_check_task,
            CronTrigger(minute='*/30', timezone='America/Sao_Paulo'),
            id='health_check',
            name='🏥 Health Check (Every 30 min)',
            replace_existing=True
        )
        logger.info("✅ Task 4: Health Check (a cada 30 min) agendada")
        
        # Task 5: Cleanup Logs (23:00)
        self.scheduler.add_job(
            self.cleanup_logs_task,
            CronTrigger(hour=23, minute=0, timezone='America/Sao_Paulo'),
            id='cleanup_logs',
            name='🧹 Cleanup Logs at 23:00',
            replace_existing=True
        )
        logger.info("✅ Task 5: Cleanup Logs (23:00) agendada")
        
        # Task 6: Performance Analysis (Sunday 18:00)
        self.scheduler.add_job(
            self.performance_analysis_task,
            CronTrigger(day_of_week=6, hour=18, minute=0, timezone='America/Sao_Paulo'),
            id='performance_analysis',
            name='📊 Performance Analysis (Sunday 18:00)',
            replace_existing=True
        )
        logger.info("✅ Task 6: Performance Analysis (Domingo 18:00) agendada")
        
        logger.info("\n" + "="*70)
        logger.info("🎯 SCHEDULER INICIADO COM 6 TAREFAS AUTOMÁTICAS!")
        logger.info("="*70 + "\n")
        
        # Iniciar scheduler
        self.scheduler.start()
        
        # Loop infinito
        try:
            while True:
                import time
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("⏹️ Scheduler parado")
            self.scheduler.shutdown()

# Teste
if __name__ == '__main__':
    scheduler = AdvancedScheduler()
    scheduler.start()
