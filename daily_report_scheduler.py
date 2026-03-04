#!/usr/bin/env python3
"""
Daily Report Scheduler
Gera relatório PDF diário, envia por email e alerta Telegram
Roda a cada dia às 08:00 GMT-3
"""

import os
import time
import schedule
from datetime import datetime
from pdf_report_generator import ReportGenerator
import requests
from pathlib import Path

class DailyReportScheduler:
    def __init__(self):
        self.generator = ReportGenerator()
        self.telegram_token = os.getenv('TELEGRAM_BOT_TOKEN', '')
        self.telegram_chat_id = os.getenv('TELEGRAM_CHAT_ID', '')
        self.resend_api_key = os.getenv('RESEND_API_KEY', '')
        self.recipient_email = 'betoegreja@gmail.com'
    
    def send_email_with_pdf(self, pdf_path):
        """Envia email com PDF anexado via Resend"""
        try:
            if not self.resend_api_key:
                print("❌ RESEND_API_KEY não configurada")
                return False
            
            # Ler PDF como base64
            with open(pdf_path, 'rb') as f:
                pdf_data = f.read()
                import base64
                pdf_base64 = base64.b64encode(pdf_data).decode('utf-8')
            
            # Enviar via Resend
            url = "https://api.resend.com/emails"
            headers = {
                "Authorization": f"Bearer {self.resend_api_key}",
                "Content-Type": "application/json"
            }
            
            data = {
                "from": "Nina <ninaegreja@gmail.com>",
                "to": self.recipient_email,
                "subject": f"📊 Egreja Investment AI - Relatório {datetime.now().strftime('%d/%m/%Y')}",
                "html": f"""
                <h2>📊 Relatório Diário - Egreja Investment AI</h2>
                <p>Olá Beto,</p>
                <p>Seu relatório diário foi gerado com sucesso!</p>
                <p><strong>Data:</strong> {datetime.now().strftime('%d/%m/%Y %H:%M GMT-3')}</p>
                <p>O PDF com análises, gráficos e estatísticas está em anexo.</p>
                <hr>
                <p>Nina 🦅</p>
                """,
                "attachments": [
                    {
                        "filename": f"relatorio_{datetime.now().strftime('%Y%m%d')}.pdf",
                        "content": pdf_base64,
                        "content_type": "application/pdf"
                    }
                ]
            }
            
            response = requests.post(url, json=data, headers=headers)
            
            if response.status_code == 200:
                print(f"✅ Email enviado com sucesso para {self.recipient_email}")
                return True
            else:
                print(f"❌ Erro ao enviar email: {response.text}")
                return False
        
        except Exception as e:
            print(f"❌ Erro ao enviar email: {e}")
            return False
    
    def send_telegram_alert(self, message):
        """Envia alerta via Telegram"""
        try:
            if not self.telegram_token or not self.telegram_chat_id:
                print("⚠️ Telegram não configurado")
                return False
            
            url = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
            data = {
                "chat_id": self.telegram_chat_id,
                "text": message,
                "parse_mode": "HTML"
            }
            
            response = requests.post(url, json=data)
            
            if response.status_code == 200:
                print(f"✅ Alerta Telegram enviado")
                return True
            else:
                print(f"⚠️ Erro ao enviar Telegram: {response.text}")
                return False
        
        except Exception as e:
            print(f"⚠️ Erro ao enviar Telegram: {e}")
            return False
    
    def generate_and_send_report(self):
        """Gera relatório e envia por email + Telegram"""
        try:
            print(f"\n{'='*60}")
            print(f"📊 Gerando relatório diário - {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
            print(f"{'='*60}")
            
            # Gerar PDF
            pdf_path = self.generator.generate_pdf()
            
            if not pdf_path:
                print("❌ Falha ao gerar PDF")
                return False
            
            # Enviar email
            email_sent = self.send_email_with_pdf(pdf_path)
            
            # Enviar Telegram
            telegram_msg = """
📊 <b>Relatório Diário Egreja Investment AI</b>
            
<b>Status:</b> ✅ Gerado com Sucesso
<b>Data:</b> {date}
            
O PDF foi enviado para seu email com:
• Análise de 40 ativos
• Gráficos de performance
• Estatísticas detalhadas
            
Próximo relatório: Amanhã às 08:00 GMT-3
            """.format(date=datetime.now().strftime('%d/%m/%Y %H:%M'))
            
            telegram_sent = self.send_telegram_alert(telegram_msg)
            
            if email_sent or telegram_sent:
                print(f"✅ Relatório enviado com sucesso!")
                return True
            else:
                print(f"⚠️ Relatório gerado mas falha ao enviar")
                return False
        
        except Exception as e:
            print(f"❌ Erro ao gerar relatório: {e}")
            return False
    
    def start_scheduler(self):
        """Inicia agendador que roda diariamente às 08:00"""
        try:
            # Agendar para 08:00 GMT-3 todos os dias
            schedule.every().day.at("08:00").do(self.generate_and_send_report)
            
            print("🕐 Scheduler iniciado!")
            print("📅 Relatórios serão gerados diariamente às 08:00 GMT-3")
            
            # Loop de verificação
            while True:
                schedule.run_pending()
                time.sleep(60)  # Verifica a cada minuto
        
        except Exception as e:
            print(f"❌ Erro no scheduler: {e}")

if __name__ == '__main__':
    scheduler = DailyReportScheduler()
    
    # Modo de teste (gera agora)
    if '--test' in __import__('sys').argv:
        print("🧪 Modo teste - gerando relatório agora...")
        scheduler.generate_and_send_report()
    else:
        # Modo produção (agenda para 08:00)
        print("🚀 Iniciando scheduler...")
        scheduler.start_scheduler()
