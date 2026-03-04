#!/usr/bin/env python3
"""
Alerts Manager - Telegram, Email, WhatsApp
Envia alertas quando há sinais relevantes (score > 80 ou < 20)
"""

import os
import requests
import json
from datetime import datetime

class AlertsManager:
    def __init__(self):
        # Telegram
        self.telegram_token = os.getenv('TELEGRAM_BOT_TOKEN', '')
        self.telegram_chat_id = os.getenv('TELEGRAM_CHAT_ID', '')
        
        # Email (Resend)
        self.resend_api_key = os.getenv('RESEND_API_KEY', '')
        self.email_to = 'betoegreja@gmail.com'
        
        # WhatsApp (Twilio)
        self.twilio_account_sid = os.getenv('TWILIO_ACCOUNT_SID', '')
        self.twilio_auth_token = os.getenv('TWILIO_AUTH_TOKEN', '')
        self.twilio_whatsapp_from = os.getenv('TWILIO_WHATSAPP_NUMBER', '')
        self.whatsapp_to = os.getenv('WHATSAPP_TO_NUMBER', '+5511948600022')  # Seu WhatsApp
    
    def send_telegram_alert(self, signal):
        """Envia alerta para Telegram quando score > 80 ou < 20"""
        try:
            if not self.telegram_token or not self.telegram_chat_id:
                return False
            
            # Emojis baseado no sinal
            emoji = "🟢" if "COMPRA" in signal['signal'] else "🔴" if "VENDA" in signal['signal'] else "🟡"
            
            message = f"""
{emoji} <b>{signal['symbol']}</b> - {signal['market_type']}

<b>Score:</b> {signal['score']}/100
<b>Sinal:</b> {signal['signal']}
<b>Preço:</b> ${signal['price']:.2f}
<b>RSI:</b> {signal['rsi']:.1f}
<b>EMA9:</b> {signal['ema9']:.2f}
<b>EMA21:</b> {signal['ema21']:.2f}
<b>EMA50:</b> {signal['ema50']:.2f}

Hora: {datetime.now().strftime('%H:%M:%S')}
            """
            
            url = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
            data = {
                "chat_id": self.telegram_chat_id,
                "text": message,
                "parse_mode": "HTML"
            }
            
            response = requests.post(url, json=data)
            return response.status_code == 200
        
        except Exception as e:
            print(f"❌ Erro Telegram: {e}")
            return False
    
    def send_email_alert(self, signal):
        """Envia alerta por email via Resend"""
        try:
            if not self.resend_api_key:
                return False
            
            emoji = "🟢" if "COMPRA" in signal['signal'] else "🔴" if "VENDA" in signal['signal'] else "🟡"
            
            url = "https://api.resend.com/emails"
            headers = {
                "Authorization": f"Bearer {self.resend_api_key}",
                "Content-Type": "application/json"
            }
            
            data = {
                "from": "Nina <ninaegreja@gmail.com>",
                "to": self.email_to,
                "subject": f"{emoji} {signal['symbol']} - Score {signal['score']}/100",
                "html": f"""
                <h2>{emoji} Alerta: {signal['symbol']} ({signal['market_type']})</h2>
                <p><strong>Sinal:</strong> {signal['signal']}</p>
                <p><strong>Score:</strong> {signal['score']}/100</p>
                <p><strong>Preço:</strong> ${signal['price']:.2f}</p>
                <p><strong>RSI:</strong> {signal['rsi']:.1f}</p>
                <p><strong>EMAs:</strong> 9:{signal['ema9']:.2f} > 21:{signal['ema21']:.2f} > 50:{signal['ema50']:.2f}</p>
                <hr>
                <p>Egreja Investment AI</p>
                """
            }
            
            response = requests.post(url, json=data, headers=headers)
            return response.status_code == 200
        
        except Exception as e:
            print(f"❌ Erro Email: {e}")
            return False
    
    def send_whatsapp_alert(self, signal):
        """Envia alerta via WhatsApp (Twilio)"""
        try:
            if not self.twilio_account_sid or not self.twilio_auth_token:
                return False
            
            emoji = "🟢" if "COMPRA" in signal['signal'] else "🔴" if "VENDA" in signal['signal'] else "🟡"
            
            message = f"""{emoji} {signal['symbol']} - {signal['market_type']}

Sinal: {signal['signal']}
Score: {signal['score']}/100
Preço: ${signal['price']:.2f}
RSI: {signal['rsi']:.1f}

Hora: {datetime.now().strftime('%H:%M:%S')}"""
            
            url = f"https://api.twilio.com/2010-04-01/Accounts/{self.twilio_account_sid}/Messages.json"
            auth = (self.twilio_account_sid, self.twilio_auth_token)
            
            data = {
                "From": self.twilio_whatsapp_from,
                "To": self.whatsapp_to,
                "Body": message
            }
            
            response = requests.post(url, data=data, auth=auth)
            return response.status_code == 201
        
        except Exception as e:
            print(f"❌ Erro WhatsApp: {e}")
            return False
    
    def send_alert(self, signal):
        """Envia alerta para Telegram, Email e WhatsApp se score > 80 ou < 20"""
        score = signal.get('score', 0)
        
        # Só alerta se score é muito alto (compra) ou muito baixo (venda)
        if score < 20 or score > 80:
            print(f"\n🚨 ALERTA: {signal['symbol']} - Score {score}/100")
            
            # Telegram (prioritário)
            if self.telegram_token and self.telegram_chat_id:
                if self.send_telegram_alert(signal):
                    print(f"✅ Alerta Telegram enviado")
            
            # Email
            if self.resend_api_key:
                if self.send_email_alert(signal):
                    print(f"✅ Alerta Email enviado")
            
            # WhatsApp
            if self.twilio_account_sid and self.twilio_auth_token:
                if self.send_whatsapp_alert(signal):
                    print(f"✅ Alerta WhatsApp enviado")

# Função para testar
def test_alerts():
    """Testa alertas com um sinal fictício"""
    test_signal = {
        'symbol': 'AAPL',
        'market_type': 'NYSE',
        'price': 264.61,
        'score': 85,
        'signal': '🟢 COMPRA FORTE',
        'rsi': 72.5,
        'ema9': 260.50,
        'ema21': 255.30,
        'ema50': 245.80
    }
    
    print("🧪 Testando alertas...")
    print("Canais disponíveis: Telegram, Email, WhatsApp\n")
    
    manager = AlertsManager()
    manager.send_alert(test_signal)
    
    print("\n✅ Teste concluído!")

if __name__ == '__main__':
    test_alerts()
