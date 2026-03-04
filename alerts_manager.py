#!/usr/bin/env python3
"""
Alerts Manager
Envia alertas para Telegram, Slack e Discord quando há sinais relevantes
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
        
        # Slack
        self.slack_webhook = os.getenv('SLACK_WEBHOOK_URL', '')
        
        # Discord
        self.discord_webhook = os.getenv('DISCORD_WEBHOOK_URL', '')
    
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
    
    def send_slack_alert(self, signal):
        """Envia alerta para Slack via webhook"""
        try:
            if not self.slack_webhook:
                return False
            
            # Determinar cor
            color = "#00ff00" if signal['score'] > 60 else "#ff0000" if signal['score'] < 40 else "#ffff00"
            
            payload = {
                "attachments": [
                    {
                        "color": color,
                        "title": f"{signal['symbol']} - {signal['market_type']}",
                        "text": signal['signal'],
                        "fields": [
                            {
                                "title": "Score",
                                "value": f"{signal['score']}/100",
                                "short": True
                            },
                            {
                                "title": "Preço",
                                "value": f"${signal['price']:.2f}",
                                "short": True
                            },
                            {
                                "title": "RSI",
                                "value": f"{signal['rsi']:.1f}",
                                "short": True
                            },
                            {
                                "title": "EMA9 > EMA21 > EMA50",
                                "value": f"{signal['ema9']:.2f} > {signal['ema21']:.2f} > {signal['ema50']:.2f}",
                                "short": False
                            }
                        ],
                        "footer": "Egreja Investment AI",
                        "ts": int(datetime.now().timestamp())
                    }
                ]
            }
            
            response = requests.post(self.slack_webhook, json=payload)
            return response.status_code == 200
        
        except Exception as e:
            print(f"❌ Erro Slack: {e}")
            return False
    
    def send_discord_alert(self, signal):
        """Envia alerta para Discord via webhook"""
        try:
            if not self.discord_webhook:
                return False
            
            # Emojis
            emoji = "🟢" if "COMPRA" in signal['signal'] else "🔴" if "VENDA" in signal['signal'] else "🟡"
            
            # Cor baseada no score
            color = 65280 if signal['score'] > 60 else 16711680 if signal['score'] < 40 else 16776960  # Verde, Vermelho, Amarelo
            
            embed = {
                "title": f"{emoji} {signal['symbol']} - {signal['market_type']}",
                "description": signal['signal'],
                "color": color,
                "fields": [
                    {
                        "name": "Score",
                        "value": f"{signal['score']}/100",
                        "inline": True
                    },
                    {
                        "name": "Preço",
                        "value": f"${signal['price']:.2f}",
                        "inline": True
                    },
                    {
                        "name": "RSI",
                        "value": f"{signal['rsi']:.1f}",
                        "inline": True
                    },
                    {
                        "name": "EMAs",
                        "value": f"9: {signal['ema9']:.2f}\n21: {signal['ema21']:.2f}\n50: {signal['ema50']:.2f}",
                        "inline": False
                    }
                ],
                "footer": {
                    "text": "Egreja Investment AI"
                },
                "timestamp": datetime.now().isoformat()
            }
            
            payload = {"embeds": [embed]}
            
            response = requests.post(self.discord_webhook, json=payload)
            return response.status_code in [200, 204]
        
        except Exception as e:
            print(f"❌ Erro Discord: {e}")
            return False
    
    def send_alert(self, signal):
        """Envia alerta para todos os canais configurados se score > 80 ou < 20"""
        score = signal.get('score', 0)
        
        # Só alerta se score é muito alto (compra) ou muito baixo (venda)
        if score < 20 or score > 80:
            print(f"\n🚨 ALERTA: {signal['symbol']} - Score {score}/100")
            
            # Telegram
            if self.telegram_token and self.telegram_chat_id:
                if self.send_telegram_alert(signal):
                    print(f"✅ Alerta Telegram enviado")
            
            # Slack
            if self.slack_webhook:
                if self.send_slack_alert(signal):
                    print(f"✅ Alerta Slack enviado")
            
            # Discord
            if self.discord_webhook:
                if self.send_discord_alert(signal):
                    print(f"✅ Alerta Discord enviado")

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
    
    manager = AlertsManager()
    manager.send_alert(test_signal)

if __name__ == '__main__':
    test_alerts()
