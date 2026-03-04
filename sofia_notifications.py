#!/usr/bin/env python3
"""
Sofia Notifications - Sistema de notificações inteligentes
Suporta Email, Telegram, Discord e Slack
"""

import logging
import json
import os
from datetime import datetime
from typing import Dict, List, Optional
import requests
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class NotificationManager:
    """Gerenciador de notificações multi-canal"""
    
    def __init__(self):
        self.config_file = 'data/notification_config.json'
        self.config = self._load_config()
        self.history_file = 'data/notification_history.json'
        self.history = self._load_history()
        
        logger.info("✅ Notification Manager Inicializado")
    
    def _load_config(self) -> Dict:
        """Carrega configuração de notificações"""
        try:
            if os.path.exists(self.config_file):
                with open(self.config_file, 'r') as f:
                    return json.load(f)
        except Exception as e:
            logger.warning(f"Erro ao carregar config: {e}")
        
        # Config padrão
        return {
            'email': {
                'enabled': False,
                'smtp_server': 'smtp.gmail.com',
                'smtp_port': 587,
                'sender': 'seu_email@gmail.com',
                'password': 'sua_senha',
                'recipients': []
            },
            'telegram': {
                'enabled': False,
                'bot_token': '',
                'chat_id': ''
            },
            'discord': {
                'enabled': False,
                'webhook_url': ''
            },
            'slack': {
                'enabled': False,
                'webhook_url': ''
            },
            'notification_thresholds': {
                'high_confidence': 80,  # Notificar quando confiança > 80%
                'high_profit': 100,     # Notificar quando lucro > $100
                'accuracy_improvement': 5,  # Notificar quando acurácia melhora 5%
                'win_rate_threshold': 70    # Notificar quando win rate > 70%
            }
        }
    
    def _load_history(self) -> List:
        """Carrega histórico de notificações"""
        try:
            if os.path.exists(self.history_file):
                with open(self.history_file, 'r') as f:
                    return json.load(f)
        except Exception as e:
            logger.warning(f"Erro ao carregar histórico: {e}")
        
        return []
    
    def _save_history(self):
        """Salva histórico de notificações"""
        try:
            os.makedirs(os.path.dirname(self.history_file), exist_ok=True)
            with open(self.history_file, 'w') as f:
                json.dump(self.history, f, indent=2)
        except Exception as e:
            logger.error(f"Erro ao salvar histórico: {e}")
    
    def notify_high_confidence_recommendation(self, symbol: str, recommendation: str, confidence: float, reasoning: List[str]):
        """Notifica quando Sofia gera recomendação com alta confiança"""
        
        threshold = self.config['notification_thresholds']['high_confidence']
        
        if confidence < threshold:
            return
        
        title = f"🎯 Alta Confiança: {symbol} - {recommendation}"
        message = f"""
Sofia IA detectou uma oportunidade com alta confiança!

📊 Símbolo: {symbol}
🎯 Recomendação: {recommendation}
📈 Confiança: {confidence:.1f}%

Motivos:
{chr(10).join([f"• {r}" for r in reasoning])}

⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
"""
        
        self._send_notification(title, message, 'high_confidence')
    
    def notify_profitable_trade(self, symbol: str, pnl: float, pnl_percent: float):
        """Notifica quando trade fecha com lucro"""
        
        threshold = self.config['notification_thresholds']['high_profit']
        
        if pnl < threshold:
            return
        
        title = f"💰 Trade Lucrativo: {symbol}"
        message = f"""
Um trade foi fechado com lucro!

📊 Símbolo: {symbol}
💵 P&L: ${pnl:.2f} ({pnl_percent:.2f}%)

✅ Trade bem-sucedido!

⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
"""
        
        self._send_notification(title, message, 'profitable_trade')
    
    def notify_accuracy_improvement(self, symbol: str, old_accuracy: float, new_accuracy: float):
        """Notifica quando acurácia melhora significativamente"""
        
        improvement = new_accuracy - old_accuracy
        threshold = self.config['notification_thresholds']['accuracy_improvement']
        
        if improvement < threshold:
            return
        
        title = f"📈 Sofia Melhorou: {symbol}"
        message = f"""
Sofia IA melhorou significativamente sua precisão!

📊 Símbolo: {symbol}
📊 Acurácia Anterior: {old_accuracy:.1f}%
📊 Acurácia Atual: {new_accuracy:.1f}%
📈 Melhoria: +{improvement:.1f}%

Sofia está aprendendo e ficando mais precisa!

⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
"""
        
        self._send_notification(title, message, 'accuracy_improvement')
    
    def notify_high_win_rate(self, symbol: str, win_rate: float, trades_count: int):
        """Notifica quando win rate é alto"""
        
        threshold = self.config['notification_thresholds']['win_rate_threshold']
        
        if win_rate < threshold or trades_count < 5:
            return
        
        title = f"🏆 Excelente Win Rate: {symbol}"
        message = f"""
Sofia está tendo excelentes resultados!

📊 Símbolo: {symbol}
🏆 Win Rate: {win_rate:.1f}%
📈 Trades: {trades_count}

Sofia está gerando lucros consistentes!

⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
"""
        
        self._send_notification(title, message, 'high_win_rate')
    
    def notify_daily_summary(self, summary: Dict):
        """Notifica resumo diário"""
        
        title = f"📊 Resumo Diário - {summary['date']}"
        message = f"""
Resumo de Trading do Dia

📊 Data: {summary['date']}
📈 Total de Trades: {summary['total_trades']}
💵 P&L Total: ${summary['total_pnl']:.2f}
🏆 Win Rate: {summary['win_rate']:.1f}%

Análise:
{self._analyze_daily_performance(summary)}

⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
"""
        
        self._send_notification(title, message, 'daily_summary')
    
    def _analyze_daily_performance(self, summary: Dict) -> str:
        """Analisa performance do dia"""
        
        analysis = []
        
        if summary['total_trades'] >= 10:
            analysis.append("✅ Meta de 10 trades diários atingida!")
        else:
            analysis.append(f"⚠️  Apenas {summary['total_trades']} trades (meta: 10)")
        
        if summary['total_pnl'] > 0:
            analysis.append(f"✅ Dia lucrativo: +${summary['total_pnl']:.2f}")
        elif summary['total_pnl'] < 0:
            analysis.append(f"❌ Dia com prejuízo: ${summary['total_pnl']:.2f}")
        else:
            analysis.append("➖ Dia neutro (sem lucro nem prejuízo)")
        
        if summary['win_rate'] > 60:
            analysis.append(f"🏆 Excelente win rate: {summary['win_rate']:.1f}%")
        elif summary['win_rate'] > 50:
            analysis.append(f"✅ Bom win rate: {summary['win_rate']:.1f}%")
        else:
            analysis.append(f"⚠️  Win rate baixo: {summary['win_rate']:.1f}%")
        
        return "\n".join(analysis)
    
    def _send_notification(self, title: str, message: str, notification_type: str):
        """Envia notificação em todos os canais habilitados"""
        
        # Registra no histórico
        notification = {
            'type': notification_type,
            'title': title,
            'timestamp': datetime.now().isoformat(),
            'sent_to': []
        }
        
        # Email
        if self.config['email']['enabled']:
            if self._send_email(title, message):
                notification['sent_to'].append('email')
        
        # Telegram
        if self.config['telegram']['enabled']:
            if self._send_telegram(title, message):
                notification['sent_to'].append('telegram')
        
        # Discord
        if self.config['discord']['enabled']:
            if self._send_discord(title, message):
                notification['sent_to'].append('discord')
        
        # Slack
        if self.config['slack']['enabled']:
            if self._send_slack(title, message):
                notification['sent_to'].append('slack')
        
        self.history.append(notification)
        self._save_history()
        
        logger.info(f"📨 Notificação enviada: {title} ({', '.join(notification['sent_to'])})")
    
    def _send_email(self, title: str, message: str) -> bool:
        """Envia notificação por email"""
        try:
            config = self.config['email']
            
            msg = MIMEMultipart()
            msg['From'] = config['sender']
            msg['To'] = ', '.join(config['recipients'])
            msg['Subject'] = title
            
            msg.attach(MIMEText(message, 'plain'))
            
            server = smtplib.SMTP(config['smtp_server'], config['smtp_port'])
            server.starttls()
            server.login(config['sender'], config['password'])
            server.send_message(msg)
            server.quit()
            
            return True
        except Exception as e:
            logger.error(f"Erro ao enviar email: {e}")
            return False
    
    def _send_telegram(self, title: str, message: str) -> bool:
        """Envia notificação por Telegram"""
        try:
            config = self.config['telegram']
            
            text = f"*{title}*\n\n{message}"
            
            url = f"https://api.telegram.org/bot{config['bot_token']}/sendMessage"
            data = {
                'chat_id': config['chat_id'],
                'text': text,
                'parse_mode': 'Markdown'
            }
            
            response = requests.post(url, json=data, timeout=5)
            return response.status_code == 200
        except Exception as e:
            logger.error(f"Erro ao enviar Telegram: {e}")
            return False
    
    def _send_discord(self, title: str, message: str) -> bool:
        """Envia notificação por Discord"""
        try:
            config = self.config['discord']
            
            data = {
                'embeds': [{
                    'title': title,
                    'description': message,
                    'color': 3447003,
                    'timestamp': datetime.now().isoformat()
                }]
            }
            
            response = requests.post(config['webhook_url'], json=data, timeout=5)
            return response.status_code == 204
        except Exception as e:
            logger.error(f"Erro ao enviar Discord: {e}")
            return False
    
    def _send_slack(self, title: str, message: str) -> bool:
        """Envia notificação por Slack"""
        try:
            config = self.config['slack']
            
            data = {
                'blocks': [
                    {
                        'type': 'header',
                        'text': {
                            'type': 'plain_text',
                            'text': title
                        }
                    },
                    {
                        'type': 'section',
                        'text': {
                            'type': 'mrkdwn',
                            'text': message
                        }
                    }
                ]
            }
            
            response = requests.post(config['webhook_url'], json=data, timeout=5)
            return response.status_code == 200
        except Exception as e:
            logger.error(f"Erro ao enviar Slack: {e}")
            return False
    
    def configure_email(self, smtp_server: str, smtp_port: int, sender: str, password: str, recipients: List[str]):
        """Configura notificações por email"""
        self.config['email'] = {
            'enabled': True,
            'smtp_server': smtp_server,
            'smtp_port': smtp_port,
            'sender': sender,
            'password': password,
            'recipients': recipients
        }
        self._save_config()
        logger.info("✅ Email configurado")
    
    def configure_telegram(self, bot_token: str, chat_id: str):
        """Configura notificações por Telegram"""
        self.config['telegram'] = {
            'enabled': True,
            'bot_token': bot_token,
            'chat_id': chat_id
        }
        self._save_config()
        logger.info("✅ Telegram configurado")
    
    def configure_discord(self, webhook_url: str):
        """Configura notificações por Discord"""
        self.config['discord'] = {
            'enabled': True,
            'webhook_url': webhook_url
        }
        self._save_config()
        logger.info("✅ Discord configurado")
    
    def configure_slack(self, webhook_url: str):
        """Configura notificações por Slack"""
        self.config['slack'] = {
            'enabled': True,
            'webhook_url': webhook_url
        }
        self._save_config()
        logger.info("✅ Slack configurado")
    
    def _save_config(self):
        """Salva configuração"""
        try:
            os.makedirs(os.path.dirname(self.config_file), exist_ok=True)
            with open(self.config_file, 'w') as f:
                json.dump(self.config, f, indent=2)
        except Exception as e:
            logger.error(f"Erro ao salvar config: {e}")


def main():
    """Teste do notification manager"""
    
    notif = NotificationManager()
    
    # Teste: Notificação de alta confiança
    notif.notify_high_confidence_recommendation(
        'BTCUSDT',
        'BUY',
        85.5,
        ['Tendência positiva', 'Volatilidade controlada', 'Histórico de acurácia: 100%']
    )
    
    # Teste: Notificação de trade lucrativo
    notif.notify_profitable_trade('ETHUSDT', 150.50, 2.5)
    
    # Teste: Notificação de melhoria de acurácia
    notif.notify_accuracy_improvement('BNBUSDT', 75.0, 82.5)
    
    # Teste: Notificação de high win rate
    notif.notify_high_win_rate('ADAUSDT', 75.5, 10)
    
    # Teste: Resumo diário
    notif.notify_daily_summary({
        'date': '2026-02-18',
        'total_trades': 12,
        'total_pnl': 450.75,
        'win_rate': 66.7
    })
    
    logger.info("✅ Testes de notificação concluídos")


if __name__ == "__main__":
    main()
