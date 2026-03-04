#!/usr/bin/env python3
"""
Alert System - Sistema de Alertas em Tempo Real
Notificações para oportunidades, trades executados e eventos críticos
"""

import os
import json
import logging
from datetime import datetime
from typing import Dict, List, Optional
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class AlertSystem:
    """Sistema de alertas em tempo real"""
    
    def __init__(self, email_config: Optional[Dict] = None):
        """
        Inicializar sistema de alertas
        
        Args:
            email_config: Configuração de email (opcional)
                {
                    'smtp_server': 'smtp.gmail.com',
                    'smtp_port': 587,
                    'sender_email': 'seu_email@gmail.com',
                    'sender_password': 'sua_senha_app',
                    'recipient_email': 'seu_email@gmail.com'
                }
        """
        self.email_config = email_config
        self.alerts_log = []
        self.alerts_dir = "logs/alerts"
        
        os.makedirs(self.alerts_dir, exist_ok=True)
        
        logger.info("✅ Alert System Inicializado")
    
    def send_email_alert(self, subject: str, message: str, alert_type: str = "INFO") -> bool:
        """Enviar alerta por email"""
        if not self.email_config:
            logger.warning("⚠️ Email não configurado")
            return False
        
        try:
            # Criar mensagem
            msg = MIMEMultipart()
            msg['From'] = self.email_config['sender_email']
            msg['To'] = self.email_config['recipient_email']
            msg['Subject'] = f"[{alert_type}] {subject}"
            
            # Corpo da mensagem
            body = f"""
            <html>
                <body style="font-family: Arial, sans-serif;">
                    <h2 style="color: {'#d32f2f' if alert_type == 'ERROR' else '#1976d2'};">
                        {subject}
                    </h2>
                    <p>{message}</p>
                    <p style="color: #666; font-size: 12px;">
                        Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                    </p>
                </body>
            </html>
            """
            
            msg.attach(MIMEText(body, 'html'))
            
            # Enviar email
            with smtplib.SMTP(self.email_config['smtp_server'], self.email_config['smtp_port']) as server:
                server.starttls()
                server.login(self.email_config['sender_email'], self.email_config['sender_password'])
                server.send_message(msg)
            
            logger.info(f"✅ Email enviado: {subject}")
            return True
        
        except Exception as e:
            logger.error(f"❌ Erro ao enviar email: {e}")
            return False
    
    def send_telegram_alert(self, message: str, bot_token: Optional[str] = None, chat_id: Optional[str] = None) -> bool:
        """Enviar alerta via Telegram"""
        if not bot_token or not chat_id:
            logger.warning("⚠️ Telegram não configurado")
            return False
        
        try:
            import requests
            
            url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
            payload = {
                'chat_id': chat_id,
                'text': message,
                'parse_mode': 'HTML'
            }
            
            response = requests.post(url, json=payload, timeout=10)
            
            if response.status_code == 200:
                logger.info(f"✅ Alerta Telegram enviado")
                return True
            else:
                logger.error(f"❌ Erro Telegram: {response.text}")
                return False
        
        except Exception as e:
            logger.error(f"❌ Erro ao enviar Telegram: {e}")
            return False
    
    def alert_opportunity_found(self, symbol: str, signal: str, confidence: float, price: float) -> None:
        """Alerta de oportunidade encontrada"""
        alert = {
            'type': 'OPPORTUNITY',
            'timestamp': datetime.now().isoformat(),
            'symbol': symbol,
            'signal': signal,
            'confidence': confidence,
            'price': price
        }
        
        self.alerts_log.append(alert)
        
        message = f"""
        🎯 OPORTUNIDADE DE TRADING ENCONTRADA
        
        Ativo: {symbol}
        Sinal: {signal}
        Confiança: {confidence:.1%}
        Preço: ${price:,.2f}
        
        Ação: Sistema está preparado para executar trade automaticamente
        """
        
        logger.info(f"🎯 OPORTUNIDADE: {symbol} | {signal} | Confiança: {confidence:.1%}")
        self._save_alert(alert)
    
    def alert_trade_executed(self, symbol: str, side: str, entry_price: float, quantity: float, stop_loss: float, take_profit: float) -> None:
        """Alerta de trade executado"""
        alert = {
            'type': 'TRADE_EXECUTED',
            'timestamp': datetime.now().isoformat(),
            'symbol': symbol,
            'side': side,
            'entry_price': entry_price,
            'quantity': quantity,
            'stop_loss': stop_loss,
            'take_profit': take_profit,
            'position_value': entry_price * quantity
        }
        
        self.alerts_log.append(alert)
        
        message = f"""
        ✅ TRADE EXECUTADO
        
        Ativo: {symbol}
        Direção: {side}
        Preço de Entrada: ${entry_price:,.2f}
        Quantidade: {quantity:.4f}
        Valor da Posição: ${entry_price * quantity:,.2f}
        
        Stop Loss: ${stop_loss:,.2f}
        Take Profit: ${take_profit:,.2f}
        Risco/Recompensa: 1:{(take_profit - entry_price) / (entry_price - stop_loss):.2f}
        """
        
        logger.info(f"✅ TRADE EXECUTADO: {symbol} | {side} | ${entry_price:,.2f}")
        self._save_alert(alert)
    
    def alert_trade_closed(self, symbol: str, exit_price: float, pnl: float, pnl_pct: float, reason: str) -> None:
        """Alerta de trade fechado"""
        alert = {
            'type': 'TRADE_CLOSED',
            'timestamp': datetime.now().isoformat(),
            'symbol': symbol,
            'exit_price': exit_price,
            'pnl': pnl,
            'pnl_pct': pnl_pct,
            'reason': reason
        }
        
        self.alerts_log.append(alert)
        
        emoji = "✅" if pnl > 0 else "⚠️"
        
        message = f"""
        {emoji} TRADE FECHADO
        
        Ativo: {symbol}
        Preço de Saída: ${exit_price:,.2f}
        P&L: ${pnl:,.2f} ({pnl_pct:.2f}%)
        Razão: {reason}
        """
        
        logger.info(f"{emoji} TRADE FECHADO: {symbol} | P&L: ${pnl:,.2f} ({pnl_pct:.2f}%)")
        self._save_alert(alert)
    
    def alert_loss_limit_reached(self, daily_loss: float, max_loss: float) -> None:
        """Alerta de limite de perda diária atingido"""
        alert = {
            'type': 'LOSS_LIMIT_REACHED',
            'timestamp': datetime.now().isoformat(),
            'daily_loss': daily_loss,
            'max_loss': max_loss,
            'severity': 'CRITICAL'
        }
        
        self.alerts_log.append(alert)
        
        message = f"""
        🚨 LIMITE DE PERDA DIÁRIA ATINGIDO
        
        Perda Diária: ${daily_loss:,.2f}
        Limite Máximo: ${max_loss:,.2f}
        
        ⛔ TRADING SUSPENSO ATÉ PRÓXIMO DIA
        """
        
        logger.error(f"🚨 LIMITE DE PERDA ATINGIDO: ${daily_loss:,.2f}")
        self._save_alert(alert)
    
    def alert_position_at_risk(self, symbol: str, current_price: float, stop_loss: float, pnl_pct: float) -> None:
        """Alerta de posição em risco"""
        alert = {
            'type': 'POSITION_AT_RISK',
            'timestamp': datetime.now().isoformat(),
            'symbol': symbol,
            'current_price': current_price,
            'stop_loss': stop_loss,
            'pnl_pct': pnl_pct
        }
        
        self.alerts_log.append(alert)
        
        distance_to_sl = abs(current_price - stop_loss) / stop_loss * 100
        
        message = f"""
        ⚠️ POSIÇÃO EM RISCO
        
        Ativo: {symbol}
        Preço Atual: ${current_price:,.2f}
        Stop Loss: ${stop_loss:,.2f}
        Distância: {distance_to_sl:.2f}%
        P&L: {pnl_pct:.2f}%
        
        Monitore esta posição com atenção!
        """
        
        logger.warning(f"⚠️ POSIÇÃO EM RISCO: {symbol} | Distância SL: {distance_to_sl:.2f}%")
        self._save_alert(alert)
    
    def alert_daily_summary(self, summary: Dict) -> None:
        """Alerta com resumo diário"""
        alert = {
            'type': 'DAILY_SUMMARY',
            'timestamp': datetime.now().isoformat(),
            'summary': summary
        }
        
        self.alerts_log.append(alert)
        
        message = f"""
        📊 RESUMO DIÁRIO DE TRADING
        
        P&L Total: ${summary['total_pnl']:,.2f} ({summary['total_pnl_pct']:.2f}%)
        Trades Vencedores: {summary['win_trades']}
        Trades Perdedores: {summary['loss_trades']}
        Taxa de Acerto: {summary['win_rate']:.1f}%
        Posições Abertas: {summary['open_positions']}
        Capital Restante: ${summary['remaining_capital']:,.2f}
        """
        
        logger.info(f"📊 RESUMO: P&L ${summary['total_pnl']:,.2f} | Win Rate {summary['win_rate']:.1f}%")
        self._save_alert(alert)
    
    def alert_system_error(self, error_message: str, error_type: str = "UNKNOWN") -> None:
        """Alerta de erro do sistema"""
        alert = {
            'type': 'SYSTEM_ERROR',
            'timestamp': datetime.now().isoformat(),
            'error_type': error_type,
            'error_message': error_message,
            'severity': 'HIGH'
        }
        
        self.alerts_log.append(alert)
        
        message = f"""
        🔴 ERRO DO SISTEMA
        
        Tipo: {error_type}
        Mensagem: {error_message}
        
        ⚠️ Ação manual pode ser necessária
        """
        
        logger.error(f"🔴 ERRO: {error_type} - {error_message}")
        self._save_alert(alert)
    
    def _save_alert(self, alert: Dict) -> None:
        """Salvar alerta em arquivo JSON"""
        try:
            alert_file = f"{self.alerts_dir}/alerts_{datetime.now().strftime('%Y%m%d')}.json"
            
            # Carregar alertas existentes
            if os.path.exists(alert_file):
                with open(alert_file, 'r') as f:
                    alerts = json.load(f)
            else:
                alerts = []
            
            # Adicionar novo alerta
            alerts.append(alert)
            
            # Salvar
            with open(alert_file, 'w') as f:
                json.dump(alerts, f, indent=2, default=str)
        
        except Exception as e:
            logger.error(f"❌ Erro ao salvar alerta: {e}")
    
    def get_alerts_summary(self) -> Dict:
        """Obter resumo de alertas"""
        summary = {
            'total_alerts': len(self.alerts_log),
            'opportunities': len([a for a in self.alerts_log if a['type'] == 'OPPORTUNITY']),
            'trades_executed': len([a for a in self.alerts_log if a['type'] == 'TRADE_EXECUTED']),
            'trades_closed': len([a for a in self.alerts_log if a['type'] == 'TRADE_CLOSED']),
            'errors': len([a for a in self.alerts_log if a['type'] == 'SYSTEM_ERROR']),
            'last_alert': self.alerts_log[-1]['timestamp'] if self.alerts_log else None
        }
        
        return summary


class WebhookNotifier:
    """Notificador via Webhook (Discord, Slack, etc)"""
    
    @staticmethod
    def send_discord_webhook(webhook_url: str, message: str, title: str = "Trading Alert") -> bool:
        """Enviar alerta para Discord"""
        try:
            import requests
            
            payload = {
                'embeds': [{
                    'title': title,
                    'description': message,
                    'color': 3447003,  # Blue
                    'timestamp': datetime.now().isoformat()
                }]
            }
            
            response = requests.post(webhook_url, json=payload, timeout=10)
            
            if response.status_code == 204:
                logger.info(f"✅ Alerta Discord enviado")
                return True
            else:
                logger.error(f"❌ Erro Discord: {response.text}")
                return False
        
        except Exception as e:
            logger.error(f"❌ Erro ao enviar Discord: {e}")
            return False
    
    @staticmethod
    def send_slack_webhook(webhook_url: str, message: str, title: str = "Trading Alert") -> bool:
        """Enviar alerta para Slack"""
        try:
            import requests
            
            payload = {
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
            
            response = requests.post(webhook_url, json=payload, timeout=10)
            
            if response.status_code == 200:
                logger.info(f"✅ Alerta Slack enviado")
                return True
            else:
                logger.error(f"❌ Erro Slack: {response.text}")
                return False
        
        except Exception as e:
            logger.error(f"❌ Erro ao enviar Slack: {e}")
            return False


def main():
    """Testar sistema de alertas"""
    
    # Criar sistema de alertas
    alert_system = AlertSystem()
    
    # Simular alertas
    logger.info("\n" + "="*70)
    logger.info("TESTANDO SISTEMA DE ALERTAS")
    logger.info("="*70)
    
    # Alerta de oportunidade
    alert_system.alert_opportunity_found('BTCUSDT', 'BUY', 0.85, 45000.00)
    
    # Alerta de trade executado
    alert_system.alert_trade_executed('BTCUSDT', 'BUY', 45000.00, 0.5, 44100.00, 47250.00)
    
    # Alerta de trade fechado
    alert_system.alert_trade_closed('BTCUSDT', 45900.00, 450.00, 2.0, 'TAKE_PROFIT')
    
    # Resumo diário
    daily_summary = {
        'total_pnl': 1250.00,
        'total_pnl_pct': 0.125,
        'win_trades': 3,
        'loss_trades': 1,
        'win_rate': 75.0,
        'open_positions': 2,
        'remaining_capital': 1001250.00
    }
    alert_system.alert_daily_summary(daily_summary)
    
    # Resumo de alertas
    summary = alert_system.get_alerts_summary()
    logger.info("\n" + "="*70)
    logger.info("RESUMO DE ALERTAS")
    logger.info("="*70)
    logger.info(f"Total de Alertas: {summary['total_alerts']}")
    logger.info(f"Oportunidades: {summary['opportunities']}")
    logger.info(f"Trades Executados: {summary['trades_executed']}")
    logger.info(f"Trades Fechados: {summary['trades_closed']}")
    logger.info(f"Erros: {summary['errors']}")
    logger.info("="*70 + "\n")
    
    print("✅ SISTEMA DE ALERTAS TESTADO COM SUCESSO!")


if __name__ == "__main__":
    main()
