#!/usr/bin/env python3
"""
ArbitrageAI - Notificações Telegram
Sistema de notificações em tempo real via Telegram Bot
"""

import os
import logging
import requests
from typing import Optional, Dict
from dotenv import load_dotenv

logger = logging.getLogger(__name__)
load_dotenv()


class TelegramNotifier:
    """Envia notificações via Telegram Bot"""
    
    def __init__(self):
        """Inicializar notificador Telegram"""
        self.bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
        self.chat_id = os.getenv('TELEGRAM_CHAT_ID')
        
        if not self.bot_token:
            logger.warning("⚠️  TELEGRAM_BOT_TOKEN não configurado")
            self.enabled = False
        elif not self.chat_id:
            logger.warning("⚠️  TELEGRAM_CHAT_ID não configurado")
            self.enabled = False
        else:
            self.enabled = True
            logger.info("✅ Telegram Notifier ativado")
        
        self.base_url = f"https://api.telegram.org/bot{self.bot_token}"
    
    def send_message(self, text: str, parse_mode: str = 'HTML') -> bool:
        """
        Envia mensagem via Telegram
        
        Args:
            text: Texto da mensagem (suporta HTML ou Markdown)
            parse_mode: 'HTML' ou 'Markdown'
        
        Returns:
            True se enviado com sucesso, False caso contrário
        """
        if not self.enabled:
            return False
        
        try:
            url = f"{self.base_url}/sendMessage"
            payload = {
                'chat_id': self.chat_id,
                'text': text,
                'parse_mode': parse_mode
            }
            
            response = requests.post(url, json=payload, timeout=10)
            response.raise_for_status()
            
            return True
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Erro ao enviar mensagem Telegram: {e}")
            return False
        except Exception as e:
            logger.error(f"Erro inesperado ao enviar Telegram: {e}")
            return False
    
    def notify_trade_opened(self, trade: Dict) -> bool:
        """
        Notifica quando uma trade é aberta
        
        Args:
            trade: Dict com informações da trade
        """
        try:
            symbol = trade.get('symbol', 'N/A')
            recommendation = trade.get('recommendation', 'N/A')
            price = trade.get('entry_price', 0)
            score = trade.get('confidence', 0)
            trade_id = trade.get('id', 'N/A')
            
            # Emoji baseado no tipo
            emoji = '🟢' if recommendation == 'BUY' else '🔴'
            
            message = f"""
{emoji} <b>NOVA TRADE ABERTA</b>

<b>Símbolo:</b> {symbol}
<b>Tipo:</b> {recommendation}
<b>Preço Entrada:</b> ${price:,.2f}
<b>Score ML:</b> {score}%
<b>Trade ID:</b> #{trade_id}

<i>Sistema autônomo em operação</i>
"""
            
            return self.send_message(message)
            
        except Exception as e:
            logger.error(f"Erro ao notificar trade aberta: {e}")
            return False
    
    def notify_trade_closed(self, trade: Dict) -> bool:
        """
        Notifica quando uma trade é fechada
        
        Args:
            trade: Dict com informações da trade
        """
        try:
            symbol = trade.get('symbol', 'N/A')
            recommendation = trade.get('recommendation', 'N/A')
            entry_price = trade.get('entry_price', 0)
            exit_price = trade.get('exit_price', 0)
            pnl = trade.get('pnl', 0)
            pnl_pct = trade.get('pnl_percent', 0)
            close_reason = trade.get('close_reason', 'N/A')
            trade_id = trade.get('id', 'N/A')
            
            # Emoji baseado no resultado
            if pnl > 0:
                emoji = '💰'
                result = 'LUCRO'
            elif pnl < 0:
                emoji = '📉'
                result = 'PREJUÍZO'
            else:
                emoji = '➖'
                result = 'EMPATE'
            
            # Emoji para motivo do fechamento
            reason_emoji = {
                'TAKE_PROFIT': '🎯',
                'STOP_LOSS': '🛑',
                'TRAILING_STOP': '📊',
                'TIMEOUT': '⏰'
            }.get(close_reason, '❓')
            
            message = f"""
{emoji} <b>TRADE FECHADA - {result}</b>

<b>Símbolo:</b> {symbol}
<b>Tipo:</b> {recommendation}
<b>Entrada:</b> ${entry_price:,.2f}
<b>Saída:</b> ${exit_price:,.2f}
<b>P&L:</b> ${pnl:,.2f} ({pnl_pct:+.2f}%)
<b>Motivo:</b> {reason_emoji} {close_reason}
<b>Trade ID:</b> #{trade_id}
"""
            
            return self.send_message(message)
            
        except Exception as e:
            logger.error(f"Erro ao notificar trade fechada: {e}")
            return False
    
    def notify_exceptional_opportunity(self, analysis: Dict) -> bool:
        """
        Notifica quando detecta oportunidade excepcional (score >90%)
        
        Args:
            analysis: Dict com análise técnica
        """
        try:
            symbol = analysis.get('symbol', 'N/A')
            recommendation = analysis.get('recommendation', 'N/A')
            score = analysis.get('score', 0)
            price = analysis.get('current_price', 0)
            
            message = f"""
🚀 <b>OPORTUNIDADE EXCEPCIONAL!</b>

<b>Símbolo:</b> {symbol}
<b>Recomendação:</b> {recommendation}
<b>Score ML:</b> {score}% ⭐
<b>Preço Atual:</b> ${price:,.2f}

<b>Indicadores:</b>
• RSI: {analysis.get('indicators', {}).get('rsi', 'N/A')}
• MACD: {analysis.get('indicators', {}).get('macd', 'N/A')}
• EMA 9/21: {analysis.get('indicators', {}).get('ema_9', 'N/A'):.2f} / {analysis.get('indicators', {}).get('ema_21', 'N/A'):.2f}

<i>Score acima de 90% - Alta confiança!</i>
"""
            
            return self.send_message(message)
            
        except Exception as e:
            logger.error(f"Erro ao notificar oportunidade excepcional: {e}")
            return False
    
    def notify_ml_retrained(self, stats: Dict) -> bool:
        """
        Notifica quando modelo ML é retreinado
        
        Args:
            stats: Dict com estatísticas do retreinamento
        """
        try:
            trades_used = stats.get('trades_used', 0)
            accuracy = stats.get('accuracy', 0)
            
            message = f"""
🧠 <b>MODELO ML RETREINADO</b>

<b>Trades Utilizadas:</b> {trades_used}
<b>Nova Acurácia:</b> {accuracy:.1f}%

<i>Sistema de aprendizado contínuo ativo</i>
"""
            
            return self.send_message(message)
            
        except Exception as e:
            logger.error(f"Erro ao notificar retreinamento ML: {e}")
            return False
    
    def notify_system_status(self, status: Dict) -> bool:
        """
        Notifica status geral do sistema (comando /status)
        
        Args:
            status: Dict com status do sistema
        """
        try:
            open_trades = status.get('open_trades', 0)
            total_pnl = status.get('total_pnl', 0)
            win_rate = status.get('win_rate', 0)
            
            message = f"""
📊 <b>STATUS DO SISTEMA</b>

<b>Trades Abertas:</b> {open_trades}
<b>P&L Total:</b> ${total_pnl:,.2f}
<b>Taxa de Acerto:</b> {win_rate:.1f}%

<i>Sistema operando normalmente</i>
"""
            
            return self.send_message(message)
            
        except Exception as e:
            logger.error(f"Erro ao notificar status: {e}")
            return False
