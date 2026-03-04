#!/usr/bin/env python3
"""
Voice Alerts via Twilio
Faz chamada de voz quando score > 85 ou < 15 (CRÍTICO)
"""

import os
from twilio.rest import Client
from datetime import datetime

class VoiceAlertsManager:
    def __init__(self):
        self.account_sid = os.getenv('TWILIO_ACCOUNT_SID', '')
        self.auth_token = os.getenv('TWILIO_AUTH_TOKEN', '')
        self.twilio_phone = os.getenv('TWILIO_PHONE_NUMBER', '')  # Seu número Twilio
        self.your_phone = os.getenv('YOUR_PHONE_NUMBER', '+5511948600022')  # Seu celular
        
        if self.account_sid and self.auth_token:
            self.client = Client(self.account_sid, self.auth_token)
        else:
            self.client = None
    
    def make_call(self, signal):
        """Faz chamada de voz com alerta crítico"""
        try:
            if not self.client:
                print("❌ Twilio não configurado para Voice Calls")
                return False
            
            emoji = "🟢" if "COMPRA" in signal['signal'] else "🔴"
            
            # Criar mensagem de voz (TwiML)
            twiml = f"""<?xml version="1.0" encoding="UTF-8"?>
            <Response>
                <Say language="pt-BR" voice="alice">
                    Alerta crítico de investimento.
                    {signal['symbol']}.
                    Sinal: {self.get_signal_text(signal['signal'])}.
                    Score: {signal['score']} de 100.
                    Preço atual: {self.format_price(signal['price'])}.
                    Acesse seu dashboard para ação imediata.
                </Say>
                <Pause length="2"/>
                <Say language="pt-BR" voice="alice">
                    Chamada finalizada.
                </Say>
            </Response>"""
            
            # Fazer chamada
            call = self.client.calls.create(
                to=self.your_phone,
                from_=self.twilio_phone,
                twiml=twiml
            )
            
            print(f"✅ Chamada iniciada para {self.your_phone}")
            print(f"   SID: {call.sid}")
            return True
        
        except Exception as e:
            print(f"❌ Erro ao fazer chamada: {e}")
            return False
    
    def send_whatsapp_voice_alert(self, signal):
        """Envia mensagem de voz via WhatsApp (simula com texto + áudio)"""
        try:
            if not self.client:
                return False
            
            emoji = "🟢" if "COMPRA" in signal['signal'] else "🔴"
            message_text = f"""{emoji} ALERTA CRÍTICO via WhatsApp
            
{signal['symbol']} - {signal['market_type']}
Sinal: {signal['signal']}
Score: {signal['score']}/100
Preço: ${signal['price']:.2f}

Hora: {datetime.now().strftime('%H:%M:%S')}
Action: Verifique seu dashboard AGORA!"""
            
            message = self.client.messages.create(
                from_=f"whatsapp:{os.getenv('TWILIO_WHATSAPP_NUMBER', '')}",
                to=f"whatsapp:{self.your_phone}",
                body=message_text
            )
            
            print(f"✅ Alerta WhatsApp enviado: {message.sid}")
            return True
        
        except Exception as e:
            print(f"⚠️ Erro WhatsApp: {e}")
            return False
    
    def get_signal_text(self, signal_str):
        """Converte emoji/texto do sinal para português falado"""
        if "COMPRA" in signal_str:
            return "Compra forte"
        elif "VENDA" in signal_str:
            return "Venda forte"
        else:
            return "Sinal neutro"
    
    def format_price(self, price):
        """Formata preço para leitura clara"""
        if price > 1000:
            return f"{price:,.0f}"
        else:
            return f"{price:.2f}"
    
    def send_voice_alert(self, signal):
        """Envia alerta de voz se score for CRÍTICO"""
        score = signal.get('score', 0)
        
        # Apenas para sinais MUITO críticos
        if score > 85 or score < 15:
            print(f"\n☎️ ALERTA CRÍTICO POR CHAMADA DE VOZ: {signal['symbol']} - Score {score}/100")
            
            # Fazer chamada
            if self.make_call(signal):
                # Também enviar WhatsApp
                self.send_whatsapp_voice_alert(signal)
                return True
        
        return False

# Teste
def test_voice_alerts():
    """Testa voice alerts com sinal fictício"""
    test_signal = {
        'symbol': 'AAPL',
        'market_type': 'NYSE',
        'price': 264.61,
        'score': 88,
        'signal': '🟢 COMPRA FORTE',
        'rsi': 72.5,
        'ema9': 260.50,
        'ema21': 255.30,
        'ema50': 245.80
    }
    
    print("🧪 Testando Voice Alerts...")
    print("Nota: Você receberá uma chamada de voz se Twilio estiver configurado!\n")
    
    manager = VoiceAlertsManager()
    manager.send_voice_alert(test_signal)
    
    print("\n✅ Teste concluído!")

if __name__ == '__main__':
    test_voice_alerts()
