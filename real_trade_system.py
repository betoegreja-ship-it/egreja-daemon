"""
Sistema de Trades REAL com Cotações da Binance
Compra e venda com preços verdadeiros do mercado
"""

import requests
import mysql.connector
import os
import json
from datetime import datetime, timedelta
from typing import Dict, Optional
import time

class RealTradeSystem:
    def __init__(self):
        self.binance_base = "https://api.binance.com/api/v3"
        self.capital = 1_000_000  # US $1.000.000
        self.max_loss_percent = 0.05  # 5%
        self.max_position_percent = 0.30  # 30%
        self.profit_target = 0.025  # 2.5%
        self.stop_loss = 0.02  # 2%
        self.max_trade_duration_hours = 2
        
        # Conectar ao banco
        self.db = self._connect_db()
        
    def _connect_db(self):
        """Conectar ao banco de dados MySQL"""
        db_url = os.getenv("DATABASE_URL", "")
        
        if not db_url:
            raise Exception("DATABASE_URL não configurado")
        
        # Parse DATABASE_URL
        # mysql://user:pass@host:port/dbname?ssl-mode=REQUIRED
        import re
        match = re.match(r'mysql://([^:]+):([^@]+)@([^:]+):(\d+)/([^\?]+)', db_url)
        
        if not match:
            raise Exception(f"DATABASE_URL inválido: {db_url}")
        
        user, password, host, port, database = match.groups()
        
        return mysql.connector.connect(
            host=host,
            port=int(port),
            user=user,
            password=password,
            database=database,
            ssl_disabled=False
        )
    
    def get_real_price(self, symbol: str) -> Optional[float]:
        """Buscar preço REAL atual da Binance"""
        try:
            url = f"{self.binance_base}/ticker/price?symbol={symbol}"
            response = requests.get(url, timeout=5)
            data = response.json()
            
            if "price" in data:
                price = float(data["price"])
                print(f"  💰 {symbol}: ${price:.2f} (Binance REAL)")
                return price
            
            return None
            
        except Exception as e:
            print(f"  ⚠️ Erro ao buscar preço de {symbol}: {e}")
            return None
    
    def open_trade(self, symbol: str, action: str, confidence: float) -> Optional[int]:
        """
        Abrir trade com preço REAL da Binance
        
        Args:
            symbol: Par de trading (ex: BTCUSDT)
            action: BUY ou SELL
            confidence: Nível de confiança (0-1)
        
        Returns:
            trade_id ou None se falhar
        """
        # Buscar preço REAL
        entry_price = self.get_real_price(symbol)
        
        if not entry_price:
            print(f"  ❌ Não foi possível obter preço real de {symbol}")
            return None
        
        # Calcular quantidade baseada em 30% do capital
        position_size = self.capital * self.max_position_percent
        quantity = position_size / entry_price
        
        # Calcular preços alvo
        if action == "BUY":
            take_profit_price = entry_price * (1 + self.profit_target)
            stop_loss_price = entry_price * (1 - self.stop_loss)
        else:  # SELL
            take_profit_price = entry_price * (1 - self.profit_target)
            stop_loss_price = entry_price * (1 + self.stop_loss)
        
        # Salvar no banco
        cursor = self.db.cursor()
        
        query = """
        INSERT INTO trades (
            symbol, recommendation, entry_price, quantity,
            confidence, status, opened_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        
        values = (
            symbol,
            action,
            str(entry_price),
            str(quantity),
            int(confidence * 100),  # Converter para 0-100
            'OPEN',
            datetime.now()
        )
        
        cursor.execute(query, values)
        self.db.commit()
        
        trade_id = cursor.lastrowid
        cursor.close()
        
        print(f"\n  ✅ TRADE ABERTO #{trade_id}")
        print(f"     Símbolo: {symbol}")
        print(f"     Ação: {action}")
        print(f"     Preço de Entrada: ${entry_price:.2f} (REAL)")
        print(f"     Quantidade: {quantity:.6f}")
        print(f"     Posição: ${position_size:.2f}")
        print(f"     Take Profit: ${take_profit_price:.2f}")
        print(f"     Stop Loss: ${stop_loss_price:.2f}")
        print(f"     Confiança: {confidence*100:.1f}%\n")
        
        return trade_id
    
    def check_and_close_trade(self, trade_id: int) -> bool:
        """
        Verificar se trade deve ser fechado
        
        Condições de fechamento:
        1. Atingiu take profit
        2. Atingiu stop loss
        3. Passou 2 horas
        
        Returns:
            True se fechou, False se ainda está aberto
        """
        # Buscar trade do banco
        cursor = self.db.cursor(dictionary=True)
        cursor.execute("""
            SELECT * FROM trades 
            WHERE id = %s AND status = 'OPEN'
        """, (trade_id,))
        
        trade = cursor.fetchone()
        cursor.close()
        
        if not trade:
            return False  # Trade não existe ou já está fechado
        
        # Buscar preço REAL atual
        current_price = self.get_real_price(trade['symbol'])
        
        if not current_price:
            return False  # Não conseguiu buscar preço
        
        # Verificar tempo decorrido
        opened_at = trade['opened_at']
        time_elapsed = datetime.now() - opened_at
        hours_elapsed = time_elapsed.total_seconds() / 3600
        
        # Converter strings para float
        entry_price = float(trade['entry_price'])
        quantity = float(trade['quantity'])
        action = trade['recommendation']
        
        # Calcular P&L atual
        if action == 'BUY':
            pnl = (current_price - entry_price) * quantity
            pnl_percent = ((current_price - entry_price) / entry_price) * 100
        else:  # SELL
            pnl = (entry_price - current_price) * quantity
            pnl_percent = ((entry_price - current_price) / entry_price) * 100
        
        # Decidir se fecha
        close_reason = None
        
        # Calcular preços alvo
        if action == 'BUY':
            take_profit_price = entry_price * (1 + self.profit_target)
            stop_loss_price = entry_price * (1 - self.stop_loss)
            
            if current_price >= take_profit_price:
                close_reason = 'TAKE_PROFIT'
            elif current_price <= stop_loss_price:
                close_reason = 'STOP_LOSS'
        else:  # SELL
            take_profit_price = entry_price * (1 - self.profit_target)
            stop_loss_price = entry_price * (1 + self.stop_loss)
            
            if current_price <= take_profit_price:
                close_reason = 'TAKE_PROFIT'
            elif current_price >= stop_loss_price:
                close_reason = 'STOP_LOSS'
        
        if hours_elapsed >= self.max_trade_duration_hours:
            close_reason = 'TIMEOUT'
        
        if not close_reason:
            # Trade ainda está aberto
            print(f"  ⏳ Trade #{trade_id} ainda aberto: {hours_elapsed:.1f}h, P&L: ${pnl:.2f} ({pnl_percent:+.2f}%)")
            return False
        
        # Fechar trade
        self.close_trade(trade_id, current_price, close_reason, pnl, pnl_percent)
        return True
    
    def close_trade(self, trade_id: int, exit_price: float, close_reason: str, pnl: float, pnl_percent: float):
        """Fechar trade com preço REAL"""
        cursor = self.db.cursor()
        
        query = """
        UPDATE trades SET
            exit_price = %s,
            pnl = %s,
            pnl_percent = %s,
            close_reason = %s,
            status = 'CLOSED',
            closed_at = %s,
            duration = TIMESTAMPDIFF(MINUTE, opened_at, %s)
        WHERE id = %s
        """
        
        now = datetime.now()
        values = (str(exit_price), str(pnl), str(pnl_percent), close_reason, now, now, trade_id)
        
        cursor.execute(query, values)
        self.db.commit()
        cursor.close()
        
        emoji = "🟢" if pnl > 0 else "🔴"
        print(f"\n  {emoji} TRADE FECHADO #{trade_id}")
        print(f"     Preço de Saída: ${exit_price:.2f} (REAL)")
        print(f"     P&L: ${pnl:.2f} ({pnl_percent:+.2f}%)")
        print(f"     Razão: {close_reason}\n")
    
    def get_open_trades(self) -> list:
        """Buscar todos os trades abertos"""
        cursor = self.db.cursor(dictionary=True)
        cursor.execute("SELECT * FROM trades WHERE status = 'OPEN'")
        trades = cursor.fetchall()
        cursor.close()
        return trades
    
    def monitor_open_trades(self):
        """Monitorar e fechar trades abertos"""
        open_trades = self.get_open_trades()
        
        if not open_trades:
            print("  📭 Nenhum trade aberto")
            return
        
        print(f"\n📊 Monitorando {len(open_trades)} trades abertos...\n")
        
        for trade in open_trades:
            self.check_and_close_trade(trade['id'])
    
    def run_trading_cycle(self, symbols: list, sofia_recommendations: Dict = None):
        """
        Executar ciclo de trading REAL
        
        Args:
            symbols: Lista de símbolos para tradear
            sofia_recommendations: Recomendações de Sofia (opcional)
        """
        print("\n" + "="*60)
        print("🚀 INICIANDO CICLO DE TRADING REAL")
        print("="*60 + "\n")
        
        # 1. Monitorar trades abertos
        print("📊 FASE 1: Monitorar Trades Abertos")
        self.monitor_open_trades()
        
        # 2. Abrir novos trades
        print("\n📈 FASE 2: Abrir Novos Trades")
        
        trades_opened = 0
        
        for symbol in symbols:
            # Se temos recomendações de Sofia, usar
            if sofia_recommendations and symbol in sofia_recommendations:
                rec = sofia_recommendations[symbol]
                action = rec['action']
                confidence = rec['confidence']
                
                if action in ['BUY', 'SELL'] and confidence > 0.55:
                    trade_id = self.open_trade(symbol, action, confidence)
                    if trade_id:
                        trades_opened += 1
            else:
                # Sem Sofia, usar lógica simples
                # Por enquanto, não abrir trades sem Sofia
                pass
        
        print(f"\n✅ {trades_opened} novos trades abertos")
        
        # 3. Resumo
        open_trades = self.get_open_trades()
        print(f"\n📊 RESUMO: {len(open_trades)} trades abertos no total\n")
        
        print("="*60 + "\n")


if __name__ == "__main__":
    # Teste do sistema
    system = RealTradeSystem()
    
    # Símbolos para tradear
    symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]
    
    # Recomendações fictícias de Sofia para teste
    sofia_recs = {
        "BTCUSDT": {"action": "BUY", "confidence": 0.65},
        "ETHUSDT": {"action": "SELL", "confidence": 0.60},
    }
    
    # Executar ciclo
    system.run_trading_cycle(symbols, sofia_recs)
    
    print("\n⏰ Aguardando 10 segundos para simular passagem de tempo...\n")
    time.sleep(10)
    
    # Monitorar novamente
    system.monitor_open_trades()
