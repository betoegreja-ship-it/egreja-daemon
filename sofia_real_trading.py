"""
Sofia IA + Real Trading System
Integração completa: Sofia analisa → Abre trades reais → Aprende com resultados
"""

from real_trade_system import RealTradeSystem
from sofia_regenerative_ai import SofiaRegenerativeAI
from simple_market_api import SimpleMarketAPI
import time
from datetime import datetime

class SofiaRealTrading:
    def __init__(self):
        self.sofia = SofiaRegenerativeAI()
        self.market_api = SimpleMarketAPI()
        self.trade_system = RealTradeSystem()
        
        self.symbols = [
            "BTCUSDT", "ETHUSDT", "BNBUSDT", "ADAUSDT", "XRPUSDT",
            "LTCUSDT", "DOGEUSDT", "MATICUSDT", "SOLUSDT", "DOTUSDT"
        ]
    
    def run_trading_cycle(self):
        """Executar ciclo completo: Análise → Trades → Aprendizado"""
        
        print("\n" + "="*70)
        print("🧠 SOFIA IA + REAL TRADING SYSTEM")
        print("="*70)
        print(f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        
        # 1. Buscar dados reais do mercado
        print("📊 FASE 1: Buscar Dados Reais do Mercado")
        market_data = self.market_api.get_market_data()
        print(f"   ✅ {len(market_data)} símbolos obtidos\n")
        
        # 2. Sofia analisa mercado
        print("🧠 FASE 2: Sofia Analisa Mercado")
        sofia_recommendations = {}
        
        for symbol in self.symbols:
            if symbol in market_data:
                # Preparar dados no formato que Sofia espera
                symbol_data = {symbol: market_data[symbol]}
                analysis = self.sofia.analyze_market(symbol_data)
                
                if analysis and symbol in analysis:
                    rec = analysis[symbol]
                    sofia_recommendations[symbol] = {
                        "action": rec["recommendation"],
                        "confidence": rec["confidence"] / 100  # Converter para 0-1
                    }
                    
                    print(f"   {symbol}: {rec['recommendation']} (confiança: {rec['confidence']}%)")
        
        print(f"\n   ✅ {len(sofia_recommendations)} recomendações geradas\n")
        
        # 3. Monitorar trades abertos
        print("📊 FASE 3: Monitorar Trades Abertos")
        self.trade_system.monitor_open_trades()
        
        # 4. Abrir novos trades baseados em Sofia
        print("\n📈 FASE 4: Abrir Novos Trades (Sofia)")
        trades_opened = 0
        
        for symbol, rec in sofia_recommendations.items():
            if rec["action"] in ["BUY", "SELL"] and rec["confidence"] > 0.55:
                trade_id = self.trade_system.open_trade(
                    symbol,
                    rec["action"],
                    rec["confidence"]
                )
                
                if trade_id:
                    trades_opened += 1
                    
                    # Sofia aprende que abriu um trade
                    # (aprendizado real acontece quando trade fecha)
        
        print(f"\n   ✅ {trades_opened} novos trades abertos\n")
        
        # 5. Resumo
        open_trades = self.trade_system.get_open_trades()
        
        print("="*70)
        print("📊 RESUMO DO CICLO")
        print("="*70)
        print(f"🧠 Recomendações Sofia: {len(sofia_recommendations)}")
        print(f"📈 Trades Abertos: {trades_opened}")
        print(f"⏳ Trades em Andamento: {len(open_trades)}")
        print("="*70 + "\n")
        
        return {
            "recommendations": len(sofia_recommendations),
            "trades_opened": trades_opened,
            "open_trades": len(open_trades)
        }
    
    def learn_from_closed_trades(self):
        """Sofia aprende com trades fechados"""
        
        print("\n🧠 SOFIA APRENDENDO COM TRADES FECHADOS...\n")
        
        # Buscar trades fechados recentemente (últimas 24h)
        cursor = self.trade_system.db.cursor(dictionary=True)
        cursor.execute("""
            SELECT * FROM trades 
            WHERE status = 'CLOSED' 
            AND closed_at >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
            ORDER BY closed_at DESC
        """)
        
        closed_trades = cursor.fetchall()
        cursor.close()
        
        if not closed_trades:
            print("   📭 Nenhum trade fechado nas últimas 24h\n")
            return
        
        print(f"   📊 {len(closed_trades)} trades fechados para aprender\n")
        
        for trade in closed_trades:
            symbol = trade['symbol']
            pnl = float(trade['pnl']) if trade['pnl'] else 0
            
            # Determinar se foi ganho ou perda
            success = pnl > 0
            
            # Sofia já aprende automaticamente via banco de dados
            # (métricas são atualizadas quando trade fecha)
            
            emoji = "✅" if success else "❌"
            print(f"   {emoji} {symbol}: P&L ${pnl:.2f}")
        
        print(f"\n   🧠 Sofia aprendeu com {len(closed_trades)} trades!\n")


if __name__ == "__main__":
    sofia_trading = SofiaRealTrading()
    
    # Executar ciclo de trading
    result = sofia_trading.run_trading_cycle()
    
    # Aprender com trades fechados
    sofia_trading.learn_from_closed_trades()
    
    print("\n✅ Ciclo completo executado!\n")
