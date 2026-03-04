"""
Trade Monitor Daemon - Monitora trades 24/7
Verifica trades abertos a cada 5 minutos e fecha quando atingem meta ou timeout
"""

import time
import sys
import traceback
from datetime import datetime
from real_trade_system import RealTradeSystem

def main():
    print("\n" + "="*60)
    print("🤖 TRADE MONITOR DAEMON - INICIADO")
    print("="*60)
    print(f"⏰ Hora de início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("📊 Monitorando trades a cada 5 minutos...")
    print("🛑 Pressione Ctrl+C para parar\n")
    print("="*60 + "\n")
    
    system = RealTradeSystem()
    
    cycle_count = 0
    
    try:
        while True:
            cycle_count += 1
            
            print(f"\n{'='*60}")
            print(f"🔄 CICLO #{cycle_count} - {datetime.now().strftime('%H:%M:%S')}")
            print(f"{'='*60}\n")
            
            try:
                # Monitorar trades abertos
                system.monitor_open_trades()
                print(f"\n✅ Ciclo #{cycle_count} completado com sucesso")
            except Exception as e:
                print(f"\n❌ ERRO no ciclo #{cycle_count}: {e}")
                print(f"Traceback: {traceback.format_exc()}")
            
            # Aguardar 5 minutos
            print(f"\n⏰ Próximo ciclo em 5 minutos...\n")
            sys.stdout.flush()  # Forçar flush do buffer
            time.sleep(300)  # 5 minutos
            
    except KeyboardInterrupt:
        print("\n\n" + "="*60)
        print("🛑 DAEMON INTERROMPIDO PELO USUÁRIO")
        print("="*60)
        print(f"⏰ Hora de parada: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"📊 Total de ciclos executados: {cycle_count}")
        print("="*60 + "\n")
        sys.exit(0)
    except Exception as e:
        print(f"\n\n❌ ERRO FATAL: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
