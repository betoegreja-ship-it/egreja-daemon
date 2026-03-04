"""
ArbitrageAI - Sistema de Aprendizado Melhorado
Implementa todas as melhorias sugeridas nos testes
"""

import json
import os
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional
import mysql.connector
from dotenv import load_dotenv
import requests
import numpy as np

load_dotenv()


class ImprovedLearningSystem:
    """
    Sistema de Aprendizado Melhorado
    
    Melhorias implementadas:
    1. Score mínimo reduzido de 75 para 70
    2. Filtro de tendência geral (não operar em mercados laterais)
    3. Análise de múltiplos timeframes (1h, 4h, 1d)
    4. Take-profit aumentado para 3% e stop-loss para 2%
    5. Detecção de padrões de candlestick
    """
    
    def __init__(self):
        self.db_config = self._parse_database_url()
        
        # Configurações melhoradas
        self.config = {
            'min_score': 70,  # Reduzido de 75 para 70
            'take_profit_pct': 0.03,  # Aumentado de 2.5% para 3%
            'stop_loss_pct': 0.02,  # Aumentado de 1.5% para 2%
            'max_trade_duration': 7200,  # 2 horas
            'min_trend_strength': 25,  # ADX mínimo para operar
            'require_multi_timeframe_confirmation': True,
            'max_open_trades': 3,
            'max_position_size': 0.30
        }
        
        print("🧠 Sistema de Aprendizado Melhorado inicializado")
        print(f"   Score mínimo: {self.config['min_score']}")
        print(f"   Take-profit: {self.config['take_profit_pct']*100}%")
        print(f"   Stop-loss: {self.config['stop_loss_pct']*100}%")
    
    def _parse_database_url(self) -> Dict[str, str]:
        """Parse DATABASE_URL"""
        url = os.getenv('DATABASE_URL')
        if not url:
            raise ValueError("DATABASE_URL não encontrada")
        
        parts = url.replace('mysql://', '').split('@')
        user_pass = parts[0].split(':')
        host_port_db = parts[1].split('/')
        host_port = host_port_db[0].split(':')
        db_name = host_port_db[1].split('?')[0]
        
        return {
            'host': host_port[0],
            'port': int(host_port[1]),
            'user': user_pass[0],
            'password': user_pass[1],
            'database': db_name,
            'ssl_verify_cert': True,
            'ssl_verify_identity': True
        }
    
    def _get_db_connection(self):
        """Cria conexão com banco"""
        return mysql.connector.connect(**self.db_config)
    
    def detect_market_trend(self, symbol: str) -> Dict[str, any]:
        """
        Detecta tendência geral do mercado em múltiplos timeframes
        
        Args:
            symbol: Símbolo do ativo
            
        Returns:
            Dicionário com análise de tendência
        """
        try:
            timeframes = {
                '1h': '1h',
                '4h': '4h',
                '1d': '1d'
            }
            
            trends = {}
            
            for tf_name, tf_interval in timeframes.items():
                # Buscar candles
                url = "https://api.binance.com/api/v3/klines"
                params = {
                    'symbol': symbol,
                    'interval': tf_interval,
                    'limit': 50
                }
                
                response = requests.get(url, params=params, timeout=10)
                response.raise_for_status()
                candles = response.json()
                
                closes = [float(c[4]) for c in candles]
                
                # Calcular EMA 20 e EMA 50
                ema_20 = self._calculate_ema(closes, 20)
                ema_50 = self._calculate_ema(closes, 50)
                
                current_price = closes[-1]
                
                # Determinar tendência
                if current_price > ema_20 > ema_50:
                    trend = "ALTA"
                    strength = ((current_price - ema_50) / ema_50) * 100
                elif current_price < ema_20 < ema_50:
                    trend = "BAIXA"
                    strength = ((ema_50 - current_price) / ema_50) * 100
                else:
                    trend = "LATERAL"
                    strength = 0
                
                trends[tf_name] = {
                    'trend': trend,
                    'strength': abs(strength),
                    'ema_20': ema_20,
                    'ema_50': ema_50
                }
            
            # Verificar alinhamento de timeframes
            all_trends = [t['trend'] for t in trends.values()]
            
            if all_trends.count("ALTA") >= 2:
                overall_trend = "ALTA"
                confidence = (all_trends.count("ALTA") / len(all_trends)) * 100
            elif all_trends.count("BAIXA") >= 2:
                overall_trend = "BAIXA"
                confidence = (all_trends.count("BAIXA") / len(all_trends)) * 100
            else:
                overall_trend = "LATERAL"
                confidence = 50
            
            avg_strength = np.mean([t['strength'] for t in trends.values()])
            
            return {
                'overall_trend': overall_trend,
                'confidence': confidence,
                'strength': avg_strength,
                'timeframes': trends,
                'aligned': all_trends.count(overall_trend) == len(all_trends)
            }
            
        except Exception as e:
            print(f"❌ Erro ao detectar tendência de {symbol}: {e}")
            return {
                'overall_trend': 'UNKNOWN',
                'confidence': 0,
                'strength': 0,
                'timeframes': {},
                'aligned': False
            }
    
    def detect_candlestick_patterns(self, candles: List[List]) -> List[str]:
        """
        Detecta padrões de candlestick
        
        Args:
            candles: Lista de candles [open, high, low, close]
            
        Returns:
            Lista de padrões detectados
        """
        patterns = []
        
        if len(candles) < 3:
            return patterns
        
        # Pegar últimos 3 candles
        c1 = candles[-3]
        c2 = candles[-2]
        c3 = candles[-1]
        
        # Extrair OHLC
        o1, h1, l1, c1_close = float(c1[1]), float(c1[2]), float(c1[3]), float(c1[4])
        o2, h2, l2, c2_close = float(c2[1]), float(c2[2]), float(c2[3]), float(c2[4])
        o3, h3, l3, c3_close = float(c3[1]), float(c3[2]), float(c3[3]), float(c3[4])
        
        # Doji (corpo pequeno)
        body3 = abs(c3_close - o3)
        range3 = h3 - l3
        if body3 < (range3 * 0.1):
            patterns.append("DOJI")
        
        # Hammer (martelo - bullish)
        lower_shadow = min(o3, c3_close) - l3
        upper_shadow = h3 - max(o3, c3_close)
        if lower_shadow > (body3 * 2) and upper_shadow < body3:
            patterns.append("HAMMER")
        
        # Shooting Star (estrela cadente - bearish)
        if upper_shadow > (body3 * 2) and lower_shadow < body3:
            patterns.append("SHOOTING_STAR")
        
        # Engulfing Bullish
        if c1_close < o1 and c2_close > o2 and c2_close > o1 and o2 < c1_close:
            patterns.append("BULLISH_ENGULFING")
        
        # Engulfing Bearish
        if c1_close > o1 and c2_close < o2 and c2_close < o1 and o2 > c1_close:
            patterns.append("BEARISH_ENGULFING")
        
        return patterns
    
    def should_trade(self, symbol: str, score: float, signal: str) -> Tuple[bool, str]:
        """
        Decide se deve executar trade baseado em filtros avançados
        
        Args:
            symbol: Símbolo do ativo
            score: Score da análise técnica
            signal: Sinal (BUY/SELL/HOLD)
            
        Returns:
            Tupla (should_trade, reason)
        """
        # Verificar score mínimo
        if score < self.config['min_score']:
            return (False, f"Score abaixo do mínimo ({score:.1f} < {self.config['min_score']})")
        
        # Verificar sinal
        if signal == "HOLD":
            return (False, "Sinal neutro (HOLD)")
        
        # Detectar tendência geral
        trend_analysis = self.detect_market_trend(symbol)
        
        # Não operar em mercado lateral
        if trend_analysis['overall_trend'] == "LATERAL":
            return (False, "Mercado lateral - aguardando definição de tendência")
        
        # Verificar força da tendência
        if trend_analysis['strength'] < self.config['min_trend_strength']:
            return (False, f"Tendência fraca ({trend_analysis['strength']:.1f} < {self.config['min_trend_strength']})")
        
        # Verificar alinhamento de timeframes
        if self.config['require_multi_timeframe_confirmation']:
            if not trend_analysis['aligned']:
                return (False, "Timeframes não alinhados - aguardando confirmação")
        
        # Verificar se sinal está alinhado com tendência
        if signal == "BUY" and trend_analysis['overall_trend'] != "ALTA":
            return (False, f"Sinal BUY mas tendência é {trend_analysis['overall_trend']}")
        
        if signal == "SELL" and trend_analysis['overall_trend'] != "BAIXA":
            return (False, f"Sinal SELL mas tendência é {trend_analysis['overall_trend']}")
        
        # Tudo OK - pode operar
        return (True, f"Condições favoráveis - Tendência {trend_analysis['overall_trend']} com {trend_analysis['confidence']:.1f}% confiança")
    
    def backtest_improved_strategy(
        self,
        symbol: str,
        days: int = 30
    ) -> Dict:
        """
        Executa backtest com estratégia melhorada
        
        Args:
            symbol: Símbolo do ativo
            days: Número de dias para testar
            
        Returns:
            Resultados do backtest
        """
        print(f"\n🔬 Backtest Melhorado: {symbol} ({days} dias)")
        
        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            # Buscar dados históricos
            url = "https://api.binance.com/api/v3/klines"
            params = {
                'symbol': symbol,
                'interval': '1h',
                'startTime': int(start_date.timestamp() * 1000),
                'endTime': int(end_date.timestamp() * 1000),
                'limit': 1000
            }
            
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            candles = response.json()
            
            if not candles:
                return {}
            
            # Simular trades com nova estratégia
            capital = 10000
            trades = []
            position = None
            
            for i in range(200, len(candles)):
                closes = [float(c[4]) for c in candles[:i+1]]
                current_price = closes[-1]
                timestamp = datetime.fromtimestamp(candles[i][0] / 1000)
                
                # Calcular score simplificado
                ema_20 = self._calculate_ema(closes, 20)
                ema_50 = self._calculate_ema(closes, 50)
                
                score = 50
                if current_price > ema_20 > ema_50:
                    score = 75  # Tendência de alta
                    signal = "BUY"
                elif current_price < ema_20 < ema_50:
                    score = 75  # Tendência de baixa
                    signal = "SELL"
                else:
                    signal = "HOLD"
                
                if position is None:
                    # Verificar entrada
                    should_enter, reason = self.should_trade(symbol, score, signal)
                    
                    if should_enter and signal == "BUY":
                        quantity = (capital * self.config['max_position_size']) / current_price
                        position = {
                            'entry_price': current_price,
                            'quantity': quantity,
                            'entry_time': timestamp,
                            'signal': signal
                        }
                else:
                    # Verificar saída
                    pnl_pct = ((current_price - position['entry_price']) / position['entry_price']) * 100
                    duration_hours = (timestamp - position['entry_time']).total_seconds() / 3600
                    
                    should_close = False
                    close_reason = None
                    
                    # Novos parâmetros
                    if pnl_pct >= self.config['take_profit_pct'] * 100:
                        should_close = True
                        close_reason = 'TAKE_PROFIT'
                    elif pnl_pct <= -self.config['stop_loss_pct'] * 100:
                        should_close = True
                        close_reason = 'STOP_LOSS'
                    elif duration_hours >= (self.config['max_trade_duration'] / 3600):
                        should_close = True
                        close_reason = 'TIMEOUT'
                    
                    if should_close:
                        pnl = (current_price - position['entry_price']) * position['quantity']
                        capital += pnl
                        
                        trades.append({
                            'pnl': pnl,
                            'pnl_pct': pnl_pct,
                            'close_reason': close_reason,
                            'duration_hours': duration_hours
                        })
                        
                        position = None
            
            # Calcular métricas
            if not trades:
                print("   ⚠️  Nenhum trade executado")
                return {}
            
            total_trades = len(trades)
            winning_trades = [t for t in trades if t['pnl'] > 0]
            win_rate = (len(winning_trades) / total_trades) * 100
            total_pnl = sum(t['pnl'] for t in trades)
            return_pct = ((capital - 10000) / 10000) * 100
            
            print(f"   📊 Resultados:")
            print(f"      Capital Final: ${capital:,.2f}")
            print(f"      P&L Total: ${total_pnl:+,.2f} ({return_pct:+.2f}%)")
            print(f"      Trades: {total_trades}")
            print(f"      Win Rate: {win_rate:.1f}%")
            
            return {
                'symbol': symbol,
                'final_capital': capital,
                'total_pnl': total_pnl,
                'return_pct': return_pct,
                'total_trades': total_trades,
                'win_rate': win_rate,
                'trades': trades
            }
            
        except Exception as e:
            print(f"   ❌ Erro: {e}")
            return {}
    
    def _calculate_ema(self, prices: List[float], period: int) -> float:
        """Calcula EMA"""
        if len(prices) < period:
            return prices[-1] if prices else 0
        
        prices_array = np.array(prices)
        multiplier = 2 / (period + 1)
        ema = np.mean(prices_array[:period])
        
        for price in prices_array[period:]:
            ema = (price - ema) * multiplier + ema
        
        return float(ema)


def main():
    """Teste do sistema melhorado"""
    system = ImprovedLearningSystem()
    
    print("\n" + "="*70)
    print("🧪 TESTE DO SISTEMA MELHORADO")
    print("="*70)
    
    # Testar 3 ativos
    symbols = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT']
    results = []
    
    for symbol in symbols:
        result = system.backtest_improved_strategy(symbol, days=30)
        if result:
            results.append(result)
    
    # Comparar resultados
    if results:
        print("\n" + "="*70)
        print("📊 COMPARAÇÃO DE RESULTADOS")
        print("="*70)
        
        for result in results:
            print(f"\n{result['symbol']}:")
            print(f"  Return: {result['return_pct']:+.2f}%")
            print(f"  Win Rate: {result['win_rate']:.1f}%")
            print(f"  Trades: {result['total_trades']}")
        
        best = max(results, key=lambda x: x['return_pct'])
        print(f"\n🏆 Melhor Resultado: {best['symbol']} ({best['return_pct']:+.2f}%)")
    
    print("\n" + "="*70)


if __name__ == "__main__":
    main()
