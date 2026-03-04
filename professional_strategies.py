#!/usr/bin/env python3.11
"""
Estratégias Profissionais de Arbitragem Estatística
Implementação de Pairs Trading, Mean Reversion e Market Neutral
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from scipy import stats
from statsmodels.tsa.stattools import coint, adfuller
from data_collector import DataCollector

class ProfessionalStrategies:
    """Estratégias profissionais de arbitragem"""
    
    def __init__(self):
        self.data_collector = DataCollector()
        
        # Parâmetros de risco
        self.max_position_size = 0.20  # 20% do capital por par
        self.stop_loss_z_score = 3.5   # Stop loss em 3.5 desvios padrão
        self.take_profit_z_score = 0.5 # Take profit quando z-score < 0.5
        self.min_confidence = 0.65     # Mínimo de confiança para trade
        
    def test_cointegration(self, price1: pd.Series, price2: pd.Series) -> Dict:
        """
        Testa cointegração entre dois ativos usando teste de Engle-Granger
        
        Returns:
            Dict com p-value, hedge_ratio, is_cointegrated
        """
        # Teste de cointegração
        score, p_value, _ = coint(price1, price2)
        
        # Hedge ratio via regressão
        slope, intercept, r_value, _, _ = stats.linregress(price2, price1)
        
        # Teste de estacionariedade do spread
        spread = price1 - slope * price2
        adf_result = adfuller(spread.dropna())
        adf_p_value = adf_result[1]
        
        is_cointegrated = (p_value < 0.05) and (adf_p_value < 0.05)
        
        return {
            'coint_p_value': p_value,
            'adf_p_value': adf_p_value,
            'hedge_ratio': slope,
            'r_squared': r_value ** 2,
            'is_cointegrated': is_cointegrated,
            'confidence': 1 - min(p_value, adf_p_value)
        }
    
    def calculate_half_life(self, spread: pd.Series) -> float:
        """
        Calcula half-life de mean reversion do spread
        Half-life = tempo médio para o spread reverter 50% ao mean
        """
        spread_lag = spread.shift(1)
        spread_diff = spread - spread_lag
        
        # Regressão: spread_diff = lambda * spread_lag + epsilon
        spread_lag = spread_lag.dropna()
        spread_diff = spread_diff.dropna()
        
        # Alinhar índices
        common_idx = spread_lag.index.intersection(spread_diff.index)
        spread_lag = spread_lag[common_idx]
        spread_diff = spread_diff[common_idx]
        
        if len(spread_lag) < 10:
            return np.nan
        
        slope, _, _, _, _ = stats.linregress(spread_lag, spread_diff)
        
        if slope >= 0:
            return np.nan
        
        half_life = -np.log(2) / slope
        
        return half_life
    
    def calculate_technical_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calcula indicadores técnicos profissionais
        
        Args:
            df: DataFrame com coluna 'Close'
        
        Returns:
            DataFrame com indicadores adicionados
        """
        # RSI (Relative Strength Index)
        delta = df['Close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        df['RSI'] = 100 - (100 / (1 + rs))
        
        # Bollinger Bands
        df['SMA_20'] = df['Close'].rolling(window=20).mean()
        df['BB_std'] = df['Close'].rolling(window=20).std()
        df['BB_upper'] = df['SMA_20'] + (df['BB_std'] * 2)
        df['BB_lower'] = df['SMA_20'] - (df['BB_std'] * 2)
        df['BB_position'] = (df['Close'] - df['BB_lower']) / (df['BB_upper'] - df['BB_lower'])
        
        # MACD
        df['EMA_12'] = df['Close'].ewm(span=12, adjust=False).mean()
        df['EMA_26'] = df['Close'].ewm(span=26, adjust=False).mean()
        df['MACD'] = df['EMA_12'] - df['EMA_26']
        df['MACD_signal'] = df['MACD'].ewm(span=9, adjust=False).mean()
        df['MACD_hist'] = df['MACD'] - df['MACD_signal']
        
        # ATR (Average True Range) para volatilidade
        high_low = df['High'] - df['Low']
        high_close = np.abs(df['High'] - df['Close'].shift())
        low_close = np.abs(df['Low'] - df['Close'].shift())
        ranges = pd.concat([high_low, high_close, low_close], axis=1)
        true_range = ranges.max(axis=1)
        df['ATR'] = true_range.rolling(14).mean()
        
        # ADX (Average Directional Index) para força da tendência
        plus_dm = df['High'].diff()
        minus_dm = -df['Low'].diff()
        plus_dm[plus_dm < 0] = 0
        minus_dm[minus_dm < 0] = 0
        
        tr = true_range
        atr = tr.rolling(14).mean()
        
        plus_di = 100 * (plus_dm.rolling(14).mean() / atr)
        minus_di = 100 * (minus_dm.rolling(14).mean() / atr)
        
        dx = 100 * np.abs(plus_di - minus_di) / (plus_di + minus_di)
        df['ADX'] = dx.rolling(14).mean()
        
        return df
    
    def pairs_trading_signal(self, symbol1: str, symbol2: str, period: str = '6mo') -> Dict:
        """
        Gera sinal de Pairs Trading com cointegração
        
        Returns:
            Dict com sinal, confiança, hedge_ratio, z_score, etc.
        """
        # Buscar dados
        df1 = self.data_collector.fetch_ohlcv(symbol1, period=period, interval='1d')
        df2 = self.data_collector.fetch_ohlcv(symbol2, period=period, interval='1d')
        
        if df1.empty or df2.empty:
            return None
        
        # Alinhar séries
        df = pd.DataFrame({
            'price1': df1['Close'],
            'price2': df2['Close']
        }).dropna()
        
        if len(df) < 50:
            return None
        
        # Teste de cointegração
        coint_result = self.test_cointegration(df['price1'], df['price2'])
        
        if not coint_result['is_cointegrated']:
            return None
        
        # Calcular spread
        hedge_ratio = coint_result['hedge_ratio']
        df['spread'] = df['price1'] - hedge_ratio * df['price2']
        
        # Z-score do spread
        spread_mean = df['spread'].mean()
        spread_std = df['spread'].std()
        df['z_score'] = (df['spread'] - spread_mean) / spread_std
        
        # Half-life de reversão
        half_life = self.calculate_half_life(df['spread'])
        
        # Z-score atual
        current_z = df['z_score'].iloc[-1]
        
        # Gerar sinal
        if current_z > 2.0:
            # Spread muito alto: SHORT pair (vender symbol1, comprar symbol2)
            direction = 'SHORT_PAIR'
            confidence = min(0.95, 0.6 + abs(current_z) * 0.1)
        elif current_z < -2.0:
            # Spread muito baixo: LONG pair (comprar symbol1, vender symbol2)
            direction = 'LONG_PAIR'
            confidence = min(0.95, 0.6 + abs(current_z) * 0.1)
        else:
            direction = 'NEUTRAL'
            confidence = 0.3
        
        # Ajustar confiança baseado em cointegração
        confidence *= coint_result['confidence']
        
        # Position sizing baseado em ATR
        df1_with_indicators = self.calculate_technical_indicators(df1.copy())
        current_atr = df1_with_indicators['ATR'].iloc[-1]
        current_price = df['price1'].iloc[-1]
        atr_pct = (current_atr / current_price) * 100
        
        # Reduzir position size se volatilidade alta
        if atr_pct > 3:
            position_size = self.max_position_size * 0.5
        elif atr_pct > 2:
            position_size = self.max_position_size * 0.75
        else:
            position_size = self.max_position_size
        
        return {
            'strategy': 'pairs_trading',
            'symbol1': symbol1,
            'symbol2': symbol2,
            'direction': direction,
            'confidence': round(confidence, 3),
            'z_score': round(current_z, 3),
            'hedge_ratio': round(hedge_ratio, 4),
            'half_life': round(half_life, 2) if not np.isnan(half_life) else None,
            'r_squared': round(coint_result['r_squared'], 4),
            'coint_p_value': round(coint_result['coint_p_value'], 4),
            'position_size': round(position_size, 3),
            'stop_loss_z': self.stop_loss_z_score,
            'take_profit_z': self.take_profit_z_score,
            'current_price1': round(df['price1'].iloc[-1], 2),
            'current_price2': round(df['price2'].iloc[-1], 2),
            'timestamp': df.index[-1].isoformat()
        }
    
    def mean_reversion_signal(self, symbol: str, period: str = '3mo') -> Dict:
        """
        Gera sinal de Mean Reversion com múltiplos indicadores
        
        Returns:
            Dict com sinal, confiança, indicadores técnicos
        """
        # Buscar dados
        df = self.data_collector.fetch_ohlcv(symbol, period=period, interval='1d')
        
        if df.empty or len(df) < 50:
            return None
        
        # Calcular indicadores técnicos
        df = self.calculate_technical_indicators(df)
        
        # Valores atuais
        current_price = df['Close'].iloc[-1]
        current_rsi = df['RSI'].iloc[-1]
        current_bb_pos = df['BB_position'].iloc[-1]
        current_adx = df['ADX'].iloc[-1]
        
        # Z-score do preço
        price_mean = df['Close'].rolling(50).mean().iloc[-1]
        price_std = df['Close'].rolling(50).std().iloc[-1]
        z_score = (current_price - price_mean) / price_std
        
        # Lógica de sinal
        signals = []
        
        # RSI
        if current_rsi < 30:
            signals.append(('LONG', 0.7))
        elif current_rsi > 70:
            signals.append(('SHORT', 0.7))
        
        # Bollinger Bands
        if current_bb_pos < 0.1:  # Preço perto da banda inferior
            signals.append(('LONG', 0.6))
        elif current_bb_pos > 0.9:  # Preço perto da banda superior
            signals.append(('SHORT', 0.6))
        
        # Z-score
        if z_score < -2:
            signals.append(('LONG', 0.8))
        elif z_score > 2:
            signals.append(('SHORT', 0.8))
        
        # ADX < 25 indica mercado lateral (bom para mean reversion)
        adx_multiplier = 1.2 if current_adx < 25 else 0.8
        
        # Consolidar sinais
        if not signals:
            direction = 'NEUTRAL'
            confidence = 0.3
        else:
            # Contar votos
            long_votes = sum(conf for dir, conf in signals if dir == 'LONG')
            short_votes = sum(conf for dir, conf in signals if dir == 'SHORT')
            
            if long_votes > short_votes:
                direction = 'LONG'
                confidence = min(0.95, (long_votes / len(signals)) * adx_multiplier)
            elif short_votes > long_votes:
                direction = 'SHORT'
                confidence = min(0.95, (short_votes / len(signals)) * adx_multiplier)
            else:
                direction = 'NEUTRAL'
                confidence = 0.4
        
        # Position sizing baseado em ATR
        current_atr = df['ATR'].iloc[-1]
        atr_pct = (current_atr / current_price) * 100
        
        if atr_pct > 3:
            position_size = self.max_position_size * 0.5
        elif atr_pct > 2:
            position_size = self.max_position_size * 0.75
        else:
            position_size = self.max_position_size
        
        return {
            'strategy': 'mean_reversion',
            'symbol': symbol,
            'direction': direction,
            'confidence': round(confidence, 3),
            'z_score': round(z_score, 3),
            'rsi': round(current_rsi, 2),
            'bb_position': round(current_bb_pos, 3),
            'adx': round(current_adx, 2),
            'position_size': round(position_size, 3),
            'current_price': round(current_price, 2),
            'sma_20': round(df['SMA_20'].iloc[-1], 2),
            'atr': round(current_atr, 2),
            'timestamp': df.index[-1].isoformat()
        }
    
    def scan_all_opportunities(self) -> List[Dict]:
        """
        Escaneia todas as oportunidades de arbitragem
        
        Returns:
            Lista de sinais ordenados por confiança
        """
        print("\n" + "="*70)
        print("ESCANEAMENTO DE OPORTUNIDADES DE ARBITRAGEM")
        print("="*70)
        
        all_signals = []
        
        # 1. Pairs Trading
        print("\n1. PAIRS TRADING")
        print("-" * 70)
        
        pairs = self.data_collector.get_pair_candidates(min_correlation=0.7)
        
        for i, (sym1, sym2, corr) in enumerate(pairs[:10], 1):  # Top 10 pares
            print(f"   [{i}/10] Analisando {sym1} / {sym2}...")
            signal = self.pairs_trading_signal(sym1, sym2)
            
            if signal and signal['confidence'] >= self.min_confidence:
                all_signals.append(signal)
                print(f"         ✓ Sinal: {signal['direction']} | Confiança: {signal['confidence']:.1%}")
        
        # 2. Mean Reversion
        print("\n2. MEAN REVERSION")
        print("-" * 70)
        
        symbols = self.data_collector.get_all_symbols()
        
        for i, symbol in enumerate(symbols, 1):
            print(f"   [{i}/{len(symbols)}] Analisando {symbol}...")
            signal = self.mean_reversion_signal(symbol)
            
            if signal and signal['confidence'] >= self.min_confidence:
                all_signals.append(signal)
                print(f"         ✓ Sinal: {signal['direction']} | Confiança: {signal['confidence']:.1%}")
        
        # Ordenar por confiança
        all_signals.sort(key=lambda x: x['confidence'], reverse=True)
        
        print(f"\n{'='*70}")
        print(f"RESUMO: {len(all_signals)} oportunidades encontradas")
        print(f"{'='*70}\n")
        
        return all_signals

if __name__ == "__main__":
    strategies = ProfessionalStrategies()
    
    # Escanear oportunidades
    signals = strategies.scan_all_opportunities()
    
    # Exibir top 5
    print("\n" + "="*70)
    print("TOP 5 OPORTUNIDADES")
    print("="*70)
    
    for i, signal in enumerate(signals[:5], 1):
        print(f"\n{i}. {signal['strategy'].upper()}")
        if signal['strategy'] == 'pairs_trading':
            print(f"   Par: {signal['symbol1']} / {signal['symbol2']}")
            print(f"   Direção: {signal['direction']}")
            print(f"   Confiança: {signal['confidence']:.1%}")
            print(f"   Z-Score: {signal['z_score']:.2f}")
            print(f"   Hedge Ratio: {signal['hedge_ratio']:.4f}")
            print(f"   R²: {signal['r_squared']:.4f}")
        else:
            print(f"   Símbolo: {signal['symbol']}")
            print(f"   Direção: {signal['direction']}")
            print(f"   Confiança: {signal['confidence']:.1%}")
            print(f"   Z-Score: {signal['z_score']:.2f}")
            print(f"   RSI: {signal['rsi']:.1f}")
