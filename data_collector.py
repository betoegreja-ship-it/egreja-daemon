#!/usr/bin/env python3.11
"""
Módulo de Coleta de Dados Reais
Integração com Yahoo Finance para dados confiáveis e validados
"""

import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Tuple
import json
import os

class DataCollector:
    """Coletor de dados de mercado com validação e cache"""
    
    def __init__(self, cache_dir: str = '/home/ubuntu/market_data_cache'):
        self.cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)
        
        # Universo de ativos para arbitragem
        self.crypto_pairs = ['BTC-USD', 'ETH-USD']
        self.etf_pairs = [
            ('SPY', 'QQQ'),  # S&P 500 vs NASDAQ
            ('XLF', 'XLE'),  # Financials vs Energy
            ('GLD', 'SLV'),  # Gold vs Silver
        ]
        self.stock_pairs = [
            ('KO', 'PEP'),   # Coca-Cola vs Pepsi
            ('BA', 'LMT'),   # Boeing vs Lockheed Martin
            ('JPM', 'BAC'),  # JPMorgan vs Bank of America
        ]
        
    def get_cache_path(self, symbol: str, period: str) -> str:
        """Retorna caminho do arquivo de cache"""
        return f"{self.cache_dir}/{symbol}_{period}.csv"
    
    def is_cache_valid(self, cache_path: str, max_age_hours: int = 1) -> bool:
        """Verifica se cache é válido"""
        if not os.path.exists(cache_path):
            return False
        
        file_time = datetime.fromtimestamp(os.path.getmtime(cache_path))
        age = datetime.now() - file_time
        
        return age.total_seconds() < (max_age_hours * 3600)
    
    def fetch_ohlcv(self, symbol: str, period: str = '1y', interval: str = '1h') -> pd.DataFrame:
        """
        Busca dados OHLCV com cache
        
        Args:
            symbol: Símbolo do ativo (ex: 'AAPL', 'BTC-USD')
            period: Período de dados ('1d', '5d', '1mo', '3mo', '6mo', '1y', '2y', '5y')
            interval: Intervalo ('1m', '2m', '5m', '15m', '30m', '60m', '1h', '1d', '1wk', '1mo')
        
        Returns:
            DataFrame com colunas: Open, High, Low, Close, Volume, Adj Close
        """
        cache_path = self.get_cache_path(symbol, f"{period}_{interval}")
        
        # Usar cache se válido
        if self.is_cache_valid(cache_path):
            print(f"✓ Usando cache para {symbol}")
            return pd.read_csv(cache_path, index_col=0, parse_dates=True)
        
        # Buscar dados novos
        print(f"⬇ Baixando dados para {symbol}...")
        try:
            ticker = yf.Ticker(symbol)
            df = ticker.history(period=period, interval=interval)
            
            if df.empty:
                print(f"⚠️ Sem dados para {symbol}")
                return pd.DataFrame()
            
            # Salvar em cache
            df.to_csv(cache_path)
            print(f"✓ Dados salvos em cache: {len(df)} registros")
            
            return df
            
        except Exception as e:
            print(f"❌ Erro ao buscar {symbol}: {e}")
            return pd.DataFrame()
    
    def get_ticker_info(self, symbol: str) -> Dict:
        """Busca informações fundamentais do ticker"""
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info
            
            return {
                'symbol': symbol,
                'name': info.get('longName', symbol),
                'sector': info.get('sector', 'Unknown'),
                'industry': info.get('industry', 'Unknown'),
                'market_cap': info.get('marketCap', 0),
                'beta': info.get('beta', 1.0),
                'pe_ratio': info.get('trailingPE', None),
                'dividend_yield': info.get('dividendYield', 0),
            }
        except Exception as e:
            print(f"⚠️ Erro ao buscar info de {symbol}: {e}")
            return {'symbol': symbol, 'name': symbol}
    
    def validate_data_quality(self, df: pd.DataFrame, symbol: str) -> Tuple[bool, List[str]]:
        """
        Valida qualidade dos dados
        
        Returns:
            (is_valid, issues_list)
        """
        issues = []
        
        if df.empty:
            return False, ['DataFrame vazio']
        
        # Verificar colunas necessárias
        required_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            issues.append(f"Colunas faltando: {missing_cols}")
        
        # Verificar valores nulos
        null_pct = (df[required_cols].isnull().sum() / len(df) * 100)
        high_null_cols = null_pct[null_pct > 5].index.tolist()
        if high_null_cols:
            issues.append(f"Muitos valores nulos em: {high_null_cols}")
        
        # Verificar valores negativos
        for col in ['Open', 'High', 'Low', 'Close', 'Volume']:
            if col in df.columns and (df[col] < 0).any():
                issues.append(f"Valores negativos em {col}")
        
        # Verificar consistência OHLC
        if all(col in df.columns for col in ['Open', 'High', 'Low', 'Close']):
            invalid_ohlc = (
                (df['High'] < df['Low']) |
                (df['High'] < df['Open']) |
                (df['High'] < df['Close']) |
                (df['Low'] > df['Open']) |
                (df['Low'] > df['Close'])
            ).sum()
            
            if invalid_ohlc > 0:
                issues.append(f"{invalid_ohlc} registros com OHLC inconsistente")
        
        # Verificar gaps extremos (> 50% em um período)
        if 'Close' in df.columns:
            returns = df['Close'].pct_change().abs()
            extreme_gaps = (returns > 0.5).sum()
            if extreme_gaps > len(df) * 0.01:  # Mais de 1% dos dados
                issues.append(f"{extreme_gaps} gaps extremos detectados")
        
        is_valid = len(issues) == 0
        
        return is_valid, issues
    
    def get_all_symbols(self) -> List[str]:
        """Retorna lista de todos os símbolos"""
        symbols = self.crypto_pairs.copy()
        
        for pair in self.etf_pairs + self.stock_pairs:
            symbols.extend(pair)
        
        return list(set(symbols))
    
    def get_pair_candidates(self, min_correlation: float = 0.7) -> List[Tuple[str, str, float]]:
        """
        Identifica pares candidatos para arbitragem baseado em correlação
        
        Returns:
            Lista de tuplas (symbol1, symbol2, correlation)
        """
        print("\n" + "="*60)
        print("IDENTIFICANDO PARES PARA ARBITRAGEM")
        print("="*60)
        
        symbols = self.get_all_symbols()
        candidates = []
        
        # Buscar dados para todos os símbolos
        data = {}
        for symbol in symbols:
            df = self.fetch_ohlcv(symbol, period='6mo', interval='1d')
            if not df.empty and 'Close' in df.columns:
                data[symbol] = df['Close']
        
        # Calcular correlações entre todos os pares
        print(f"\nAnalisando correlações entre {len(data)} ativos...")
        
        symbols_list = list(data.keys())
        for i in range(len(symbols_list)):
            for j in range(i + 1, len(symbols_list)):
                sym1, sym2 = symbols_list[i], symbols_list[j]
                
                # Alinhar séries temporais
                df_combined = pd.DataFrame({
                    sym1: data[sym1],
                    sym2: data[sym2]
                }).dropna()
                
                if len(df_combined) < 50:  # Mínimo de dados
                    continue
                
                # Calcular correlação
                corr = df_combined[sym1].corr(df_combined[sym2])
                
                if abs(corr) >= min_correlation:
                    candidates.append((sym1, sym2, corr))
        
        # Ordenar por correlação absoluta
        candidates.sort(key=lambda x: abs(x[2]), reverse=True)
        
        print(f"\n✓ {len(candidates)} pares candidatos encontrados (correlação >= {min_correlation})")
        
        return candidates
    
    def calculate_spread(self, symbol1: str, symbol2: str, period: str = '6mo') -> pd.DataFrame:
        """
        Calcula spread entre dois ativos
        
        Returns:
            DataFrame com colunas: price1, price2, spread, z_score
        """
        # Buscar dados
        df1 = self.fetch_ohlcv(symbol1, period=period, interval='1d')
        df2 = self.fetch_ohlcv(symbol2, period=period, interval='1d')
        
        if df1.empty or df2.empty:
            return pd.DataFrame()
        
        # Alinhar séries
        df = pd.DataFrame({
            'price1': df1['Close'],
            'price2': df2['Close']
        }).dropna()
        
        # Calcular spread em log-prices
        df['log_price1'] = np.log(df['price1'])
        df['log_price2'] = np.log(df['price2'])
        
        # Hedge ratio via regressão linear
        from scipy import stats
        slope, intercept, r_value, p_value, std_err = stats.linregress(
            df['log_price2'], df['log_price1']
        )
        
        # Spread = log(price1) - beta * log(price2)
        df['spread'] = df['log_price1'] - slope * df['log_price2']
        
        # Z-score do spread
        df['z_score'] = (df['spread'] - df['spread'].mean()) / df['spread'].std()
        
        # Adicionar metadados
        df.attrs['hedge_ratio'] = slope
        df.attrs['r_squared'] = r_value ** 2
        df.attrs['p_value'] = p_value
        
        return df

if __name__ == "__main__":
    collector = DataCollector()
    
    print("="*60)
    print("TESTE DO COLETOR DE DADOS")
    print("="*60)
    
    # Teste 1: Buscar dados individuais
    print("\n1. Buscando dados de BTC-USD...")
    btc_data = collector.fetch_ohlcv('BTC-USD', period='1mo', interval='1h')
    print(f"   Registros: {len(btc_data)}")
    
    if not btc_data.empty:
        is_valid, issues = collector.validate_data_quality(btc_data, 'BTC-USD')
        print(f"   Qualidade: {'✓ Válido' if is_valid else '⚠️ Problemas'}")
        if issues:
            for issue in issues:
                print(f"     - {issue}")
    
    # Teste 2: Identificar pares
    print("\n2. Identificando pares candidatos...")
    pairs = collector.get_pair_candidates(min_correlation=0.7)
    
    print(f"\nTop 5 pares por correlação:")
    for sym1, sym2, corr in pairs[:5]:
        print(f"   {sym1} <-> {sym2}: {corr:.3f}")
    
    # Teste 3: Calcular spread
    if pairs:
        sym1, sym2, _ = pairs[0]
        print(f"\n3. Calculando spread para {sym1} / {sym2}...")
        spread_df = collector.calculate_spread(sym1, sym2)
        
        if not spread_df.empty:
            print(f"   Registros: {len(spread_df)}")
            print(f"   Hedge Ratio: {spread_df.attrs['hedge_ratio']:.4f}")
            print(f"   R²: {spread_df.attrs['r_squared']:.4f}")
            print(f"   Z-score atual: {spread_df['z_score'].iloc[-1]:.2f}")
