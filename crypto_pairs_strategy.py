#!/usr/bin/env python3.11
"""
ArbitrageAI v2 - Estratégia Profissional de Crypto Pairs Trading
Foco: ETH/BTC com cointegração, hedge ratio e gestão de risco automática
"""

import sys
sys.path.insert(0, '/home/ubuntu/arbitrage-dashboard')

import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import json
import yfinance as yf
from scipy import stats
from statsmodels.tsa.stattools import coint, adfuller
import warnings
warnings.filterwarnings('ignore')

class CryptoPairsStrategy:
    """Estratégia profissional de Crypto Pairs Trading com ETH/BTC"""
    
    def __init__(self):
        self.symbols = ['ETH-USD', 'BTC-USD']
        self.lookback_period = 252  # 1 ano para cointegração
        self.entry_threshold = 2.0  # z-score para entrada
        self.exit_threshold = 0.5   # z-score para saída
        self.stop_loss_threshold = 3.5  # z-score para stop-loss
        self.position_size_pct = 0.20  # 20% do capital por trade
        
    def fetch_data(self, days=365):
        """Buscar dados históricos de ETH e BTC"""
        print(f"\n📊 Buscando dados históricos ({days} dias)...")
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        dfs = []
        for symbol in self.symbols:
            try:
                df = yf.download(symbol, start=start_date, end=end_date, progress=False)
                close_col = 'Adj Close' if 'Adj Close' in df.columns else 'Close'
                df_clean = df[[close_col]].copy()
                df_clean.columns = [symbol]
                dfs.append(df_clean)
                print(f"✓ {symbol}: {len(df)} dias de dados")
            except Exception as e:
                print(f"✗ Erro ao buscar {symbol}: {e}")
                return None
        
        return pd.concat(dfs, axis=1)
    
    def calculate_hedge_ratio(self, eth_prices, btc_prices):
        """Calcular hedge ratio via regressão linear"""
        # Log prices para melhor relação linear
        log_eth = np.log(eth_prices)
        log_btc = np.log(btc_prices)
        
        # Regressão: log(ETH) = alpha + beta * log(BTC)
        slope, intercept, r_value, p_value, std_err = stats.linregress(log_btc, log_eth)
        
        return slope, r_value**2, p_value
    
    def test_cointegration(self, eth_prices, btc_prices):
        """Teste de Engle-Granger para cointegração"""
        # Log prices
        log_eth = np.log(eth_prices)
        log_btc = np.log(btc_prices)
        
        # Teste de cointegração
        score, pvalue, _ = coint(log_eth, log_btc)
        
        # Teste ADF no resíduo
        spread = log_eth - (self.hedge_ratio * log_btc)
        adf_stat, adf_pvalue, _, _, _, _ = adfuller(spread)
        
        return {
            'coint_pvalue': pvalue,
            'adf_pvalue': adf_pvalue,
            'is_cointegrated': pvalue < 0.05 and adf_pvalue < 0.05
        }
    
    def calculate_spread_zscore(self, eth_prices, btc_prices):
        """Calcular z-score do spread (ETH vs BTC)"""
        log_eth = np.log(eth_prices)
        log_btc = np.log(btc_prices)
        
        # Spread = log(ETH) - hedge_ratio * log(BTC)
        spread = log_eth - (self.hedge_ratio * log_btc)
        
        # Z-score
        mean_spread = np.mean(spread)
        std_spread = np.std(spread)
        
        if std_spread == 0:
            return 0, spread, mean_spread, std_spread
        
        z_score = (spread[-1] - mean_spread) / std_spread
        
        return z_score, spread, mean_spread, std_spread
    
    def calculate_half_life(self, spread):
        """Calcular half-life do spread (tempo para reversão)"""
        # Regressão: spread(t) = alpha + beta * spread(t-1)
        spread_lag = spread[:-1]
        spread_current = spread[1:]
        
        slope, _, _, _, _ = stats.linregress(spread_lag, spread_current)
        
        if slope >= 1 or slope <= 0:
            return 0
        
        half_life = -np.log(2) / np.log(slope)
        return half_life
    
    def generate_signals(self, df):
        """Gerar sinais de trading baseado em z-score"""
        eth_prices = df['ETH-USD'].values
        btc_prices = df['BTC-USD'].values
        
        # Calcular hedge ratio
        self.hedge_ratio, r_squared, p_value = self.calculate_hedge_ratio(eth_prices, btc_prices)
        
        print(f"\n📈 Análise de Cointegração:")
        print(f"   Hedge Ratio: {self.hedge_ratio:.4f}")
        print(f"   R² (explicação): {r_squared:.4f}")
        print(f"   P-value: {p_value:.6f}")
        
        # Teste de cointegração
        coint_test = self.test_cointegration(eth_prices, btc_prices)
        print(f"\n🔗 Teste de Cointegração:")
        print(f"   Cointegrado: {'✓ SIM' if coint_test['is_cointegrated'] else '✗ NÃO'}")
        print(f"   Coint p-value: {coint_test['coint_pvalue']:.6f}")
        print(f"   ADF p-value: {coint_test['adf_pvalue']:.6f}")
        
        if not coint_test['is_cointegrated']:
            print("\n⚠️  AVISO: Pares não são cointegrados. Resultados podem ser não-confiáveis.")
        
        # Calcular spread e z-score
        z_score, spread, mean_spread, std_spread = self.calculate_spread_zscore(eth_prices, btc_prices)
        
        # Calcular half-life
        half_life = self.calculate_half_life(spread)
        
        print(f"\n📊 Análise do Spread:")
        print(f"   Z-score atual: {z_score:.4f}")
        print(f"   Média do spread: {mean_spread:.6f}")
        print(f"   Desvio padrão: {std_spread:.6f}")
        print(f"   Half-life (dias): {half_life:.1f}")
        
        # Gerar sinal
        signal = self._generate_signal(z_score)
        
        # Preços atuais
        eth_current = eth_prices[-1]
        btc_current = btc_prices[-1]
        
        return {
            'symbol': 'ETH-USD/BTC-USD',
            'signal': signal,
            'z_score': z_score,
            'hedge_ratio': self.hedge_ratio,
            'r_squared': r_squared,
            'half_life': half_life,
            'eth_price': eth_current,
            'btc_price': btc_current,
            'spread': spread[-1],
            'mean_spread': mean_spread,
            'std_spread': std_spread,
            'cointegrated': coint_test['is_cointegrated'],
            'timestamp': datetime.now().isoformat()
        }
    
    def _generate_signal(self, z_score):
        """Gerar sinal baseado em z-score"""
        if z_score < -self.entry_threshold:
            return 'LONG_ETH_SHORT_BTC'  # ETH está muito barato relativo a BTC
        elif z_score > self.entry_threshold:
            return 'SHORT_ETH_LONG_BTC'  # ETH está muito caro relativo a BTC
        elif abs(z_score) < self.exit_threshold:
            return 'CLOSE_POSITION'  # Convergência - fechar posição
        else:
            return 'HOLD'
    
    def calculate_position_size(self, capital, atr_pct=0.02):
        """Calcular tamanho da posição dinamicamente baseado em ATR"""
        # Position size = base_size * (1 - atr_pct / 5)
        base_size = self.position_size_pct
        adjusted_size = base_size * (1 - atr_pct / 5)
        
        # Mínimo 5% do capital
        adjusted_size = max(adjusted_size, 0.05)
        
        position_size = capital * adjusted_size
        
        return position_size, adjusted_size
    
    def calculate_risk_metrics(self, signal, capital=100000):
        """Calcular métricas de risco para o trade"""
        z_score = signal['z_score']
        
        # Calcular tamanho da posição
        position_size, pct = self.calculate_position_size(capital)
        
        # Calcular stop-loss
        stop_loss_z = self.stop_loss_threshold
        stop_loss_distance = (stop_loss_z - abs(z_score)) * signal['std_spread']
        
        # Calcular take-profit (z-score volta a 0)
        take_profit_distance = abs(z_score) * signal['std_spread']
        
        # Calcular risco/recompensa
        risk_reward_ratio = take_profit_distance / stop_loss_distance if stop_loss_distance > 0 else 0
        
        return {
            'position_size': position_size,
            'position_size_pct': pct,
            'stop_loss_distance': stop_loss_distance,
            'take_profit_distance': take_profit_distance,
            'risk_reward_ratio': risk_reward_ratio,
            'expected_profit': position_size * (take_profit_distance / signal['eth_price']) if signal['eth_price'] > 0 else 0
        }
    
    def run_analysis(self, capital=100000):
        """Executar análise completa"""
        print("="*70)
        print("ARBITRAGEAI V2 - CRYPTO PAIRS TRADING (ETH/BTC)")
        print("="*70)
        
        # Buscar dados
        df = self.fetch_data(days=365)
        if df is None:
            return None
        
        # Gerar sinais
        signal = self.generate_signals(df)
        
        # Calcular métricas de risco
        risk_metrics = self.calculate_risk_metrics(signal, capital)
        
        print(f"\n🎯 SINAL DE TRADING:")
        print(f"   Ação: {signal['signal']}")
        print(f"   Confiança: {'Alta ✓' if signal['cointegrated'] else 'Baixa ✗'}")
        
        print(f"\n💰 MÉTRICAS DE RISCO:")
        print(f"   Tamanho da posição: ${risk_metrics['position_size']:,.0f} ({risk_metrics['position_size_pct']*100:.1f}%)")
        print(f"   Stop-loss: {risk_metrics['stop_loss_distance']:.6f} ({risk_metrics['stop_loss_distance']/signal['eth_price']*100:.2f}%)")
        print(f"   Take-profit: {risk_metrics['take_profit_distance']:.6f} ({risk_metrics['take_profit_distance']/signal['eth_price']*100:.2f}%)")
        print(f"   Risk/Reward: {risk_metrics['risk_reward_ratio']:.2f}:1")
        print(f"   Lucro esperado: ${risk_metrics['expected_profit']:,.0f}")
        
        # Compilar resultado
        result = {
            'timestamp': datetime.now().isoformat(),
            'signal': {
                'symbol': signal['symbol'],
                'signal': signal['signal'],
                'z_score': float(signal['z_score']),
                'hedge_ratio': float(signal['hedge_ratio']),
                'r_squared': float(signal['r_squared']),
                'half_life': float(signal['half_life']),
                'eth_price': float(signal['eth_price']),
                'btc_price': float(signal['btc_price']),
                'spread': float(signal['spread']),
                'mean_spread': float(signal['mean_spread']),
                'std_spread': float(signal['std_spread']),
                'cointegrated': bool(signal['cointegrated']),
                'timestamp': signal['timestamp']
            },
            'risk_metrics': {
                'position_size': float(risk_metrics['position_size']),
                'position_size_pct': float(risk_metrics['position_size_pct']),
                'stop_loss_distance': float(risk_metrics['stop_loss_distance']),
                'take_profit_distance': float(risk_metrics['take_profit_distance']),
                'risk_reward_ratio': float(risk_metrics['risk_reward_ratio']),
                'expected_profit': float(risk_metrics['expected_profit'])
            },
            'recommendation': self._generate_recommendation(signal, risk_metrics)
        }
        
        return result
    
    def _generate_recommendation(self, signal, risk_metrics):
        """Gerar recomendação final"""
        if not signal['cointegrated']:
            return "⚠️  NÃO RECOMENDADO: Pares não cointegrados"
        
        if abs(signal['z_score']) < self.entry_threshold:
            return "⏸️  AGUARDE: Sem sinal claro"
        
        if risk_metrics['risk_reward_ratio'] < 1.5:
            return "⚠️  RISCO ALTO: Risk/Reward desfavorável"
        
        if signal['signal'] == 'LONG_ETH_SHORT_BTC':
            return f"✅ COMPRAR ETH / VENDER BTC (Z-score: {signal['z_score']:.2f})"
        elif signal['signal'] == 'SHORT_ETH_LONG_BTC':
            return f"✅ VENDER ETH / COMPRAR BTC (Z-score: {signal['z_score']:.2f})"
        else:
            return "⏸️  FECHAR POSIÇÃO: Convergência detectada"

def main():
    strategy = CryptoPairsStrategy()
    result = strategy.run_analysis(capital=100000)
    
    if result:
        # Salvar resultado em JSON
        output_file = '/home/ubuntu/arbitrage-dashboard/client/public/crypto-signal.json'
        with open(output_file, 'w') as f:
            json.dump(result, f, indent=2)
        
        print(f"\n✓ Resultado salvo em: {output_file}")
        print("\n" + "="*70)
        print(f"RECOMENDAÇÃO: {result['recommendation']}")
        print("="*70)

if __name__ == "__main__":
    main()
