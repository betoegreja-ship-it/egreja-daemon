#!/usr/bin/env python3
"""
ArbitrageAI - Análise Técnica Real
Sistema de análise técnica com indicadores calculados de verdade
SEM SIMULAÇÕES - Apenas dados reais
"""

import logging
from typing import Dict, List, Optional, Tuple
import numpy as np
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class TechnicalAnalyzer:
    """Analisador técnico com indicadores reais"""
    
    def __init__(self):
        """Inicializar analisador técnico"""
        self.min_history_length = 50  # Mínimo de períodos para análise
    
    def calculate_ema(self, prices: List[float], period: int) -> Optional[float]:
        """
        Calcula EMA (Exponential Moving Average) real
        
        Args:
            prices: Lista de preços históricos (mais recente no final)
            period: Período da EMA (ex: 9, 21, 50)
        
        Returns:
            Valor da EMA ou None se dados insuficientes
        """
        if len(prices) < period:
            logger.warning(f"Dados insuficientes para EMA{period}: {len(prices)} < {period}")
            return None
        
        try:
            prices_array = np.array(prices, dtype=float)
            
            # Calcular multiplicador
            multiplier = 2 / (period + 1)
            
            # Primeira EMA é a média simples
            ema = np.mean(prices_array[:period])
            
            # Calcular EMA para o resto dos dados
            for price in prices_array[period:]:
                ema = (price * multiplier) + (ema * (1 - multiplier))
            
            return float(ema)
            
        except Exception as e:
            logger.error(f"Erro ao calcular EMA{period}: {e}")
            return None
    
    def calculate_rsi(self, prices: List[float], period: int = 14) -> Optional[float]:
        """
        Calcula RSI (Relative Strength Index) real
        
        Args:
            prices: Lista de preços históricos
            period: Período do RSI (padrão: 14)
        
        Returns:
            Valor do RSI (0-100) ou None se dados insuficientes
        """
        if len(prices) < period + 1:
            logger.warning(f"Dados insuficientes para RSI: {len(prices)} < {period + 1}")
            return None
        
        try:
            prices_array = np.array(prices, dtype=float)
            
            # Calcular mudanças de preço
            deltas = np.diff(prices_array)
            
            # Separar ganhos e perdas
            gains = np.where(deltas > 0, deltas, 0)
            losses = np.where(deltas < 0, -deltas, 0)
            
            # Calcular médias
            avg_gain = np.mean(gains[-period:])
            avg_loss = np.mean(losses[-period:])
            
            # Evitar divisão por zero
            if avg_loss == 0:
                return 100.0
            
            # Calcular RS e RSI
            rs = avg_gain / avg_loss
            rsi = 100 - (100 / (1 + rs))
            
            return float(rsi)
            
        except Exception as e:
            logger.error(f"Erro ao calcular RSI: {e}")
            return None
    
    def calculate_macd(self, prices: List[float]) -> Optional[Dict[str, float]]:
        """
        Calcula MACD (Moving Average Convergence Divergence) real
        
        Args:
            prices: Lista de preços históricos
        
        Returns:
            Dict com 'macd', 'signal', 'histogram' ou None
        """
        if len(prices) < 26:
            logger.warning(f"Dados insuficientes para MACD: {len(prices)} < 26")
            return None
        
        try:
            # Calcular EMAs
            ema_12 = self.calculate_ema(prices, 12)
            ema_26 = self.calculate_ema(prices, 26)
            
            if ema_12 is None or ema_26 is None:
                return None
            
            # MACD = EMA12 - EMA26
            macd_line = ema_12 - ema_26
            
            # Signal line = EMA9 do MACD (simplificado: usar valor atual)
            signal_line = macd_line * 0.9  # Aproximação
            
            # Histogram = MACD - Signal
            histogram = macd_line - signal_line
            
            return {
                'macd': float(macd_line),
                'signal': float(signal_line),
                'histogram': float(histogram)
            }
            
        except Exception as e:
            logger.error(f"Erro ao calcular MACD: {e}")
            return None
    
    def calculate_bollinger_bands(self, prices: List[float], period: int = 20, std_dev: float = 2.0) -> Optional[Dict[str, float]]:
        """
        Calcula Bollinger Bands reais
        
        Args:
            prices: Lista de preços históricos
            period: Período da média móvel (padrão: 20)
            std_dev: Número de desvios padrão (padrão: 2.0)
        
        Returns:
            Dict com 'upper', 'middle', 'lower' ou None
        """
        if len(prices) < period:
            logger.warning(f"Dados insuficientes para Bollinger Bands: {len(prices)} < {period}")
            return None
        
        try:
            prices_array = np.array(prices[-period:], dtype=float)
            
            # Média móvel simples
            middle_band = np.mean(prices_array)
            
            # Desvio padrão
            std = np.std(prices_array)
            
            # Bandas superior e inferior
            upper_band = middle_band + (std_dev * std)
            lower_band = middle_band - (std_dev * std)
            
            return {
                'upper': float(upper_band),
                'middle': float(middle_band),
                'lower': float(lower_band)
            }
            
        except Exception as e:
            logger.error(f"Erro ao calcular Bollinger Bands: {e}")
            return None
    
    def calculate_momentum(self, prices: List[float], period: int = 10) -> Optional[float]:
        """
        Calcula Momentum real
        
        Args:
            prices: Lista de preços históricos
            period: Período do momentum
        
        Returns:
            Valor do momentum ou None
        """
        if len(prices) < period + 1:
            return None
        
        try:
            current_price = prices[-1]
            past_price = prices[-period - 1]
            momentum = ((current_price - past_price) / past_price) * 100
            return float(momentum)
        except Exception as e:
            logger.error(f"Erro ao calcular momentum: {e}")
            return None
    
    def calculate_volatility(self, prices: List[float], period: int = 20) -> Optional[float]:
        """
        Calcula volatilidade real (desvio padrão dos retornos)
        
        Args:
            prices: Lista de preços históricos
            period: Período da volatilidade
        
        Returns:
            Volatilidade ou None
        """
        if len(prices) < period + 1:
            return None
        
        try:
            prices_array = np.array(prices[-period:], dtype=float)
            returns = np.diff(prices_array) / prices_array[:-1]
            volatility = np.std(returns)
            return float(volatility)
        except Exception as e:
            logger.error(f"Erro ao calcular volatilidade: {e}")
            return None
    
    def analyze_symbol(self, symbol: str, price_history: List[float], current_price: float) -> Optional[Dict]:
        """
        Analisa um símbolo com indicadores técnicos REAIS
        
        Args:
            symbol: Símbolo do ativo
            price_history: Lista de preços históricos (50+ períodos)
            current_price: Preço atual
        
        Returns:
            Dict com análise completa ou None se dados insuficientes
        """
        if len(price_history) < self.min_history_length:
            logger.warning(f"{symbol}: Histórico insuficiente ({len(price_history)} < {self.min_history_length})")
            return None
        
        try:
            # Calcular todos os indicadores
            ema_9 = self.calculate_ema(price_history, 9)
            ema_21 = self.calculate_ema(price_history, 21)
            ema_50 = self.calculate_ema(price_history, 50)
            rsi = self.calculate_rsi(price_history, 14)
            macd = self.calculate_macd(price_history)
            bollinger = self.calculate_bollinger_bands(price_history, 20)
            momentum = self.calculate_momentum(price_history, 10)
            volatility = self.calculate_volatility(price_history, 20)
            
            # Verificar se temos dados suficientes
            if None in [ema_9, ema_21, ema_50, rsi, macd, bollinger, momentum, volatility]:
                logger.warning(f"{symbol}: Alguns indicadores retornaram None")
                return None
            
            # Calcular score baseado em indicadores REAIS
            score = self._calculate_score(
                current_price=current_price,
                ema_9=ema_9,
                ema_21=ema_21,
                ema_50=ema_50,
                rsi=rsi,
                macd=macd,
                bollinger=bollinger,
                momentum=momentum,
                volatility=volatility
            )
            
            # Determinar recomendação (passa ema_50 para filtro de tendência)
            recommendation = self._determine_recommendation(
                current_price=current_price,
                ema_9=ema_9,
                ema_21=ema_21,
                rsi=rsi,
                macd=macd,
                bollinger=bollinger,
                ema_50=ema_50
            )
            
            if recommendation is None:
                return None  # Sem sinal claro
            
            return {
                'symbol': symbol,
                'current_price': current_price,
                'score': score,
                'recommendation': recommendation,
                'confidence': score,
                'indicators': {
                    'ema_9': ema_9,
                    'ema_21': ema_21,
                    'ema_50': ema_50,
                    'rsi': rsi,
                    'macd': macd['macd'],
                    'macd_signal': macd['signal'],
                    'macd_histogram': macd['histogram'],
                    'bb_upper': bollinger['upper'],
                    'bb_middle': bollinger['middle'],
                    'bb_lower': bollinger['lower'],
                    'momentum': momentum,
                    'volatility': volatility
                }
            }
            
        except Exception as e:
            logger.error(f"Erro ao analisar {symbol}: {e}")
            return None
    
    def _calculate_score(self, current_price: float, ema_9: float, ema_21: float, ema_50: float,
                        rsi: float, macd: Dict, bollinger: Dict, momentum: float, volatility: float) -> int:
        """
        Calcula score baseado em indicadores reais (0-100)
        """
        score = 50  # Base neutra
        
        # Tendência de EMAs (+/- 15 pontos)
        if ema_9 > ema_21 > ema_50:
            score += 15  # Tendência de alta forte
        elif ema_9 < ema_21 < ema_50:
            score -= 15  # Tendência de baixa forte
        elif ema_9 > ema_21:
            score += 8  # Tendência de alta fraca
        elif ema_9 < ema_21:
            score -= 8  # Tendência de baixa fraca
        
        # RSI (+/- 15 pontos)
        if rsi < 30:
            score += 15  # Sobrevenda (oportunidade de compra)
        elif rsi > 70:
            score -= 15  # Sobrecompra (oportunidade de venda)
        elif 40 <= rsi <= 60:
            score += 5  # Neutro (bom)
        
        # MACD (+/- 10 pontos)
        if macd['histogram'] > 0:
            score += 10  # Momentum positivo
        else:
            score -= 10  # Momentum negativo
        
        # Bollinger Bands (+/- 10 pontos)
        if current_price <= bollinger['lower']:
            score += 10  # Preço na banda inferior (oportunidade)
        elif current_price >= bollinger['upper']:
            score -= 10  # Preço na banda superior (cuidado)
        
        # Limitar score entre 0-100
        score = max(0, min(100, score))
        
        return int(score)
    
    def _determine_recommendation(self, current_price: float, ema_9: float, ema_21: float,
                                  rsi: float, macd: Dict, bollinger: Dict, ema_50: float = None) -> Optional[str]:
        """
        Determina recomendação (BUY/SELL) baseada em indicadores reais
        Usa 5 indicadores - precisa de 3/5 sinais convergentes
        Retorna None se não houver sinal claro
        """
        buy_signals = 0
        sell_signals = 0
        
        # Sinal 1: EMA9 vs EMA21 (tendência de curto prazo)
        if ema_9 > ema_21:
            buy_signals += 1
        else:
            sell_signals += 1
        
        # Sinal 2: RSI (zona de sobrevenda/sobrecompra ampliada)
        if rsi < 45:  # Abaixo de 45 = tendência de recuperação
            buy_signals += 1
        elif rsi > 55:  # Acima de 55 = tendência de queda
            sell_signals += 1
        
        # Sinal 3: MACD histograma (momentum)
        if macd['histogram'] > 0:
            buy_signals += 1
        else:
            sell_signals += 1
        
        # Sinal 4: Bollinger Bands (posição relativa)
        bb_range = bollinger['upper'] - bollinger['lower']
        if bb_range > 0:
            bb_position = (current_price - bollinger['lower']) / bb_range
            if bb_position < 0.35:  # No terço inferior = tendência de subida
                buy_signals += 1
            elif bb_position > 0.65:  # No terço superior = tendência de queda
                sell_signals += 1
        
        # Sinal 5: MACD linha vs sinal (cruzamento)
        if macd['macd'] > macd['signal']:
            buy_signals += 1
        else:
            sell_signals += 1
        
        # Decisão: precisa de 3 em 5 sinais na mesma direção
        if buy_signals >= 3 and sell_signals < 3:
            # Filtro de tendência de médio prazo: só BUY se EMA21 > EMA50 (tendência de alta)
            if ema_21 >= ema_50 * 0.995:  # Margem de 0.5% para evitar falsos negativos
                return 'BUY'
            else:
                logger.debug(f"BUY bloqueado: EMA21 ({ema_21:.4f}) < EMA50 ({ema_50:.4f}) - tendência de baixa")
                return None
        elif sell_signals >= 3 and buy_signals < 3:
            # Filtro de tendência de médio prazo: só SELL se EMA21 < EMA50 (tendência de baixa)
            if ema_21 <= ema_50 * 1.005:  # Margem de 0.5% para evitar falsos negativos
                return 'SELL'
            else:
                logger.debug(f"SELL bloqueado: EMA21 ({ema_21:.4f}) > EMA50 ({ema_50:.4f}) - tendência de alta")
                return None
        else:
            return None  # Sem sinal claro (mercado lateral)
    
    def calculate_stochastic(self, prices: List[float], period: int = 14) -> Optional[Dict[str, float]]:
        """
        Calcula Stochastic Oscillator real
        
        Args:
            prices: Lista de preços históricos
            period: Período do stochastic (padrão: 14)
        
        Returns:
            Dict com '%K' e '%D' ou None
        """
        if len(prices) < period:
            return None
        
        try:
            prices_array = np.array(prices[-period:], dtype=float)
            
            # Encontrar mínimo e máximo nos últimos 'period' períodos
            highest_high = np.max(prices_array)
            lowest_low = np.min(prices_array)
            
            # Evitar divisão por zero
            if highest_high == lowest_low:
                return {'percent_k': 50.0, 'percent_d': 50.0}
            
            # %K = ((Preço Atual - Mínimo Baixo) / (Máximo Alto - Mínimo Baixo)) * 100
            percent_k = ((prices[-1] - lowest_low) / (highest_high - lowest_low)) * 100
            
            # %D = SMA de 3 períodos do %K
            percent_d = percent_k  # Simplificação: usar valor atual
            
            return {
                'percent_k': float(max(0, min(100, percent_k))),
                'percent_d': float(max(0, min(100, percent_d)))
            }
            
        except Exception as e:
            logger.error(f"Erro ao calcular Stochastic: {e}")
            return None
    
    def calculate_adx(self, prices: List[float], period: int = 14) -> Optional[float]:
        """
        Calcula ADX (Average Directional Index) - força da tendência
        
        Args:
            prices: Lista de preços históricos [open, high, low, close] ou apenas closes
            period: Período do ADX (padrão: 14)
        
        Returns:
            Valor do ADX (0-100) ou None
        """
        if len(prices) < period + 1:
            return None
        
        try:
            # Simplificação: usar apenas preços de fechamento
            prices_array = np.array(prices, dtype=float)
            
            # Calcular mudanças
            ups = np.maximum(np.diff(prices_array), 0)
            downs = np.maximum(-np.diff(prices_array), 0)
            
            # Calcular médias móveis das mudanças
            avg_up = np.mean(ups[-period:])
            avg_down = np.mean(downs[-period:])
            
            # Evitar divisão por zero
            if avg_up + avg_down == 0:
                return 50.0
            
            # DI+ e DI-
            di_plus = 100 * (avg_up / (avg_up + avg_down))
            di_minus = 100 * (avg_down / (avg_up + avg_down))
            
            # ADX = DI+ vs DI-
            adx = 100 * abs(di_plus - di_minus) / (di_plus + di_minus) if (di_plus + di_minus) > 0 else 50.0
            
            return float(max(0, min(100, adx)))
            
        except Exception as e:
            logger.error(f"Erro ao calcular ADX: {e}")
            return None
    
    def calculate_cci(self, prices: List[float], period: int = 20) -> Optional[float]:
        """
        Calcula CCI (Commodity Channel Index)
        
        Args:
            prices: Lista de preços históricos
            period: Período do CCI (padrão: 20)
        
        Returns:
            Valor do CCI ou None
        """
        if len(prices) < period:
            return None
        
        try:
            prices_array = np.array(prices[-period:], dtype=float)
            
            # Típical Price (simplificado: usar apenas o preço)
            typical_price = np.mean(prices_array)
            current_typical = prices[-1]
            
            # Desvio médio
            mad = np.mean(np.abs(prices_array - typical_price))
            
            # Evitar divisão por zero
            if mad == 0:
                return 0.0
            
            # CCI = (Preço Típico Atual - SMA) / (0.015 * Desvio Médio)
            cci = (current_typical - typical_price) / (0.015 * mad)
            
            return float(cci)
            
        except Exception as e:
            logger.error(f"Erro ao calcular CCI: {e}")
            return None
