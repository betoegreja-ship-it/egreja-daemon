#!/usr/bin/env python3
"""
ArbitrageAI - Machine Learning Engine
Sistema de aprendizado contínuo que melhora baseado em trades reais
Aprende quais indicadores funcionam melhor para cada ativo
"""

import logging
from typing import Dict, List, Optional, Tuple
import numpy as np
from datetime import datetime, timedelta
import json
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler
import pickle

logger = logging.getLogger(__name__)


class MachineLearningEngine:
    """Motor de ML que aprende com trades e melhora decisões"""
    
    def __init__(self, model_path: str = 'models/'):
        """Inicializar motor de ML"""
        self.model_path = model_path
        self.models = {}  # Modelos por símbolo
        self.scaler = StandardScaler()
        self.trade_history = {}  # Histórico de trades e resultados
        self.weights = {}  # Pesos dos indicadores por símbolo
        self.performance_metrics = {}  # Métricas de desempenho
    
    def record_trade(self, symbol: str, trade: Dict, result: Dict) -> None:
        """
        Registra um trade realizado e seu resultado
        Usado para feedback e aprendizado
        
        Args:
            symbol: Símbolo do ativo
            trade: {'signal': 'BUY', 'price': 100, 'timestamp': '...', 'score': 75, 'indicators': {...}}
            result: {'exit_price': 105, 'profit': 500, 'duration': 3600, 'result': 'WIN'}
        """
        
        if symbol not in self.trade_history:
            self.trade_history[symbol] = []
        
        trade_record = {
            **trade,
            'result': result,
            'recorded_at': datetime.now().isoformat()
        }
        
        self.trade_history[symbol].append(trade_record)
        
        # Atualizar pesos dos indicadores baseado no resultado
        self._update_indicator_weights(symbol, trade, result)
        
        # Salvar histórico em arquivo
        self._save_trade_history(symbol)
    
    def _update_indicator_weights(self, symbol: str, trade: Dict, result: Dict) -> None:
        """
        Atualiza pesos dos indicadores baseado em sucesso/falha
        Indicadores que levaram a trades vencedores ganham mais peso
        """
        
        if symbol not in self.weights:
            self.weights[symbol] = {
                'rsi': 1.0,
                'macd': 1.0,
                'bollinger': 1.0,
                'ema': 1.0,
                'momentum': 1.0,
                'cci': 1.0,
                'adx': 1.0
            }
        
        # Se trade foi vencedor, aumentar peso dos indicadores
        profit = result.get('profit', 0)
        is_win = result.get('result') == 'WIN'
        
        if is_win and profit > 0:
            # Aumentar peso: +5% para cada 100 lucrados
            adjustment = 1 + (min(profit, 500) / 10000)
        else:
            # Diminuir peso se perdeu: -3%
            adjustment = 0.97
        
        # Ajustar pesos dos indicadores que foram usados
        indicators = trade.get('indicators', {})
        
        for indicator in indicators.keys():
            if indicator in self.weights[symbol]:
                self.weights[symbol][indicator] *= adjustment
        
        # Normalizar pesos
        total_weight = sum(self.weights[symbol].values())
        for key in self.weights[symbol]:
            self.weights[symbol][key] /= total_weight
    
    def _save_trade_history(self, symbol: str) -> None:
        """Salva histórico de trades em arquivo para persistência"""
        try:
            filename = f'trades_{symbol}_{datetime.now().strftime("%Y%m")}.json'
            with open(f'data/{filename}', 'w') as f:
                json.dump(self.trade_history.get(symbol, []), f)
        except Exception as e:
            logger.warning(f"Erro ao salvar histórico de trades: {e}")
    
    def calculate_performance(self, symbol: str) -> Dict:
        """
        Calcula métricas de desempenho do sistema para um símbolo
        Win rate, profit factor, Sharpe ratio, etc
        """
        
        trades = self.trade_history.get(symbol, [])
        if not trades:
            return {
                'symbol': symbol,
                'trades_total': 0,
                'win_rate': 0,
                'profit_factor': 0,
                'avg_profit': 0,
                'max_drawdown': 0,
                'sharpe_ratio': 0
            }
        
        wins = len([t for t in trades if t.get('result', {}).get('result') == 'WIN'])
        losses = len(trades) - wins
        
        win_trades = [t['result'].get('profit', 0) for t in trades if t.get('result', {}).get('result') == 'WIN']
        loss_trades = [abs(t['result'].get('profit', 0)) for t in trades if t.get('result', {}).get('result') == 'LOSS']
        
        total_profit = sum(win_trades)
        total_loss = sum(loss_trades) if loss_trades else 1
        
        # Win rate
        win_rate = (wins / len(trades) * 100) if trades else 0
        
        # Profit factor
        profit_factor = total_profit / total_loss if total_loss > 0 else 0
        
        # Average profit
        avg_profit = total_profit / len(trades) if trades else 0
        
        # Sharpe ratio (simplificado)
        if len(trades) > 1:
            returns = [t['result'].get('profit', 0) for t in trades]
            sharpe = (np.mean(returns) / np.std(returns)) if np.std(returns) > 0 else 0
        else:
            sharpe = 0
        
        metrics = {
            'symbol': symbol,
            'trades_total': len(trades),
            'trades_won': wins,
            'trades_lost': losses,
            'win_rate': round(win_rate, 2),
            'profit_factor': round(profit_factor, 2),
            'total_profit': round(total_profit, 2),
            'avg_profit': round(avg_profit, 2),
            'sharpe_ratio': round(sharpe, 2)
        }
        
        self.performance_metrics[symbol] = metrics
        return metrics
    
    def train_symbol_model(self, symbol: str) -> Optional[RandomForestClassifier]:
        """
        Treina modelo de ML específico para um símbolo
        Aprende padrões: quais indicadores levam a trades vencedores
        
        Args:
            symbol: Símbolo do ativo
        
        Returns:
            Modelo treinado ou None se dados insuficientes
        """
        
        trades = self.trade_history.get(symbol, [])
        
        # Precisa de pelo menos 20 trades para treinar
        if len(trades) < 20:
            logger.info(f"{symbol}: Histórico insuficiente para treinar ({len(trades)} < 20)")
            return None
        
        try:
            # Preparar dados
            X = []  # Features (indicadores)
            y = []  # Target (vencedor=1, perdedor=0)
            
            for trade in trades:
                indicators = trade.get('indicators', {})
                
                # Normalizar indicadores em escala 0-100
                features = [
                    min(100, max(0, indicators.get('rsi', 50))),
                    min(100, max(0, (indicators.get('macd', 0) + 10) * 5)),
                    min(100, max(0, indicators.get('momentum', 0) + 50)),
                    min(100, max(0, indicators.get('cci', 0) / 2 + 50)),
                ]
                
                X.append(features)
                
                # Target: 1 se venceu, 0 se perdeu
                is_win = 1 if trade.get('result', {}).get('result') == 'WIN' else 0
                y.append(is_win)
            
            # Treinar modelo
            model = RandomForestClassifier(n_estimators=100, max_depth=10, random_state=42)
            X_array = np.array(X)
            y_array = np.array(y)
            
            model.fit(X_array, y_array)
            
            # Armazenar modelo
            self.models[symbol] = model
            
            # Feature importance
            feature_importance = {
                'rsi': model.feature_importances_[0],
                'macd': model.feature_importances_[1],
                'momentum': model.feature_importances_[2],
                'cci': model.feature_importances_[3]
            }
            
            logger.info(f"{symbol}: Modelo treinado com {len(trades)} trades")
            logger.info(f"  Importância: {feature_importance}")
            
            return model
        
        except Exception as e:
            logger.error(f"Erro ao treinar modelo para {symbol}: {e}")
            return None
    
    def predict_trade_success(self, symbol: str, indicators: Dict) -> Tuple[float, str]:
        """
        Prediz probabilidade de sucesso para um trade baseado em histórico
        
        Args:
            symbol: Símbolo do ativo
            indicators: Dict com indicadores atuais
        
        Returns:
            (probabilidade, confiança_texto)
        """
        
        # Se modelo foi treinado, usar ML
        if symbol in self.models:
            try:
                features = [
                    min(100, max(0, indicators.get('rsi', 50))),
                    min(100, max(0, (indicators.get('macd', 0) + 10) * 5)),
                    min(100, max(0, indicators.get('momentum', 0) + 50)),
                    min(100, max(0, indicators.get('cci', 0) / 2 + 50)),
                ]
                
                model = self.models[symbol]
                probability = model.predict_proba([features])[0][1]  # Prob de ganhar
                
                if probability > 0.65:
                    confidence = "Alta confiança"
                elif probability > 0.55:
                    confidence = "Média confiança"
                else:
                    confidence = "Baixa confiança"
                
                return probability, confidence
            
            except Exception as e:
                logger.warning(f"Erro ao prever para {symbol}: {e}")
                return 0.5, "Erro na predição"
        
        # Fallback: usar pesos dos indicadores
        if symbol in self.weights:
            weights = self.weights[symbol]
            score = (
                (indicators.get('rsi', 50) / 100) * weights.get('rsi', 0.15) +
                (min(100, max(0, (indicators.get('macd', 0) + 10) * 5)) / 100) * weights.get('macd', 0.2) +
                (min(100, max(0, indicators.get('momentum', 0) + 50)) / 100) * weights.get('momentum', 0.1)
            )
            
            return score, "Score ponderado"
        
        return 0.5, "Sem histórico"
    
    def get_adaptive_analysis(self, symbol: str, signals: Dict) -> Dict:
        """
        Retorna análise adaptativa para um símbolo
        Combina sinais técnicos + ML + histórico de desempenho
        """
        
        performance = self.calculate_performance(symbol)
        weights = self.weights.get(symbol, {})
        
        # Predição de sucesso
        indicators = signals.get('technical_analysis', {}).get('indicators', {})
        win_probability, confidence = self.predict_trade_success(symbol, indicators)
        
        # Ajustar recomendação final baseado em performance histórica
        adaptive_signal = signals.get('signal', '🟡 MANTER')
        
        if win_probability > 0.70 and performance.get('win_rate', 0) > 50:
            # Aumentar confiança se performance é boa
            adaptive_signal = adaptive_signal.replace('COMPRA', 'COMPRA FORTE')
        elif win_probability < 0.40 or performance.get('win_rate', 0) < 30:
            # Reduzir confiança se performance é ruim
            adaptive_signal = adaptive_signal.replace('COMPRA', 'MANTER')
        
        return {
            'symbol': symbol,
            'adaptive_signal': adaptive_signal,
            'win_probability': round(win_probability * 100, 1),
            'confidence': confidence,
            'performance': performance,
            'indicator_weights': weights,
            'recommendation': f"Chance de sucesso: {win_probability*100:.1f}% (Base: {performance['trades_total']} trades)"
        }


if __name__ == '__main__':
    # Teste
    print("=== MACHINE LEARNING ENGINE ===\n")
    
    ml = MachineLearningEngine()
    
    # Simular alguns trades
    symbol = 'PETR4.SA'
    
    for i in range(10):
        trade = {
            'signal': 'BUY',
            'price': 40 + i,
            'score': 70 + np.random.rand() * 20,
            'indicators': {
                'rsi': 40 + np.random.rand() * 30,
                'macd': np.random.rand() * 2 - 1,
                'momentum': np.random.rand() * 10 - 5,
                'cci': np.random.rand() * 100
            }
        }
        
        # 70% de chance de win
        is_win = np.random.rand() > 0.3
        result = {
            'exit_price': trade['price'] * (1 + np.random.rand() * 0.05 - 0.02),
            'profit': 500 if is_win else -300,
            'result': 'WIN' if is_win else 'LOSS'
        }
        
        ml.record_trade(symbol, trade, result)
    
    # Treinar modelo
    ml.train_symbol_model(symbol)
    
    # Ver performance
    perf = ml.calculate_performance(symbol)
    print(f"Performance para {symbol}:")
    print(f"  Win Rate: {perf['win_rate']}%")
    print(f"  Profit Factor: {perf['profit_factor']}")
    print(f"  Total Profit: ${perf['total_profit']}")
