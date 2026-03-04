#!/usr/bin/env python3
"""
ArbitrageAI - Sistema de Machine Learning Básico
Usa Random Forest para prever probabilidade de sucesso de trades

Features utilizadas:
- Score de confiança
- EMAs (9, 21, 50)
- Momentum
- ROC (Rate of Change)
- Volatilidade
"""

import os
import sys
import json
import pickle
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
import mysql.connector
from dotenv import load_dotenv
import numpy as np

logger = logging.getLogger(__name__)
load_dotenv()


class MLPredictor:
    """Sistema de ML para prever sucesso de trades"""
    
    def __init__(self, db_config: Dict):
        self.db_config = db_config
        self.model = None
        self.model_path = '/home/ubuntu/arbitrage-dashboard/ml_model.pkl'
        self.scaler = None
        self.feature_names = [
            'score', 'ema_9', 'ema_21', 'ema_50',
            'momentum', 'roc', 'volatility', 'confidence'
        ]
        
        # Tentar carregar modelo existente
        self.load_model()
    
    def _get_db_connection(self):
        """Cria conexão com banco"""
        return mysql.connector.connect(**self.db_config)
    
    def collect_training_data(self) -> Tuple[np.ndarray, np.ndarray]:
        """Coleta dados históricos de trades para treinamento"""
        conn = self._get_db_connection()
        cursor = conn.cursor(dictionary=True)
        
        # Buscar trades fechados com P&L
        cursor.execute("""
            SELECT 
                symbol, recommendation, entry_price, exit_price,
                quantity, pnl, confidence, opened_at, closed_at,
                close_reason
            FROM trades
            WHERE status = 'CLOSED'
            AND pnl IS NOT NULL
            AND confidence IS NOT NULL
            ORDER BY closed_at DESC
            LIMIT 200
        """)
        
        trades = cursor.fetchall()
        cursor.close()
        conn.close()
        
        if len(trades) < 10:
            logger.warning(f"Poucos dados para treinamento: {len(trades)} trades")
            return None, None
        
        logger.info(f"📊 Coletados {len(trades)} trades para treinamento")
        
        # Preparar features e labels
        X = []
        y = []
        
        for trade in trades:
            # Calcular features
            features = self._extract_features(trade)
            if features is None:
                continue
            
            # Label: 1 se lucro, 0 se prejuízo
            pnl = float(trade['pnl'])
            label = 1 if pnl > 0 else 0
            
            X.append(features)
            y.append(label)
        
        if len(X) < 10:
            logger.warning("Poucos dados válidos após extração de features")
            return None, None
        
        return np.array(X), np.array(y)
    
    def _extract_features(self, trade: Dict) -> Optional[List[float]]:
        """Extrai features de um trade"""
        try:
            # Features básicas
            confidence = float(trade.get('confidence', 50))
            
            # Simular EMAs (em produção, calcular de verdade)
            entry_price = float(trade['entry_price'])
            ema_9 = entry_price * 0.98
            ema_21 = entry_price * 0.97
            ema_50 = entry_price * 0.95
            
            # Simular momentum e ROC
            momentum = np.random.uniform(-5, 5)
            roc = np.random.uniform(-3, 3)
            volatility = np.random.uniform(0.01, 0.05)
            
            # Score (usar confidence como proxy)
            score = confidence
            
            return [
                score, ema_9, ema_21, ema_50,
                momentum, roc, volatility, confidence
            ]
            
        except Exception as e:
            logger.error(f"Erro ao extrair features: {e}")
            return None
    
    def train_model(self):
        """Treina modelo Random Forest"""
        try:
            # Tentar importar sklearn
            try:
                from sklearn.ensemble import RandomForestClassifier
                from sklearn.model_selection import train_test_split
                from sklearn.preprocessing import StandardScaler
                from sklearn.metrics import accuracy_score, classification_report
            except ImportError:
                logger.error("sklearn não instalado. Instalando...")
                os.system("sudo pip3 install scikit-learn --quiet")
                from sklearn.ensemble import RandomForestClassifier
                from sklearn.model_selection import train_test_split
                from sklearn.preprocessing import StandardScaler
                from sklearn.metrics import accuracy_score, classification_report
            
            # Coletar dados
            X, y = self.collect_training_data()
            if X is None or len(X) < 10:
                logger.warning("Dados insuficientes para treinamento")
                return False
            
            logger.info(f"📈 Treinando modelo com {len(X)} amostras...")
            logger.info(f"   Trades lucrativos: {np.sum(y)} ({np.mean(y)*100:.1f}%)")
            logger.info(f"   Trades prejuízo: {len(y) - np.sum(y)} ({(1-np.mean(y))*100:.1f}%)")
            
            # Normalizar features
            self.scaler = StandardScaler()
            X_scaled = self.scaler.fit_transform(X)
            
            # Split train/test
            X_train, X_test, y_train, y_test = train_test_split(
                X_scaled, y, test_size=0.2, random_state=42
            )
            
            # Treinar Random Forest
            self.model = RandomForestClassifier(
                n_estimators=100,
                max_depth=10,
                min_samples_split=5,
                random_state=42,
                n_jobs=-1
            )
            
            self.model.fit(X_train, y_train)
            
            # Avaliar
            y_pred = self.model.predict(X_test)
            accuracy = accuracy_score(y_test, y_pred)
            
            logger.info(f"✅ Modelo treinado com sucesso!")
            logger.info(f"   Acurácia: {accuracy*100:.1f}%")
            logger.info(f"   Amostras treino: {len(X_train)}")
            logger.info(f"   Amostras teste: {len(X_test)}")
            
            # Feature importance
            importances = self.model.feature_importances_
            for name, importance in zip(self.feature_names, importances):
                logger.info(f"   {name}: {importance:.3f}")
            
            # Salvar modelo
            self.save_model()
            
            return True
            
        except Exception as e:
            logger.error(f"Erro ao treinar modelo: {e}", exc_info=True)
            return False
    
    def predict_success_probability(self, analysis: Dict) -> float:
        """Prevê probabilidade de sucesso de um trade"""
        if self.model is None:
            logger.warning("Modelo não treinado, retornando probabilidade padrão")
            return 0.5  # 50% se modelo não existe
        
        try:
            # Extrair features da análise
            features = [
                analysis.get('score', 50),
                analysis.get('ema_9', 0),
                analysis.get('ema_21', 0),
                analysis.get('ema_50', 0),
                analysis.get('momentum', 0),
                analysis.get('roc', 0),
                analysis.get('volatility', 0.02),
                analysis.get('confidence', 50)
            ]
            
            # Normalizar
            features_scaled = self.scaler.transform([features])
            
            # Prever probabilidade
            proba = self.model.predict_proba(features_scaled)[0][1]  # Prob de sucesso
            
            return float(proba)
            
        except Exception as e:
            logger.error(f"Erro ao prever: {e}")
            return 0.5
    
    def save_model(self):
        """Salva modelo treinado"""
        try:
            with open(self.model_path, 'wb') as f:
                pickle.dump({
                    'model': self.model,
                    'scaler': self.scaler,
                    'feature_names': self.feature_names,
                    'trained_at': datetime.now().isoformat()
                }, f)
            logger.info(f"💾 Modelo salvo em {self.model_path}")
        except Exception as e:
            logger.error(f"Erro ao salvar modelo: {e}")
    
    def load_model(self):
        """Carrega modelo salvo"""
        if not os.path.exists(self.model_path):
            logger.info("Nenhum modelo salvo encontrado")
            return False
        
        try:
            with open(self.model_path, 'rb') as f:
                data = pickle.load(f)
                self.model = data['model']
                self.scaler = data['scaler']
                self.feature_names = data['feature_names']
                trained_at = data.get('trained_at', 'unknown')
            
            logger.info(f"✅ Modelo carregado (treinado em: {trained_at})")
            return True
            
        except Exception as e:
            logger.error(f"Erro ao carregar modelo: {e}")
            return False


if __name__ == "__main__":
    # Teste do sistema de ML
    from intelligent_daemon import IntelligentDaemon
    
    daemon = IntelligentDaemon()
    predictor = MLPredictor(daemon.db_config)
    
    # Treinar modelo
    success = predictor.train_model()
    
    if success:
        print("\n✅ Modelo treinado com sucesso!")
        print(f"   Modelo salvo em: {predictor.model_path}")
    else:
        print("\n❌ Falha ao treinar modelo")
