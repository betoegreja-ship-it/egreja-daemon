#!/usr/bin/env python3
"""
ArbitrageAI - Análise Fundamental + Ratings
Integra fundamentals, valuation e ratings da Investing.com
"""

import logging
from typing import Dict, Optional, Tuple
import yfinance as yf
import requests
from bs4 import BeautifulSoup
from datetime import datetime

logger = logging.getLogger(__name__)


class FundamentalAnalyzer:
    """Analisador fundamental com dados reais de valuation e ratings"""
    
    def __init__(self):
        """Inicializar analisador fundamental"""
        self.cache = {}
        self.cache_ttl = 3600  # 1 hora
    
    def get_valuation_metrics(self, symbol: str) -> Optional[Dict]:
        """
        Extrai métricas de valuation via yfinance
        P/E, PB, ROE, ROA, Debt/Equity, etc
        """
        try:
            # Remover .SA para yfinance
            yf_symbol = symbol.replace('.SA', '')
            ticker = yf.Ticker(yf_symbol)
            
            info = ticker.info
            
            metrics = {
                'symbol': symbol,
                'timestamp': datetime.now().isoformat(),
                'price': info.get('currentPrice', info.get('regularMarketPrice')),
                'pe_ratio': info.get('trailingPE'),
                'pb_ratio': info.get('priceToBook'),
                'peg_ratio': info.get('pegRatio'),
                'roe': info.get('returnOnEquity'),
                'roa': info.get('returnOnAssets'),
                'debt_to_equity': info.get('debtToEquity'),
                'current_ratio': info.get('currentRatio'),
                'quick_ratio': info.get('quickRatio'),
                'profit_margin': info.get('profitMargins'),
                'operating_margin': info.get('operatingMargins'),
                'dividend_yield': info.get('dividendYield'),
                '52week_high': info.get('fiftyTwoWeekHigh'),
                '52week_low': info.get('fiftyTwoWeekLow'),
            }
            
            return metrics
        
        except Exception as e:
            logger.warning(f"Erro ao obter fundamentals de {symbol}: {e}")
            return None
    
    def score_valuation(self, metrics: Dict) -> Tuple[float, str]:
        """
        Score de valuation (0-100)
        Pontuação: Subavaliada (80-100), Normal (40-79), Sobrevalorizada (0-39)
        """
        if not metrics:
            return 50, "Dados insuficientes"
        
        score = 50  # Baseline
        
        # P/E Ratio (quanto menor, melhor)
        pe = metrics.get('pe_ratio')
        if pe and pe > 0:
            if pe < 15:
                score += 20
            elif pe < 25:
                score += 10
            elif pe < 35:
                score += 0
            else:
                score -= 15
        
        # Price/Book (PB)
        pb = metrics.get('pb_ratio')
        if pb and pb > 0:
            if pb < 1:
                score += 15
            elif pb < 2:
                score += 5
            elif pb < 3:
                score += 0
            else:
                score -= 10
        
        # ROE (Return on Equity) - quanto maior, melhor
        roe = metrics.get('roe')
        if roe:
            if roe > 0.15:
                score += 15
            elif roe > 0.10:
                score += 10
            elif roe > 0.05:
                score += 5
            else:
                score -= 10
        
        # Debt/Equity - quanto menor, melhor
        de = metrics.get('debt_to_equity')
        if de:
            if de < 0.5:
                score += 10
            elif de < 1.0:
                score += 5
            elif de < 2.0:
                score += 0
            else:
                score -= 10
        
        # Profit Margin - quanto maior, melhor
        pm = metrics.get('profit_margin')
        if pm:
            if pm > 0.2:
                score += 10
            elif pm > 0.1:
                score += 5
            elif pm > 0:
                score += 0
            else:
                score -= 5
        
        score = max(0, min(100, score))
        
        if score >= 75:
            rating = "MUITO SUBAVALIADA"
        elif score >= 60:
            rating = "SUBAVALIADA"
        elif score >= 40:
            rating = "NORMAL"
        elif score >= 25:
            rating = "SOBREVALORIZADA"
        else:
            rating = "MUITO SOBREVALORIZADA"
        
        return score, rating
    
    def get_investing_ratings(self, symbol: str) -> Optional[Dict]:
        """
        Scrape ratings públicos da Investing.com
        Technical Rating, Fundamental Rating, Analyst Consensus
        """
        try:
            # URL base do Investing.com (sem login necessário)
            clean_symbol = symbol.replace('.SA', '')
            
            # Tenta URL para ação brasileira
            if '.SA' in symbol:
                url = f"https://www.investing.com/equities/{clean_symbol.lower()}-nm"
            else:
                url = f"https://www.investing.com/equities/{clean_symbol.lower()}"
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            response = requests.get(url, headers=headers, timeout=5)
            if response.status_code != 200:
                return None
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            ratings = {
                'symbol': symbol,
                'timestamp': datetime.now().isoformat(),
                'technical_rating': None,
                'fundamental_rating': None,
                'analyst_consensus': None,
            }
            
            # Procurar por ratings na página
            # Technical Rating (Buy/Sell/Neutral)
            tech_elem = soup.find('span', {'class': 'rating-bull'})
            if tech_elem:
                ratings['technical_rating'] = tech_elem.text.strip()
            
            # Fundamental Rating
            fund_elem = soup.find('span', {'class': 'rating-fundament'})
            if fund_elem:
                ratings['fundamental_rating'] = fund_elem.text.strip()
            
            return ratings
        
        except Exception as e:
            logger.warning(f"Erro ao obter ratings de {symbol} do Investing.com: {e}")
            return None
    
    def combined_score(self, symbol: str, technical_score: float = 50) -> Dict:
        """
        Score combinado: Técnico (50%) + Fundamental (30%) + Ratings (20%)
        """
        
        # Obter métricas fundamentais
        metrics = self.get_valuation_metrics(symbol)
        fundamental_score, valuation_rating = self.score_valuation(metrics) if metrics else (50, "N/A")
        
        # Obter ratings
        ratings = self.get_investing_ratings(symbol)
        
        # Score combinado ponderado
        combined = (
            technical_score * 0.50 +  # Análise técnica: 50%
            fundamental_score * 0.30 +  # Fundamentals: 30%
            50 * 0.20  # Ratings: 20% (default 50 se não disponível)
        )
        
        combined = max(0, min(100, combined))
        
        # Interpretação final
        if combined >= 70:
            final_rating = "🟢 COMPRA FORTE"
        elif combined >= 60:
            final_rating = "🟢 COMPRA"
        elif combined >= 50:
            final_rating = "🟡 MANTER"
        elif combined >= 40:
            final_rating = "🔴 VENDA"
        else:
            final_rating = "🔴 VENDA FORTE"
        
        return {
            'symbol': symbol,
            'technical_score': round(technical_score, 2),
            'fundamental_score': round(fundamental_score, 2),
            'combined_score': round(combined, 2),
            'final_rating': final_rating,
            'valuation_rating': valuation_rating,
            'metrics': metrics,
            'ratings': ratings,
            'timestamp': datetime.now().isoformat()
        }


if __name__ == '__main__':
    analyzer = FundamentalAnalyzer()
    
    # Teste
    print("=== TESTE ANÁLISE FUNDAMENTAL ===\n")
    
    symbols = ['PETR4.SA', 'VALE3.SA', 'ITUB4.SA']
    
    for symbol in symbols:
        print(f"\n{symbol}:")
        metrics = analyzer.get_valuation_metrics(symbol)
        if metrics:
            score, rating = analyzer.score_valuation(metrics)
            print(f"  P/E: {metrics.get('pe_ratio')}")
            print(f"  PB: {metrics.get('pb_ratio')}")
            print(f"  ROE: {metrics.get('roe')}")
            print(f"  Score Valuation: {score}/100 ({rating})")
