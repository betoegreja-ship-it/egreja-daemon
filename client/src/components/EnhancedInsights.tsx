import React from 'react';
import { Card, CardContent, CardHeader } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { TrendingUp, TrendingDown, AlertTriangle, Target, Clock, Activity, Star, Moon } from 'lucide-react';
import { formatPrice } from '@/lib/utils';

interface Analysis {
  id: number;
  symbol: string;
  recommendation: string;
  confidence: number;
  price?: number;
  indicators?: any;
  reasoning?: string;
  marketData?: string;
  marketStatus?: string;
  executed?: number | boolean;
  tradeId?: number | null;
  createdAt?: Date | string;
  created_at?: string;
}

interface EnhancedInsightsProps {
  analyses: Analysis[];
}

export function EnhancedInsights({ analyses }: EnhancedInsightsProps) {
  if (!analyses || analyses.length === 0) {
    return (
      <Card className="bg-gradient-to-br from-slate-900 to-slate-800 border-slate-700">
        <CardContent className="p-8 text-center">
          <Activity className="w-16 h-16 mx-auto mb-4 text-slate-600" />
          <h3 className="text-xl font-semibold text-white mb-2">
            Mercado Sem Oportunidades no Momento
          </h3>
          <p className="text-slate-400">
            O sistema está monitorando continuamente. Novas análises aparecerão quando
            oportunidades de alta confiança forem identificadas.
          </p>
        </CardContent>
      </Card>
    );
  }

  const getSignalStrength = (confidence: number): number => {
    if (confidence >= 90) return 5;
    if (confidence >= 80) return 4;
    if (confidence >= 70) return 3;
    if (confidence >= 60) return 2;
    return 1;
  };

  const getUrgencyLevel = (confidence: number, recommendation: string, marketStatus?: string): string => {
    if (marketStatus === 'PRE_MARKET') return 'Pré-Mercado';
    if (recommendation === 'HOLD') return 'Baixa';
    if (confidence >= 85) return 'Alta';
    if (confidence >= 70) return 'Média';
    return 'Baixa';
  };

  const getRecommendationColor = (rec: string) => {
    switch (rec) {
      case 'BUY': return 'text-green-400 bg-green-500/10 border-green-500/30';
      case 'SELL': return 'text-red-400 bg-red-500/10 border-red-500/30';
      default: return 'text-yellow-400 bg-yellow-500/10 border-yellow-500/30';
    }
  };

  const getUrgencyColor = (urgency: string) => {
    switch (urgency) {
      case 'Alta': return 'text-red-400 bg-red-500/10';
      case 'Média': return 'text-yellow-400 bg-yellow-500/10';
      case 'Pré-Mercado': return 'text-purple-400 bg-purple-500/10';
      default: return 'text-blue-400 bg-blue-500/10';
    }
  };

  const isStockSymbol = (symbol: string): boolean => {
    return symbol.endsWith('.SA') || (!symbol.endsWith('USDT') && !symbol.endsWith('BTC') && !symbol.endsWith('ETH'));
  };

  const getMarketLabel = (symbol: string): string => {
    if (symbol.endsWith('.SA')) return 'B3';
    if (!symbol.endsWith('USDT')) return 'NYSE';
    return 'CRYPTO';
  };

  const getMarketLabelColor = (symbol: string): string => {
    if (symbol.endsWith('.SA')) return 'text-green-400 bg-green-500/10 border-green-500/30';
    if (!symbol.endsWith('USDT')) return 'text-blue-400 bg-blue-500/10 border-blue-500/30';
    return 'text-yellow-400 bg-yellow-500/10 border-yellow-500/30';
  };

  const calculateTakeProfitStopLoss = (price: number, recommendation: string) => {
    if (recommendation === 'BUY') {
      return {
        takeProfit: formatPrice(price * 1.03),
        stopLoss: formatPrice(price * 0.98),
        potentialProfit: '3%',
        potentialLoss: '2%'
      };
    } else if (recommendation === 'SELL') {
      return {
        takeProfit: formatPrice(price * 0.97),
        stopLoss: formatPrice(price * 1.02),
        potentialProfit: '3%',
        potentialLoss: '2%'
      };
    }
    return null;
  };

  const getEstimatedDuration = (confidence: number): string => {
    if (confidence >= 85) return '30-60min';
    if (confidence >= 70) return '1-2h';
    return '2-4h';
  };

  const getJustification = (indicators: any, recommendation: string): string[] => {
    const reasons: string[] = [];
    
    if (indicators?.rsi && indicators.rsi > 0) {
      if (indicators.rsi < 30 && recommendation === 'BUY') {
        reasons.push(`RSI em sobrevenda (${Number(indicators.rsi).toFixed(1)}) — sinal de reversão de alta`);
      } else if (indicators.rsi > 70 && recommendation === 'SELL') {
        reasons.push(`RSI em sobrecompra (${Number(indicators.rsi).toFixed(1)}) — sinal de correção`);
      } else {
        reasons.push(`RSI: ${Number(indicators.rsi).toFixed(1)} — indicador técnico considerado`);
      }
    }

    if (indicators?.macd_signal === 'bullish' && recommendation === 'BUY') {
      reasons.push('MACD cruzou acima da linha de sinal — momentum positivo');
    } else if (indicators?.macd_signal === 'bearish' && recommendation === 'SELL') {
      reasons.push('MACD cruzou abaixo da linha de sinal — momentum negativo');
    } else if (indicators?.macd && indicators.macd !== 0) {
      reasons.push(`MACD: ${Number(indicators.macd).toFixed(4)} — sinal de momentum detectado`);
    }

    if (indicators?.ema_trend === 'uptrend' && recommendation === 'BUY') {
      reasons.push('EMAs alinhadas em tendência de alta');
    } else if (indicators?.ema_trend === 'downtrend' && recommendation === 'SELL') {
      reasons.push('EMAs alinhadas em tendência de baixa');
    }

    if (indicators?.bollinger_position === 'lower' && recommendation === 'BUY') {
      reasons.push('Preço tocou banda inferior de Bollinger — possível reversão');
    } else if (indicators?.bollinger_position === 'upper' && recommendation === 'SELL') {
      reasons.push('Preço tocou banda superior de Bollinger — possível correção');
    }

    if (indicators?.volume_surge) {
      reasons.push('Volume acima da média — confirmação do movimento');
    }

    if (reasons.length === 0) {
      reasons.push('Múltiplos indicadores técnicos convergindo para esta recomendação');
    }

    return reasons;
  };

  const getRisks = (recommendation: string, confidence: number, marketStatus?: string): string[] => {
    const risks: string[] = [];
    
    if (marketStatus === 'PRE_MARKET') {
      risks.push('Mercado fechado — sinal válido para quando o mercado abrir');
      risks.push('Condições podem mudar até a abertura do mercado');
    }

    if (confidence < 75) {
      risks.push('Confiança moderada — considere posição menor');
    }

    if (recommendation === 'BUY') {
      risks.push('Risco de correção de curto prazo');
      risks.push('Monitorar suporte em -2%');
    } else if (recommendation === 'SELL') {
      risks.push('Risco de reversão de tendência');
      risks.push('Monitorar resistência em +2%');
    }

    return risks;
  };

  // Separar insights de mercado aberto e pré-mercado
  const openMarketAnalyses = analyses.filter(a => !a.marketStatus || a.marketStatus === 'OPEN');
  const preMarketAnalyses = analyses.filter(a => a.marketStatus === 'PRE_MARKET');

  const renderAnalysis = (analysis: Analysis) => {
    const signalStrength = getSignalStrength(analysis.confidence);
    const marketStatus = analysis.marketStatus || 'OPEN';
    const urgency = getUrgencyLevel(analysis.confidence, analysis.recommendation, marketStatus);
    const price = analysis.price || 0;
    const targets = calculateTakeProfitStopLoss(price, analysis.recommendation);
    const duration = getEstimatedDuration(analysis.confidence);
    const indicators = analysis.indicators || {};
    const justifications = getJustification(indicators, analysis.recommendation);
    const risks = getRisks(analysis.recommendation, analysis.confidence, marketStatus);
    const isPreMarket = marketStatus === 'PRE_MARKET';
    const isStock = isStockSymbol(analysis.symbol);

    return (
      <Card 
        key={analysis.id}
        className={`border transition-all ${
          isPreMarket 
            ? 'bg-gradient-to-br from-purple-950/30 to-slate-900 border-purple-800/30 hover:border-purple-700/50' 
            : 'bg-gradient-to-br from-slate-900 to-slate-800 border-slate-700 hover:border-slate-600'
        }`}
      >
        <CardHeader className="pb-3">
          <div className="flex items-start justify-between">
            <div className="flex items-center gap-3 flex-wrap">
              <div className="text-2xl font-bold text-white font-mono">
                {analysis.symbol.replace('USDT', '').replace('.SA', '')}
              </div>
              {isStock && (
                <Badge className={`${getMarketLabelColor(analysis.symbol)} border text-xs`}>
                  {getMarketLabel(analysis.symbol)}
                </Badge>
              )}
              <Badge className={`${getRecommendationColor(analysis.recommendation)} border font-semibold`}>
                {analysis.recommendation === 'BUY' ? '▲ COMPRA' : analysis.recommendation === 'SELL' ? '▼ VENDA' : '— AGUARDAR'}
              </Badge>
              {isPreMarket && (
                <Badge className="bg-purple-500/20 text-purple-300 border-purple-500/30 flex items-center gap-1">
                  <Moon className="w-3 h-3" />
                  PRÉ-MERCADO
                </Badge>
              )}
              {analysis.executed && (
                <Badge className="bg-blue-500/20 text-blue-400 border-blue-500/30">
                  ✓ EXECUTADO
                </Badge>
              )}
            </div>
            <div className="flex items-center gap-2">
              <div className="flex">
                {[...Array(5)].map((_, i) => (
                  <Star
                    key={i}
                    className={`w-4 h-4 ${
                      i < signalStrength
                        ? 'text-yellow-400 fill-yellow-400'
                        : 'text-slate-600'
                    }`}
                  />
                ))}
              </div>
              <Badge className={`${getUrgencyColor(urgency)} text-xs`}>
                {urgency}
              </Badge>
            </div>
          </div>
        </CardHeader>

        <CardContent className="space-y-4">
          {/* Métricas Principais */}
          <div className="grid grid-cols-4 gap-3">
            <div className="bg-slate-800/50 rounded-lg p-3">
              <div className="text-xs text-slate-400 mb-1">Preço Atual</div>
              <div className="text-lg font-bold text-white font-mono">
                ${formatPrice(price)}
              </div>
            </div>
            <div className="bg-slate-800/50 rounded-lg p-3">
              <div className="text-xs text-slate-400 mb-1">Confiança</div>
              <div className="text-lg font-bold text-green-400">
                {analysis.confidence}%
              </div>
            </div>
            {targets && (
              <>
                <div className="bg-slate-800/50 rounded-lg p-3">
                  <div className="text-xs text-slate-400 mb-1 flex items-center gap-1">
                    <Target className="w-3 h-3" />
                    Take Profit
                  </div>
                  <div className="text-lg font-bold text-green-400 font-mono">
                    ${targets.takeProfit}
                  </div>
                  <div className="text-xs text-green-400/70">
                    +{targets.potentialProfit}
                  </div>
                </div>
                <div className="bg-slate-800/50 rounded-lg p-3">
                  <div className="text-xs text-slate-400 mb-1 flex items-center gap-1">
                    <AlertTriangle className="w-3 h-3" />
                    Stop Loss
                  </div>
                  <div className="text-lg font-bold text-red-400 font-mono">
                    ${targets.stopLoss}
                  </div>
                  <div className="text-xs text-red-400/70">
                    -{targets.potentialLoss}
                  </div>
                </div>
              </>
            )}
          </div>

          {/* Indicadores Técnicos se disponíveis */}
          {(indicators?.rsi > 0 || indicators?.macd !== 0) && (
            <div className="flex gap-4 text-sm">
              {indicators?.rsi > 0 && (
                <div className="flex items-center gap-2">
                  <span className="text-slate-400">RSI:</span>
                  <span className={`font-mono font-bold ${
                    indicators.rsi < 30 ? 'text-green-400' : indicators.rsi > 70 ? 'text-red-400' : 'text-white'
                  }`}>{Number(indicators.rsi).toFixed(1)}</span>
                </div>
              )}
              {indicators?.macd && indicators.macd !== 0 && (
                <div className="flex items-center gap-2">
                  <span className="text-slate-400">MACD:</span>
                  <span className={`font-mono font-bold ${indicators.macd > 0 ? 'text-green-400' : 'text-red-400'}`}>
                    {Number(indicators.macd).toFixed(4)}
                  </span>
                </div>
              )}
            </div>
          )}

          {/* Duração Estimada */}
          {!isPreMarket && (
            <div className="flex items-center gap-2 text-sm text-slate-300">
              <Clock className="w-4 h-4 text-blue-400" />
              <span>Duração estimada: <strong>{duration}</strong></span>
            </div>
          )}

          {/* Justificativa */}
          <div className="space-y-2">
            <div className="text-sm font-semibold text-white flex items-center gap-2">
              <Activity className="w-4 h-4 text-green-400" />
              Por que esta recomendação?
            </div>
            <ul className="space-y-1">
              {justifications.map((reason, idx) => (
                <li key={idx} className="text-sm text-slate-300 flex items-start gap-2">
                  <span className="text-green-400 mt-0.5">•</span>
                  <span>{reason}</span>
                </li>
              ))}
            </ul>
          </div>

          {/* Riscos */}
          <div className="space-y-2">
            <div className="text-sm font-semibold text-white flex items-center gap-2">
              <AlertTriangle className="w-4 h-4 text-yellow-400" />
              Riscos Identificados
            </div>
            <ul className="space-y-1">
              {risks.map((risk, idx) => (
                <li key={idx} className="text-sm text-slate-400 flex items-start gap-2">
                  <span className="text-yellow-400 mt-0.5">•</span>
                  <span>{risk}</span>
                </li>
              ))}
            </ul>
          </div>

          {/* Timestamp */}
          <div className="text-xs text-slate-500 pt-2 border-t border-slate-700">
            Análise gerada em {new Date(analysis.created_at || analysis.createdAt || Date.now()).toLocaleString('pt-BR')}
          </div>
        </CardContent>
      </Card>
    );
  };

  return (
    <div className="space-y-6">
      {/* Insights de mercado aberto */}
      {openMarketAnalyses.length > 0 && (
        <div className="space-y-4">
          {openMarketAnalyses.map(renderAnalysis)}
        </div>
      )}

      {/* Insights pré-mercado (ações) */}
      {preMarketAnalyses.length > 0 && (
        <div className="space-y-4">
          <div className="flex items-center gap-3 pb-2 border-b border-purple-800/30">
            <Moon className="w-5 h-5 text-purple-400" />
            <h3 className="text-sm font-semibold text-purple-300 uppercase tracking-wider">
              Sinais Pré-Mercado — Ações
            </h3>
            <Badge className="bg-purple-500/20 text-purple-300 border-purple-500/30 text-xs">
              {preMarketAnalyses.length} sinais
            </Badge>
          </div>
          {preMarketAnalyses.map(renderAnalysis)}
        </div>
      )}
    </div>
  );
}
