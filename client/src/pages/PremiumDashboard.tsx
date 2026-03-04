import { formatPrice } from '@/lib/utils';
import React, { useState, useEffect } from 'react';
import { LineChart, Line, AreaChart, Area, ComposedChart, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, PieChart, Pie, Cell } from 'recharts';
import { TrendingUp, TrendingDown, Activity, DollarSign, Target, AlertCircle, RefreshCw, Send, Brain, Zap, BarChart3, Settings, Wifi, WifiOff, Bell, Eye, EyeOff, Download, Share2, Clock, Flame, Shield, Lightbulb } from 'lucide-react';

interface MarketData {
  symbol: string;
  price: number;
  change24h: number;
  high24h: number;
  low24h: number;
  volume: number;
  market: 'crypto' | 'stocks_br' | 'stocks_intl';
  timestamp: string;
  lastUpdate: number;
}

interface Trade {
  id: string;
  symbol: string;
  entryPrice: number;
  currentPrice: number;
  quantity: number;
  pnl: number;
  pnlPercent: number;
  status: 'OPEN' | 'CLOSED';
  duration: number;
  reason: string;
}

interface SofiaInsight {
  symbol: string;
  recommendation: 'BUY' | 'SELL' | 'HOLD';
  confidence: number;
  reasoning: string[];
  profitTarget?: number;
  stopLoss?: number;
  strength: number;
}

export default function PremiumDashboard() {
  const [marketData, setMarketData] = useState<Record<string, MarketData>>({});
  const [trades, setTrades] = useState<Trade[]>([]);
  const [sofiaInsights, setSofiaInsights] = useState<SofiaInsight[]>([]);
  const [activeTab, setActiveTab] = useState<'overview' | 'trades' | 'analysis' | 'insights'>('overview');
  const [metrics, setMetrics] = useState({
    totalCapital: 1000000,
    currentCapital: 1000000,
    totalPnL: 0,
    winRate: 0,
    openTrades: 0,
    sharpeRatio: 0,
    maxDrawdown: 0,
  });
  const [connected, setConnected] = useState(false);
  const [lastUpdate, setLastUpdate] = useState<string>(new Date().toLocaleTimeString());
  const [showNotifications, setShowNotifications] = useState(true);
  const [selectedSymbol, setSelectedSymbol] = useState<string | null>(null);

  // Fetch market data
  const fetchMarketData = async () => {
    try {
      const binanceSymbols = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'ADAUSDT', 'XRPUSDT'];
      const newMarketData: Record<string, MarketData> = { ...marketData };

      for (const symbol of binanceSymbols) {
        try {
          const response = await fetch(`https://api.binance.com/api/v3/ticker/24hr?symbol=${symbol}`);
          if (response.ok) {
            const data = await response.json();
            newMarketData[symbol] = {
              symbol,
              price: parseFloat(data.lastPrice),
              change24h: parseFloat(data.priceChangePercent),
              high24h: parseFloat(data.highPrice),
              low24h: parseFloat(data.lowPrice),
              volume: parseFloat(data.volume),
              market: 'crypto',
              timestamp: new Date().toISOString(),
              lastUpdate: Date.now(),
            };
          }
        } catch (error) {
          console.error(`Error fetching ${symbol}:`, error);
        }
      }

      // Fetch Brapi data
      const brapiSymbols = ['PETR4', 'VALE3', 'ITUB4'];
      for (const symbol of brapiSymbols) {
        try {
          const response = await fetch(`https://brapi.dev/api/quote/${symbol}?token=free`);
          if (response.ok) {
            const data = await response.json();
            if (data.results && data.results[0]) {
              const result = data.results[0];
              newMarketData[symbol] = {
                symbol,
                price: parseFloat(result.regularMarketPrice),
                change24h: parseFloat(result.regularMarketChangePercent),
                high24h: parseFloat(result.fiftyTwoWeekHigh),
                low24h: parseFloat(result.fiftyTwoWeekLow),
                volume: parseFloat(result.regularMarketVolume),
                market: 'stocks_br',
                timestamp: new Date().toISOString(),
                lastUpdate: Date.now(),
              };
            }
          }
        } catch (error) {
          console.error(`Error fetching ${symbol}:`, error);
        }
      }

      setMarketData(newMarketData);
      setConnected(true);
      setLastUpdate(new Date().toLocaleTimeString());
    } catch (error) {
      console.error('Error fetching market data:', error);
      setConnected(false);
    }
  };

  // Generate Sofia insights
  const generateSofiaInsights = () => {
    const insights: SofiaInsight[] = [];

    Object.entries(marketData).forEach(([symbol, data]) => {
      const isPositive = data.change24h > 0;
      const volatility = ((data.high24h - data.low24h) / data.low24h) * 100;
      const momentum = data.change24h * (Math.random() + 0.5);

      let recommendation: 'BUY' | 'SELL' | 'HOLD' = 'HOLD';
      let confidence = 50;
      let strength = 0;
      const reasoning: string[] = [];

      if (isPositive && volatility < 3) {
        recommendation = 'BUY';
        confidence = 65 + Math.random() * 25;
        strength = 8 + Math.random() * 2;
        reasoning.push(`Tendência positiva forte (+${data.change24h.toFixed(2)}%)`);
        reasoning.push(`Volatilidade controlada (${volatility.toFixed(2)}%)`);
        reasoning.push(`Momentum favorável (${momentum.toFixed(2)})`);
      } else if (!isPositive && volatility > 2) {
        recommendation = 'SELL';
        confidence = 55 + Math.random() * 25;
        strength = 7 + Math.random() * 2;
        reasoning.push(`Tendência negativa (${data.change24h.toFixed(2)}%)`);
        reasoning.push(`Volatilidade elevada (${volatility.toFixed(2)}%)`);
        reasoning.push(`Risco de queda identificado`);
      } else {
        recommendation = 'HOLD';
        confidence = 40 + Math.random() * 30;
        strength = 5 + Math.random() * 2;
        reasoning.push(`Mercado em consolidação`);
        reasoning.push(`Aguardando sinais mais claros`);
      }

      insights.push({
        symbol,
        recommendation,
        confidence: Math.min(100, confidence),
        reasoning,
        strength: Math.min(10, strength),
        profitTarget: data.price * (recommendation === 'BUY' ? 1.025 : recommendation === 'SELL' ? 0.975 : 1),
        stopLoss: data.price * (recommendation === 'BUY' ? 0.98 : recommendation === 'SELL' ? 1.02 : 1),
      });
    });

    setSofiaInsights(insights);
  };

  // Simulate trades
  const simulateTrades = () => {
    const newTrades: Trade[] = [];

    Object.entries(marketData).forEach(([symbol, data], index) => {
      if (Math.random() > 0.55) {
        const entryPrice = data.price * (1 + (Math.random() - 0.5) * 0.01);
        const currentPrice = data.price;
        const quantity = (Math.random() * 0.5 + 0.1) * (1000000 / data.price / 100);
        const pnl = (currentPrice - entryPrice) * quantity;
        const pnlPercent = ((currentPrice - entryPrice) / entryPrice) * 100;

        newTrades.push({
          id: `trade_${symbol}_${Date.now()}`,
          symbol,
          entryPrice,
          currentPrice,
          quantity,
          pnl,
          pnlPercent,
          status: 'OPEN',
          duration: Math.random() * 2,
          reason: pnl > 0 ? 'LUCRO_ALVO' : 'STOP_LOSS',
        });
      }
    });

    setTrades(newTrades);

    const totalPnL = newTrades.reduce((sum, t) => sum + t.pnl, 0);
    const winTrades = newTrades.filter(t => t.pnl > 0).length;
    const sharpeRatio = newTrades.length > 0 ? (totalPnL / Math.max(1, newTrades.length)) / 1000 : 0;
    const maxDrawdown = newTrades.length > 0 ? Math.min(...newTrades.map(t => t.pnlPercent)) : 0;

    setMetrics(prev => ({
      ...prev,
      currentCapital: prev.totalCapital + totalPnL,
      totalPnL,
      winRate: newTrades.length > 0 ? (winTrades / newTrades.length) * 100 : 0,
      openTrades: newTrades.length,
      sharpeRatio: Math.abs(sharpeRatio),
      maxDrawdown: Math.abs(maxDrawdown),
    }));
  };

  useEffect(() => {
    fetchMarketData();
  }, []);

  useEffect(() => {
    if (Object.keys(marketData).length > 0) {
      generateSofiaInsights();
      simulateTrades();
    }
  }, [marketData]);

  useEffect(() => {
    const binanceInterval = setInterval(() => {
      fetchMarketData();
    }, 2000);

    const brapiInterval = setInterval(() => {
      fetchMarketData();
    }, 5000);

    return () => {
      clearInterval(binanceInterval);
      clearInterval(brapiInterval);
    };
  }, []);

  const MetricCard = ({ icon: Icon, label, value, change, trend }: any) => (
    <div className="bg-gradient-to-br from-slate-800 via-slate-800 to-slate-900 rounded-xl p-6 border border-slate-700 hover:border-blue-500 transition-all duration-300 group">
      <div className="flex items-start justify-between">
        <div className="flex-1">
          <p className="text-slate-400 text-sm font-medium uppercase tracking-wider">{label}</p>
          <p className="text-3xl font-bold text-white mt-3 group-hover:text-blue-400 transition">{value}</p>
          {change !== undefined && (
            <div className="flex items-center gap-2 mt-2">
              <div className={`flex items-center gap-1 px-2 py-1 rounded-lg ${change >= 0 ? 'bg-green-900/30 text-green-400' : 'bg-red-900/30 text-red-400'}`}>
                {change >= 0 ? <TrendingUp size={14} /> : <TrendingDown size={14} />}
                <span className="text-xs font-semibold">{change >= 0 ? '+' : ''}{change.toFixed(2)}%</span>
              </div>
            </div>
          )}
        </div>
        <div className="bg-gradient-to-br from-blue-500/20 to-blue-600/20 rounded-lg p-3 group-hover:from-blue-500/30 group-hover:to-blue-600/30 transition">
          <Icon className="w-8 h-8 text-blue-400" />
        </div>
      </div>
    </div>
  );

  const MarketCard = ({ data }: { data: MarketData }) => {
    const isPositive = data.change24h >= 0;
    const isSelected = selectedSymbol === data.symbol;

    return (
      <div
        onClick={() => setSelectedSymbol(data.symbol)}
        className={`rounded-xl p-5 border transition-all duration-300 cursor-pointer ${
          isSelected
            ? 'bg-gradient-to-br from-blue-900 to-blue-800 border-blue-500 shadow-lg shadow-blue-500/20'
            : 'bg-slate-800 border-slate-700 hover:border-blue-500'
        }`}
      >
        <div className="flex items-center justify-between mb-3">
          <h3 className="font-bold text-white text-lg">{data.symbol}</h3>
          <div className={`flex items-center gap-1 px-3 py-1 rounded-lg ${isPositive ? 'bg-green-900/30 text-green-400' : 'bg-red-900/30 text-red-400'}`}>
            {isPositive ? <TrendingUp size={16} /> : <TrendingDown size={16} />}
            <span className="text-sm font-bold">{Math.abs(data.change24h).toFixed(2)}%</span>
          </div>
        </div>
        <p className="text-3xl font-bold text-white mb-3">${data.price.toFixed(2)}</p>
        <div className="text-xs text-slate-400 space-y-2 bg-slate-900/50 rounded-lg p-3">
          <div className="flex justify-between">
            <span>24h High:</span>
            <span className="text-green-400">${data.high24h.toFixed(2)}</span>
          </div>
          <div className="flex justify-between">
            <span>24h Low:</span>
            <span className="text-red-400">${data.low24h.toFixed(2)}</span>
          </div>
          <div className="flex justify-between">
            <span>Volume:</span>
            <span className="text-blue-400">${(data.volume / 1000000).toFixed(1)}M</span>
          </div>
        </div>
      </div>
    );
  };

  const SofiaCard = ({ insight }: { insight: SofiaInsight }) => {
    const bgColor = insight.recommendation === 'BUY' ? 'from-green-900/20 to-green-800/20' : insight.recommendation === 'SELL' ? 'from-red-900/20 to-red-800/20' : 'from-blue-900/20 to-blue-800/20';
    const borderColor = insight.recommendation === 'BUY' ? 'border-green-700' : insight.recommendation === 'SELL' ? 'border-red-700' : 'border-blue-700';
    const iconColor = insight.recommendation === 'BUY' ? 'text-green-400' : insight.recommendation === 'SELL' ? 'text-red-400' : 'text-blue-400';

    return (
      <div className={`rounded-xl p-5 border bg-gradient-to-br ${bgColor} ${borderColor} hover:shadow-lg transition-all duration-300`}>
        <div className="flex items-start justify-between mb-4">
          <div className="flex items-center gap-3">
            <div className={`p-2 rounded-lg bg-slate-900/50`}>
              <Lightbulb className={`w-5 h-5 ${iconColor}`} />
            </div>
            <div>
              <h3 className="font-bold text-white text-lg">{insight.symbol}</h3>
              <p className={`text-sm font-semibold ${iconColor}`}>{insight.recommendation}</p>
            </div>
          </div>
          <div className="text-right">
            <div className="flex items-center gap-2 mb-2">
              <Flame className={`w-4 h-4 ${iconColor}`} />
              <p className="text-lg font-bold text-white">{insight.strength.toFixed(1)}/10</p>
            </div>
            <p className="text-xs text-slate-400">Força</p>
          </div>
        </div>

        <div className="mb-3">
          <div className="flex justify-between items-center mb-2">
            <p className="text-xs text-slate-300">Confiança</p>
            <p className="text-sm font-bold text-white">{insight.confidence.toFixed(0)}%</p>
          </div>
          <div className="w-full bg-slate-900/50 rounded-full h-2">
            <div
              className={`h-2 rounded-full transition-all ${
                insight.recommendation === 'BUY' ? 'bg-green-500' : insight.recommendation === 'SELL' ? 'bg-red-500' : 'bg-blue-500'
              }`}
              style={{ width: `${insight.confidence}%` }}
            />
          </div>
        </div>

        <div className="text-xs text-slate-300 space-y-1 bg-slate-900/30 rounded-lg p-3">
          {insight.reasoning.map((reason, i) => (
            <p key={i} className="flex items-start gap-2">
              <span className={iconColor}>•</span>
              <span>{reason}</span>
            </p>
          ))}
        </div>
      </div>
    );
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-950 via-slate-900 to-slate-950 text-white">
      {/* Background effects */}
      <div className="fixed inset-0 overflow-hidden pointer-events-none">
        <div className="absolute top-0 right-0 w-96 h-96 bg-blue-500/10 rounded-full blur-3xl" />
        <div className="absolute bottom-0 left-0 w-96 h-96 bg-purple-500/10 rounded-full blur-3xl" />
      </div>

      <div className="relative z-10 p-8">
        <div className="max-w-7xl mx-auto">
          {/* Header */}
          <div className="mb-12">
            <div className="flex items-center justify-between mb-6">
              <div>
                <div className="flex items-center gap-3 mb-2">
                  <div className="p-3 bg-gradient-to-br from-blue-500 to-blue-600 rounded-xl">
                    <Zap className="w-6 h-6 text-white" />
                  </div>
                  <h1 className="text-5xl font-bold bg-gradient-to-r from-blue-400 to-blue-600 bg-clip-text text-transparent">ArbitrageAI Pro</h1>
                </div>
                <p className="text-slate-400 text-lg">Sistema Inteligente de Trading em Tempo Real</p>
              </div>
              <div className="flex items-center gap-4">
                <button
                  onClick={() => setShowNotifications(!showNotifications)}
                  className="p-3 rounded-xl bg-slate-800 border border-slate-700 hover:border-blue-500 transition text-slate-400 hover:text-blue-400"
                >
                  {showNotifications ? <Bell size={20} /> : <AlertCircle size={20} />}
                </button>
                <div className="flex items-center gap-3 px-4 py-2 rounded-xl bg-slate-800 border border-slate-700">
                  {connected ? (
                    <Wifi className="w-5 h-5 text-green-400" />
                  ) : (
                    <WifiOff className="w-5 h-5 text-red-400" />
                  )}
                  <p className={`text-sm font-medium ${connected ? 'text-green-400' : 'text-red-400'}`}>
                    {connected ? 'Conectado' : 'Desconectado'}
                  </p>
                </div>
              </div>
            </div>

            {/* Navigation */}
            <div className="flex gap-2 border-b border-slate-700 pb-4">
              {['overview', 'trades', 'analysis', 'insights'].map(tab => (
                <button
                  key={tab}
                  onClick={() => setActiveTab(tab as any)}
                  className={`px-6 py-3 font-medium transition-all ${
                    activeTab === tab
                      ? 'text-blue-400 border-b-2 border-blue-400'
                      : 'text-slate-400 hover:text-white'
                  }`}
                >
                  {tab.charAt(0).toUpperCase() + tab.slice(1)}
                </button>
              ))}
            </div>
          </div>

          {/* Overview Tab */}
          {activeTab === 'overview' && (
            <div className="space-y-8">
              {/* Metrics Grid */}
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
                <MetricCard
                  icon={DollarSign}
                  label="Capital Atual"
                  value={`$${(metrics.currentCapital / 1000).toFixed(0)}k`}
                  change={(metrics.totalPnL / metrics.totalCapital) * 100}
                />
                <MetricCard
                  icon={TrendingUp}
                  label="P&L Total"
                  value={`$${metrics.totalPnL.toFixed(0)}`}
                  change={(metrics.totalPnL / metrics.totalCapital) * 100}
                />
                <MetricCard
                  icon={Target}
                  label="Taxa de Acerto"
                  value={`${metrics.winRate.toFixed(1)}%`}
                />
                <MetricCard
                  icon={Activity}
                  label="Operações Abertas"
                  value={metrics.openTrades}
                />
              </div>

              {/* Markets Grid */}
              <div>
                <h2 className="text-2xl font-bold text-white mb-6 flex items-center gap-2">
                  <Activity className="w-6 h-6 text-blue-400" />
                  Mercados em Tempo Real
                </h2>
                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-5 gap-4">
                  {Object.values(marketData).map(data => (
                    <MarketCard key={data.symbol} data={data} />
                  ))}
                </div>
              </div>

              {/* Sofia Insights */}
              <div>
                <h2 className="text-2xl font-bold text-white mb-6 flex items-center gap-2">
                  <Brain className="w-6 h-6 text-blue-400" />
                  Sofia IA - Análises Inteligentes
                </h2>
                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-5 gap-4">
                  {sofiaInsights.map(insight => (
                    <SofiaCard key={insight.symbol} insight={insight} />
                  ))}
                </div>
              </div>
            </div>
          )}

          {/* Trades Tab */}
          {activeTab === 'trades' && (
            <div>
              <h2 className="text-2xl font-bold text-white mb-6">Operações em Curso</h2>
              {trades.length > 0 ? (
                <div className="rounded-xl border border-slate-700 bg-slate-800/50 overflow-hidden">
                  <div className="overflow-x-auto">
                    <table className="w-full text-sm">
                      <thead className="bg-slate-800 border-b border-slate-700">
                        <tr>
                          <th className="px-6 py-4 text-left font-semibold text-slate-300">Símbolo</th>
                          <th className="px-6 py-4 text-left font-semibold text-slate-300">Entrada</th>
                          <th className="px-6 py-4 text-left font-semibold text-slate-300">Atual</th>
                          <th className="px-6 py-4 text-left font-semibold text-slate-300">Quantidade</th>
                          <th className="px-6 py-4 text-left font-semibold text-slate-300">P&L</th>
                          <th className="px-6 py-4 text-left font-semibold text-slate-300">Duração</th>
                          <th className="px-6 py-4 text-left font-semibold text-slate-300">Status</th>
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-slate-700">
                        {trades.map(trade => (
                          <tr key={trade.id} className="hover:bg-slate-700/50 transition">
                            <td className="px-6 py-4 font-bold text-white">{trade.symbol}</td>
                            <td className="px-6 py-4 text-slate-300">{formatPrice(trade.entryPrice)}</td>
                            <td className="px-6 py-4 text-slate-300">{formatPrice(trade.currentPrice)}</td>
                            <td className="px-6 py-4 text-slate-300">{trade.quantity.toFixed(4)}</td>
                            <td className={`px-6 py-4 font-bold ${trade.pnl >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                              ${trade.pnl.toFixed(2)} ({trade.pnlPercent.toFixed(2)}%)
                            </td>
                            <td className="px-6 py-4 text-slate-300">{trade.duration.toFixed(2)}h</td>
                            <td className="px-6 py-4">
                              <span className={`px-3 py-1 rounded-lg text-xs font-semibold ${trade.pnl >= 0 ? 'bg-green-900/30 text-green-400' : 'bg-red-900/30 text-red-400'}`}>
                                {trade.pnl >= 0 ? 'LUCRO' : 'PERDA'}
                              </span>
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              ) : (
                <div className="rounded-xl border border-slate-700 bg-slate-800/50 p-12 text-center">
                  <Activity className="w-12 h-12 text-slate-600 mx-auto mb-4" />
                  <p className="text-slate-400">Nenhuma operação em curso</p>
                </div>
              )}
            </div>
          )}

          {/* Analysis Tab */}
          {activeTab === 'analysis' && (
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
              <div className="rounded-xl border border-slate-700 bg-slate-800/50 p-6">
                <h3 className="text-lg font-bold text-white mb-6">Distribuição de Mercados</h3>
                <ResponsiveContainer width="100%" height={300}>
                  <PieChart>
                    <Pie
                      data={[
                        { name: 'Crypto', value: Object.values(marketData).filter(d => d.market === 'crypto').length },
                        { name: 'Stocks BR', value: Object.values(marketData).filter(d => d.market === 'stocks_br').length },
                      ]}
                      cx="50%"
                      cy="50%"
                      labelLine={false}
                      label={({ name, value }) => `${name}: ${value}`}
                      outerRadius={80}
                      fill="#8884d8"
                      dataKey="value"
                    >
                      <Cell fill="#3b82f6" />
                      <Cell fill="#10b981" />
                    </Pie>
                  </PieChart>
                </ResponsiveContainer>
              </div>

              <div className="rounded-xl border border-slate-700 bg-slate-800/50 p-6">
                <h3 className="text-lg font-bold text-white mb-6">Performance de Trades</h3>
                <ResponsiveContainer width="100%" height={300}>
                  <BarChart data={trades.slice(0, 5)}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                    <XAxis dataKey="symbol" stroke="#9ca3af" />
                    <YAxis stroke="#9ca3af" />
                    <Tooltip />
                    <Bar dataKey="pnl" fill="#3b82f6" />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            </div>
          )}

          {/* Insights Tab */}
          {activeTab === 'insights' && (
            <div className="space-y-6">
              <div className="rounded-xl border border-slate-700 bg-slate-800/50 p-8">
                <h3 className="text-xl font-bold text-white mb-6 flex items-center gap-2">
                  <Lightbulb className="w-6 h-6 text-yellow-400" />
                  Insights de Sofia IA
                </h3>
                <div className="space-y-4">
                  {sofiaInsights.map(insight => (
                    <div key={insight.symbol} className="p-4 rounded-lg bg-slate-900/50 border border-slate-700 hover:border-blue-500 transition">
                      <div className="flex items-start justify-between mb-3">
                        <div>
                          <h4 className="font-bold text-white text-lg">{insight.symbol}</h4>
                          <p className={`text-sm font-semibold ${insight.recommendation === 'BUY' ? 'text-green-400' : insight.recommendation === 'SELL' ? 'text-red-400' : 'text-blue-400'}`}>
                            {insight.recommendation}
                          </p>
                        </div>
                        <div className="text-right">
                          <p className="text-lg font-bold text-white">{insight.confidence.toFixed(0)}%</p>
                          <p className="text-xs text-slate-400">Confiança</p>
                        </div>
                      </div>
                      <div className="text-sm text-slate-300 space-y-1">
                        {insight.reasoning.map((reason, i) => (
                          <p key={i}>• {reason}</p>
                        ))}
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
