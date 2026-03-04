import { formatPrice } from '@/lib/utils';
import React, { useState, useEffect, useRef } from 'react';
import { LineChart, Line, AreaChart, Area, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, PieChart, Pie, Cell } from 'recharts';
import { TrendingUp, TrendingDown, Activity, DollarSign, Target, AlertCircle, RefreshCw, Send, Brain, Zap, BarChart3, Settings, Wifi, WifiOff } from 'lucide-react';

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
}

export default function UnifiedTradingSystem() {
  const [marketData, setMarketData] = useState<Record<string, MarketData>>({});
  const [trades, setTrades] = useState<Trade[]>([]);
  const [sofiaInsights, setSofiaInsights] = useState<SofiaInsight[]>([]);
  const [chatMessages, setChatMessages] = useState<Array<{ role: string; content: string }>>([]);
  const [chatInput, setChatInput] = useState('');
  const [activeTab, setActiveTab] = useState<'dashboard' | 'trades' | 'analysis' | 'chat'>('dashboard');
  const [metrics, setMetrics] = useState({
    totalCapital: 1000000,
    currentCapital: 1000000,
    totalPnL: 0,
    winRate: 0,
    openTrades: 0,
  });
  const [loading, setLoading] = useState(true);
  const [connected, setConnected] = useState(false);
  const [lastUpdate, setLastUpdate] = useState<string>(new Date().toLocaleTimeString());
  const wsRef = useRef<WebSocket | null>(null);
  const updateIntervalRef = useRef<NodeJS.Timeout | null>(null);

  // Fetch real market data from Binance
  const fetchBinanceData = async () => {
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

      setMarketData(newMarketData);
      setConnected(true);
      setLastUpdate(new Date().toLocaleTimeString());
      setLoading(false);
    } catch (error) {
      console.error('Error fetching market data:', error);
      setConnected(false);
    }
  };

  // Fetch Brapi data (B3)
  const fetchBrapiData = async () => {
    try {
      const brapiSymbols = ['PETR4', 'VALE3', 'ITUB4'];
      const newMarketData: Record<string, MarketData> = { ...marketData };

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
      console.error('Error fetching Brapi data:', error);
    }
  };

  // Generate Sofia insights
  const generateSofiaInsights = () => {
    const insights: SofiaInsight[] = [];

    Object.entries(marketData).forEach(([symbol, data]) => {
      const isPositive = data.change24h > 0;
      const volatility = ((data.high24h - data.low24h) / data.low24h) * 100;

      let recommendation: 'BUY' | 'SELL' | 'HOLD' = 'HOLD';
      let confidence = 50;
      const reasoning: string[] = [];

      if (isPositive && volatility < 3) {
        recommendation = 'BUY';
        confidence = 65 + Math.random() * 20;
        reasoning.push('Tendência positiva com volatilidade controlada');
        reasoning.push(`Variação 24h: +${data.change24h.toFixed(2)}%`);
      } else if (!isPositive && volatility > 2) {
        recommendation = 'SELL';
        confidence = 55 + Math.random() * 20;
        reasoning.push('Tendência negativa com volatilidade elevada');
        reasoning.push(`Variação 24h: ${data.change24h.toFixed(2)}%`);
      } else {
        recommendation = 'HOLD';
        confidence = 40 + Math.random() * 20;
        reasoning.push('Mercado em consolidação');
        reasoning.push(`Volatilidade: ${volatility.toFixed(2)}%`);
      }

      insights.push({
        symbol,
        recommendation,
        confidence: Math.min(100, confidence),
        reasoning,
        profitTarget: data.price * (recommendation === 'BUY' ? 1.025 : recommendation === 'SELL' ? 0.975 : 1),
        stopLoss: data.price * (recommendation === 'BUY' ? 0.98 : recommendation === 'SELL' ? 1.02 : 1),
      });
    });

    setSofiaInsights(insights);
  };

  // Simulate trades based on market data
  const simulateTrades = () => {
    const newTrades: Trade[] = [];

    Object.entries(marketData).forEach(([symbol, data], index) => {
      if (Math.random() > 0.6) {
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

    // Update metrics
    const totalPnL = newTrades.reduce((sum, t) => sum + t.pnl, 0);
    const winTrades = newTrades.filter(t => t.pnl > 0).length;

    setMetrics(prev => ({
      ...prev,
      currentCapital: prev.totalCapital + totalPnL,
      totalPnL,
      winRate: newTrades.length > 0 ? (winTrades / newTrades.length) * 100 : 0,
      openTrades: newTrades.length,
    }));
  };

  // Handle Sofia chat
  const handleSofiaChat = async () => {
    if (!chatInput.trim()) return;

    const userMessage = { role: 'user', content: chatInput };
    setChatMessages(prev => [...prev, userMessage]);
    setChatInput('');

    // Simulate Sofia response
    const sofiaResponse = {
      role: 'sofia',
      content: `Analisando: "${chatInput}". Com base nos dados atuais, identifiquei ${sofiaInsights.length} oportunidades. Recomendo focar em ativos com >60% confiança e volatilidade <3%.`,
    };

    setTimeout(() => {
      setChatMessages(prev => [...prev, sofiaResponse]);
    }, 500);
  };

  // Initial load
  useEffect(() => {
    fetchBinanceData();
    fetchBrapiData();
  }, []);

  // Generate insights and trades when market data updates
  useEffect(() => {
    if (Object.keys(marketData).length > 0) {
      generateSofiaInsights();
      simulateTrades();
    }
  }, [marketData]);

  // Real-time updates
  useEffect(() => {
    // Update Binance every 2 seconds
    const binanceInterval = setInterval(() => {
      fetchBinanceData();
    }, 2000);

    // Update Brapi every 5 seconds
    const brapiInterval = setInterval(() => {
      fetchBrapiData();
    }, 5000);

    return () => {
      clearInterval(binanceInterval);
      clearInterval(brapiInterval);
    };
  }, []);

  const MetricCard = ({ icon: Icon, label, value, change }: any) => (
    <div className="bg-gradient-to-br from-slate-800 to-slate-900 rounded-lg p-6 border border-slate-700 hover:border-blue-500 transition">
      <div className="flex items-start justify-between">
        <div>
          <p className="text-slate-400 text-sm font-medium">{label}</p>
          <p className="text-2xl font-bold text-white mt-2">{value}</p>
          {change !== undefined && (
            <p className={`text-xs mt-1 ${change >= 0 ? 'text-green-400' : 'text-red-400'}`}>
              {change >= 0 ? '+' : ''}{change.toFixed(2)}%
            </p>
          )}
        </div>
        <Icon className="w-8 h-8 text-blue-400" />
      </div>
    </div>
  );

  const MarketCard = ({ data }: { data: MarketData }) => {
    const isPositive = data.change24h >= 0;
    return (
      <div className="bg-slate-800 rounded-lg p-4 border border-slate-700 hover:border-blue-500 transition">
        <div className="flex items-center justify-between mb-2">
          <h3 className="font-bold text-white">{data.symbol}</h3>
          <div className={`flex items-center gap-1 ${isPositive ? 'text-green-400' : 'text-red-400'}`}>
            {isPositive ? <TrendingUp size={16} /> : <TrendingDown size={16} />}
            <span className="text-sm font-medium">{Math.abs(data.change24h).toFixed(2)}%</span>
          </div>
        </div>
        <p className="text-2xl font-bold text-white">${data.price.toFixed(2)}</p>
        <div className="text-xs text-slate-400 mt-2 space-y-1">
          <p>24h High: ${data.high24h.toFixed(2)}</p>
          <p>24h Low: ${data.low24h.toFixed(2)}</p>
        </div>
      </div>
    );
  };

  const SofiaCard = ({ insight }: { insight: SofiaInsight }) => {
    const bgColor = insight.recommendation === 'BUY' ? 'bg-green-900' : insight.recommendation === 'SELL' ? 'bg-red-900' : 'bg-blue-900';
    const borderColor = insight.recommendation === 'BUY' ? 'border-green-700' : insight.recommendation === 'SELL' ? 'border-red-700' : 'border-blue-700';

    return (
      <div className={`rounded-lg p-4 border ${bgColor} ${borderColor}`}>
        <div className="flex items-start justify-between mb-2">
          <div>
            <h3 className="font-bold text-white">{insight.symbol}</h3>
            <p className="text-sm text-slate-200">{insight.recommendation}</p>
          </div>
          <div className="text-right">
            <p className="text-lg font-bold text-white">{insight.confidence.toFixed(0)}%</p>
            <p className="text-xs text-slate-300">Confiança</p>
          </div>
        </div>
        <div className="text-xs text-slate-200 space-y-1">
          {insight.reasoning.map((reason, i) => (
            <p key={i}>• {reason}</p>
          ))}
        </div>
      </div>
    );
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center min-h-screen bg-slate-950">
        <div className="text-center">
          <Activity className="w-12 h-12 text-blue-400 animate-spin mx-auto mb-4" />
          <p className="text-slate-300 font-medium">Conectando aos mercados reais...</p>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-slate-950 text-white p-6">
      <div className="max-w-7xl mx-auto">
        {/* Header */}
        <div className="mb-8">
          <div className="flex items-center justify-between mb-4">
            <div>
              <h1 className="text-4xl font-bold text-white">ArbitrageAI Pro</h1>
              <p className="text-slate-400 mt-2">Sistema Unificado de Trading Inteligente em Tempo Real</p>
            </div>
            <div className="text-right">
              <div className="flex items-center gap-2 justify-end mb-2">
                {connected ? (
                  <Wifi className="w-5 h-5 text-green-400" />
                ) : (
                  <WifiOff className="w-5 h-5 text-red-400" />
                )}
                <p className={`text-sm ${connected ? 'text-green-400' : 'text-red-400'}`}>
                  {connected ? '● Conectado' : '● Desconectado'}
                </p>
              </div>
              <p className="text-sm text-slate-400">Atualizado: {lastUpdate}</p>
            </div>
          </div>

          {/* Navigation Tabs */}
          <div className="flex gap-4 border-b border-slate-700">
            {['dashboard', 'trades', 'analysis', 'chat'].map(tab => (
              <button
                key={tab}
                onClick={() => setActiveTab(tab as any)}
                className={`px-4 py-2 font-medium transition ${
                  activeTab === tab
                    ? 'border-b-2 border-blue-400 text-blue-400'
                    : 'text-slate-400 hover:text-white'
                }`}
              >
                {tab.charAt(0).toUpperCase() + tab.slice(1)}
              </button>
            ))}
          </div>
        </div>

        {/* Dashboard Tab */}
        {activeTab === 'dashboard' && (
          <div className="space-y-8">
            {/* Metrics */}
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-5 gap-4">
              <MetricCard
                icon={DollarSign}
                label="Capital Atual"
                value={`$${(metrics.currentCapital / 1000).toFixed(0)}k`}
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
              <MetricCard
                icon={Zap}
                label="Sofia IA"
                value={`${sofiaInsights.filter(s => s.confidence > 60).length}/${sofiaInsights.length}`}
              />
            </div>

            {/* Market Data */}
            <div>
              <h2 className="text-2xl font-bold text-white mb-4">Mercados em Tempo Real</h2>
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-5 gap-4">
                {Object.values(marketData).map(data => (
                  <MarketCard key={data.symbol} data={data} />
                ))}
              </div>
            </div>

            {/* Sofia Insights */}
            <div>
              <h2 className="text-2xl font-bold text-white mb-4 flex items-center gap-2">
                <Brain className="w-6 h-6 text-blue-400" />
                Análises Sofia IA
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
            <h2 className="text-2xl font-bold text-white mb-4">Operações em Curso</h2>
            {trades.length > 0 ? (
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead className="bg-slate-800 border-b border-slate-700">
                    <tr>
                      <th className="px-4 py-3 text-left">Símbolo</th>
                      <th className="px-4 py-3 text-left">Entrada</th>
                      <th className="px-4 py-3 text-left">Atual</th>
                      <th className="px-4 py-3 text-left">Quantidade</th>
                      <th className="px-4 py-3 text-left">P&L</th>
                      <th className="px-4 py-3 text-left">Duração</th>
                      <th className="px-4 py-3 text-left">Razão</th>
                    </tr>
                  </thead>
                  <tbody>
                    {trades.map(trade => (
                      <tr key={trade.id} className="border-b border-slate-700 hover:bg-slate-800">
                        <td className="px-4 py-3 font-medium">{trade.symbol}</td>
                        <td className="px-4 py-3">{formatPrice(trade.entryPrice)}</td>
                        <td className="px-4 py-3">{formatPrice(trade.currentPrice)}</td>
                        <td className="px-4 py-3">{trade.quantity.toFixed(4)}</td>
                        <td className={`px-4 py-3 font-bold ${trade.pnl >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                          ${trade.pnl.toFixed(2)} ({trade.pnlPercent.toFixed(2)}%)
                        </td>
                        <td className="px-4 py-3">{trade.duration.toFixed(2)}h</td>
                        <td className="px-4 py-3">{trade.reason}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <p className="text-slate-400">Nenhuma operação em curso</p>
            )}
          </div>
        )}

        {/* Analysis Tab */}
        {activeTab === 'analysis' && (
          <div>
            <h2 className="text-2xl font-bold text-white mb-4">Análise Comportamental</h2>
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
              <div className="bg-slate-800 rounded-lg p-6 border border-slate-700">
                <h3 className="text-lg font-bold text-white mb-4">Distribuição de Mercados</h3>
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

              <div className="bg-slate-800 rounded-lg p-6 border border-slate-700">
                <h3 className="text-lg font-bold text-white mb-4">Performance</h3>
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
          </div>
        )}

        {/* Chat Tab */}
        {activeTab === 'chat' && (
          <div className="flex flex-col h-96 bg-slate-800 rounded-lg border border-slate-700">
            <div className="flex-1 overflow-y-auto p-4 space-y-4">
              {chatMessages.length === 0 && (
                <div className="text-center text-slate-400 mt-20">
                  <Brain className="w-12 h-12 mx-auto mb-4 opacity-50" />
                  <p>Converse com Sofia IA sobre o mercado</p>
                </div>
              )}
              {chatMessages.map((msg, i) => (
                <div key={i} className={`flex ${msg.role === 'user' ? 'justify-end' : 'justify-start'}`}>
                  <div
                    className={`max-w-xs px-4 py-2 rounded-lg ${
                      msg.role === 'user'
                        ? 'bg-blue-600 text-white'
                        : 'bg-slate-700 text-slate-100'
                    }`}
                  >
                    {msg.content}
                  </div>
                </div>
              ))}
            </div>

            <div className="border-t border-slate-700 p-4 flex gap-2">
              <input
                type="text"
                value={chatInput}
                onChange={e => setChatInput(e.target.value)}
                onKeyPress={e => e.key === 'Enter' && handleSofiaChat()}
                placeholder="Pergunte algo para Sofia IA..."
                className="flex-1 bg-slate-700 text-white px-4 py-2 rounded border border-slate-600 focus:outline-none focus:border-blue-400"
              />
              <button
                onClick={handleSofiaChat}
                className="bg-blue-600 hover:bg-blue-700 text-white px-4 py-2 rounded transition"
              >
                <Send size={20} />
              </button>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
