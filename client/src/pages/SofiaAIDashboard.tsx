import React, { useState, useEffect } from 'react';
import { formatPrice } from '@/lib/utils';
import { LineChart, Line, AreaChart, Area, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, PieChart, Pie, Cell } from 'recharts';
import { Brain, TrendingUp, TrendingDown, Target, AlertCircle, RefreshCw, Zap, BarChart3, Activity, Clock, Flame, Shield, Lightbulb, CheckCircle, XCircle, Wifi, WifiOff } from 'lucide-react';

interface Recommendation {
  symbol: string;
  recommendation: string;
  confidence: number;
  reasoning: string[];
  profit_target: number;
  stop_loss: number;
  current_price: number;
  accuracy: number;
  win_rate: number;
}

interface Trade {
  id: string;
  symbol: string;
  recommendation: string;
  entry_price: number;
  exit_price: number;
  pnl: number;
  pnl_percent: number;
  executed_at: string;
  status: string;
}

interface Stats {
  total_trades_all_time: number;
  total_trades_today: number;
  total_pnl_today: number;
  win_rate_today: number;
  overall_accuracy: number;
  symbols_monitored: number;
  best_symbol: string;
  best_accuracy: number;
}

interface Metrics {
  [symbol: string]: {
    total_trades: number;
    correct_predictions: number;
    accuracy: number;
    win_rate: number;
    avg_profit: number;
  };
}

export default function SofiaAIDashboard() {
  const [recommendations, setRecommendations] = useState<Recommendation[]>([]);
  const [stats, setStats] = useState<Stats | null>(null);
  const [metrics, setMetrics] = useState<Metrics>({});
  const [trades, setTrades] = useState<Trade[]>([]);
  const [loading, setLoading] = useState(true);
  const [connected, setConnected] = useState(false);
  const [lastUpdate, setLastUpdate] = useState<string>('');
  const [activeTab, setActiveTab] = useState('overview');

  const API_BASE = 'http://localhost:5000/api/sofia';

  useEffect(() => {
    fetchData();
    const interval = setInterval(fetchData, 5000); // Atualiza a cada 5 segundos
    return () => clearInterval(interval);
  }, []);

  const fetchData = async () => {
    try {
      const [recsRes, statsRes, metricsRes, tradesRes] = await Promise.all([
        fetch(`${API_BASE}/recommendations`),
        fetch(`${API_BASE}/stats`),
        fetch(`${API_BASE}/metrics`),
        fetch(`${API_BASE}/trades?limit=20`)
      ]);

      if (recsRes.ok && statsRes.ok && metricsRes.ok && tradesRes.ok) {
        const recsData = await recsRes.json();
        const statsData = await statsRes.json();
        const metricsData = await metricsRes.json();
        const tradesData = await tradesRes.json();

        setRecommendations(recsData.recommendations || []);
        setStats(statsData.stats);
        setMetrics(metricsData.metrics || {});
        setTrades(tradesData.trades || []);
        setConnected(true);
        setLastUpdate(new Date().toLocaleTimeString());
      } else {
        setConnected(false);
      }
    } catch (error) {
      console.error('Erro ao buscar dados:', error);
      setConnected(false);
    } finally {
      setLoading(false);
    }
  };

  const executeTradesCycle = async () => {
    try {
      setLoading(true);
      const res = await fetch(`${API_BASE}/execute-cycle`, { method: 'POST' });
      if (res.ok) {
        await fetchData();
      }
    } catch (error) {
      console.error('Erro ao executar ciclo:', error);
    } finally {
      setLoading(false);
    }
  };

  const getRecommendationColor = (rec: string) => {
    if (rec === 'BUY') return 'text-green-400';
    if (rec === 'SELL') return 'text-red-400';
    return 'text-yellow-400';
  };

  const getRecommendationBg = (rec: string) => {
    if (rec === 'BUY') return 'bg-green-900/30 border-green-700';
    if (rec === 'SELL') return 'bg-red-900/30 border-red-700';
    return 'bg-yellow-900/30 border-yellow-700';
  };

  const metricsChartData = Object.entries(metrics).map(([symbol, data]) => ({
    symbol,
    accuracy: data.accuracy,
    win_rate: data.win_rate,
    avg_profit: data.avg_profit
  }));

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-950 via-slate-900 to-slate-800">
      {/* Header */}
      <div className="sticky top-0 z-50 border-b border-slate-700 bg-slate-900/95 backdrop-blur">
        <div className="max-w-7xl mx-auto px-4 py-6">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-3">
              <div className="p-2 bg-gradient-to-br from-blue-500 to-purple-600 rounded-lg">
                <Brain className="w-6 h-6 text-white" />
              </div>
              <div>
                <h1 className="text-2xl font-bold text-white">Sofia IA</h1>
                <p className="text-sm text-slate-400">Sistema Inteligente de Trading</p>
              </div>
            </div>
            <div className="flex items-center gap-4">
              <div className="flex items-center gap-2">
                {connected ? (
                  <>
                    <Wifi className="w-4 h-4 text-green-400" />
                    <span className="text-sm text-green-400">Conectado</span>
                  </>
                ) : (
                  <>
                    <WifiOff className="w-4 h-4 text-red-400" />
                    <span className="text-sm text-red-400">Desconectado</span>
                  </>
                )}
              </div>
              <span className="text-xs text-slate-400">Atualizado: {lastUpdate}</span>
              <button
                onClick={fetchData}
                disabled={loading}
                className="p-2 hover:bg-slate-800 rounded-lg transition"
              >
                <RefreshCw className={`w-4 h-4 ${loading ? 'animate-spin' : ''}`} />
              </button>
            </div>
          </div>

          {/* Tabs */}
          <div className="flex gap-8 mt-6 border-b border-slate-700">
            {['overview', 'recommendations', 'metrics', 'trades'].map(tab => (
              <button
                key={tab}
                onClick={() => setActiveTab(tab)}
                className={`pb-3 px-1 font-medium transition ${
                  activeTab === tab
                    ? 'text-blue-400 border-b-2 border-blue-400'
                    : 'text-slate-400 hover:text-slate-300'
                }`}
              >
                {tab.charAt(0).toUpperCase() + tab.slice(1)}
              </button>
            ))}
          </div>
        </div>
      </div>

      <div className="max-w-7xl mx-auto px-4 py-8">
        {/* Overview Tab */}
        {activeTab === 'overview' && stats && (
          <div className="space-y-8">
            {/* Stats Cards */}
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
              <div className="rounded-xl border border-slate-700 bg-slate-800/50 p-6 hover:border-blue-600 transition">
                <div className="flex items-center justify-between">
                  <div>
                    <p className="text-slate-400 text-sm">Acurácia Geral</p>
                    <p className="text-3xl font-bold text-white mt-2">{stats.overall_accuracy}%</p>
                  </div>
                  <Brain className="w-8 h-8 text-blue-400 opacity-50" />
                </div>
              </div>

              <div className="rounded-xl border border-slate-700 bg-slate-800/50 p-6 hover:border-green-600 transition">
                <div className="flex items-center justify-between">
                  <div>
                    <p className="text-slate-400 text-sm">Trades Hoje</p>
                    <p className="text-3xl font-bold text-white mt-2">{stats.total_trades_today}</p>
                  </div>
                  <Activity className="w-8 h-8 text-green-400 opacity-50" />
                </div>
              </div>

              <div className="rounded-xl border border-slate-700 bg-slate-800/50 p-6 hover:border-purple-600 transition">
                <div className="flex items-center justify-between">
                  <div>
                    <p className="text-slate-400 text-sm">P&L Hoje</p>
                    <p className={`text-3xl font-bold mt-2 ${stats.total_pnl_today >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                      ${stats.total_pnl_today.toFixed(2)}
                    </p>
                  </div>
                  <TrendingUp className="w-8 h-8 text-purple-400 opacity-50" />
                </div>
              </div>

              <div className="rounded-xl border border-slate-700 bg-slate-800/50 p-6 hover:border-yellow-600 transition">
                <div className="flex items-center justify-between">
                  <div>
                    <p className="text-slate-400 text-sm">Win Rate</p>
                    <p className="text-3xl font-bold text-white mt-2">{stats.win_rate_today.toFixed(1)}%</p>
                  </div>
                  <Target className="w-8 h-8 text-yellow-400 opacity-50" />
                </div>
              </div>
            </div>

            {/* Execute Button */}
            <button
              onClick={executeTradesCycle}
              disabled={loading}
              className="w-full bg-gradient-to-r from-blue-600 to-purple-600 hover:from-blue-700 hover:to-purple-700 disabled:opacity-50 text-white font-bold py-3 px-6 rounded-lg transition flex items-center justify-center gap-2"
            >
              <Zap className="w-5 h-5" />
              {loading ? 'Executando...' : 'Executar Ciclo de Trading'}
            </button>

            {/* Charts */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
              <div className="rounded-xl border border-slate-700 bg-slate-800/50 p-6">
                <h3 className="text-lg font-bold text-white mb-4">Acurácia por Símbolo</h3>
                <ResponsiveContainer width="100%" height={300}>
                  <BarChart data={metricsChartData}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                    <XAxis dataKey="symbol" stroke="#9ca3af" />
                    <YAxis stroke="#9ca3af" />
                    <Tooltip />
                    <Bar dataKey="accuracy" fill="#3b82f6" />
                  </BarChart>
                </ResponsiveContainer>
              </div>

              <div className="rounded-xl border border-slate-700 bg-slate-800/50 p-6">
                <h3 className="text-lg font-bold text-white mb-4">Win Rate por Símbolo</h3>
                <ResponsiveContainer width="100%" height={300}>
                  <BarChart data={metricsChartData}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                    <XAxis dataKey="symbol" stroke="#9ca3af" />
                    <YAxis stroke="#9ca3af" />
                    <Tooltip />
                    <Bar dataKey="win_rate" fill="#10b981" />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            </div>
          </div>
        )}

        {/* Recommendations Tab */}
        {activeTab === 'recommendations' && (
          <div className="space-y-4">
            <h2 className="text-2xl font-bold text-white mb-6">Recomendações de Sofia</h2>
            {recommendations.map((rec, idx) => (
              <div
                key={idx}
                className={`rounded-xl border p-6 ${getRecommendationBg(rec.recommendation)} hover:border-opacity-100 transition`}
              >
                <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                  <div>
                    <div className="flex items-center gap-3 mb-4">
                      <h3 className="text-xl font-bold text-white">{rec.symbol}</h3>
                      <span className={`text-lg font-bold ${getRecommendationColor(rec.recommendation)}`}>
                        {rec.recommendation}
                      </span>
                    </div>
                    <div className="space-y-2">
                      <p className="text-slate-300">
                        <span className="text-slate-400">Preço Atual:</span> ${rec.current_price.toFixed(2)}
                      </p>
                      <p className="text-slate-300">
                        <span className="text-slate-400">Confiança:</span> {rec.confidence.toFixed(1)}%
                      </p>
                      <p className="text-slate-300">
                        <span className="text-slate-400">Acurácia Histórica:</span> {rec.accuracy.toFixed(1)}%
                      </p>
                      <p className="text-slate-300">
                        <span className="text-slate-400">Win Rate:</span> {rec.win_rate.toFixed(1)}%
                      </p>
                    </div>
                  </div>

                  <div>
                    <div className="space-y-3">
                      <div className="bg-slate-900/50 rounded-lg p-3">
                        <p className="text-slate-400 text-sm">Meta de Lucro</p>
                        <p className="text-green-400 font-bold">${rec.profit_target.toFixed(2)}</p>
                      </div>
                      <div className="bg-slate-900/50 rounded-lg p-3">
                        <p className="text-slate-400 text-sm">Stop Loss</p>
                        <p className="text-red-400 font-bold">${rec.stop_loss.toFixed(2)}</p>
                      </div>
                      <div className="bg-slate-900/50 rounded-lg p-3">
                        <p className="text-slate-400 text-sm mb-2">Motivo</p>
                        <ul className="text-slate-300 text-sm space-y-1">
                          {rec.reasoning.map((reason, i) => (
                            <li key={i} className="flex gap-2">
                              <span className="text-blue-400">•</span>
                              {reason}
                            </li>
                          ))}
                        </ul>
                      </div>
                    </div>
                  </div>
                </div>
              </div>
            ))}
          </div>
        )}

        {/* Metrics Tab */}
        {activeTab === 'metrics' && (
          <div className="space-y-6">
            <h2 className="text-2xl font-bold text-white mb-6">Métricas de Aprendizado</h2>
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
              {Object.entries(metrics).map(([symbol, data]) => (
                <div key={symbol} className="rounded-xl border border-slate-700 bg-slate-800/50 p-6">
                  <h3 className="text-lg font-bold text-white mb-4">{symbol}</h3>
                  <div className="space-y-3">
                    <div className="flex justify-between items-center">
                      <span className="text-slate-400">Trades:</span>
                      <span className="text-white font-bold">{data.total_trades}</span>
                    </div>
                    <div className="flex justify-between items-center">
                      <span className="text-slate-400">Acertos:</span>
                      <span className="text-green-400 font-bold">{data.correct_predictions}</span>
                    </div>
                    <div className="flex justify-between items-center">
                      <span className="text-slate-400">Acurácia:</span>
                      <span className="text-blue-400 font-bold">{data.accuracy.toFixed(1)}%</span>
                    </div>
                    <div className="flex justify-between items-center">
                      <span className="text-slate-400">Win Rate:</span>
                      <span className="text-green-400 font-bold">{data.win_rate.toFixed(1)}%</span>
                    </div>
                    <div className="flex justify-between items-center">
                      <span className="text-slate-400">Lucro Médio:</span>
                      <span className={data.avg_profit >= 0 ? 'text-green-400 font-bold' : 'text-red-400 font-bold'}>
                        ${data.avg_profit.toFixed(2)}
                      </span>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Trades Tab */}
        {activeTab === 'trades' && (
          <div className="space-y-4">
            <h2 className="text-2xl font-bold text-white mb-6">Histórico de Trades</h2>
            <div className="overflow-x-auto rounded-xl border border-slate-700">
              <table className="w-full">
                <thead className="bg-slate-800/50 border-b border-slate-700">
                  <tr>
                    <th className="px-6 py-3 text-left text-sm font-bold text-slate-300">Símbolo</th>
                    <th className="px-6 py-3 text-left text-sm font-bold text-slate-300">Recomendação</th>
                    <th className="px-6 py-3 text-left text-sm font-bold text-slate-300">Entrada</th>
                    <th className="px-6 py-3 text-left text-sm font-bold text-slate-300">Saída</th>
                    <th className="px-6 py-3 text-left text-sm font-bold text-slate-300">P&L</th>
                    <th className="px-6 py-3 text-left text-sm font-bold text-slate-300">Data</th>
                  </tr>
                </thead>
                <tbody>
                  {trades.map((trade, idx) => (
                    <tr key={idx} className="border-b border-slate-700 hover:bg-slate-800/30 transition">
                      <td className="px-6 py-4 text-white font-bold">{trade.symbol}</td>
                      <td className={`px-6 py-4 font-bold ${getRecommendationColor(trade.recommendation)}`}>
                        {trade.recommendation}
                      </td>
                      <td className="px-6 py-4 text-slate-300 font-mono text-xs">{formatPrice(trade.entry_price)}</td>
                      <td className="px-6 py-4 text-slate-300 font-mono text-xs">{formatPrice(trade.exit_price)}</td>
                      <td className={`px-6 py-4 font-bold ${trade.pnl >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                        ${trade.pnl.toFixed(2)} ({trade.pnl_percent.toFixed(2)}%)
                      </td>
                      <td className="px-6 py-4 text-slate-400 text-sm">
                        {new Date(trade.executed_at).toLocaleString()}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
