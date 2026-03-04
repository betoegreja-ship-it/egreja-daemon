import { formatPrice } from '@/lib/utils';
import React, { useState, useEffect, useRef } from 'react';
import { LineChart, Line, AreaChart, Area, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, PieChart, Pie, Cell } from 'recharts';
import { TrendingUp, TrendingDown, Activity, DollarSign, Target, AlertCircle, RefreshCw } from 'lucide-react';

interface Trade {
  id: number;
  symbol: string;
  entryPrice: number;
  currentPrice: number;
  quantity: number;
  pnl: number;
  pnlPercent: number;
  status: 'OPEN' | 'CLOSED';
  duration: number;
  entryTime: string;
  exitTime?: string;
  reason?: string;
}

interface PriceData {
  symbol: string;
  price: number;
  change24h: number;
  high24h: number;
  low24h: number;
  volume: number;
  timestamp: string;
}

interface DashboardMetrics {
  totalCapital: number;
  currentCapital: number;
  totalPnL: number;
  totalPnLPercent: number;
  winRate: number;
  totalTrades: number;
  openTrades: number;
  closedTrades: number;
  largestGain: number;
  largestLoss: number;
}

export default function RealTimeTrading() {
  const [prices, setPrices] = useState<Record<string, PriceData>>({});
  const [trades, setTrades] = useState<Trade[]>([]);
  const [metrics, setMetrics] = useState<DashboardMetrics>({
    totalCapital: 1000000,
    currentCapital: 1000000,
    totalPnL: 0,
    totalPnLPercent: 0,
    winRate: 0,
    totalTrades: 0,
    openTrades: 0,
    closedTrades: 0,
    largestGain: 0,
    largestLoss: 0,
  });
  const [chartData, setChartData] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [lastUpdate, setLastUpdate] = useState<string>(new Date().toLocaleTimeString());
  const wsRef = useRef<WebSocket | null>(null);
  const symbols = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'ADAUSDT', 'XRPUSDT'];

  // Buscar cotações reais da Binance via REST API
  const fetchRealPrices = async () => {
    try {
      const priceData: Record<string, PriceData> = {};
      
      for (const symbol of symbols) {
        const response = await fetch(`https://api.binance.com/api/v3/ticker/24hr?symbol=${symbol}`);
        if (response.ok) {
          const data = await response.json();
          priceData[symbol] = {
            symbol,
            price: parseFloat(data.lastPrice),
            change24h: parseFloat(data.priceChangePercent),
            high24h: parseFloat(data.highPrice),
            low24h: parseFloat(data.lowPrice),
            volume: parseFloat(data.volume),
            timestamp: new Date().toISOString(),
          };
        }
      }
      
      setPrices(priceData);
      setLastUpdate(new Date().toLocaleTimeString());
      
      // Atualizar gráfico
      const newChartData = Object.values(priceData).map(p => ({
        symbol: p.symbol,
        price: p.price,
        change: p.change24h,
        timestamp: new Date().toLocaleTimeString(),
      }));
      setChartData(newChartData);
      
    } catch (error) {
      console.error('Erro ao buscar preços:', error);
    }
  };

  // Conectar ao WebSocket da Binance para atualizações em tempo real
  const connectWebSocket = () => {
    try {
      const streams = symbols.map(s => s.toLowerCase() + '@ticker').join('/');
      const wsUrl = `wss://stream.binance.com:9443/stream?streams=${streams}`;
      
      wsRef.current = new WebSocket(wsUrl);
      
      wsRef.current.onmessage = (event) => {
        try {
          const message = JSON.parse(event.data);
          if (message.data) {
            const data = message.data;
            const symbol = data.s;
            
            setPrices(prev => ({
              ...prev,
              [symbol]: {
                symbol,
                price: parseFloat(data.c),
                change24h: parseFloat(data.P),
                high24h: parseFloat(data.h),
                low24h: parseFloat(data.l),
                volume: parseFloat(data.v),
                timestamp: new Date().toISOString(),
              }
            }));
            
            setLastUpdate(new Date().toLocaleTimeString());
          }
        } catch (error) {
          console.error('Erro ao processar WebSocket:', error);
        }
      };
      
      wsRef.current.onerror = (error) => {
        console.error('WebSocket erro:', error);
      };
      
    } catch (error) {
      console.error('Erro ao conectar WebSocket:', error);
    }
  };

  // Simular operações com preços reais
  const simulateTrades = () => {
    const newTrades: Trade[] = [];
    
    Object.values(prices).forEach((priceData, index) => {
      if (Math.random() > 0.5) {
        const entryPrice = priceData.price * (1 + (Math.random() - 0.5) * 0.01);
        const currentPrice = priceData.price;
        const quantity = (Math.random() * 0.5 + 0.1);
        const pnl = (currentPrice - entryPrice) * quantity;
        const pnlPercent = ((currentPrice - entryPrice) / entryPrice) * 100;
        
        newTrades.push({
          id: index + 1,
          symbol: priceData.symbol,
          entryPrice,
          currentPrice,
          quantity,
          pnl,
          pnlPercent,
          status: 'OPEN',
          duration: Math.random() * 2,
          entryTime: new Date(Date.now() - Math.random() * 3600000).toLocaleTimeString(),
        });
      }
    });
    
    setTrades(newTrades);
    
    // Atualizar métricas
    const totalPnL = newTrades.reduce((sum, t) => sum + t.pnl, 0);
    const winTrades = newTrades.filter(t => t.pnl > 0).length;
    
    setMetrics(prev => ({
      ...prev,
      currentCapital: prev.totalCapital + totalPnL,
      totalPnL,
      totalPnLPercent: (totalPnL / prev.totalCapital) * 100,
      winRate: newTrades.length > 0 ? (winTrades / newTrades.length) * 100 : 0,
      totalTrades: newTrades.length,
      openTrades: newTrades.filter(t => t.status === 'OPEN').length,
      closedTrades: newTrades.filter(t => t.status === 'CLOSED').length,
      largestGain: Math.max(...newTrades.map(t => t.pnl), 0),
      largestLoss: Math.min(...newTrades.map(t => t.pnl), 0),
    }));
  };

  useEffect(() => {
    setLoading(true);
    fetchRealPrices();
    connectWebSocket();
    
    // Atualizar preços a cada 5 segundos
    const priceInterval = setInterval(fetchRealPrices, 5000);
    
    // Simular trades a cada 10 segundos
    const tradeInterval = setInterval(simulateTrades, 10000);
    
    setLoading(false);
    
    return () => {
      clearInterval(priceInterval);
      clearInterval(tradeInterval);
      if (wsRef.current) {
        wsRef.current.close();
      }
    };
  }, []);

  const MetricCard = ({ icon: Icon, label, value, subtext, color }: any) => (
    <div className={`bg-white rounded-lg p-6 shadow-md border-l-4 ${color}`}>
      <div className="flex items-start justify-between">
        <div>
          <p className="text-gray-600 text-sm font-medium">{label}</p>
          <p className="text-2xl font-bold text-gray-900 mt-2">{value}</p>
          {subtext && <p className="text-xs text-gray-500 mt-1">{subtext}</p>}
        </div>
        <Icon className="w-8 h-8 text-gray-400" />
      </div>
    </div>
  );

  const PriceCard = ({ data }: { data: PriceData }) => {
    const isPositive = data.change24h >= 0;
    return (
      <div className="bg-white rounded-lg p-4 shadow-md">
        <div className="flex items-center justify-between mb-2">
          <h3 className="font-bold text-gray-900">{data.symbol}</h3>
          <div className={`flex items-center gap-1 ${isPositive ? 'text-green-600' : 'text-red-600'}`}>
            {isPositive ? <TrendingUp size={16} /> : <TrendingDown size={16} />}
            <span className="text-sm font-medium">{Math.abs(data.change24h).toFixed(2)}%</span>
          </div>
        </div>
        <p className="text-2xl font-bold text-gray-900">${data.price.toFixed(2)}</p>
        <div className="text-xs text-gray-500 mt-2 space-y-1">
          <p>24h High: ${data.high24h.toFixed(2)}</p>
          <p>24h Low: ${data.low24h.toFixed(2)}</p>
        </div>
      </div>
    );
  };

  const TradeRow = ({ trade }: { trade: Trade }) => {
    const isPositive = trade.pnl >= 0;
    return (
      <tr className={`border-b hover:bg-gray-50 ${isPositive ? 'bg-green-50' : 'bg-red-50'}`}>
        <td className="px-4 py-3 font-medium text-gray-900">{trade.symbol}</td>
        <td className="px-4 py-3 text-gray-600">{formatPrice(trade.entryPrice)}</td>
        <td className="px-4 py-3 text-gray-600">{formatPrice(trade.currentPrice)}</td>
        <td className="px-4 py-3 text-gray-600">{trade.quantity.toFixed(4)}</td>
        <td className={`px-4 py-3 font-bold ${isPositive ? 'text-green-600' : 'text-red-600'}`}>
          ${trade.pnl.toFixed(2)} ({trade.pnlPercent.toFixed(2)}%)
        </td>
        <td className="px-4 py-3 text-gray-600">{trade.duration.toFixed(2)}h</td>
        <td className="px-4 py-3">
          <span className={`px-2 py-1 rounded text-xs font-medium ${
            trade.status === 'OPEN' 
              ? 'bg-blue-100 text-blue-800' 
              : 'bg-gray-100 text-gray-800'
          }`}>
            {trade.status}
          </span>
        </td>
      </tr>
    );
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center min-h-screen bg-gray-50">
        <div className="text-center">
          <Activity className="w-12 h-12 text-blue-600 animate-spin mx-auto mb-4" />
          <p className="text-gray-600 font-medium">Conectando ao mercado real...</p>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gray-50 p-6">
      <div className="max-w-7xl mx-auto">
        {/* Header */}
        <div className="flex items-center justify-between mb-8">
          <div>
            <h1 className="text-3xl font-bold text-gray-900">Dashboard de Trading em Tempo Real</h1>
            <p className="text-gray-600 mt-2">Cotações reais da Binance Brasil • Operações simuladas com preços reais</p>
          </div>
          <div className="flex items-center gap-2 text-sm text-gray-600">
            <Activity size={16} className="text-green-600" />
            <span>Última atualização: {lastUpdate}</span>
          </div>
        </div>

        {/* Métricas principais */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mb-8">
          <MetricCard
            icon={DollarSign}
            label="Capital Atual"
            value={`$${metrics.currentCapital.toLocaleString('pt-BR', { maximumFractionDigits: 2 })}`}
            subtext={`Inicial: $${metrics.totalCapital.toLocaleString('pt-BR', { maximumFractionDigits: 0 })}`}
            color="border-blue-500"
          />
          <MetricCard
            icon={TrendingUp}
            label="P&L Total"
            value={`$${metrics.totalPnL.toFixed(2)}`}
            subtext={`${metrics.totalPnLPercent.toFixed(2)}% de retorno`}
            color={metrics.totalPnL >= 0 ? 'border-green-500' : 'border-red-500'}
          />
          <MetricCard
            icon={Target}
            label="Taxa de Acerto"
            value={`${metrics.winRate.toFixed(1)}%`}
            subtext={`${metrics.totalTrades} operações`}
            color="border-purple-500"
          />
          <MetricCard
            icon={Activity}
            label="Operações Abertas"
            value={metrics.openTrades}
            subtext={`${metrics.closedTrades} fechadas`}
            color="border-orange-500"
          />
        </div>

        {/* Cotações em tempo real */}
        <div className="mb-8">
          <h2 className="text-xl font-bold text-gray-900 mb-4">Cotações em Tempo Real</h2>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-5 gap-4">
            {Object.values(prices).map(price => (
              <PriceCard key={price.symbol} data={price} />
            ))}
          </div>
        </div>

        {/* Gráfico de preços */}
        <div className="bg-white rounded-lg shadow-md p-6 mb-8">
          <h2 className="text-xl font-bold text-gray-900 mb-4">Variação de Preços (24h)</h2>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="symbol" />
              <YAxis />
              <Tooltip />
              <Bar dataKey="change" fill="#8884d8" name="Variação %" />
            </BarChart>
          </ResponsiveContainer>
        </div>

        {/* Operações em curso */}
        <div className="bg-white rounded-lg shadow-md p-6">
          <h2 className="text-xl font-bold text-gray-900 mb-4">Operações em Curso</h2>
          {trades.length > 0 ? (
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead>
                  <tr className="border-b bg-gray-50">
                    <th className="px-4 py-3 text-left text-sm font-semibold text-gray-900">Símbolo</th>
                    <th className="px-4 py-3 text-left text-sm font-semibold text-gray-900">Entrada</th>
                    <th className="px-4 py-3 text-left text-sm font-semibold text-gray-900">Atual</th>
                    <th className="px-4 py-3 text-left text-sm font-semibold text-gray-900">Quantidade</th>
                    <th className="px-4 py-3 text-left text-sm font-semibold text-gray-900">P&L</th>
                    <th className="px-4 py-3 text-left text-sm font-semibold text-gray-900">Duração</th>
                    <th className="px-4 py-3 text-left text-sm font-semibold text-gray-900">Status</th>
                  </tr>
                </thead>
                <tbody>
                  {trades.map(trade => (
                    <TradeRow key={trade.id} trade={trade} />
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <div className="text-center py-8">
              <AlertCircle className="w-12 h-12 text-gray-400 mx-auto mb-3" />
              <p className="text-gray-600">Nenhuma operação em curso no momento</p>
            </div>
          )}
        </div>

        {/* Rodapé */}
        <div className="mt-8 text-center text-sm text-gray-600">
          <p>Dashboard atualizado em tempo real • Cotações da Binance Brasil • Dados reais</p>
          <p className="mt-2">Última atualização: {new Date().toLocaleString('pt-BR')}</p>
        </div>
      </div>
    </div>
  );
}
