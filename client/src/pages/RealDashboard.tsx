import { useState, useEffect, useMemo, useCallback } from 'react';
import { trpc } from '@/lib/trpc';
import { formatPrice } from '@/lib/utils';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { Input } from '@/components/ui/input';
import {
  LineChart, Line, AreaChart, Area, BarChart, Bar,
  XAxis, YAxis, CartesianGrid, Tooltip as RechartsTooltip,
  ResponsiveContainer
} from 'recharts';
import {
  TrendingUp, TrendingDown, Activity, DollarSign, Target,
  RefreshCw, Clock, Globe, Search, Download, Info,
  Brain, Zap, Shield, BarChart3, ChevronUp, ChevronDown,
  Cpu, Database, Award
} from 'lucide-react';
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip';
import { StocksTab } from '@/components/StocksTab';
import { EnhancedInsights } from '@/components/EnhancedInsights';
import { CapitalEvolutionChart } from '@/components/CapitalEvolutionChart';
import { TradeDistributionCharts } from '@/components/TradeDistributionCharts';

// ─── QuantVault Design Tokens ─────────────────────────────────────────────────
const QV = {
  gold: '#C9A84C',
  goldDim: '#7A5C1A',
  profit: '#22C55E',
  loss: '#EF4444',
  accent: '#60A5FA',
  surface1: '#060A10',
  surface2: '#0B1018',
  surface3: '#111820',
  border: '#1A2332',
  muted: '#374151',
  text: '#E2E8F0',
  textDim: '#64748B',
};

// ─── Sub-components ───────────────────────────────────────────────────────────
function StatCard({ label, value, sub, icon: Icon, gold = false }: {
  label: string; value: React.ReactNode; sub?: React.ReactNode;
  icon: React.ElementType; gold?: boolean;
}) {
  return (
    <div className="relative overflow-hidden rounded-lg p-4 flex flex-col gap-2"
      style={{
        background: gold ? 'linear-gradient(135deg, #0B1018, #130E00)' : QV.surface2,
        border: `1px solid ${gold ? QV.goldDim : QV.border}`,
        boxShadow: gold ? `0 0 24px ${QV.gold}08` : 'none',
      }}>
      <div className="absolute top-0 left-0 right-0 h-px"
        style={{ background: `linear-gradient(90deg, transparent, ${gold ? QV.gold : QV.accent}35, transparent)` }} />
      <div className="flex items-center justify-between">
        <span className="text-xs font-semibold tracking-widest uppercase" style={{ color: QV.textDim }}>{label}</span>
        <Icon className="w-4 h-4" style={{ color: gold ? QV.gold : QV.accent }} />
      </div>
      <div className="text-2xl font-bold" style={{ fontFamily: 'JetBrains Mono, monospace', color: QV.text }}>
        {value}
      </div>
      {sub && <div className="text-xs" style={{ color: QV.textDim }}>{sub}</div>}
    </div>
  );
}

function ActionBadge({ action }: { action: string }) {
  const isBuy = action === 'BUY';
  return (
    <span className="text-xs font-bold tracking-wider px-2 py-0.5 rounded-sm"
      style={{
        background: isBuy ? `${QV.accent}15` : `${QV.loss}15`,
        color: isBuy ? QV.accent : QV.loss,
        border: `1px solid ${isBuy ? QV.accent : QV.loss}35`,
        fontFamily: 'JetBrains Mono, monospace',
      }}>
      {action}
    </span>
  );
}

function StatusBadge({ status }: { status: string }) {
  const isOpen = status === 'OPEN';
  return (
    <span className="text-xs font-bold tracking-wider px-2 py-0.5 rounded-sm"
      style={{
        background: isOpen ? `${QV.gold}10` : `${QV.muted}15`,
        color: isOpen ? QV.gold : QV.textDim,
        border: `1px solid ${isOpen ? QV.gold : QV.muted}35`,
        fontFamily: 'JetBrains Mono, monospace',
      }}>
      {isOpen ? '● OPEN' : '○ CLOSED'}
    </span>
  );
}

// ─── Main Dashboard ───────────────────────────────────────────────────────────
export default function RealDashboard() {
  const [lastUpdate, setLastUpdate] = useState<string>(new Date().toLocaleTimeString());
  const [marketData, setMarketData] = useState<Record<string, any>>({});
  const [marketFilter, setMarketFilter] = useState<'all' | 'crypto' | 'b3' | 'nyse'>('all');
  const [historySearch, setHistorySearch] = useState('');
  const [historyActionFilter, setHistoryActionFilter] = useState<'all' | 'BUY' | 'SELL'>('all');
  const [isConnected, setIsConnected] = useState(true);
  const [pricesSentAt, setPricesSentAt] = useState(0);

  // Mutation para enviar preços ao servidor (contorna bloqueio geográfico da Binance)
  const updatePrices = trpc.prices.updateFromBrowser.useMutation();

  // Data queries
  const { data: openTrades, refetch: refetchOpenTrades } = trpc.sofia.getOpenTrades.useQuery(undefined, { refetchInterval: 5000 });
  const { data: dailyStats } = trpc.sofia.getDailyStats.useQuery(undefined, { refetchInterval: 10000 });
  const now = useMemo(() => new Date(), []);
  const { data: monthlyStats } = trpc.sofia.getMonthlyStats.useQuery({ year: now.getFullYear(), month: now.getMonth() + 1 }, { refetchInterval: 10000 });
  const { data: yearlyStats } = trpc.sofia.getYearlyStats.useQuery({ year: now.getFullYear() }, { refetchInterval: 10000 });
  const { data: totalYearlyPnL } = trpc.sofia.getTotalYearlyPnL.useQuery(undefined, { refetchInterval: 10000 });
  const { data: closedTrades } = trpc.sofia.getClosedTrades.useQuery(undefined, { refetchInterval: 30000 });
  const { data: globalStats } = trpc.sofia.getGlobalStats.useQuery(undefined, { refetchInterval: 10000 });
  const { data: historicalData } = trpc.sofia.getHistoricalPnL.useQuery({ days: 30 }, { refetchInterval: 60000 });
  const { data: sofiaAnalyses } = trpc.sofia.getSofiaAnalyses.useQuery({ limit: 10 }, { refetchInterval: 30000 });
  const { data: allTrades } = trpc.sofia.getTrades.useQuery({ limit: 100 }, { refetchInterval: 60000 });
  const { data: stocks } = trpc.stocks.getAllStocks.useQuery(undefined, { refetchInterval: 5000 });
  const { data: marketStatus } = trpc.stocks.getMarketStatus.useQuery(undefined, { refetchInterval: 60000 });
  const { data: mlStats } = trpc.ml.getPerformanceStats.useQuery();
  const { data: trainingHistory } = trpc.ml.getTrainingHistory.useQuery();
  const { data: marketComparison } = trpc.ml.getMarketComparison.useQuery();
  const { data: featureImportance } = trpc.ml.getFeatureImportance.useQuery();

  // Derived metrics
  const initialCapital = globalStats?.initialCapital || 1000000;
  const currentCapital = globalStats?.currentCapital || initialCapital;
  const closedPnL = globalStats?.totalPnl || 0;
  const winRate = globalStats?.winRate || 0;
  const openTradesCount = openTrades?.length || 0;
  const totalInvested = openTrades?.reduce((sum, t) => sum + parseFloat(t.entryPrice) * parseFloat(t.quantity), 0) || 0;

  // P&L das trades abertas calculado em tempo real com preços do browser
  const openTradesPnL = useMemo(() => {
    if (!openTrades || openTrades.length === 0) return 0;
    return openTrades.reduce((sum, trade) => {
      const entryPrice = parseFloat(trade.entryPrice);
      const quantity = parseFloat(trade.quantity);
      const positionSize = entryPrice * quantity;
      const livePrice = marketData[trade.symbol]?.price;
      if (!livePrice) return sum + (trade.pnl ? parseFloat(trade.pnl) : 0);
      const pnlPct = trade.recommendation === 'BUY'
        ? (livePrice - entryPrice) / entryPrice
        : (entryPrice - livePrice) / entryPrice;
      return sum + pnlPct * positionSize;
    }, 0);
  }, [openTrades, marketData]);

  // Total P&L = trades fechadas + trades abertas em tempo real
  const totalPnL = closedPnL + openTradesPnL;
  const gainPercent = (totalPnL / initialCapital) * 100;

  const getMarketType = (symbol: string): 'crypto' | 'b3' | 'nyse' => {
    if (symbol.endsWith('USDT')) return 'crypto';
    if (symbol.endsWith('.SA')) return 'b3';
    return 'nyse';
  };

  const filteredOpenTrades = useMemo(() => {
    if (!openTrades) return [];
    if (marketFilter === 'all') return openTrades;
    return openTrades.filter(t => getMarketType(t.symbol) === marketFilter);
  }, [openTrades, marketFilter]);

  const filteredTrades = useMemo(() => {
    if (!allTrades) return [];
    return allTrades.filter(t => {
      const matchSearch = historySearch === '' || t.symbol.toLowerCase().includes(historySearch.toLowerCase());
      const matchAction = historyActionFilter === 'all' || t.recommendation === historyActionFilter;
      return matchSearch && matchAction;
    });
  }, [allTrades, historySearch, historyActionFilter]);

  const exportCSV = useCallback(() => {
    if (!allTrades || allTrades.length === 0) return;
    const headers = ['ID', 'Symbol', 'Action', 'Confidence', 'Entry', 'Exit', 'Qty', 'PnL_USD', 'PnL_%', 'Status', 'Close_Reason', 'Duration_min', 'Opened_At', 'Closed_At'];
    const rows = allTrades.map(t => [
      t.id, t.symbol, t.recommendation, t.confidence,
      t.entryPrice, t.exitPrice || '', t.quantity,
      t.pnl || '', t.pnlPercent || '', t.status,
      t.closeReason || '', t.duration || '',
      new Date(t.openedAt).toLocaleString(),
      t.closedAt ? new Date(t.closedAt).toLocaleString() : ''
    ]);
    const csv = [headers, ...rows].map(r => r.map(v => `"${String(v).replace(/"/g, '""')}"`).join(',')).join('\n');
    const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a'); a.href = url;
    a.download = `egreja_investment_${new Date().toISOString().split('T')[0]}.csv`; a.click();
    URL.revokeObjectURL(url);
  }, [allTrades]);

  const fetchMarketData = async () => {
    try {
      const fixedCryptoSymbols = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'ADAUSDT', 'XRPUSDT', 'LTCUSDT', 'DOGEUSDT', 'MATICUSDT', 'SOLUSDT', 'DOTUSDT', 'AVAXUSDT', 'LINKUSDT', 'ATOMUSDT', 'UNIUSDT', 'FILUSDT', 'PAXGUSDT'];
      const tradeSymbols = openTrades ? openTrades.map(t => t.symbol) : [];
      const allSymbols = Array.from(new Set([...fixedCryptoSymbols, ...tradeSymbols]));
      const cryptoSymbols = allSymbols.filter(s => s.endsWith('USDT'));
      const newData: Record<string, any> = {};
      for (const symbol of cryptoSymbols) {
        try {
          const res = await fetch(`https://api.binance.com/api/v3/ticker/24hr?symbol=${symbol}`);
          if (res.ok) {
            const d = await res.json();
            newData[symbol] = { price: parseFloat(d.lastPrice), change24h: parseFloat(d.priceChangePercent), high24h: parseFloat(d.highPrice), low24h: parseFloat(d.lowPrice) };
          }
        } catch { /* silent */ }
      }
      if (stocks && stocks.length > 0) {
        stocks.forEach((stock: any) => {
          [stock.symbol, `${stock.symbol}.SA`].forEach(sym => {
            newData[sym] = { price: stock.price, change24h: stock.change_percent || 0, high24h: stock.high || stock.price, low24h: stock.low || stock.price };
          });
        });
      }
      setMarketData(newData);
      setLastUpdate(new Date().toLocaleTimeString());
      setIsConnected(true);

      // Enviar preços ao servidor a cada 10s para fechar trades com P&L correto
      const now = Date.now();
      if (now - pricesSentAt > 10000 && Object.keys(newData).length > 0) {
        const prices: Record<string, number> = {};
        for (const [sym, d] of Object.entries(newData)) {
          if (d?.price) prices[sym] = d.price;
        }
        updatePrices.mutate({ prices });
        setPricesSentAt(now);
      }
    } catch { setIsConnected(false); }
  };

  useEffect(() => {
    fetchMarketData();
    const interval = setInterval(fetchMarketData, 2000);
    return () => clearInterval(interval);
  }, [openTrades, stocks]);

  // ── Render ────────────────────────────────────────────────────────────────
  return (
    <div className="min-h-screen" style={{ background: QV.surface1, color: QV.text }}>

      {/* HEADER */}
      <header style={{ background: '#040810', borderBottom: `1px solid ${QV.border}` }}>
        <div className="px-6 py-3 flex items-center justify-between">
          {/* Brand */}
          <div className="flex items-center gap-3">
            <div className="flex items-center justify-center w-9 h-9 rounded-lg"
              style={{ background: `linear-gradient(135deg, ${QV.gold}, ${QV.goldDim})` }}>
              <Shield className="w-5 h-5" style={{ color: '#040810' }} />
            </div>
            <div>
              <div className="flex items-baseline gap-1.5">
                <span className="text-xl font-bold tracking-tight" style={{ color: QV.text }}>Egreja</span>
                <span className="text-xl font-bold tracking-tight" style={{ color: QV.gold }}>Investment AI</span>
                <span className="text-xs font-bold px-1.5 py-0.5 rounded ml-1"
                  style={{ background: `${QV.gold}18`, color: QV.gold, border: `1px solid ${QV.gold}35` }}>PRO</span>
              </div>
              <p className="text-xs tracking-widest uppercase" style={{ color: QV.textDim }}>
                Egreja Group Family Office
              </p>
            </div>
          </div>

          {/* Controls */}
          <div className="flex items-center gap-4">
            <div className="flex items-center gap-1.5">
              <span className="w-2 h-2 rounded-full" style={{
                background: isConnected ? QV.profit : QV.loss,
                animation: 'qv-pulse 2s ease-in-out infinite'
              }} />
              <span className="text-xs font-bold tracking-wider" style={{ color: isConnected ? QV.profit : QV.loss }}>
                {isConnected ? 'LIVE' : 'OFFLINE'}
              </span>
            </div>
            <div className="flex items-center gap-1.5" style={{ color: QV.textDim }}>
              <Clock className="w-3.5 h-3.5" />
              <span className="text-xs" style={{ fontFamily: 'JetBrains Mono, monospace' }}>{lastUpdate}</span>
            </div>
            <button onClick={() => { refetchOpenTrades(); fetchMarketData(); }}
              className="flex items-center gap-1.5 text-xs px-3 py-1.5 rounded font-semibold transition-all hover:opacity-80"
              style={{ background: QV.surface3, color: QV.textDim, border: `1px solid ${QV.border}` }}>
              <RefreshCw className="w-3 h-3" />
              Refresh
            </button>
          </div>
        </div>

        {/* Market status bar */}
        <div className="px-6 py-2 flex items-center gap-3 text-xs flex-wrap"
          style={{ background: '#020608', borderTop: `1px solid ${QV.border}` }}>
          <span className="font-bold tracking-widest uppercase" style={{ color: QV.textDim }}>Markets</span>
          <div className="flex items-center gap-1.5 px-2.5 py-1 rounded"
            style={{ background: `${QV.profit}10`, border: `1px solid ${QV.profit}25` }}>
            <span className="w-1.5 h-1.5 rounded-full" style={{ background: QV.profit, animation: 'qv-pulse 2s ease-in-out infinite' }} />
            <span style={{ color: QV.profit }} className="font-bold">CRYPTO</span>
            <span style={{ color: `${QV.profit}70` }}>24/7</span>
          </div>
          {marketStatus ? (
            <>
              {[
                { key: 'b3', label: 'B3', hours: '10h-17h BRT', data: marketStatus.b3 },
                { key: 'nyse', label: 'NYSE', hours: '9h30-16h EST', data: marketStatus.nyse },
              ].map(({ key, label, hours, data }) => (
                <div key={key} className="flex items-center gap-1.5 px-2.5 py-1 rounded"
                  style={{
                    background: data?.is_open ? `${QV.profit}10` : `${QV.loss}10`,
                    border: `1px solid ${data?.is_open ? QV.profit : QV.loss}25`
                  }}>
                  <span className="w-1.5 h-1.5 rounded-full" style={{ background: data?.is_open ? QV.profit : QV.loss }} />
                  <span style={{ color: data?.is_open ? QV.profit : QV.loss }} className="font-bold">{label}</span>
                  <span style={{ color: QV.textDim }}>{data?.is_open ? 'OPEN' : 'CLOSED'} {hours}</span>
                </div>
              ))}
            </>
          ) : <span style={{ color: QV.textDim }}>Loading...</span>}
          <span className="ml-auto" style={{ color: QV.muted }}>Updates every 2s</span>
        </div>
      </header>

      {/* METRICS STRIP */}
      <div className="px-6 py-5 grid grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-4"
        style={{ background: '#070B12', borderBottom: `1px solid ${QV.border}` }}>
        <StatCard label="Portfolio Value" icon={DollarSign} gold
          value={<span style={{ color: QV.gold }}>${((initialCapital + totalPnL) / 1000).toFixed(0)}k</span>}
          sub={<span style={{ color: gainPercent >= 0 ? QV.profit : QV.loss }} className="flex items-center gap-1">
            {gainPercent >= 0 ? <ChevronUp className="w-3 h-3" /> : <ChevronDown className="w-3 h-3" />}
            {gainPercent >= 0 ? '+' : ''}{gainPercent.toFixed(2)}% all-time
          </span>}
        />
        <StatCard label="Total P&L" icon={Activity}
          value={<span style={{ color: totalPnL >= 0 ? QV.profit : QV.loss }}>
            {totalPnL >= 0 ? '+' : ''}${Math.abs(totalPnL).toLocaleString('en-US', { maximumFractionDigits: 0 })}
          </span>}
          sub={<span style={{ color: totalPnL >= 0 ? QV.profit : QV.loss }}>{gainPercent >= 0 ? '+' : ''}{gainPercent.toFixed(2)}%</span>}
        />
        <StatCard label="Win Rate" icon={Target}
          value={<span style={{ color: winRate >= 55 ? QV.profit : winRate >= 45 ? QV.gold : QV.loss }}>{winRate.toFixed(1)}%</span>}
          sub={`${dailyStats?.totalTrades || 0} trades total`}
        />
        <StatCard label="Open Positions" icon={Zap}
          value={openTradesCount}
          sub="Active trades"
        />
        <StatCard label="Capital Deployed" icon={Database}
          value={`$${(totalInvested / 1000).toFixed(0)}k`}
          sub={`${openTradesCount} positions`}
        />
      </div>

      {/* MAIN TABS */}
      <div className="px-6 py-6">
        <Tabs defaultValue="overview" className="w-full">
          <TabsList className="w-full justify-start gap-0 rounded-none border-b h-auto p-0 mb-6"
            style={{ background: 'transparent', borderColor: QV.border }}>
            {[
              { value: 'overview', label: 'Overview', icon: Activity },
              { value: 'performance', label: 'Performance', icon: BarChart3 },
              { value: 'history', label: 'Trade History', icon: Clock },
              { value: 'ml', label: 'ML Intelligence', icon: Brain },
              { value: 'insights', label: 'AI Insights', icon: Zap },
              { value: 'markets', label: 'Markets', icon: Globe },
              { value: 'stocks', label: 'Equities', icon: TrendingUp },
            ].map(({ value, label, icon: Icon }) => (
              <TabsTrigger key={value} value={value}
                className="relative rounded-none border-b-2 border-transparent px-4 py-3 text-xs font-bold tracking-wider uppercase transition-all data-[state=active]:border-b-2 data-[state=active]:shadow-none data-[state=active]:bg-transparent"
                style={{ color: QV.textDim, background: 'transparent' }}>
                <span className="flex items-center gap-1.5">
                  <Icon className="w-3.5 h-3.5" />
                  {label}
                  {value === 'ml' && (
                    <span className="text-xs px-1 py-0.5 rounded"
                      style={{ background: `${QV.gold}18`, color: QV.gold, fontSize: '0.55rem', border: `1px solid ${QV.gold}30` }}>AI</span>
                  )}
                </span>
              </TabsTrigger>
            ))}
          </TabsList>

          {/* ── OVERVIEW ─────────────────────────────────────────────────── */}
          <TabsContent value="overview" className="space-y-6 mt-0">
            <div className="rounded-lg overflow-hidden"
              style={{ background: QV.surface2, border: `1px solid ${QV.border}` }}>
              <div className="px-5 py-4 flex items-center justify-between"
                style={{ borderBottom: `1px solid ${QV.border}` }}>
                <div>
                  <h2 className="text-sm font-bold tracking-widest uppercase" style={{ color: QV.text }}>Open Positions</h2>
                  <p className="text-xs mt-0.5" style={{ color: QV.textDim }}>{openTradesCount} active · P&L live every 2s</p>
                </div>
                <div className="flex gap-2">
                  {(['all', 'crypto', 'b3', 'nyse'] as const).map(f => (
                    <button key={f} onClick={() => setMarketFilter(f)}
                      className="text-xs px-3 py-1.5 rounded font-bold tracking-wider uppercase transition-all"
                      style={{
                        background: marketFilter === f ? `${QV.gold}18` : 'transparent',
                        color: marketFilter === f ? QV.gold : QV.textDim,
                        border: `1px solid ${marketFilter === f ? QV.gold + '45' : QV.border}`,
                      }}>
                      {f === 'all' ? `All (${openTradesCount})` : f.toUpperCase()}
                    </button>
                  ))}
                </div>
              </div>
              <div className="overflow-x-auto">
                <table className="w-full">
                  <thead>
                    <tr style={{ borderBottom: `1px solid ${QV.border}` }}>
                      {['Symbol', 'Direction', 'Size', 'Entry', 'Current', 'P&L', 'Status'].map(h => (
                        <th key={h} className="px-4 py-3 text-left text-xs font-bold tracking-widest uppercase"
                          style={{ color: QV.textDim }}>{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {filteredOpenTrades && filteredOpenTrades.length > 0 ? filteredOpenTrades.map(trade => {
                      const entryPrice = parseFloat(trade.entryPrice);
                      const quantity = parseFloat(trade.quantity);
                      const positionSize = entryPrice * quantity;
                      // Sempre calcular P&L em tempo real com preço atual do browser
                      // O browser acessa a Binance diretamente (sem bloqueio geográfico)
                      const livePrice = marketData[trade.symbol]?.price;
                      const currentPrice = livePrice || entryPrice;
                      const hasLivePrice = !!livePrice;
                      const pnlPct = trade.recommendation === 'BUY'
                        ? (currentPrice - entryPrice) / entryPrice
                        : (entryPrice - currentPrice) / entryPrice;
                      // Usar P&L calculado em tempo real (não o do banco que pode estar desatualizado)
                      const pnl = hasLivePrice ? pnlPct * positionSize : (trade.pnl ? parseFloat(trade.pnl) : 0);
                      const pnlPercent = hasLivePrice ? pnlPct * 100 : (trade.pnlPercent ? parseFloat(trade.pnlPercent) : 0);
                      const isPos = pnl >= 0;
                      return (
                        <tr key={trade.id} className="transition-colors hover:bg-white/[0.02]"
                          style={{ borderBottom: `1px solid ${QV.border}50` }}>
                          <td className="px-4 py-3 font-bold text-sm"
                            style={{ color: QV.text, fontFamily: 'JetBrains Mono, monospace' }}>{trade.symbol}</td>
                          <td className="px-4 py-3"><ActionBadge action={trade.recommendation} /></td>
                          <td className="px-4 py-3 text-sm"
                            style={{ color: QV.textDim, fontFamily: 'JetBrains Mono, monospace' }}>
                            ${(positionSize / 1000).toFixed(1)}k
                          </td>
                          <td className="px-4 py-3 text-sm"
                            style={{ color: QV.textDim, fontFamily: 'JetBrains Mono, monospace' }}>
                            {formatPrice(trade.entryPrice)}
                          </td>
                          <td className="px-4 py-3 text-sm font-semibold"
                            style={{ color: QV.text, fontFamily: 'JetBrains Mono, monospace' }}>
                            {formatPrice(currentPrice)}
                          </td>
                          <td className="px-4 py-3">
                            <div className="flex flex-col items-end">
                              <span className="text-sm font-bold"
                                style={{ color: isPos ? QV.profit : QV.loss, fontFamily: 'JetBrains Mono, monospace' }}>
                                {isPos ? '+' : ''}${Math.abs(pnl).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                              </span>
                              <span className="text-xs" style={{ color: isPos ? QV.profit : QV.loss }}>
                                {isPos ? '+' : ''}{pnlPercent.toFixed(2)}%
                              </span>
                            </div>
                          </td>
                          <td className="px-4 py-3"><StatusBadge status={trade.status} /></td>
                        </tr>
                      );
                    }) : (
                      <tr>
                        <td colSpan={7} className="px-4 py-12 text-center text-sm" style={{ color: QV.textDim }}>
                          No open positions at this time
                        </td>
                      </tr>
                    )}
                  </tbody>
                </table>
              </div>
            </div>
          </TabsContent>

          {/* ── PERFORMANCE ──────────────────────────────────────────────── */}
          <TabsContent value="performance" className="space-y-6 mt-0">
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              {[
                { label: 'Daily P&L', pnl: dailyStats?.totalPnl || 0, trades: dailyStats?.totalTrades || 0, win: dailyStats?.winRate || 0, wins: dailyStats?.winningTrades || 0, losses: dailyStats?.losingTrades || 0 },
                { label: 'Monthly P&L', pnl: monthlyStats?.totalPnl || 0, trades: monthlyStats?.totalTrades || 0, win: monthlyStats?.winRate || 0, wins: monthlyStats?.winningTrades || 0, losses: monthlyStats?.losingTrades || 0 },
                { label: 'Annual P&L', pnl: totalYearlyPnL || 0, trades: yearlyStats?.totalTrades || 0, win: yearlyStats?.winRate || 0, wins: yearlyStats?.winningTrades || 0, losses: yearlyStats?.losingTrades || 0 },
              ].map(({ label, pnl, trades, win, wins, losses }) => (
                <div key={label} className="rounded-lg p-5 space-y-4"
                  style={{ background: QV.surface2, border: `1px solid ${QV.border}` }}>
                  <div className="flex items-center justify-between">
                    <span className="text-xs font-bold tracking-widest uppercase" style={{ color: QV.textDim }}>{label}</span>
                    <span className="text-xs px-2 py-0.5 rounded"
                      style={{ background: `${QV.gold}12`, color: QV.gold, border: `1px solid ${QV.gold}28` }}>
                      {win.toFixed(1)}% win
                    </span>
                  </div>
                  <div className="text-3xl font-bold"
                    style={{ color: pnl >= 0 ? QV.profit : QV.loss, fontFamily: 'JetBrains Mono, monospace' }}>
                    {pnl >= 0 ? '+' : ''}${Math.abs(pnl).toLocaleString('en-US', { maximumFractionDigits: 2 })}
                  </div>
                  <div className="grid grid-cols-3 gap-2 pt-2" style={{ borderTop: `1px solid ${QV.border}` }}>
                    {[{ v: trades, l: 'Trades', c: QV.text }, { v: wins, l: 'Wins', c: QV.profit }, { v: losses, l: 'Losses', c: QV.loss }].map(({ v, l, c }) => (
                      <div key={l} className="text-center">
                        <div className="text-lg font-bold" style={{ color: c }}>{v}</div>
                        <div className="text-xs" style={{ color: QV.textDim }}>{l}</div>
                      </div>
                    ))}
                  </div>
                </div>
              ))}
            </div>

            {closedTrades && closedTrades.length > 0 && (
              <CapitalEvolutionChart
                trades={closedTrades.map(t => ({ id: String(t.id), symbol: t.symbol, pnl: t.pnl || '0', closedAt: t.closedAt! }))}
                initialCapital={initialCapital}
              />
            )}

            {openTrades && openTrades.length > 0 && (
              <TradeDistributionCharts trades={[...openTrades, ...(closedTrades || [])].map(t => ({ ...t, id: String(t.id) }))} />
            )}

            <div className="rounded-lg p-5" style={{ background: QV.surface2, border: `1px solid ${QV.border}` }}>
              <h3 className="text-xs font-bold tracking-widest uppercase mb-4" style={{ color: QV.textDim }}>
                P&L Evolution — Last 30 Days
              </h3>
              <ResponsiveContainer width="100%" height={280}>
                <AreaChart data={historicalData || []}>
                  <defs>
                    <linearGradient id="pnlGrad" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%" stopColor={QV.gold} stopOpacity={0.22} />
                      <stop offset="95%" stopColor={QV.gold} stopOpacity={0} />
                    </linearGradient>
                  </defs>
                  <CartesianGrid strokeDasharray="3 3" stroke={QV.border} />
                  <XAxis dataKey="date" stroke={QV.muted} tick={{ fill: QV.textDim, fontSize: 11 }} />
                  <YAxis stroke={QV.muted} tick={{ fill: QV.textDim, fontSize: 11 }} />
                  <RechartsTooltip contentStyle={{ background: QV.surface3, border: `1px solid ${QV.border}`, borderRadius: 6, color: QV.text }} />
                  <Area type="monotone" dataKey="pnl" stroke={QV.gold} strokeWidth={2} fillOpacity={1} fill="url(#pnlGrad)" name="P&L ($)" />
                </AreaChart>
              </ResponsiveContainer>
            </div>
          </TabsContent>

          {/* ── TRADE HISTORY ────────────────────────────────────────────── */}
          <TabsContent value="history" className="mt-0">
            <div className="rounded-lg overflow-hidden" style={{ background: QV.surface2, border: `1px solid ${QV.border}` }}>
              <div className="px-5 py-4 flex flex-col sm:flex-row items-start sm:items-center gap-3"
                style={{ borderBottom: `1px solid ${QV.border}` }}>
                <div>
                  <h2 className="text-sm font-bold tracking-widest uppercase" style={{ color: QV.text }}>Trade History</h2>
                  <p className="text-xs mt-0.5" style={{ color: QV.textDim }}>{filteredTrades.length} of {allTrades?.length || 0} trades</p>
                </div>
                <div className="flex items-center gap-2 ml-auto flex-wrap">
                  <div className="relative">
                    <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 w-3.5 h-3.5" style={{ color: QV.muted }} />
                    <Input placeholder="Search symbol..." value={historySearch}
                      onChange={e => setHistorySearch(e.target.value)}
                      className="pl-8 h-8 text-xs w-36"
                      style={{ background: QV.surface3, border: `1px solid ${QV.border}`, color: QV.text }} />
                  </div>
                  {(['all', 'BUY', 'SELL'] as const).map(f => (
                    <button key={f} onClick={() => setHistoryActionFilter(f)}
                      className="text-xs px-3 py-1.5 rounded font-bold tracking-wider uppercase transition-all"
                      style={{
                        background: historyActionFilter === f ? `${QV.gold}18` : 'transparent',
                        color: historyActionFilter === f ? QV.gold : QV.textDim,
                        border: `1px solid ${historyActionFilter === f ? QV.gold + '45' : QV.border}`,
                      }}>
                      {f}
                    </button>
                  ))}
                  <button onClick={exportCSV}
                    className="flex items-center gap-1.5 text-xs px-3 py-1.5 rounded font-bold transition-all"
                    style={{ background: `${QV.gold}12`, color: QV.gold, border: `1px solid ${QV.gold}35` }}>
                    <Download className="w-3 h-3" />CSV
                  </button>
                </div>
              </div>

              <TooltipProvider>
                <div className="overflow-x-auto">
                  <table className="w-full">
                    <thead>
                      <tr style={{ borderBottom: `1px solid ${QV.border}` }}>
                        {['Symbol', 'Direction', 'Entry', 'Exit', 'P&L', 'P&L %', 'Status', 'Date', ''].map(h => (
                          <th key={h} className="px-4 py-3 text-left text-xs font-bold tracking-widest uppercase"
                            style={{ color: QV.textDim }}>{h}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {filteredTrades.length > 0 ? filteredTrades.map(trade => {
                        const pnl = parseFloat(trade.pnl || '0');
                        const pnlPct = parseFloat(trade.pnlPercent || '0');
                        const isPos = pnl >= 0;
                        return (
                          <tr key={trade.id} className="transition-colors hover:bg-white/[0.02]"
                            style={{ borderBottom: `1px solid ${QV.border}40` }}>
                            <td className="px-4 py-3 font-bold text-sm"
                              style={{ color: QV.text, fontFamily: 'JetBrains Mono, monospace' }}>{trade.symbol}</td>
                            <td className="px-4 py-3"><ActionBadge action={trade.recommendation} /></td>
                            <td className="px-4 py-3 text-xs"
                              style={{ color: QV.textDim, fontFamily: 'JetBrains Mono, monospace' }}>
                              {formatPrice(trade.entryPrice)}
                            </td>
                            <td className="px-4 py-3 text-xs"
                              style={{ color: QV.textDim, fontFamily: 'JetBrains Mono, monospace' }}>
                              {trade.exitPrice ? formatPrice(trade.exitPrice) : <span style={{ color: QV.muted }}>—</span>}
                            </td>
                            <td className="px-4 py-3 text-sm font-bold"
                              style={{ color: isPos ? QV.profit : QV.loss, fontFamily: 'JetBrains Mono, monospace' }}>
                              {isPos ? '+' : ''}${Math.abs(pnl).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                            </td>
                            <td className="px-4 py-3 text-sm font-semibold"
                              style={{ color: isPos ? QV.profit : QV.loss, fontFamily: 'JetBrains Mono, monospace' }}>
                              {isPos ? '+' : ''}{pnlPct.toFixed(2)}%
                            </td>
                            <td className="px-4 py-3"><StatusBadge status={trade.status} /></td>
                            <td className="px-4 py-3 text-xs" style={{ color: QV.textDim }}>
                              {new Date(trade.openedAt).toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: '2-digit' })}
                            </td>
                            <td className="px-4 py-3">
                              <Tooltip>
                                <TooltipTrigger asChild>
                                  <button className="p-1 rounded hover:bg-white/5">
                                    <Info className="w-3.5 h-3.5" style={{ color: QV.muted }} />
                                  </button>
                                </TooltipTrigger>
                                <TooltipContent side="left" className="text-xs space-y-1.5 p-3 max-w-xs"
                                  style={{ background: QV.surface3, border: `1px solid ${QV.border}`, color: QV.text }}>
                                  <div className="font-bold mb-2" style={{ color: QV.gold }}>{trade.symbol} — Details</div>
                                  <div className="flex justify-between gap-6"><span style={{ color: QV.textDim }}>AI Confidence</span><span className="font-semibold">{trade.confidence}%</span></div>
                                  <div className="flex justify-between gap-6"><span style={{ color: QV.textDim }}>Quantity</span><span style={{ fontFamily: 'JetBrains Mono, monospace' }}>{parseFloat(trade.quantity).toFixed(4)}</span></div>
                                  {trade.closeReason && (
                                    <div className="flex justify-between gap-6">
                                      <span style={{ color: QV.textDim }}>Close Reason</span>
                                      <span className="font-bold" style={{ color: trade.closeReason === 'TAKE_PROFIT' ? QV.profit : trade.closeReason === 'STOP_LOSS' ? QV.loss : QV.gold }}>
                                        {trade.closeReason}
                                      </span>
                                    </div>
                                  )}
                                  {trade.duration && <div className="flex justify-between gap-6"><span style={{ color: QV.textDim }}>Duration</span><span>{trade.duration < 60 ? `${trade.duration}m` : `${(trade.duration / 60).toFixed(1)}h`}</span></div>}
                                  <div className="flex justify-between gap-6"><span style={{ color: QV.textDim }}>Opened</span><span>{new Date(trade.openedAt).toLocaleString()}</span></div>
                                  {trade.closedAt && <div className="flex justify-between gap-6"><span style={{ color: QV.textDim }}>Closed</span><span>{new Date(trade.closedAt).toLocaleString()}</span></div>}
                                </TooltipContent>
                              </Tooltip>
                            </td>
                          </tr>
                        );
                      }) : (
                        <tr><td colSpan={9} className="px-4 py-12 text-center text-sm" style={{ color: QV.textDim }}>No trades found</td></tr>
                      )}
                    </tbody>
                  </table>
                </div>
              </TooltipProvider>
            </div>
          </TabsContent>

          {/* ── ML INTELLIGENCE ──────────────────────────────────────────── */}
          <TabsContent value="ml" className="space-y-6 mt-0">
            {/* ML header banner */}
            <div className="flex items-center gap-4 p-5 rounded-lg"
              style={{ background: `linear-gradient(135deg, ${QV.surface2}, #0C0E00)`, border: `1px solid ${QV.border}` }}>
              <div className="flex items-center justify-center w-12 h-12 rounded-xl"
                style={{ background: `linear-gradient(135deg, ${QV.gold}28, ${QV.gold}08)`, border: `1px solid ${QV.gold}35` }}>
                <Brain className="w-6 h-6" style={{ color: QV.gold }} />
              </div>
              <div>
                <h2 className="text-lg font-bold" style={{ color: QV.text }}>ML Intelligence Engine</h2>
                <p className="text-xs" style={{ color: QV.textDim }}>Adaptive model — auto-retrained daily on trade outcomes</p>
              </div>
              <div className="ml-auto flex items-center gap-2">
                <span className="w-2 h-2 rounded-full" style={{ background: QV.profit, animation: 'qv-pulse 2s ease-in-out infinite' }} />
                <span className="text-xs font-bold tracking-wider" style={{ color: QV.profit }}>MODEL ACTIVE</span>
              </div>
            </div>

            {/* ML stat cards */}
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
              <StatCard label="Model Accuracy" icon={Target} gold
                value={<span style={{ color: QV.gold }}>{mlStats?.currentAccuracy ? `${(mlStats.currentAccuracy * 100).toFixed(1)}%` : 'N/A'}</span>}
                sub={mlStats?.lastTrainingDate ? `Trained: ${new Date(mlStats.lastTrainingDate).toLocaleDateString()}` : 'Not yet trained'}
              />
              <StatCard label="Prediction Win Rate" icon={Award}
                value={<span style={{ color: (mlStats?.winRate || 0) >= 0.55 ? QV.profit : QV.gold }}>
                  {mlStats?.winRate ? `${(mlStats.winRate * 100).toFixed(1)}%` : 'N/A'}
                </span>}
                sub={`${mlStats?.totalTrades || 0} trades analyzed`}
              />
              <StatCard label="Retrainings" icon={Cpu}
                value={mlStats?.totalRetrainings || 0}
                sub="Auto-retrain every 24h"
              />
              <StatCard label="Next Training" icon={Clock}
                value={<span className="text-base">{mlStats?.nextTrainingIn || 'Soon'}</span>}
                sub="Scheduled auto-retrain"
              />
            </div>

            {/* Charts */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
              <div className="rounded-lg p-5" style={{ background: QV.surface2, border: `1px solid ${QV.border}` }}>
                <h3 className="text-xs font-bold tracking-widest uppercase mb-4" style={{ color: QV.textDim }}>Accuracy Evolution</h3>
                {trainingHistory && trainingHistory.length > 0 ? (
                  <ResponsiveContainer width="100%" height={220}>
                    <LineChart data={trainingHistory}>
                      <CartesianGrid strokeDasharray="3 3" stroke={QV.border} />
                      <XAxis dataKey="date" stroke={QV.muted} tick={{ fill: QV.textDim, fontSize: 10 }}
                        tickFormatter={d => new Date(d).toLocaleDateString('en-US', { day: '2-digit', month: '2-digit' })} />
                      <YAxis domain={[0, 100]} stroke={QV.muted} tick={{ fill: QV.textDim, fontSize: 10 }} />
                      <RechartsTooltip contentStyle={{ background: QV.surface3, border: `1px solid ${QV.border}`, color: QV.text }}
                        formatter={(v: number) => [`${v.toFixed(1)}%`, 'Accuracy']} />
                      <Line type="monotone" dataKey="accuracy" stroke={QV.gold} strokeWidth={2} dot={{ fill: QV.gold, r: 3 }} name="Accuracy" />
                    </LineChart>
                  </ResponsiveContainer>
                ) : (
                  <div className="flex items-center justify-center h-[220px] text-sm" style={{ color: QV.textDim }}>
                    No training history yet
                  </div>
                )}
              </div>

              <div className="rounded-lg p-5" style={{ background: QV.surface2, border: `1px solid ${QV.border}` }}>
                <h3 className="text-xs font-bold tracking-widest uppercase mb-4" style={{ color: QV.textDim }}>Win Rate by Market</h3>
                {marketComparison && marketComparison.length > 0 ? (
                  <ResponsiveContainer width="100%" height={220}>
                    <BarChart data={marketComparison}>
                      <CartesianGrid strokeDasharray="3 3" stroke={QV.border} />
                      <XAxis dataKey="market" stroke={QV.muted} tick={{ fill: QV.textDim, fontSize: 10 }} />
                      <YAxis domain={[0, 100]} stroke={QV.muted} tick={{ fill: QV.textDim, fontSize: 10 }} />
                      <RechartsTooltip contentStyle={{ background: QV.surface3, border: `1px solid ${QV.border}`, color: QV.text }}
                        formatter={(v: number) => [`${v.toFixed(1)}%`, 'Win Rate']} />
                      <Bar dataKey="winRate" fill={QV.gold} name="Win Rate (%)" radius={[3, 3, 0, 0]} />
                    </BarChart>
                  </ResponsiveContainer>
                ) : (
                  <div className="flex items-center justify-center h-[220px] text-sm" style={{ color: QV.textDim }}>
                    Insufficient data
                  </div>
                )}
              </div>
            </div>

            {/* Feature importance */}
            <div className="rounded-lg p-5" style={{ background: QV.surface2, border: `1px solid ${QV.border}` }}>
              <h3 className="text-xs font-bold tracking-widest uppercase mb-4" style={{ color: QV.textDim }}>
                Feature Importance — Signal Weights
              </h3>
              {featureImportance && featureImportance.length > 0 ? (
                <div className="space-y-3">
                  {featureImportance.slice(0, 10).map((f: any, i: number) => (
                    <div key={f.feature} className="flex items-center gap-3">
                      <span className="text-xs w-36 truncate" style={{ color: QV.textDim, fontFamily: 'JetBrains Mono, monospace' }}>{f.feature}</span>
                      <div className="flex-1 h-1.5 rounded-full" style={{ background: QV.border }}>
                        <div className="h-1.5 rounded-full transition-all"
                          style={{ width: `${(f.importance * 100).toFixed(1)}%`, background: i === 0 ? QV.gold : i < 3 ? QV.accent : QV.textDim }} />
                      </div>
                      <span className="text-xs w-12 text-right" style={{ color: QV.text, fontFamily: 'JetBrains Mono, monospace' }}>
                        {(f.importance * 100).toFixed(1)}%
                      </span>
                    </div>
                  ))}
                </div>
              ) : (
                <div className="flex items-center justify-center py-10 text-sm" style={{ color: QV.textDim }}>
                  Feature data not available — model needs more training cycles
                </div>
              )}
            </div>

            {/* Market comparison table */}
            {marketComparison && marketComparison.length > 0 && (
              <div className="rounded-lg overflow-hidden" style={{ background: QV.surface2, border: `1px solid ${QV.border}` }}>
                <div className="px-5 py-4" style={{ borderBottom: `1px solid ${QV.border}` }}>
                  <h3 className="text-xs font-bold tracking-widest uppercase" style={{ color: QV.textDim }}>Market Comparison</h3>
                </div>
                <table className="w-full">
                  <thead>
                    <tr style={{ borderBottom: `1px solid ${QV.border}` }}>
                      {['Market', 'Total Trades', 'Winners', 'Win Rate', 'Avg P&L'].map(h => (
                        <th key={h} className="px-4 py-3 text-left text-xs font-bold tracking-widest uppercase"
                          style={{ color: QV.textDim }}>{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {marketComparison.map((m: any) => (
                      <tr key={m.market} className="transition-colors hover:bg-white/[0.02]"
                        style={{ borderBottom: `1px solid ${QV.border}40` }}>
                        <td className="px-4 py-3 font-bold text-sm" style={{ color: QV.text }}>{m.market}</td>
                        <td className="px-4 py-3 text-sm" style={{ color: QV.textDim }}>{m.totalTrades}</td>
                        <td className="px-4 py-3 text-sm font-semibold" style={{ color: QV.profit }}>{m.winningTrades}</td>
                        <td className="px-4 py-3 text-sm font-bold" style={{ color: m.winRate >= 50 ? QV.profit : QV.loss }}>
                          {m.winRate.toFixed(1)}%
                        </td>
                        <td className="px-4 py-3 text-sm" style={{ color: (m.avgPnl || 0) >= 0 ? QV.profit : QV.loss }}>
                          {(m.avgPnl || 0) >= 0 ? '+' : ''}${(m.avgPnl || 0).toFixed(2)}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </TabsContent>

          {/* ── AI INSIGHTS ──────────────────────────────────────────────── */}
          <TabsContent value="insights" className="mt-0">
            <EnhancedInsights analyses={sofiaAnalyses || []} />
          </TabsContent>

          {/* ── MARKETS ──────────────────────────────────────────────────── */}
          <TabsContent value="markets" className="mt-0">
            <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 xl:grid-cols-5 gap-3">
              {Object.entries(marketData).map(([symbol, data]) => (
                <div key={symbol} className="rounded-lg p-4" style={{ background: QV.surface2, border: `1px solid ${QV.border}` }}>
                  <div className="flex items-center justify-between mb-2">
                    <span className="text-xs font-bold" style={{ color: QV.text, fontFamily: 'JetBrains Mono, monospace' }}>{symbol}</span>
                    <span className="text-xs font-semibold" style={{ color: data.change24h >= 0 ? QV.profit : QV.loss }}>
                      {data.change24h >= 0 ? '+' : ''}{data.change24h.toFixed(2)}%
                    </span>
                  </div>
                  <div className="text-lg font-bold" style={{ color: QV.text, fontFamily: 'JetBrains Mono, monospace' }}>
                    {formatPrice(data.price)}
                  </div>
                  <div className="mt-2 grid grid-cols-2 gap-1 text-xs" style={{ color: QV.textDim }}>
                    <span>H: {formatPrice(data.high24h)}</span>
                    <span>L: {formatPrice(data.low24h)}</span>
                  </div>
                </div>
              ))}
            </div>
          </TabsContent>

          {/* ── EQUITIES ─────────────────────────────────────────────────── */}
          <TabsContent value="stocks" className="mt-0">
            <StocksTab stocks={stocks} marketStatus={marketStatus} />
          </TabsContent>
        </Tabs>
      </div>

      {/* FOOTER */}
      <footer style={{ background: '#040810', borderTop: `1px solid ${QV.border}`, marginTop: '2rem' }}>
        <div className="px-6 py-4 flex flex-col sm:flex-row items-center justify-between gap-2">
          <div className="flex items-center gap-2">
            <div className="flex items-center justify-center w-6 h-6 rounded"
              style={{ background: `linear-gradient(135deg, ${QV.gold}, ${QV.goldDim})` }}>
              <Shield className="w-3.5 h-3.5" style={{ color: '#040810' }} />
            </div>
            <span className="text-sm font-bold" style={{ color: QV.text }}>Egreja Investment</span>
            <span className="text-xs px-2 py-0.5 rounded"
              style={{ background: `${QV.gold}15`, color: QV.gold, border: `1px solid ${QV.gold}30` }}>
              family office Egreja Group
            </span>
          </div>
          <div className="text-xs text-center" style={{ color: QV.textDim }}>
            Desenvolvido by{' '}
            <span className="font-semibold" style={{ color: QV.gold }}>Estrela Digital</span>
          </div>
          <div className="text-xs" style={{ color: QV.textDim }}>
            &copy; {new Date().getFullYear()} Egreja Group &middot; Todos os direitos reservados
          </div>
        </div>
      </footer>

    </div>
  );
}
