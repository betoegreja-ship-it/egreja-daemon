import { useState, useEffect, useCallback } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { Button } from '@/components/ui/button';
import {
  LineChart, Line, BarChart, Bar,
  XAxis, YAxis, CartesianGrid, Tooltip as RechartsTooltip,
  ResponsiveContainer, PieChart, Pie, Cell
} from 'recharts';
import {
  Activity, TrendingUp, TrendingDown, RefreshCw, Shield, Zap,
  BarChart3, Target, AlertTriangle, CheckCircle, Clock, ArrowUpRight,
  ArrowDownRight, Layers, Globe, ChevronUp, ChevronDown
} from 'lucide-react';

// ─── QuantVault Design Tokens ───────────────────────────────────────
const QV = {
  gold: '#C9A84C',
  goldDim: '#7A5C1A',
  profit: '#22C55E',
  loss: '#EF4444',
  accent: '#60A5FA',
  purple: '#A78BFA',
  cyan: '#22D3EE',
  orange: '#FB923C',
  surface1: '#060A10',
  surface2: '#0B1018',
  surface3: '#111820',
  border: '#1A2332',
  muted: '#374151',
  text: '#E2E8F0',
  textDim: '#64748B',
};

const STRATEGY_META: Record<string, { label: string; color: string; icon: typeof Activity; desc: string }> = {
  PCP:          { label: 'Put-Call Parity',      color: QV.accent,  icon: Target,       desc: 'Arbitragem de paridade put-call (B3: Calls AM, Puts EU)' },
  FST:          { label: 'Futuro Sintético',     color: QV.purple,  icon: Layers,       desc: 'Futuro Sintético Triangular com calibração Caso 0' },
  ROLL_ARB:     { label: 'Roll Arbitrage',       color: QV.cyan,    icon: RefreshCw,    desc: 'Spread entre futuros de vencimentos consecutivos' },
  ETF_BASKET:   { label: 'ETF vs Basket',        color: QV.gold,    icon: BarChart3,    desc: 'BOVA11 NAV vs preço (premium/discount)' },
  SKEW_ARB:     { label: 'Skew Arbitrage',       color: QV.orange,  icon: TrendingUp,   desc: 'Risk reversal IV skew z-score' },
  INTERLISTED:  { label: 'ADR Inter-Listed',     color: '#EC4899',  icon: Globe,        desc: 'PETR4/PBR e VALE3/VALE com hedge FX' },
  DIVIDEND_ARB: { label: 'Dividend Arbitrage',   color: QV.profit,  icon: Zap,          desc: 'Captura de dividendos com hedge de put' },
  VOL_ARB:      { label: 'Vol Arbitrage',        color: '#F43F5E',  icon: Activity,     desc: 'IV vs Realized Vol com delta-hedge' },
};

const STATUS_LABELS: Record<number, string> = { 0: 'OBSERVE', 1: 'SHADOW', 2: 'PAPER_SMALL', 3: 'PAPER_FULL' };
const STATUS_COLORS: Record<number, string> = { 0: QV.textDim, 1: QV.orange, 2: QV.accent, 3: QV.profit };

// API base — same origin (Flask serves at /strategies/*)
const API_BASE = '/strategies';

// ─── Types ──────────────────────────────────────────────────────────

interface StrategyHealth {
  healthy: boolean;
  last_heartbeat: string | null;
  opportunities_1h: number;
}

interface HealthData {
  overall_health: string;
  strategies: Record<string, StrategyHealth>;
  timestamp: string;
}

interface Opportunity {
  id: number;
  strategy_type: string;
  symbol: string;
  strike: number | null;
  expiry: string | null;
  opportunity_type: string | null;
  expected_edge_bps: number | null;
  liquidity_score: number | null;
  decision: string | null;
  timestamp: string;
}

interface StatusEntry {
  symbol: string;
  strategy_type: string;
  active_status: number;
  updated_at: string;
}

interface LiquidityScore {
  [symbol: string]: number;
}

interface Scorecard {
  strategy_type: string;
  symbol: string;
  period: string;
  opportunities_seen: number;
  opportunities_approved: number;
  trades_executed: number;
  pnl_total: number;
  sharpe: number;
  fill_ratio: number;
  timestamp: string;
}

// ─── Sub-components ─────────────────────────────────────────────────

function StatCard({ label, value, sub, icon: Icon, color = QV.accent }: {
  label: string; value: React.ReactNode; sub?: React.ReactNode;
  icon: React.ElementType; color?: string;
}) {
  return (
    <div className="relative overflow-hidden rounded-lg p-4 flex flex-col gap-2"
      style={{ background: QV.surface2, border: `1px solid ${QV.border}` }}>
      <div className="absolute top-0 left-0 right-0 h-px"
        style={{ background: `linear-gradient(90deg, transparent, ${color}35, transparent)` }} />
      <div className="flex items-center justify-between">
        <span className="text-xs font-semibold tracking-widest uppercase" style={{ color: QV.textDim }}>{label}</span>
        <Icon className="w-4 h-4" style={{ color }} />
      </div>
      <div className="text-2xl font-bold" style={{ fontFamily: 'JetBrains Mono, monospace', color: QV.text }}>
        {value}
      </div>
      {sub && <div className="text-xs" style={{ color: QV.textDim }}>{sub}</div>}
    </div>
  );
}

function HealthBadge({ healthy }: { healthy: boolean }) {
  return (
    <span className="inline-flex items-center gap-1.5 text-xs font-bold px-2.5 py-1 rounded-full"
      style={{
        background: healthy ? `${QV.profit}15` : `${QV.loss}15`,
        color: healthy ? QV.profit : QV.loss,
        border: `1px solid ${healthy ? QV.profit : QV.loss}30`,
      }}>
      {healthy ? <CheckCircle className="w-3 h-3" /> : <AlertTriangle className="w-3 h-3" />}
      {healthy ? 'HEALTHY' : 'DEGRADED'}
    </span>
  );
}

function StatusBadge({ status }: { status: number }) {
  const label = STATUS_LABELS[status] || 'UNKNOWN';
  const color = STATUS_COLORS[status] || QV.textDim;
  return (
    <span className="text-xs font-bold tracking-wider px-2 py-0.5 rounded-sm"
      style={{
        background: `${color}15`,
        color,
        border: `1px solid ${color}35`,
        fontFamily: 'JetBrains Mono, monospace',
      }}>
      {label}
    </span>
  );
}

function PromotionPipeline({ status }: { status: number }) {
  const stages = ['OBSERVE', 'SHADOW', 'PAPER_SM', 'PAPER_FULL'];
  return (
    <div className="flex items-center gap-1">
      {stages.map((stage, i) => (
        <div key={stage} className="flex items-center">
          <div className="w-2 h-2 rounded-full" style={{
            background: i <= status ? (STATUS_COLORS[i] || QV.textDim) : QV.muted,
            boxShadow: i === status ? `0 0 6px ${STATUS_COLORS[i]}80` : 'none',
          }} />
          {i < stages.length - 1 && (
            <div className="w-4 h-px" style={{ background: i < status ? QV.accent : QV.muted }} />
          )}
        </div>
      ))}
      <span className="ml-2 text-xs" style={{ color: STATUS_COLORS[status], fontFamily: 'JetBrains Mono' }}>
        {STATUS_LABELS[status]}
      </span>
    </div>
  );
}

// ─── Main Component ─────────────────────────────────────────────────

export default function DerivativesDashboard() {
  const [health, setHealth] = useState<HealthData | null>(null);
  const [opportunities, setOpportunities] = useState<Opportunity[]>([]);
  const [statuses, setStatuses] = useState<StatusEntry[]>([]);
  const [liquidityScores, setLiquidityScores] = useState<LiquidityScore>({});
  const [scorecards, setScorecards] = useState<Scorecard[]>([]);
  const [loading, setLoading] = useState(true);
  const [lastRefresh, setLastRefresh] = useState<Date>(new Date());
  const [activeTab, setActiveTab] = useState('overview');
  const [autoRefresh, setAutoRefresh] = useState(true);

  const fetchData = useCallback(async () => {
    try {
      const [healthRes, oppsRes, statusRes, liqRes, scoreRes] = await Promise.allSettled([
        fetch(`${API_BASE}/health`).then(r => r.json()),
        fetch(`${API_BASE}/opportunities?limit=200`).then(r => r.json()),
        fetch(`${API_BASE}/status`).then(r => r.json()),
        fetch(`${API_BASE}/liquidity-score`).then(r => r.json()),
        fetch(`${API_BASE}/scorecard`).then(r => r.json()),
      ]);

      if (healthRes.status === 'fulfilled') setHealth(healthRes.value);
      if (oppsRes.status === 'fulfilled') setOpportunities(oppsRes.value?.by_strategy
        ? Object.values(oppsRes.value.by_strategy).flat() as Opportunity[]
        : []);
      if (statusRes.status === 'fulfilled') setStatuses(statusRes.value?.statuses || []);
      if (liqRes.status === 'fulfilled') setLiquidityScores(liqRes.value?.liquidity_scores || {});
      if (scoreRes.status === 'fulfilled') setScorecards(scoreRes.value?.scorecards || []);

      setLastRefresh(new Date());
    } catch (err) {
      console.error('Derivatives fetch error:', err);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchData();
  }, [fetchData]);

  useEffect(() => {
    if (!autoRefresh) return;
    const interval = setInterval(fetchData, 30000); // 30s refresh
    return () => clearInterval(interval);
  }, [autoRefresh, fetchData]);

  const strategyCounts = Object.entries(STRATEGY_META).map(([key, meta]) => {
    const opps = opportunities.filter(o => o.strategy_type === key);
    const healthEntry = health?.strategies?.[`${key.toLowerCase()}_scan_loop`] ||
                        health?.strategies?.[`${key.toLowerCase().replace('_', '')}_scan_loop`];
    return { key, ...meta, opps, healthy: healthEntry?.healthy ?? false, count: opps.length };
  });

  const totalOpps = opportunities.length;
  const healthyCount = health ? Object.values(health.strategies).filter(s => s.healthy).length : 0;
  const totalStrategies = 8;
  const avgLiquidity = Object.values(liquidityScores).length > 0
    ? (Object.values(liquidityScores).reduce((a, b) => a + b, 0) / Object.values(liquidityScores).length).toFixed(1)
    : '—';

  // ─── Render ─────────────────────────────────────────────────────

  return (
    <div className="min-h-screen" style={{ background: QV.surface1, color: QV.text }}>
      {/* Header */}
      <div className="sticky top-0 z-50 border-b" style={{ background: `${QV.surface1}F0`, borderColor: QV.border, backdropFilter: 'blur(12px)' }}>
        <div className="max-w-[1600px] mx-auto px-6 py-4 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <div className="w-8 h-8 rounded-lg flex items-center justify-center" style={{ background: `${QV.gold}20`, border: `1px solid ${QV.goldDim}` }}>
              <Layers className="w-4 h-4" style={{ color: QV.gold }} />
            </div>
            <div>
              <h1 className="text-lg font-bold tracking-tight" style={{ color: QV.text }}>Derivativos B3</h1>
              <p className="text-xs" style={{ color: QV.textDim }}>8 Estratégias · Paper Mode · {health?.overall_health || 'Loading...'}</p>
            </div>
          </div>
          <div className="flex items-center gap-3">
            <span className="text-xs" style={{ color: QV.textDim, fontFamily: 'JetBrains Mono' }}>
              {lastRefresh.toLocaleTimeString('pt-BR')}
            </span>
            <Button variant="outline" size="sm" onClick={fetchData}
              className="border-zinc-700 hover:bg-zinc-800 text-xs">
              <RefreshCw className={`w-3 h-3 mr-1.5 ${loading ? 'animate-spin' : ''}`} />
              Atualizar
            </Button>
            <Button variant={autoRefresh ? 'default' : 'outline'} size="sm"
              onClick={() => setAutoRefresh(!autoRefresh)}
              className={`text-xs ${autoRefresh ? 'bg-blue-600 hover:bg-blue-700' : 'border-zinc-700 hover:bg-zinc-800'}`}>
              <Activity className="w-3 h-3 mr-1.5" />
              Auto {autoRefresh ? 'ON' : 'OFF'}
            </Button>
          </div>
        </div>
      </div>

      <div className="max-w-[1600px] mx-auto px-6 py-6 space-y-6">

        {/* KPI Cards */}
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <StatCard label="Estratégias Ativas" value={`${healthyCount}/${totalStrategies}`}
            sub={health?.overall_health === 'HEALTHY' ? 'Todas saudáveis' : 'Verificar scan loops'}
            icon={Shield} color={healthyCount === totalStrategies ? QV.profit : QV.orange} />
          <StatCard label="Oportunidades" value={totalOpps}
            sub="Última hora" icon={Target} color={QV.accent} />
          <StatCard label="Liquidez Média" value={avgLiquidity}
            sub="Score 0-100" icon={BarChart3} color={QV.purple} />
          <StatCard label="Ativos Monitorados" value={Object.keys(liquidityScores).length || 8}
            sub="Universo Tier A" icon={Globe} color={QV.gold} />
        </div>

        {/* Main Tabs */}
        <Tabs value={activeTab} onValueChange={setActiveTab}>
          <TabsList className="bg-zinc-900 border border-zinc-800 p-1">
            <TabsTrigger value="overview" className="data-[state=active]:bg-zinc-800 text-xs">
              Visão Geral
            </TabsTrigger>
            {Object.entries(STRATEGY_META).map(([key, meta]) => (
              <TabsTrigger key={key} value={key} className="data-[state=active]:bg-zinc-800 text-xs">
                <div className="w-1.5 h-1.5 rounded-full mr-1.5" style={{ background: meta.color }} />
                {meta.label.split(' ')[0]}
              </TabsTrigger>
            ))}
          </TabsList>

          {/* Overview Tab */}
          <TabsContent value="overview" className="space-y-6 mt-4">

            {/* Strategy Grid */}
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
              {strategyCounts.map(s => {
                const Icon = s.icon;
                return (
                  <Card key={s.key} className="cursor-pointer hover:border-zinc-600 transition-colors"
                    style={{ background: QV.surface2, borderColor: QV.border }}
                    onClick={() => setActiveTab(s.key)}>
                    <CardContent className="p-4 space-y-3">
                      <div className="flex items-center justify-between">
                        <div className="flex items-center gap-2">
                          <div className="w-7 h-7 rounded flex items-center justify-center" style={{ background: `${s.color}15` }}>
                            <Icon className="w-3.5 h-3.5" style={{ color: s.color }} />
                          </div>
                          <span className="text-sm font-semibold">{s.label}</span>
                        </div>
                        <HealthBadge healthy={s.healthy} />
                      </div>
                      <p className="text-xs" style={{ color: QV.textDim }}>{s.desc}</p>
                      <div className="flex items-center justify-between pt-2" style={{ borderTop: `1px solid ${QV.border}` }}>
                        <span className="text-xs" style={{ color: QV.textDim }}>Oportunidades</span>
                        <span className="text-sm font-bold" style={{ fontFamily: 'JetBrains Mono', color: s.count > 0 ? QV.profit : QV.textDim }}>
                          {s.count}
                        </span>
                      </div>
                    </CardContent>
                  </Card>
                );
              })}
            </div>

            {/* Promotion Status Table */}
            {statuses.length > 0 && (
              <Card style={{ background: QV.surface2, borderColor: QV.border }}>
                <CardHeader className="pb-3">
                  <CardTitle className="text-sm font-semibold flex items-center gap-2">
                    <ArrowUpRight className="w-4 h-4" style={{ color: QV.accent }} />
                    Pipeline de Promoção
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="overflow-x-auto">
                    <table className="w-full text-xs">
                      <thead>
                        <tr style={{ borderBottom: `1px solid ${QV.border}` }}>
                          <th className="text-left py-2 px-3" style={{ color: QV.textDim }}>Ativo</th>
                          <th className="text-left py-2 px-3" style={{ color: QV.textDim }}>Estratégia</th>
                          <th className="text-left py-2 px-3" style={{ color: QV.textDim }}>Status</th>
                          <th className="text-left py-2 px-3" style={{ color: QV.textDim }}>Pipeline</th>
                          <th className="text-right py-2 px-3" style={{ color: QV.textDim }}>Atualizado</th>
                        </tr>
                      </thead>
                      <tbody>
                        {statuses.map((s, i) => (
                          <tr key={i} style={{ borderBottom: `1px solid ${QV.border}22` }}>
                            <td className="py-2 px-3 font-bold" style={{ fontFamily: 'JetBrains Mono' }}>{s.symbol}</td>
                            <td className="py-2 px-3">{STRATEGY_META[s.strategy_type]?.label || s.strategy_type}</td>
                            <td className="py-2 px-3"><StatusBadge status={s.active_status} /></td>
                            <td className="py-2 px-3"><PromotionPipeline status={s.active_status} /></td>
                            <td className="py-2 px-3 text-right" style={{ color: QV.textDim }}>
                              {s.updated_at ? new Date(s.updated_at).toLocaleString('pt-BR') : '—'}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </CardContent>
              </Card>
            )}

            {/* Liquidity Scores */}
            {Object.keys(liquidityScores).length > 0 && (
              <Card style={{ background: QV.surface2, borderColor: QV.border }}>
                <CardHeader className="pb-3">
                  <CardTitle className="text-sm font-semibold flex items-center gap-2">
                    <BarChart3 className="w-4 h-4" style={{ color: QV.purple }} />
                    Scores de Liquidez
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="h-64">
                    <ResponsiveContainer width="100%" height="100%">
                      <BarChart data={Object.entries(liquidityScores).map(([symbol, score]) => ({ symbol, score }))}>
                        <CartesianGrid strokeDasharray="3 3" stroke={QV.border} />
                        <XAxis dataKey="symbol" stroke={QV.textDim} tick={{ fontSize: 11, fontFamily: 'JetBrains Mono' }} />
                        <YAxis domain={[0, 100]} stroke={QV.textDim} tick={{ fontSize: 11 }} />
                        <RechartsTooltip
                          contentStyle={{ background: QV.surface3, border: `1px solid ${QV.border}`, borderRadius: 8, fontSize: 12 }}
                          labelStyle={{ color: QV.text, fontWeight: 'bold' }}
                        />
                        <Bar dataKey="score" radius={[4, 4, 0, 0]}>
                          {Object.entries(liquidityScores).map(([symbol, score], i) => (
                            <Cell key={symbol} fill={
                              score >= 80 ? QV.profit : score >= 50 ? QV.orange : QV.loss
                            } />
                          ))}
                        </Bar>
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                  <div className="flex gap-4 mt-3 justify-center">
                    <span className="flex items-center gap-1.5 text-xs" style={{ color: QV.textDim }}>
                      <div className="w-2 h-2 rounded-full" style={{ background: QV.profit }} /> ≥80 (Paper Full)
                    </span>
                    <span className="flex items-center gap-1.5 text-xs" style={{ color: QV.textDim }}>
                      <div className="w-2 h-2 rounded-full" style={{ background: QV.orange }} /> ≥50 (Shadow)
                    </span>
                    <span className="flex items-center gap-1.5 text-xs" style={{ color: QV.textDim }}>
                      <div className="w-2 h-2 rounded-full" style={{ background: QV.loss }} /> &lt;50 (Observe)
                    </span>
                  </div>
                </CardContent>
              </Card>
            )}

            {/* Scorecards */}
            {scorecards.length > 0 && (
              <Card style={{ background: QV.surface2, borderColor: QV.border }}>
                <CardHeader className="pb-3">
                  <CardTitle className="text-sm font-semibold flex items-center gap-2">
                    <Target className="w-4 h-4" style={{ color: QV.gold }} />
                    Scorecards de Performance
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="overflow-x-auto">
                    <table className="w-full text-xs">
                      <thead>
                        <tr style={{ borderBottom: `1px solid ${QV.border}` }}>
                          <th className="text-left py-2 px-3" style={{ color: QV.textDim }}>Estratégia</th>
                          <th className="text-left py-2 px-3" style={{ color: QV.textDim }}>Ativo</th>
                          <th className="text-right py-2 px-3" style={{ color: QV.textDim }}>Opp. Vistas</th>
                          <th className="text-right py-2 px-3" style={{ color: QV.textDim }}>Aprovadas</th>
                          <th className="text-right py-2 px-3" style={{ color: QV.textDim }}>Executadas</th>
                          <th className="text-right py-2 px-3" style={{ color: QV.textDim }}>P&L</th>
                          <th className="text-right py-2 px-3" style={{ color: QV.textDim }}>Sharpe</th>
                          <th className="text-right py-2 px-3" style={{ color: QV.textDim }}>Fill %</th>
                        </tr>
                      </thead>
                      <tbody>
                        {scorecards.map((sc, i) => (
                          <tr key={i} style={{ borderBottom: `1px solid ${QV.border}22` }}>
                            <td className="py-2 px-3 font-semibold">{STRATEGY_META[sc.strategy_type]?.label || sc.strategy_type}</td>
                            <td className="py-2 px-3" style={{ fontFamily: 'JetBrains Mono' }}>{sc.symbol}</td>
                            <td className="py-2 px-3 text-right">{sc.opportunities_seen}</td>
                            <td className="py-2 px-3 text-right">{sc.opportunities_approved}</td>
                            <td className="py-2 px-3 text-right">{sc.trades_executed}</td>
                            <td className="py-2 px-3 text-right font-bold" style={{
                              color: sc.pnl_total >= 0 ? QV.profit : QV.loss, fontFamily: 'JetBrains Mono'
                            }}>
                              R$ {sc.pnl_total?.toLocaleString('pt-BR', { minimumFractionDigits: 2 })}
                            </td>
                            <td className="py-2 px-3 text-right">{sc.sharpe?.toFixed(2)}</td>
                            <td className="py-2 px-3 text-right">{(sc.fill_ratio * 100)?.toFixed(1)}%</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </CardContent>
              </Card>
            )}

            {/* Recent Opportunities */}
            <Card style={{ background: QV.surface2, borderColor: QV.border }}>
              <CardHeader className="pb-3">
                <CardTitle className="text-sm font-semibold flex items-center gap-2">
                  <Zap className="w-4 h-4" style={{ color: QV.cyan }} />
                  Oportunidades Recentes
                  <span className="ml-auto text-xs font-normal" style={{ color: QV.textDim }}>
                    {opportunities.length} encontradas
                  </span>
                </CardTitle>
              </CardHeader>
              <CardContent>
                {opportunities.length === 0 ? (
                  <div className="text-center py-12 space-y-3">
                    <Clock className="w-10 h-10 mx-auto" style={{ color: QV.muted }} />
                    <p className="text-sm" style={{ color: QV.textDim }}>
                      Nenhuma oportunidade detectada ainda
                    </p>
                    <p className="text-xs" style={{ color: QV.muted }}>
                      FST precisa de 5 dias de calibração · Scan loops rodando a cada 45-60s
                    </p>
                  </div>
                ) : (
                  <div className="overflow-x-auto">
                    <table className="w-full text-xs">
                      <thead>
                        <tr style={{ borderBottom: `1px solid ${QV.border}` }}>
                          <th className="text-left py-2 px-3" style={{ color: QV.textDim }}>Hora</th>
                          <th className="text-left py-2 px-3" style={{ color: QV.textDim }}>Estratégia</th>
                          <th className="text-left py-2 px-3" style={{ color: QV.textDim }}>Ativo</th>
                          <th className="text-left py-2 px-3" style={{ color: QV.textDim }}>Tipo</th>
                          <th className="text-right py-2 px-3" style={{ color: QV.textDim }}>Strike</th>
                          <th className="text-right py-2 px-3" style={{ color: QV.textDim }}>Edge (bps)</th>
                          <th className="text-right py-2 px-3" style={{ color: QV.textDim }}>Liquidez</th>
                          <th className="text-left py-2 px-3" style={{ color: QV.textDim }}>Decisão</th>
                        </tr>
                      </thead>
                      <tbody>
                        {opportunities.slice(0, 50).map((opp, i) => {
                          const meta = STRATEGY_META[opp.strategy_type];
                          return (
                            <tr key={i} className="hover:bg-zinc-800/50" style={{ borderBottom: `1px solid ${QV.border}22` }}>
                              <td className="py-2 px-3" style={{ fontFamily: 'JetBrains Mono', color: QV.textDim }}>
                                {opp.timestamp ? new Date(opp.timestamp).toLocaleTimeString('pt-BR') : '—'}
                              </td>
                              <td className="py-2 px-3">
                                <span className="inline-flex items-center gap-1">
                                  <div className="w-1.5 h-1.5 rounded-full" style={{ background: meta?.color || QV.accent }} />
                                  {meta?.label || opp.strategy_type}
                                </span>
                              </td>
                              <td className="py-2 px-3 font-bold" style={{ fontFamily: 'JetBrains Mono' }}>{opp.symbol}</td>
                              <td className="py-2 px-3" style={{ color: QV.textDim }}>{opp.opportunity_type || '—'}</td>
                              <td className="py-2 px-3 text-right" style={{ fontFamily: 'JetBrains Mono' }}>
                                {opp.strike ? `R$ ${opp.strike.toFixed(2)}` : '—'}
                              </td>
                              <td className="py-2 px-3 text-right font-bold" style={{
                                fontFamily: 'JetBrains Mono',
                                color: (opp.expected_edge_bps || 0) > 0 ? QV.profit : QV.textDim,
                              }}>
                                {opp.expected_edge_bps?.toFixed(2) || '—'}
                              </td>
                              <td className="py-2 px-3 text-right">{opp.liquidity_score?.toFixed(1) || '—'}</td>
                              <td className="py-2 px-3">
                                {opp.decision && (
                                  <span className="text-xs font-bold px-1.5 py-0.5 rounded" style={{
                                    background: opp.decision === 'APPROVED' ? `${QV.profit}15` : `${QV.loss}15`,
                                    color: opp.decision === 'APPROVED' ? QV.profit : QV.loss,
                                  }}>
                                    {opp.decision}
                                  </span>
                                )}
                              </td>
                            </tr>
                          );
                        })}
                      </tbody>
                    </table>
                  </div>
                )}
              </CardContent>
            </Card>
          </TabsContent>

          {/* Individual Strategy Tabs */}
          {Object.entries(STRATEGY_META).map(([key, meta]) => {
            const stratOpps = opportunities.filter(o => o.strategy_type === key);
            const stratStatuses = statuses.filter(s => s.strategy_type === key);
            const stratScorecards = scorecards.filter(s => s.strategy_type === key);
            const Icon = meta.icon;
            const healthKey = Object.keys(health?.strategies || {}).find(k =>
              k.includes(key.toLowerCase().replace('_', ''))
            );
            const stratHealth = healthKey ? health?.strategies[healthKey] : null;

            return (
              <TabsContent key={key} value={key} className="space-y-6 mt-4">
                {/* Strategy Header */}
                <div className="flex items-center gap-4 p-4 rounded-lg" style={{ background: QV.surface2, border: `1px solid ${QV.border}` }}>
                  <div className="w-12 h-12 rounded-lg flex items-center justify-center" style={{ background: `${meta.color}15`, border: `1px solid ${meta.color}30` }}>
                    <Icon className="w-6 h-6" style={{ color: meta.color }} />
                  </div>
                  <div className="flex-1">
                    <h2 className="text-lg font-bold">{meta.label}</h2>
                    <p className="text-xs" style={{ color: QV.textDim }}>{meta.desc}</p>
                  </div>
                  <div className="flex items-center gap-3">
                    <HealthBadge healthy={stratHealth?.healthy ?? false} />
                    <div className="text-right">
                      <div className="text-xs" style={{ color: QV.textDim }}>Último Heartbeat</div>
                      <div className="text-xs" style={{ fontFamily: 'JetBrains Mono', color: QV.text }}>
                        {stratHealth?.last_heartbeat
                          ? new Date(stratHealth.last_heartbeat).toLocaleTimeString('pt-BR')
                          : 'Aguardando...'}
                      </div>
                    </div>
                  </div>
                </div>

                {/* Strategy KPIs */}
                <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                  <StatCard label="Oportunidades" value={stratOpps.length} icon={Zap} color={meta.color} />
                  <StatCard label="Ativos Ativos" value={stratStatuses.length}
                    sub={stratStatuses.map(s => s.symbol).join(', ') || 'Nenhum'}
                    icon={Globe} color={QV.accent} />
                  <StatCard label="Última Atividade"
                    value={stratHealth?.last_heartbeat
                      ? new Date(stratHealth.last_heartbeat).toLocaleTimeString('pt-BR')
                      : '—'}
                    icon={Clock} color={QV.textDim} />
                  <StatCard label="Opp. 1h" value={stratHealth?.opportunities_1h ?? 0}
                    icon={Target} color={QV.profit} />
                </div>

                {/* Strategy Statuses */}
                {stratStatuses.length > 0 && (
                  <Card style={{ background: QV.surface2, borderColor: QV.border }}>
                    <CardHeader className="pb-3">
                      <CardTitle className="text-sm font-semibold">Status por Ativo</CardTitle>
                    </CardHeader>
                    <CardContent>
                      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-3">
                        {stratStatuses.map((s, i) => (
                          <div key={i} className="p-3 rounded-lg" style={{ background: QV.surface3, border: `1px solid ${QV.border}` }}>
                            <div className="flex items-center justify-between mb-2">
                              <span className="font-bold text-sm" style={{ fontFamily: 'JetBrains Mono' }}>{s.symbol}</span>
                              <StatusBadge status={s.active_status} />
                            </div>
                            <PromotionPipeline status={s.active_status} />
                          </div>
                        ))}
                      </div>
                    </CardContent>
                  </Card>
                )}

                {/* Strategy Opportunities */}
                <Card style={{ background: QV.surface2, borderColor: QV.border }}>
                  <CardHeader className="pb-3">
                    <CardTitle className="text-sm font-semibold">
                      Oportunidades {meta.label}
                    </CardTitle>
                  </CardHeader>
                  <CardContent>
                    {stratOpps.length === 0 ? (
                      <div className="text-center py-8">
                        <Clock className="w-8 h-8 mx-auto mb-2" style={{ color: QV.muted }} />
                        <p className="text-sm" style={{ color: QV.textDim }}>
                          {key === 'FST' ? 'Em calibração — aguardando 5 dias de dados' :
                           'Scan loop rodando — aguardando oportunidades'}
                        </p>
                      </div>
                    ) : (
                      <div className="overflow-x-auto">
                        <table className="w-full text-xs">
                          <thead>
                            <tr style={{ borderBottom: `1px solid ${QV.border}` }}>
                              <th className="text-left py-2 px-3" style={{ color: QV.textDim }}>Hora</th>
                              <th className="text-left py-2 px-3" style={{ color: QV.textDim }}>Ativo</th>
                              <th className="text-left py-2 px-3" style={{ color: QV.textDim }}>Tipo</th>
                              <th className="text-right py-2 px-3" style={{ color: QV.textDim }}>Strike</th>
                              <th className="text-right py-2 px-3" style={{ color: QV.textDim }}>Edge (bps)</th>
                              <th className="text-right py-2 px-3" style={{ color: QV.textDim }}>Liquidez</th>
                            </tr>
                          </thead>
                          <tbody>
                            {stratOpps.slice(0, 30).map((opp, i) => (
                              <tr key={i} className="hover:bg-zinc-800/50" style={{ borderBottom: `1px solid ${QV.border}22` }}>
                                <td className="py-2 px-3" style={{ fontFamily: 'JetBrains Mono', color: QV.textDim }}>
                                  {opp.timestamp ? new Date(opp.timestamp).toLocaleTimeString('pt-BR') : '—'}
                                </td>
                                <td className="py-2 px-3 font-bold" style={{ fontFamily: 'JetBrains Mono' }}>{opp.symbol}</td>
                                <td className="py-2 px-3">{opp.opportunity_type || '—'}</td>
                                <td className="py-2 px-3 text-right" style={{ fontFamily: 'JetBrains Mono' }}>
                                  {opp.strike ? `R$ ${opp.strike.toFixed(2)}` : '—'}
                                </td>
                                <td className="py-2 px-3 text-right font-bold" style={{
                                  fontFamily: 'JetBrains Mono',
                                  color: (opp.expected_edge_bps || 0) > 0 ? QV.profit : QV.textDim,
                                }}>
                                  {opp.expected_edge_bps?.toFixed(2) || '—'}
                                </td>
                                <td className="py-2 px-3 text-right">{opp.liquidity_score?.toFixed(1) || '—'}</td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    )}
                  </CardContent>
                </Card>

                {/* Strategy Scorecards */}
                {stratScorecards.length > 0 && (
                  <Card style={{ background: QV.surface2, borderColor: QV.border }}>
                    <CardHeader className="pb-3">
                      <CardTitle className="text-sm font-semibold">Scorecard</CardTitle>
                    </CardHeader>
                    <CardContent>
                      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                        {stratScorecards.map((sc, i) => (
                          <div key={i} className="p-3 rounded-lg space-y-2" style={{ background: QV.surface3, border: `1px solid ${QV.border}` }}>
                            <div className="text-xs font-semibold" style={{ color: QV.textDim }}>{sc.symbol}</div>
                            <div className="text-lg font-bold" style={{
                              fontFamily: 'JetBrains Mono',
                              color: sc.pnl_total >= 0 ? QV.profit : QV.loss,
                            }}>
                              R$ {sc.pnl_total?.toLocaleString('pt-BR', { minimumFractionDigits: 2 })}
                            </div>
                            <div className="grid grid-cols-2 gap-2 text-xs" style={{ color: QV.textDim }}>
                              <div>Sharpe: <span style={{ color: QV.text }}>{sc.sharpe?.toFixed(2)}</span></div>
                              <div>Fill: <span style={{ color: QV.text }}>{(sc.fill_ratio * 100)?.toFixed(1)}%</span></div>
                              <div>Trades: <span style={{ color: QV.text }}>{sc.trades_executed}</span></div>
                              <div>Aprovadas: <span style={{ color: QV.text }}>{sc.opportunities_approved}</span></div>
                            </div>
                          </div>
                        ))}
                      </div>
                    </CardContent>
                  </Card>
                )}
              </TabsContent>
            );
          })}
        </Tabs>
      </div>
    </div>
  );
}
