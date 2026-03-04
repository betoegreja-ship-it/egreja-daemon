import { useState, useEffect } from 'react';
import { TrendingUp, TrendingDown, AlertCircle, Activity, DollarSign, Target } from 'lucide-react';
import { Card } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { LineChart, Line, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, PieChart, Pie, Cell } from 'recharts';

interface Opportunity {
  id: string;
  symbol: string;
  strategy: string;
  direction: 'LONG' | 'SHORT' | 'NEUTRAL';
  confidence: number;
  zScore: number;
  currentPrice: number;
  timestamp: string;
}

interface BacktestResult {
  symbol: string;
  totalReturn?: number;
  totalReturnPct?: number;
  sharpeRatio: number;
  winRate: number;
  maxDrawdown?: number;
  maxDrawdownPct?: number;
}

export default function Dashboard() {
  const [opportunities, setOpportunities] = useState<Opportunity[]>([]);
  const [backtestResults, setBacktestResults] = useState<BacktestResult[]>([]);
  const [cryptoSignal, setCryptoSignal] = useState<any>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    // Carregar sinais do API (daemon no Railway)
    async function loadData() {
      try {
        // Primeiro tenta carregar do /api/signals (novo)
        const signalsResponse = await fetch('/api/signals');
        const signalsData = await signalsResponse.json();
        
        if (signalsData.signals && signalsData.signals.length > 0) {
          // Converter sinais do daemon em opportunities
          const opp: Opportunity[] = signalsData.signals.map((s: any, idx: number) => ({
            id: `${s.symbol}-${idx}`,
            symbol: s.symbol,
            strategy: s.market_type === 'B3' ? 'B3 Mean Reversion' : 'NYSE Momentum',
            direction: s.signal.includes('COMPRA') ? 'LONG' : s.signal.includes('VENDA') ? 'SHORT' : 'NEUTRAL',
            confidence: s.score,
            zScore: (s.score - 50) / 10,
            currentPrice: s.price,
            timestamp: s.created_at || new Date().toISOString()
          }));
          
          setOpportunities(opp);
          setCryptoSignal({
            portfolio_value: signalsData.portfolio_value || 1000000,
            pnl: signalsData.pnl || 0,
            win_rate: signalsData.win_rate || 50
          });
        }
        
        // Fallback: tenta carregar dados estáticos
        Promise.all([
          fetch('/backtest-results.json').then(r => r.json()),
          fetch('/crypto-signal.json').then(r => r.json())
        ]).then(([backtestData, cryptoData]) => {
          setBacktestResults(backtestData.results || []);
        }).catch(() => {});
        
      } catch (err) {
        console.error('Erro ao carregar sinais:', err);
        // Fallback para dados estáticos
        Promise.all([
          fetch('/dashboard-data.json').then(r => r.json()),
          fetch('/backtest-results.json').then(r => r.json()),
          fetch('/crypto-signal.json').then(r => r.json())
        ])
          .then(([dashData, backtestData, cryptoData]) => {
            setOpportunities(dashData.opportunities || []);
            setBacktestResults(backtestData.results || []);
            setCryptoSignal(cryptoData);
          })
          .catch(e => console.error('Erro ao carregar fallback:', e));
      } finally {
        setLoading(false);
      }
    }
    
    loadData();
    
    // Atualizar a cada 2 minutos
    const interval = setInterval(loadData, 120000);
    return () => clearInterval(interval);
  }, []);

  const performanceData = [
    { month: 'Jan', return: 2.5 },
    { month: 'Feb', return: -1.2 },
    { month: 'Mar', return: 3.8 },
    { month: 'Apr', return: 1.5 },
    { month: 'May', return: -0.5 },
    { month: 'Jun', return: 4.2 }
  ];

  const strategyDistribution = [
    { name: 'Mean Reversion', value: 60 },
    { name: 'Pairs Trading', value: 40 }
  ];

  const COLORS = ['#3b82f6', '#10b981'];

  return (
    <div className="min-h-screen bg-background">
      {/* Header */}
      <header className="border-b border-border bg-card sticky top-0 z-50">
        <div className="container py-4 flex items-center justify-between">
          <div>
            <h1 className="text-3xl font-bold text-foreground">ArbitrageAI</h1>
            <p className="text-sm text-muted-foreground">Dashboard Profissional de Trading</p>
          </div>
          <div className="flex gap-2">
            <Button variant="outline">Configurações</Button>
            <Button>Atualizar Dados</Button>
          </div>
        </div>
      </header>

      {/* Main Content */}
      <main className="container py-8">
        {/* KPI Cards */}
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-8">
          <Card className="p-6 bg-card border-border">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm text-muted-foreground mb-1">Oportunidades</p>
                <p className="text-3xl font-bold text-foreground">{opportunities.length}</p>
              </div>
              <Target className="w-12 h-12 text-primary/20" />
            </div>
          </Card>

          <Card className="p-6 bg-card border-border">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm text-muted-foreground mb-1">Confiança Média</p>
                <p className="text-3xl font-bold text-foreground">
                  {opportunities.length > 0 
                    ? (opportunities.reduce((sum, o) => sum + o.confidence, 0) / opportunities.length * 100).toFixed(0)
                    : '0'}%
                </p>
              </div>
              <Activity className="w-12 h-12 text-accent/20" />
            </div>
          </Card>

          <Card className="p-6 bg-card border-border">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm text-muted-foreground mb-1">Retorno Médio (Backtest)</p>
                <p className="text-3xl font-bold text-green-600">
                  {backtestResults.length > 0
                    ? (backtestResults.reduce((sum: number, r: any) => sum + (r.total_return || 0), 0) / backtestResults.length * 100).toFixed(2)
                    : '0'}%
                </p>
              </div>
              <TrendingUp className="w-12 h-12 text-green-600/20" />
            </div>
          </Card>

          <Card className="p-6 bg-card border-border">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm text-muted-foreground mb-1">Sharpe Ratio Médio</p>
                <p className="text-3xl font-bold text-foreground">
                  {backtestResults.length > 0
                    ? (backtestResults.reduce((sum: number, r: any) => sum + (r.sharpe_ratio || 0), 0) / backtestResults.length).toFixed(2)
                    : '0'}
                </p>
              </div>
              <DollarSign className="w-12 h-12 text-primary/20" />
            </div>
          </Card>
        </div>

        {/* Charts Row */}
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6 mb-8">
          {/* Performance Chart */}
          <Card className="lg:col-span-2 p-6 bg-card border-border">
            <h2 className="text-lg font-semibold text-foreground mb-4">Performance Histórica</h2>
            <ResponsiveContainer width="100%" height={300}>
              <LineChart data={performanceData}>
                <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                <XAxis dataKey="month" stroke="#6b7280" />
                <YAxis stroke="#6b7280" />
                <Tooltip 
                  contentStyle={{ backgroundColor: '#fff', border: '1px solid #e5e7eb' }}
                  formatter={(value: any) => typeof value === 'number' ? `${value.toFixed(2)}%` : value}
                />
                <Legend />
                <Line 
                  type="monotone" 
                  dataKey="return" 
                  stroke="#3b82f6" 
                  strokeWidth={2}
                  dot={{ fill: '#3b82f6', r: 4 }}
                  activeDot={{ r: 6 }}
                />
              </LineChart>
            </ResponsiveContainer>
          </Card>

          {/* Strategy Distribution */}
          <Card className="p-6 bg-card border-border">
            <h2 className="text-lg font-semibold text-foreground mb-4">Distribuição de Estratégias</h2>
            <ResponsiveContainer width="100%" height={300}>
              <PieChart>
                <Pie
                  data={strategyDistribution}
                  cx="50%"
                  cy="50%"
                  labelLine={false}
                  label={({ name, value }) => `${name}: ${value}%`}
                  outerRadius={80}
                  fill="#8884d8"
                  dataKey="value"
                >
                  {strategyDistribution.map((entry, index) => (
                    <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                  ))}
                </Pie>
                <Tooltip formatter={(value) => `${value}%`} />
              </PieChart>
            </ResponsiveContainer>
          </Card>
        </div>

        {/* Opportunities Table */}
        <Card className="p-6 bg-card border-border mb-8">
          <h2 className="text-lg font-semibold text-foreground mb-4">Oportunidades Identificadas</h2>
          {opportunities.length === 0 ? (
            <div className="text-center py-8">
              <AlertCircle className="w-12 h-12 text-muted-foreground mx-auto mb-2" />
              <p className="text-muted-foreground">Nenhuma oportunidade identificada no momento</p>
            </div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead>
                  <tr className="border-b border-border">
                    <th className="text-left py-3 px-4 font-semibold text-foreground">Símbolo</th>
                    <th className="text-left py-3 px-4 font-semibold text-foreground">Estratégia</th>
                    <th className="text-left py-3 px-4 font-semibold text-foreground">Direção</th>
                    <th className="text-left py-3 px-4 font-semibold text-foreground">Confiança</th>
                    <th className="text-left py-3 px-4 font-semibold text-foreground">Z-Score</th>
                    <th className="text-left py-3 px-4 font-semibold text-foreground">Ação</th>
                  </tr>
                </thead>
                <tbody>
                  {opportunities.map((opp) => (
                    <tr key={opp.id} className="border-b border-border hover:bg-muted/50 transition">
                      <td className="py-3 px-4 font-medium text-foreground">{opp.symbol}</td>
                      <td className="py-3 px-4 text-muted-foreground">{opp.strategy}</td>
                      <td className="py-3 px-4">
                        <span className={`inline-flex items-center gap-1 px-2 py-1 rounded-md text-sm font-medium ${
                          opp.direction === 'LONG' ? 'bg-green-100 text-green-700' :
                          opp.direction === 'SHORT' ? 'bg-red-100 text-red-700' :
                          'bg-gray-100 text-gray-700'
                        }`}>
                          {opp.direction === 'LONG' && <TrendingUp className="w-4 h-4" />}
                          {opp.direction === 'SHORT' && <TrendingDown className="w-4 h-4" />}
                          {opp.direction}
                        </span>
                      </td>
                      <td className="py-3 px-4">
                        <div className="flex items-center gap-2">
                          <div className="w-16 bg-muted rounded-full h-2">
                            <div 
                              className="bg-primary h-2 rounded-full" 
                              style={{ width: `${(opp.confidence || 0) * 100}%` }}
                            />
                          </div>
                          <span className="text-sm font-medium">{((opp.confidence || 0) * 100).toFixed(0)}%</span>
                        </div>
                      </td>
                      <td className="py-3 px-4 text-muted-foreground">{(opp.zScore || 0).toFixed(2)}</td>
                      <td className="py-3 px-4">
                        <Button size="sm" variant="outline">Detalhes</Button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </Card>

        {/* Crypto Signal */}
        {cryptoSignal && (
          <Card className="p-6 bg-card border-border mb-8">
            <h2 className="text-lg font-semibold text-foreground mb-4">Sinal Crypto (ETH/BTC)</h2>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              <div>
                <p className="text-sm text-muted-foreground">Recomendacao</p>
                <p className="text-lg font-bold text-primary">{cryptoSignal.recommendation}</p>
              </div>
              <div>
                <p className="text-sm text-muted-foreground">Z-Score</p>
                <p className="text-lg font-bold">{cryptoSignal.signal?.z_score?.toFixed(2) || 'N/A'}</p>
              </div>
              <div>
                <p className="text-sm text-muted-foreground">Cointegrado</p>
                <p className="text-lg font-bold">{cryptoSignal.signal?.cointegrated ? 'Sim' : 'Nao'}</p>
              </div>
            </div>
          </Card>
        )}

        {/* Backtest Results */}
        <Card className="p-6 bg-card border-border">
          <h2 className="text-lg font-semibold text-foreground mb-4">Resultados do Backtesting</h2>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={backtestResults}>
              <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
              <XAxis dataKey="symbol" stroke="#6b7280" />
              <YAxis stroke="#6b7280" />
              <Tooltip 
                contentStyle={{ backgroundColor: '#fff', border: '1px solid #e5e7eb' }}
                formatter={(value: any) => typeof value === 'number' ? `${(value * 100).toFixed(2)}%` : value}
              />
              <Legend />
              <Bar dataKey="totalReturnPct" fill="#3b82f6" name="Retorno Total (%)" radius={[8, 8, 0, 0]} />
              <Bar dataKey="maxDrawdownPct" fill="#ef4444" name="Max Drawdown (%)" radius={[8, 8, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </Card>
      </main>
    </div>
  );
}
