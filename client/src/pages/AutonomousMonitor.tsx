import { useEffect, useState } from 'react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { LineChart, Line, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, PieChart, Pie, Cell } from 'recharts';
import { AlertCircle, TrendingUp, Zap, RefreshCw, Activity } from 'lucide-react';

interface CycleData {
  timestamp: string;
  real_time_signals: number;
  backtests: number;
  improvements: number;
  new_strategies: number;
  avg_return: number;
}

interface SystemStatus {
  status: 'running' | 'idle' | 'error';
  last_cycle: string;
  next_cycle: string;
  uptime: number;
  cycles_completed: number;
}

export default function AutonomousMonitor() {
  const [cycleHistory, setCycleHistory] = useState<CycleData[]>([]);
  const [systemStatus, setSystemStatus] = useState<SystemStatus>({
    status: 'idle',
    last_cycle: 'Nunca',
    next_cycle: 'Em 24h',
    uptime: 0,
    cycles_completed: 0
  });

  useEffect(() => {
    // Carregar dados do ciclo
    fetch('/dashboard_data.json')
      .then(res => res.json())
      .then(data => {
        // Simular histórico de ciclos
        const history: CycleData[] = [
          {
            timestamp: new Date(Date.now() - 86400000).toLocaleDateString(),
            real_time_signals: 3,
            backtests: 3,
            improvements: 1,
            new_strategies: 2,
            avg_return: 0.02
          },
          {
            timestamp: new Date(Date.now() - 172800000).toLocaleDateString(),
            real_time_signals: 2,
            backtests: 3,
            improvements: 2,
            new_strategies: 1,
            avg_return: 0.015
          },
          {
            timestamp: new Date().toLocaleDateString(),
            real_time_signals: 0,
            backtests: 0,
            improvements: 0,
            new_strategies: 0,
            avg_return: 0
          }
        ];
        setCycleHistory(history);

        setSystemStatus({
          status: 'idle',
          last_cycle: new Date(Date.now() - 3600000).toLocaleTimeString(),
          next_cycle: new Date(Date.now() + 86400000).toLocaleTimeString(),
          uptime: 7,
          cycles_completed: 2
        });
      });
  }, []);

  const performanceData = [
    { name: 'Sinais Reais', value: cycleHistory.reduce((a, b) => a + b.real_time_signals, 0) },
    { name: 'Backtests', value: cycleHistory.reduce((a, b) => a + b.backtests, 0) },
    { name: 'Melhorias', value: cycleHistory.reduce((a, b) => a + b.improvements, 0) }
  ];

  const COLORS = ['#3b82f6', '#10b981', '#f59e0b'];

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-900 via-slate-800 to-slate-900 p-6">
      <div className="max-w-7xl mx-auto">
        {/* Header */}
        <div className="mb-8">
          <h1 className="text-4xl font-bold text-white mb-2">🤖 Monitor Autônomo</h1>
          <p className="text-slate-400">Sistema de Trading Autônomo e Auto-Regenerativo</p>
        </div>

        {/* Status Cards */}
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-8">
          <Card className="bg-slate-800 border-slate-700">
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium text-slate-300 flex items-center gap-2">
                <Activity className="w-4 h-4" />
                Status
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-white">
                {systemStatus.status === 'running' ? '🟢 Ativo' : '🟡 Inativo'}
              </div>
              <p className="text-xs text-slate-400 mt-1">
                Próximo ciclo: {systemStatus.next_cycle}
              </p>
            </CardContent>
          </Card>

          <Card className="bg-slate-800 border-slate-700">
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium text-slate-300 flex items-center gap-2">
                <Zap className="w-4 h-4" />
                Ciclos Completos
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-blue-400">{systemStatus.cycles_completed}</div>
              <p className="text-xs text-slate-400 mt-1">Última execução: {systemStatus.last_cycle}</p>
            </CardContent>
          </Card>

          <Card className="bg-slate-800 border-slate-700">
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium text-slate-300 flex items-center gap-2">
                <TrendingUp className="w-4 h-4" />
                Sinais Gerados
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-green-400">
                {cycleHistory.reduce((a, b) => a + b.real_time_signals, 0)}
              </div>
              <p className="text-xs text-slate-400 mt-1">Últimos 7 dias</p>
            </CardContent>
          </Card>

          <Card className="bg-slate-800 border-slate-700">
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium text-slate-300 flex items-center gap-2">
                <RefreshCw className="w-4 h-4" />
                Uptime
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-purple-400">{systemStatus.uptime}d</div>
              <p className="text-xs text-slate-400 mt-1">Sem interrupções</p>
            </CardContent>
          </Card>
        </div>

        {/* Charts */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
          {/* Histórico de Ciclos */}
          <Card className="bg-slate-800 border-slate-700">
            <CardHeader>
              <CardTitle className="text-white">Histórico de Ciclos</CardTitle>
              <CardDescription>Últimos 7 dias</CardDescription>
            </CardHeader>
            <CardContent>
              <ResponsiveContainer width="100%" height={300}>
                <LineChart data={cycleHistory}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
                  <XAxis dataKey="timestamp" stroke="#94a3b8" />
                  <YAxis stroke="#94a3b8" />
                  <Tooltip
                    contentStyle={{
                      backgroundColor: '#1e293b',
                      border: '1px solid #475569'
                    }}
                  />
                  <Legend />
                  <Line
                    type="monotone"
                    dataKey="real_time_signals"
                    stroke="#3b82f6"
                    name="Sinais Reais"
                    strokeWidth={2}
                  />
                  <Line
                    type="monotone"
                    dataKey="improvements"
                    stroke="#10b981"
                    name="Melhorias"
                    strokeWidth={2}
                  />
                </LineChart>
              </ResponsiveContainer>
            </CardContent>
          </Card>

          {/* Distribuição de Atividades */}
          <Card className="bg-slate-800 border-slate-700">
            <CardHeader>
              <CardTitle className="text-white">Distribuição de Atividades</CardTitle>
              <CardDescription>Últimos ciclos</CardDescription>
            </CardHeader>
            <CardContent className="flex justify-center">
              <ResponsiveContainer width="100%" height={300}>
                <PieChart>
                  <Pie
                    data={performanceData}
                    cx="50%"
                    cy="50%"
                    labelLine={false}
                    label={({ name, value }) => `${name}: ${value}`}
                    outerRadius={80}
                    fill="#8884d8"
                    dataKey="value"
                  >
                    {COLORS.map((color, index) => (
                      <Cell key={`cell-${index}`} fill={color} />
                    ))}
                  </Pie>
                  <Tooltip
                    contentStyle={{
                      backgroundColor: '#1e293b',
                      border: '1px solid #475569'
                    }}
                  />
                </PieChart>
              </ResponsiveContainer>
            </CardContent>
          </Card>
        </div>

        {/* Detalhes de Estratégias */}
        <Card className="bg-slate-800 border-slate-700">
          <CardHeader>
            <CardTitle className="text-white">Estratégias Auto-Regeneradas</CardTitle>
            <CardDescription>Novas estratégias geradas pelo sistema</CardDescription>
          </CardHeader>
          <CardContent>
            <div className="space-y-4">
              <div className="p-4 bg-slate-700 rounded-lg border border-slate-600">
                <div className="flex items-start justify-between">
                  <div>
                    <h3 className="font-semibold text-white">Enhanced Mean Reversion - BTCUSDT</h3>
                    <p className="text-sm text-slate-400 mt-1">
                      Estratégia otimizada com threshold de confiança aumentado
                    </p>
                  </div>
                  <span className="px-3 py-1 bg-green-500/20 text-green-400 rounded text-xs font-medium">
                    Ativa
                  </span>
                </div>
                <div className="mt-3 grid grid-cols-3 gap-4">
                  <div>
                    <p className="text-xs text-slate-400">Retorno Esperado</p>
                    <p className="text-lg font-semibold text-green-400">+2.2%</p>
                  </div>
                  <div>
                    <p className="text-xs text-slate-400">Confiança</p>
                    <p className="text-lg font-semibold text-blue-400">75%</p>
                  </div>
                  <div>
                    <p className="text-xs text-slate-400">Status</p>
                    <p className="text-lg font-semibold text-yellow-400">Testando</p>
                  </div>
                </div>
              </div>

              <div className="p-4 bg-slate-700 rounded-lg border border-slate-600">
                <div className="flex items-start justify-between">
                  <div>
                    <h3 className="font-semibold text-white">Pairs Trading - BTC/ETH</h3>
                    <p className="text-sm text-slate-400 mt-1">
                      Estratégia de cointegração com hedge ratio dinâmico
                    </p>
                  </div>
                  <span className="px-3 py-1 bg-blue-500/20 text-blue-400 rounded text-xs font-medium">
                    Pendente
                  </span>
                </div>
                <div className="mt-3 grid grid-cols-3 gap-4">
                  <div>
                    <p className="text-xs text-slate-400">Retorno Esperado</p>
                    <p className="text-lg font-semibold text-green-400">+5.0%</p>
                  </div>
                  <div>
                    <p className="text-xs text-slate-400">Correlação</p>
                    <p className="text-lg font-semibold text-blue-400">97.6%</p>
                  </div>
                  <div>
                    <p className="text-xs text-slate-400">Status</p>
                    <p className="text-lg font-semibold text-slate-400">Validando</p>
                  </div>
                </div>
              </div>
            </div>
          </CardContent>
        </Card>

        {/* Info Box */}
        <div className="mt-8 p-4 bg-blue-500/10 border border-blue-500/30 rounded-lg flex gap-3">
          <AlertCircle className="w-5 h-5 text-blue-400 flex-shrink-0 mt-0.5" />
          <div>
            <p className="text-sm text-blue-300">
              <strong>Sistema Autônomo Ativo:</strong> O sistema executa automaticamente ciclos diários de análise, simulação e auto-regeneração. Novos parâmetros são gerados continuamente baseado no aprendizado histórico.
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}
