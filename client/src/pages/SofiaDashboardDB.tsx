import { trpc } from "@/lib/trpc";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Badge } from "@/components/ui/badge";
import { Activity, TrendingUp, TrendingDown, Target, AlertCircle } from "lucide-react";
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, LineChart, Line } from "recharts";

export default function SofiaDashboardDB() {
  // Queries
  const { data: trades = [], isLoading: tradesLoading } = trpc.sofia.getTrades.useQuery({ limit: 50 });
  const { data: openTrades = [] } = trpc.sofia.getOpenTrades.useQuery();
  const { data: metrics = [] } = trpc.sofia.getAllSofiaMetrics.useQuery();
  const { data: analyses = [] } = trpc.sofia.getSofiaAnalyses.useQuery({ limit: 20 });
  const { data: dailyStats } = trpc.sofia.getDailyStats.useQuery();

  // Calcular estatísticas
  const totalTrades = trades.length;
  const closedTrades = trades.filter(t => t.status === "CLOSED");
  const winningTrades = closedTrades.filter(t => parseFloat(t.pnl || "0") > 0);
  const totalPnl = closedTrades.reduce((sum, t) => sum + parseFloat(t.pnl || "0"), 0);
  const winRate = closedTrades.length > 0 ? (winningTrades.length / closedTrades.length) * 100 : 0;

  // Dados para gráficos
  const metricsChartData = metrics.map(m => ({
    symbol: m.symbol,
    accuracy: m.accuracy,
    totalTrades: m.totalTrades,
    pnl: parseFloat(m.totalPnl),
  }));

  const tradesChartData = trades.slice(0, 20).reverse().map((t, i) => ({
    index: i + 1,
    pnl: parseFloat(t.pnl || "0"),
    symbol: t.symbol,
  }));

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-900 via-blue-900 to-slate-900 p-6">
      <div className="max-w-7xl mx-auto space-y-6">
        {/* Header */}
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-4xl font-bold text-white flex items-center gap-3">
              <Activity className="w-10 h-10 text-blue-400" />
              Sofia IA - Dashboard Completo
            </h1>
            <p className="text-slate-300 mt-2">Dados persistentes do banco de dados</p>
          </div>
          <Badge variant="outline" className="text-green-400 border-green-400 px-4 py-2">
            ● Conectado ao BD
          </Badge>
        </div>

        {/* Stats Cards */}
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
          <Card className="bg-slate-800/50 border-slate-700">
            <CardHeader className="pb-2">
              <CardTitle className="text-sm text-slate-400">Total de Trades</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-3xl font-bold text-white">{totalTrades}</div>
              <p className="text-xs text-slate-400 mt-1">{openTrades.length} abertas</p>
            </CardContent>
          </Card>

          <Card className="bg-slate-800/50 border-slate-700">
            <CardHeader className="pb-2">
              <CardTitle className="text-sm text-slate-400">P&L Total</CardTitle>
            </CardHeader>
            <CardContent>
              <div className={`text-3xl font-bold ${totalPnl >= 0 ? "text-green-400" : "text-red-400"}`}>
                ${totalPnl.toFixed(2)}
              </div>
              <p className="text-xs text-slate-400 mt-1">
                {winningTrades.length} ganhos / {closedTrades.length - winningTrades.length} perdas
              </p>
            </CardContent>
          </Card>

          <Card className="bg-slate-800/50 border-slate-700">
            <CardHeader className="pb-2">
              <CardTitle className="text-sm text-slate-400">Taxa de Acerto</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-3xl font-bold text-white">{winRate.toFixed(1)}%</div>
              <p className="text-xs text-slate-400 mt-1">
                {closedTrades.length} trades fechadas
              </p>
            </CardContent>
          </Card>

          <Card className="bg-slate-800/50 border-slate-700">
            <CardHeader className="pb-2">
              <CardTitle className="text-sm text-slate-400">Análises Sofia</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-3xl font-bold text-white">{analyses.length}</div>
              <p className="text-xs text-slate-400 mt-1">Últimas 20 análises</p>
            </CardContent>
          </Card>
        </div>

        {/* Tabs */}
        <Tabs defaultValue="trades" className="w-full">
          <TabsList className="bg-slate-800/50 border-slate-700">
            <TabsTrigger value="trades">Trades</TabsTrigger>
            <TabsTrigger value="metrics">Métricas</TabsTrigger>
            <TabsTrigger value="analyses">Análises</TabsTrigger>
            <TabsTrigger value="charts">Gráficos</TabsTrigger>
          </TabsList>

          {/* Trades Tab */}
          <TabsContent value="trades" className="space-y-4">
            <Card className="bg-slate-800/50 border-slate-700">
              <CardHeader>
                <CardTitle className="text-white">Histórico de Trades</CardTitle>
                <CardDescription className="text-slate-400">Últimas 50 operações</CardDescription>
              </CardHeader>
              <CardContent>
                {tradesLoading ? (
                  <p className="text-slate-400">Carregando...</p>
                ) : (
                  <div className="overflow-x-auto">
                    <table className="w-full">
                      <thead>
                        <tr className="border-b border-slate-700">
                          <th className="text-left p-2 text-slate-400">Símbolo</th>
                          <th className="text-left p-2 text-slate-400">Recomendação</th>
                          <th className="text-left p-2 text-slate-400">Confiança</th>
                          <th className="text-left p-2 text-slate-400">Entrada</th>
                          <th className="text-left p-2 text-slate-400">Saída</th>
                          <th className="text-left p-2 text-slate-400">P&L</th>
                          <th className="text-left p-2 text-slate-400">Status</th>
                        </tr>
                      </thead>
                      <tbody>
                        {trades.slice(0, 20).map((trade) => (
                          <tr key={trade.id} className="border-b border-slate-700/50 hover:bg-slate-700/30">
                            <td className="p-2 text-white font-medium">{trade.symbol}</td>
                            <td className="p-2">
                              <Badge variant={trade.recommendation === "BUY" ? "default" : "destructive"}>
                                {trade.recommendation}
                              </Badge>
                            </td>
                            <td className="p-2 text-slate-300">{trade.confidence}%</td>
                            <td className="p-2 text-slate-300">${trade.entryPrice}</td>
                            <td className="p-2 text-slate-300">{trade.exitPrice ? `$${trade.exitPrice}` : "-"}</td>
                            <td className={`p-2 font-medium ${parseFloat(trade.pnl || "0") >= 0 ? "text-green-400" : "text-red-400"}`}>
                              {trade.pnl ? `$${parseFloat(trade.pnl).toFixed(2)}` : "-"}
                            </td>
                            <td className="p-2">
                              <Badge variant={trade.status === "OPEN" ? "outline" : "secondary"}>
                                {trade.status}
                              </Badge>
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </CardContent>
            </Card>
          </TabsContent>

          {/* Metrics Tab */}
          <TabsContent value="metrics" className="space-y-4">
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
              {metrics.map((metric) => (
                <Card key={metric.id} className="bg-slate-800/50 border-slate-700">
                  <CardHeader>
                    <CardTitle className="text-white flex items-center justify-between">
                      {metric.symbol}
                      <Badge variant="outline" className="text-blue-400 border-blue-400">
                        {metric.accuracy}% acurácia
                      </Badge>
                    </CardTitle>
                  </CardHeader>
                  <CardContent className="space-y-2">
                    <div className="flex justify-between text-sm">
                      <span className="text-slate-400">Total Trades:</span>
                      <span className="text-white font-medium">{metric.totalTrades}</span>
                    </div>
                    <div className="flex justify-between text-sm">
                      <span className="text-slate-400">Ganhos:</span>
                      <span className="text-green-400 font-medium">{metric.winningTrades}</span>
                    </div>
                    <div className="flex justify-between text-sm">
                      <span className="text-slate-400">Perdas:</span>
                      <span className="text-red-400 font-medium">{metric.losingTrades}</span>
                    </div>
                    <div className="flex justify-between text-sm">
                      <span className="text-slate-400">P&L Total:</span>
                      <span className={`font-medium ${parseFloat(metric.totalPnl) >= 0 ? "text-green-400" : "text-red-400"}`}>
                        ${parseFloat(metric.totalPnl).toFixed(2)}
                      </span>
                    </div>
                  </CardContent>
                </Card>
              ))}
            </div>
          </TabsContent>

          {/* Analyses Tab */}
          <TabsContent value="analyses" className="space-y-4">
            <Card className="bg-slate-800/50 border-slate-700">
              <CardHeader>
                <CardTitle className="text-white">Análises Sofia</CardTitle>
                <CardDescription className="text-slate-400">Últimas 20 análises geradas</CardDescription>
              </CardHeader>
              <CardContent>
                <div className="space-y-3">
                  {analyses.map((analysis) => (
                    <div key={analysis.id} className="p-4 bg-slate-700/30 rounded-lg border border-slate-600">
                      <div className="flex items-center justify-between mb-2">
                        <div className="flex items-center gap-2">
                          <span className="text-white font-medium">{analysis.symbol}</span>
                          <Badge variant={analysis.recommendation === "BUY" ? "default" : analysis.recommendation === "SELL" ? "destructive" : "secondary"}>
                            {analysis.recommendation}
                          </Badge>
                        </div>
                        <Badge variant="outline" className="text-blue-400 border-blue-400">
                          {analysis.confidence}% confiança
                        </Badge>
                      </div>
                      <p className="text-sm text-slate-400">
                        {new Date(analysis.createdAt).toLocaleString("pt-BR")}
                      </p>
                    </div>
                  ))}
                </div>
              </CardContent>
            </Card>
          </TabsContent>

          {/* Charts Tab */}
          <TabsContent value="charts" className="space-y-4">
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
              <Card className="bg-slate-800/50 border-slate-700">
                <CardHeader>
                  <CardTitle className="text-white">Acurácia por Símbolo</CardTitle>
                </CardHeader>
                <CardContent>
                  <ResponsiveContainer width="100%" height={300}>
                    <BarChart data={metricsChartData}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
                      <XAxis dataKey="symbol" stroke="#94a3b8" />
                      <YAxis stroke="#94a3b8" />
                      <Tooltip 
                        contentStyle={{ backgroundColor: "#1e293b", border: "1px solid #334155" }}
                        labelStyle={{ color: "#fff" }}
                      />
                      <Legend />
                      <Bar dataKey="accuracy" fill="#3b82f6" name="Acurácia (%)" />
                    </BarChart>
                  </ResponsiveContainer>
                </CardContent>
              </Card>

              <Card className="bg-slate-800/50 border-slate-700">
                <CardHeader>
                  <CardTitle className="text-white">P&L por Trade</CardTitle>
                </CardHeader>
                <CardContent>
                  <ResponsiveContainer width="100%" height={300}>
                    <LineChart data={tradesChartData}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
                      <XAxis dataKey="index" stroke="#94a3b8" />
                      <YAxis stroke="#94a3b8" />
                      <Tooltip 
                        contentStyle={{ backgroundColor: "#1e293b", border: "1px solid #334155" }}
                        labelStyle={{ color: "#fff" }}
                      />
                      <Legend />
                      <Line type="monotone" dataKey="pnl" stroke="#10b981" name="P&L ($)" />
                    </LineChart>
                  </ResponsiveContainer>
                </CardContent>
              </Card>
            </div>
          </TabsContent>
        </Tabs>
      </div>
    </div>
  );
}
