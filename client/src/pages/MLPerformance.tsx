import { trpc } from "@/lib/trpc";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { Badge } from "@/components/ui/badge";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Brain, TrendingUp, Target, Activity, Calendar, BarChart3 } from "lucide-react";
import { LineChart, Line, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, PieChart, Pie, Cell } from 'recharts';

export default function MLPerformance() {
  const { data: mlStats, isLoading: statsLoading } = trpc.ml.getPerformanceStats.useQuery();
  const { data: trainingHistory, isLoading: historyLoading } = trpc.ml.getTrainingHistory.useQuery();
  const { data: marketComparison, isLoading: comparisonLoading } = trpc.ml.getMarketComparison.useQuery();
  const { data: featureImportance, isLoading: featuresLoading } = trpc.ml.getFeatureImportance.useQuery();

  if (statsLoading) {
    return (
      <div className="container py-8 space-y-6">
        <Skeleton className="h-12 w-64" />
        <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
          {[1, 2, 3, 4].map((i) => (
            <Skeleton key={i} className="h-32" />
          ))}
        </div>
      </div>
    );
  }

  const COLORS = ['#10b981', '#3b82f6', '#f59e0b', '#ef4444'];

  return (
    <div className="container py-8 space-y-6">
      {/* Header */}
      <div className="flex items-center gap-3">
        <Brain className="h-8 w-8 text-primary" />
        <div>
          <h1 className="text-3xl font-bold">ML Performance</h1>
          <p className="text-muted-foreground">
            Análise de desempenho do modelo de Machine Learning
          </p>
        </div>
      </div>

      {/* Métricas Principais */}
      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Acurácia Atual</CardTitle>
            <Target className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">
              {mlStats?.currentAccuracy ? `${(mlStats.currentAccuracy * 100).toFixed(1)}%` : 'N/A'}
            </div>
            <p className="text-xs text-muted-foreground">
              {mlStats?.lastTrainingDate 
                ? `Último treino: ${new Date(mlStats.lastTrainingDate).toLocaleDateString('pt-BR')}`
                : 'Modelo não treinado'}
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Taxa de Acerto</CardTitle>
            <TrendingUp className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold text-green-600">
              {mlStats?.winRate ? `${(mlStats.winRate * 100).toFixed(1)}%` : 'N/A'}
            </div>
            <p className="text-xs text-muted-foreground">
              {mlStats?.totalTrades ? `${mlStats.totalTrades} trades analisadas` : 'Sem dados'}
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Retreinamentos</CardTitle>
            <Activity className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">
              {mlStats?.totalRetrainings || 0}
            </div>
            <p className="text-xs text-muted-foreground">
              Total de retreinamentos
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Próximo Treino</CardTitle>
            <Calendar className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">
              {mlStats?.nextTrainingIn || 'Em breve'}
            </div>
            <p className="text-xs text-muted-foreground">
              Retreinamento automático diário
            </p>
          </CardContent>
        </Card>
      </div>

      {/* Tabs com Análises Detalhadas */}
      <Tabs defaultValue="evolution" className="space-y-4">
        <TabsList>
          <TabsTrigger value="evolution">Evolução</TabsTrigger>
          <TabsTrigger value="markets">Por Mercado</TabsTrigger>
          <TabsTrigger value="features">Feature Importance</TabsTrigger>
          <TabsTrigger value="history">Histórico</TabsTrigger>
        </TabsList>

        {/* Tab: Evolução da Acurácia */}
        <TabsContent value="evolution" className="space-y-4">
          <Card>
            <CardHeader>
              <CardTitle>Evolução da Acurácia ao Longo do Tempo</CardTitle>
              <CardDescription>
                Acompanhe como o modelo está melhorando com cada retreinamento
              </CardDescription>
            </CardHeader>
            <CardContent>
              {historyLoading ? (
                <Skeleton className="h-[300px]" />
              ) : trainingHistory && trainingHistory.length > 0 ? (
                <ResponsiveContainer width="100%" height={300}>
                  <LineChart data={trainingHistory}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis 
                      dataKey="date" 
                      tickFormatter={(date) => new Date(date).toLocaleDateString('pt-BR', { day: '2-digit', month: '2-digit' })}
                    />
                    <YAxis domain={[0, 100]} />
                    <Tooltip 
                      labelFormatter={(date) => new Date(date).toLocaleDateString('pt-BR')}
                      formatter={(value: number) => [`${value.toFixed(1)}%`, 'Acurácia']}
                    />
                    <Legend />
                    <Line 
                      type="monotone" 
                      dataKey="accuracy" 
                      stroke="#10b981" 
                      strokeWidth={2}
                      name="Acurácia"
                    />
                  </LineChart>
                </ResponsiveContainer>
              ) : (
                <div className="flex items-center justify-center h-[300px] text-muted-foreground">
                  Nenhum histórico de treinamento disponível
                </div>
              )}
            </CardContent>
          </Card>
        </TabsContent>

        {/* Tab: Comparação por Mercado */}
        <TabsContent value="markets" className="space-y-4">
          <div className="grid gap-4 md:grid-cols-2">
            <Card>
              <CardHeader>
                <CardTitle>Taxa de Acerto por Mercado</CardTitle>
                <CardDescription>
                  Desempenho do modelo em diferentes mercados
                </CardDescription>
              </CardHeader>
              <CardContent>
                {comparisonLoading ? (
                  <Skeleton className="h-[300px]" />
                ) : marketComparison && marketComparison.length > 0 ? (
                  <ResponsiveContainer width="100%" height={300}>
                    <BarChart data={marketComparison}>
                      <CartesianGrid strokeDasharray="3 3" />
                      <XAxis dataKey="market" />
                      <YAxis domain={[0, 100]} />
                      <Tooltip formatter={(value: number) => `${value.toFixed(1)}%`} />
                      <Legend />
                      <Bar dataKey="winRate" fill="#10b981" name="Taxa de Acerto (%)" />
                    </BarChart>
                  </ResponsiveContainer>
                ) : (
                  <div className="flex items-center justify-center h-[300px] text-muted-foreground">
                    Dados insuficientes para comparação
                  </div>
                )}
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle>Distribuição de Trades</CardTitle>
                <CardDescription>
                  Quantidade de trades por mercado
                </CardDescription>
              </CardHeader>
              <CardContent>
                {comparisonLoading ? (
                  <Skeleton className="h-[300px]" />
                ) : marketComparison && marketComparison.length > 0 ? (
                  <ResponsiveContainer width="100%" height={300}>
                    <PieChart>
                      <Pie
                        data={marketComparison}
                        dataKey="totalTrades"
                        nameKey="market"
                        cx="50%"
                        cy="50%"
                        outerRadius={100}
                        label={(entry) => `${entry.market}: ${entry.totalTrades}`}
                      >
                        {marketComparison.map((entry: any, index: number) => (
                          <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                        ))}
                      </Pie>
                      <Tooltip />
                    </PieChart>
                  </ResponsiveContainer>
                ) : (
                  <div className="flex items-center justify-center h-[300px] text-muted-foreground">
                    Dados insuficientes para visualização
                  </div>
                )}
              </CardContent>
            </Card>
          </div>

          {/* Tabela Detalhada */}
          <Card>
            <CardHeader>
              <CardTitle>Comparação Detalhada</CardTitle>
              <CardDescription>
                Métricas completas por mercado
              </CardDescription>
            </CardHeader>
            <CardContent>
              {comparisonLoading ? (
                <Skeleton className="h-[200px]" />
              ) : marketComparison && marketComparison.length > 0 ? (
                <div className="overflow-x-auto">
                  <table className="w-full">
                    <thead>
                      <tr className="border-b">
                        <th className="text-left p-2">Mercado</th>
                        <th className="text-right p-2">Total Trades</th>
                        <th className="text-right p-2">Vencedoras</th>
                        <th className="text-right p-2">Taxa de Acerto</th>
                        <th className="text-right p-2">P&L Médio</th>
                      </tr>
                    </thead>
                    <tbody>
                      {marketComparison.map((market: any) => (
                        <tr key={market.market} className="border-b">
                          <td className="p-2 font-medium">{market.market}</td>
                          <td className="text-right p-2">{market.totalTrades}</td>
                          <td className="text-right p-2">{market.winningTrades}</td>
                          <td className="text-right p-2">
                            <Badge variant={market.winRate >= 50 ? "default" : "destructive"}>
                              {market.winRate.toFixed(1)}%
                            </Badge>
                          </td>
                          <td className="text-right p-2">
                            <span className={market.avgPnl >= 0 ? "text-green-600" : "text-red-600"}>
                              ${market.avgPnl.toFixed(2)}
                            </span>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : (
                <div className="flex items-center justify-center h-[200px] text-muted-foreground">
                  Nenhum dado disponível
                </div>
              )}
            </CardContent>
          </Card>
        </TabsContent>

        {/* Tab: Feature Importance */}
        <TabsContent value="features" className="space-y-4">
          <Card>
            <CardHeader>
              <CardTitle>Importância dos Indicadores</CardTitle>
              <CardDescription>
                Quais indicadores técnicos mais influenciam as decisões do modelo
              </CardDescription>
            </CardHeader>
            <CardContent>
              {featuresLoading ? (
                <Skeleton className="h-[400px]" />
              ) : featureImportance && featureImportance.length > 0 ? (
                <ResponsiveContainer width="100%" height={400}>
                  <BarChart data={featureImportance} layout="vertical">
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis type="number" domain={[0, 100]} />
                    <YAxis dataKey="feature" type="category" width={100} />
                    <Tooltip formatter={(value: number) => `${value.toFixed(1)}%`} />
                    <Bar dataKey="importance" fill="#3b82f6" name="Importância (%)" />
                  </BarChart>
                </ResponsiveContainer>
              ) : (
                <div className="flex items-center justify-center h-[400px] text-muted-foreground">
                  Dados de feature importance não disponíveis
                </div>
              )}
            </CardContent>
          </Card>
        </TabsContent>

        {/* Tab: Histórico de Retreinamentos */}
        <TabsContent value="history" className="space-y-4">
          <Card>
            <CardHeader>
              <CardTitle>Histórico de Retreinamentos</CardTitle>
              <CardDescription>
                Registro completo de todos os retreinamentos do modelo
              </CardDescription>
            </CardHeader>
            <CardContent>
              {historyLoading ? (
                <Skeleton className="h-[400px]" />
              ) : trainingHistory && trainingHistory.length > 0 ? (
                <div className="overflow-x-auto">
                  <table className="w-full">
                    <thead>
                      <tr className="border-b">
                        <th className="text-left p-2">Data</th>
                        <th className="text-right p-2">Trades Utilizadas</th>
                        <th className="text-right p-2">Acurácia</th>
                        <th className="text-left p-2">Versão</th>
                        <th className="text-left p-2">Notas</th>
                      </tr>
                    </thead>
                    <tbody>
                      {trainingHistory.map((training: any, idx: number) => (
                        <tr key={idx} className="border-b">
                          <td className="p-2">
                            {new Date(training.date).toLocaleString('pt-BR')}
                          </td>
                          <td className="text-right p-2">{training.tradesUsed}</td>
                          <td className="text-right p-2">
                            <Badge>
                              {training.accuracy.toFixed(1)}%
                            </Badge>
                          </td>
                          <td className="p-2">{training.version}</td>
                          <td className="p-2 text-muted-foreground">{training.notes}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : (
                <div className="flex items-center justify-center h-[400px] text-muted-foreground">
                  Nenhum histórico de retreinamento disponível
                </div>
              )}
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>
    </div>
  );
}
