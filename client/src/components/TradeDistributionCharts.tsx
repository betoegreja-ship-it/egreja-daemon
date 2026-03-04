import { useMemo } from 'react';
import { PieChart, Pie, Cell, ResponsiveContainer, Legend, Tooltip } from 'recharts';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';

interface Trade {
  id: string;
  symbol: string;
  entryPrice: string;
  quantity: string;
  pnl: string | null;
  status: string;
}

interface TradeDistributionChartsProps {
  trades: Trade[];
}

const COLORS = {
  Crypto: '#3b82f6', // blue
  B3: '#10b981',     // green
  NYSE: '#f59e0b',   // amber
};

export function TradeDistributionCharts({ trades }: TradeDistributionChartsProps) {
  // Classificar trades por mercado
  const getMarketType = (symbol: string): 'Crypto' | 'B3' | 'NYSE' => {
    if (symbol.endsWith('USDT')) return 'Crypto';
    if (symbol.endsWith('.SA')) return 'B3';
    return 'NYSE';
  };

  // Dados de distribuição de capital
  const capitalDistribution = useMemo(() => {
    const distribution: Record<string, number> = {
      Crypto: 0,
      B3: 0,
      NYSE: 0,
    };

    trades.forEach(trade => {
      const market = getMarketType(trade.symbol);
      const capital = parseFloat(trade.entryPrice) * parseFloat(trade.quantity);
      distribution[market] += capital;
    });

    return Object.entries(distribution)
      .filter(([_, value]) => value > 0)
      .map(([name, value]) => ({
        name,
        value,
        percentage: 0, // Será calculado depois
      }));
  }, [trades]);

  // Calcular percentuais
  const totalCapital = capitalDistribution.reduce((sum, item) => sum + item.value, 0);
  capitalDistribution.forEach(item => {
    item.percentage = (item.value / totalCapital) * 100;
  });

  // Dados de distribuição de P&L
  const pnlDistribution = useMemo(() => {
    const distribution: Record<string, number> = {
      Crypto: 0,
      B3: 0,
      NYSE: 0,
    };

    trades
      .filter(t => t.pnl !== null && t.status === 'CLOSED')
      .forEach(trade => {
        const market = getMarketType(trade.symbol);
        const pnl = parseFloat(trade.pnl || '0');
        distribution[market] += pnl;
      });

    return Object.entries(distribution)
      .filter(([_, value]) => Math.abs(value) > 0.01) // Filtrar valores muito pequenos
      .map(([name, value]) => ({
        name,
        value: Math.abs(value), // Usar valor absoluto para o gráfico
        actualValue: value,      // Manter valor real para tooltip
        percentage: 0,
      }));
  }, [trades]);

  // Calcular percentuais de P&L
  const totalPnL = pnlDistribution.reduce((sum, item) => sum + item.value, 0);
  pnlDistribution.forEach(item => {
    item.percentage = (item.value / totalPnL) * 100;
  });

  // Custom label para mostrar percentual
  const renderLabel = (entry: any) => {
    return `${entry.percentage.toFixed(1)}%`;
  };

  // Custom tooltip
  const CustomTooltip = ({ active, payload }: any) => {
    if (active && payload && payload.length) {
      const data = payload[0].payload;
      const isCapital = data.actualValue === undefined;
      
      return (
        <div className="bg-slate-900 border border-slate-700 p-3 rounded-lg shadow-lg">
          <p className="text-white font-semibold">{data.name}</p>
          {isCapital ? (
            <>
              <p className="text-blue-400">
                Capital: ${data.value.toLocaleString('pt-BR', { minimumFractionDigits: 2 })}
              </p>
              <p className="text-gray-400 text-sm">
                {data.percentage.toFixed(1)}% do total
              </p>
            </>
          ) : (
            <>
              <p className={`${data.actualValue >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                P&L: ${data.actualValue >= 0 ? '+' : ''}{data.actualValue.toLocaleString('pt-BR', { minimumFractionDigits: 2 })}
              </p>
              <p className="text-gray-400 text-sm">
                {data.percentage.toFixed(1)}% do total
              </p>
            </>
          )}
        </div>
      );
    }
    return null;
  };

  return (
    <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
      {/* Distribuição de Capital */}
      <Card className="bg-slate-900/50 border-slate-800">
        <CardHeader>
          <CardTitle className="text-white">Distribuição de Capital por Mercado</CardTitle>
          <p className="text-sm text-gray-400 mt-1">
            Total investido: ${totalCapital.toLocaleString('pt-BR', { minimumFractionDigits: 2 })}
          </p>
        </CardHeader>
        <CardContent>
          {capitalDistribution.length > 0 ? (
            <>
              <ResponsiveContainer width="100%" height={300}>
                <PieChart>
                  <Pie
                    data={capitalDistribution}
                    cx="50%"
                    cy="50%"
                    labelLine={false}
                    label={renderLabel}
                    outerRadius={100}
                    fill="#8884d8"
                    dataKey="value"
                  >
                    {capitalDistribution.map((entry, index) => (
                      <Cell key={`cell-${index}`} fill={COLORS[entry.name as keyof typeof COLORS]} />
                    ))}
                  </Pie>
                  <Tooltip content={<CustomTooltip />} />
                </PieChart>
              </ResponsiveContainer>
              
              {/* Legenda customizada */}
              <div className="mt-4 space-y-2">
                {capitalDistribution.map((entry) => (
                  <div key={entry.name} className="flex items-center justify-between">
                    <div className="flex items-center gap-2">
                      <div 
                        className="w-4 h-4 rounded" 
                        style={{ backgroundColor: COLORS[entry.name as keyof typeof COLORS] }}
                      />
                      <span className="text-white text-sm">{entry.name}</span>
                    </div>
                    <div className="text-right">
                      <p className="text-white text-sm font-semibold">
                        ${entry.value.toLocaleString('pt-BR', { minimumFractionDigits: 0 })}
                      </p>
                      <p className="text-gray-400 text-xs">
                        {entry.percentage.toFixed(1)}%
                      </p>
                    </div>
                  </div>
                ))}
              </div>
            </>
          ) : (
            <div className="h-[300px] flex items-center justify-center text-gray-400">
              Nenhum dado disponível
            </div>
          )}
        </CardContent>
      </Card>

      {/* Distribuição de P&L */}
      <Card className="bg-slate-900/50 border-slate-800">
        <CardHeader>
          <CardTitle className="text-white">P&L por Categoria</CardTitle>
          <p className="text-sm text-gray-400 mt-1">
            Contribuição de cada mercado para o resultado
          </p>
        </CardHeader>
        <CardContent>
          {pnlDistribution.length > 0 ? (
            <>
              <ResponsiveContainer width="100%" height={300}>
                <PieChart>
                  <Pie
                    data={pnlDistribution}
                    cx="50%"
                    cy="50%"
                    labelLine={false}
                    label={renderLabel}
                    outerRadius={100}
                    fill="#8884d8"
                    dataKey="value"
                  >
                    {pnlDistribution.map((entry, index) => (
                      <Cell key={`cell-${index}`} fill={COLORS[entry.name as keyof typeof COLORS]} />
                    ))}
                  </Pie>
                  <Tooltip content={<CustomTooltip />} />
                </PieChart>
              </ResponsiveContainer>
              
              {/* Legenda customizada */}
              <div className="mt-4 space-y-2">
                {pnlDistribution.map((entry) => (
                  <div key={entry.name} className="flex items-center justify-between">
                    <div className="flex items-center gap-2">
                      <div 
                        className="w-4 h-4 rounded" 
                        style={{ backgroundColor: COLORS[entry.name as keyof typeof COLORS] }}
                      />
                      <span className="text-white text-sm">{entry.name}</span>
                    </div>
                    <div className="text-right">
                      <p className={`text-sm font-semibold ${entry.actualValue >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                        ${entry.actualValue >= 0 ? '+' : ''}{entry.actualValue.toLocaleString('pt-BR', { minimumFractionDigits: 0 })}
                      </p>
                      <p className="text-gray-400 text-xs">
                        {entry.percentage.toFixed(1)}%
                      </p>
                    </div>
                  </div>
                ))}
              </div>
            </>
          ) : (
            <div className="h-[300px] flex items-center justify-center text-gray-400">
              Nenhuma trade fechada ainda
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
