import { useMemo } from 'react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, ReferenceLine, Dot } from 'recharts';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { TrendingUp, TrendingDown } from 'lucide-react';

interface Trade {
  id: string;
  symbol: string;
  pnl: string;
  closedAt: Date;
}

interface CapitalEvolutionChartProps {
  trades: Trade[];
  initialCapital?: number;
}

export function CapitalEvolutionChart({ trades, initialCapital = 1000000 }: CapitalEvolutionChartProps) {
  const chartData = useMemo(() => {
    if (!trades || trades.length === 0) return [];

    // Ordenar trades por data
    const sortedTrades = [...trades].sort((a, b) => 
      new Date(a.closedAt).getTime() - new Date(b.closedAt).getTime()
    );

    let runningCapital = initialCapital;
    const data: Array<{
      date: string;
      capital: number;
      pnl: number;
      isImportant: boolean;
      tradeInfo: { symbol: string; pnl: number } | null;
    }> = [
      {
        date: 'Início',
        capital: initialCapital,
        pnl: 0,
        isImportant: false,
        tradeInfo: null,
      }
    ];

    sortedTrades.forEach((trade, index) => {
      const tradePnL = parseFloat(trade.pnl || '0');
      runningCapital += tradePnL;

      // Marcar trades importantes (> $5000 de P&L absoluto)
      const isImportant = Math.abs(tradePnL) > 5000;

      data.push({
        date: new Date(trade.closedAt).toLocaleDateString('pt-BR', { 
          day: '2-digit', 
          month: '2-digit' 
        }),
        capital: runningCapital,
        pnl: tradePnL,
        isImportant,
        tradeInfo: isImportant ? {
          symbol: trade.symbol,
          pnl: tradePnL,
        } : null,
      });
    });

    return data;
  }, [trades, initialCapital]);

  // Calcular drawdown máximo
  const maxDrawdown = useMemo(() => {
    if (chartData.length === 0) return 0;
    
    let peak = chartData[0].capital;
    let maxDD = 0;

    chartData.forEach((point) => {
      if (point.capital > peak) {
        peak = point.capital;
      } else {
        const dd = ((peak - point.capital) / peak) * 100;
        maxDD = Math.max(maxDD, dd);
      }
    });

    return maxDD;
  }, [chartData]);

  const currentCapital = chartData[chartData.length - 1]?.capital || initialCapital;
  const totalGain = currentCapital - initialCapital;
  const totalGainPercent = ((totalGain / initialCapital) * 100);

  // Custom dot para marcar trades importantes
  const CustomDot = (props: any) => {
    const { cx, cy, payload } = props;
    if (payload.isImportant) {
      const color = payload.pnl > 0 ? '#10b981' : '#ef4444';
      return (
        <g>
          <circle cx={cx} cy={cy} r={6} fill={color} stroke="#fff" strokeWidth={2} />
          <circle cx={cx} cy={cy} r={3} fill="#fff" />
        </g>
      );
    }
    return null;
  };

  // Custom tooltip
  const CustomTooltip = ({ active, payload }: any) => {
    if (active && payload && payload.length) {
      const data = payload[0].payload;
      return (
        <div className="bg-slate-900 border border-slate-700 p-3 rounded-lg shadow-lg">
          <p className="text-white font-semibold">{data.date}</p>
          <p className="text-blue-400">
            Capital: ${data.capital.toLocaleString('pt-BR', { minimumFractionDigits: 2 })}
          </p>
          {data.tradeInfo && (
            <>
              <p className="text-gray-400 text-sm mt-1">Trade Importante:</p>
              <p className="text-white text-sm">{data.tradeInfo.symbol}</p>
              <p className={`text-sm font-semibold ${data.tradeInfo.pnl > 0 ? 'text-green-400' : 'text-red-400'}`}>
                P&L: ${data.tradeInfo.pnl > 0 ? '+' : ''}{data.tradeInfo.pnl.toLocaleString('pt-BR', { minimumFractionDigits: 2 })}
              </p>
            </>
          )}
        </div>
      );
    }
    return null;
  };

  return (
    <Card className="bg-slate-900/50 border-slate-800">
      <CardHeader>
        <div className="flex items-center justify-between">
          <CardTitle className="text-white">Evolução Patrimonial</CardTitle>
          <div className="flex items-center gap-2">
            {totalGain >= 0 ? (
              <TrendingUp className="w-5 h-5 text-green-400" />
            ) : (
              <TrendingDown className="w-5 h-5 text-red-400" />
            )}
            <span className={`text-lg font-bold ${totalGain >= 0 ? 'text-green-400' : 'text-red-400'}`}>
              {totalGain >= 0 ? '+' : ''}${totalGain.toLocaleString('pt-BR', { minimumFractionDigits: 2 })}
              {' '}({totalGain >= 0 ? '+' : ''}{totalGainPercent.toFixed(2)}%)
            </span>
          </div>
        </div>
        {maxDrawdown > 0 && (
          <p className="text-sm text-gray-400 mt-2">
            🔴 Drawdown máximo: {maxDrawdown.toFixed(2)}%
          </p>
        )}
      </CardHeader>
      <CardContent>
        <ResponsiveContainer width="100%" height={400}>
          <LineChart data={chartData}>
            <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
            <XAxis 
              dataKey="date" 
              stroke="#94a3b8"
              tick={{ fill: '#94a3b8' }}
            />
            <YAxis 
              stroke="#94a3b8"
              tick={{ fill: '#94a3b8' }}
              tickFormatter={(value) => `$${(value / 1000).toFixed(0)}k`}
            />
            <Tooltip content={<CustomTooltip />} />
            <Legend />
            
            {/* Linha de capital inicial */}
            <ReferenceLine 
              y={initialCapital} 
              stroke="#64748b" 
              strokeDasharray="5 5"
              label={{ value: 'Capital Inicial', fill: '#64748b', position: 'insideTopRight' }}
            />
            
            {/* Linha de evolução */}
            <Line 
              type="monotone" 
              dataKey="capital" 
              stroke="#3b82f6" 
              strokeWidth={3}
              dot={<CustomDot />}
              name="Capital"
              activeDot={{ r: 8 }}
            />
          </LineChart>
        </ResponsiveContainer>
        
        {/* Legenda */}
        <div className="mt-4 flex items-center gap-6 text-sm">
          <div className="flex items-center gap-2">
            <div className="w-3 h-3 rounded-full bg-green-500 border-2 border-white"></div>
            <span className="text-gray-400">Trade Importante (+)</span>
          </div>
          <div className="flex items-center gap-2">
            <div className="w-3 h-3 rounded-full bg-red-500 border-2 border-white"></div>
            <span className="text-gray-400">Trade Importante (-)</span>
          </div>
        </div>
      </CardContent>
    </Card>
  );
}
