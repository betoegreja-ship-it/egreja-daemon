import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Clock, TrendingUp, TrendingDown } from 'lucide-react';

interface Stock {
  symbol: string;
  name: string;
  price: number;
  change: number;
  change_percent: number;
  volume: number;
  market: string;
}

interface MarketStatus {
  nyse: {
    is_open: boolean;
    timezone: string;
    hours: string;
    current_time: string;
  };
  b3: {
    is_open: boolean;
    timezone: string;
    hours: string;
    current_time: string;
  };
}

interface StocksTabProps {
  stocks?: Stock[];
  marketStatus?: MarketStatus;
}

export function StocksTab({ stocks, marketStatus }: StocksTabProps) {
  const usStocks = stocks?.filter(s => s.market === 'US') || [];
  const brStocks = stocks?.filter(s => s.market === 'BR') || [];

  return (
    <div className="space-y-6 mt-6">
      {/* Status do Mercado */}
      {marketStatus && (
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-6">
          <Card className="bg-slate-900/50 border-slate-800">
            <CardHeader>
              <CardTitle className="flex items-center gap-2 text-white">
                <Clock className="w-5 h-5" />
                NYSE (Nova York)
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="flex items-center gap-3">
                <div className={`w-3 h-3 rounded-full ${marketStatus.nyse.is_open ? 'bg-green-500' : 'bg-red-500'}`} />
                <span className="text-lg font-semibold text-white">
                  {marketStatus.nyse.is_open ? 'ABERTO' : 'FECHADO'}
                </span>
              </div>
              <p className="text-sm text-white mt-2">{marketStatus.nyse.hours}</p>
              <p className="text-xs text-slate-400 mt-1">{marketStatus.nyse.current_time}</p>
            </CardContent>
          </Card>
          
          <Card className="bg-slate-900/50 border-slate-800">
            <CardHeader>
              <CardTitle className="flex items-center gap-2 text-white">
                <Clock className="w-5 h-5" />
                B3 (São Paulo)
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="flex items-center gap-3">
                <div className={`w-3 h-3 rounded-full ${marketStatus.b3.is_open ? 'bg-green-500' : 'bg-red-500'}`} />
                <span className="text-lg font-semibold text-white">
                  {marketStatus.b3.is_open ? 'ABERTO' : 'FECHADO'}
                </span>
              </div>
              <p className="text-sm text-white mt-2">{marketStatus.b3.hours}</p>
              <p className="text-xs text-slate-400 mt-1">{marketStatus.b3.current_time}</p>
            </CardContent>
          </Card>
        </div>
      )}

      {/* Ações Americanas */}
      <Card className="bg-slate-900/50 border-slate-800">
        <CardHeader>
          <CardTitle className="text-white">Ações Americanas (NYSE/NASDAQ)</CardTitle>
        </CardHeader>
        <CardContent>
          {usStocks.length === 0 ? (
            <p className="text-white text-center py-8">Carregando cotações...</p>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead>
                  <tr className="border-b border-slate-700">
                    <th className="text-left p-3 text-white font-semibold">Símbolo</th>
                    <th className="text-left p-3 text-white font-semibold">Nome</th>
                    <th className="text-right p-3 text-white font-semibold">Preço</th>
                    <th className="text-right p-3 text-white font-semibold">Variação</th>
                    <th className="text-right p-3 text-white font-semibold">Volume</th>
                  </tr>
                </thead>
                <tbody>
                  {usStocks.map((stock) => (
                    <tr key={stock.symbol} className="border-b border-slate-800 hover:bg-slate-800/50">
                      <td className="p-3 text-white font-bold">{stock.symbol}</td>
                      <td className="p-3 text-white">{stock.name}</td>
                      <td className="p-3 text-right text-white font-semibold">${stock.price.toFixed(2)}</td>
                      <td className="p-3 text-right">
                        <div className={`flex items-center justify-end gap-1 ${stock.change >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                          {stock.change >= 0 ? <TrendingUp className="w-4 h-4" /> : <TrendingDown className="w-4 h-4" />}
                          <span className="font-semibold">{stock.change >= 0 ? '+' : ''}{stock.change.toFixed(2)}</span>
                          <span>({stock.change_percent >= 0 ? '+' : ''}{stock.change_percent.toFixed(2)}%)</span>
                        </div>
                      </td>
                      <td className="p-3 text-right text-white">{stock.volume.toLocaleString()}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </CardContent>
      </Card>

      {/* Ações Brasileiras */}
      <Card className="bg-slate-900/50 border-slate-800">
        <CardHeader>
          <CardTitle className="text-white">Ações Brasileiras (B3)</CardTitle>
        </CardHeader>
        <CardContent>
          {brStocks.length === 0 ? (
            <p className="text-white text-center py-8">Carregando cotações...</p>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead>
                  <tr className="border-b border-slate-700">
                    <th className="text-left p-3 text-white font-semibold">Símbolo</th>
                    <th className="text-left p-3 text-white font-semibold">Nome</th>
                    <th className="text-right p-3 text-white font-semibold">Preço</th>
                    <th className="text-right p-3 text-white font-semibold">Variação</th>
                    <th className="text-right p-3 text-white font-semibold">Volume</th>
                  </tr>
                </thead>
                <tbody>
                  {brStocks.map((stock) => (
                    <tr key={stock.symbol} className="border-b border-slate-800 hover:bg-slate-800/50">
                      <td className="p-3 text-white font-bold">{stock.symbol}</td>
                      <td className="p-3 text-white">{stock.name}</td>
                      <td className="p-3 text-right text-white font-semibold">R${stock.price.toFixed(2)}</td>
                      <td className="p-3 text-right">
                        <div className={`flex items-center justify-end gap-1 ${stock.change >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                          {stock.change >= 0 ? <TrendingUp className="w-4 h-4" /> : <TrendingDown className="w-4 h-4" />}
                          <span className="font-semibold">{stock.change >= 0 ? '+' : ''}{stock.change.toFixed(2)}</span>
                          <span>({stock.change_percent >= 0 ? '+' : ''}{stock.change_percent.toFixed(2)}%)</span>
                        </div>
                      </td>
                      <td className="p-3 text-right text-white">{stock.volume > 0 ? stock.volume.toLocaleString() : '-'}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
