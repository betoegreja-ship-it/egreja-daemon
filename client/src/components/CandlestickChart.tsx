import React, { useState, useEffect } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { ComposedChart, Line, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, ReferenceLine } from 'recharts';
import { TrendingUp, TrendingDown, Activity } from 'lucide-react';

interface CandlestickChartProps {
  symbol: string;
  onClose?: () => void;
}

type Timeframe = '1m' | '5m' | '15m' | '1h' | '4h' | '1d';

interface CandleData {
  time: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
  sma20?: number;
  sma50?: number;
  ema12?: number;
  ema26?: number;
  bollingerUpper?: number;
  bollingerLower?: number;
}

export function CandlestickChart({ symbol, onClose }: CandlestickChartProps) {
  const [timeframe, setTimeframe] = useState<Timeframe>('15m');
  const [data, setData] = useState<CandleData[]>([]);
  const [loading, setLoading] = useState(true);
  const [showIndicators, setShowIndicators] = useState({
    sma: true,
    ema: false,
    bollinger: false,
  });

  useEffect(() => {
    fetchCandleData();
    const interval = setInterval(fetchCandleData, 60000); // Atualiza a cada minuto
    return () => clearInterval(interval);
  }, [symbol, timeframe]);

  const fetchCandleData = async () => {
    try {
      setLoading(true);
      
      // Mapear timeframe para intervalo da API
      const intervalMap: Record<Timeframe, string> = {
        '1m': '1m',
        '5m': '5m',
        '15m': '15m',
        '1h': '1h',
        '4h': '4h',
        '1d': '1d',
      };

      const interval = intervalMap[timeframe];
      const limit = timeframe === '1m' ? 60 : timeframe === '5m' ? 100 : 100;

      const response = await fetch(
        `https://api.binance.com/api/v3/klines?symbol=${symbol}&interval=${interval}&limit=${limit}`
      );

      if (!response.ok) {
        // Fallback para dados simulados se API falhar
        setData(generateMockData(limit));
        setLoading(false);
        return;
      }

      const rawData = await response.json();
      const processedData = processCandles(rawData);
      setData(processedData);
      setLoading(false);
    } catch (error) {
      console.error('Error fetching candle data:', error);
      setData(generateMockData(100));
      setLoading(false);
    }
  };

  const processCandles = (rawData: any[]): CandleData[] => {
    const candles: CandleData[] = rawData.map((candle) => ({
      time: new Date(candle[0]).toLocaleTimeString('pt-BR', { hour: '2-digit', minute: '2-digit' }),
      open: parseFloat(candle[1]),
      high: parseFloat(candle[2]),
      low: parseFloat(candle[3]),
      close: parseFloat(candle[4]),
      volume: parseFloat(candle[5]),
    }));

    // Calcular indicadores
    return calculateIndicators(candles);
  };

  const calculateIndicators = (candles: CandleData[]): CandleData[] => {
    const closes = candles.map(c => c.close);
    
    // SMA 20 e 50
    for (let i = 0; i < candles.length; i++) {
      if (i >= 19) {
        const sum20 = closes.slice(i - 19, i + 1).reduce((a, b) => a + b, 0);
        candles[i].sma20 = sum20 / 20;
      }
      if (i >= 49) {
        const sum50 = closes.slice(i - 49, i + 1).reduce((a, b) => a + b, 0);
        candles[i].sma50 = sum50 / 50;
      }
    }

    // EMA 12 e 26
    const ema12 = calculateEMA(closes, 12);
    const ema26 = calculateEMA(closes, 26);
    candles.forEach((candle, i) => {
      candle.ema12 = ema12[i];
      candle.ema26 = ema26[i];
    });

    // Bollinger Bands
    for (let i = 19; i < candles.length; i++) {
      const slice = closes.slice(i - 19, i + 1);
      const mean = slice.reduce((a, b) => a + b, 0) / 20;
      const variance = slice.reduce((sum, val) => sum + Math.pow(val - mean, 2), 0) / 20;
      const stdDev = Math.sqrt(variance);
      
      candles[i].bollingerUpper = mean + (2 * stdDev);
      candles[i].bollingerLower = mean - (2 * stdDev);
    }

    return candles;
  };

  const calculateEMA = (data: number[], period: number): number[] => {
    const k = 2 / (period + 1);
    const ema: number[] = [];
    ema[0] = data[0];

    for (let i = 1; i < data.length; i++) {
      ema[i] = data[i] * k + ema[i - 1] * (1 - k);
    }

    return ema;
  };

  const generateMockData = (count: number): CandleData[] => {
    const data: CandleData[] = [];
    let price = 50000 + Math.random() * 10000;

    for (let i = 0; i < count; i++) {
      const change = (Math.random() - 0.5) * 500;
      const open = price;
      const close = price + change;
      const high = Math.max(open, close) + Math.random() * 200;
      const low = Math.min(open, close) - Math.random() * 200;

      data.push({
        time: `${String(Math.floor(i / 60)).padStart(2, '0')}:${String(i % 60).padStart(2, '0')}`,
        open,
        high,
        low,
        close,
        volume: 100 + Math.random() * 500,
      });

      price = close;
    }

    return calculateIndicators(data);
  };

  const CustomCandlestick = (props: any) => {
    const { x, y, width, height, open, close, high, low } = props;
    const isGreen = close > open;
    const color = isGreen ? '#10b981' : '#ef4444';
    const bodyHeight = Math.abs(close - open) * height / (high - low);
    const bodyY = y + (high - Math.max(open, close)) * height / (high - low);

    return (
      <g>
        {/* Wick */}
        <line
          x1={x + width / 2}
          y1={y}
          x2={x + width / 2}
          y2={y + height}
          stroke={color}
          strokeWidth={1}
        />
        {/* Body */}
        <rect
          x={x}
          y={bodyY}
          width={width}
          height={bodyHeight || 1}
          fill={color}
          stroke={color}
          strokeWidth={1}
        />
      </g>
    );
  };

  const currentPrice = data.length > 0 ? data[data.length - 1].close : 0;
  const priceChange = data.length > 1 ? currentPrice - data[0].close : 0;
  const priceChangePercent = data.length > 1 ? (priceChange / data[0].close) * 100 : 0;

  return (
    <Card className="bg-gradient-to-br from-slate-900 to-slate-800 border-slate-700">
      <CardHeader>
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-4">
            <CardTitle className="text-2xl text-white">
              {symbol.replace('USDT', '')} / USDT
            </CardTitle>
            <div className="flex flex-col">
              <div className="text-2xl font-bold text-white">
                ${currentPrice.toFixed(2)}
              </div>
              <div className={`flex items-center gap-1 text-sm ${priceChange >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                {priceChange >= 0 ? <TrendingUp className="w-4 h-4" /> : <TrendingDown className="w-4 h-4" />}
                {priceChange >= 0 ? '+' : ''}{priceChange.toFixed(2)} ({priceChangePercent.toFixed(2)}%)
              </div>
            </div>
          </div>

          <div className="flex items-center gap-2">
            {/* Timeframe Selector */}
            <div className="flex gap-1">
              {(['1m', '5m', '15m', '1h', '4h', '1d'] as Timeframe[]).map((tf) => (
                <Button
                  key={tf}
                  variant={timeframe === tf ? 'default' : 'outline'}
                  size="sm"
                  onClick={() => setTimeframe(tf)}
                  className={timeframe === tf ? 'bg-blue-600 hover:bg-blue-700' : ''}
                >
                  {tf}
                </Button>
              ))}
            </div>

            {onClose && (
              <Button variant="outline" size="sm" onClick={onClose}>
                Fechar
              </Button>
            )}
          </div>
        </div>

        {/* Indicators Toggle */}
        <div className="flex gap-2 mt-4">
          <Badge
            className={`cursor-pointer ${showIndicators.sma ? 'bg-blue-600' : 'bg-slate-700'}`}
            onClick={() => setShowIndicators(prev => ({ ...prev, sma: !prev.sma }))}
          >
            SMA 20/50
          </Badge>
          <Badge
            className={`cursor-pointer ${showIndicators.ema ? 'bg-purple-600' : 'bg-slate-700'}`}
            onClick={() => setShowIndicators(prev => ({ ...prev, ema: !prev.ema }))}
          >
            EMA 12/26
          </Badge>
          <Badge
            className={`cursor-pointer ${showIndicators.bollinger ? 'bg-orange-600' : 'bg-slate-700'}`}
            onClick={() => setShowIndicators(prev => ({ ...prev, bollinger: !prev.bollinger }))}
          >
            Bollinger Bands
          </Badge>
        </div>
      </CardHeader>

      <CardContent>
        {loading ? (
          <div className="h-96 flex items-center justify-center">
            <Activity className="w-8 h-8 text-slate-600 animate-spin" />
          </div>
        ) : (
          <div className="space-y-4">
            {/* Price Chart */}
            <ResponsiveContainer width="100%" height={400}>
              <ComposedChart data={data}>
                <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
                <XAxis
                  dataKey="time"
                  stroke="#94a3b8"
                  tick={{ fontSize: 12 }}
                  interval="preserveStartEnd"
                />
                <YAxis
                  stroke="#94a3b8"
                  tick={{ fontSize: 12 }}
                  domain={['dataMin - 100', 'dataMax + 100']}
                />
                <Tooltip
                  contentStyle={{
                    backgroundColor: '#1e293b',
                    border: '1px solid #475569',
                    borderRadius: '8px',
                  }}
                  labelStyle={{ color: '#f1f5f9' }}
                />
                <Legend />

                {/* Candlesticks */}
                <Bar dataKey="high" fill="transparent" />

                {/* Indicators */}
                {showIndicators.sma && (
                  <>
                    <Line type="monotone" dataKey="sma20" stroke="#3b82f6" strokeWidth={2} dot={false} name="SMA 20" />
                    <Line type="monotone" dataKey="sma50" stroke="#60a5fa" strokeWidth={2} dot={false} name="SMA 50" />
                  </>
                )}
                {showIndicators.ema && (
                  <>
                    <Line type="monotone" dataKey="ema12" stroke="#a855f7" strokeWidth={2} dot={false} name="EMA 12" />
                    <Line type="monotone" dataKey="ema26" stroke="#c084fc" strokeWidth={2} dot={false} name="EMA 26" />
                  </>
                )}
                {showIndicators.bollinger && (
                  <>
                    <Line type="monotone" dataKey="bollingerUpper" stroke="#f97316" strokeWidth={1} strokeDasharray="5 5" dot={false} name="BB Upper" />
                    <Line type="monotone" dataKey="bollingerLower" stroke="#f97316" strokeWidth={1} strokeDasharray="5 5" dot={false} name="BB Lower" />
                  </>
                )}
              </ComposedChart>
            </ResponsiveContainer>

            {/* Volume Chart */}
            <ResponsiveContainer width="100%" height={150}>
              <ComposedChart data={data}>
                <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
                <XAxis
                  dataKey="time"
                  stroke="#94a3b8"
                  tick={{ fontSize: 12 }}
                  interval="preserveStartEnd"
                />
                <YAxis stroke="#94a3b8" tick={{ fontSize: 12 }} />
                <Tooltip
                  contentStyle={{
                    backgroundColor: '#1e293b',
                    border: '1px solid #475569',
                    borderRadius: '8px',
                  }}
                />
                <Bar dataKey="volume" fill="#6366f1" opacity={0.6} name="Volume" />
              </ComposedChart>
            </ResponsiveContainer>
          </div>
        )}
      </CardContent>
    </Card>
  );
}
