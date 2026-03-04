/**
 * Stock Market Integration - tRPC procedures
 * Provides real-time stock quotes and market status via Yahoo Finance API
 */

import { z } from "zod";
import { publicProcedure } from "./_core/trpc";

// Stock quote schema
const StockQuoteSchema = z.object({
  symbol: z.string(),
  name: z.string(),
  price: z.number(),
  open: z.number(),
  high: z.number(),
  low: z.number(),
  volume: z.number(),
  change: z.number(),
  change_percent: z.number(),
  currency: z.string(),
  exchange: z.string(),
  market: z.string(),
  timestamp: z.string(),
});

// Market status schema
const MarketStatusSchema = z.object({
  nyse: z.object({
    is_open: z.boolean(),
    timezone: z.string(),
    hours: z.string(),
    current_time: z.string(),
  }),
  b3: z.object({
    is_open: z.boolean(),
    timezone: z.string(),
    hours: z.string(),
    current_time: z.string(),
  }),
});

// Lista de ações para monitorar
const US_STOCKS = [
  { symbol: "AAPL", name: "Apple Inc." },
  { symbol: "MSFT", name: "Microsoft Corporation" },
  { symbol: "GOOGL", name: "Alphabet Inc." },
  { symbol: "AMZN", name: "Amazon.com Inc." },
  { symbol: "TSLA", name: "Tesla Inc." },
  { symbol: "NVDA", name: "NVIDIA Corporation" },
  { symbol: "META", name: "Meta Platforms Inc." },
  { symbol: "NFLX", name: "Netflix Inc." },
  { symbol: "AMD", name: "Advanced Micro Devices" },
  { symbol: "BABA", name: "Alibaba Group" },
];

const BR_STOCKS = [
  { symbol: "PETR4.SA", name: "Petrobras PN" },
  { symbol: "VALE3.SA", name: "Vale ON" },
  { symbol: "ITUB4.SA", name: "Itaú Unibanco PN" },
  { symbol: "BBDC4.SA", name: "Bradesco PN" },
  { symbol: "ABEV3.SA", name: "Ambev ON" },
  { symbol: "WEGE3.SA", name: "WEG ON" },
  { symbol: "RENT3.SA", name: "Localiza ON" },
  { symbol: "MGLU3.SA", name: "Magazine Luiza ON" },
];

/**
 * Fetch stock quote from Yahoo Finance
 */
async function fetchYahooQuote(symbol: string, market: string): Promise<any> {
  try {
    const response = await fetch(
      `https://query1.finance.yahoo.com/v8/finance/chart/${symbol}?interval=1d&range=1d`,
      {
        headers: {
          "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        },
      }
    );

    if (!response.ok) {
      throw new Error(`Yahoo Finance API error: ${response.status}`);
    }

    const data = await response.json();
    const result = data.chart.result[0];
    const meta = result.meta;
    const quote = result.indicators.quote[0];

    const currentPrice = meta.regularMarketPrice || 0;
    const previousClose = meta.chartPreviousClose || meta.previousClose || 0;
    const change = currentPrice - previousClose;
    const changePercent = previousClose > 0 ? (change / previousClose) * 100 : 0;

    return {
      symbol: symbol.replace(".SA", ""),
      name: meta.longName || meta.shortName || symbol,
      price: currentPrice,
      open: quote.open?.[0] || currentPrice,
      high: quote.high?.[0] || currentPrice,
      low: quote.low?.[0] || currentPrice,
      volume: quote.volume?.[0] || 0,
      change: change,
      change_percent: changePercent,
      currency: meta.currency || "USD",
      exchange: meta.exchangeName || market,
      market: market,
      timestamp: new Date(meta.regularMarketTime * 1000).toISOString(),
    };
  } catch (error) {
    console.error(`Error fetching ${symbol}:`, error);
    return null;
  }
}

/**
 * Check if market is open
 */
function isMarketOpen(timezone: string, openHour: number, closeHour: number): boolean {
  const now = new Date();
  const options: Intl.DateTimeFormatOptions = {
    timeZone: timezone,
    hour: "numeric",
    minute: "numeric",
    weekday: "short",
    hour12: false,
  };
  
  const formatter = new Intl.DateTimeFormat("en-US", options);
  const parts = formatter.formatToParts(now);
  
  const weekday = parts.find(p => p.type === "weekday")?.value || "";
  const hour = parseInt(parts.find(p => p.type === "hour")?.value || "0");
  const minute = parseInt(parts.find(p => p.type === "minute")?.value || "0");
  
  // Check if weekend
  if (weekday === "Sat" || weekday === "Sun") {
    return false;
  }
  
  // Check if within trading hours
  const currentMinutes = hour * 60 + minute;
  const openMinutes = openHour * 60 + 30; // 9:30 AM
  const closeMinutes = closeHour * 60; // 4:00 PM or 5:00 PM
  
  return currentMinutes >= openMinutes && currentMinutes < closeMinutes;
}

/**
 * Get all stock quotes (US + BR)
 */
export const getAllStocks = publicProcedure
  .output(z.array(StockQuoteSchema))
  .query(async () => {
    try {
      const usPromises = US_STOCKS.map(stock => fetchYahooQuote(stock.symbol, "US"));
      const brPromises = BR_STOCKS.map(stock => fetchYahooQuote(stock.symbol, "BR"));
      
      const [usResults, brResults] = await Promise.all([
        Promise.all(usPromises),
        Promise.all(brPromises),
      ]);
      
      const allStocks = [...usResults, ...brResults].filter(stock => stock !== null);
      return allStocks;
    } catch (error) {
      console.error("Error fetching stocks:", error);
      return [];
    }
  });

/**
 * Get market status (NYSE and B3)
 */
export const getMarketStatus = publicProcedure
  .output(MarketStatusSchema)
  .query(async () => {
    const nyseOpen = isMarketOpen("America/New_York", 9, 16);
    const b3Open = isMarketOpen("America/Sao_Paulo", 10, 17);
    
    const nyseTime = new Date().toLocaleString("en-US", {
      timeZone: "America/New_York",
      hour12: false,
    });
    
    const b3Time = new Date().toLocaleString("en-US", {
      timeZone: "America/Sao_Paulo",
      hour12: false,
    });
    
    return {
      nyse: {
        is_open: nyseOpen,
        timezone: "America/New_York",
        hours: "9:30 AM - 4:00 PM EST",
        current_time: nyseTime,
      },
      b3: {
        is_open: b3Open,
        timezone: "America/Sao_Paulo",
        hours: "10:00 AM - 5:00 PM BRT",
        current_time: b3Time,
      },
    };
  });

/**
 * Get single stock quote
 */
export const getStockQuote = publicProcedure
  .input(z.object({
    symbol: z.string(),
    region: z.enum(["US", "BR"]).default("US"),
  }))
  .output(StockQuoteSchema.nullable())
  .query(async ({ input }) => {
    const symbolWithSuffix = input.region === "BR" ? `${input.symbol}.SA` : input.symbol;
    return await fetchYahooQuote(symbolWithSuffix, input.region);
  });
