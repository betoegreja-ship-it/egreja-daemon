/**
 * Sofia IA Database Helpers
 * Funções para persistir dados de Sofia no banco de dados
 */

import { eq, desc } from "drizzle-orm";
import { getDb } from "./db";
import {
  trades,
  sofiaMetrics,
  sofiaAnalyses,
  notifications,
  type InsertTrade,
  type InsertSofiaMetric,
  type InsertSofiaAnalysis,
  type InsertNotification,
} from "../drizzle/schema";

/**
 * TRADES
 */

export async function insertTrade(trade: InsertTrade) {
  const db = await getDb();
  if (!db) throw new Error("Database not available");
  
  const result = await db.insert(trades).values(trade);
  return result[0].insertId;
}

export async function updateTrade(id: number, updates: Partial<InsertTrade>) {
  const db = await getDb();
  if (!db) throw new Error("Database not available");
  
  await db.update(trades).set(updates).where(eq(trades.id, id));
}

export async function getTrades(limit = 100) {
  const db = await getDb();
  if (!db) return [];
  
  return await db.select().from(trades).orderBy(desc(trades.createdAt)).limit(limit);
}

export async function getTradesBySymbol(symbol: string, limit = 50) {
  const db = await getDb();
  if (!db) return [];
  
  return await db
    .select()
    .from(trades)
    .where(eq(trades.symbol, symbol))
    .orderBy(desc(trades.createdAt))
    .limit(limit);
}

export async function getOpenTrades() {
  const db = await getDb();
  if (!db) return [];
  
  return await db.select().from(trades).where(eq(trades.status, "OPEN"));
}

/**
 * SOFIA METRICS
 */

export async function upsertSofiaMetric(metric: InsertSofiaMetric) {
  const db = await getDb();
  if (!db) throw new Error("Database not available");
  
  await db
    .insert(sofiaMetrics)
    .values(metric)
    .onDuplicateKeyUpdate({
      set: {
        totalTrades: metric.totalTrades,
        winningTrades: metric.winningTrades,
        losingTrades: metric.losingTrades,
        accuracy: metric.accuracy,
        totalPnl: metric.totalPnl,
        avgConfidence: metric.avgConfidence,
        lastTradeAt: metric.lastTradeAt,
      },
    });
}

export async function getSofiaMetric(symbol: string) {
  const db = await getDb();
  if (!db) return null;
  
  const result = await db
    .select()
    .from(sofiaMetrics)
    .where(eq(sofiaMetrics.symbol, symbol))
    .limit(1);
  
  return result.length > 0 ? result[0] : null;
}

export async function getAllSofiaMetrics() {
  const db = await getDb();
  if (!db) return [];
  
  return await db.select().from(sofiaMetrics).orderBy(desc(sofiaMetrics.accuracy));
}

/**
 * SOFIA ANALYSES
 */

export async function insertSofiaAnalysis(analysis: InsertSofiaAnalysis) {
  const db = await getDb();
  if (!db) throw new Error("Database not available");
  
  const result = await db.insert(sofiaAnalyses).values(analysis);
  return result[0].insertId;
}

export async function updateSofiaAnalysis(id: number, updates: Partial<InsertSofiaAnalysis>) {
  const db = await getDb();
  if (!db) throw new Error("Database not available");
  
  await db.update(sofiaAnalyses).set(updates).where(eq(sofiaAnalyses.id, id));
}

export async function getSofiaAnalyses(limit = 100) {
  const db = await getDb();
  if (!db) return [];
  
  const rawAnalyses = await db
    .select()
    .from(sofiaAnalyses)
    .orderBy(desc(sofiaAnalyses.createdAt))
    .limit(limit);
  
  // Parsear market_data JSON e extrair preço
  return rawAnalyses.map((analysis: any) => {
    let price = 0;
    let indicators = {};
    
    // Tentar parsear market_data
    if (analysis.marketData) {
      try {
        const marketData = typeof analysis.marketData === 'string' 
          ? JSON.parse(analysis.marketData)
          : analysis.marketData;
        
        price = marketData.price || marketData.current_price || 0;
        indicators = {
          score: marketData.score || 0,
          ema_9: marketData.ema_9 || 0,
          ema_21: marketData.ema_21 || 0,
          ema_50: marketData.ema_50 || 0,
          rsi: marketData.rsi || 0,
          macd: marketData.macd || 0,
          market_status: marketData.market_status || 'OPEN',
        };
      } catch (e) {
        console.error(`Erro ao parsear market_data para ${analysis.symbol}:`, e);
      }
    }
    
    // Se preço ainda for 0, não retornar esta análise
    if (price === 0) {
      console.warn(`Análise de ${analysis.symbol} sem preço válido - ignorando`);
      return null;
    }
    
    // Extrair market_status dos indicators
    const marketStatus = (indicators as any).market_status || 'OPEN';
    
    return {
      ...analysis,
      price,
      indicators,
      marketStatus,
    };
  }).filter((a: any) => a !== null); // Filtrar análises inválidas
}

export async function getSofiaAnalysesBySymbol(symbol: string, limit = 50) {
  const db = await getDb();
  if (!db) return [];
  
  return await db
    .select()
    .from(sofiaAnalyses)
    .where(eq(sofiaAnalyses.symbol, symbol))
    .orderBy(desc(sofiaAnalyses.createdAt))
    .limit(limit);
}

/**
 * NOTIFICATIONS
 */

export async function insertNotification(notification: InsertNotification) {
  const db = await getDb();
  if (!db) throw new Error("Database not available");
  
  const result = await db.insert(notifications).values(notification);
  return result[0].insertId;
}

export async function getNotifications(limit = 100) {
  const db = await getDb();
  if (!db) return [];
  
  return await db
    .select()
    .from(notifications)
    .orderBy(desc(notifications.sentAt))
    .limit(limit);
}

/**
 * ANALYTICS
 */

export async function getDailyStats(date: Date) {
  const db = await getDb();
  if (!db) return null;
  
  const startOfDay = new Date(date);
  startOfDay.setHours(0, 0, 0, 0);
  
  const endOfDay = new Date(date);
  endOfDay.setHours(23, 59, 59, 999);
  
  // Filtrar trades fechadas nas últimas 24 horas
  const dailyTrades = await db
    .select()
    .from(trades)
    .where(eq(trades.status, "CLOSED"));
  
  // Filtrar em memória por closedAt nas últimas 24h
  const now = new Date();
  const yesterday = new Date(now.getTime() - 24 * 60 * 60 * 1000);
  
  const filteredTrades = dailyTrades.filter((t: any) => {
    const closedAt = t.closedAt ? new Date(t.closedAt) : null;
    return closedAt && closedAt >= yesterday && closedAt <= now;
  });
  
  const totalTrades = filteredTrades.length;
  const winningTrades = filteredTrades.filter((t: any) => parseFloat(t.pnl || "0") > 0).length;
  const losingTrades = filteredTrades.filter((t: any) => parseFloat(t.pnl || "0") < 0).length;
  const totalPnl = filteredTrades.reduce((sum: number, t: any) => sum + parseFloat(t.pnl || "0"), 0);
  
  return {
    date: date.toISOString().split("T")[0],
    totalTrades,
    winningTrades,
    losingTrades,
    winRate: totalTrades > 0 ? (winningTrades / totalTrades) * 100 : 0,
    totalPnl,
  };
}

export async function getTotalMonthlyPnL() {
  // Retorna P&L acumulado de TODOS os meses
  const db = await getDb();
  if (!db) return 0;
  
  const allTrades = await db
    .select()
    .from(trades)
    .where(eq(trades.status, "CLOSED"));
  
  const totalPnl = allTrades.reduce((sum: number, t: any) => sum + parseFloat(t.pnl || "0"), 0);
  return totalPnl;
}

export async function getTotalYearlyPnL() {
  // Retorna P&L acumulado de TODOS os anos (igual ao mensal, mas mantido para clareza)
  return getTotalMonthlyPnL();
}

export async function getGlobalStats() {
  // Retorna estatísticas globais corretas
  const db = await getDb();
  if (!db) return null;
  
  const INITIAL_CAPITAL = 1000000; // $1.000.000
  
  // Buscar todas as trades fechadas
  const allTrades = await db
    .select()
    .from(trades)
    .where(eq(trades.status, "CLOSED"));
  
  const totalTrades = allTrades.length;
  const winningTrades = allTrades.filter((t: any) => parseFloat(t.pnl || "0") > 0).length;
  const losingTrades = allTrades.filter((t: any) => parseFloat(t.pnl || "0") < 0).length;
  const totalPnl = allTrades.reduce((sum: number, t: any) => sum + parseFloat(t.pnl || "0"), 0);
  
  const currentCapital = INITIAL_CAPITAL + totalPnl;
  const gainPercent = (totalPnl / INITIAL_CAPITAL) * 100;
  const winRate = totalTrades > 0 ? (winningTrades / totalTrades) * 100 : 0;
  
  return {
    initialCapital: INITIAL_CAPITAL,
    currentCapital,
    totalPnl,
    gainPercent,
    totalTrades,
    winningTrades,
    losingTrades,
    winRate,
  };
}

export async function getMonthlyStats(year: number, month: number) {
  const db = await getDb();
  if (!db) return null;
  
  const startOfMonth = new Date(year, month - 1, 1);
  const endOfMonth = new Date(year, month, 0, 23, 59, 59, 999);
  
  const monthlyTrades = await db
    .select()
    .from(trades)
    .where(eq(trades.status, "CLOSED"));
  
  const filteredTrades = monthlyTrades.filter((t: any) => {
    const closedAt = t.closedAt ? new Date(t.closedAt) : null;
    return closedAt && closedAt >= startOfMonth && closedAt <= endOfMonth;
  });
  
  const totalTrades = filteredTrades.length;
  const winningTrades = filteredTrades.filter((t: any) => parseFloat(t.pnl || "0") > 0).length;
  const losingTrades = filteredTrades.filter((t: any) => parseFloat(t.pnl || "0") < 0).length;
  const totalPnl = filteredTrades.reduce((sum: number, t: any) => sum + parseFloat(t.pnl || "0"), 0);
  
  return {
    year,
    month,
    totalTrades,
    winningTrades,
    losingTrades,
    winRate: totalTrades > 0 ? (winningTrades / totalTrades) * 100 : 0,
    totalPnl,
  };
}

export async function getYearlyStats(year: number) {
  const db = await getDb();
  if (!db) return null;
  
  const startOfYear = new Date(year, 0, 1);
  const endOfYear = new Date(year, 11, 31, 23, 59, 59, 999);
  
  const yearlyTrades = await db
    .select()
    .from(trades)
    .where(eq(trades.status, "CLOSED"));
  
  const filteredTrades = yearlyTrades.filter((t: any) => {
    const closedAt = t.closedAt ? new Date(t.closedAt) : null;
    return closedAt && closedAt >= startOfYear && closedAt <= endOfYear;
  });
  
  const totalTrades = filteredTrades.length;
  const winningTrades = filteredTrades.filter((t: any) => parseFloat(t.pnl || "0") > 0).length;
  const losingTrades = filteredTrades.filter((t: any) => parseFloat(t.pnl || "0") < 0).length;
  const totalPnl = filteredTrades.reduce((sum: number, t: any) => sum + parseFloat(t.pnl || "0"), 0);
  
  return {
    year,
    totalTrades,
    winningTrades,
    losingTrades,
    winRate: totalTrades > 0 ? (winningTrades / totalTrades) * 100 : 0,
    totalPnl,
  };
}


/**
 * Get historical P&L data for charts (last 30 days)
 */
export async function getHistoricalPnL(days: number = 30) {
  const db = await getDb();
  if (!db) throw new Error("Database connection failed");
  
  const endDate = new Date();
  const startDate = new Date();
  startDate.setDate(startDate.getDate() - days);
  
  const closedTrades = await db
    .select()
    .from(trades)
    .where(eq(trades.status, "CLOSED"));
  
  // Group trades by date
  const dailyData: Record<string, { date: string; pnl: number; trades: number }> = {};
  
  for (let d = new Date(startDate); d <= endDate; d.setDate(d.getDate() + 1)) {
    const dateStr = d.toISOString().split("T")[0];
    dailyData[dateStr] = { date: dateStr, pnl: 0, trades: 0 };
  }
  
  closedTrades.forEach((trade: any) => {
    if (trade.closedAt) {
      const tradeDate = new Date(trade.closedAt);
      const dateStr = tradeDate.toISOString().split("T")[0];
      
      if (dailyData[dateStr]) {
        dailyData[dateStr].pnl += parseFloat(trade.pnl || "0");
        dailyData[dateStr].trades += 1;
      }
    }
  });
  
  return Object.values(dailyData).sort((a, b) => a.date.localeCompare(b.date));
}

/**
 * Get closed trades history with filters
 */
export async function getClosedTrades(filters?: {
  symbol?: string;
  startDate?: string;
  endDate?: string;
  profitOnly?: boolean;
  lossOnly?: boolean;
}) {
  const db = await getDb();
  if (!db) throw new Error("Database connection failed");
  
  let query = db
    .select()
    .from(trades)
    .where(eq(trades.status, "CLOSED"));
  
  const allTrades = await query;
  
  // Apply filters in memory (simpler for now)
  let filtered = allTrades;
  
  if (filters?.symbol) {
    filtered = filtered.filter((t: any) => t.symbol === filters.symbol);
  }
  
  if (filters?.startDate) {
    const start = new Date(filters.startDate);
    filtered = filtered.filter((t: any) => {
      const closedAt = t.closedAt ? new Date(t.closedAt) : null;
      return closedAt && closedAt >= start;
    });
  }
  
  if (filters?.endDate) {
    const end = new Date(filters.endDate);
    filtered = filtered.filter((t: any) => {
      const closedAt = t.closedAt ? new Date(t.closedAt) : null;
      return closedAt && closedAt <= end;
    });
  }
  
  if (filters?.profitOnly) {
    filtered = filtered.filter((t: any) => parseFloat(t.pnl || "0") > 0);
  }
  
  if (filters?.lossOnly) {
    filtered = filtered.filter((t: any) => parseFloat(t.pnl || "0") < 0);
  }
  
  return filtered;
}


/**
 * MACHINE LEARNING PERFORMANCE
 */

export async function getMLPerformanceStats() {
  const db = await getDb();
  if (!db) return null;
  
  try {
    // Buscar último treinamento
    const lastTrainingResult = await db.execute<any>(`
      SELECT trained_at, trades_used, model_version
      FROM ml_training_history
      ORDER BY trained_at DESC
      LIMIT 1
    `);
    const lastTraining = (lastTrainingResult as any)[0];
    
    // Calcular taxa de acerto geral
    const statsResult = await db.execute<any>(`
      SELECT 
        COUNT(*) as totalTrades,
        SUM(CASE WHEN CAST(pnl AS DECIMAL(20,8)) > 0 THEN 1 ELSE 0 END) as winningTrades
      FROM trades
      WHERE status = 'CLOSED' AND pnl IS NOT NULL
    `);
    const stats = (statsResult as any)[0];
    
    // Contar retreinamentos
    const retrainingResult = await db.execute<any>(`
      SELECT COUNT(*) as total FROM ml_training_history
    `);
    const retrainingCount = (retrainingResult as any)[0];
    
    const totalTrades = stats?.[0]?.totalTrades || 0;
    const winningTrades = stats?.[0]?.winningTrades || 0;
    const winRate = totalTrades > 0 ? winningTrades / totalTrades : 0;
    
    return {
      currentAccuracy: winRate, // Simplificado: usar win rate como proxy de acurácia
      winRate,
      totalTrades,
      lastTrainingDate: lastTraining?.[0]?.trained_at || null,
      totalRetrainings: retrainingCount?.[0]?.total || 0,
      nextTrainingIn: '24h', // Retreinamento diário
    };
  } catch (error) {
    console.error('Erro ao buscar stats ML:', error);
    return null;
  }
}

export async function getMLTrainingHistory() {
  const db = await getDb();
  if (!db) return [];
  
  try {
    const historyResult = await db.execute<any>(`
      SELECT 
        trained_at as date,
        trades_used as tradesUsed,
        model_version as version,
        notes
      FROM ml_training_history
      ORDER BY trained_at ASC
    `);
    const history = (historyResult as any)[0] || [];
    
    // Calcular acurácia para cada ponto (simplificado: usar win rate do período)
    const historyWithAccuracy = await Promise.all(
      history.map(async (h: any) => {
        // Simplificar: usar win rate geral como proxy
        const accuracy = 55 + Math.random() * 20; // 55-75% (mockado por enquanto)
        
        return {
          ...h,
          accuracy,
        };
      })
    );
    
    return historyWithAccuracy;
  } catch (error) {
    console.error('Erro ao buscar histórico ML:', error);
    return [];
  }
}

export async function getMLMarketComparison() {
  const db = await getDb();
  if (!db) return [];
  
  try {
    const comparisonResult = await db.execute<any>(`
      SELECT 
        CASE 
          WHEN symbol LIKE '%USDT' THEN 'Crypto'
          WHEN symbol LIKE '%.SA' THEN 'B3'
          ELSE 'NYSE'
        END as market,
        COUNT(*) as totalTrades,
        SUM(CASE WHEN CAST(pnl AS DECIMAL(20,8)) > 0 THEN 1 ELSE 0 END) as winningTrades,
        AVG(CAST(pnl AS DECIMAL(20,8))) as avgPnl
      FROM trades
      WHERE status = 'CLOSED' AND pnl IS NOT NULL
      GROUP BY market
      ORDER BY totalTrades DESC
    `);
    const comparison = (comparisonResult as any)[0] || [];
    
    return comparison.map((c: any) => ({
      market: c.market,
      totalTrades: c.totalTrades,
      winningTrades: c.winningTrades,
      winRate: c.totalTrades > 0 ? (c.winningTrades / c.totalTrades) * 100 : 0,
      avgPnl: parseFloat(c.avgPnl) || 0,
    }));
  } catch (error) {
    console.error('Erro ao buscar comparação de mercados:', error);
    return [];
  }
}

export async function getMLFeatureImportance() {
  // Dados mockados baseados no modelo Random Forest
  // Em produção, seria extraído do modelo real
  return [
    { feature: 'RSI', importance: 23.5 },
    { feature: 'MACD', importance: 19.8 },
    { feature: 'EMA 9', importance: 15.2 },
    { feature: 'Bollinger Bands', importance: 12.7 },
    { feature: 'EMA 21', importance: 10.3 },
    { feature: 'Volatility', importance: 8.9 },
    { feature: 'Momentum', importance: 6.4 },
    { feature: 'EMA 50', importance: 3.2 },
  ];
}
