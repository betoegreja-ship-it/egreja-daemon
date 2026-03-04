import { COOKIE_NAME } from "@shared/const";
import { getSessionCookieOptions } from "./_core/cookies";
import { systemRouter } from "./_core/systemRouter";
import { publicProcedure, protectedProcedure, router } from "./_core/trpc";
import * as sofiaDb from "./sofia_db";
import * as stocks from "./stocks";
import { z } from "zod";

export const appRouter = router({
    // if you need to use socket.io, read and register route in server/_core/index.ts, all api should start with '/api/' so that the gateway can route correctly
  system: systemRouter,
  auth: router({
    me: publicProcedure.query(opts => opts.ctx.user),
    logout: publicProcedure.mutation(({ ctx }) => {
      const cookieOptions = getSessionCookieOptions(ctx.req);
      ctx.res.clearCookie(COOKIE_NAME, { ...cookieOptions, maxAge: -1 });
      return {
        success: true,
      } as const;
    }),
  }),

  sofia: router({
    // Trades
    getTrades: publicProcedure
      .input(z.object({ limit: z.number().optional() }).optional())
      .query(async ({ input }) => {
        return await sofiaDb.getTrades(input?.limit);
      }),
    
    getTradesBySymbol: publicProcedure
      .input(z.object({ symbol: z.string(), limit: z.number().optional() }))
      .query(async ({ input }) => {
        return await sofiaDb.getTradesBySymbol(input.symbol, input.limit);
      }),
    
    getOpenTrades: publicProcedure.query(async () => {
      return await sofiaDb.getOpenTrades();
    }),
    
    // Sofia Metrics
    getSofiaMetric: publicProcedure
      .input(z.object({ symbol: z.string() }))
      .query(async ({ input }) => {
        return await sofiaDb.getSofiaMetric(input.symbol);
      }),
    
    getAllSofiaMetrics: publicProcedure.query(async () => {
      return await sofiaDb.getAllSofiaMetrics();
    }),
    
    // Sofia Analyses
    getSofiaAnalyses: publicProcedure
      .input(z.object({ limit: z.number().optional() }).optional())
      .query(async ({ input }) => {
        return await sofiaDb.getSofiaAnalyses(input?.limit);
      }),
    
    getSofiaAnalysesBySymbol: publicProcedure
      .input(z.object({ symbol: z.string(), limit: z.number().optional() }))
      .query(async ({ input }) => {
        return await sofiaDb.getSofiaAnalysesBySymbol(input.symbol, input.limit);
      }),
    
    // Notifications
    getNotifications: publicProcedure
      .input(z.object({ limit: z.number().optional() }).optional())
      .query(async ({ input }) => {
        return await sofiaDb.getNotifications(input?.limit);
      }),
    
    // Analytics
    getDailyStats: publicProcedure
      .input(z.object({ date: z.date().optional() }).optional())
      .query(async ({ input }) => {
        const date = input?.date || new Date();
        return await sofiaDb.getDailyStats(date);
      }),
    
    getMonthlyStats: publicProcedure
      .input(z.object({ year: z.number(), month: z.number() }).optional())
      .query(async ({ input }) => {
        const now = new Date();
        const year = input?.year || now.getFullYear();
        const month = input?.month || now.getMonth() + 1;
        return await sofiaDb.getMonthlyStats(year, month);
      }),
    
    getTotalMonthlyPnL: publicProcedure.query(async () => {
      return await sofiaDb.getTotalMonthlyPnL();
    }),
    
    getTotalYearlyPnL: publicProcedure.query(async () => {
      return await sofiaDb.getTotalYearlyPnL();
    }),
    
    getGlobalStats: publicProcedure.query(async () => {
      return await sofiaDb.getGlobalStats();
    }),
    
    getYearlyStats: publicProcedure
      .input(z.object({ year: z.number() }).optional())
      .query(async ({ input }) => {
        const year = input?.year || new Date().getFullYear();
        return await sofiaDb.getYearlyStats(year);
      }),
    
    getHistoricalPnL: publicProcedure
      .input(z.object({ days: z.number().optional() }).optional())
      .query(async ({ input }) => {
        const days = input?.days || 30;
        return await sofiaDb.getHistoricalPnL(days);
      }),
    
    getClosedTrades: publicProcedure
      .input(z.object({
        symbol: z.string().optional(),
        startDate: z.string().optional(),
        endDate: z.string().optional(),
        profitOnly: z.boolean().optional(),
        lossOnly: z.boolean().optional(),
      }).optional())
      .query(async ({ input }) => {
        return await sofiaDb.getClosedTrades(input || {});
      }),
  }),

  ml: router({
    getPerformanceStats: publicProcedure.query(async () => {
      return await sofiaDb.getMLPerformanceStats();
    }),
    
    getTrainingHistory: publicProcedure.query(async () => {
      return await sofiaDb.getMLTrainingHistory();
    }),
    
    getMarketComparison: publicProcedure.query(async () => {
      return await sofiaDb.getMLMarketComparison();
    }),
    
    getFeatureImportance: publicProcedure.query(async () => {
      return await sofiaDb.getMLFeatureImportance();
    }),
  }),

  stocks: router({
    getAllStocks: stocks.getAllStocks,
    getMarketStatus: stocks.getMarketStatus,
    getStockQuote: stocks.getStockQuote,
  }),

  // Endpoint para o browser enviar preços ao servidor (contorna bloqueio geográfico da Binance)
  // ⚠️  SEGURANÇA: Requer token secreto + validação de range de preços
  prices: router({
    updateFromBrowser: publicProcedure
      .input(z.object({
        token: z.string(), // Deve ser process.env.INTERNAL_PRICE_TOKEN
        prices: z.record(
          z.string(), 
          z.number()
            .positive("Preço deve ser positivo")
            .max(10_000_000, "Preço não pode exceder $10M (proteção anti-manipulação)")
        ),
      }))
      .mutation(async ({ input }) => {
        // 🔐 Validar token secreto
        const expectedToken = process.env.INTERNAL_PRICE_TOKEN;
        if (!expectedToken || input.token !== expectedToken) {
          throw new Error('UNAUTHORIZED: Token inválido ou não configurado');
        }
        
        // 🛡️ Validar que não são preços ridículos
        for (const [symbol, price] of Object.entries(input.prices)) {
          // Preços crypto normalmente entre $0.00001 e $100.000
          // Preços ações entre $1 e $1.000
          // Se for absurdo, ignorar (possível manipulação)
          if (price < 0.00001 || price > 10_000_000) {
            console.warn(`[SECURITY] Preço suspeito ignorado: ${symbol} = ${price}`);
            delete input.prices[symbol]; // Remover do processamento
          }
        }
        
        // Importar no topo (não dynamic import)
        const { updateTradesWithPrices } = await import('./tradeMonitor');
        await updateTradesWithPrices(input.prices);
        return { ok: true };
      }),
  }),
});

export type AppRouter = typeof appRouter;
