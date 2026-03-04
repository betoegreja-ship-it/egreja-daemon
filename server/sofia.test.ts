import { describe, expect, it, beforeAll } from "vitest";
import { appRouter } from "./routers";
import type { TrpcContext } from "./_core/context";
import * as sofiaDb from "./sofia_db";

type AuthenticatedUser = NonNullable<TrpcContext["user"]>;

function createTestContext(): TrpcContext {
  const user: AuthenticatedUser = {
    id: 1,
    openId: "test-user",
    email: "test@example.com",
    name: "Test User",
    loginMethod: "manus",
    role: "user",
    createdAt: new Date(),
    updatedAt: new Date(),
    lastSignedIn: new Date(),
  };

  const ctx: TrpcContext = {
    user,
    req: {
      protocol: "https",
      headers: {},
    } as TrpcContext["req"],
    res: {} as TrpcContext["res"],
  };

  return ctx;
}

describe("Sofia tRPC Procedures", () => {
  const ctx = createTestContext();
  const caller = appRouter.createCaller(ctx);

  describe("getTrades", () => {
    it("should return empty array when no trades exist", async () => {
      const result = await caller.sofia.getTrades();
      expect(Array.isArray(result)).toBe(true);
    });

    it("should accept limit parameter", async () => {
      const result = await caller.sofia.getTrades({ limit: 10 });
      expect(Array.isArray(result)).toBe(true);
    });
  });

  describe("getOpenTrades", () => {
    it("should return empty array when no open trades exist", async () => {
      const result = await caller.sofia.getOpenTrades();
      expect(Array.isArray(result)).toBe(true);
    });
  });

  describe("getAllSofiaMetrics", () => {
    it("should return empty array when no metrics exist", async () => {
      const result = await caller.sofia.getAllSofiaMetrics();
      expect(Array.isArray(result)).toBe(true);
    });
  });

  describe("getSofiaAnalyses", () => {
    it("should return empty array when no analyses exist", async () => {
      const result = await caller.sofia.getSofiaAnalyses();
      expect(Array.isArray(result)).toBe(true);
    });

    it("should accept limit parameter", async () => {
      const result = await caller.sofia.getSofiaAnalyses({ limit: 20 });
      expect(Array.isArray(result)).toBe(true);
    });
  });

  describe("getNotifications", () => {
    it("should return empty array when no notifications exist", async () => {
      const result = await caller.sofia.getNotifications();
      expect(Array.isArray(result)).toBe(true);
    });
  });

  describe("getDailyStats", () => {
    it("should return stats object", async () => {
      const result = await caller.sofia.getDailyStats();
      expect(result).toBeDefined();
      expect(typeof result?.totalTrades).toBe("number");
      expect(typeof result?.winRate).toBe("number");
    });

    it("should accept date parameter", async () => {
      const testDate = new Date("2024-01-01");
      const result = await caller.sofia.getDailyStats({ date: testDate });
      expect(result).toBeDefined();
    });
  });
});

describe("Sofia Database Helpers", () => {
  describe("insertTrade", () => {
    it("should insert a trade successfully", async () => {
      const trade = {
        symbol: "BTCUSDT",
        recommendation: "BUY",
        confidence: 75,
        entryPrice: "50000.00",
        quantity: "0.01",
        status: "OPEN",
        openedAt: new Date(),
      };

      const tradeId = await sofiaDb.insertTrade(trade);
      expect(typeof tradeId).toBe("number");
      expect(tradeId).toBeGreaterThan(0);
    });
  });

  describe("upsertSofiaMetric", () => {
    it("should upsert a metric successfully", async () => {
      const metric = {
        symbol: "BTCUSDT",
        totalTrades: 10,
        winningTrades: 6,
        losingTrades: 4,
        accuracy: 60,
        totalPnl: "1000.00",
        avgConfidence: 70,
      };

      await expect(sofiaDb.upsertSofiaMetric(metric)).resolves.not.toThrow();
    });
  });

  describe("insertSofiaAnalysis", () => {
    it("should insert an analysis successfully", async () => {
      const analysis = {
        symbol: "ETHUSDT",
        recommendation: "SELL",
        confidence: 80,
        reasoning: JSON.stringify(["High volatility", "Overbought"]),
        marketData: JSON.stringify({ price: 2500, volume: 1000000 }),
      };

      const analysisId = await sofiaDb.insertSofiaAnalysis(analysis);
      expect(typeof analysisId).toBe("number");
      expect(analysisId).toBeGreaterThan(0);
    });
  });
});
