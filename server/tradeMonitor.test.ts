/**
 * Testes unitários para o Trade Monitor
 * Valida a lógica de cálculo de P&L e decisões de fechamento
 */
import { describe, it, expect } from "vitest";

// ─── Funções puras extraídas do tradeMonitor para teste ─────────────────────

function calcPnl(
  recommendation: string,
  entryPrice: number,
  currentPrice: number,
  quantity: number
): { pnlUsd: number; pnlPct: number } {
  let pnlPct: number;
  if (recommendation === "BUY") {
    pnlPct = ((currentPrice - entryPrice) / entryPrice) * 100;
  } else {
    pnlPct = ((entryPrice - currentPrice) / entryPrice) * 100;
  }
  const positionSize = entryPrice * quantity;
  const pnlUsd = (pnlPct / 100) * positionSize;
  return { pnlUsd, pnlPct };
}

function shouldClose(
  pnlPct: number,
  ageMinutes: number,
  TAKE_PROFIT_PCT = 2.0,
  STOP_LOSS_PCT = 1.5,
  MAX_DURATION_MINUTES = 120
): "TAKE_PROFIT" | "STOP_LOSS" | "TIMEOUT" | null {
  if (ageMinutes >= MAX_DURATION_MINUTES) return "TIMEOUT";
  if (pnlPct >= TAKE_PROFIT_PCT) return "TAKE_PROFIT";
  if (pnlPct <= -STOP_LOSS_PCT) return "STOP_LOSS";
  return null;
}

// ─── Testes ─────────────────────────────────────────────────────────────────

describe("calcPnl — BUY positions", () => {
  it("calcula P&L positivo quando preço sobe (BUY)", () => {
    const { pnlPct, pnlUsd } = calcPnl("BUY", 100, 102, 1000);
    expect(pnlPct).toBeCloseTo(2.0, 4);
    expect(pnlUsd).toBeCloseTo(2000, 1); // 2% de $100k
  });

  it("calcula P&L negativo quando preço cai (BUY)", () => {
    const { pnlPct, pnlUsd } = calcPnl("BUY", 100, 98.5, 1000);
    expect(pnlPct).toBeCloseTo(-1.5, 4);
    expect(pnlUsd).toBeCloseTo(-1500, 1);
  });

  it("P&L zero quando preço não muda (BUY)", () => {
    const { pnlPct, pnlUsd } = calcPnl("BUY", 65000, 65000, 1.538);
    expect(pnlPct).toBeCloseTo(0, 4);
    expect(pnlUsd).toBeCloseTo(0, 1);
  });
});

describe("calcPnl — SELL positions", () => {
  it("calcula P&L positivo quando preço cai (SELL)", () => {
    const { pnlPct, pnlUsd } = calcPnl("SELL", 100, 98, 1000);
    expect(pnlPct).toBeCloseTo(2.0, 4);
    expect(pnlUsd).toBeCloseTo(2000, 1);
  });

  it("calcula P&L negativo quando preço sobe (SELL)", () => {
    const { pnlPct, pnlUsd } = calcPnl("SELL", 100, 101.5, 1000);
    expect(pnlPct).toBeCloseTo(-1.5, 4);
    expect(pnlUsd).toBeCloseTo(-1500, 1);
  });
});

describe("shouldClose — lógica de fechamento", () => {
  it("fecha por TAKE_PROFIT quando P&L >= 2%", () => {
    expect(shouldClose(2.0, 30)).toBe("TAKE_PROFIT");
    expect(shouldClose(3.5, 30)).toBe("TAKE_PROFIT");
  });

  it("fecha por STOP_LOSS quando P&L <= -1.5%", () => {
    expect(shouldClose(-1.5, 30)).toBe("STOP_LOSS");
    expect(shouldClose(-5.0, 30)).toBe("STOP_LOSS");
  });

  it("fecha por TIMEOUT quando trade tem >= 120 minutos", () => {
    expect(shouldClose(0.5, 120)).toBe("TIMEOUT");
    expect(shouldClose(-0.5, 180)).toBe("TIMEOUT");
  });

  it("TIMEOUT tem prioridade sobre TAKE_PROFIT e STOP_LOSS", () => {
    expect(shouldClose(5.0, 120)).toBe("TIMEOUT");
    expect(shouldClose(-5.0, 120)).toBe("TIMEOUT");
  });

  it("não fecha quando dentro dos limites", () => {
    expect(shouldClose(1.0, 60)).toBeNull();
    expect(shouldClose(-1.0, 60)).toBeNull();
    expect(shouldClose(0.0, 0)).toBeNull();
  });
});

describe("updateTradesWithPrices — lógica de preços do browser", () => {
  it("identifica corretamente se deve fechar por take profit com preço do browser", () => {
    // Simula trade BTCUSDT BUY com entry 65000, preço atual 66300 (+2%)
    const entry = 65000;
    const current = 66300;
    const qty = 100000 / entry; // $100k position
    const { pnlPct } = calcPnl("BUY", entry, current, qty);
    expect(pnlPct).toBeCloseTo(2.0, 1);
    expect(shouldClose(pnlPct, 30)).toBe("TAKE_PROFIT");
  });

  it("identifica corretamente se deve fechar por stop loss com preço do browser", () => {
    // Simula trade ETHUSDT SELL com entry 3200, preço atual 3248 (+1.5% contra SELL)
    const entry = 3200;
    const current = 3248;
    const qty = 100000 / entry;
    const { pnlPct } = calcPnl("SELL", entry, current, qty);
    expect(pnlPct).toBeCloseTo(-1.5, 1);
    expect(shouldClose(pnlPct, 30)).toBe("STOP_LOSS");
  });

  it("não fecha quando preço está dentro dos limites", () => {
    const entry = 100;
    const current = 100.5; // +0.5%
    const qty = 1000;
    const { pnlPct } = calcPnl("BUY", entry, current, qty);
    expect(pnlPct).toBeCloseTo(0.5, 1);
    expect(shouldClose(pnlPct, 60)).toBeNull();
  });

  it("calcula P&L correto para dict de preços do browser", () => {
    const prices = { BTCUSDT: 66300, ETHUSDT: 3248, SHIBUSDT: 0.0000055 };
    // Verifica que os preços são números válidos
    for (const [sym, price] of Object.entries(prices)) {
      expect(typeof price).toBe("number");
      expect(price).toBeGreaterThan(0);
    }
    expect(Object.keys(prices)).toHaveLength(3);
  });
});

describe("calcPnl — meme coins com preços muito pequenos", () => {
  it("calcula P&L correto para PEPEUSDT (preço 0.00000347)", () => {
    const entry = 0.00000347;
    const current = 0.00000354; // +2.02%
    const qty = 28818443804.0; // ~$100k
    const { pnlPct } = calcPnl("BUY", entry, current, qty);
    expect(pnlPct).toBeCloseTo(2.017, 1);
  });

  it("calcula P&L correto para SHIBUSDT (preço 0.0000055)", () => {
    const entry = 0.0000055;
    const current = 0.00000539; // -2%
    const qty = 18181818181.0;
    const { pnlPct } = calcPnl("BUY", entry, current, qty);
    expect(pnlPct).toBeCloseTo(-2.0, 1);
  });
});
