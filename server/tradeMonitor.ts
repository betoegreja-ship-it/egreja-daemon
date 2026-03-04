/**
 * Egreja Investment AI — Trade Monitor (Node.js)
 *
 * Roda como setInterval dentro do servidor Express (sempre ativo).
 * Monitora trades abertas a cada 2 minutos e fecha por TP/SL/TIMEOUT.
 *
 * REGRA CRÍTICA: Nunca fechar uma trade sem um preço válido.
 * Se não conseguir preço, aguarda o próximo ciclo.
 */

import { eq } from "drizzle-orm";
import { getDb, resetDb } from "./db";
import { trades } from "../drizzle/schema";

// ─── Configurações de risco ─────────────────────────────────────────────────
const TAKE_PROFIT_PCT      = 2.0;   // +2% fecha com lucro
const STOP_LOSS_PCT        = 1.5;   // -1.5% fecha com prejuízo
const MAX_DURATION_MINUTES = 120;   // 2h máximo por trade
const MONITOR_INTERVAL_MS  = 2 * 60 * 1000; // ciclo a cada 2 min

// ─── Cache de preços ─────────────────────────────────────────────────────────
// TTL longo (10 min) para que preços do browser sobrevivam entre ciclos
const priceCache = new Map<string, { price: number; ts: number }>();
const PRICE_CACHE_TTL_MS = 10 * 60 * 1000; // 10 minutos

function getCachedPrice(symbol: string): number | null {
  const cached = priceCache.get(symbol);
  if (cached && Date.now() - cached.ts < PRICE_CACHE_TTL_MS) {
    return cached.price;
  }
  return null;
}

function setCachedPrice(symbol: string, price: number) {
  if (price > 0) priceCache.set(symbol, { price, ts: Date.now() });
}

// ─── Buscar preço: cache → Binance → OKX → CoinGecko ────────────────────────
async function getPrice(symbol: string): Promise<number | null> {
  // 1. Cache (preços enviados pelo browser ou buscados anteriormente)
  const cached = getCachedPrice(symbol);
  if (cached) return cached;

  // 2. Binance (pode estar bloqueada por geo-block HTTP 451)
  try {
    const res = await fetch(
      `https://api.binance.com/api/v3/ticker/price?symbol=${symbol}`,
      { signal: AbortSignal.timeout(5000) }
    );
    if (res.ok) {
      const data = await res.json() as { price: string };
      const price = parseFloat(data.price);
      if (!isNaN(price) && price > 0) {
        setCachedPrice(symbol, price);
        return price;
      }
    }
  } catch { /* fallback */ }

  // 3. OKX
  try {
    const instId = symbol.replace("USDT", "-USDT");
    const res = await fetch(
      `https://www.okx.com/api/v5/market/ticker?instId=${instId}`,
      { signal: AbortSignal.timeout(5000) }
    );
    if (res.ok) {
      const data = await res.json() as { data: Array<{ last: string }> };
      const price = parseFloat(data.data?.[0]?.last ?? "0");
      if (!isNaN(price) && price > 0) {
        setCachedPrice(symbol, price);
        return price;
      }
    }
  } catch { /* fallback */ }

  // 4. CoinGecko
  const cgMap: Record<string, string> = {
    BTCUSDT: "bitcoin", ETHUSDT: "ethereum", BNBUSDT: "binancecoin",
    SOLUSDT: "solana", ADAUSDT: "cardano", XRPUSDT: "ripple",
    DOGEUSDT: "dogecoin", SHIBUSDT: "shiba-inu", PEPEUSDT: "pepe",
    LTCUSDT: "litecoin", DOTUSDT: "polkadot", LINKUSDT: "chainlink",
    UNIUSDT: "uniswap", AVAXUSDT: "avalanche-2", MATICUSDT: "matic-network",
    NEARUSDT: "near", AAVEUSDT: "aave", CRVUSDT: "curve-dao-token",
    FILUSDT: "filecoin", VETUSDT: "vechain", ALGOUSDT: "algorand",
    SUSHIUSDT: "sushi", BONKUSDT: "bonk", FLOKIUSDT: "floki",
  };
  const cgId = cgMap[symbol];
  if (cgId) {
    try {
      const res = await fetch(
        `https://api.coingecko.com/api/v3/simple/price?ids=${cgId}&vs_currencies=usd`,
        { signal: AbortSignal.timeout(8000) }
      );
      if (res.ok) {
        const data = await res.json() as Record<string, { usd: number }>;
        const price = data[cgId]?.usd;
        if (price && price > 0) {
          setCachedPrice(symbol, price);
          return price;
        }
      }
    } catch { /* sem preço */ }
  }

  // Nenhuma fonte disponível — retornar null (NÃO fechar a trade)
  return null;
}

// ─── Calcular P&L ────────────────────────────────────────────────────────────
// ⚠️ Mantemos apenas para cálculo de dashboard (o daemon Python é responsável por fechamentos)
function calcPnl(
  recommendation: string,
  entryPrice: number,
  currentPrice: number,
  quantity: number
): { pnlUsd: number; pnlPct: number } {
  const pnlPct =
    recommendation === "BUY"
      ? ((currentPrice - entryPrice) / entryPrice) * 100
      : ((entryPrice - currentPrice) / entryPrice) * 100;
  const pnlUsd = (pnlPct / 100) * (entryPrice * quantity);
  return { pnlUsd, pnlPct };
}

// ─── Ciclo principal ─────────────────────────────────────────────────────────
// ⚠️ IMPORTANTE: Esse monitor é APENAS para atualizar P&L em tempo real
// O fechamento de trades (TP/SL/TIMEOUT) é responsabilidade do daemon Python
// Isso centraliza a lógica de trading em um único processo e evita race conditions
async function monitorCycle() {
  try {
    const db = await getDb();
    if (!db) return;

    const openTrades = await db.select().from(trades).where(eq(trades.status, "OPEN"));
    if (openTrades.length === 0) return;

    console.log(`[TradeMonitor] 📊 Atualizando P&L de ${openTrades.length} trades abertas...`);

    for (const trade of openTrades) {
      const entryPrice = parseFloat(trade.entryPrice ?? "0");
      const quantity   = parseFloat(trade.quantity ?? "0");
      if (isNaN(entryPrice) || isNaN(quantity) || entryPrice <= 0) continue;

      // Buscar preço atual (cache → Binance → OKX → CoinGecko)
      const currentPrice = await getPrice(trade.symbol);

      // ⚠️ REGRA: Se não tiver preço, pular (não atualizar com dados inválidos)
      if (!currentPrice) {
        console.debug(`[TradeMonitor] ⚠️ Sem preço para ${trade.symbol} — pulando atualização`);
        continue;
      }

      const { pnlUsd, pnlPct } = calcPnl(trade.recommendation, entryPrice, currentPrice, quantity);

      // ✅ ÚNICO JOB: Atualizar P&L para o dashboard mostrar valores atualizados
      // Fechamento é feito pelo daemon Python (source of truth)
      await db.update(trades).set({
        pnl: pnlUsd.toFixed(2),
        pnlPercent: pnlPct.toFixed(4),
      }).where(eq(trades.id, trade.id));

      // Log apenas para trades com movimentação significativa
      if (Math.abs(pnlPct) >= 1) {
        const sign = pnlUsd >= 0 ? "+" : "";
        console.log(`[TradeMonitor] 📈 ${trade.symbol}: ${sign}${pnlPct.toFixed(2)}% (${sign}$${pnlUsd.toFixed(2)})`);
      }
    }
  } catch (err: any) {
    // Reconectar ao banco após hibernação do sandbox
    if (
      err?.cause?.code === "ECONNRESET" ||
      err?.message?.includes("ECONNRESET") ||
      err?.message?.includes("Failed query")
    ) {
      console.warn("[TradeMonitor] 🔄 ECONNRESET detectado — reconectando ao banco...");
      resetDb();
    } else {
      console.error("[TradeMonitor] ❌ Erro no ciclo:", err?.message ?? err);
    }
  }
}

// ─── Atualizar cache com preços do browser ───────────────────────────────────
// Chamado pelo frontend a cada 10s via trpc.prices.updateFromBrowser
// Contorna o bloqueio geográfico da Binance no servidor
// ⚠️ APENAS atualiza P&L para o dashboard — fechamentos são feitos pelo daemon Python
export async function updateTradesWithPrices(prices: Record<string, number>) {
  try {
    // Atualizar cache de preços (TTL 10 min)
    for (const [symbol, price] of Object.entries(prices)) {
      setCachedPrice(symbol, price);
    }

    const db = await getDb();
    if (!db) return;

    const openTrades = await db.select().from(trades).where(eq(trades.status, "OPEN"));
    if (openTrades.length === 0) return;

    for (const trade of openTrades) {
      const currentPrice = prices[trade.symbol];
      if (!currentPrice || currentPrice <= 0) continue;

      const entryPrice = parseFloat(trade.entryPrice ?? "0");
      const quantity   = parseFloat(trade.quantity ?? "0");
      if (isNaN(entryPrice) || isNaN(quantity) || entryPrice <= 0) continue;

      const { pnlUsd, pnlPct } = calcPnl(trade.recommendation, entryPrice, currentPrice, quantity);

      // ✅ Atualizar P&L para o dashboard (sem fechar)
      // Fechamento é responsabilidade exclusiva do daemon Python
      await db.update(trades).set({
        pnl: pnlUsd.toFixed(2),
        pnlPercent: pnlPct.toFixed(4),
      }).where(eq(trades.id, trade.id));
    }
  } catch (err: any) {
    if (err?.cause?.code === "ECONNRESET" || err?.message?.includes("ECONNRESET")) {
      resetDb();
    }
    console.error("[TradeMonitor] Erro ao processar preços do browser:", err?.message ?? err);
  }
}

// ─── Iniciar monitor ─────────────────────────────────────────────────────────
export function startTradeMonitor() {
  console.log("[TradeMonitor] 🚀 Egreja Investment AI — Trade Monitor iniciado");
  console.log(`[TradeMonitor]    TP: +${TAKE_PROFIT_PCT}% | SL: -${STOP_LOSS_PCT}% | Timeout: ${MAX_DURATION_MINUTES}min`);
  console.log(`[TradeMonitor]    Ciclo: ${MONITOR_INTERVAL_MS / 60000} min | Cache TTL: ${PRICE_CACHE_TTL_MS / 60000} min`);
  console.log(`[TradeMonitor]    ⚠️  REGRA: Nunca fechar trade sem preço válido`);

  monitorCycle().catch(console.error);

  const interval = setInterval(() => {
    monitorCycle().catch(console.error);
  }, MONITOR_INTERVAL_MS);

  interval.unref();
  return interval;
}
