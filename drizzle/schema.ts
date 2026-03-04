import { int, mysqlEnum, mysqlTable, text, timestamp, varchar } from "drizzle-orm/mysql-core";

/**
 * Core user table backing auth flow.
 * Extend this file with additional tables as your product grows.
 * Columns use camelCase to match both database fields and generated types.
 */
export const users = mysqlTable("users", {
  /**
   * Surrogate primary key. Auto-incremented numeric value managed by the database.
   * Use this for relations between tables.
   */
  id: int("id").autoincrement().primaryKey(),
  /** Manus OAuth identifier (openId) returned from the OAuth callback. Unique per user. */
  openId: varchar("openId", { length: 64 }).notNull().unique(),
  name: text("name"),
  email: varchar("email", { length: 320 }),
  loginMethod: varchar("loginMethod", { length: 64 }),
  role: mysqlEnum("role", ["user", "admin"]).default("user").notNull(),
  createdAt: timestamp("createdAt").defaultNow().notNull(),
  updatedAt: timestamp("updatedAt").defaultNow().onUpdateNow().notNull(),
  lastSignedIn: timestamp("lastSignedIn").defaultNow().notNull(),
});

export type User = typeof users.$inferSelect;
export type InsertUser = typeof users.$inferInsert;

/**
 * Sofia IA - Trades executados
 * Armazena histórico completo de todas as operações
 */
export const trades = mysqlTable("trades", {
  id: int("id").autoincrement().primaryKey(),
  symbol: varchar("symbol", { length: 20 }).notNull(),
  recommendation: varchar("recommendation", { length: 10 }).notNull(), // BUY, SELL, HOLD
  confidence: int("confidence").notNull(), // 0-100
  entryPrice: varchar("entry_price", { length: 50 }).notNull(),
  exitPrice: varchar("exit_price", { length: 50 }),
  quantity: varchar("quantity", { length: 50 }).notNull(),
  pnl: varchar("pnl", { length: 50 }),
  pnlPercent: varchar("pnl_percent", { length: 50 }),
  status: varchar("status", { length: 20 }).notNull(), // OPEN, CLOSED
  closeReason: varchar("close_reason", { length: 50 }), // TAKE_PROFIT, STOP_LOSS, TIMEOUT, MANUAL
  openedAt: timestamp("opened_at").defaultNow().notNull(),
  closedAt: timestamp("closed_at"),
  duration: int("duration"), // em minutos
  createdAt: timestamp("created_at").defaultNow().notNull(),
});

export type Trade = typeof trades.$inferSelect;
export type InsertTrade = typeof trades.$inferInsert;

/**
 * Sofia IA - Métricas de aprendizado por símbolo
 * Rastreia acurácia e performance de Sofia para cada ativo
 */
export const sofiaMetrics = mysqlTable("sofia_metrics", {
  id: int("id").autoincrement().primaryKey(),
  symbol: varchar("symbol", { length: 20 }).notNull().unique(),
  totalTrades: int("total_trades").default(0).notNull(),
  winningTrades: int("winning_trades").default(0).notNull(),
  losingTrades: int("losing_trades").default(0).notNull(),
  accuracy: int("accuracy").default(0).notNull(), // 0-100
  totalPnl: varchar("total_pnl", { length: 50 }).default("0").notNull(),
  avgConfidence: int("avg_confidence").default(0).notNull(),
  lastTradeAt: timestamp("last_trade_at"),
  updatedAt: timestamp("updated_at").defaultNow().onUpdateNow().notNull(),
});

export type SofiaMetric = typeof sofiaMetrics.$inferSelect;
export type InsertSofiaMetric = typeof sofiaMetrics.$inferInsert;

/**
 * Sofia IA - Análises e recomendações
 * Armazena cada análise gerada por Sofia
 */
export const sofiaAnalyses = mysqlTable("sofia_analyses", {
  id: int("id").autoincrement().primaryKey(),
  symbol: varchar("symbol", { length: 20 }).notNull(),
  recommendation: varchar("recommendation", { length: 10 }).notNull(),
  confidence: int("confidence").notNull(),
  reasoning: text("reasoning").notNull(), // JSON array de motivos
  marketData: text("market_data").notNull(), // JSON com dados de mercado
  executed: int("executed").default(0).notNull(), // 0 = não executado, 1 = executado
  tradeId: int("trade_id"),
  createdAt: timestamp("created_at").defaultNow().notNull(),
});

export type SofiaAnalysis = typeof sofiaAnalyses.$inferSelect;
export type InsertSofiaAnalysis = typeof sofiaAnalyses.$inferInsert;

/**
 * Notificações enviadas
 * Histórico de todas as notificações do sistema
 */
export const notifications = mysqlTable("notifications", {
  id: int("id").autoincrement().primaryKey(),
  type: varchar("type", { length: 50 }).notNull(),
  title: varchar("title", { length: 255 }).notNull(),
  message: text("message").notNull(),
  channels: text("channels").notNull(), // JSON array de canais (email, telegram, etc)
  sentAt: timestamp("sent_at").defaultNow().notNull(),
});

export type Notification = typeof notifications.$inferSelect;
export type InsertNotification = typeof notifications.$inferInsert;

/**
 * ML Models - Versionamento de modelos de Machine Learning
 * Armazena cada versão treinada do modelo com métricas de performance
 */
export const mlModels = mysqlTable("ml_models", {
  id: int("id").autoincrement().primaryKey(),
  version: varchar("version", { length: 50 }).notNull().unique(), // Ex: v1.0.0, v1.1.0
  modelType: varchar("model_type", { length: 50 }).notNull(), // RandomForest, XGBoost, etc
  accuracy: varchar("accuracy", { length: 20 }).notNull(), // 0.0-1.0
  precision: varchar("precision", { length: 20 }).notNull(),
  recall: varchar("recall", { length: 20 }).notNull(),
  f1Score: varchar("f1_score", { length: 20 }).notNull(),
  trainingSize: int("training_size").notNull(), // Número de samples
  s3Path: varchar("s3_path", { length: 500 }).notNull(), // URL do modelo no S3
  isActive: int("is_active").default(0).notNull(), // 0 = inativo, 1 = ativo
  notes: text("notes"), // Notas sobre o treinamento
  trainedAt: timestamp("trained_at").defaultNow().notNull(),
  createdAt: timestamp("created_at").defaultNow().notNull(),
});

export type MLModel = typeof mlModels.$inferSelect;
export type InsertMLModel = typeof mlModels.$inferInsert;