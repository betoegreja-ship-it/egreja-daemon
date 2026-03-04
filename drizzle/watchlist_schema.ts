import { mysqlTable, int, varchar, decimal, boolean, timestamp, mysqlEnum } from 'drizzle-orm/mysql-core';

export const watchlist = mysqlTable('watchlist', {
  id: int('id').primaryKey().autoincrement(),
  userId: varchar('user_id', { length: 255 }).notNull(),
  symbol: varchar('symbol', { length: 50 }).notNull(),
  assetType: mysqlEnum('asset_type', ['crypto', 'stock']).notNull(),
  addedAt: timestamp('added_at').defaultNow().notNull(),
});

export const priceAlerts = mysqlTable('price_alerts', {
  id: int('id').primaryKey().autoincrement(),
  userId: varchar('user_id', { length: 255 }).notNull(),
  symbol: varchar('symbol', { length: 50 }).notNull(),
  assetType: mysqlEnum('asset_type', ['crypto', 'stock']).notNull(),
  targetPrice: decimal('target_price', { precision: 18, scale: 8 }).notNull(),
  condition: mysqlEnum('condition', ['above', 'below']).notNull(),
  isActive: boolean('is_active').default(true).notNull(),
  triggered: boolean('triggered').default(false).notNull(),
  triggeredAt: timestamp('triggered_at'),
  createdAt: timestamp('created_at').defaultNow().notNull(),
});

export const alertHistory = mysqlTable('alert_history', {
  id: int('id').primaryKey().autoincrement(),
  alertId: int('alert_id').notNull(),
  symbol: varchar('symbol', { length: 50 }).notNull(),
  targetPrice: decimal('target_price', { precision: 18, scale: 8 }).notNull(),
  actualPrice: decimal('actual_price', { precision: 18, scale: 8 }).notNull(),
  condition: mysqlEnum('condition', ['above', 'below']).notNull(),
  triggeredAt: timestamp('triggered_at').defaultNow().notNull(),
});
