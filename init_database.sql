-- Egreja Investment AI - Database Schema
-- Criado: 2026-03-04

-- Tabela de Sinais de Mercado (análises de cada ativo)
CREATE TABLE IF NOT EXISTS market_signals (
    id INT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    market_type VARCHAR(10) NOT NULL, -- 'B3', 'NYSE', 'CRYPTO'
    price DECIMAL(20, 8) NOT NULL,
    score INT NOT NULL, -- 0-100
    signal VARCHAR(50) NOT NULL, -- '🟢 COMPRA FORTE', '🔴 VENDA', etc
    rsi DECIMAL(10, 2),
    ema9 DECIMAL(20, 8),
    ema21 DECIMAL(20, 8),
    ema50 DECIMAL(20, 8),
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_symbol (symbol),
    INDEX idx_market_type (market_type),
    INDEX idx_timestamp (timestamp),
    INDEX idx_score (score)
);

-- Tabela de Métricas de Portfólio (agregadas)
CREATE TABLE IF NOT EXISTS portfolio_metrics (
    id INT AUTO_INCREMENT PRIMARY KEY,
    total_portfolio_value DECIMAL(20, 2) NOT NULL, -- $1.035M
    total_pnl DECIMAL(20, 2) NOT NULL, -- +$34.775
    pnl_percentage DECIMAL(10, 2) NOT NULL, -- +3.48%
    win_rate DECIMAL(10, 2) NOT NULL, -- 47.7%
    total_trades INT NOT NULL,
    open_positions INT NOT NULL,
    capital_deployed DECIMAL(20, 2) NOT NULL,
    last_analysis_time DATETIME,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_created_at (created_at)
);

-- Tabela de Trades Executados
CREATE TABLE IF NOT EXISTS trades (
    id INT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    direction VARCHAR(10) NOT NULL, -- 'BUY', 'SELL'
    entry_price DECIMAL(20, 8) NOT NULL,
    exit_price DECIMAL(20, 8),
    size DECIMAL(20, 8) NOT NULL,
    pnl DECIMAL(20, 2),
    pnl_percentage DECIMAL(10, 2),
    status VARCHAR(20) NOT NULL, -- 'OPEN', 'CLOSED'
    entry_time DATETIME NOT NULL,
    exit_time DATETIME,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_symbol (symbol),
    INDEX idx_status (status),
    INDEX idx_entry_time (entry_time)
);

-- Tabela de Log de Análises
CREATE TABLE IF NOT EXISTS analysis_logs (
    id INT AUTO_INCREMENT PRIMARY KEY,
    total_analyzed INT NOT NULL,
    buy_signals INT NOT NULL,
    sell_signals INT NOT NULL,
    hold_signals INT NOT NULL,
    top_buy VARCHAR(100), -- ex: "AAPL, MSFT, GOOGL"
    top_sell VARCHAR(100),
    analysis_duration_ms INT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_created_at (created_at)
);

-- Índices compostos para queries comuns
CREATE INDEX idx_signal_latest ON market_signals (symbol, created_at DESC);
CREATE INDEX idx_metric_latest ON portfolio_metrics (created_at DESC);
