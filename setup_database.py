#!/usr/bin/env python3
"""
Setup de banco de dados - Cria tabelas automaticamente
Roda UMA VEZ na primeira execução
"""

import mysql.connector
import os
from dotenv import load_dotenv
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

# SQL para criar tabelas
SQL_INIT = """
-- Tabela de Sinais de Mercado
CREATE TABLE IF NOT EXISTS market_signals (
    id INT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    market_type VARCHAR(10) NOT NULL,
    price DECIMAL(20, 8) NOT NULL,
    score INT NOT NULL,
    signal VARCHAR(50) NOT NULL,
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
    INDEX idx_score (score),
    INDEX idx_signal_latest (symbol, created_at DESC)
);

-- Tabela de Métricas de Portfólio
CREATE TABLE IF NOT EXISTS portfolio_metrics (
    id INT AUTO_INCREMENT PRIMARY KEY,
    total_portfolio_value DECIMAL(20, 2) NOT NULL,
    total_pnl DECIMAL(20, 2) NOT NULL,
    pnl_percentage DECIMAL(10, 2) NOT NULL,
    win_rate DECIMAL(10, 2) NOT NULL,
    total_trades INT NOT NULL,
    open_positions INT NOT NULL,
    capital_deployed DECIMAL(20, 2) NOT NULL,
    last_analysis_time DATETIME,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_created_at (created_at),
    INDEX idx_metric_latest (created_at DESC)
);

-- Tabela de Trades
CREATE TABLE IF NOT EXISTS trades (
    id INT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    direction VARCHAR(10) NOT NULL,
    entry_price DECIMAL(20, 8) NOT NULL,
    exit_price DECIMAL(20, 8),
    size DECIMAL(20, 8) NOT NULL,
    pnl DECIMAL(20, 2),
    pnl_percentage DECIMAL(10, 2),
    status VARCHAR(20) NOT NULL,
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
    top_buy VARCHAR(200),
    top_sell VARCHAR(200),
    analysis_duration_ms INT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_created_at (created_at)
);
"""

def setup_database():
    """Cria tabelas no banco de dados"""
    try:
        # Conectar
        config = {
            'host': os.getenv('MYSQLHOST', 'localhost'),
            'user': os.getenv('MYSQLUSER', 'root'),
            'password': os.getenv('MYSQLPASSWORD', ''),
            'database': os.getenv('MYSQLDATABASE', 'railway'),
            'port': int(os.getenv('MYSQLPORT', 3306))
        }
        
        logger.info(f"Conectando ao MySQL ({config['host']})...")
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()
        
        # Executar SQL
        logger.info("Criando tabelas...")
        for statement in SQL_INIT.split(';'):
            statement = statement.strip()
            if statement and not statement.startswith('--'):
                cursor.execute(statement)
        
        conn.commit()
        logger.info("✅ Banco de dados inicializado com sucesso!")
        
        # Listar tabelas criadas
        cursor.execute("SHOW TABLES;")
        tables = cursor.fetchall()
        logger.info(f"Tabelas: {', '.join([t[0] for t in tables])}")
        
        cursor.close()
        conn.close()
        
        return True
    
    except Exception as e:
        logger.error(f"❌ Erro ao setup: {e}")
        return False

if __name__ == '__main__':
    setup_database()
