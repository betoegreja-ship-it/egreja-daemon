CREATE TABLE `notifications` (
	`id` int AUTO_INCREMENT NOT NULL,
	`type` varchar(50) NOT NULL,
	`title` varchar(255) NOT NULL,
	`message` text NOT NULL,
	`channels` text NOT NULL,
	`sent_at` timestamp NOT NULL DEFAULT (now()),
	CONSTRAINT `notifications_id` PRIMARY KEY(`id`)
);
--> statement-breakpoint
CREATE TABLE `sofia_analyses` (
	`id` int AUTO_INCREMENT NOT NULL,
	`symbol` varchar(20) NOT NULL,
	`recommendation` varchar(10) NOT NULL,
	`confidence` int NOT NULL,
	`reasoning` text NOT NULL,
	`market_data` text NOT NULL,
	`executed` int NOT NULL DEFAULT 0,
	`trade_id` int,
	`created_at` timestamp NOT NULL DEFAULT (now()),
	CONSTRAINT `sofia_analyses_id` PRIMARY KEY(`id`)
);
--> statement-breakpoint
CREATE TABLE `sofia_metrics` (
	`id` int AUTO_INCREMENT NOT NULL,
	`symbol` varchar(20) NOT NULL,
	`total_trades` int NOT NULL DEFAULT 0,
	`winning_trades` int NOT NULL DEFAULT 0,
	`losing_trades` int NOT NULL DEFAULT 0,
	`accuracy` int NOT NULL DEFAULT 0,
	`total_pnl` varchar(50) NOT NULL DEFAULT '0',
	`avg_confidence` int NOT NULL DEFAULT 0,
	`last_trade_at` timestamp,
	`updated_at` timestamp NOT NULL DEFAULT (now()) ON UPDATE CURRENT_TIMESTAMP,
	CONSTRAINT `sofia_metrics_id` PRIMARY KEY(`id`),
	CONSTRAINT `sofia_metrics_symbol_unique` UNIQUE(`symbol`)
);
--> statement-breakpoint
CREATE TABLE `trades` (
	`id` int AUTO_INCREMENT NOT NULL,
	`symbol` varchar(20) NOT NULL,
	`recommendation` varchar(10) NOT NULL,
	`confidence` int NOT NULL,
	`entry_price` varchar(50) NOT NULL,
	`exit_price` varchar(50),
	`quantity` varchar(50) NOT NULL,
	`pnl` varchar(50),
	`pnl_percent` varchar(50),
	`status` varchar(20) NOT NULL,
	`close_reason` varchar(50),
	`opened_at` timestamp NOT NULL DEFAULT (now()),
	`closed_at` timestamp,
	`duration` int,
	`created_at` timestamp NOT NULL DEFAULT (now()),
	CONSTRAINT `trades_id` PRIMARY KEY(`id`)
);
