CREATE TABLE `ml_models` (
	`id` int AUTO_INCREMENT NOT NULL,
	`version` varchar(50) NOT NULL,
	`model_type` varchar(50) NOT NULL,
	`accuracy` varchar(20) NOT NULL,
	`precision` varchar(20) NOT NULL,
	`recall` varchar(20) NOT NULL,
	`f1_score` varchar(20) NOT NULL,
	`training_size` int NOT NULL,
	`s3_path` varchar(500) NOT NULL,
	`is_active` int NOT NULL DEFAULT 0,
	`notes` text,
	`trained_at` timestamp NOT NULL DEFAULT (now()),
	`created_at` timestamp NOT NULL DEFAULT (now()),
	CONSTRAINT `ml_models_id` PRIMARY KEY(`id`),
	CONSTRAINT `ml_models_version_unique` UNIQUE(`version`)
);
