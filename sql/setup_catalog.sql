CREATE CATALOG IF NOT EXISTS market_pipeline;

CREATE SCHEMA IF NOT EXISTS market_pipeline.landing;
CREATE SCHEMA IF NOT EXISTS market_pipeline.bronze;
CREATE SCHEMA IF NOT EXISTS market_pipeline.silver;
CREATE SCHEMA IF NOT EXISTS market_pipeline.gold;

CREATE VOLUME IF NOT EXISTS market_pipeline.landing.trades_raw;
CREATE VOLUME IF NOT EXISTS market_pipeline.landing.reference;
CREATE VOLUME IF NOT EXISTS market_pipeline.landing.checkpoints;