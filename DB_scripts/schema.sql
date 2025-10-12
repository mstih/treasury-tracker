CREATE TABLE IF NOT EXISTS tariff_daily (
    id BIGSERIAL PRIMARY KEY,
    dts_date DATE NOT NULL UNIQUE,
    tariff_millions NUMERIC,
    total_deposits_millions NUMERIC,
    raw JSONB,
    fetched_at TIMESTAMP DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_tariff_daily_rate ON tariff_daily (dts_date);

CREATE TABLE IF NOT EXISTS tariff_monthly (
    month DATE PRIMARY KEY,
    tariff_millions_sum NUMERIC DEFAULT 0,
    total_deposits_millions_sum NUMERIC DEFAULT 0,
    updated_at TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS tariff_yearly (
    year INT PRIMARY KEY,
    tariff_millions_sum NUMERIC DEFAULT 0,
    total_deposits_millions_sum NUMERIC DEFAULT 0,
    updated_at TIMESTAMP DEFAULT now()
);