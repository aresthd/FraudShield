CREATE TABLE IF NOT EXISTS transactions_raw (
    transaction_id VARCHAR(100) PRIMARY KEY,
    customer_id VARCHAR(100),
    account_id VARCHAR(100),
    transaction_type VARCHAR(50),
    amount NUMERIC(20, 4),
    currency VARCHAR(10),
    merchant_id VARCHAR(100),
    merchant_name VARCHAR(255),
    merchant_category VARCHAR(100),
    payment_method VARCHAR(100),
    device_id VARCHAR(100),
    ip_address VARCHAR(50),
    city VARCHAR(100),
    country VARCHAR(100),
    latitude NUMERIC(10, 6),
    longitude NUMERIC(10, 6),
    timestamp TIMESTAMPTZ,
    channel VARCHAR(100),
    load_ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS transactions_enriched (
    transaction_id VARCHAR(100) PRIMARY KEY,
    customer_id VARCHAR(100),
    account_id VARCHAR(100),
    transaction_type VARCHAR(50),
    amount NUMERIC(20, 4),
    currency VARCHAR(10),
    merchant_id VARCHAR(100),
    merchant_name VARCHAR(255),
    merchant_category VARCHAR(100),
    payment_method VARCHAR(100),
    device_id VARCHAR(100),
    ip_address VARCHAR(50),
    city VARCHAR(100),
    country VARCHAR(100),
    latitude NUMERIC(10, 6),
    longitude NUMERIC(10, 6),
    channel VARCHAR(100),
    account_age_days INTEGER,
    customer_risk_level VARCHAR(50),
    merchant_risk_level VARCHAR(50),
    is_new_device BOOLEAN,
    is_new_location BOOLEAN,
    is_foreign_transaction BOOLEAN,
    is_night_transaction BOOLEAN,
    is_high_amount BOOLEAN,
    event_date DATE,
    event_hour INTEGER,
    timestamp TIMESTAMPTZ,
    processing_time TIMESTAMPTZ,
    load_ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS fraud_results (
    result_id VARCHAR(100) PRIMARY KEY,
    transaction_id VARCHAR(100),
    customer_id VARCHAR(100),
    amount NUMERIC(20, 4),
    fraud_score INTEGER,
    status VARCHAR(50),
    risk_level VARCHAR(50),
    triggered_rules JSONB,
    rule_count INTEGER,
    velocity_count INTEGER,
    velocity_amount NUMERIC(20, 4),
    window_start TIMESTAMPTZ,
    window_end TIMESTAMPTZ,
    previous_city VARCHAR(100),
    current_city VARCHAR(100),
    distance_km NUMERIC(20, 4),
    time_since_last_transaction_sec BIGINT,
    is_impossible_travel BOOLEAN,
    detection_method VARCHAR(100),
    processed_at TIMESTAMPTZ,
    load_ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS fraud_alerts (
    alert_id VARCHAR(100) PRIMARY KEY,
    result_id VARCHAR(100),
    transaction_id VARCHAR(100),
    customer_id VARCHAR(100),
    severity VARCHAR(50),
    fraud_score INTEGER,
    status VARCHAR(50),
    alert_type VARCHAR(100),
    triggered_rules JSONB,
    message TEXT,
    created_at TIMESTAMPTZ,
    load_ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS fraud_rules (
    rule_id VARCHAR(100) PRIMARY KEY,
    rule_name VARCHAR(255),
    rule_type VARCHAR(100),
    description TEXT,
    threshold NUMERIC(20, 4),
    window_seconds INTEGER,
    max_transactions INTEGER,
    score INTEGER,
    severity VARCHAR(50),
    enabled BOOLEAN,
    action VARCHAR(50),
    version INTEGER,
    updated_at TIMESTAMPTZ,
    updated_by VARCHAR(100),
    operation VARCHAR(50),
    load_ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS fraud_dlq (
    dlq_id VARCHAR(100) PRIMARY KEY,
    source_topic VARCHAR(255),
    source_partition INTEGER,
    source_offset BIGINT,
    event_key VARCHAR(255),
    error_type VARCHAR(100),
    error_message TEXT,
    failed_at_stage VARCHAR(100),
    retry_count INTEGER,
    timestamp TIMESTAMPTZ,
    original_payload JSONB,
    load_ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_transactions_raw_customer_id
    ON transactions_raw(customer_id);

CREATE INDEX IF NOT EXISTS idx_transactions_raw_timestamp
    ON transactions_raw(timestamp);

CREATE INDEX IF NOT EXISTS idx_transactions_enriched_customer_id
    ON transactions_enriched(customer_id);

CREATE INDEX IF NOT EXISTS idx_fraud_results_customer_id
    ON fraud_results(customer_id);

CREATE INDEX IF NOT EXISTS idx_fraud_results_status
    ON fraud_results(status);

CREATE INDEX IF NOT EXISTS idx_fraud_results_processed_at
    ON fraud_results(processed_at);

CREATE INDEX IF NOT EXISTS idx_fraud_alerts_customer_id
    ON fraud_alerts(customer_id);

CREATE INDEX IF NOT EXISTS idx_fraud_alerts_created_at
    ON fraud_alerts(created_at);

CREATE INDEX IF NOT EXISTS idx_fraud_rules_rule_type
    ON fraud_rules(rule_type);

CREATE INDEX IF NOT EXISTS idx_fraud_dlq_source_topic
    ON fraud_dlq(source_topic);