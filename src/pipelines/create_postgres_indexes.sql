CREATE INDEX IF NOT EXISTS idx_fact_order_order_date_key
ON fact_order(order_date_key);

CREATE INDEX IF NOT EXISTS idx_fact_order_customer_id
ON fact_order(customer_id);

CREATE INDEX IF NOT EXISTS idx_fact_order_security_id
ON fact_order(security_id);

CREATE INDEX IF NOT EXISTS idx_fact_trade_trade_date_key
ON fact_trade(trade_date_key);

CREATE INDEX IF NOT EXISTS idx_fact_trade_customer_id
ON fact_trade(customer_id);

CREATE INDEX IF NOT EXISTS idx_fact_trade_security_id
ON fact_trade(security_id);

CREATE INDEX IF NOT EXISTS idx_cash_transaction_customer_id
ON fact_cash_transaction(customer_id);

CREATE INDEX IF NOT EXISTS idx_obt_customer_trading_activity_customer_id
ON obt_customer_trading_activity(customer_id);
