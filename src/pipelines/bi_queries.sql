-- 1. Daily order amount
SELECT
  order_date_key,
  COUNT(*) AS total_orders,
  SUM(order_amount) AS total_order_amount
FROM fact_order
GROUP BY order_date_key
ORDER BY order_date_key;

-- 2. Top traded securities
SELECT
  security_id,
  ticker,
  COUNT(*) AS total_trades,
  SUM(trade_amount) AS total_trade_amount
FROM fact_trade
GROUP BY security_id, ticker
ORDER BY total_trade_amount DESC
LIMIT 10;

-- 3. Customer trading activity
SELECT
  customer_id,
  customer_segment,
  total_orders,
  total_trades,
  total_trade_amount,
  total_fee_amount
FROM obt_customer_trading_activity
ORDER BY total_trade_amount DESC
LIMIT 20;

-- 4. Explain example for optimization evidence
EXPLAIN ANALYZE
SELECT
  customer_id,
  SUM(order_amount) AS total_order_amount
FROM fact_order
WHERE order_date_key >= 20250101
GROUP BY customer_id
ORDER BY total_order_amount DESC
LIMIT 20;
