-- Lab 4 – SQL analytics with DuckDB
-- Run with: duckdb analytics.duckdb -init init.sql, then .read answers/lab4/queries.sql

-- Query the bronze layer
SELECT uuid, username, name, birthdate
FROM read_csv(getvariable('bucket') || '/bronze/users.csv', header = true, strict_mode = false)
LIMIT 3;
SELECT Delimiter, Quote, HasHeader FROM sniff_csv(getvariable('bucket') || '/bronze/users.csv');
DESCRIBE FROM read_csv(getvariable('bucket') || '/bronze/orders.csv');
SELECT column_name, column_type, approx_unique, null_percentage
FROM (SUMMARIZE FROM read_csv(getvariable('bucket') || '/bronze/orders.csv'));

-- Load the tables
CREATE OR REPLACE TABLE users AS FROM read_csv(getvariable('bucket') || '/bronze/users.csv');
CREATE OR REPLACE TABLE orders AS FROM read_csv(getvariable('bucket') || '/bronze/orders.csv');
SELECT min(date) AS first_order, max(date) AS last_order FROM orders;

-- Data quality checks
SELECT count(*) AS orphan_orders FROM orders o ANTI JOIN users u ON o.user_uuid = u.uuid;
SELECT count(*) AS inactive_users FROM users u ANTI JOIN orders o ON o.user_uuid = u.uuid;
SELECT uuid, count(*) AS occurrences FROM orders GROUP BY uuid HAVING count(*) > 1;

-- Aggregations
SELECT product, count(*) AS orders, sum(quantity) AS quantity,
  round(100 * sum(quantity) / sum(sum(quantity)) OVER (), 1) AS share_pct
FROM orders GROUP BY product ORDER BY quantity DESC;
SELECT date_trunc('month', date) AS month, count(*) AS orders, sum(quantity) AS quantity
FROM orders GROUP BY month ORDER BY month;

-- Joins
SELECT u.username, u.name, count(*) AS orders, sum(o.quantity) AS quantity
FROM orders o JOIN users u ON o.user_uuid = u.uuid
GROUP BY ALL ORDER BY quantity DESC, u.username LIMIT 5;
SELECT (date_diff('year', u.birthdate, DATE '2020-01-01') // 20) * 20 AS age_group,
  count(DISTINCT u.uuid) AS users, count(*) AS orders, round(avg(o.quantity), 2) AS avg_quantity
FROM orders o JOIN users u ON o.user_uuid = u.uuid
GROUP BY age_group ORDER BY age_group;

-- Window functions
WITH daily AS (SELECT date::DATE AS day, sum(quantity) AS quantity FROM orders GROUP BY day)
SELECT day, quantity,
  sum(quantity) OVER (ORDER BY day) AS cumulative,
  round(avg(quantity) OVER (ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 1) AS avg_7d
FROM daily ORDER BY day LIMIT 10;
SELECT strftime(date, '%Y-%m') AS month, product, sum(quantity) AS quantity
FROM orders GROUP BY month, product
QUALIFY rank() OVER (PARTITION BY month ORDER BY sum(quantity) DESC) = 1
ORDER BY month;

-- Pivot
PIVOT (SELECT strftime(date, '%Y-%m') AS month, product, quantity FROM orders)
ON product USING sum(quantity) ORDER BY month;

-- Exercises
-- E1. Average number of orders per user, and average quantity per order
SELECT
  round(count(*) / count(DISTINCT user_uuid), 2) AS avg_orders_per_user,
  round(avg(quantity), 2) AS avg_quantity_per_order
FROM orders;

-- E2. First and last order of each user, and number of days between them
SELECT u.username,
  min(o.date)::DATE AS first_order,
  max(o.date)::DATE AS last_order,
  date_diff('day', min(o.date), max(o.date)) AS days_between
FROM orders o JOIN users u ON o.user_uuid = u.uuid
GROUP BY ALL
ORDER BY first_order;

-- E3. Hour of the day with the highest quantity sold
SELECT hour(date) AS hour, sum(quantity) AS quantity
FROM orders
GROUP BY hour
ORDER BY quantity DESC
LIMIT 3;

-- E4. Month-over-month variation of the quantity sold per product, in percent
WITH monthly AS (
  SELECT product, date_trunc('month', date) AS month, sum(quantity) AS quantity
  FROM orders
  GROUP BY product, month
)
SELECT product, strftime(month, '%Y-%m') AS month, quantity,
  lag(quantity) OVER w AS previous_quantity,
  round(100 * (quantity - lag(quantity) OVER w) / lag(quantity) OVER w, 1) AS variation_pct
FROM monthly
WINDOW w AS (PARTITION BY product ORDER BY month)
ORDER BY product, month;

-- E5. Users who ordered every product at least once
SELECT u.username, u.name
FROM orders o JOIN users u ON o.user_uuid = u.uuid
GROUP BY ALL
HAVING count(DISTINCT o.product) = (SELECT count(DISTINCT product) FROM orders)
ORDER BY u.username;
