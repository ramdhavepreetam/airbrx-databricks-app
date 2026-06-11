"""
Core SQL queries (§5).

Key schema facts (verified against live workspace):
  billing_usage_v: columns are d (DATE), sku_name, warehouse_id (STRING), usage_quantity,
                   usage_start_time, usage_end_time, billing_origin_product
  query_history_v: columns are statement_id, executed_by, warehouse_id, statement_type,
                   execution_status, total_duration_ms, read_bytes, start_time, end_time,
                   statement_text
"""

BILLING_VIEW       = "airbrx_app.state.billing_usage_v"
QUERY_HISTORY_VIEW = "airbrx_app.state.query_history_v"

# §5a — priced usage by warehouse and day
# dbu_rate_usd: passed at runtime from DBU_RATE_USD env var (default $0.70/DBU)
PRICED_USAGE = """
SELECT
  d,
  sku_name,
  warehouse_id,
  SUM(CAST(usage_quantity AS DOUBLE))                         AS dbus,
  SUM(CAST(usage_quantity AS DOUBLE)) * {dbu_rate_usd}        AS usd
FROM {billing_view}
WHERE d >= date_sub(current_date(), {lookback_days})
  AND warehouse_id IS NOT NULL
GROUP BY ALL
ORDER BY d
"""

# §5b — per-fingerprint attributed cost (duration-share allocation)
ATTRIBUTED_COST = """
WITH wh_cost AS (
  SELECT d, warehouse_id,
         SUM(CAST(usage_quantity AS DOUBLE)) * {dbu_rate_usd} AS wh_usd
  FROM {billing_view}
  WHERE d >= date_sub(current_date(), {lookback_days})
    AND warehouse_id IS NOT NULL
  GROUP BY ALL
),
stmt AS (
  SELECT d, warehouse_id, statement_id, total_duration_ms, ck
  FROM {state_catalog}.{state_schema}.fingerprint_history
  WHERE d >= date_sub(current_date(), {lookback_days})
),
wh_dur AS (
  SELECT d, warehouse_id, SUM(total_duration_ms) AS tot_ms
  FROM stmt GROUP BY ALL
)
SELECT
  s.ck,
  COUNT(*)                                                             AS execs,
  SUM(c.wh_usd * (s.total_duration_ms / NULLIF(w.tot_ms, 0)))        AS attributed_usd
FROM stmt s
JOIN wh_dur  w USING (d, warehouse_id)
JOIN wh_cost c USING (d, warehouse_id)
GROUP BY s.ck
ORDER BY attributed_usd DESC
"""

# §5c — gateway coverage ratio
COVERAGE = """
SELECT
  CAST(date(start_time) AS DATE)                                     AS d,
  COUNT_IF(statement_text LIKE '/* abx %')                           AS gateway_routed,
  COUNT(*)                                                           AS total,
  COUNT_IF(statement_text LIKE '/* abx %') * 1.0 / COUNT(*)         AS coverage_ratio
FROM {state_catalog}.{state_schema}.query_history_v
WHERE start_time >= date_sub(current_date(), {lookback_days})
GROUP BY ALL
ORDER BY d
"""

# Incremental ingest — new query_history rows since last checkpoint
INCREMENTAL_HISTORY = """
SELECT
  statement_id,
  warehouse_id,
  CAST(total_duration_ms AS BIGINT)           AS total_duration_ms,
  CAST(start_time AS TIMESTAMP)               AS start_time,
  statement_text
FROM {state_catalog}.{state_schema}.query_history_v
WHERE CAST(start_time AS TIMESTAMP) > '{checkpoint_ts}'
  AND execution_status = 'FINISHED'
ORDER BY start_time
"""
