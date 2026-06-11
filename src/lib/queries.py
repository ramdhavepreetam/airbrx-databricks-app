"""
Core SQL queries (§5). All use the shared views from airbrx_shared.system_views.

Key schema facts (verified against live workspace):
  billing_usage.usage_metadata  — JSON string; extract warehouse_id with get_json_object
  query_history.compute         — JSON string; extract warehouse_id with get_json_object
  No list_prices view available — cost = usage_quantity (DBUs) * {dbu_rate_usd} (env var)
"""

# SP reads from these views in airbrx_app.state (which it owns).
# Those views in turn read from airbrx_shared — keeping data inside the workspace.
BILLING_VIEW = "airbrx_app.state.billing_usage_v"
QUERY_HISTORY_VIEW = "airbrx_app.state.query_history_v"

# §5a — priced usage by warehouse and day
# dbu_rate_usd: passed at runtime from DBU_RATE_USD env var (default $0.70/DBU)
PRICED_USAGE = """
WITH priced AS (
  SELECT
    CAST(usage_date AS DATE)                                          AS d,
    sku_name,
    usage_metadata.warehouse_id                 AS warehouse_id,
    CAST(usage_quantity AS DOUBLE)                                    AS dbus,
    CAST(usage_quantity AS DOUBLE) * {dbu_rate_usd}                   AS usd
  FROM {billing_view}
  WHERE CAST(usage_date AS DATE) >= date_sub(current_date(), {lookback_days})
    AND usage_metadata.warehouse_id IS NOT NULL
)
SELECT d, sku_name, warehouse_id, SUM(dbus) AS dbus, SUM(usd) AS usd
FROM priced
GROUP BY ALL
ORDER BY d
"""

# §5b — per-fingerprint attributed cost (duration-share allocation)
ATTRIBUTED_COST = """
WITH priced AS (
  SELECT
    CAST(usage_date AS DATE)                                          AS d,
    usage_metadata.warehouse_id                 AS warehouse_id,
    CAST(usage_quantity AS DOUBLE) * {dbu_rate_usd}                   AS usd
  FROM {billing_view}
  WHERE CAST(usage_date AS DATE) >= date_sub(current_date(), {lookback_days})
    AND usage_metadata.warehouse_id IS NOT NULL
),
wh_cost AS (
  SELECT d, warehouse_id, SUM(usd) AS wh_usd
  FROM priced
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
  executed_by,
  get_json_object(compute, '$.warehouse_id')  AS warehouse_id,
  statement_type,
  execution_status,
  CAST(total_duration_ms AS BIGINT)           AS total_duration_ms,
  CAST(start_time AS TIMESTAMP)               AS start_time,
  CAST(end_time   AS TIMESTAMP)               AS end_time,
  statement_text
FROM {state_catalog}.{state_schema}.query_history_v
WHERE CAST(start_time AS TIMESTAMP) > '{checkpoint_ts}'
  AND execution_status = 'FINISHED'
ORDER BY start_time
"""
