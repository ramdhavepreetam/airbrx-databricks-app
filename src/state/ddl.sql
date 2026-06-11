-- DDL for airbrx_app.state — run once by the app SP after catalog/schema are created.
-- All tables live inside the customer's workspace; nothing here leaves the account.
-- Application always provides timestamps; no DEFAULT expressions used (avoids needing
-- the delta.feature.allowColumnDefaults TBLPROPERTY on older runtimes).

-- Hourly ingest: one row per finished statement that passed through fingerprinting.
-- Raw statement_text is NOT stored here — only the hash (ck).
CREATE TABLE IF NOT EXISTS airbrx_app.state.fingerprint_history (
  ck                STRING    NOT NULL COMMENT 'Canonical query fingerprint (§6c)',
  statement_id      STRING    NOT NULL,
  d                 DATE      NOT NULL COMMENT 'Partition date = date(start_time)',
  start_time        TIMESTAMP COMMENT 'Original query start_time from query_history',
  warehouse_id      STRING,
  total_duration_ms BIGINT,
  route             STRING    COMMENT 'warehouse | cache_miss | smaller_wh | bypass | NULL',
  rule_id           STRING    COMMENT 'Airbrx rule that matched, if any',
  ingested_at       TIMESTAMP NOT NULL
)
USING DELTA
PARTITIONED BY (d)
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true');

-- Daily aggregate: total cost by SKU and day, used as the waterfall base.
CREATE TABLE IF NOT EXISTS airbrx_app.state.waterfall_daily (
  d            DATE    NOT NULL,
  sku_name     STRING  NOT NULL,
  warehouse_id STRING,
  dbus         DOUBLE,
  usd          DOUBLE,
  layer        STRING  COMMENT 'raw | projected_saving | realized_saving',
  updated_at   TIMESTAMP NOT NULL
)
USING DELTA
PARTITIONED BY (d);

-- Daily coverage: gateway-routed vs total statement counts.
CREATE TABLE IF NOT EXISTS airbrx_app.state.coverage_daily (
  d               DATE    NOT NULL,
  gateway_routed  BIGINT,
  total           BIGINT,
  coverage_ratio  DOUBLE,
  updated_at      TIMESTAMP NOT NULL
)
USING DELTA
PARTITIONED BY (d);

-- Per-fingerprint realized savings, updated daily once a pre/post window exists.
CREATE TABLE IF NOT EXISTS airbrx_app.state.rule_effectiveness (
  ck            STRING  NOT NULL,
  rule_id       STRING,
  window        STRING  NOT NULL COMMENT 'ISO date range: YYYY-MM-DD/YYYY-MM-DD',
  baseline_rate DOUBLE  COMMENT 'execs per day in pre-window',
  observed      BIGINT  COMMENT 'actual execs in current window',
  avoided       BIGINT  COMMENT 'max(0, baseline_rate * window_len - observed)',
  realized_usd  DOUBLE,
  updated_at    TIMESTAMP NOT NULL
)
USING DELTA;

-- Airbrx rule definitions pulled from api.airbrx.ai.
CREATE TABLE IF NOT EXISTS airbrx_app.state.rules (
  rule_id      STRING    NOT NULL,
  rule_type    STRING    COMMENT 'cache | bypass | smaller_wh',
  description  STRING,
  config_json  STRING    COMMENT 'Rule config as JSON string',
  pulled_at    TIMESTAMP NOT NULL
)
USING DELTA;

-- Outbound API call audit trail.
-- d column added explicitly so PARTITIONED BY works without an expression.
CREATE TABLE IF NOT EXISTS airbrx_app.state.sync_log (
  d             DATE      NOT NULL COMMENT 'Partition date = date(ts)',
  ts            TIMESTAMP NOT NULL,
  direction     STRING    NOT NULL COMMENT 'push_pull | pull_only | push_only',
  payload_bytes BIGINT,
  status        STRING    NOT NULL COMMENT 'ok | error: <message>'
)
USING DELTA
PARTITIONED BY (d);
