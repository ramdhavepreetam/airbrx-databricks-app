# Operations Guide

## First-time setup checklist

### 1. App service principal
After deploying the app, the auto-created SP is shown in `databricks apps get airbrx-app`:
```
"service_principal_name": "app-ylpvko airbrx-app"
```
Use this exact name in all GRANT statements below.

### 2. Run admin SQL (workspace admin required)
Open Databricks SQL Editor and run each block:

```sql
-- Catalog and schema
CREATE CATALOG IF NOT EXISTS airbrx_app;
CREATE SCHEMA  IF NOT EXISTS airbrx_app.state;
GRANT ALL PRIVILEGES ON CATALOG airbrx_app TO `app-ylpvko airbrx-app`;

-- Billing tables
GRANT USE SCHEMA ON SCHEMA system.billing TO `app-ylpvko airbrx-app`;
GRANT SELECT ON TABLE system.billing.usage        TO `app-ylpvko airbrx-app`;
GRANT SELECT ON TABLE system.billing.list_prices  TO `app-ylpvko airbrx-app`;

-- Query history view (admin-owned, avoids direct SP access to system.query.history)
CREATE OR REPLACE VIEW airbrx_app.state.query_history_v AS
SELECT statement_id, executed_by, warehouse_id, statement_type,
       execution_status, total_duration_ms, read_bytes,
       start_time, end_time, statement_text
FROM system.query.history
WHERE start_time >= current_date() - INTERVAL 90 DAYS;
GRANT SELECT ON VIEW airbrx_app.state.query_history_v TO `app-ylpvko airbrx-app`;

-- Per-user scoped view for OBO UI
CREATE OR REPLACE VIEW airbrx_app.state.query_history_self_v AS
SELECT * FROM airbrx_app.state.query_history_v
WHERE executed_by = current_user();
```

### 3. Initialize state tables
```bash
databricks sql execute --warehouse-id 771ee5c628aafe42 -f src/state/ddl.sql
```

### 4. Create the secret scope and store the API key
```bash
databricks secrets create-scope airbrx-secrets
databricks secrets put-secret airbrx-secrets api-key --string-value "<your-airbrx-api-key>"
```
Grant the SP read access to the scope:
```bash
databricks secrets put-acl airbrx-secrets "app-ylpvko airbrx-app" READ
```

### 5. Grant SP CAN USE on the SQL warehouse
In the Databricks UI: SQL Warehouses → Serverless Starter Warehouse → Permissions → Add `app-ylpvko airbrx-app` with CAN USE.

### 6. Verify billing schema fields before running the job
```sql
DESCRIBE system.billing.list_prices;
-- Confirm the price field path: pricing.default or pricing.dbu_or_token.default
SELECT sku_name, pricing FROM system.billing.list_prices LIMIT 5;
```
Update `src/lib/queries.py` line `p.pricing.default` if the field name differs.

### 7. Create and start the analysis job
```bash
# Fill in existing_cluster_id in resources/job.yml first
databricks jobs create --json @resources/job.yml
databricks jobs run-now --job-id <job-id>
```

### 8. Redeploy the app after any code change
```bash
databricks apps deploy airbrx-app \
  --source-code-path "/Workspace/Users/ramdhavepreetam@gmail.com/airbrx-databricks-app" \
  --no-wait
```

---

## Ongoing operations

### Job schedule
- **Hourly:** `--mode ingest` — pulls new query_history rows into fingerprint_history
- **Daily (after ingest):** `--mode recompute` — rebuilds waterfall, coverage, rule_effectiveness
- **Daily (after recompute):** `--mode sync` — pushes aggregates to api.airbrx.ai

### Monitoring
- Check `airbrx_app.state.sync_log` for API sync failures
- Check job run history in Databricks Workflows for analysis job failures
- Check `system.access.outbound_network` if egress to `api.airbrx.ai` is failing

### Realized savings will show as $0 for the first 30 days
This is expected. The baseline window requires 30 days of pre-deployment data in `fingerprint_history`. After 60 days of operation, the pre/post comparison window is fully populated and `realized_usd` will show real numbers.

---

## Known limitations (v1)

| Limitation | Impact | Resolution |
|---|---|---|
| OBO is Public Preview | UI may behave differently for non-admin users | Revisit before GA |
| `system.billing.list_prices` price struct is Public Preview | `pricing.default` field name may change | Verify against live schema before each deploy |
| `system.query.history` column names are Public Preview | View in §3 admin SQL may need adjustment | Single adjustment point in `query_history_v` view |
| No egress allowlist on free/standard tier | `api.airbrx.ai` call may be blocked | Contact customer network admin with NCC IPs |
| Realized savings require 60+ days of data | First month shows $0 | Communicate to design partners upfront |
