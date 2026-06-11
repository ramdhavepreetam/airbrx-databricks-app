# Airbrx Databricks App — v1

A Databricks App that reads cost and query-history system tables **in-place** and produces
a cost-optimization report populated from the customer's actual usage.

No query text or usage rows leave the workspace. Only aggregate effectiveness stats
and rule IDs cross the boundary to `api.airbrx.ai`.

---

## Customer Admin Onboarding (run once, by a workspace/metastore admin)

```sql
-- 1. App state catalog and schema (owned by the app SP)
CREATE CATALOG IF NOT EXISTS airbrx_app;
CREATE SCHEMA  IF NOT EXISTS airbrx_app.state;
GRANT ALL PRIVILEGES ON CATALOG airbrx_app TO `<app-service-principal>`;

-- 2. Cost tables: grant the SP read access
GRANT USE SCHEMA ON SCHEMA system.billing TO `<app-service-principal>`;
GRANT SELECT ON TABLE system.billing.usage       TO `<app-service-principal>`;
GRANT SELECT ON TABLE system.billing.list_prices TO `<app-service-principal>`;

-- 3. Query history: expose via an admin-owned view (preferred over direct grant)
CREATE OR REPLACE VIEW airbrx_app.state.query_history_v AS
SELECT statement_id, executed_by, warehouse_id, statement_type,
       execution_status, total_duration_ms, read_bytes,
       start_time, end_time, statement_text
FROM system.query.history
WHERE start_time >= current_date() - INTERVAL 90 DAYS;
GRANT SELECT ON VIEW airbrx_app.state.query_history_v TO `<app-service-principal>`;

-- 4. Per-viewer scoped view for the interactive UI (non-admins see only their own)
CREATE OR REPLACE VIEW airbrx_app.state.query_history_self_v AS
SELECT * FROM airbrx_app.state.query_history_v
WHERE executed_by = current_user();
```

Also required (via UI or API, not SQL):
- Grant the app SP **CAN USE** on the SQL warehouse the job runs against.
- Add `api.airbrx.ai` to the workspace **egress allowlist** (network policy).
  Supply your Airbrx NCC stable egress IPs to the customer's network admin.
  On failure, check `system.access.outbound_network` for the denied FQDN.

> **Before running:** verify `system.query.history` column names and the
> `system.billing.list_prices` price struct field (`pricing.default`) against
> the live Databricks system-table reference — these are Public Preview and have changed before.

---

## Deploy Steps

### 1. Create a secret scope and store the API key

```bash
databricks secrets create-scope airbrx-secrets
databricks secrets put-secret airbrx-secrets api-key --string-value "<your-airbrx-api-key>"
```

### 2. Deploy the app (git-backed)

In the Databricks workspace UI:
1. Go to **Apps** → **Create App**
2. Select **Git provider** and point to this repo
3. Set entrypoint to `app.yaml`
4. The workspace auto-creates an **App Service Principal** — note its name

Run the admin SQL above substituting `<app-service-principal>` with that name.

### 3. Initialize the state tables

```bash
databricks sql execute --warehouse-id <wh-id> -f src/state/ddl.sql
```

### 4. Create and run the analysis job

```bash
databricks jobs create --json @resources/job.yml
# Fill in existing_cluster_id in job.yml first, or use serverless
databricks jobs run-now --job-id <job-id>
```

---

## Repo Layout

```
airbrx-databricks-app/
├── app.yaml                  Databricks app config (entrypoint, OBO flag)
├── requirements.txt
├── README.md                 This file (admin runbook + deploy steps)
├── src/
│   ├── streamlit_app.py      Interactive UI — 4 screens (OBO auth)
│   ├── jobs/
│   │   └── analysis.py       Scheduled job (ingest + recompute + sync)
│   ├── lib/
│   │   ├── fingerprint.py    Canonical fingerprint — SHARED CONTRACT with gateway
│   │   ├── tags.py           abx comment-tag parser
│   │   ├── queries.py        Parameterized SQL (§5)
│   │   └── airbrx_client.py  Airbrx API client (reads key from secret scope)
│   └── state/
│       └── ddl.sql           airbrx_app.state table definitions
└── resources/
    └── job.yml               Lakeflow job definition
```

---

## Important Notes

- **OBO (on-behalf-of-user) auth is Public Preview** — fine for design partners; revisit before GA.
- **Marketplace listing is deferred past v1.** This is a deploy-to-workspace app.
- Cost attribution uses duration-share allocation. The UI states this assumption explicitly.
- Cache hits appear as *absent* statements in `query.history` — absence is the savings signal.
