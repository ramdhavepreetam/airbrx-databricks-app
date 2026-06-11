# TODO — Airbrx Databricks App v1

Status legend: [ ] open  [~] blocked  [x] done

---

## Blockers before first design-partner demo

- [ ] **VERIFY: `pricing.default` field name** — run `DESCRIBE system.billing.list_prices` on the live workspace and confirm the price struct path. Update `src/lib/queries.py` if it differs. Single point of change.
- [ ] **VERIFY: `system.query.history` column names** — run `DESCRIBE system.query.history` and confirm `statement_text`, `total_duration_ms`, `warehouse_id`, etc. exist with these exact names. Update `airbrx_app.state.query_history_v` DDL if they differ.
- [ ] **Run admin SQL** — execute `docs/OPERATIONS.md §2` in the test workspace with SP name `app-ylpvko airbrx-app`.
- [ ] **Initialize state tables** — run `src/state/ddl.sql` against the warehouse.
- [ ] **Create secret scope + store API key** — `docs/OPERATIONS.md §4`.
- [ ] **Grant SP CAN USE on warehouse** — `docs/OPERATIONS.md §5`.
- [ ] **Create and run the analysis job once** to validate ingest → fingerprint_history populates.
- [ ] **Redeploy the app** with the fixed `app.yaml` (resources section added) so the Streamlit UI can actually query the warehouse.

---

## Code — must fix before sharing with design partners

- [x] `realized_usd` was always 0.0 — fixed to use `cost_per_exec` from attributed cost
- [x] Checkpoint never advanced — was using `MAX(ingested_at)` but filtering on `start_time`; fixed to `MAX(start_time)`
- [x] `start_time` not stored in `fingerprint_history` — added to DDL and ingest records
- [x] `waterfall_daily` column mismatch — `usage_date` aliased as `d` in PRICED_USAGE
- [x] `_recompute_coverage` full overwrite wiped history — added `replaceWhere`
- [x] `ddl.sql` partition expression `PARTITIONED BY (date(ts))` — fixed to explicit `d DATE` column
- [x] `ddl.sql` DEFAULT expressions — removed; application provides timestamps
- [x] Secret `.value` base64 decode — added in `airbrx_client.py`
- [x] Screen 4 SQL alias conflict `r.rule_id` with USING — rewritten to use explicit ON clause
- [x] Unused `WorkspaceClient` import in `streamlit_app.py` — removed
- [x] `__import__` inline in `analysis.py` — replaced with `from pyspark.sql import functions as F`
- [x] `app.yaml` missing `resources` section — SQL warehouse + secret scope added

---

## Integration validation (needs live workspace + data)

- [ ] Run ingest job and verify `fingerprint_history` row count > 0
- [ ] Run with a known query, confirm `ck` matches expected fingerprint from `test_fingerprint.py`
- [ ] Verify `waterfall_daily.usd` totals match the workspace billing dashboard (within 1%)
- [ ] Verify `coverage_daily.coverage_ratio` = 0 before any gateway traffic (correct baseline)
- [ ] Trigger `--mode sync`, check `sync_log` shows `status = ok`
- [ ] Confirm Streamlit UI loads without 403 errors (requires resources section in app.yaml)
- [ ] Test OBO: log in as a non-admin user, confirm they only see their own queries in drill-down

---

## Fingerprint contract

- [ ] Share `docs/CONTRACT.md` with the gateway team and get sign-off that their implementation matches the golden vectors in `tests/test_fingerprint.py`
- [ ] Run `pytest tests/test_fingerprint.py` against the gateway's implementation (or provide the golden vectors as a test fixture)
- [ ] Confirm `v=1` is stamped in all gateway-forwarded queries

---

## Before GA / marketplace

- [~] OBO moves from Public Preview to GA — now GA for compliance-profile workspaces (June 2026); GA for all workspaces TBD. Monitor Databricks release notes.
- [ ] `system.billing.list_prices` and `system.query.history` schemas reach GA — re-verify column names on live workspace before listing
- [x] Automate the admin SQL onboarding — `resources/setup.py` covers catalog/schema/grants/views/DDL/secrets/job in one command
- [ ] Add egress allowlist NCC IPs to `docs/OPERATIONS.md` for AWS/Azure/GCP — contact Airbrx infra for stable egress IP list
- [x] Ingest scales to 10M+ rows — replaced `collect()` + Python loop with Spark UDFs; statement_text never touches the driver
- [x] Security review: `push_effectiveness` sends only `{rule_id, ck, window, avoided, realized_usd}` — all aggregates, no PII, no query text. `executed_by` is selected in views but dropped before `fingerprint_history` write and never serialized to wire.
- [ ] Apply to Databricks Technology Partner program (partner.databricks.com) — prerequisite for public marketplace listing
- [x] Git-backed deployment — `databricks.yml` Asset Bundle added; deploy via `databricks bundle deploy`
- [x] CI pipeline — `.github/workflows/ci.yml` runs `pytest tests/` + `databricks bundle validate` on every PR
