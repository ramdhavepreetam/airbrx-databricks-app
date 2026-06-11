# TODO — Airbrx Databricks App v1

Status legend: [ ] open  [~] in progress / blocked  [x] done

---

## Immediate (before sharing with design partners)

- [ ] **Fix Airbrx API key** — `sync_log` shows `401 Unauthorized` on `api.airbrx.ai`.
  The key in the secret scope is missing or wrong. Replace it:
  ```bash
  databricks secrets put-secret airbrx-secrets api-key --string-value "<valid-key>"
  ```
  Then confirm sync works:
  ```bash
  databricks jobs run-now 874887062442519 --python-params '["--mode","sync","--warehouse-id","771ee5c628aafe42"]'
  ```

- [ ] **Test OBO (non-admin login)** — Log into the app as a non-admin user and confirm
  the drill-down only shows their own queries (`query_history_self_v` filter is working).

- [ ] **Verify waterfall totals** — Spot-check `waterfall_daily.usd` totals against the
  workspace billing dashboard for a recent week (target: within 1%).

---

## Fingerprint contract (gateway team sign-off)

- [ ] Share `docs/CONTRACT.md` with the gateway team and get sign-off that their
  implementation produces the same `ck` values as `tests/test_fingerprint.py`.
- [ ] Confirm gateway stamps `v=1` in all forwarded queries.

---

## Before GA / marketplace listing

- [~] **OBO → GA** — Now GA for compliance-profile workspaces (June 2026).
  GA for all workspaces TBD. Monitor Databricks release notes before public listing.
- [ ] **Egress allowlist NCC IPs** — Add stable AWS/Azure/GCP egress IPs to
  `docs/OPERATIONS.md`. Get IPs from Airbrx infra team.
- [ ] **Apply to Databricks Technology Partner program** — Required for a public
  marketplace listing. Start at partner.databricks.com.
- [ ] **Verify system table schemas at GA** — `system.billing.usage` and
  `system.query.history` are Public Preview. Re-verify column names before listing.

---

## Done ✅

**Setup & deploy**
- [x] Admin SQL onboarding automated — `resources/setup.py` one-command setup
- [x] State tables initialized — all 6 tables exist with data
- [x] Analysis job running — scheduled hourly, UNPAUSED, last run succeeded (1,347 fingerprints)
- [x] App deployed — https://airbrx-app-7474644090124992.aws.databricksapps.com
- [x] Git repo — https://github.com/ramdhavepreetam/airbrx-databricks-app

**Code quality**
- [x] Analysis job rewritten to pure SQL (Statement Execution API) — no PySpark/PyArrow dependency
- [x] Billing view schema verified live (`d`, `warehouse_id` top-level columns)
- [x] Query history view schema verified live (all expected columns confirmed)
- [x] Ingest scales to any size — SQL INSERT, no Python collect() loop
- [x] Security review passed — outbound to api.airbrx.ai contains only `{rule_id, ck, window, avoided, realized_usd}`, no PII, no query text
- [x] All code bugs fixed (realized_usd, checkpoint, waterfall column mismatch, DDL, etc.)

**Marketplace readiness**
- [x] `databricks.yml` Asset Bundle — `databricks bundle deploy` works
- [x] CI pipeline — `pytest tests/` + `bundle validate` on every PR
- [x] No secrets or personal data in git history
- [x] Warehouse ID and workspace URL removed from source (env vars only)
