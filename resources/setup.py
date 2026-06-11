#!/usr/bin/env python3
"""
One-command setup for the Airbrx Databricks App.
Run once, as a workspace/metastore admin, after deploying the app:

    python resources/setup.py [options]

Steps performed:
  1. Resolve the app's auto-created service principal
  2. Create airbrx_app catalog + state schema, grant SP full ownership
  3. Grant SP read access to system.billing tables
  4. Create admin-owned query_history views (avoids direct system.query.history grant)
  5. Initialize airbrx_app.state state tables from src/state/ddl.sql
  6. Create the Airbrx secret scope and store your API key
  7. Grant SP READ on the secret scope
  8. Create the analysis job in Databricks Workflows

Pre-requisites:
  - Databricks CLI authenticated:  databricks auth login
  - You are a workspace admin and metastore admin
  - A SQL warehouse is available (pass --warehouse-id or you'll be prompted)
"""
import argparse
import getpass
import os
import sys
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DDL_PATH   = os.path.join(SCRIPT_DIR, "..", "src", "state", "ddl.sql")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def ok(msg: str) -> None:
    print(f"  \033[32m✓\033[0m {msg}")


def info(msg: str) -> None:
    print(f"  {msg}")


def fail(msg: str) -> None:
    print(f"  \033[31m✗\033[0m {msg}", file=sys.stderr)
    sys.exit(1)


def run_sql(w: WorkspaceClient, wh_id: str, sql: str) -> None:
    resp = w.statement_execution.execute_statement(
        warehouse_id=wh_id,
        statement=sql.strip(),
        wait_timeout="60s",
    )
    for _ in range(60):
        state = resp.status.state
        if state in (
            StatementState.SUCCEEDED, StatementState.FAILED,
            StatementState.CANCELED, StatementState.CLOSED,
        ):
            break
        time.sleep(2)
        resp = w.statement_execution.get_statement(resp.statement_id)
    if resp.status.state != StatementState.SUCCEEDED:
        raise RuntimeError(
            f"SQL failed ({resp.status.state}): "
            f"{resp.status.error}\n---\n{sql[:300]}"
        )


def run_sql_file(w: WorkspaceClient, wh_id: str, path: str) -> int:
    with open(path) as f:
        content = f.read()
    statements = [
        s.strip()
        for s in content.split(";")
        if s.strip() and not s.strip().startswith("--")
    ]
    for stmt in statements:
        run_sql(w, wh_id, stmt)
    return len(statements)


def pick_warehouse(w: WorkspaceClient) -> str:
    warehouses = list(w.warehouses.list())
    if not warehouses:
        fail("No SQL warehouses found. Create one first.")
    print("\n  Available warehouses:")
    for i, wh in enumerate(warehouses):
        print(f"    [{i}] {wh.name}  ({wh.id})")
    while True:
        try:
            idx = int(input("  Select number: "))
            return warehouses[idx].id
        except (ValueError, IndexError):
            print("  Invalid selection, try again.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Set up Airbrx Databricks App (run as workspace admin)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--app-name",      default="airbrx-cost-intelligence")
    parser.add_argument("--warehouse-id",  default=os.environ.get("DATABRICKS_WAREHOUSE_ID", ""))
    parser.add_argument("--catalog",       default="airbrx_app")
    parser.add_argument("--schema",        default="state")
    parser.add_argument("--secret-scope",  default="airbrx-secrets")
    parser.add_argument("--secret-key",    default="api-key")
    parser.add_argument("--skip-job",      action="store_true",
                        help="Skip analysis job creation (create it manually later)")
    args = parser.parse_args()

    w = WorkspaceClient()
    print(f"\nConnected to: \033[1m{w.config.host}\033[0m\n")

    # ------------------------------------------------------------------
    # Step 1 — Resolve the app service principal
    # ------------------------------------------------------------------
    print("[1/7] Resolving app service principal...")
    try:
        app    = w.apps.get(args.app_name)
        sp_name = app.service_principal_name
        ok(f"Service principal: {sp_name}")
    except Exception as e:
        info(f"Could not auto-detect SP ({e})")
        sp_name = input("  Enter app service principal name: ").strip()
        if not sp_name:
            fail("Service principal name is required.")

    # ------------------------------------------------------------------
    # Step 2 — Pick warehouse
    # ------------------------------------------------------------------
    if not args.warehouse_id:
        print("\n[2/7] Select a SQL warehouse for setup SQL...")
        args.warehouse_id = pick_warehouse(w)
    else:
        print(f"[2/7] Using warehouse {args.warehouse_id}")
        ok("Warehouse set")

    # ------------------------------------------------------------------
    # Step 3 — Catalog, schema, ownership grant
    # ------------------------------------------------------------------
    print(f"\n[3/7] Creating {args.catalog}.{args.schema}...")
    for stmt in [
        f"CREATE CATALOG IF NOT EXISTS {args.catalog}",
        f"CREATE SCHEMA  IF NOT EXISTS {args.catalog}.{args.schema}",
        f"GRANT ALL PRIVILEGES ON CATALOG {args.catalog} TO `{sp_name}`",
    ]:
        run_sql(w, args.warehouse_id, stmt)
    ok(f"Catalog and schema ready; SP owns {args.catalog}")

    # ------------------------------------------------------------------
    # Step 4 — System table grants
    # ------------------------------------------------------------------
    print("\n[4/7] Granting system.billing access to SP...")
    for stmt in [
        f"GRANT USE SCHEMA ON SCHEMA system.billing TO `{sp_name}`",
        f"GRANT SELECT ON TABLE system.billing.usage        TO `{sp_name}`",
        f"GRANT SELECT ON TABLE system.billing.list_prices  TO `{sp_name}`",
    ]:
        run_sql(w, args.warehouse_id, stmt)
    ok("Billing grants applied")

    # ------------------------------------------------------------------
    # Step 5 — Admin-owned query history views
    # ------------------------------------------------------------------
    print("\n[5/7] Creating query_history views...")
    cat, sch = args.catalog, args.schema
    run_sql(w, args.warehouse_id, f"""
        CREATE OR REPLACE VIEW {cat}.{sch}.query_history_v AS
        SELECT statement_id, executed_by, warehouse_id, statement_type,
               execution_status, total_duration_ms, read_bytes,
               start_time, end_time, statement_text
        FROM system.query.history
        WHERE start_time >= current_date() - INTERVAL 90 DAYS
    """)
    run_sql(w, args.warehouse_id,
            f"GRANT SELECT ON VIEW {cat}.{sch}.query_history_v TO `{sp_name}`")
    run_sql(w, args.warehouse_id, f"""
        CREATE OR REPLACE VIEW {cat}.{sch}.query_history_self_v AS
        SELECT * FROM {cat}.{sch}.query_history_v
        WHERE executed_by = current_user()
    """)
    ok("query_history_v and query_history_self_v created")

    # ------------------------------------------------------------------
    # Step 6 — State tables DDL
    # ------------------------------------------------------------------
    print("\n[6/7] Initializing state tables (DDL)...")
    if not os.path.exists(DDL_PATH):
        fail(f"DDL file not found at {DDL_PATH}")
    n = run_sql_file(w, args.warehouse_id, DDL_PATH)
    ok(f"Executed {n} DDL statements from src/state/ddl.sql")

    # ------------------------------------------------------------------
    # Step 7 — Secret scope + API key
    # ------------------------------------------------------------------
    print("\n[7/7] Configuring Airbrx API key...")
    try:
        w.secrets.create_scope(scope=args.secret_scope)
        ok(f"Created secret scope '{args.secret_scope}'")
    except Exception:
        info(f"Scope '{args.secret_scope}' already exists — continuing")

    api_key = getpass.getpass("  Enter your Airbrx API key (input hidden): ")
    if not api_key:
        fail("API key cannot be empty.")
    w.secrets.put_secret(scope=args.secret_scope, key=args.secret_key, string_value=api_key)
    w.secrets.put_acl(scope=args.secret_scope, principal=sp_name, permission="READ")
    ok(f"API key stored in {args.secret_scope}/{args.secret_key}; SP granted READ")

    # ------------------------------------------------------------------
    # Optional — Create analysis job
    # ------------------------------------------------------------------
    if not args.skip_job:
        print("\n[+]  Creating analysis job in Databricks Workflows...")
        existing = [j for j in w.jobs.list(name="airbrx-analysis")]
        if existing:
            info(f"Job 'airbrx-analysis' already exists (ID: {existing[0].job_id}) — skipping.")
        else:
            try:
                import json
                job_yml_path = os.path.join(SCRIPT_DIR, "job.yml")
                # Prefer DAB deploy; fall back to job.yml import
                if os.path.exists(job_yml_path):
                    info("Run: databricks jobs create --json @resources/job.yml")
                    info("     (fill in existing_cluster_id or use serverless)")
                else:
                    info("Run: databricks bundle deploy to create the job via DAB")
            except Exception as exc:
                info(f"Could not create job automatically: {exc}")

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    print("\n" + "─" * 60)
    print("\033[1mSetup complete.\033[0m  Next steps:\n")
    print(f"  1. Grant '{sp_name}' CAN USE on warehouse {args.warehouse_id}")
    print(f"     (Warehouses → {args.warehouse_id} → Permissions → Add SP)")
    print(f"\n  2. Add api.airbrx.ai to your workspace egress allowlist")
    print(f"     (Admin Console → Networking → Egress → add api.airbrx.ai)")
    print(f"\n  3. Create and run the analysis job:")
    print(f"     databricks bundle deploy --target prod")
    print(f"     databricks jobs run-now --job-name airbrx-analysis")
    print(f"\n  4. Open the app:")
    print(f"     {w.config.host}/apps/{args.app_name}")
    print()


if __name__ == "__main__":
    main()
