"""
Airbrx analysis job — uses Databricks Statement Execution API (SQL warehouse).
No PySpark/PyArrow dependency. Runs as a Python serverless task.

Modes:
  --mode ingest     Hourly: pull new query_history rows → fingerprint_history
  --mode recompute  Daily:  rebuild waterfall, coverage, rule_effectiveness
  --mode sync       On-demand: push aggregates to api.airbrx.ai, pull rules
  --mode all        All three in order (default)
"""
import argparse
import base64
import hashlib
import logging
import os
import re
import time
from datetime import datetime, timezone, timedelta

import requests
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

CATALOG       = os.environ.get("AIRBRX_STATE_CATALOG",  "airbrx_app")
SCHEMA        = os.environ.get("AIRBRX_STATE_SCHEMA",   "state")
BILLING_VIEW  = os.environ.get("AIRBRX_BILLING_VIEW",   "airbrx_app.state.billing_usage_v")
DBU_RATE_USD  = float(os.environ.get("DBU_RATE_USD",     "0.70"))
LOOKBACK_DAYS = 30
INSERT_BATCH  = 500  # rows per VALUES batch

# Resolved in main() after arg parsing; module-level placeholder.
WAREHOUSE_ID: str = ""

# ---------------------------------------------------------------------------
# Inlined fingerprint — SHARED CONTRACT with the gateway (§6c)
# ---------------------------------------------------------------------------
_FP_VERSION   = 1
_BLOCK_CMT    = re.compile(r"/\*.*?\*/", re.DOTALL)
_LINE_CMT     = re.compile(r"--[^\n]*")
_WHITESPACE   = re.compile(r"\s+")
_STR_LIT      = re.compile(r"'(?:[^'\\]|\\.)*'")
_NUM_LIT      = re.compile(r"\b\d+(?:\.\d+)?\b")
_ABX          = re.compile(r"^/\*\s*abx\s+(?P<kv>[^*]+)\*/", re.DOTALL)


def _normalize(sql: str) -> str:
    sql = _BLOCK_CMT.sub("", sql)
    sql = _LINE_CMT.sub("", sql)
    sql = sql.lower()
    sql = _WHITESPACE.sub(" ", sql).strip()
    sql = _STR_LIT.sub("?", sql)
    sql = _NUM_LIT.sub("?", sql)
    return sql


def fingerprint(sql: str) -> str:
    digest = hashlib.sha256(_normalize(sql).encode("utf-8")).digest()
    return base64.b32encode(digest).decode("ascii").lower()[:16]


def parse_tag(sql: str) -> dict | None:
    m = _ABX.match(sql.lstrip())
    if not m:
        return None
    try:
        return dict(kv.split("=", 1) for kv in m.group("kv").split())
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# SQL execution helper
# ---------------------------------------------------------------------------
def _get_client() -> WorkspaceClient:
    return WorkspaceClient()


def run_sql(w: WorkspaceClient, sql: str, timeout: str = "50s") -> list[list]:
    """Execute SQL, poll until done, return rows (or [])."""
    resp = w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=sql.strip(),
        wait_timeout=timeout,
    )
    for _ in range(120):
        if resp.status.state in (
            StatementState.SUCCEEDED, StatementState.FAILED,
            StatementState.CANCELED, StatementState.CLOSED,
        ):
            break
        time.sleep(2)
        resp = w.statement_execution.get_statement(resp.statement_id)

    if resp.status.state != StatementState.SUCCEEDED:
        raise RuntimeError(
            f"SQL failed ({resp.status.state}): {resp.status.error}\n---\n{sql[:400]}"
        )
    return resp.result.data_array or [] if resp.result else []


def exec_sql(w: WorkspaceClient, sql: str) -> None:
    run_sql(w, sql)


# ---------------------------------------------------------------------------
# Hourly ingest
# ---------------------------------------------------------------------------
def _get_checkpoint(w: WorkspaceClient) -> datetime:
    try:
        rows = run_sql(w, f"SELECT MAX(start_time) FROM {CATALOG}.{SCHEMA}.fingerprint_history")
        val  = rows[0][0] if rows and rows[0][0] else None
        if val:
            return datetime.fromisoformat(str(val).replace("Z", "+00:00"))
    except Exception:
        pass
    return datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)


def run_ingest(w: WorkspaceClient) -> None:
    checkpoint     = _get_checkpoint(w)
    checkpoint_str = checkpoint.strftime("%Y-%m-%d %H:%M:%S")
    logger.info("Ingest checkpoint: %s", checkpoint_str)

    rows = run_sql(w, f"""
        SELECT statement_id, warehouse_id, total_duration_ms,
               CAST(start_time AS TIMESTAMP) AS start_time,
               statement_text
        FROM {CATALOG}.{SCHEMA}.query_history_v
        WHERE CAST(start_time AS TIMESTAMP) > '{checkpoint_str}'
          AND execution_status = 'FINISHED'
        ORDER BY start_time
    """, timeout="50s")

    logger.info("Fetched %d new rows", len(rows))
    if not rows:
        return

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    records = []
    for r in rows:
        stmt       = r[4] or ""
        tag        = parse_tag(stmt)
        ck         = fingerprint(stmt)
        start_raw  = str(r[3]).split(".")[0] if r[3] else None
        d          = start_raw[:10] if start_raw else None
        records.append((
            ck,
            str(r[0]).replace("'", "''"),   # statement_id
            d,
            start_raw,
            str(r[1]).replace("'", "''") if r[1] else None,  # warehouse_id
            r[2],                           # total_duration_ms
            tag.get("route") if tag else None,
            tag.get("rule")  if tag else None,
            now,
        ))

    # Bulk-insert in batches to avoid statement size limits
    inserted = 0
    for i in range(0, len(records), INSERT_BATCH):
        batch  = records[i : i + INSERT_BATCH]
        values = ",\n".join(
            f"('{ck}', '{sid}', '{d}', '{st}', "
            f"{'NULL' if wh is None else repr(wh)}, "
            f"{'NULL' if ms is None else int(ms)}, "
            f"{'NULL' if route is None else repr(route)}, "
            f"{'NULL' if rule is None else repr(rule)}, "
            f"'{ing}')"
            for ck, sid, d, st, wh, ms, route, rule, ing in batch
        )
        exec_sql(w, f"""
            INSERT INTO {CATALOG}.{SCHEMA}.fingerprint_history
              (ck, statement_id, d, start_time, warehouse_id,
               total_duration_ms, route, rule_id, ingested_at)
            VALUES {values}
        """)
        inserted += len(batch)
        logger.info("Inserted batch %d/%d (%d rows)", i // INSERT_BATCH + 1,
                    -(-len(records) // INSERT_BATCH), len(batch))

    logger.info("Ingest complete — %d rows total", inserted)


# ---------------------------------------------------------------------------
# Daily recompute
# ---------------------------------------------------------------------------
def run_recompute(w: WorkspaceClient) -> None:
    logger.info("Recompute started")
    _recompute_waterfall(w)
    _recompute_coverage(w)
    _recompute_rule_effectiveness(w)
    logger.info("Recompute complete")


def _recompute_waterfall(w: WorkspaceClient) -> None:
    exec_sql(w, f"""
        INSERT OVERWRITE {CATALOG}.{SCHEMA}.waterfall_daily
        SELECT
          d,
          sku_name,
          warehouse_id,
          SUM(CAST(usage_quantity AS DOUBLE))                         AS dbus,
          SUM(CAST(usage_quantity AS DOUBLE)) * {DBU_RATE_USD}        AS usd,
          'raw'                                                       AS layer,
          current_timestamp()                                         AS updated_at
        FROM {BILLING_VIEW}
        WHERE d >= date_sub(current_date(), {LOOKBACK_DAYS})
          AND warehouse_id IS NOT NULL
        GROUP BY ALL
    """)
    logger.info("waterfall_daily updated")


def _recompute_coverage(w: WorkspaceClient) -> None:
    exec_sql(w, f"""
        INSERT OVERWRITE {CATALOG}.{SCHEMA}.coverage_daily
        SELECT
          CAST(date(start_time) AS DATE)                              AS d,
          COUNT_IF(statement_text LIKE '/* abx %')                   AS gateway_routed,
          COUNT(*)                                                    AS total,
          COUNT_IF(statement_text LIKE '/* abx %') * 1.0 / COUNT(*)  AS coverage_ratio,
          current_timestamp()                                         AS updated_at
        FROM {CATALOG}.{SCHEMA}.query_history_v
        WHERE start_time >= date_sub(current_date(), {LOOKBACK_DAYS})
        GROUP BY ALL
        ORDER BY d
    """)
    logger.info("coverage_daily updated")


def _recompute_rule_effectiveness(w: WorkspaceClient) -> None:
    exec_sql(w, f"""
        INSERT OVERWRITE {CATALOG}.{SCHEMA}.rule_effectiveness
        WITH attributed AS (
          SELECT fh.ck,
                 COUNT(*)                                                          AS execs,
                 SUM(wc.wh_usd * (fh.total_duration_ms / NULLIF(wd.tot_ms, 0))) AS attributed_usd
          FROM {CATALOG}.{SCHEMA}.fingerprint_history fh
          JOIN (
            SELECT d, warehouse_id,
                   SUM(CAST(usage_quantity AS DOUBLE)) * {DBU_RATE_USD} AS wh_usd
            FROM {BILLING_VIEW}
            WHERE d >= date_sub(current_date(), {LOOKBACK_DAYS})
              AND warehouse_id IS NOT NULL
            GROUP BY 1, 2
          ) wc ON fh.d = wc.d AND fh.warehouse_id = wc.warehouse_id
          JOIN (
            SELECT d, warehouse_id, SUM(total_duration_ms) AS tot_ms
            FROM {CATALOG}.{SCHEMA}.fingerprint_history
            WHERE d >= date_sub(current_date(), {LOOKBACK_DAYS})
            GROUP BY 1, 2
          ) wd ON fh.d = wd.d AND fh.warehouse_id = wd.warehouse_id
          WHERE fh.d >= date_sub(current_date(), {LOOKBACK_DAYS})
          GROUP BY fh.ck
        ),
        baseline AS (
          SELECT ck, rule_id, COUNT(*) / 30.0 AS baseline_rate
          FROM {CATALOG}.{SCHEMA}.fingerprint_history
          WHERE d BETWEEN date_sub(current_date(), 60) AND date_sub(current_date(), 31)
          GROUP BY ck, rule_id
        ),
        current_window AS (
          SELECT ck, COUNT(*) AS observed
          FROM {CATALOG}.{SCHEMA}.fingerprint_history
          WHERE d >= date_sub(current_date(), 30)
          GROUP BY ck
        )
        SELECT
          b.ck,
          b.rule_id,
          CONCAT(DATE_FORMAT(date_sub(current_date(), 30), 'yyyy-MM-dd'), '/',
                 DATE_FORMAT(current_date(), 'yyyy-MM-dd'))           AS window,
          b.baseline_rate,
          c.observed,
          GREATEST(0, CAST(b.baseline_rate * 30 - c.observed AS BIGINT)) AS avoided,
          GREATEST(0.0, b.baseline_rate * 30 - c.observed)
            * COALESCE(a.attributed_usd / NULLIF(a.execs, 0), 0.0)   AS realized_usd,
          current_timestamp()                                         AS updated_at
        FROM baseline b
        JOIN current_window c USING (ck)
        LEFT JOIN attributed a USING (ck)
    """)
    logger.info("rule_effectiveness updated")


# ---------------------------------------------------------------------------
# API sync
# ---------------------------------------------------------------------------
def run_sync(w: WorkspaceClient) -> None:
    logger.info("API sync started")
    ts     = datetime.now(timezone.utc)
    status = "ok"
    payload_bytes = 0
    try:
        scope   = os.environ.get("AIRBRX_SECRET_SCOPE", "airbrx-secrets")
        key     = os.environ.get("AIRBRX_SECRET_KEY",   "api-key")
        encoded = w.secrets.get_secret(scope=scope, key=key).value
        api_key = base64.b64decode(encoded).decode("utf-8")

        base_url = os.environ.get("AIRBRX_API_URL", "https://api.airbrx.ai").rstrip("/")
        headers  = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type":  "application/json",
            "X-Client":      "databricks-app-v1",
        }

        # Pull rules → upsert into rules table
        rules_resp = requests.get(f"{base_url}/v1/rules", headers=headers, timeout=15)
        rules_resp.raise_for_status()
        rules = rules_resp.json().get("rules", [])
        logger.info("Pulled %d rules", len(rules))
        if rules:
            values = ", ".join(
                f"('{r['rule_id']}', '{r.get('rule_type','')}', "
                f"'{str(r.get('description','')).replace(chr(39), chr(39)+chr(39))}', "
                f"'{str(r).replace(chr(39), chr(39)+chr(39))}', current_timestamp())"
                for r in rules
            )
            exec_sql(w, f"""
                MERGE INTO {CATALOG}.{SCHEMA}.rules AS target
                USING (SELECT * FROM VALUES {values}
                       AS t(rule_id, rule_type, description, config_json, pulled_at))
                  AS source ON target.rule_id = source.rule_id
                WHEN MATCHED     THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
            """)

        # Push effectiveness aggregates
        eff_rows = run_sql(w, f"""
            SELECT rule_id, ck, window, avoided, realized_usd
            FROM {CATALOG}.{SCHEMA}.rule_effectiveness
            WHERE window = (SELECT MAX(window) FROM {CATALOG}.{SCHEMA}.rule_effectiveness)
        """)
        payload = {
            "reported_at":    ts.isoformat(),
            "effectiveness":  [
                {"rule_id": r[0], "ck": r[1], "window": r[2],
                 "avoided": r[3], "realized_usd": r[4]}
                for r in eff_rows
            ],
        }
        push_resp = requests.post(f"{base_url}/v1/effectiveness",
                                  json=payload, headers=headers, timeout=15)
        push_resp.raise_for_status()
        payload_bytes = len(str(payload).encode())
        logger.info("Pushed %d effectiveness rows", len(eff_rows))

    except Exception as exc:
        status = f"error: {exc}"
        logger.exception("Sync failed: %s", exc)

    d_str = ts.date().isoformat()
    ts_str = ts.strftime("%Y-%m-%d %H:%M:%S")
    exec_sql(w, f"""
        INSERT INTO {CATALOG}.{SCHEMA}.sync_log
          (d, ts, direction, payload_bytes, status)
        VALUES ('{d_str}', '{ts_str}', 'push_pull', {payload_bytes}, '{status}')
    """)


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------
def main() -> None:
    global WAREHOUSE_ID
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["ingest", "recompute", "sync", "all"],
                        default="all")
    parser.add_argument("--warehouse-id", default=os.environ.get("DATABRICKS_WAREHOUSE_ID", ""),
                        help="SQL Warehouse ID (overrides DATABRICKS_WAREHOUSE_ID env var)")
    args = parser.parse_args()

    WAREHOUSE_ID = args.warehouse_id
    if not WAREHOUSE_ID:
        # Auto-discover: pick first available warehouse
        w0 = _get_client()
        whs = list(w0.warehouses.list())
        if not whs:
            raise SystemExit("No SQL warehouses found. Pass --warehouse-id or set DATABRICKS_WAREHOUSE_ID.")
        WAREHOUSE_ID = whs[0].id
        logger.info("Auto-discovered warehouse: %s (%s)", whs[0].name, WAREHOUSE_ID)

    w = _get_client()
    if args.mode in ("ingest",    "all"): run_ingest(w)
    if args.mode in ("recompute", "all"): run_recompute(w)
    if args.mode in ("sync",      "all"): run_sync(w)


if __name__ == "__main__":
    main()
