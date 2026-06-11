"""
Airbrx analysis job — self-contained, no relative imports.
Runs as the app service principal inside a Databricks serverless Python task.

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
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StringType, MapType

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

CATALOG      = os.environ.get("AIRBRX_STATE_CATALOG", "airbrx_app")
SCHEMA       = os.environ.get("AIRBRX_STATE_SCHEMA",  "state")
BILLING_VIEW = os.environ.get("AIRBRX_BILLING_VIEW",  "airbrx_app.state.billing_usage_v")
DBU_RATE_USD = float(os.environ.get("DBU_RATE_USD",   "0.70"))
LOOKBACK_DAYS = 30

# ---------------------------------------------------------------------------
# Inlined fingerprint (§6c) — SHARED CONTRACT with the gateway
# ---------------------------------------------------------------------------
_FP_VERSION   = 1
_TRUNCATE_LEN = 16
_BLOCK_CMT  = re.compile(r"/\*.*?\*/", re.DOTALL)
_LINE_CMT   = re.compile(r"--[^\n]*")
_WHITESPACE = re.compile(r"\s+")
_STR_LIT    = re.compile(r"'(?:[^'\\]|\\.)*'")
_NUM_LIT    = re.compile(r"\b\d+(?:\.\d+)?\b")

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
    return base64.b32encode(digest).decode("ascii").lower()[:_TRUNCATE_LEN]

# ---------------------------------------------------------------------------
# Inlined tag parser (§6b)
# ---------------------------------------------------------------------------
_ABX = re.compile(r"^/\*\s*abx\s+(?P<kv>[^*]+)\*/", re.DOTALL)

def parse_tag(sql: str):
    m = _ABX.match(sql.lstrip())
    if not m:
        return None
    try:
        return dict(kv.split("=", 1) for kv in m.group("kv").split())
    except ValueError:
        return None

# ---------------------------------------------------------------------------
# Spark session — use the active session provided by Databricks
# ---------------------------------------------------------------------------
def get_spark() -> SparkSession:
    session = SparkSession.getActiveSession()
    if session:
        return session
    return SparkSession.builder.getOrCreate()

# ---------------------------------------------------------------------------
# Hourly ingest
# ---------------------------------------------------------------------------
def _get_checkpoint(spark) -> datetime:
    try:
        row = spark.sql(
            f"SELECT MAX(start_time) AS last FROM {CATALOG}.{SCHEMA}.fingerprint_history"
        ).first()
        if row and row["last"]:
            return row["last"]
    except Exception:
        pass
    return datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)

@F.udf(returnType=StringType())
def _fingerprint_udf(sql: str) -> str:
    if not sql:
        return None
    return fingerprint(sql)


@F.udf(returnType=MapType(StringType(), StringType()))
def _parse_tag_udf(sql: str):
    if not sql:
        return {}
    result = parse_tag(sql)
    return result if result else {}


def run_ingest(spark) -> None:
    checkpoint = _get_checkpoint(spark)
    logger.info("Ingest checkpoint: %s", checkpoint.isoformat())

    new_count = spark.sql(f"""
        SELECT COUNT(*) AS n FROM {CATALOG}.{SCHEMA}.query_history_v
        WHERE CAST(start_time AS TIMESTAMP) > '{checkpoint.strftime("%Y-%m-%d %H:%M:%S")}'
          AND execution_status = 'FINISHED'
    """).first()["n"]

    logger.info("New rows to ingest: %d", new_count)
    if not new_count:
        return

    # Process entirely in Spark — statement_text never moves to the driver.
    # Fingerprinting and tag parsing run as UDFs on executors.
    (
        spark.sql(f"""
            SELECT statement_id, warehouse_id, total_duration_ms,
                   CAST(start_time AS TIMESTAMP) AS start_time,
                   statement_text
            FROM {CATALOG}.{SCHEMA}.query_history_v
            WHERE CAST(start_time AS TIMESTAMP) > '{checkpoint.strftime("%Y-%m-%d %H:%M:%S")}'
              AND execution_status = 'FINISHED'
        """)
        .withColumn("ck",      _fingerprint_udf("statement_text"))
        .withColumn("_tag",    _parse_tag_udf("statement_text"))
        .withColumn("route",   F.col("_tag")["route"])
        .withColumn("rule_id", F.col("_tag")["rule"])
        .withColumn("d",       F.to_date("start_time"))
        .withColumn("ingested_at", F.current_timestamp())
        .drop("_tag", "statement_text")
        .write.mode("append")
        .partitionBy("d")
        .saveAsTable(f"{CATALOG}.{SCHEMA}.fingerprint_history")
    )
    logger.info("Wrote %d rows to fingerprint_history", new_count)

# ---------------------------------------------------------------------------
# Daily recompute
# ---------------------------------------------------------------------------
def run_recompute(spark) -> None:
    logger.info("Recompute started")
    _recompute_waterfall(spark)
    _recompute_coverage(spark)
    _recompute_rule_effectiveness(spark)
    logger.info("Recompute complete")

def _recompute_waterfall(spark) -> None:
    df = spark.sql(f"""
        SELECT
          CAST(usage_date AS DATE)                        AS d,
          sku_name,
          usage_metadata.warehouse_id                     AS warehouse_id,
          SUM(CAST(usage_quantity AS DOUBLE))             AS dbus,
          SUM(CAST(usage_quantity AS DOUBLE)) * {DBU_RATE_USD} AS usd
        FROM {BILLING_VIEW}
        WHERE CAST(usage_date AS DATE) >= date_sub(current_date(), {LOOKBACK_DAYS})
          AND usage_metadata.warehouse_id IS NOT NULL
        GROUP BY 1, 2, 3
    """).withColumn("layer", F.lit("raw")).withColumn("updated_at", F.current_timestamp())
    df.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.waterfall_daily")
    logger.info("waterfall_daily updated")

def _recompute_coverage(spark) -> None:
    df = spark.sql(f"""
        SELECT
          CAST(date(start_time) AS DATE)                                    AS d,
          COUNT_IF(statement_text LIKE '/* abx %')                          AS gateway_routed,
          COUNT(*)                                                          AS total,
          COUNT_IF(statement_text LIKE '/* abx %') * 1.0 / COUNT(*)        AS coverage_ratio
        FROM {CATALOG}.{SCHEMA}.query_history_v
        WHERE start_time >= date_sub(current_date(), {LOOKBACK_DAYS})
        GROUP BY 1
        ORDER BY 1
    """).withColumn("updated_at", F.current_timestamp())
    df.write.mode("overwrite") \
      .option("replaceWhere", f"d >= date_sub(current_date(), {LOOKBACK_DAYS})") \
      .saveAsTable(f"{CATALOG}.{SCHEMA}.coverage_daily")
    logger.info("coverage_daily updated")

def _recompute_rule_effectiveness(spark) -> None:
    spark.sql(f"""
        WITH attributed AS (
          SELECT fh.ck,
                 COUNT(*)                                                         AS execs,
                 SUM(wc.wh_usd * (fh.total_duration_ms / NULLIF(wd.tot_ms, 0))) AS attributed_usd
          FROM {CATALOG}.{SCHEMA}.fingerprint_history fh
          JOIN (
            SELECT d, warehouse_id, SUM(CAST(usage_quantity AS DOUBLE)) * {DBU_RATE_USD} AS wh_usd
            FROM {BILLING_VIEW}
            WHERE CAST(usage_date AS DATE) >= date_sub(current_date(), {LOOKBACK_DAYS})
              AND usage_metadata.warehouse_id IS NOT NULL
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
        INSERT OVERWRITE {CATALOG}.{SCHEMA}.rule_effectiveness
        SELECT
          b.ck,
          b.rule_id,
          CONCAT(DATE_FORMAT(date_sub(current_date(), 30), 'yyyy-MM-dd'), '/',
                 DATE_FORMAT(current_date(), 'yyyy-MM-dd'))                          AS window,
          b.baseline_rate,
          c.observed,
          GREATEST(0, CAST(b.baseline_rate * 30 - c.observed AS BIGINT))            AS avoided,
          GREATEST(0.0, b.baseline_rate * 30 - c.observed)
            * COALESCE(a.attributed_usd / NULLIF(a.execs, 0), 0.0)                  AS realized_usd,
          current_timestamp()                                                        AS updated_at
        FROM baseline b
        JOIN current_window c USING (ck)
        LEFT JOIN attributed a USING (ck)
    """)
    logger.info("rule_effectiveness updated")

# ---------------------------------------------------------------------------
# API sync
# ---------------------------------------------------------------------------
def run_sync(spark) -> None:
    logger.info("API sync started")
    try:
        import requests
        from databricks.sdk import WorkspaceClient
        wc = WorkspaceClient()
        scope  = os.environ.get("AIRBRX_SECRET_SCOPE", "airbrx-secrets")
        key    = os.environ.get("AIRBRX_SECRET_KEY",   "api-key")
        encoded = wc.secrets.get_secret(scope=scope, key=key).value
        api_key = base64.b64decode(encoded).decode("utf-8")

        base_url = os.environ.get("AIRBRX_API_URL", "https://api.airbrx.ai").rstrip("/")
        headers  = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json",
                    "X-Client": "databricks-app-v1"}

        rules_resp = requests.get(f"{base_url}/v1/rules", headers=headers, timeout=15)
        rules_resp.raise_for_status()
        logger.info("Pulled %d rules", len(rules_resp.json().get("rules", [])))

        eff_rows = [
            row.asDict()
            for row in spark.sql(f"""
                SELECT rule_id, ck, window, avoided, realized_usd
                FROM {CATALOG}.{SCHEMA}.rule_effectiveness
                WHERE window = (SELECT MAX(window) FROM {CATALOG}.{SCHEMA}.rule_effectiveness)
            """).collect()
        ]
        payload = {"reported_at": datetime.now(timezone.utc).isoformat(), "effectiveness": eff_rows}
        push_resp = requests.post(f"{base_url}/v1/effectiveness", json=payload,
                                  headers=headers, timeout=15)
        push_resp.raise_for_status()
        logger.info("Pushed %d effectiveness rows", len(eff_rows))

        import pandas as pd
        spark.createDataFrame(pd.DataFrame([{
            "d": datetime.now(timezone.utc).date(),
            "ts": datetime.now(timezone.utc),
            "direction": "push_pull",
            "payload_bytes": len(str(eff_rows).encode()),
            "status": "ok",
        }])).write.mode("append").saveAsTable(f"{CATALOG}.{SCHEMA}.sync_log")
    except Exception as exc:
        logger.exception("Sync failed: %s", exc)

# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["ingest", "recompute", "sync", "all"], default="all")
    args = parser.parse_args()
    spark = get_spark()
    if args.mode in ("ingest", "all"):
        run_ingest(spark)
    if args.mode in ("recompute", "all"):
        run_recompute(spark)
    if args.mode in ("sync", "all"):
        run_sync(spark)

if __name__ == "__main__":
    main()
