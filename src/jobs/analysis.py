"""
Airbrx analysis job — runs as the app service principal.

Modes:
  --mode ingest     Hourly: pull new query_history rows → fingerprint_history
  --mode recompute  Daily:  rebuild waterfall, coverage, redundancy, rule_effectiveness
  --mode sync       On-demand: pull rules + push effectiveness to api.airbrx.ai
  --mode all        Run all three in order (default)
"""
import argparse
import logging
import os
import sys

# Add src/ to path so sibling lib/ package is importable when job runs from src/jobs/
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from datetime import datetime, timezone, timedelta
from pyspark.sql import SparkSession, functions as F

from lib.fingerprint import fingerprint
from lib.tags import parse_tag
from lib.queries import INCREMENTAL_HISTORY, PRICED_USAGE, COVERAGE, ATTRIBUTED_COST
from lib.airbrx_client import AirbrxClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

CATALOG = os.environ.get("AIRBRX_STATE_CATALOG", "airbrx_app")
SCHEMA = os.environ.get("AIRBRX_STATE_SCHEMA", "state")
BILLING_VIEW = os.environ.get("AIRBRX_BILLING_VIEW", "airbrx_app.state.billing_usage_v")
DBU_RATE_USD = float(os.environ.get("DBU_RATE_USD", "0.70"))
LOOKBACK_DAYS = 30


def get_spark() -> SparkSession:
    # Inside a Databricks job the session already exists — get it directly.
    # Fall back to DatabricksSession for local dev only.
    session = SparkSession.getActiveSession()
    if session:
        return session
    try:
        from databricks.connect import DatabricksSession
        return DatabricksSession.builder.getOrCreate()
    except Exception:
        return SparkSession.builder.getOrCreate()


# ---------------------------------------------------------------------------
# Hourly ingest
# ---------------------------------------------------------------------------

def _get_checkpoint(spark) -> datetime:
    """Return the max start_time already ingested, or LOOKBACK_DAYS ago if table is empty."""
    try:
        row = spark.sql(
            f"SELECT MAX(start_time) AS last FROM {CATALOG}.{SCHEMA}.fingerprint_history"
        ).first()
        if row and row["last"]:
            return row["last"]
    except Exception:
        pass
    return datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)


def run_ingest(spark) -> None:
    checkpoint = _get_checkpoint(spark)
    logger.info("Incremental ingest from %s", checkpoint.isoformat())

    sql = INCREMENTAL_HISTORY.format(
        state_catalog=CATALOG,
        state_schema=SCHEMA,
        checkpoint_ts=checkpoint.strftime("%Y-%m-%d %H:%M:%S"),
    )
    rows = spark.sql(sql).collect()
    logger.info("Fetched %d new rows from query_history_v", len(rows))

    if not rows:
        return

    now = datetime.now(timezone.utc)
    records = []
    for row in rows:
        stmt_text = row["statement_text"] or ""
        tag = parse_tag(stmt_text)
        ck = fingerprint(stmt_text)
        start_time = row["start_time"]
        records.append({
            "ck": ck,
            "statement_id": row["statement_id"],
            "d": start_time.date() if start_time else None,
            "start_time": start_time,
            "warehouse_id": row["warehouse_id"],
            "total_duration_ms": row["total_duration_ms"],
            "route": tag.get("route") if tag else None,
            "rule_id": tag.get("rule") if tag else None,
            "ingested_at": now,
        })

    import pandas as pd
    df = spark.createDataFrame(pd.DataFrame(records))
    df.write.mode("append").saveAsTable(f"{CATALOG}.{SCHEMA}.fingerprint_history")
    logger.info("Wrote %d rows to fingerprint_history", len(records))


# ---------------------------------------------------------------------------
# Daily recompute
# ---------------------------------------------------------------------------

def run_recompute(spark) -> None:
    logger.info("Daily recompute started")
    _recompute_waterfall(spark)
    _recompute_coverage(spark)
    _recompute_rule_effectiveness(spark)
    logger.info("Daily recompute complete")


def _recompute_waterfall(spark) -> None:
    sql = PRICED_USAGE.format(
        billing_view=BILLING_VIEW,
        dbu_rate_usd=DBU_RATE_USD,
        lookback_days=LOOKBACK_DAYS,
    )
    df = (
        spark.sql(sql)
        .withColumn("layer", F.lit("raw"))
        .withColumn("updated_at", F.current_timestamp())
    )
    df.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.waterfall_daily")
    logger.info("waterfall_daily updated")


def _recompute_coverage(spark) -> None:
    sql = COVERAGE.format(
        state_catalog=CATALOG,
        state_schema=SCHEMA,
        lookback_days=LOOKBACK_DAYS,
    )
    df = spark.sql(sql).withColumn("updated_at", F.current_timestamp())
    df.write \
        .mode("overwrite") \
        .option("replaceWhere", f"d >= date_sub(current_date(), {LOOKBACK_DAYS})") \
        .saveAsTable(f"{CATALOG}.{SCHEMA}.coverage_daily")
    logger.info("coverage_daily updated")


def _recompute_rule_effectiveness(spark) -> None:
    # Step 1: attributed cost per fingerprint (§5b) — used to derive cost_per_exec
    attr_sql = ATTRIBUTED_COST.format(
        billing_view=BILLING_VIEW,
        dbu_rate_usd=DBU_RATE_USD,
        state_catalog=CATALOG,
        state_schema=SCHEMA,
        lookback_days=LOOKBACK_DAYS,
    )
    spark.sql(attr_sql).createOrReplaceTempView("_attr_cost")

    # Step 2: baseline (pre-window [-60,-31]) vs current (last 30 days) + realized savings
    spark.sql(f"""
        WITH baseline AS (
          SELECT ck, rule_id,
                 COUNT(*) / 30.0 AS baseline_rate
          FROM {CATALOG}.{SCHEMA}.fingerprint_history
          WHERE d BETWEEN date_sub(current_date(), 60)
                      AND date_sub(current_date(), 31)
          GROUP BY ck, rule_id
        ),
        current_window AS (
          SELECT ck, COUNT(*) AS observed
          FROM {CATALOG}.{SCHEMA}.fingerprint_history
          WHERE d >= date_sub(current_date(), 30)
          GROUP BY ck
        ),
        cost_per_exec AS (
          SELECT ck,
                 attributed_usd / NULLIF(execs, 0) AS cpp
          FROM _attr_cost
          WHERE execs > 0
        )
        INSERT OVERWRITE {CATALOG}.{SCHEMA}.rule_effectiveness
        SELECT
          b.ck,
          b.rule_id,
          CONCAT(
            DATE_FORMAT(date_sub(current_date(), 30), 'yyyy-MM-dd'),
            '/',
            DATE_FORMAT(current_date(), 'yyyy-MM-dd')
          )                                                                AS window,
          b.baseline_rate,
          c.observed,
          GREATEST(0, CAST(b.baseline_rate * 30 - c.observed AS BIGINT)) AS avoided,
          GREATEST(0.0, b.baseline_rate * 30 - c.observed)
            * COALESCE(cpe.cpp, 0.0)                                      AS realized_usd,
          current_timestamp()                                              AS updated_at
        FROM baseline b
        JOIN current_window c USING (ck)
        LEFT JOIN cost_per_exec cpe USING (ck)
    """)
    spark.catalog.dropTempView("_attr_cost")
    logger.info("rule_effectiveness updated")


# ---------------------------------------------------------------------------
# API sync
# ---------------------------------------------------------------------------

def run_sync(spark) -> None:
    logger.info("Airbrx API sync started")
    client = AirbrxClient()
    client.sync(spark, CATALOG, SCHEMA)
    logger.info("Airbrx API sync complete")


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["ingest", "recompute", "sync", "all"],
                        default="all")
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
