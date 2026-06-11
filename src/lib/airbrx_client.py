"""
Airbrx API client. Reads the API key from a Databricks secret scope.
Only aggregates and rule IDs cross the boundary — no query text, no raw rows.
"""
import os
import logging
from datetime import datetime, timezone
from typing import Any

import requests
from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)

_DEFAULT_TIMEOUT = 15  # seconds


class AirbrxClient:
    def __init__(self, workspace_client: WorkspaceClient | None = None):
        self._wc = workspace_client or WorkspaceClient()
        self._scope = os.environ.get("AIRBRX_SECRET_SCOPE", "airbrx-secrets")
        self._key = os.environ.get("AIRBRX_SECRET_KEY", "api-key")
        self._base_url = os.environ.get("AIRBRX_API_URL", "https://api.airbrx.ai").rstrip("/")
        self._api_key: str | None = None

    def _get_api_key(self) -> str:
        if self._api_key is None:
            import base64
            # Databricks REST API returns secret value as base64-encoded bytes
            encoded = self._wc.secrets.get_secret(
                scope=self._scope, key=self._key
            ).value
            self._api_key = base64.b64decode(encoded).decode("utf-8")
        return self._api_key

    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self._get_api_key()}",
            "Content-Type": "application/json",
            "X-Client": "databricks-app-v1",
        }

    def pull_rules(self) -> list[dict]:
        """Fetch current cache/bypass rule definitions from Airbrx cloud."""
        resp = requests.get(
            f"{self._base_url}/v1/rules",
            headers=self._headers(),
            timeout=_DEFAULT_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json().get("rules", [])

    def push_effectiveness(self, rows: list[dict[str, Any]]) -> None:
        """
        Push aggregate rule effectiveness stats. Payload shape:
        [{"rule_id": str, "ck": str, "window": str, "avoided": int, "realized_usd": float}]
        No query text or raw user data included.
        """
        payload = {
            "reported_at": datetime.now(timezone.utc).isoformat(),
            "effectiveness": rows,
        }
        resp = requests.post(
            f"{self._base_url}/v1/effectiveness",
            json=payload,
            headers=self._headers(),
            timeout=_DEFAULT_TIMEOUT,
        )
        resp.raise_for_status()

    def sync(self, spark, state_catalog: str, state_schema: str) -> None:
        """Pull rules and push aggregates; log result to sync_log."""
        ts = datetime.now(timezone.utc)
        status = "ok"
        payload_bytes = 0
        try:
            rules = self.pull_rules()
            _upsert_rules(spark, rules, state_catalog, state_schema)

            effectiveness_rows = _read_effectiveness(spark, state_catalog, state_schema)
            self.push_effectiveness(effectiveness_rows)
            payload_bytes = len(str(effectiveness_rows).encode())
        except Exception as exc:
            status = f"error: {exc}"
            logger.exception("Airbrx sync failed")
            raise
        finally:
            _log_sync(spark, ts, status, payload_bytes, state_catalog, state_schema)


def _upsert_rules(spark, rules: list[dict], catalog: str, schema: str) -> None:
    if not rules:
        return
    import pandas as pd
    df = spark.createDataFrame(pd.DataFrame(rules))
    df.createOrReplaceTempView("_incoming_rules")
    spark.sql(f"""
        MERGE INTO {catalog}.{schema}.rules AS target
        USING _incoming_rules AS source
        ON target.rule_id = source.rule_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


def _read_effectiveness(spark, catalog: str, schema: str) -> list[dict]:
    df = spark.sql(f"""
        SELECT rule_id, ck, window, avoided, realized_usd
        FROM {catalog}.{schema}.rule_effectiveness
        WHERE window = (SELECT MAX(window) FROM {catalog}.{schema}.rule_effectiveness)
    """)
    return [row.asDict() for row in df.collect()]


def _log_sync(spark, ts, status: str, payload_bytes: int, catalog: str, schema: str) -> None:
    import pandas as pd
    row = pd.DataFrame([{
        "d": ts.date(),
        "ts": ts,
        "direction": "push_pull",
        "payload_bytes": payload_bytes,
        "status": status,
    }])
    spark.createDataFrame(row).write.mode("append").saveAsTable(
        f"{catalog}.{schema}.sync_log"
    )
