"""
Airbrx Databricks App — interactive UI.
Uses databricks-sdk StatementExecution API — auto-authenticates inside a Databricks App
without needing DATABRICKS_TOKEN to be set explicitly.
"""
import os
import time
import pandas as pd
import streamlit as st
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

CATALOG = os.environ.get("AIRBRX_STATE_CATALOG", "airbrx_app")
SCHEMA  = os.environ.get("AIRBRX_STATE_SCHEMA",  "state")
WH_ID   = os.environ.get("DATABRICKS_WAREHOUSE_ID", "771ee5c628aafe42")

st.set_page_config(page_title="Airbrx — Cost Intelligence", layout="wide")


@st.cache_resource
def get_client():
    return WorkspaceClient()


def run_query(sql_text: str) -> pd.DataFrame:
    try:
        w = get_client()
        resp = w.statement_execution.execute_statement(
            warehouse_id=WH_ID,
            statement=sql_text,
            wait_timeout="50s",
        )
        # Poll until done
        for _ in range(30):
            if resp.status.state in (
                StatementState.SUCCEEDED, StatementState.FAILED,
                StatementState.CANCELED, StatementState.CLOSED,
            ):
                break
            resp = w.statement_execution.get_statement(resp.statement_id)
            time.sleep(2)

        if resp.status.state != StatementState.SUCCEEDED:
            st.error(f"Query failed: {resp.status.error}")
            return pd.DataFrame()

        if not resp.result or not resp.result.data_array:
            return pd.DataFrame()

        cols = [c.name for c in resp.manifest.schema.columns]
        return pd.DataFrame(resp.result.data_array, columns=cols)
    except Exception as e:
        st.error(f"Query error: {e}")
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
st.sidebar.title("Airbrx")
page = st.sidebar.radio(
    "View",
    ["Coverage", "Redundant Spend", "Projected vs. Realized", "Per-Rule Attribution"],
)
lookback = st.sidebar.slider("Lookback days", min_value=7, max_value=90, value=30, step=7)
st.sidebar.markdown("---")
st.sidebar.caption("Costs allocated by query duration share. Cache hits are invisible by design.")


# ---------------------------------------------------------------------------
# Screen 1 — Coverage
# ---------------------------------------------------------------------------
if page == "Coverage":
    st.title("Gateway Coverage")
    st.caption("Percentage of warehouse traffic routed through Airbrx.")

    with st.spinner("Loading..."):
        df = run_query(f"""
            SELECT d, gateway_routed, total,
                   ROUND(coverage_ratio * 100, 1) AS coverage_pct
            FROM {CATALOG}.{SCHEMA}.coverage_daily
            WHERE d >= date_sub(current_date(), {lookback})
            ORDER BY d
        """)

    if df.empty:
        st.info("No coverage data yet — the analysis job is still populating this.")
    else:
        df["coverage_pct"] = pd.to_numeric(df["coverage_pct"], errors="coerce")
        df["total"]         = pd.to_numeric(df["total"],         errors="coerce")
        df["gateway_routed"]= pd.to_numeric(df["gateway_routed"],errors="coerce")

        col1, col2, col3 = st.columns(3)
        col1.metric("Avg Coverage",      f"{df['coverage_pct'].mean():.1f}%")
        col2.metric("Total Statements",  f"{int(df['total'].sum()):,}")
        col3.metric("Gateway Routed",    f"{int(df['gateway_routed'].sum()):,}")

        import plotly.express as px
        fig = px.area(df, x="d", y="coverage_pct",
                      labels={"d": "Date", "coverage_pct": "Coverage %"},
                      title="Daily Gateway Coverage %")
        fig.update_yaxes(range=[0, 100])
        st.plotly_chart(fig, use_container_width=True)

        untagged = 100 - df["coverage_pct"].mean()
        if untagged > 10:
            st.warning(
                f"**{untagged:.1f}% of spend is untagged** — this connection isn't routed "
                "through the Airbrx gateway yet."
            )


# ---------------------------------------------------------------------------
# Screen 2 — Redundant Spend
# ---------------------------------------------------------------------------
elif page == "Redundant Spend":
    st.title("Redundant Spend")
    st.caption("Top query patterns hitting the warehouse more than once.")

    with st.spinner("Loading..."):
        df = run_query(f"""
            SELECT ck,
                   COUNT(*)                                   AS execs,
                   ROUND(SUM(total_duration_ms)/1000.0, 1)   AS total_sec,
                   MIN(d)                                     AS first_seen,
                   MAX(d)                                     AS last_seen,
                   COALESCE(MAX(rule_id), 'unmatched')        AS rule_id
            FROM {CATALOG}.{SCHEMA}.fingerprint_history
            WHERE d >= date_sub(current_date(), {lookback})
            GROUP BY ck
            HAVING COUNT(*) > 1
            ORDER BY execs DESC
            LIMIT 50
        """)

    if df.empty:
        st.info("No redundant queries yet — analysis job may still be running.")
    else:
        df["execs"] = pd.to_numeric(df["execs"], errors="coerce")
        st.metric("Redundant fingerprints", len(df))
        st.dataframe(
            df.rename(columns={
                "ck": "Fingerprint", "execs": "Executions",
                "total_sec": "Total Duration (s)", "first_seen": "First Seen",
                "last_seen": "Last Seen", "rule_id": "Rule",
            }),
            use_container_width=True,
        )


# ---------------------------------------------------------------------------
# Screen 3 — Projected vs. Realized
# ---------------------------------------------------------------------------
elif page == "Projected vs. Realized":
    st.title("Projected vs. Realized Savings")

    with st.spinner("Loading..."):
        waterfall = run_query(f"""
            SELECT d, sku_name, SUM(usd) AS usd, layer
            FROM {CATALOG}.{SCHEMA}.waterfall_daily
            WHERE d >= date_sub(current_date(), {lookback})
            GROUP BY d, sku_name, layer ORDER BY d
        """)
        effectiveness = run_query(f"""
            SELECT COALESCE(SUM(realized_usd), 0) AS total_realized
            FROM {CATALOG}.{SCHEMA}.rule_effectiveness
        """)

    if waterfall.empty:
        st.info("No waterfall data yet — run the analysis job first.")
    else:
        waterfall["usd"] = pd.to_numeric(waterfall["usd"], errors="coerce")
        raw = waterfall[waterfall["layer"] == "raw"]
        total_usd = raw["usd"].sum() if not raw.empty else 0
        realized  = pd.to_numeric(
            effectiveness["total_realized"].iloc[0], errors="coerce"
        ) if not effectiveness.empty else 0

        col1, col2, col3 = st.columns(3)
        col1.metric("Total Warehouse Spend", f"${total_usd:,.0f}")
        col2.metric("Realized Savings", f"${realized:,.0f}" if realized else "Pending baseline")
        col3.metric("Savings Rate",
                    f"{(realized/total_usd*100):.1f}%" if total_usd and realized else "—")

        import plotly.express as px
        if not raw.empty:
            fig = px.bar(raw, x="d", y="usd", color="sku_name",
                         labels={"d": "Date", "usd": "USD", "sku_name": "SKU"},
                         title="Daily Warehouse Spend by SKU")
            st.plotly_chart(fig, use_container_width=True)

        if not realized:
            st.info("Realized savings require 30+ days of baseline data.")


# ---------------------------------------------------------------------------
# Screen 4 — Per-Rule Attribution
# ---------------------------------------------------------------------------
elif page == "Per-Rule Attribution":
    st.title("Per-Rule Attribution")
    st.caption("Realized savings per Airbrx rule.")

    with st.spinner("Loading..."):
        df = run_query(f"""
            SELECT re.rule_id, ru.description, re.window,
                   re.baseline_rate, re.observed, re.avoided,
                   ROUND(re.realized_usd, 2) AS realized_usd,
                   re.updated_at
            FROM {CATALOG}.{SCHEMA}.rule_effectiveness re
            LEFT JOIN {CATALOG}.{SCHEMA}.rules ru ON re.rule_id = ru.rule_id
            ORDER BY re.realized_usd DESC NULLS LAST
        """)

    if df.empty:
        st.info("No rule attribution data yet.")
    else:
        st.dataframe(
            df.rename(columns={
                "rule_id": "Rule ID", "description": "Description",
                "window": "Window", "baseline_rate": "Baseline (exec/day)",
                "observed": "Observed", "avoided": "Avoided",
                "realized_usd": "Realized ($)", "updated_at": "Updated",
            }),
            use_container_width=True,
        )
