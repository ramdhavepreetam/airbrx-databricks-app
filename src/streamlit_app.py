"""
Airbrx Databricks App — interactive UI (OBO auth).
Reads from SP-computed airbrx_app.state.* aggregates + query_history_self_v for drill-down.
"""
import os
import streamlit as st
from databricks.connect import DatabricksSession

CATALOG = os.environ.get("AIRBRX_STATE_CATALOG", "airbrx_app")
SCHEMA = os.environ.get("AIRBRX_STATE_SCHEMA", "state")

st.set_page_config(page_title="Airbrx — Cost Intelligence", layout="wide")


@st.cache_resource
def get_spark():
    return DatabricksSession.builder.getOrCreate()


@st.cache_data(ttl=300)
def query(_spark, sql: str):
    return _spark.sql(sql).toPandas()


spark = get_spark()

# ---------------------------------------------------------------------------
# Sidebar navigation
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

    df = query(spark, f"""
        SELECT d, gateway_routed, total, ROUND(coverage_ratio * 100, 1) AS coverage_pct
        FROM {CATALOG}.{SCHEMA}.coverage_daily
        WHERE d >= current_date() - INTERVAL {lookback} DAYS
        ORDER BY d
    """)

    if df.empty:
        st.info("No coverage data yet. Run the analysis job first.")
    else:
        col1, col2, col3 = st.columns(3)
        col1.metric("Avg Coverage", f"{df['coverage_pct'].mean():.1f}%")
        col2.metric("Total Statements", f"{df['total'].sum():,}")
        col3.metric("Gateway Routed", f"{df['gateway_routed'].sum():,}")

        import plotly.express as px
        fig = px.area(df, x="d", y="coverage_pct",
                      labels={"d": "Date", "coverage_pct": "Coverage %"},
                      title="Daily Gateway Coverage %")
        fig.update_yaxes(range=[0, 100])
        st.plotly_chart(fig, use_container_width=True)

        untagged_pct = 100 - df["coverage_pct"].mean()
        if untagged_pct > 10:
            st.warning(
                f"**{untagged_pct:.1f}% of spend is untagged** — this connection isn't routed through "
                "the Airbrx gateway yet. Route it to capture additional savings."
            )


# ---------------------------------------------------------------------------
# Screen 2 — Redundant Spend
# ---------------------------------------------------------------------------
elif page == "Redundant Spend":
    st.title("Redundant Spend")
    st.caption("Top query patterns hitting the warehouse more than once, by allocated cost.")

    df = query(spark, f"""
        SELECT ck,
               COUNT(*)                                        AS execs,
               ROUND(SUM(total_duration_ms) / 1000.0, 1)      AS total_sec,
               MIN(d)                                          AS first_seen,
               MAX(d)                                          AS last_seen,
               COALESCE(MAX(rule_id), 'unmatched')            AS rule_id
        FROM {CATALOG}.{SCHEMA}.fingerprint_history
        WHERE d >= current_date() - INTERVAL {lookback} DAYS
        GROUP BY ck
        HAVING COUNT(*) > 1
        ORDER BY execs DESC
        LIMIT 50
    """)

    if df.empty:
        st.info("No redundant queries detected in this window.")
    else:
        st.metric("Redundant fingerprints", len(df))
        st.dataframe(
            df.rename(columns={
                "ck": "Fingerprint",
                "execs": "Executions",
                "total_sec": "Total Duration (s)",
                "first_seen": "First Seen",
                "last_seen": "Last Seen",
                "rule_id": "Rule",
            }),
            use_container_width=True,
        )


# ---------------------------------------------------------------------------
# Screen 3 — Projected vs. Realized
# ---------------------------------------------------------------------------
elif page == "Projected vs. Realized":
    st.title("Projected vs. Realized Savings")

    waterfall = query(spark, f"""
        SELECT d, sku_name, SUM(usd) AS usd, layer
        FROM {CATALOG}.{SCHEMA}.waterfall_daily
        WHERE d >= current_date() - INTERVAL {lookback} DAYS
        GROUP BY d, sku_name, layer
        ORDER BY d
    """)

    effectiveness = query(spark, f"""
        SELECT SUM(realized_usd) AS total_realized, SUM(avoided) AS total_avoided
        FROM {CATALOG}.{SCHEMA}.rule_effectiveness
        WHERE window LIKE '%' || DATE_FORMAT(current_date() - INTERVAL {lookback} DAYS, 'yyyy-MM-dd') || '%'
    """)

    if waterfall.empty:
        st.info("No waterfall data yet. Run the daily recompute job first.")
    else:
        total_usd = waterfall[waterfall["layer"] == "raw"]["usd"].sum()
        realized = effectiveness["total_realized"].iloc[0] if not effectiveness.empty else 0

        col1, col2, col3 = st.columns(3)
        col1.metric("Total Warehouse Spend", f"${total_usd:,.0f}")
        col2.metric("Realized Savings", f"${realized:,.0f}" if realized else "Pending baseline")
        col3.metric("Savings Rate", f"{(realized/total_usd*100):.1f}%" if total_usd and realized else "—")

        import plotly.express as px
        fig = px.bar(
            waterfall[waterfall["layer"] == "raw"],
            x="d", y="usd", color="sku_name",
            labels={"d": "Date", "usd": "USD", "sku_name": "SKU"},
            title="Daily Warehouse Spend by SKU",
        )
        st.plotly_chart(fig, use_container_width=True)

        if not realized:
            st.info(
                "Realized savings require at least one full pre/post deployment window "
                "(30+ days of baseline data). Check back after the gateway has been running."
            )


# ---------------------------------------------------------------------------
# Screen 4 — Per-Rule Attribution
# ---------------------------------------------------------------------------
elif page == "Per-Rule Attribution":
    st.title("Per-Rule Attribution")
    st.caption("Realized savings per Airbrx rule. Rules with no recent earnings may be stale.")

    df = query(spark, f"""
        SELECT
          re.rule_id,
          ru.description,
          re.window,
          re.baseline_rate,
          re.observed,
          re.avoided,
          ROUND(re.realized_usd, 2)  AS realized_usd,
          re.updated_at,
          recent.last_active
        FROM {CATALOG}.{SCHEMA}.rule_effectiveness re
        LEFT JOIN {CATALOG}.{SCHEMA}.rules ru
          ON re.rule_id = ru.rule_id
        LEFT JOIN (
          SELECT rule_id, MAX(d) AS last_active
          FROM {CATALOG}.{SCHEMA}.fingerprint_history
          WHERE d >= date_sub(current_date(), 7)
          GROUP BY rule_id
        ) recent ON re.rule_id = recent.rule_id
        ORDER BY re.realized_usd DESC NULLS LAST
    """)

    if df.empty:
        st.info("No rule attribution data yet.")
    else:
        stale = df[df["realized_usd"].fillna(0) == 0]
        if not stale.empty:
            st.warning(
                f"{len(stale)} rule(s) have zero recent realized savings — "
                "the underlying query pattern may have changed or the dashboard was retired."
            )
        st.dataframe(
            df.rename(columns={
                "rule_id": "Rule ID",
                "description": "Description",
                "window": "Window",
                "baseline_rate": "Baseline Rate (exec/day)",
                "observed": "Observed Execs",
                "avoided": "Avoided Execs",
                "realized_usd": "Realized Savings ($)",
                "updated_at": "Last Updated",
            }),
            use_container_width=True,
        )
