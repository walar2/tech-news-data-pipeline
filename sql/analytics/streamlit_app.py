"""Streamlit SQL analytics viewer for the Tech News warehouse."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import psycopg2
import streamlit as st



ANALYTICS_DIR = Path(__file__).resolve().parent


QUERY_FILES = {
    "01 — Top Stories": "01_top_stories.sql",
    "02 — Trending Domains": "02_trending_domains.sql",
    "03 — Viral Velocity": "03_viral_velocity.sql",
    "04 — Tag Popularity Over Time": "04_tag_popularity_over_time.sql",
    "05 — Author Leaderboard": "05_author_leaderboard.sql",
    "06 — Hourly Activity Heatmap": "06_hourly_activity_heatmap.sql",
    "07 — Cross-Platform Topic Overlap": "07_cross_platform_topic_overlap.sql",
    "08 — Anomaly Detection": "08_anomaly_detection.sql",
}


def get_connection():
    """Create a PostgreSQL connection to the local tech_news warehouse."""

    return psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="tech_news",
        user="airflow",
        password="airflow",
    )


def load_sql(file_name: str) -> str:
    """Load a SQL file from the analytics folder."""

    sql_path = ANALYTICS_DIR / file_name

    if not sql_path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")

    return sql_path.read_text(encoding="utf-8")


def run_query(sql: str) -> pd.DataFrame:
    """Run SQL and return the result as a pandas DataFrame."""

    with get_connection() as conn:
        return pd.read_sql_query(sql, conn)


st.set_page_config(
    page_title="Tech News SQL Analytics",
    layout="wide",
)

st.title("Tech News SQL Analytics")
st.caption("Run analytics SQL files against the local PostgreSQL warehouse.")

selected_query = st.sidebar.selectbox(
    "Choose query",
    list(QUERY_FILES.keys()),
)

sql_file = QUERY_FILES[selected_query]
sql_text = load_sql(sql_file)

st.subheader(selected_query)

with st.expander("View SQL", expanded=False):
    st.code(sql_text, language="sql")

if st.button("Run query", type="primary"):
    try:
        df = run_query(sql_text)

        st.success(f"Query completed. Rows returned: {len(df)}")

        st.dataframe(
            df,
            use_container_width=True,
            hide_index=True,
        )

        csv_data = df.to_csv(index=False).encode("utf-8")

        st.download_button(
            label="Download result as CSV",
            data=csv_data,
            file_name=sql_file.replace(".sql", "_result.csv"),
            mime="text/csv",
        )

    except Exception as error:
        st.error(f"Query failed: {type(error).__name__}: {error}")