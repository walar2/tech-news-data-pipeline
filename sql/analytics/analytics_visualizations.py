from __future__ import annotations

import os
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import psycopg2


ANALYTICS_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = ANALYTICS_DIR / "outputs"

QUERY_FILES = {
    "01_top_stories": "01_top_stories.sql",
    "02_trending_domains": "02_trending_domains.sql",
    "03_viral_velocity": "03_viral_velocity.sql",
    "04_tag_popularity_over_time": "04_tag_popularity_over_time.sql",
    "05_author_leaderboard": "05_author_leaderboard.sql",
    "06_hourly_activity_heatmap": "06_hourly_activity_heatmap.sql",
    "07_cross_platform_topic_overlap": "07_cross_platform_topic_overlap.sql",
    "08_anomaly_detection": "08_anomaly_detection.sql",
}


def get_connection():
    """Create a PostgreSQL connection to the local tech_news warehouse."""
    return psycopg2.connect(
        host=os.getenv("TECH_NEWS_DB_HOST", "localhost"),
        port=int(os.getenv("TECH_NEWS_DB_PORT", "5432")),
        dbname=os.getenv("TECH_NEWS_DB_NAME", "tech_news"),
        user=os.getenv("TECH_NEWS_DB_USER", "airflow"),
        password=os.getenv("TECH_NEWS_DB_PASSWORD", "airflow"),
    )


def load_sql(file_name: str) -> str:
    sql_path = ANALYTICS_DIR / file_name
    return sql_path.read_text(encoding="utf-8")


def short_label(value: object, max_length: int = 45) -> str:
    text = "" if pd.isna(value) else str(value)
    return text if len(text) <= max_length else text[: max_length - 3] + "..."


def to_number(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(0)


def scalar_to_float(value: object) -> float:
    if value is None or pd.isna(value):
        return 0.0
    return float(value)


def save_no_data_chart(
    output_path: Path,
    title: str,
    message: str = "No data returned by this query.",
) -> None:
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.text(0.5, 0.5, message, ha="center", va="center", fontsize=12)
    ax.set_title(title)
    ax.set_axis_off()
    fig.tight_layout()
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)


def save_chart(fig, output_path: Path) -> None:
    fig.tight_layout()
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)


def plot_top_stories(df: pd.DataFrame, output_path: Path) -> None:
    title = "Top HackerNews Stories by Score"
    if df.empty:
        save_no_data_chart(output_path, title)
        return

    data = df.head(20).copy()
    data["score"] = to_number(data["score"])
    data["story_label"] = data["title"].apply(short_label)

    fig, ax = plt.subplots(figsize=(12, 8))
    ax.barh(data["story_label"], data["score"])
    ax.invert_yaxis()
    ax.set_title(title)
    ax.set_xlabel("Score")
    ax.set_ylabel("Story")
    save_chart(fig, output_path)


def plot_trending_domains(df: pd.DataFrame, output_path: Path) -> None:
    title = "Trending HackerNews Domains by Share Count"
    if df.empty:
        save_no_data_chart(output_path, title)
        return

    data = df.head(20).copy()
    data["share_count"] = to_number(data["share_count"])

    fig, ax = plt.subplots(figsize=(11, 7))
    ax.barh(data["domain"], data["share_count"])
    ax.invert_yaxis()
    ax.set_title(title)
    ax.set_xlabel("Share count")
    ax.set_ylabel("Domain")
    save_chart(fig, output_path)


def plot_viral_velocity(df: pd.DataFrame, output_path: Path) -> None:
    title = "Viral Velocity: Score Growth per Hour"
    if df.empty:
        save_no_data_chart(
            output_path,
            title,
            "No repeated snapshots available for viral velocity yet.",
        )
        return

    data = df.head(10).copy()
    metric = "score_growth_per_hour" if "score_growth_per_hour" in data.columns else "score_growth"
    data[metric] = to_number(data[metric])
    data["story_label"] = data["title"].apply(short_label)

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.barh(data["story_label"], data[metric])
    ax.invert_yaxis()
    ax.set_title(title)
    ax.set_xlabel(metric.replace("_", " ").title())
    ax.set_ylabel("Story")
    save_chart(fig, output_path)


def plot_tag_popularity(df: pd.DataFrame, output_path: Path) -> None:
    title = "Dev.to Tag Popularity Over Time"
    if df.empty:
        save_no_data_chart(output_path, title)
        return

    week_columns = [
        "four_weeks_ago_reactions",
        "three_weeks_ago_reactions",
        "two_weeks_ago_reactions",
        "previous_week_reactions",
        "current_week_reactions",
    ]
    week_labels = [
        "4 weeks ago",
        "3 weeks ago",
        "2 weeks ago",
        "Previous week",
        "Current week",
    ]

    data = df.head(10).copy()

    fig, ax = plt.subplots(figsize=(12, 7))
    for _, row in data.iterrows():
        values = [scalar_to_float(row.get(column, 0)) for column in week_columns]
        ax.plot(
            week_labels,
            values,
            marker="o",
            label=short_label(row["tag_name"], 20),
        )

    ax.set_title(title)
    ax.set_xlabel("Week")
    ax.set_ylabel("Reactions")
    ax.legend(fontsize=8)
    save_chart(fig, output_path)


def plot_author_leaderboard(df: pd.DataFrame, output_path: Path) -> None:
    title = "Author Leaderboard by Engagement Score"
    if df.empty:
        save_no_data_chart(output_path, title)
        return

    data = df.head(20).copy()
    data["engagement_score"] = to_number(data["engagement_score"])
    data["author_label"] = data["author"].apply(short_label)

    fig, ax = plt.subplots(figsize=(11, 8))
    ax.barh(data["author_label"], data["engagement_score"])
    ax.invert_yaxis()
    ax.set_title(title)
    ax.set_xlabel("Engagement score")
    ax.set_ylabel("Author")
    save_chart(fig, output_path)


def plot_hourly_activity_heatmap(df: pd.DataFrame, output_path: Path) -> None:
    title = "Hourly Activity Heatmap: Posts by Day and Hour UTC"
    if df.empty:
        save_no_data_chart(output_path, title)
        return

    data = df.copy()
    data["day_of_week_number"] = to_number(data["day_of_week_number"]).astype(int)
    data["hour_utc"] = to_number(data["hour_utc"]).astype(int)
    data["post_count"] = to_number(data["post_count"])

    pivot = data.pivot_table(
        index="day_of_week_number",
        columns="hour_utc",
        values="post_count",
        aggfunc="sum",
        fill_value=0,
    )

    pivot = pivot.reindex(
        index=list(range(7)),
        columns=list(range(24)),
        fill_value=0,
    )

    day_labels = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]

    fig, ax = plt.subplots(figsize=(14, 6))
    image = ax.imshow(pivot.values, aspect="auto")
    ax.set_title(title)
    ax.set_xlabel("Hour UTC")
    ax.set_ylabel("Day of week")
    ax.set_xticks(range(24))
    ax.set_yticks(range(7))
    ax.set_yticklabels(day_labels)
    fig.colorbar(image, ax=ax, label="Post count")
    save_chart(fig, output_path)


def plot_topic_overlap(df: pd.DataFrame, output_path: Path) -> None:
    title = "Cross-Platform Topic Overlap"
    if df.empty:
        save_no_data_chart(
            output_path,
            title,
            "No overlapping keywords found for the selected period.",
        )
        return

    data = df.head(20).copy()
    data["combined_mentions"] = to_number(data["combined_mentions"])

    fig, ax = plt.subplots(figsize=(11, 7))
    ax.barh(data["keyword"], data["combined_mentions"])
    ax.invert_yaxis()
    ax.set_title(title)
    ax.set_xlabel("Combined mentions")
    ax.set_ylabel("Keyword")
    save_chart(fig, output_path)


def plot_anomaly_detection(df: pd.DataFrame, output_path: Path) -> None:
    title = "Story Volume Anomaly Detection"
    if df.empty:
        save_no_data_chart(
            output_path,
            title,
            "No anomalous days detected in the past 30 days.",
        )
        return

    data = df.copy()
    data["post_date"] = pd.to_datetime(data["post_date"]).dt.strftime("%Y-%m-%d")

    metric = "z_score" if "z_score" in data.columns else "story_count"
    data[metric] = to_number(data[metric])

    fig, ax = plt.subplots(figsize=(11, 6))
    ax.bar(data["post_date"], data[metric])
    ax.set_title(title)
    ax.set_xlabel("Post date")
    ax.set_ylabel(metric.replace("_", " ").title())
    ax.tick_params(axis="x", rotation=45)
    save_chart(fig, output_path)


PLOTTERS = {
    "01_top_stories": plot_top_stories,
    "02_trending_domains": plot_trending_domains,
    "03_viral_velocity": plot_viral_velocity,
    "04_tag_popularity_over_time": plot_tag_popularity,
    "05_author_leaderboard": plot_author_leaderboard,
    "06_hourly_activity_heatmap": plot_hourly_activity_heatmap,
    "07_cross_platform_topic_overlap": plot_topic_overlap,
    "08_anomaly_detection": plot_anomaly_detection,
}


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    with get_connection() as conn:
        for query_name, file_name in QUERY_FILES.items():
            print(f"Running {file_name}...")

            sql = load_sql(file_name)
            df = pd.read_sql_query(sql, conn)

            csv_path = OUTPUT_DIR / f"{query_name}.csv"
            png_path = OUTPUT_DIR / f"{query_name}.png"

            df.to_csv(csv_path, index=False)
            PLOTTERS[query_name](df, png_path)

            print(f"Saved CSV: {csv_path}")
            print(f"Saved chart: {png_path}")

    print("All analytics queries and visualizations completed.")


if __name__ == "__main__":
    main()