# Tech News Data Pipeline

A local end-to-end data engineering project that ingests technology news from HackerNews and Dev.to, stores the raw API responses, transforms the data into analytics-ready warehouse tables, runs daily data quality checks, and sends a daily Telegram report with an attached dataset.

The project is orchestrated with Apache Airflow and uses PostgreSQL as the local data warehouse.

## Project Overview

This pipeline collects technology news data from two sources:

- HackerNews API
- Dev.to API

The data is processed through a Bronze → Silver → Gold warehouse pattern:

- Bronze stores raw API payloads.
- Silver stores cleaned and validated records.
- Gold stores dimensional models, fact tables, snapshots, and daily report outputs.

The project also includes:

- Airflow DAG orchestration
- XCom usage between DAG tasks
- TaskGroups for organized task structure
- Custom Airflow hook for data quality and reporting snapshot logic
- Daily data quality checks
- Email alerting for quality failures
- Telegram report delivery
- SQL analytics queries
- Streamlit SQL analytics viewer

## Architecture Summary

The pipeline contains four main DAGs:

| DAG | File | Purpose | Schedule |
|---|---|---|---|
| DAG 1 | `hackernews_hourly_pipeline.py` | Fetches HackerNews top/best stories, validates, loads Bronze/Silver/Gold | `@hourly` |
| DAG 2 | `devto_daily_pipeline.py` | Fetches Dev.to articles, validates, loads Bronze/Silver/Gold | `@daily` |
| DAG 3 | `data_quality_checks.py` | Checks freshness, row count anomaly, null rates, sends email alert, creates report snapshot tables | `@daily` |
| DAG 4 | `telegram_daily_report.py` | Sends daily Telegram report using output from `data_quality_checks.py` | Daily after quality checks |

Data flow:

```text
HData flow:                                                                   

```text
HackerNews API ── bronze.hackernews_payload ── silver.hackernews_content ───────┐
                                                                               ├── Gold ── data_quality_checks.py ── telegram_daily_report.py
Dev.to API ────── bronze.devto_payload ─────── silver.devto_article ────────────┘