# Supply Chain Data Pipeline (ETL with Airflow & PostgreSQL)

## Problem

Manual data pipelines using scripts and cron jobs caused:
- No retry on failure
- No monitoring
- High latency
- No data validation

## Solution

Built an automated ETL pipeline using:
- Apache Airflow (scheduling)
- PostgreSQL (data warehouse)
- Python (data processing)

Pipeline flow:
CSV → Extract → Bronze → Transform → Silver → Validate

## Tech Stack

- Python
- PostgreSQL
- Apache Airflow
- SQL

## Status

Working pipeline with automated scheduling and validation.
