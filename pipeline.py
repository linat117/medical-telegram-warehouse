from dagster import op, job, schedule, RetryPolicy
import subprocess
import sys
import os

@op(
    tags={"kind": "scraping", "component": "telegram"},
    description="Scrapes data from Telegram channels"
)
def scrape_telegram_data():
    """Scrape telegram data from configured channels."""
    subprocess.check_call([
        sys.executable,
        "src/scraper.py"
    ])

@op(
    tags={"kind": "loading", "component": "postgres"},
    description="Loads raw JSON data into PostgreSQL"
)
def load_raw_to_postgres():
    """Load raw telegram messages from data lake into PostgreSQL."""
    subprocess.check_call([
        sys.executable,
        "src/load_raw.py"
    ])


@op(
    tags={"kind": "transformation", "component": "dbt"},
    description="Runs dbt transformations on raw data"
)
def run_dbt_transformations():
    """Run dbt transformations to create marts and staging models."""
    subprocess.check_call(
        ["dbt", "run"],
        cwd="medical_warehouse"
    )

@op(
    tags={"kind": "enrichment", "component": "yolo"},
    description="Runs YOLO object detection on images"
)
def run_yolo_enrichment():
    """Run YOLO object detection on scraped images and load results."""
    subprocess.check_call([
        sys.executable,
        "src/yolo_detect.py"
    ])


@job
def medical_data_pipeline():
    scrape_telegram_data()
    load_raw_to_postgres()
    run_dbt_transformations()
    run_yolo_enrichment()

@schedule(
    cron_schedule="0 2 * * *",
    job=medical_data_pipeline,
    execution_timezone="Africa/Addis_Ababa",
)
def daily_medical_pipeline_schedule():
    return {}
