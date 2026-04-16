"""
airflow/dags/job_pipeline_dag.py
─────────────────────────────────
Orchestrates the full Egyptian Job Market Analytics Pipeline:

  scrape_wuzzuf
       │
       ▼
  validate_raw_data
       │
       ▼
  produce_to_kafka
       │
       ▼
  consume_from_kafka
       │
       ▼
  load_to_warehouse   ← Phase 4
       │
       ▼
  pipeline_summary

Schedule: daily at 08:00 Cairo time (UTC+2 → 06:00 UTC)
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator

# ── Paths inside the Astro container ──────────────────────────────────────────
# Dockerfile COPYs: scraper/ and Kafka/ → /usr/local/airflow/
# data/ is mounted live via docker-compose.override.yml
# PYTHONPATH is already set to /usr/local/airflow by the Dockerfile
AIRFLOW_HOME = Path("/opt/airflow")
PROJECT_ROOT = AIRFLOW_HOME / "project"
sys.path.insert(0, str(PROJECT_ROOT))

logger = logging.getLogger(__name__)

# ── Keywords to scrape ─────────────────────────────────────────────────────────
KEYWORDS = [
    "data engineer",
    "data analyst",
    "machine learning",
    "backend developer",
]
MAX_PAGES = 3

# ── Paths ──────────────────────────────────────────────────────────────────────
RAW_DIR     = PROJECT_ROOT / "data" / "raw"
STAGING_DIR = PROJECT_ROOT / "data" / "staging"

# ── Default DAG args ──────────────────────────────────────────────────────────
default_args = {
    "owner":            "salma",
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry":   False,
}


# ══════════════════════════════════════════════════════════════════════════════
# TASK FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════

def scrape_wuzzuf(**context) -> dict:
    """
    Task 1: Scrape job listings from Wuzzuf for all keywords.
    Uses the mock generator when Wuzzuf is unreachable (CI/offline mode).

    Each keyword writes to ONE fixed file (wuzzuf_<keyword>.json).
    New jobs are merged in by job_id; existing jobs are updated.
    Pushes scraped file paths to XCom for downstream tasks.
    """
    import re
    from scraper.mock_data_generator import generate

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    scraped_files = []

    def _kw_slug(kw: str) -> str:
        return re.sub(r"[^a-z0-9]+", "_", kw.lower()).strip("_")

    for keyword in KEYWORDS:
        # Fixed filename — same path on every DAG run
        fixed_path = RAW_DIR / f"wuzzuf_{_kw_slug(keyword)}.json"

        logger.info("Scraping keyword: '%s' → %s", keyword, fixed_path.name)
        try:
            from scraper.wuzzuf_scraper import WuzzufScraper
            scraper = WuzzufScraper(
                keyword=keyword,
                max_pages=MAX_PAGES,
                output_dir=RAW_DIR,
            )
            scraper.run()
            if scraper.jobs:
                out = scraper.save()          # save() now does the upsert
                scraped_files.append(str(out))
                logger.info(
                    "Real scrape OK: %d new/updated jobs → %s",
                    len(scraper.jobs), out.name,
                )
                scraper.close()
                continue
            scraper.close()
        except Exception as exc:
            logger.warning("Real scraper failed (%s) — falling back to mock data", exc)

        # Fallback: mock data (upserts into the same fixed file)
        out = generate(keyword=keyword, count=45, output_dir=RAW_DIR)
        scraped_files.append(str(out))
        logger.info("Mock data generated/updated: %s", out.name)

    logger.info("Scraping complete. Files: %d", len(scraped_files))
    context["ti"].xcom_push(key="scraped_files", value=scraped_files)
    return {"scraped_files": scraped_files, "total_keywords": len(KEYWORDS)}


def validate_raw_data(**context) -> dict:
    """
    Task 2: Validate raw JSON files from Task 1.
    Fails the task if any file has 0 jobs or >50% missing fields.
    """
    from scraper.validator import validate_file

    scraped_files = context["ti"].xcom_pull(
        task_ids="scrape_wuzzuf", key="scraped_files"
    )
    if not scraped_files:
        raise ValueError("No scraped files received from scrape_wuzzuf task")

    total_jobs   = 0
    failed_files = []

    for file_path in scraped_files:
        path = Path(file_path)
        if not path.exists():
            logger.warning("File not found: %s", file_path)
            continue

        report = validate_file(path)
        jobs   = report.get("total", 0)

        if jobs == 0:
            failed_files.append(f"{path.name}: 0 jobs")
            continue

        # Check if >50% of jobs have missing critical fields
        missing = report.get("missing", {})
        for field, count in missing.items():
            if field in ("title", "company", "url") and count / jobs > 0.5:
                failed_files.append(
                    f"{path.name}: {field} missing in {count}/{jobs} jobs"
                )

        total_jobs += jobs
        logger.info("Validated %s: %d jobs, issues: %s", path.name, jobs, missing)

    if failed_files:
        raise ValueError(f"Validation failed:\n" + "\n".join(failed_files))

    logger.info("Validation passed. Total jobs ready: %d", total_jobs)
    context["ti"].xcom_push(key="total_raw_jobs", value=total_jobs)
    return {"total_jobs": total_jobs, "files_validated": len(scraped_files)}


def produce_to_kafka(**context) -> dict:
    """
    Task 3: Publish validated jobs to the Kafka topic.
    Falls back to in-memory simulation if Kafka is unavailable.
    """
    scraped_files = context["ti"].xcom_pull(
        task_ids="scrape_wuzzuf", key="scraped_files"
    )

    files = [Path(f) for f in scraped_files if Path(f).exists()]
    total_published = 0

    try:
        from Kafka.producer import make_producer, publish_jobs, load_jobs_from_file
        from kafka.errors import NoBrokersAvailable

        logger.info("Connecting to Kafka...")
        producer = make_producer()

        for file_path in files:
            jobs = load_jobs_from_file(file_path)
            sent = publish_jobs(producer, jobs, source_file=file_path.name)
            total_published += sent
            logger.info("Published %d jobs from %s", sent, file_path.name)

        producer.close()
        mode = "kafka"

    except Exception as exc:
        logger.warning("Kafka unavailable (%s) — using in-memory simulation", exc)
        # Store jobs in XCom for consumer task to pick up directly
        all_jobs = []
        for file_path in files:
            with open(file_path, encoding="utf-8") as f:
                data = json.load(f)
            jobs = data.get("jobs", [])
            for job in jobs:
                job["_source_file"] = file_path.name
                job["_pipeline_stage"] = "raw"
            all_jobs.extend(jobs)
        total_published = len(all_jobs)
        context["ti"].xcom_push(key="simulated_jobs", value=all_jobs)
        mode = "simulation"

    logger.info("Producer done. Total published: %d (mode: %s)", total_published, mode)
    context["ti"].xcom_push(key="producer_mode", value=mode)
    context["ti"].xcom_push(key="total_published", value=total_published)
    return {"total_published": total_published, "mode": mode}


def consume_from_kafka(**context) -> dict:
    """
    Task 4: Consume messages from Kafka, clean them, write to staging.
    If producer ran in simulation mode, processes jobs directly from XCom.
    """
    from Kafka.consumer import clean_job, write_batch

    STAGING_DIR.mkdir(parents=True, exist_ok=True)
    mode = context["ti"].xcom_pull(
        task_ids="produce_to_kafka", key="producer_mode"
    )

    cleaned_jobs = []

    if mode == "simulation":
        # Direct path: jobs passed via XCom
        raw_jobs = context["ti"].xcom_pull(
            task_ids="produce_to_kafka", key="simulated_jobs"
        )
        for raw in raw_jobs:
            cleaned_jobs.append(clean_job(raw))
        logger.info("Cleaned %d jobs from simulation", len(cleaned_jobs))

    else:
        # Real Kafka path
        try:
            from Kafka.consumer import make_consumer
            consumer = make_consumer()
            for message in consumer:
                cleaned_jobs.append(clean_job(message.value))
            consumer.close()
            logger.info("Consumed and cleaned %d jobs from Kafka", len(cleaned_jobs))
        except Exception as exc:
            raise RuntimeError(f"Kafka consumer failed: {exc}") from exc

    # Write to staging in batches of 50
    BATCH_SIZE  = 50
    batch_num   = 1
    staged_files = []

    for i in range(0, len(cleaned_jobs), BATCH_SIZE):
        batch = cleaned_jobs[i: i + BATCH_SIZE]
        out   = write_batch(batch, batch_num)
        staged_files.append(str(out))
        batch_num += 1

    logger.info("Wrote %d staging files", len(staged_files))
    context["ti"].xcom_push(key="staged_files",  value=staged_files)
    context["ti"].xcom_push(key="total_cleaned", value=len(cleaned_jobs))
    return {"total_cleaned": len(cleaned_jobs), "staging_files": len(staged_files)}


def load_to_warehouse(**context) -> dict:
    """
    Task 5: Load cleaned staging files into PostgreSQL star schema.
    Uses upsert logic — safe to re-run, no duplicate job_ids.
    """
    import psycopg2
    from warehouse.loader import get_conn, ensure_schema, load_staging_file

    staged_files = context["ti"].xcom_pull(
        task_ids="consume_from_kafka", key="staged_files"
    ) or []
    run_id = context["run_id"]

    if not staged_files:
        logger.warning("No staged files to load — skipping warehouse load")
        context["ti"].xcom_push(key="warehouse_inserted", value=0)
        return {"inserted": 0, "skipped": 0}

    logger.info("Connecting to PostgreSQL ...")
    conn = get_conn()
    ensure_schema(conn)

    total_inserted = 0
    total_skipped  = 0

    for file_path in staged_files:
        path = Path(file_path)
        if not path.exists():
            logger.warning("Staging file not found: %s", file_path)
            continue
        result = load_staging_file(conn, path, run_id=run_id)
        total_inserted += result["inserted"]
        total_skipped  += result["skipped"]

    conn.close()
    logger.info(
        "Warehouse load complete. inserted=%d  skipped=%d",
        total_inserted, total_skipped,
    )
    context["ti"].xcom_push(key="warehouse_inserted", value=total_inserted)
    return {"inserted": total_inserted, "skipped": total_skipped}


def pipeline_summary(**context) -> dict:
    """
    Task 5: Log a summary of the full pipeline run.
    This task always runs — even if upstream tasks partially failed.
    """
    ti = context["ti"]

    raw_jobs    = ti.xcom_pull(task_ids="validate_raw_data",  key="total_raw_jobs")      or 0
    published   = ti.xcom_pull(task_ids="produce_to_kafka",   key="total_published")     or 0
    cleaned     = ti.xcom_pull(task_ids="consume_from_kafka", key="total_cleaned")       or 0
    staged_files= ti.xcom_pull(task_ids="consume_from_kafka", key="staged_files")        or []
    mode        = ti.xcom_pull(task_ids="produce_to_kafka",   key="producer_mode")       or "unknown"
    inserted    = ti.xcom_pull(task_ids="load_to_warehouse",  key="warehouse_inserted")  or 0
    run_date    = context["ds"]

    summary = {
        "run_date":         run_date,
        "raw_jobs":         raw_jobs,
        "published":        published,
        "cleaned":          cleaned,
        "staging_files":    len(staged_files),
        "kafka_mode":       mode,
        "warehouse_loaded": inserted,
    }

    logger.info("=" * 55)
    logger.info("PIPELINE RUN SUMMARY — %s", run_date)
    logger.info("  Raw jobs scraped   : %d", raw_jobs)
    logger.info("  Published to Kafka : %d", published)
    logger.info("  Cleaned jobs       : %d", cleaned)
    logger.info("  Staging files      : %d", len(staged_files))
    logger.info("  Kafka mode         : %s", mode)
    logger.info("  Loaded to warehouse: %d", inserted)
    logger.info("=" * 55)

    return summary


# ══════════════════════════════════════════════════════════════════════════════
# DAG DEFINITION
# ══════════════════════════════════════════════════════════════════════════════

with DAG(
    dag_id="egyptian_job_market_pipeline",
    description="Daily pipeline: scrape Wuzzuf → Kafka → staging → warehouse",
    default_args=default_args,
    schedule="0 6 * * *",       # 06:00 UTC = 08:00 Cairo time, every day
    start_date=datetime(2026, 4, 1),
    catchup=False,               # don't backfill missed runs
    max_active_runs=1,           # only one run at a time
    tags=["job-market", "egypt", "kafka", "etl"],
) as dag:

    # ── Task 1: Scrape ─────────────────────────────────────────────────────────
    t_scrape = PythonOperator(
        task_id="scrape_wuzzuf",
        python_callable=scrape_wuzzuf,
        execution_timeout=timedelta(minutes=20),
    )

    # ── Task 2: Validate ───────────────────────────────────────────────────────
    t_validate = PythonOperator(
        task_id="validate_raw_data",
        python_callable=validate_raw_data,
        execution_timeout=timedelta(minutes=5),
    )

    # ── Task 3: Produce to Kafka ───────────────────────────────────────────────
    t_produce = PythonOperator(
        task_id="produce_to_kafka",
        python_callable=produce_to_kafka,
        execution_timeout=timedelta(minutes=10),
    )

    # ── Task 4: Consume from Kafka ─────────────────────────────────────────────
    t_consume = PythonOperator(
        task_id="consume_from_kafka",
        python_callable=consume_from_kafka,
        execution_timeout=timedelta(minutes=10),
    )

    # ── Task 5: Load to warehouse ──────────────────────────────────────────────
    t_load = PythonOperator(
        task_id="load_to_warehouse",
        python_callable=load_to_warehouse,
        execution_timeout=timedelta(minutes=15),
    )

    # ── Task 6: Summary ────────────────────────────────────────────────────────
    t_summary = PythonOperator(
        task_id="pipeline_summary",
        python_callable=pipeline_summary,
        trigger_rule="all_done",       # runs even if upstream tasks failed
        execution_timeout=timedelta(minutes=2),
    )

    # ── Dependencies ───────────────────────────────────────────────────────────
    t_scrape >> t_validate >> t_produce >> t_consume >> t_load >> t_summary
