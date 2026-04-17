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
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator

AIRFLOW_HOME = Path("/opt/airflow")
PROJECT_ROOT = AIRFLOW_HOME / "project"
sys.path.insert(0, str(PROJECT_ROOT))

logger = logging.getLogger(__name__)

KEYWORDS = [
    "data engineer",
    "data analyst",
    "machine learning",
    "backend developer",
]
MAX_PAGES = 3

RAW_DIR     = PROJECT_ROOT / "data" / "raw"
STAGING_DIR = PROJECT_ROOT / "data" / "staging"

default_args = {
    "owner": "salma",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# ─────────────────────────────────────────────────────────
# SCRAPE
# ─────────────────────────────────────────────────────────
def scrape_wuzzuf(**context):
    import re
    from scraper.mock_data_generator import generate

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    scraped_files = []

    def slug(kw):
        return re.sub(r"[^a-z0-9]+", "_", kw.lower()).strip("_")

    for keyword in KEYWORDS:
        path = RAW_DIR / f"wuzzuf_{slug(keyword)}.json"

        try:
            from scraper.wuzzuf_scraper import WuzzufScraper

            scraper = WuzzufScraper(keyword=keyword, max_pages=MAX_PAGES, output_dir=RAW_DIR)
            scraper.run()

            if scraper.jobs:
                out = scraper.save()
                scraped_files.append(str(out))
                scraper.close()
                continue

            scraper.close()

        except Exception as e:
            logger.warning("Scraper failed → using mock (%s)", e)

        out = generate(keyword=keyword, count=45, output_dir=RAW_DIR)
        scraped_files.append(str(out))

    context["ti"].xcom_push(key="scraped_files", value=scraped_files)
    return {"files": scraped_files}


# ─────────────────────────────────────────────────────────
# VALIDATE
# ─────────────────────────────────────────────────────────
def validate_raw_data(**context):
    from scraper.validator import validate_file

    files = context["ti"].xcom_pull(task_ids="scrape_wuzzuf", key="scraped_files")

    total = 0

    for f in files:
        report = validate_file(Path(f))
        if report["total"] == 0:
            raise ValueError(f"{f} has no data")

        total += report["total"]

    context["ti"].xcom_push(key="total_raw_jobs", value=total)
    return {"total": total}


# ─────────────────────────────────────────────────────────
# PRODUCER
# ─────────────────────────────────────────────────────────
def produce_to_kafka(**context):
    files = context["ti"].xcom_pull(task_ids="scrape_wuzzuf", key="scraped_files")

    total = 0

    try:
        from Kafka.producer import make_producer, publish_jobs, load_jobs_from_file

        producer = make_producer()

        for f in files:
            jobs = load_jobs_from_file(Path(f))
            total += publish_jobs(producer, jobs, Path(f).name)

        producer.close()
        mode = "kafka"

    except Exception as e:
        logger.warning("Kafka failed → simulation (%s)", e)

        all_jobs = []
        for f in files:
            with open(f) as file:
                data = json.load(file)
                all_jobs.extend(data.get("jobs", []))

        context["ti"].xcom_push(key="simulated_jobs", value=all_jobs)
        total = len(all_jobs)
        mode = "simulation"

    context["ti"].xcom_push(key="producer_mode", value=mode)
    context["ti"].xcom_push(key="total_published", value=total)

    return {"total": total, "mode": mode}


# ─────────────────────────────────────────────────────────
# CONSUMER
# ─────────────────────────────────────────────────────────
def consume_from_kafka(**context):
    from Kafka.consumer import clean_job

    STAGING_DIR.mkdir(parents=True, exist_ok=True)

    mode = context["ti"].xcom_pull(task_ids="produce_to_kafka", key="producer_mode")

    cleaned_jobs = []

    if mode == "simulation":
        raw_jobs = context["ti"].xcom_pull(task_ids="produce_to_kafka", key="simulated_jobs")
        cleaned_jobs = [clean_job(j) for j in raw_jobs]

    else:
        from Kafka.consumer import make_consumer

        consumer = make_consumer()
        for msg in consumer:
            cleaned_jobs.append(clean_job(msg.value))
        consumer.close()

    # ✅ LOAD EXISTING JOBS (correct way)
    existing_jobs = {}

    for file in STAGING_DIR.glob("staging_batch_*.json"):
        try:
            with open(file) as f:
                data = json.load(f)
                for job in data.get("jobs", []):
                    existing_jobs[job["job_id"]] = job
        except:
            pass

    # ✅ UPSERT
    for job in cleaned_jobs:
        existing_jobs[job["job_id"]] = job

    all_jobs = list(existing_jobs.values())

    # ✅ WRITE BATCHES (FIXED)
    BATCH_SIZE = 50
    staged_files = []
    batch_num = 1

    for i in range(0, len(all_jobs), BATCH_SIZE):
        batch = all_jobs[i:i + BATCH_SIZE]

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = STAGING_DIR / f"staging_batch_{batch_num:04d}_{timestamp}.json"

        payload = {
            "metadata": {
                "batch": batch_num,
                "count": len(batch),
                "written_at": datetime.now(timezone.utc).isoformat(),
            },
            "jobs": batch,
        }

        with open(path, "w") as f:
            json.dump(payload, f, indent=2)

        staged_files.append(str(path))
        batch_num += 1

    # ✅ RETURN OUTSIDE LOOP (FIXED)
    context["ti"].xcom_push(key="staged_files", value=staged_files)
    context["ti"].xcom_push(key="total_cleaned", value=len(cleaned_jobs))

    return {"cleaned": len(cleaned_jobs)}


# ─────────────────────────────────────────────────────────
# LOAD TO DB
# ─────────────────────────────────────────────────────────
def load_to_warehouse(**context):
    from warehouse.loader import get_conn, ensure_schema, load_staging_file

    files = context["ti"].xcom_pull(task_ids="consume_from_kafka", key="staged_files")

    if not files:
        return {"inserted": 0}

    conn = get_conn()
    ensure_schema(conn)

    inserted = 0

    for f in files:
        res = load_staging_file(conn, Path(f), run_id=context["run_id"])
        inserted += res["inserted"]

    conn.close()

    context["ti"].xcom_push(key="warehouse_inserted", value=inserted)
    return {"inserted": inserted}


# ─────────────────────────────────────────────────────────
# SUMMARY
# ─────────────────────────────────────────────────────────
def pipeline_summary(**context):
    ti = context["ti"]

    logger.info("RAW: %s", ti.xcom_pull(task_ids="validate_raw_data", key="total_raw_jobs"))
    logger.info("PUBLISHED: %s", ti.xcom_pull(task_ids="produce_to_kafka", key="total_published"))
    logger.info("CLEANED: %s", ti.xcom_pull(task_ids="consume_from_kafka", key="total_cleaned"))
    logger.info("LOADED: %s", ti.xcom_pull(task_ids="load_to_warehouse", key="warehouse_inserted"))

    return {}


# ─────────────────────────────────────────────────────────
# DAG
# ─────────────────────────────────────────────────────────
with DAG(
    dag_id="egyptian_job_market_pipeline",
    start_date=datetime(2026, 4, 1),
    schedule="0 6 * * *",
    catchup=False,
    max_active_runs=1,
) as dag:

    t1 = PythonOperator(task_id="scrape_wuzzuf", python_callable=scrape_wuzzuf)
    t2 = PythonOperator(task_id="validate_raw_data", python_callable=validate_raw_data)
    t3 = PythonOperator(task_id="produce_to_kafka", python_callable=produce_to_kafka)
    t4 = PythonOperator(task_id="consume_from_kafka", python_callable=consume_from_kafka)
    t5 = PythonOperator(task_id="load_to_warehouse", python_callable=load_to_warehouse)
    t6 = PythonOperator(task_id="pipeline_summary", python_callable=pipeline_summary, trigger_rule="all_done")

    t1 >> t2 >> t3 >> t4 >> t5 >> t6