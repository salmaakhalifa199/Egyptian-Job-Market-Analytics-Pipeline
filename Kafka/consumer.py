"""
kafka/consumer.py
──────────────────
Consumes raw job messages from Kafka, cleans and normalises the data,
then writes batches to data/staging/ as JSON files ready for the
Airflow → PostgreSQL load in Phase 3.

Cleaning steps applied:
  - Normalise location (extract city name)
  - Normalise experience to numeric min/max years
  - Deduplicate skills (lowercase, strip whitespace)
  - Parse posted_date into days_ago integer
  - Strip pipeline metadata fields added by producer

Usage:
    python kafka/consumer.py               # runs until Ctrl+C
    python kafka/consumer.py --max 100     # stops after 100 messages
"""

"""
kafka/consumer.py
──────────────────
Consumes raw job messages from Kafka, cleans and normalises the data,
then writes batches to data/staging/ as JSON files.
"""

import argparse
import json
import logging
import os
import re
import signal
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

load_dotenv(dotenv_path=Path("config/.env"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | CONSUMER | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_RAW         = os.getenv("KAFKA_TOPIC_RAW", "wuzzuf_raw_jobs")
STAGING_DIR       = Path(os.getenv("STAGING_DATA_PATH", "data/staging"))
BATCH_SIZE        = int(os.getenv("CONSUMER_BATCH_SIZE", "50"))
GROUP_ID          = "job_pipeline_consumer_group"

INTERNAL_FIELDS = {"_source_file", "_pipeline_stage"}


# ── NEW: LOAD EXISTING JOB IDS ────────────────────────────────────────────────
def load_existing_job_ids(staging_dir: Path) -> set:
    existing_ids = set()

    if not staging_dir.exists():
        return existing_ids

    for file in staging_dir.glob("staging_batch_*.json"):
        try:
            with open(file, encoding="utf-8") as f:
                data = json.load(f)
                for job in data.get("jobs", []):
                    job_id = job.get("job_id")
                    if job_id:
                        existing_ids.add(job_id)
        except Exception as e:
            logger.warning("Error reading %s: %s", file, e)

    logger.info("Loaded %d existing job_ids from staging", len(existing_ids))
    return existing_ids


# ── Cleaning functions ────────────────────────────────────────────────────────
def clean_location(raw: str) -> str:
    if not raw or raw.lower() in ("unknown", ""):
        return "Unknown"
    raw = raw.strip()
    if raw.lower().startswith("remote"):
        return "Remote"
    parts = [p.strip() for p in raw.split(",")]
    parts = [p for p in parts if p.lower() != "egypt"]
    return parts[-1] if parts else raw


def clean_experience(raw: str) -> dict:
    if not raw or raw.lower() in ("not specified", "unknown", ""):
        return {"min_years": None, "max_years": None, "label": "Not specified"}

    range_match = re.search(r"(\d+)\s*-\s*(\d+)", raw)
    if range_match:
        mn, mx = int(range_match.group(1)), int(range_match.group(2))
        return {"min_years": mn, "max_years": mx, "label": f"{mn}-{mx} years"}

    plus_match = re.search(r"(\d+)\+", raw)
    if plus_match:
        mn = int(plus_match.group(1))
        return {"min_years": mn, "max_years": None, "label": f"{mn}+ years"}

    single_match = re.search(r"(\d+)", raw)
    if single_match:
        mn = int(single_match.group(1))
        return {"min_years": mn, "max_years": mn, "label": f"{mn} years"}

    return {"min_years": None, "max_years": None, "label": raw}


def clean_skills(raw_skills: list) -> list[str]:
    if not isinstance(raw_skills, list):
        return []
    seen = set()
    result = []
    for skill in raw_skills:
        if isinstance(skill, str):
            s = skill.strip().lower()
            if s and s not in seen:
                seen.add(s)
                result.append(s)
    return result


def parse_days_ago(raw: str) -> int | None:
    if not raw:
        return None
    raw = raw.lower().strip()

    match = re.search(r"(\d+)\s*(day|week|month|hour)", raw)
    if not match:
        return None

    value, unit = int(match.group(1)), match.group(2)
    multipliers = {"hour": 0, "day": 1, "week": 7, "month": 30}
    return value * multipliers.get(unit, 1)


def clean_job(raw: dict) -> dict:
    exp = clean_experience(raw.get("experience", ""))

    cleaned = {
        "job_id": raw.get("job_id", ""),
        "url": raw.get("url", ""),
        "keyword": raw.get("keyword", ""),
        "title": (raw.get("title") or "").strip(),
        "company": (raw.get("company") or "Unknown").strip(),
        "job_type": (raw.get("job_type") or "Unknown").strip(),
        "location_raw": raw.get("location", ""),
        "location_city": clean_location(raw.get("location", "")),
        "skills": clean_skills(raw.get("skills", [])),
        "skills_count": len(clean_skills(raw.get("skills", []))),
        "experience_label": exp["label"],
        "experience_min": exp["min_years"],
        "experience_max": exp["max_years"],
        "days_ago": parse_days_ago(raw.get("posted_date", "")),
        "posted_date_raw": raw.get("posted_date", ""),
        "scraped_at": raw.get("scraped_at", ""),
        "cleaned_at": datetime.now(timezone.utc).isoformat(),
    }

    for field in INTERNAL_FIELDS:
        cleaned.pop(field, None)

    return cleaned


# ── Batch writer ──────────────────────────────────────────────────────────────
def write_batch(batch: list[dict], batch_num: int) -> Path:
    STAGING_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"staging_batch_{batch_num:04d}_{timestamp}.json"
    out_path = STAGING_DIR / filename

    payload = {
        "metadata": {
            "batch_num": batch_num,
            "job_count": len(batch),
            "written_at": datetime.now(timezone.utc).isoformat(),
            "stage": "cleaned",
        },
        "jobs": batch,
    }

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    logger.info("Wrote batch %d (%d jobs) → %s", batch_num, len(batch), out_path.resolve())
    return out_path


# ── Consumer factory ──────────────────────────────────────────────────────────
def make_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        TOPIC_RAW,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        max_poll_records=BATCH_SIZE,
        consumer_timeout_ms=5000,
    )


# ── Main ──────────────────────────────────────────────────────────────────────
def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--max", type=int, default=None)
    return parser.parse_args()


def main():
    args = parse_args()

    try:
        consumer = make_consumer()
    except NoBrokersAvailable:
        logger.error("Kafka not running")
        return

    # ✅ LOAD EXISTING IDS
    existing_ids = load_existing_job_ids(STAGING_DIR)

    running = True
    def shutdown(sig, frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, shutdown)

    batch = []
    total_new = 0
    skipped = 0
    batch_num = 1

    for message in consumer:
        if not running:
            break

        cleaned = clean_job(message.value)
        job_id = cleaned.get("job_id")

        # ✅ DEDUP
        if job_id and job_id in existing_ids:
            skipped += 1
            continue

        batch.append(cleaned)

        if job_id:
            existing_ids.add(job_id)

        total_new += 1

        if len(batch) >= BATCH_SIZE:
            write_batch(batch, batch_num)
            batch_num += 1
            batch = []

        if args.max and total_new >= args.max:
            break

    if batch:
        write_batch(batch, batch_num)

    consumer.close()

    logger.info("New jobs written: %d", total_new)
    logger.info("Duplicates skipped: %d", skipped)


if __name__ == "__main__":
    main()