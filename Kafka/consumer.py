"""
kafka/consumer.py
──────────────────
Consumes raw job messages from Kafka, cleans and normalises the data,
then writes to data/staging/ as FIXED per-keyword JSON files (incremental upsert).

Cleaning steps applied:
  - Normalise location (extract city name)
  - Normalise experience to numeric min/max years
  - Deduplicate skills (lowercase, strip whitespace)
  - Parse posted_date into days_ago integer
  - Strip pipeline metadata fields added by producer

Staging strategy (incremental):
  - One fixed file per keyword: staging_<keyword>.json
  - New job_ids are appended; existing job_ids are updated (re-cleaned)
  - No new file is created on every run — idempotent and space-efficient

Usage:
    python kafka/consumer.py               # runs until Ctrl+C
    python kafka/consumer.py --max 100     # stops after 100 messages
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

# Fields injected by producer that should not reach staging
INTERNAL_FIELDS = {"_source_file", "_pipeline_stage"}


# ── Cleaning functions ────────────────────────────────────────────────────────

def clean_location(raw: str) -> str:
    """
    Extract a clean city name from raw location strings.
    'Nasr City, Cairo, Egypt' → 'Cairo'
    'Smart Village, Giza, Egypt' → 'Giza'
    'Remote (Egypt)' → 'Remote'
    """
    if not raw or raw.lower() in ("unknown", ""):
        return "Unknown"
    raw = raw.strip()
    if raw.lower().startswith("remote"):
        return "Remote"
    parts = [p.strip() for p in raw.split(",")]
    parts = [p for p in parts if p.lower() != "egypt"]
    return parts[-1] if parts else raw


def clean_experience(raw: str) -> dict:
    """
    Parse experience string into structured min/max years.
    '2 - 4 Yrs of Exp'  → {'min_years': 2, 'max_years': 4, 'label': '2-4 years'}
    '8+ Yrs of Exp'      → {'min_years': 8, 'max_years': None, 'label': '8+ years'}
    'Not specified'      → {'min_years': None, 'max_years': None, 'label': 'Not specified'}
    """
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
    """
    Deduplicate and normalise a list of skill strings.
    ['Python', 'python', ' SQL ', 'SQL'] → ['python', 'sql']
    """
    if not isinstance(raw_skills, list):
        return []
    seen = set()
    result = []
    for skill in raw_skills:
        if isinstance(skill, str):
            normalised = skill.strip().lower()
            if normalised and normalised not in seen:
                seen.add(normalised)
                result.append(normalised)
    return result


def parse_days_ago(raw: str) -> int | None:
    """
    Convert 'X days ago' / 'X weeks ago' / 'X month ago' → integer days.
    Returns None if unparseable.
    """
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
    """
    Apply all cleaning steps to a raw job dict.
    Returns a cleaned job dict ready for staging.
    """
    exp = clean_experience(raw.get("experience", ""))

    cleaned = {
        # Identity
        "job_id":           raw.get("job_id", ""),
        "url":              raw.get("url", ""),
        "keyword":          raw.get("keyword", ""),
        # Core fields
        "title":            (raw.get("title") or "").strip(),
        "company":          (raw.get("company") or "Unknown").strip(),
        "job_type":         (raw.get("job_type") or "Unknown").strip(),
        # Cleaned / enriched fields
        "location_raw":     raw.get("location", ""),
        "location_city":    clean_location(raw.get("location", "")),
        "skills":           clean_skills(raw.get("skills", [])),
        "skills_count":     len(clean_skills(raw.get("skills", []))),
        "experience_label": exp["label"],
        "experience_min":   exp["min_years"],
        "experience_max":   exp["max_years"],
        "days_ago":         parse_days_ago(raw.get("posted_date", "")),
        "posted_date_raw":  raw.get("posted_date", ""),
        # Timestamps
        "scraped_at":       raw.get("scraped_at", ""),
        "cleaned_at":       datetime.now(timezone.utc).isoformat(),
    }

    # Strip any internal pipeline fields if they leaked in
    for field in INTERNAL_FIELDS:
        cleaned.pop(field, None)

    return cleaned


# ── Incremental staging file writer ──────────────────────────────────────────

def _keyword_slug(keyword: str) -> str:
    """Convert a keyword string to a safe filename slug."""
    return re.sub(r"[^a-z0-9]+", "_", keyword.lower()).strip("_")


def _load_staging_file(path: Path) -> dict[str, dict]:
    """
    Load an existing staging file and return a dict keyed by job_id.
    Returns an empty dict if the file does not exist or is corrupted.
    """
    if not path.exists():
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        return {job["job_id"]: job for job in data.get("jobs", []) if job.get("job_id")}
    except (json.JSONDecodeError, KeyError, OSError) as exc:
        logger.warning("Could not read staging file %s (%s) — starting fresh.", path.name, exc)
        return {}


def upsert_staging_file(cleaned_jobs: list[dict], staging_dir: Path) -> dict[str, Path]:
    """
    Upsert a list of cleaned jobs into per-keyword staging files.

    Strategy:
      - One file per keyword: staging_<keyword>.json
      - New job_ids are appended; existing job_ids are updated in-place
      - Metadata counters (total_jobs, last_updated, appended, updated) are refreshed

    Returns a dict mapping keyword → output Path for every file touched.
    """
    staging_dir.mkdir(parents=True, exist_ok=True)

    # Group jobs by keyword so we touch each file only once
    by_keyword: dict[str, list[dict]] = {}
    for job in cleaned_jobs:
        kw = job.get("keyword") or "unknown"
        by_keyword.setdefault(kw, []).append(job)

    touched: dict[str, Path] = {}

    for keyword, jobs in by_keyword.items():
        slug     = _keyword_slug(keyword)
        out_path = staging_dir / f"staging_{slug}.json"

        # Load existing records
        existing: dict[str, dict] = _load_staging_file(out_path)
        appended = 0
        updated  = 0

        for job in jobs:
            job_id = job.get("job_id", "")
            if not job_id:
                continue
            if job_id in existing:
                # Update mutable fields; preserve first-seen identity fields
                existing[job_id].update(job)
                updated += 1
            else:
                existing[job_id] = job
                appended += 1

        merged = list(existing.values())
        payload = {
            "metadata": {
                "keyword":        keyword,
                "total_jobs":     len(merged),
                "last_updated":   datetime.now(timezone.utc).isoformat(),
                "appended_count": appended,
                "updated_count":  updated,
                "stage":          "cleaned",
            },
            "jobs": merged,
        }

        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

        logger.info(
            "Staging upsert → %s  (appended=%d  updated=%d  total=%d)",
            out_path.name, appended, updated, len(merged),
        )
        touched[keyword] = out_path

    return touched


# ── Legacy batch writer (kept for backwards-compat with standalone CLI use) ──

def write_batch(batch: list[dict], batch_num: int) -> Path:
    """
    DEPRECATED for DAG use — use upsert_staging_file() instead.
    Kept so the standalone consumer CLI still works without changes.
    Delegates to upsert_staging_file so even CLI runs are incremental.
    """
    touched = upsert_staging_file(batch, STAGING_DIR)
    # Return the first file touched (mirrors old single-file return contract)
    if touched:
        return next(iter(touched.values()))
    # Fallback: write a numbered batch file (should rarely happen)
    STAGING_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename  = f"staging_batch_{batch_num:04d}_{timestamp}.json"
    out_path  = STAGING_DIR / filename
    payload = {
        "metadata": {
            "batch_num":  batch_num,
            "job_count":  len(batch),
            "written_at": datetime.now(timezone.utc).isoformat(),
            "stage":      "cleaned",
        },
        "jobs": batch,
    }
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    logger.info("Wrote fallback batch %d (%d jobs) → %s", batch_num, len(batch), out_path)
    return out_path


# ── Consumer factory ──────────────────────────────────────────────────────────

def make_consumer() -> KafkaConsumer:
    """Create and return a KafkaConsumer."""
    return KafkaConsumer(
        TOPIC_RAW,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        auto_commit_interval_ms=1000,
        max_poll_records=BATCH_SIZE,
        consumer_timeout_ms=5000,
    )


# ── Main ──────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Kafka consumer — cleans raw job messages")
    parser.add_argument(
        "--max", type=int, default=None,
        help="Stop after consuming this many messages (default: run until no messages)",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    logger.info("Connecting to Kafka at %s, topic '%s' ...", BOOTSTRAP_SERVERS, TOPIC_RAW)
    try:
        consumer = make_consumer()
    except NoBrokersAvailable:
        logger.error(
            "Cannot connect to Kafka at %s. Start Kafka first: docker-compose up -d",
            BOOTSTRAP_SERVERS,
        )
        return

    # Graceful shutdown on Ctrl+C
    running = True
    def _shutdown(sig, frame):  # noqa: ANN001
        nonlocal running
        logger.info("Shutdown signal received — flushing final batch ...")
        running = False
    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    cleaned_buffer: list[dict] = []
    total_consumed = 0

    logger.info("Consumer started. Waiting for messages (Ctrl+C to stop)...")

    for message in consumer:
        if not running:
            break

        raw_job = message.value
        cleaned = clean_job(raw_job)
        cleaned_buffer.append(cleaned)
        total_consumed += 1

        # Flush to incremental staging files every BATCH_SIZE messages
        if len(cleaned_buffer) >= BATCH_SIZE:
            upsert_staging_file(cleaned_buffer, STAGING_DIR)
            cleaned_buffer = []

        if args.max and total_consumed >= args.max:
            logger.info("Reached --max limit of %d messages.", args.max)
            break

    # Flush any remaining messages
    if cleaned_buffer:
        upsert_staging_file(cleaned_buffer, STAGING_DIR)

    consumer.close()
    logger.info("Consumer stopped. Total messages processed: %d", total_consumed)


if __name__ == "__main__":
    main()
