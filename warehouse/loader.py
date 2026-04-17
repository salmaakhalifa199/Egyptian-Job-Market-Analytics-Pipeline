"""
warehouse/loader.py
────────────────────
Loads cleaned job data from data/staging/ into the PostgreSQL
star schema (job_market schema).

Implements:
  - Upsert logic (no duplicate job_ids)
  - Dimension auto-population (company, location, skill, date)
  - Bridge table population (job ↔ skills)
  - Pipeline run tracking via run_id

Usage:
    python warehouse/loader.py                          # load all staging files
    python warehouse/loader.py --run-id manual_20260414 # with run tracking
"""

import argparse
import json
import logging
import os
from datetime import datetime, timezone, date
from pathlib import Path

from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values

load_dotenv(dotenv_path=Path("./config/.env"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | LOADER | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "sslmode": "require",  # ← Neon requires SSL
}

STAGING_DIR = Path(os.getenv("STAGING_DATA_PATH", "data/staging"))
SCHEMA      = "job_market"


# ── DB helpers ────────────────────────────────────────────────────────────────

def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def ensure_schema(conn):
    """Create the job_market schema and tables if they don't exist."""
    schema_sql = Path(__file__).parent / "schema.sql"
    with conn.cursor() as cur:
        cur.execute(schema_sql.read_text())
    conn.commit()
    logger.info("Schema ready.")


# ── Dimension loaders ─────────────────────────────────────────────────────────

def get_or_create_company(cur, company_name: str) -> int:
    cur.execute(
        f"""
        INSERT INTO {SCHEMA}.dim_company (company_name)
        VALUES (%s)
        ON CONFLICT (company_name) DO UPDATE SET company_name = EXCLUDED.company_name
        RETURNING company_id
        """,
        (company_name,),
    )
    return cur.fetchone()[0]


def get_or_create_location(cur, city: str, location_raw: str) -> int:
    cur.execute(
        f"""
        INSERT INTO {SCHEMA}.dim_location (city, location_raw)
        VALUES (%s, %s)
        ON CONFLICT (city, location_raw) DO UPDATE SET city = EXCLUDED.city
        RETURNING location_id
        """,
        (city, location_raw),
    )
    return cur.fetchone()[0]


def get_experience_id(cur, label: str) -> int:
    """Match experience label to dim_experience, inserting with correct level if unknown."""
    if not label or label.lower() in ("not specified", "unknown", ""):
        label = "Not specified"

    cur.execute(
        f"SELECT experience_id FROM {SCHEMA}.dim_experience WHERE label = %s",
        (label,),
    )
    row = cur.fetchone()
    if row:
        return row[0]

    # Parse the label to determine level automatically
    import re
    min_years = None
    max_years = None
    level = "unknown"

    range_match = re.search(r"(\d+)-(\d+)", label)
    plus_match = re.search(r"(\d+)\+", label)
    single_match = re.search(r"^(\d+)\s*years?$", label)

    if range_match:
        min_years = int(range_match.group(1))
        max_years = int(range_match.group(2))
    elif plus_match:
        min_years = int(plus_match.group(1))
    elif single_match:
        min_years = int(single_match.group(1))
        max_years = min_years

    if min_years is not None:
        if min_years <= 1:
            level = "entry"
        elif min_years <= 4:
            level = "mid"
        elif min_years <= 7:
            level = "senior"
        else:
            level = "lead"

    cur.execute(
        f"""
        INSERT INTO {SCHEMA}.dim_experience (label, min_years, max_years, level)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (label) DO UPDATE SET
            min_years = EXCLUDED.min_years,
            max_years = EXCLUDED.max_years,
            level = EXCLUDED.level
        RETURNING experience_id
        """,
        (label, min_years, max_years, level),
    )
    return cur.fetchone()[0]


def get_or_create_date(cur, dt_str: str) -> int:
    """Parse ISO datetime string and upsert into dim_date."""
    try:
        dt = datetime.fromisoformat(dt_str).date()
    except (ValueError, TypeError):
        dt = date.today()

    day_names  = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    month_names = ["January", "February", "March", "April", "May", "June",
                   "July", "August", "September", "October", "November", "December"]

    cur.execute(
        f"""
        INSERT INTO {SCHEMA}.dim_date
            (full_date, year, quarter, month, month_name, week,
             day_of_month, day_of_week, day_name, is_weekend)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (full_date) DO NOTHING
        RETURNING date_id
        """,
        (
            dt,
            dt.year,
            (dt.month - 1) // 3 + 1,
            dt.month,
            month_names[dt.month - 1],
            dt.isocalendar()[1],
            dt.day,
            dt.weekday(),
            day_names[dt.weekday()],
            dt.weekday() >= 5,
        ),
    )
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute(
        f"SELECT date_id FROM {SCHEMA}.dim_date WHERE full_date = %s", (dt,)
    )
    return cur.fetchone()[0]


def get_or_create_skill(cur, skill_name: str) -> int:
    cur.execute(
        f"""
        INSERT INTO {SCHEMA}.dim_skill (skill_name)
        VALUES (%s)
        ON CONFLICT (skill_name) DO NOTHING
        RETURNING skill_id
        """,
        (skill_name.lower().strip(),),
    )
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute(
        f"SELECT skill_id FROM {SCHEMA}.dim_skill WHERE skill_name = %s",
        (skill_name.lower().strip(),),
    )
    return cur.fetchone()[0]


# ── Fact loader ───────────────────────────────────────────────────────────────

def _trim(value: str | None, max_length: int) -> str | None:
    """Truncate string to max_length, return None if value is None."""
    if value is None:
        return None
    return value[:max_length] if len(value) > max_length else value


def load_job(cur, job: dict, run_id: str) -> int | None:
    """
    Insert or skip a single job into fact_job_postings.
    Returns posting_id if inserted, None if duplicate.
    """
    # Resolve dimension IDs
    company_id    = get_or_create_company(cur, job.get("company") or "Unknown")
    location_id   = get_or_create_location(
        cur,
        city=job.get("location_city") or "Unknown",
        location_raw=job.get("location_raw") or "",
    )
    experience_id = get_experience_id(cur, job.get("experience_label") or "Not specified")
    date_id       = get_or_create_date(cur, job.get("scraped_at") or "")

    # Insert fact row — skip if job_id already exists
    cur.execute(
        f"""
        INSERT INTO {SCHEMA}.fact_job_postings (
            job_id, company_id, location_id, experience_id, scraped_date_id,
            title, job_type, keyword, skills_count, days_ago,
            url, posted_date_raw, location_raw, scraped_at, cleaned_at,
            pipeline_run_id
        )
        VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s
        )
        ON CONFLICT (job_id) DO NOTHING
        RETURNING posting_id
        """,
       (
            job.get("job_id"),
            company_id, location_id, experience_id, date_id,
            _trim(job.get("title"), 255),
            _trim(job.get("job_type"), 50),
            _trim(job.get("keyword"), 50),
            job.get("skills_count", 0),
            job.get("days_ago"),
            job.get("url"),
            _trim(job.get("posted_date_raw"), 100),
            _trim(job.get("location_raw"), 255),
            job.get("scraped_at"),
            job.get("cleaned_at"),
            run_id,
      ),
    )
    row = cur.fetchone()
    return row[0] if row else None


def load_skills(cur, posting_id: int, skills: list[str]):
    """Populate bridge_job_skill for a given posting."""
    if not skills:
        return
    bridge_rows = []
    for skill in skills:
        skill_id = get_or_create_skill(cur, skill)
        bridge_rows.append((posting_id, skill_id))

    execute_values(
        cur,
        f"""
        INSERT INTO {SCHEMA}.bridge_job_skill (posting_id, skill_id)
        VALUES %s
        ON CONFLICT DO NOTHING
        """,
        bridge_rows,
    )


# ── Staging file loader ───────────────────────────────────────────────────────

def load_staging_file(conn, path: Path, run_id: str) -> dict:
    """Load all jobs from one staging JSON file into the warehouse."""
    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    jobs       = data.get("jobs", [])
    inserted   = 0
    skipped    = 0

    with conn.cursor() as cur:
        for job in jobs:
            posting_id = load_job(cur, job, run_id)
            if posting_id is None:
                skipped += 1
                continue
            load_skills(cur, posting_id, job.get("skills", []))
            inserted += 1

    conn.commit()
    logger.info(
        "Loaded %s → inserted=%d  skipped(dup)=%d",
        path.name, inserted, skipped,
    )
    return {"inserted": inserted, "skipped": skipped, "file": path.name}


# ── Main ──────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load staging data into PostgreSQL warehouse")
    parser.add_argument(
        "--staging-dir", type=Path, default=STAGING_DIR,
        help="Directory containing staging JSON files",
    )
    parser.add_argument(
        "--run-id", type=str,
        default=datetime.now().strftime("manual_%Y%m%d_%H%M%S"),
        help="Pipeline run ID for lineage tracking",
    )
    parser.add_argument(
        "--init-schema", action="store_true",
        help="Create/reset the schema before loading",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    logger.info("Connecting to PostgreSQL at %s:%s/%s ...",
                DB_CONFIG["host"], DB_CONFIG["port"], DB_CONFIG["dbname"])
    conn = get_conn()

    if args.init_schema:
        logger.info("Initialising schema ...")
        ensure_schema(conn)

    files = sorted(args.staging_dir.glob("staging_batch_*.json"))
    if not files:
        logger.warning("No staging files found in %s", args.staging_dir)
        return

    total_inserted = 0
    total_skipped  = 0

    for path in files:
        result = load_staging_file(conn, path, run_id=args.run_id)
        total_inserted += result["inserted"]
        total_skipped  += result["skipped"]

    conn.close()
    logger.info(
        "Load complete. Total inserted=%d  skipped=%d  files=%d",
        total_inserted, total_skipped, len(files),
    )


if __name__ == "__main__":
    main()
