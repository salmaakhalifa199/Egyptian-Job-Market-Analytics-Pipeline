"""
scraper/mock_data_generator.py
───────────────────────────────
Generates realistic mock job data for offline / CI use.
Called by the Airflow DAG when the real Wuzzuf scraper is unavailable.

Each keyword writes to a FIXED file (no timestamp in name).
New jobs are merged in by job_id — existing jobs are updated,
brand-new job_ids are appended.

Usage:
    from scraper.mock_data_generator import generate
    out = generate(keyword="data engineer", count=45, output_dir=Path("data/raw"))
"""

import json
import random
import re
from datetime import datetime, timezone
from pathlib import Path

# ── Mock data pools ────────────────────────────────────────────────────────────

COMPANIES = [
    "Vodafone Egypt", "Orange Egypt", "Etisalat Misr", "IBM Egypt",
    "Microsoft Egypt", "Amazon Egypt", "Raya Holding", "EpsilonAI",
    "Valeo Egypt", "Fawry", "Instabug", "Paymob", "Breadfast",
    "Swvl", "Yassir", "Robusta Studio", "ITWorx", "Sumerge",
    "Link Development", "SilverKey Technologies", "Confidential",
]

LOCATIONS = [
    "Nasr City, Cairo, Egypt",
    "Maadi, Cairo, Egypt",
    "New Cairo, Cairo, Egypt",
    "Heliopolis, Cairo, Egypt",
    "Smart Village, Giza, Egypt",
    "Dokki, Giza, Egypt",
    "6th of October, Giza, Egypt",
    "Cairo, Egypt",
    "Giza, Egypt",
    "Alexandria, Egypt",
    "Sheraton, Cairo, Egypt",
    "Remote (Egypt)",
]

JOB_TYPES = ["Full Time", "Part Time", "Freelance / Project"]

EXPERIENCE_RANGES = [
    "0 - 1 Yr of Exp",
    "1 - 3 Yrs of Exp",
    "2 - 4 Yrs of Exp",
    "3 - 5 Yrs of Exp",
    "3 - 7 Yrs of Exp",
    "5 - 7 Yrs of Exp",
    "5 - 10 Yrs of Exp",
    "8+ Yrs of Exp",
    "Not specified",
]

POSTED_DATES = [
    "1 day ago", "2 days ago", "3 days ago", "5 days ago",
    "1 week ago", "2 weeks ago", "3 weeks ago", "1 month ago",
    "2 months ago",
]

SKILLS_BY_KEYWORD = {
    "data engineer": [
        "Python", "SQL", "Apache Spark", "Apache Kafka", "Airflow",
        "dbt", "BigQuery", "Redshift", "Snowflake", "ETL",
        "Data Warehousing", "PostgreSQL", "AWS", "GCP", "Azure",
        "Docker", "Git", "Linux", "Pandas", "PySpark",
    ],
    "data analyst": [
        "SQL", "Python", "Power BI", "Tableau", "Excel",
        "Data Visualization", "Statistics", "R", "Google Analytics",
        "Looker", "DAX", "Data Analysis", "Reporting", "SPSS",
    ],
    "machine learning": [
        "Python", "TensorFlow", "PyTorch", "Scikit-learn", "ML",
        "Deep Learning", "NLP", "Computer Vision", "MLOps", "Pandas",
        "NumPy", "Keras", "XGBoost", "Feature Engineering", "LLM",
    ],
    "backend developer": [
        "Python", "Django", "FastAPI", "Node.js", "Express",
        "PostgreSQL", "MySQL", "Redis", "Docker", "REST APIs",
        "Git", "AWS", "Microservices", "GraphQL", ".NET",
    ],
}

TITLES_BY_KEYWORD = {
    "data engineer": [
        "Data Engineer", "Senior Data Engineer", "Junior Data Engineer",
        "Data Platform Engineer", "ETL Developer", "Data Infrastructure Engineer",
        "Analytics Engineer", "Big Data Engineer",
    ],
    "data analyst": [
        "Data Analyst", "Senior Data Analyst", "Business Intelligence Analyst",
        "Financial Analyst", "Marketing Analyst", "Product Analyst",
        "Operations Analyst", "Supply Chain Analyst",
    ],
    "machine learning": [
        "Machine Learning Engineer", "ML Engineer", "Senior ML Engineer",
        "AI Engineer", "Data Scientist", "Deep Learning Engineer",
        "NLP Engineer", "Computer Vision Engineer",
    ],
    "backend developer": [
        "Backend Developer", "Senior Backend Developer", "Python Developer",
        "Django Developer", "Node.js Developer", "API Developer",
        "Software Engineer", "Full Stack Developer",
    ],
}


def _slug(title: str, company: str, location: str) -> str:
    """Generate a URL-style slug for job_id."""
    parts = f"{title}-{company}-{location}"
    parts = re.sub(r"[^a-z0-9]+", "-", parts.lower()).strip("-")
    suffix = "".join(random.choices("abcdefghijklmnopqrstuvwxyz0123456789", k=8))
    return f"{suffix}-{parts[:60]}"


def _keyword_slug(keyword: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", keyword.lower()).strip("_")


def generate(
    keyword: str,
    count: int = 45,
    output_dir: Path = Path("data/raw"),
) -> Path:
    """
    Generate *count* mock job listings for *keyword* and upsert into a single
    fixed file per keyword (no timestamp in filename).

    - Existing jobs (matched by job_id) are updated with fresh scraped_at.
    - New job_ids are appended.
    - The file's metadata is refreshed on every run.

    Returns the path to the saved JSON file.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    kw_lower   = keyword.lower()
    skill_pool = next(
        (v for k, v in SKILLS_BY_KEYWORD.items() if k in kw_lower or kw_lower in k),
        ["Python", "SQL", "Git", "Communication", "Problem Solving"],
    )
    title_pool = next(
        (v for k, v in TITLES_BY_KEYWORD.items() if k in kw_lower or kw_lower in k),
        [f"{keyword.title()} Specialist", f"Senior {keyword.title()}"],
    )

    # ── Fixed filename — one file per keyword, forever ─────────────────────
    filename = f"wuzzuf_{_keyword_slug(keyword)}.json"
    out_path  = output_dir / filename

    # ── Load existing jobs so we can merge ──────────────────────────────────
    existing_jobs: dict[str, dict] = {}   # job_id → job dict
    if out_path.exists():
        try:
            with open(out_path, encoding="utf-8") as f:
                old_data = json.load(f)
            for job in old_data.get("jobs", []):
                existing_jobs[job["job_id"]] = job
        except (json.JSONDecodeError, KeyError):
            pass   # corrupt file — start fresh

    # ── Generate new batch ──────────────────────────────────────────────────
    scraped_at = datetime.now(timezone.utc).isoformat()
    new_jobs: list[dict] = []

    for _ in range(count):
        title   = random.choice(title_pool)
        company = random.choice(COMPANIES)
        location = random.choice(LOCATIONS)
        job_id  = _slug(title, company, location)
        skills  = random.sample(skill_pool, k=min(random.randint(4, 9), len(skill_pool)))
        posted  = random.choice(POSTED_DATES)

        new_jobs.append({
            "job_id":      job_id,
            "title":       title,
            "company":     company,
            "location":    location,
            "job_type":    random.choice(JOB_TYPES),
            "experience":  random.choice(EXPERIENCE_RANGES),
            "skills":      skills,
            "posted_date": posted,
            "url":         f"https://wuzzuf.net/jobs/p/{job_id}",
            "scraped_at":  scraped_at,
            "keyword":     keyword,
        })

    # ── Upsert: update existing, append new ─────────────────────────────────
    updated = 0
    appended = 0
    for job in new_jobs:
        if job["job_id"] in existing_jobs:
            # Refresh mutable fields; preserve job_id / url
            existing_jobs[job["job_id"]].update({
                "scraped_at":  scraped_at,
                "posted_date": job["posted_date"],
                "skills":      job["skills"],
                "experience":  job["experience"],
                "job_type":    job["job_type"],
            })
            updated += 1
        else:
            existing_jobs[job["job_id"]] = job
            appended += 1

    merged_jobs = list(existing_jobs.values())

    payload = {
        "metadata": {
            "keyword":      keyword,
            "total_jobs":   len(merged_jobs),
            "pages_scraped": 0,
            "last_updated": scraped_at,
            "updated_count": updated,
            "appended_count": appended,
            "source":       "mock_data_generator",
        },
        "jobs": merged_jobs,
    }

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    return out_path


if __name__ == "__main__":
    for kw in ["data engineer", "data analyst", "machine learning", "backend developer"]:
        p = generate(keyword=kw, count=10, output_dir=Path("data/raw"))
        print(f"Generated/updated: {p}")
