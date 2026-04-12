"""
scraper/mock_data_generator.py
────────────────────────────────
Generates realistic mock Wuzzuf job data for local testing when
you can't reach wuzzuf.net (e.g. inside a sandbox or CI environment).

Produces the exact same JSON schema as wuzzuf_scraper.py.

Usage:
    python scraper/mock_data_generator.py --count 50 --output-dir data/raw
"""

import argparse
import json
import random
import re
from datetime import datetime, timezone, timedelta
from pathlib import Path

# ── Sample data pools ─────────────────────────────────────────────────────────
COMPANIES = [
    "Vodafone Egypt", "Orange Egypt", "Etisalat Misr", "Banque Misr",
    "Commercial International Bank (CIB)", "Egyptian e-Finance", "Valeo",
    "IBM Egypt", "Microsoft Egypt", "Amazon Egypt", "Breadfast",
    "Swvl", "Instabug", "Paymob", "Fawry", "Robusta Studio",
    "Raya Holding", "Telecom Egypt", "EFG Hermes", "Bosta",
    "Halan", "MaxAB", "Khazna", "Rabbit", "Konnect",
]

LOCATIONS = [
    "Cairo, Egypt", "Giza, Egypt", "Alexandria, Egypt",
    "New Cairo, Egypt", "6th of October, Egypt",
    "Maadi, Cairo, Egypt", "Nasr City, Cairo, Egypt",
    "Smart Village, Giza, Egypt", "Downtown Cairo, Egypt",
    "Remote", "Remote (Egypt)", "Heliopolis, Cairo, Egypt",
]

JOB_TYPES = ["Full Time", "Part Time", "Internship", "Freelance / Project"]
EXPERIENCE_LEVELS = [
    "0 - 1 Yr of Exp", "1 - 2 Yrs of Exp", "2 - 4 Yrs of Exp",
    "4 - 6 Yrs of Exp", "6 - 8 Yrs of Exp", "8+ Yrs of Exp",
]

DATA_SKILLS = [
    "Python", "SQL", "Apache Spark", "Apache Kafka", "Apache Airflow",
    "PostgreSQL", "MySQL", "MongoDB", "Redis", "Elasticsearch",
    "Power BI", "Tableau", "Looker", "Google Data Studio",
    "AWS", "GCP", "Azure", "Docker", "Kubernetes",
    "Pandas", "NumPy", "Scikit-learn", "TensorFlow", "PyTorch",
    "dbt", "Snowflake", "BigQuery", "Redshift", "Databricks",
    "Hadoop", "HDFS", "Hive", "HBase", "Flink",
    "Git", "Linux", "Bash", "REST APIs", "ETL", "ELT",
]

BACKEND_SKILLS = [
    "Python", "Java", "Node.js", "Django", "FastAPI", "Spring Boot",
    "PostgreSQL", "MySQL", "Redis", "Docker", "Kubernetes",
    "AWS", "REST APIs", "GraphQL", "Microservices", "Git",
]

ML_SKILLS = [
    "Python", "TensorFlow", "PyTorch", "Scikit-learn", "Keras",
    "NLP", "Computer Vision", "Deep Learning", "Pandas", "NumPy",
    "MLflow", "Hugging Face", "OpenCV", "SQL", "Docker",
]

TITLES_BY_KEYWORD = {
    "data engineer": [
        "Data Engineer", "Senior Data Engineer", "Junior Data Engineer",
        "Data Engineer – ETL & Pipelines", "Big Data Engineer",
        "Data Platform Engineer", "Data Infrastructure Engineer",
        "Cloud Data Engineer", "Analytics Engineer",
    ],
    "data analyst": [
        "Data Analyst", "Senior Data Analyst", "Business Intelligence Analyst",
        "BI Developer", "Reporting Analyst", "Marketing Data Analyst",
        "Financial Data Analyst", "Operations Analyst",
    ],
    "machine learning": [
        "Machine Learning Engineer", "ML Engineer", "AI/ML Engineer",
        "Computer Vision Engineer", "NLP Engineer", "Deep Learning Engineer",
        "Research Scientist – ML", "Applied ML Engineer",
    ],
    "backend developer": [
        "Backend Developer", "Senior Backend Engineer", "Python Developer",
        "Node.js Developer", "Java Backend Engineer", "API Developer",
        "Software Engineer – Backend", "Full Stack Developer",
    ],
}

SKILLS_BY_KEYWORD = {
    "data engineer":    DATA_SKILLS,
    "data analyst":     DATA_SKILLS[:20] + BACKEND_SKILLS[-5:],
    "machine learning": ML_SKILLS,
    "backend developer": BACKEND_SKILLS,
}

POSTED_OPTIONS = [
    "1 day ago", "2 days ago", "3 days ago", "4 days ago",
    "5 days ago", "1 week ago", "2 weeks ago", "3 weeks ago",
    "1 month ago",
]


# ── Generator ─────────────────────────────────────────────────────────────────
def make_job(keyword: str, index: int) -> dict:
    keyword_lower = keyword.lower()
    titles = TITLES_BY_KEYWORD.get(keyword_lower, TITLES_BY_KEYWORD["data engineer"])
    skill_pool = SKILLS_BY_KEYWORD.get(keyword_lower, DATA_SKILLS)

    title = random.choice(titles)
    company = random.choice(COMPANIES)
    location = random.choice(LOCATIONS)
    job_id = f"mock-{index:04d}-{random.randint(10000, 99999)}"
    url = (
        f"https://wuzzuf.net/jobs/p/{job_id}-"
        + re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")
        + f"-{company.split()[0].lower()}-egypt"
    )

    return {
        "job_id":      job_id,
        "title":       title,
        "company":     company,
        "location":    location,
        "job_type":    random.choice(JOB_TYPES),
        "experience":  random.choice(EXPERIENCE_LEVELS),
        "skills":      random.sample(skill_pool, k=random.randint(3, 8)),
        "posted_date": random.choice(POSTED_OPTIONS),
        "url":         url,
        "scraped_at":  (
            datetime.now(timezone.utc) - timedelta(minutes=random.randint(0, 30))
        ).isoformat(),
        "keyword":     keyword,
    }


def generate(keyword: str, count: int, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    jobs = [make_job(keyword, i) for i in range(1, count + 1)]

    keyword_slug = re.sub(r"[^a-z0-9]+", "_", keyword.lower()).strip("_")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"wuzzuf_{keyword_slug}_{timestamp}_mock.json"
    output_path = output_dir / filename

    payload = {
        "metadata": {
            "keyword":       keyword,
            "total_jobs":    len(jobs),
            "pages_scraped": (count // 15) + 1,
            "saved_at":      datetime.now(timezone.utc).isoformat(),
            "source":        "mock_generator",
            "note":          "Generated mock data — replace with real scraper output",
        },
        "jobs": jobs,
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print(f"✔ Generated {len(jobs)} mock jobs → {output_path}")
    return output_path


def main():
    parser = argparse.ArgumentParser(description="Generate mock Wuzzuf job data")
    parser.add_argument("--keyword", "-k", default="data engineer",
                        help="Job search keyword")
    parser.add_argument("--count", "-n", type=int, default=50,
                        help="Number of jobs to generate (default: 50)")
    parser.add_argument("--output-dir", "-o", type=Path, default=Path("data/raw"),
                        help="Output directory (default: data/raw)")
    args = parser.parse_args()
    generate(args.keyword, args.count, args.output_dir)


if __name__ == "__main__":
    main()
