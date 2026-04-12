"""
scraper/validator.py
─────────────────────
Validates raw scraped JSON files and prints a quality report.
Run after scraping to spot missing fields or bad data before Phase 2.

Usage:
    python scraper/validator.py --file data/raw/wuzzuf_data_engineer_20260412.json
    python scraper/validator.py --dir data/raw   # validates all JSON files
"""

import argparse
import json
import logging
from pathlib import Path
from collections import Counter

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

REQUIRED_FIELDS = [
    "job_id", "title", "company", "location",
    "job_type", "experience", "skills", "posted_date", "url", "scraped_at",
]


def validate_file(path: Path) -> dict:
    """Validate a single raw JSON file. Returns a quality report dict."""
    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    jobs = data.get("jobs", [])
    meta = data.get("metadata", {})
    total = len(jobs)

    if total == 0:
        return {"file": path.name, "total": 0, "error": "No jobs found"}

    # Field completeness
    missing_counts = Counter()
    empty_skills = 0
    duplicate_ids = Counter(j.get("job_id", "") for j in jobs)
    duplicates = sum(1 for v in duplicate_ids.values() if v > 1)

    for job in jobs:
        for field in REQUIRED_FIELDS:
            val = job.get(field)
            if val is None or val == "" or val == "Unknown":
                missing_counts[field] += 1
        if not job.get("skills"):
            empty_skills += 1

    # Location breakdown
    locations = Counter(j.get("location", "Unknown") for j in jobs)
    top_locations = locations.most_common(5)

    # Top companies
    companies = Counter(j.get("company", "Unknown") for j in jobs)
    top_companies = companies.most_common(5)

    return {
        "file":         path.name,
        "keyword":      meta.get("keyword", "?"),
        "total":        total,
        "duplicates":   duplicates,
        "empty_skills": empty_skills,
        "missing":      dict(missing_counts),
        "top_locations": top_locations,
        "top_companies": top_companies,
    }


def print_report(report: dict):
    """Pretty-print a validation report."""
    sep = "─" * 55
    print(f"\n{sep}")
    print(f"  File    : {report['file']}")
    if "error" in report:
        print(f"  ERROR   : {report['error']}")
        return

    total = report["total"]
    print(f"  Keyword : {report['keyword']}")
    print(f"  Total   : {total} jobs")
    print(f"  Duplicates     : {report['duplicates']}")
    print(f"  Empty skills   : {report['empty_skills']} ({report['empty_skills']/total*100:.1f}%)")

    if report["missing"]:
        print(f"\n  Missing / unknown fields:")
        for field, count in sorted(report["missing"].items(), key=lambda x: -x[1]):
            pct = count / total * 100
            bar = "█" * int(pct / 5)
            print(f"    {field:<15} {count:>4}  {pct:5.1f}%  {bar}")
    else:
        print(f"\n  ✔ All required fields present")

    print(f"\n  Top locations:")
    for loc, count in report["top_locations"]:
        print(f"    {loc:<30} {count}")

    print(f"\n  Top companies:")
    for co, count in report["top_companies"]:
        print(f"    {co:<30} {count}")
    print(sep)


def main():
    parser = argparse.ArgumentParser(description="Validate raw Wuzzuf JSON files")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--file", type=Path, help="Path to a single JSON file")
    group.add_argument("--dir",  type=Path, help="Directory containing JSON files")
    args = parser.parse_args()

    files = []
    if args.file:
        files = [args.file]
    elif args.dir:
        files = sorted(args.dir.glob("*.json"))
        if not files:
            logger.warning(f"No JSON files found in {args.dir}")
            return

    for f in files:
        report = validate_file(f)
        print_report(report)

    print(f"\nValidated {len(files)} file(s).\n")


if __name__ == "__main__":
    main()
