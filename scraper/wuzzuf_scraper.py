"""
scraper/wuzzuf_scraper.py
─────────────────────────
Scrapes job listings from Wuzzuf.com and saves them as structured JSON.

Phase 1 of the Egyptian Job Market Analytics Pipeline.

Output schema per job:
{
    "job_id":        str,   # Wuzzuf unique job ID
    "title":         str,   # Job title
    "company":       str,   # Company name
    "location":      str,   # City / governorate
    "job_type":      str,   # Full time / Part time / etc.
    "experience":    str,   # e.g. "2 - 4 Yrs of Exp"
    "skills":        list,  # List of skill tags
    "posted_date":   str,   # "X days ago" or exact date
    "url":           str,   # Full job URL
    "scraped_at":    str,   # ISO timestamp of scrape time
    "keyword":       str    # Search keyword used
}
"""

import argparse
import json
import logging
import time
import re
from datetime import datetime, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# ── Logging setup ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────
BASE_URL = "https://wuzzuf.net/search/jobs/"
DELAY_SECONDS = 2          # Polite delay between requests
REQUEST_TIMEOUT = 15       # Seconds before giving up on a request

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9,ar;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


# ── Scraper class ─────────────────────────────────────────────────────────────
class WuzzufScraper:
    """Scrapes job listings from Wuzzuf.net for a given search keyword."""

    def __init__(self, keyword: str, max_pages: int, output_dir: Path):
        self.keyword = keyword
        self.max_pages = max_pages
        self.output_dir = output_dir
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.jobs: list[dict] = []

    # ── HTTP ──────────────────────────────────────────────────────────────────
    def _get_page(self, page: int) -> BeautifulSoup | None:
        """Fetch a single search results page and return parsed HTML."""
        params = {"q": self.keyword, "a[page]": page}
        try:
            response = self.session.get(
                BASE_URL, params=params, timeout=REQUEST_TIMEOUT
            )
            response.raise_for_status()
            return BeautifulSoup(response.text, "lxml")
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error on page {page}: {e}")
        except requests.exceptions.ConnectionError:
            logger.error(f"Connection error on page {page}. Check your internet.")
        except requests.exceptions.Timeout:
            logger.error(f"Request timed out on page {page}.")
        return None

    # ── Parsing ───────────────────────────────────────────────────────────────
    def _parse_job_card(self, card, scraped_at: str) -> dict | None:
        """Extract structured data from a single job listing card."""
        try:
            # Title and URL
            title_tag = card.select_one("h2.css-m604qf a")
            if not title_tag:
                return None
            title = title_tag.get_text(strip=True)
            url = title_tag.get("href", "")
            if not url.startswith("http"):
                url = "https://wuzzuf.net" + url

            # Job ID from URL slug  e.g. /jobs/p/abc123-Title
            job_id_match = re.search(r"/jobs/p/([^-]+)-", url)
            job_id = job_id_match.group(1) if job_id_match else url.split("/")[-1]

            # Company
            company_tag = card.select_one("a.css-17s97q8")
            company = company_tag.get_text(strip=True) if company_tag else "Unknown"

            # Location
            location_tags = card.select("span.css-5wys0k")
            location = (
                ", ".join(t.get_text(strip=True) for t in location_tags)
                if location_tags else "Egypt"
            )

            # Job type (Full time / Part time / etc.)
            job_type_tag = card.select_one("a.css-n2jc4m")
            job_type = job_type_tag.get_text(strip=True) if job_type_tag else "Unknown"

            # Experience
            exp_tag = card.select_one("span[class*='css-1ve4b75']")
            experience = exp_tag.get_text(strip=True) if exp_tag else "Not specified"

            # Skills (tags on the card)
            skill_tags = card.select("a.css-o171kl")
            skills = [s.get_text(strip=True) for s in skill_tags]

            # Posted date
            date_tag = card.select_one("div.css-4c4ojb") or card.select_one("span[class*='ago']")
            posted_date = date_tag.get_text(strip=True) if date_tag else "Unknown"

            return {
                "job_id":      job_id,
                "title":       title,
                "company":     company,
                "location":    location,
                "job_type":    job_type,
                "experience":  experience,
                "skills":      skills,
                "posted_date": posted_date,
                "url":         url,
                "scraped_at":  scraped_at,
                "keyword":     self.keyword,
            }

        except Exception as e:
            logger.warning(f"Failed to parse a card: {e}")
            return None

    def _parse_page(self, soup: BeautifulSoup, scraped_at: str) -> list[dict]:
        """Parse all job cards on a results page."""
        cards = soup.select("div.css-1gatmva")  # Main job card container
        if not cards:
            # Fallback selector
            cards = soup.select("div[class*='JobCard']")
        logger.info(f"  Found {len(cards)} job cards on this page")

        jobs = []
        for card in cards:
            job = self._parse_job_card(card, scraped_at)
            if job:
                jobs.append(job)
        return jobs

    # ── Main loop ─────────────────────────────────────────────────────────────
    def run(self) -> list[dict]:
        """Scrape all pages and return collected jobs."""
        logger.info(f"Scraping Wuzzuf for: '{self.keyword}' (up to {self.max_pages} pages)")

        for page in range(1, self.max_pages + 1):
            logger.info(f"Page {page}/{self.max_pages} ...")
            scraped_at = datetime.now(timezone.utc).isoformat()

            soup = self._get_page(page)
            if soup is None:
                logger.warning(f"Skipping page {page} (failed to fetch)")
                continue

            # Stop if Wuzzuf shows "no results"
            no_results = soup.select_one("div.css-19q2b6v")
            if no_results:
                logger.info("No more results — stopping early.")
                break

            page_jobs = self._parse_page(soup, scraped_at)
            self.jobs.extend(page_jobs)
            logger.info(f"  Total collected so far: {len(self.jobs)}")

            if page < self.max_pages:
                time.sleep(DELAY_SECONDS)

        logger.info(f"Scraping complete. Total jobs collected: {len(self.jobs)}")
        return self.jobs

    # ── Output ────────────────────────────────────────────────────────────────
    def save(self) -> Path:
        """Save all collected jobs to a timestamped JSON file."""
        self.output_dir.mkdir(parents=True, exist_ok=True)

        keyword_slug = re.sub(r"[^a-z0-9]+", "_", self.keyword.lower()).strip("_")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"wuzzuf_{keyword_slug}_{timestamp}.json"
        output_path = self.output_dir / filename

        payload = {
            "metadata": {
                "keyword":     self.keyword,
                "total_jobs":  len(self.jobs),
                "pages_scraped": self.max_pages,
                "saved_at":    datetime.now(timezone.utc).isoformat(),
                "source":      "wuzzuf.net",
            },
            "jobs": self.jobs,
        }

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

        logger.info(f"Saved {len(self.jobs)} jobs → {output_path}")
        return output_path


# ── CLI entry point ───────────────────────────────────────────────────────────
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scrape job listings from Wuzzuf.net"
    )
    parser.add_argument(
        "--keyword", "-k",
        type=str,
        default="data engineer",
        help="Search keyword (default: 'data engineer')",
    )
    parser.add_argument(
        "--max-pages", "-p",
        type=int,
        default=5,
        help="Maximum number of result pages to scrape (default: 5)",
    )
    parser.add_argument(
        "--output-dir", "-o",
        type=Path,
        default=Path("data/raw"),
        help="Directory to save JSON output (default: data/raw)",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    scraper = WuzzufScraper(
        keyword=args.keyword,
        max_pages=args.max_pages,
        output_dir=args.output_dir,
    )
    scraper.run()
    if scraper.jobs:
        output_path = scraper.save()
        print(f"\n✔ Output saved: {output_path}")
    else:
        logger.warning("No jobs collected. The page structure may have changed.")


if __name__ == "__main__":
    main()
