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

NOTE ON SELECTOR STABILITY
───────────────────────────
Wuzzuf uses CSS-in-JS (Emotion) which generates hashed class names (e.g.
css-1gatmva) that change on every front-end deployment. This scraper uses
structural / semantic selectors instead — href patterns, data attributes, and
relative position — so it survives class-name churn.

If parsing breaks again, run the bundled debug helper:
    python scraper/wuzzuf_scraper.py --debug-html path/to/saved_page.html
"""

import argparse
import json
import logging
import time
import re
from datetime import datetime, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup, Tag

# ── Logging setup ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────
BASE_URL = "https://wuzzuf.net/search/jobs/"
DELAY_SECONDS = 2
REQUEST_TIMEOUT = 15

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9,ar;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# ── Stable CSS selectors ───────────────────────────────────────────────────────
# Wuzzuf uses Emotion (CSS-in-JS) — class names like css-pkv5jc change on every
# front-end deploy.  We use structural / semantic selectors instead.
#
# Confirmed page structure (from --debug-html, April 2026):
#
#   <div class="css-9i2afk">           ← results list
#     <div class="">                   ← anonymous wrapper
#       <div class="css-ghe2tq …">    ← outer card shell
#         <div class="css-pkv5jc">    ← CARD ROOT  (2 levels above <h2>)
#           <div class="css-lptxge">  ← inner wrapper (contains <h2>)
#             <h2 …><a href="/jobs/p/…">  ← title link ✓
#
# CARD_SELECTORS are tried in order; first match wins.
# _find_cards() always has a Python walk-up heuristic as final fallback.
CARD_SELECTORS = [
    "article[data-jobid]",           # ideal — stable if Wuzzuf adds it
    "div[data-jobid]",
    "div[class*='JobCard']",
    "div[class*='job-card']",
    # April 2026 confirmed: card root is 2 levels above the <h2>
    # (h2 → div.css-lptxge → div.css-pkv5jc)
    "div:has(> div > h2 a[href*='/jobs/p/'])",   # needs soupsieve>=2.4
]

# Within a card each field uses the most specific stable anchor available.
# Lists are tried in order; first match wins.
FIELD_SELECTORS = {
    # ── Title ────────────────────────────────────────────────────────────────
    "title_link": "h2 a[href*='/jobs/p/']",

    # ── Company ──────────────────────────────────────────────────────────────
    # April 2026: /jobs/c/ hrefs are absent in this deploy.
    # Positional Python fallback in _parse_job_card handles it when CSS fails.
    "company": [
        "a[href*='/jobs/c/']",
        "a[href*='/company/']",
    ],

    # ── Location ─────────────────────────────────────────────────────────────
    "location": [
        "span[class*='location']",
        "span[class*='Location']",
    ],

    # ── Job type ─────────────────────────────────────────────────────────────
    "job_type": [
        "a[href*='filters%5Btype%5D']",
        "a[href*='filters[type]']",
        "a[href*='type%5D']",
        "span[class*='type']",
        "a[class*='type']",
    ],

    # ── Experience ───────────────────────────────────────────────────────────
    "experience": [
        "span[class*='xp']",
        "span[class*='exp']",
        "span[class*='Exp']",
        "span[class*='experience']",
        "i.fas.fa-briefcase + span",
    ],

    # ── Skills ───────────────────────────────────────────────────────────────
    # April 2026: no filters[skill] links found; pills use tag-like classes.
    "skills": [
        "a[href*='filters%5Bskill']",
        "a[href*='filters[skill]']",
        "a[href*='skills']",
        "a[class*='tag']",
        "a[class*='Tag']",
        "span[class*='tag'] a",
        "span[class*='Tag'] a",
    ],

    # ── Posted date ──────────────────────────────────────────────────────────
    "posted_date": [
        "time",
        "span[class*='date']",
        "div[class*='date']",
        "span[class*='Date']",
        "span[class*='ago']",
        "div[class*='ago']",
        "span[class*='post']",
    ],
}


def _first(card: Tag, selectors) -> Tag | None:
    """Try selectors in order, return the first match inside *card*."""
    if isinstance(selectors, str):
        selectors = [selectors]
    for sel in selectors:
        found = card.select_one(sel)
        if found:
            return found
    return None


def _all(card: Tag, selectors) -> list[Tag]:
    """Try selectors in order, return all matches for the first that hits."""
    if isinstance(selectors, str):
        selectors = [selectors]
    for sel in selectors:
        found = card.select(sel)
        if found:
            return found
    return []


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

    # ── Card discovery ────────────────────────────────────────────────────────
    def _find_cards(self, soup: BeautifulSoup) -> list[Tag]:
        """
        Locate all job cards on the page using stable selectors.
        Falls back to a heuristic: any element that directly contains a
        title link (h2 > a[href*='/jobs/p/']).
        """
        for sel in CARD_SELECTORS:
            cards = soup.select(sel)
            if cards:
                logger.debug(f"Card selector matched: {sel!r} → {len(cards)} cards")
                return cards

        # Heuristic fallback: find all title anchors, then take their grandparent
        title_links = soup.select("h2 a[href*='/jobs/p/']")
        if title_links:
            logger.warning(
                "No card container matched — using grandparent heuristic. "
                "Consider updating CARD_SELECTORS."
            )
            seen = set()
            cards = []
            for link in title_links:
                # Walk up until we find a block-level ancestor that wraps the
                # whole card (heuristic: 3 levels up from the <a>)
                container = link
                for _ in range(3):   # h2→inner-div→card-root (confirmed April 2026)
                    if container.parent:
                        container = container.parent
                card_id = id(container)
                if card_id not in seen:
                    seen.add(card_id)
                    cards.append(container)
            return cards

        return []

    # ── Parsing ───────────────────────────────────────────────────────────────
    def _parse_job_card(self, card: Tag, scraped_at: str) -> dict | None:
        try:
            # ── Title + URL ──────────────────────────────────────────────────
            title_tag = _first(card, FIELD_SELECTORS["title_link"])
            if not title_tag:
                return None
            title = title_tag.get_text(strip=True)
            url = title_tag.get("href", "")
            if not url.startswith("http"):
                url = "https://wuzzuf.net" + url

            # ── Job ID ───────────────────────────────────────────────────────
            # Prefer the data-jobid attribute on the card itself
            job_id = card.get("data-jobid", "")
            if not job_id:
                m = re.search(r"/jobs/p/([^/?#]+)", url)
                job_id = m.group(1) if m else url.split("/")[-1]

            # ── Company ──────────────────────────────────────────────────────
            company_tag = _first(card, FIELD_SELECTORS["company"])
            if company_tag:
                company = company_tag.get_text(strip=True)
            else:
                # Positional fallback: first <a> in the card that is NOT the title link
                company = "Unknown"
                for a in card.find_all("a", href=True):
                    href = a.get("href", "")
                    text = a.get_text(strip=True)
                    if "/jobs/p/" not in href and text:
                        company = text
                        break

            # ── Location ─────────────────────────────────────────────────────
            location_tag = _first(card, FIELD_SELECTORS["location"])
            if location_tag:
                location = location_tag.get_text(strip=True)
            else:
                # Positional fallback: short <span> texts outside the <h2> that
                # don't look like job-type or experience values
                skip_kw = {"yrs", "year", "full", "part", "intern", "remote", "contract"}
                spans = [
                    s.get_text(strip=True)
                    for s in card.find_all("span")
                    if s.get_text(strip=True)
                    and s.find_parent("h2") is None
                    and len(s.get_text(strip=True)) < 40
                    and not any(kw in s.get_text(strip=True).lower() for kw in skip_kw)
                ]
                location = ", ".join(spans[:2]) if spans else "Egypt"

            # ── Job type ─────────────────────────────────────────────────────
            job_type_tag = _first(card, FIELD_SELECTORS["job_type"])
            job_type = job_type_tag.get_text(strip=True) if job_type_tag else "Unknown"

            # ── Experience ───────────────────────────────────────────────────
            exp_tag = _first(card, FIELD_SELECTORS["experience"])
            experience = exp_tag.get_text(strip=True) if exp_tag else "Not specified"

            # ── Skills ───────────────────────────────────────────────────────
            skill_tags = _all(card, FIELD_SELECTORS["skills"])
            skills = [s.get_text(strip=True) for s in skill_tags if s.get_text(strip=True)]

            # ── Posted date ──────────────────────────────────────────────────
            date_tag = _first(card, FIELD_SELECTORS["posted_date"])
            if date_tag:
                # Prefer the machine-readable datetime attribute
                posted_date = date_tag.get("datetime") or date_tag.get_text(strip=True)
            else:
                posted_date = "Unknown"

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
        cards = self._find_cards(soup)
        logger.info(f"  Found {len(cards)} job cards on this page")

        jobs = []
        for card in cards:
            job = self._parse_job_card(card, scraped_at)
            if job:
                jobs.append(job)
        return jobs

    # ── No-results detection ──────────────────────────────────────────────────
    def _is_empty_results_page(self, soup: BeautifulSoup) -> bool:
        """
        Return True only when Wuzzuf explicitly signals zero results.

        We require BOTH conditions to be true before stopping early:
          1. No job-title links are present on the page.
          2. The page contains a known empty-state element OR a heading/text
             that is the dedicated "no results" message.

        Using soup.find(string=regex) alone is too broad — it matches any text
        node anywhere in the page (nav labels, meta descriptions, footers …).
        """
        # Condition 1: no title links at all
        has_jobs = bool(soup.select("h2 a[href*='/jobs/p/']"))
        if has_jobs:
            return False

        # Condition 2a: Wuzzuf renders a dedicated empty-state container
        empty_selectors = [
            "div[class*='EmptyState']",
            "div[class*='empty-state']",
            "div[class*='no-results']",
            "div[class*='NoResults']",
            "section[class*='empty']",
        ]
        for sel in empty_selectors:
            if soup.select_one(sel):
                logger.debug(f"Empty-state element matched: {sel!r}")
                return True

        # Condition 2b: a heading (h1–h3) whose text is the "no results" message
        for heading in soup.find_all(["h1", "h2", "h3"]):
            text = heading.get_text(strip=True).lower()
            if re.search(r"no (results|jobs|vacancies) found|0 jobs", text):
                logger.debug(f"Empty-state heading found: {heading.get_text(strip=True)!r}")
                return True

        # No jobs AND no explicit empty-state signal → probably a selector miss,
        # not a genuine empty page. Log a warning but do NOT stop.
        logger.warning(
            "Page returned no job cards and no recognisable empty-state element. "
            "The card selectors may need updating. Run --debug-html to inspect."
        )
        return False

    # ── Main loop ─────────────────────────────────────────────────────────────
    def run(self) -> list[dict]:
        logger.info(f"Scraping Wuzzuf for: '{self.keyword}' (up to {self.max_pages} pages)")

        for page in range(1, self.max_pages + 1):
            logger.info(f"Page {page}/{self.max_pages} ...")
            scraped_at = datetime.now(timezone.utc).isoformat()

            soup = self._get_page(page)
            if soup is None:
                logger.warning(f"Skipping page {page} (failed to fetch)")
                continue

            # Stop early only when the page genuinely has no results
            if self._is_empty_results_page(soup):
                logger.info("Wuzzuf reports no more results — stopping early.")
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
        self.output_dir.mkdir(parents=True, exist_ok=True)

        keyword_slug = re.sub(r"[^a-z0-9]+", "_", self.keyword.lower()).strip("_")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"wuzzuf_{keyword_slug}_{timestamp}.json"
        output_path = self.output_dir / filename

        payload = {
            "metadata": {
                "keyword":       self.keyword,
                "total_jobs":    len(self.jobs),
                "pages_scraped": self.max_pages,
                "saved_at":      datetime.now(timezone.utc).isoformat(),
                "source":        "wuzzuf.net",
            },
            "jobs": self.jobs,
        }

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

        logger.info(f"Saved {len(self.jobs)} jobs → {output_path}")
        return output_path


# ── Debug helper ──────────────────────────────────────────────────────────────
def debug_html(html_path: str):
    """
    Inspect a saved HTML file so you can quickly find the right selectors.

    Usage:
        python scraper/wuzzuf_scraper.py --debug-html data/raw/page1.html

    Save a page with:
        curl -A "Mozilla/5.0..." "https://wuzzuf.net/search/jobs/?q=data+engineer" \
             -o data/raw/page1.html
    """
    soup = BeautifulSoup(Path(html_path).read_text(encoding="utf-8"), "lxml")

    print("\n=== DEBUG REPORT ===\n")
    print(f"Page title : {soup.title.get_text() if soup.title else 'N/A'}")
    print(f"Total tags : {len(soup.find_all(True))}")

    # Look for title-link pattern
    title_links = soup.select("h2 a[href*='/jobs/p/']")
    print(f"\nTitle links found (h2 a[href*='/jobs/p/']): {len(title_links)}")
    for tl in title_links[:3]:
        print(f"  {tl.get_text(strip=True)!r:50s}  href={tl['href'][:60]}")

    # Show ancestor chain + dump full first card HTML
    if title_links:
        parent = title_links[0]
        print("\nAncestor chain of first title link:")
        ancestors = []
        for _ in range(6):
            if not parent.parent:
                break
            parent = parent.parent
            cls = " ".join(parent.get("class", []))[:60]
            data_id = parent.get("data-jobid", "")
            ancestors.append(parent)
            print(f"  <{parent.name}> class={cls!r}  data-jobid={data_id!r}")

        # Dump the card root (3 levels up from the <a> = 2 levels up from <h2>)
        card_root = title_links[0]
        for _ in range(3):
            if card_root.parent:
                card_root = card_root.parent
        print("\n--- First card HTML (truncated to 3000 chars) ---")
        print(card_root.prettify()[:3000])
        print("--- End of card HTML ---")

    # Show company links
    company_links = soup.select("a[href*='/jobs/c/']")
    print(f"\nCompany links (a[href*='/jobs/c/']): {len(company_links)}")
    for cl in company_links[:3]:
        print(f"  {cl.get_text(strip=True)!r}")

    # Show skill/tag links
    for pattern in ["filters%5Bskill", "filters[skill]"]:
        skill_links = soup.select(f"a[href*='{pattern}']")
        if skill_links:
            print(f"\nSkill links (href*='{pattern}'): {len(skill_links)}")
            print("  Sample:", [s.get_text(strip=True) for s in skill_links[:5]])
            break

    print("\n=== END DEBUG ===\n")


# ── CLI entry point ───────────────────────────────────────────────────────────
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scrape job listings from Wuzzuf.net"
    )
    parser.add_argument("--keyword", "-k", type=str, default="data engineer",
                        help="Search keyword (default: 'data engineer')")
    parser.add_argument("--max-pages", "-p", type=int, default=5,
                        help="Maximum number of result pages to scrape (default: 5)")
    parser.add_argument("--output-dir", "-o", type=Path, default=Path("data/raw"),
                        help="Directory to save JSON output (default: data/raw)")
    parser.add_argument("--debug-html", type=str, default=None, metavar="FILE",
                        help="Inspect a saved HTML file to help update selectors")
    return parser.parse_args()


def main():
    args = parse_args()

    if args.debug_html:
        debug_html(args.debug_html)
        return

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
        logger.warning(
            "No jobs collected.\n"
            "  1. Save a page manually:\n"
            "       curl -A \"Mozilla/5.0\" \"https://wuzzuf.net/search/jobs/?q=data+engineer\" -o page.html\n"
            "  2. Run the debug helper:\n"
            "       python scraper/wuzzuf_scraper.py --debug-html page.html\n"
            "  3. Update CARD_SELECTORS / FIELD_SELECTORS at the top of the file."
        )


if __name__ == "__main__":
    main()