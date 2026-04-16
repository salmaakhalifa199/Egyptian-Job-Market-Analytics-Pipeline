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

from playwright.sync_api import sync_playwright

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
# ── Known job-type / work-mode values ────────────────────────────────────────
# Wuzzuf's current DOM appends work-mode text (On-site / Hybrid / Remote) to
# the location span, and job-type text leaks into company fallback anchors.
# These sets are used to strip / detect those values during parsing.
KNOWN_WORK_MODES = {"on-site", "hybrid", "remote"}
KNOWN_JOB_TYPES  = {
    "full time", "part time", "internship",
    "freelance / project", "freelance", "contract",
    "temporary", "volunteer",
}

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


def _first(card, selectors):
    """Try selectors in order, return the first match inside *card*."""
    if isinstance(selectors, str):
        selectors = [selectors]
    for sel in selectors:
        found = card.query_selector(sel)
        if found:
            return found
    return None


def _all(card, selectors):
    """Try selectors in order, return all matches for the first that hits."""
    if isinstance(selectors, str):
        selectors = [selectors]
    for sel in selectors:
        found = card.query_selector_all(sel)
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
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(headless=True)
        self.context = self.browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            )
        )
        self.page = self.context.new_page()
        self.jobs: list[dict] = []

    # ── HTTP ──────────────────────────────────────────────────────────────────
    def _get_page(self, page: int) -> bool:
        params = {"q": self.keyword, "a[page]": page}
        url = f"{BASE_URL}?q={self.keyword}&a%5Bpage%5D={page}"
        try:
            self.page.goto(url, timeout=REQUEST_TIMEOUT * 1000)
            return True
        except Exception as e:
            logger.error(f"Failed to load page {page}: {e}")
            return False

    # ── Card discovery ────────────────────────────────────────────────────────
    def _find_cards(self) -> list:
        """
        Locate all job cards on the page using stable selectors.
        Falls back to a heuristic: any element that directly contains a
        title link (h2 > a[href*='/jobs/p/']).
        """
        for sel in CARD_SELECTORS:
            cards = self.page.query_selector_all(sel)
            if cards:
                logger.debug(f"Card selector matched: {sel!r} → {len(cards)} cards")
                return cards

        # Heuristic fallback: find all title anchors, then take their grandparent
        title_links = self.page.query_selector_all("h2 a[href*='/jobs/p/']")
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
                    if container:
                        container = container.query_selector("xpath=..")
                if container and id(container) not in seen:
                    seen.add(id(container))
                    cards.append(container)
            return cards

        return []

    # ── Parsing ───────────────────────────────────────────────────────────────
    def _parse_job_card(self, card, scraped_at: str) -> dict | None:
        try:
            # ── Title + URL ──────────────────────────────────────────────────
            title_tag = _first(card, FIELD_SELECTORS["title_link"])
            if not title_tag:
                return None
            title = title_tag.inner_text().strip()
            url = title_tag.get_attribute("href") or ""
            if not url.startswith("http"):
                url = "https://wuzzuf.net" + url

            # ── Job ID ───────────────────────────────────────────────────────
            # Prefer the data-jobid attribute on the card itself
            job_id = card.get_attribute("data-jobid") or ""
            if not job_id:
                m = re.search(r"/jobs/p/([^/?#]+)", url)
                job_id = m.group(1) if m else url.split("/")[-1]

            # ── Company ──────────────────────────────────────────────────────
            company_tag = _first(card, FIELD_SELECTORS["company"])
            if company_tag:
                company = company_tag.inner_text().strip()
            else:
                # Positional fallback: first <a> that is NOT the title link
                # AND whose text is not a known job-type value
                company = "Unknown"
                for a in card.query_selector_all("a[href]"):
                    href = a.get_attribute("href") or ""
                    text = a.inner_text().strip()
                    if (
                        "/jobs/p/" not in href
                        and text
                        and text.lower() not in KNOWN_JOB_TYPES
                        and text.lower() not in KNOWN_WORK_MODES
                    ):
                        company = text
                        break
            # Strip trailing " -" artifact present in Wuzzuf's current markup
            company = re.sub(r"\s*-\s*$", "", company).strip()

            # ── Location + job_type (co-located in the same span) ─────────────
            # April 2026: Wuzzuf renders location AND work-mode/job-type in one
            # span, e.g. "Cairo, Egypt, On-site" or "Cairo, Egypt, Hybrid".
            # We split off the last comma-segment when it's a known value.
            location_tag = _first(card, FIELD_SELECTORS["location"])
            if location_tag:
                raw_loc = location_tag.inner_text().strip()
            else:
                skip_kw = {"yrs", "year"}
                spans = []
                for s in card.query_selector_all("span"):
                    text = s.inner_text().strip()
                    if text and len(text) < 60 and not any(kw in text.lower() for kw in skip_kw):
                        # ElementHandle-compatible ancestor check:
                        # evaluate() runs JS in the browser context so it works
                        # with all Playwright versions (no .locator() needed)
                        inside_h2 = s.evaluate(
                            "el => el.closest('h2') !== null"
                        )
                        if not inside_h2:
                            spans.append(text)
                raw_loc = ", ".join(spans[:2]) if spans else "Egypt"

            # Parse location + work-mode out of the combined string
            loc_parts = [p.strip() for p in raw_loc.split(",")]
            extracted_job_type = "Unknown"
            # Walk from the end — strip any known work-mode or job-type suffixes
            while loc_parts and loc_parts[-1].lower() in KNOWN_WORK_MODES | KNOWN_JOB_TYPES:
                last = loc_parts.pop()
                if extracted_job_type == "Unknown":
                    extracted_job_type = last   # keep the first one found
            location = ", ".join(loc_parts) if loc_parts else raw_loc

            # ── Job type ─────────────────────────────────────────────────────
            # First try explicit selectors; fall back to what we extracted above
            job_type_tag = _first(card, FIELD_SELECTORS["job_type"])
            if job_type_tag:
                job_type = job_type_tag.inner_text().strip()
            else:
                job_type = extracted_job_type

            # ── Experience ───────────────────────────────────────────────────
            exp_tag = _first(card, FIELD_SELECTORS["experience"])
            if exp_tag:
                experience = exp_tag.inner_text().strip()
            else:
                # Structural fallback: scan all span/p text for "Yrs" / "Yr"
                # pattern — e.g. "2 - 4 Yrs of Exp", "0 - 1 Yr of Exp"
                experience = "Not specified"
                for el in card.query_selector_all("span, p, div"):
                    text = el.inner_text().strip()
                    if re.search(r"\d+.*yr", text, re.IGNORECASE) and len(text) < 50:
                        experience = text
                        break

            # ── Skills ───────────────────────────────────────────────────────
            skill_tags = _all(card, FIELD_SELECTORS["skills"])
            if skill_tags:
                skills = [s.inner_text().strip() for s in skill_tags if s.inner_text().strip()]
            else:
                # Structural fallback: small <a> tags whose href contains
                # filters[skill] OR whose text is short (≤25 chars) and
                # does NOT look like a location / job-type / company link
                skills = []
                for a in card.query_selector_all("a[href]"):
                    href = a.get_attribute("href") or ""
                    text = a.inner_text().strip()
                    if (
                        "skill" in href.lower()
                        or "tag" in href.lower()
                        or (
                            "/jobs/p/" not in href
                            and "/jobs/c/" not in href
                            and "/company/" not in href
                            and text
                            and len(text) <= 25
                            and text.lower() not in KNOWN_JOB_TYPES
                            and text.lower() not in KNOWN_WORK_MODES
                            and not re.search(r"\d+\s*(yr|yrs|year)", text, re.I)
                        )
                    ):
                        skills.append(text)

            # ── Posted date ──────────────────────────────────────────────────
            date_tag = _first(card, FIELD_SELECTORS["posted_date"])
            if date_tag:
                posted_date = date_tag.get_attribute("datetime") or date_tag.inner_text().strip()
            else:
                # Structural fallback: look for "ago" or date-like patterns
                posted_date = "Unknown"
                for el in card.query_selector_all("span, div, time, p"):
                    text = el.inner_text().strip()
                    if re.search(r"\d+\s*(hour|day|week|month)s?\s*ago", text, re.I):
                        posted_date = text
                        break
                    if re.search(r"\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\b",
                                 text, re.I) and len(text) < 30:
                        posted_date = text
                        break

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

    def _parse_page(self, scraped_at: str) -> list[dict]:
        cards = self._find_cards()
        logger.info(f"  Found {len(cards)} job cards on this page")

        jobs = []
        for card in cards:
            job = self._parse_job_card(card, scraped_at)
            if job:
                jobs.append(job)
        return jobs

    # ── No-results detection ──────────────────────────────────────────────────
    def _is_empty_results_page(self) -> bool:
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
        has_jobs = bool(self.page.query_selector_all("h2 a[href*='/jobs/p/']"))
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
            if self.page.query_selector(sel):
                logger.debug(f"Empty-state element matched: {sel!r}")
                return True

        # Condition 2b: a heading (h1–h3) whose text is the "no results" message
        for heading in self.page.query_selector_all("h1, h2, h3"):
            text = heading.inner_text().strip().lower()
            if re.search(r"no (results|jobs|vacancies) found|0 jobs", text):
                logger.debug(f"Empty-state heading found: {heading.inner_text().strip()!r}")
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

            success = self._get_page(page)
            if not success:
                logger.warning(f"Skipping page {page} (failed to fetch)")
                continue

            # Stop early only when the page genuinely has no results
            if self._is_empty_results_page():
                logger.info("Wuzzuf reports no more results — stopping early.")
                break

            page_jobs = self._parse_page(scraped_at)
            # Deduplicate: Wuzzuf sometimes returns the same cards across pages
            seen_ids = {j["job_id"] for j in self.jobs}
            new_jobs = [j for j in page_jobs if j["job_id"] not in seen_ids]
            dupes = len(page_jobs) - len(new_jobs)
            if dupes:
                logger.warning("  Skipped %d duplicate job(s) on page %d", dupes, page)

            # ── ADD THIS ──────────────────────────────────────────────────────────────────
            if dupes == len(page_jobs) and page_jobs:
                logger.info("  Entire page %d was duplicates — Wuzzuf pagination exhausted, stopping early.", page)
                break
            # ─────────────────────────────────────────────────────────────────────────────

            self.jobs.extend(new_jobs)
            logger.info("  Total collected so far: %d", len(self.jobs))

            if page < self.max_pages:
                time.sleep(DELAY_SECONDS)

        logger.info(f"Scraping complete. Total jobs collected: {len(self.jobs)}")
        return self.jobs

    def close(self):
        self.context.close()
        self.browser.close()
        self.playwright.stop()

    # ── Output ────────────────────────────────────────────────────────────────
    def save(self) -> Path:
        """
        Persist scraped jobs to a FIXED file per keyword (no timestamp).
        Upsert logic:
          - Existing job_ids → update mutable fields (scraped_at, skills, etc.)
          - New job_ids      → append
        """
        self.output_dir.mkdir(parents=True, exist_ok=True)
 
        keyword_slug = re.sub(r"[^a-z0-9]+", "_", self.keyword.lower()).strip("_")
        filename     = f"wuzzuf_{keyword_slug}.json"          # ← fixed, no timestamp
        output_path  = self.output_dir / filename
 
        # ── Load existing file ───────────────────────────────────────────────
        existing_jobs: dict[str, dict] = {}
        if output_path.exists():
            try:
                with open(output_path, encoding="utf-8") as f:
                    old_data = json.load(f)
                for job in old_data.get("jobs", []):
                    existing_jobs[job["job_id"]] = job
            except (json.JSONDecodeError, KeyError):
                logger.warning("Existing file %s was unreadable — starting fresh.", filename)
 
        # ── Upsert ──────────────────────────────────────────────────────────
        updated  = 0
        appended = 0
        for job in self.jobs:
            if job["job_id"] in existing_jobs:
                existing_jobs[job["job_id"]].update(job)   # refresh all fields
                updated += 1
            else:
                existing_jobs[job["job_id"]] = job
                appended += 1
 
        merged_jobs = list(existing_jobs.values())
 
        payload = {
            "metadata": {
                "keyword":        self.keyword,
                "total_jobs":     len(merged_jobs),
                "pages_scraped":  self.max_pages,
                "last_updated":   datetime.now(timezone.utc).isoformat(),
                "updated_count":  updated,
                "appended_count": appended,
                "source":         "wuzzuf.net",
            },
            "jobs": merged_jobs,
        }
 
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
 
        logger.info(
            "Saved %s → %s  (updated=%d  appended=%d  total=%d)",
            filename, output_path, updated, appended, len(merged_jobs),
        )
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
            "  1. Check your internet connection.\n"
            "  2. The selectors may need updating. Run --debug-html on a saved page.\n"
            "  3. Update CARD_SELECTORS / FIELD_SELECTORS at the top of the file."
        )
    scraper.close()


if __name__ == "__main__":
    main()