"""
domain_bridge.py — Domain Bridge
=======================================
Reads heavy_equipment_michigan_leads.csv, searches DuckDuckGo for each company's
website, extracts the root domain from the first organic result, and
writes heavy_equipment_michigan_leads_with_domains.csv with a `website` column appended.

Install deps:
    pip install ddgs pandas tldextract

Usage:
    python domain_bridge.py
    python domain_bridge.py --input path/to/leads.csv --output path/to/out.csv

FIXES vs v1:
  - Swapped `duckduckgo_search` (broken/renamed) for `ddgs` (current package)
  - Removed exact-phrase quotes around company name — DDG returns 0 results
    for obscure local businesses when the name is quoted strictly
  - Added tiered fallback query strategy so rare names still resolve
"""

import argparse
import logging
import time
import random
import pandas as pd
import tldextract
from ddgs import DDGS

DEFAULT_INPUT  = "heavy_equipment_michigan_leads.csv"
DEFAULT_OUTPUT = "heavy_equipment_michigan_leads_with_domains.csv"

# delay between DDG queries (they'll rate limit you if you go too fast)
DELAY_MIN = 2.0
DELAY_MAX = 4.0

# bail out after this many retries on network errors
MAX_RETRIES = 3

# skip these junk results (marketplace listings, social media, etc)
SKIP_DOMAINS = {
    "machinerytrader.com", "ironplanet.com", "equipmenttrader.com",
    "yellowpages.com", "yelp.com", "facebook.com", "linkedin.com",
    "instagram.com", "twitter.com", "x.com", "bbb.org",
    "manta.com", "bizapedia.com", "dnb.com", "zoominfo.com",
    "indeed.com", "glassdoor.com", "bloomberg.com", "mapquest.com",
    "google.com", "bing.com", "duckduckgo.com",
    "wikipedia.org", "wikidata.org",
    "apple.com", "maps.apple.com",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

def build_queries(company: str, city: str, state: str) -> list[str]:
    """Build fallback queries for DDG search (tightest to loosest)."""
    # unquoted queries work way better for local SMBs on DDG
    co    = company.title()
    state_full = "Michigan"
    return [
        f"{co} {city} {state_full} equipment dealer",
        f"{co} {state_full} equipment dealer",
        f"{co} {city} {state_full}",
    ]


def extract_root_domain(url: str) -> str:
    """Extract root domain from any URL (handles subdomains/paths)."""
    ext = tldextract.extract(url)
    if ext.domain and ext.suffix:
        return f"{ext.domain}.{ext.suffix}".lower()
    return ""


def is_valid_result(domain: str, skip_set: set) -> bool:
    """Return True if domain is non-empty and not in the skip list."""
    return bool(domain) and domain not in skip_set


def search_domain_one(ddgs: DDGS, query: str, skip_set: set) -> str:
    """One DDG query, return first useful domain or empty string."""
    results = ddgs.text(query, max_results=6)
    if not results:
        return ""
    for hit in results:
        href = hit.get("href", "") or hit.get("url", "")
        domain = extract_root_domain(href)
        if is_valid_result(domain, skip_set):
            return domain
    return ""

def run(input_file: str, output_file: str):
    # load the lead list
    df = pd.read_csv(input_file, dtype=str).fillna("")
    required = {"company", "city", "state"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"CSV is missing required columns: {missing}")

    log.info(f"Loaded {len(df)} rows from '{input_file}'")

    # skip rows that already have a domain (supports resuming)
    if "website" not in df.columns:
        df["website"] = ""

    rows_to_search = df[df["website"] == ""].index.tolist()
    log.info(f"Rows needing search: {len(rows_to_search)}")

    with DDGS() as ddgs:
        for pos, idx in enumerate(rows_to_search, start=1):
            row     = df.loc[idx]
            company = row["company"].strip()
            city    = row["city"].strip()
            state   = row["state"].strip()
            queries = build_queries(company, city, state)

            log.info(f"[{pos}/{len(rows_to_search)}] {company} — {city}, {state}")

            domain = ""

            # try increasingly broad queries until we find something
            for tier, query in enumerate(queries, start=1):
                if domain:
                    break
                log.debug(f"  Tier {tier} query: {query}")

                for attempt in range(1, MAX_RETRIES + 1):
                    try:
                        domain = search_domain_one(ddgs, query, SKIP_DOMAINS)
                        if domain:
                            log.info(f"  ✓  [{tier}] Found: {domain}")
                        break   # got a result (or confirmed nothing), move to next tier
                    except Exception as exc:
                        err_msg = str(exc)
                        if attempt < MAX_RETRIES:
                            wait = DELAY_MAX * attempt
                            log.warning(f"  ⚠  Error tier {tier} attempt {attempt}: "
                                        f"{err_msg}. Retrying in {wait:.0f}s…")
                            time.sleep(wait)
                        else:
                            log.error(f"  ✗  Tier {tier} failed after {MAX_RETRIES} attempts: {err_msg}")

                # don't hammer DDG between queries
                if tier < len(queries) and not domain:
                    time.sleep(random.uniform(1.0, 2.0))

            if not domain:
                log.warning(f"  –  No domain found after all tiers")

            df.at[idx, "website"] = domain

            # checkpoint after every row so a crash doesn't lose progress
            df.to_csv(output_file, index=False, encoding="utf-8")

            # throttle between requests (last row skip not needed, we're done anyway)
            if pos < len(rows_to_search):
                sleep_for = random.uniform(DELAY_MIN, DELAY_MAX)
                time.sleep(sleep_for)

    # final save and stats
    df.to_csv(output_file, index=False, encoding="utf-8")
    found   = df["website"].ne("").sum()
    total   = len(df)
    log.info("=" * 60)
    log.info(f"✓ Done.  Output → '{output_file}'")
    log.info(f"  Domains found : {found}/{total}  ({found/total*100:.0f}%)")
    log.info(f"  Missing       : {total - found}")
    log.info("=" * 60)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Heavy Equipment Domain Bridge")
    parser.add_argument("--input",  default=DEFAULT_INPUT,
                        help=f"Path to input CSV (default: {DEFAULT_INPUT})")
    parser.add_argument("--output", default=DEFAULT_OUTPUT,
                        help=f"Path to output CSV (default: {DEFAULT_OUTPUT})")
    args = parser.parse_args()

    run(args.input, args.output)