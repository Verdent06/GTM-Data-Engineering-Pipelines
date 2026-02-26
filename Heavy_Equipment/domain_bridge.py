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

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

DEFAULT_INPUT  = "heavy_equipment_michigan_leads.csv"
DEFAULT_OUTPUT = "heavy_equipment_michigan_leads_with_domains.csv"

# Seconds to sleep between searches (random jitter between MIN and MAX).
# DDG's free tier is lenient, but anything faster than ~1 req/sec risks a
# 202 / Ratelimit response.  2–4 s is safe for a one-off 45-row run.
DELAY_MIN = 2.0
DELAY_MAX = 4.0

# Number of retries on rate-limit / connection errors before giving up.
MAX_RETRIES = 3

# Domains to skip — these are generic directories, not the dealer's own site.
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

# ---------------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def build_queries(company: str, city: str, state: str) -> list[str]:
    """
    Return a tiered list of queries to try in order.

    Why no exact quotes?  DDG (and its Bing backend) returns zero organic
    results for obscure local SMBs when the name is wrapped in "quotes" —
    the index simply doesn't have an exact-phrase match.  Unquoted queries
    let DDG apply its own relevance ranking and still surface the right site.

    Tier 1 — tightest: full name + location + industry signal
    Tier 2 — looser:   full name + state only (drops city noise)
    Tier 3 — bare:     full name + state (no industry term, catches
                        companies whose sites don't use "equipment dealer")
    """
    co    = company.title()   # "AIS CONSTRUCTION" → "Ais Construction" (less spammy)
    state_full = "Michigan"   # spell out for better geo disambiguation
    return [
        f"{co} {city} {state_full} equipment dealer",
        f"{co} {state_full} equipment dealer",
        f"{co} {city} {state_full}",
    ]


def extract_root_domain(url: str) -> str:
    """
    Given any URL, return the registrable root domain.
    e.g. https://www.aisparts.com/contact  →  aisparts.com
    """
    ext = tldextract.extract(url)
    if ext.domain and ext.suffix:
        return f"{ext.domain}.{ext.suffix}".lower()
    return ""


def is_valid_result(domain: str, skip_set: set) -> bool:
    """Return True if domain is non-empty and not in the skip list."""
    return bool(domain) and domain not in skip_set


def search_domain_one(ddgs: DDGS, query: str, skip_set: set) -> str:
    """
    Run a single DDG text search and return the first root domain that
    passes the skip-list filter.  Returns "" if nothing useful is found.
    Raises on network/rate-limit errors (caller handles retries).
    """
    results = ddgs.text(query, max_results=6)   # list, not generator
    if not results:
        return ""
    for hit in results:
        href = hit.get("href", "") or hit.get("url", "")
        domain = extract_root_domain(href)
        if is_valid_result(domain, skip_set):
            return domain
    return ""


# ---------------------------------------------------------------------------
# MAIN PIPELINE
# ---------------------------------------------------------------------------

def run(input_file: str, output_file: str):
    # ---- Load CSV ----
    df = pd.read_csv(input_file, dtype=str).fillna("")
    required = {"company", "city", "state"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"CSV is missing required columns: {missing}")

    log.info(f"Loaded {len(df)} rows from '{input_file}'")

    # ---- Resume support: skip rows that already have a website ----
    if "website" not in df.columns:
        df["website"] = ""

    rows_to_search = df[df["website"] == ""].index.tolist()
    log.info(f"Rows needing search: {len(rows_to_search)}")

    # ---- Search loop ----
    with DDGS() as ddgs:
        for pos, idx in enumerate(rows_to_search, start=1):
            row     = df.loc[idx]
            company = row["company"].strip()
            city    = row["city"].strip()
            state   = row["state"].strip()
            queries = build_queries(company, city, state)

            log.info(f"[{pos}/{len(rows_to_search)}] {company} — {city}, {state}")

            domain = ""

            # --- Tiered query loop ---
            for tier, query in enumerate(queries, start=1):
                if domain:
                    break
                log.debug(f"  Tier {tier} query: {query}")

                for attempt in range(1, MAX_RETRIES + 1):
                    try:
                        domain = search_domain_one(ddgs, query, SKIP_DOMAINS)
                        if domain:
                            log.info(f"  ✓  [{tier}] Found: {domain}")
                        break   # success (even if empty) — move to next tier
                    except Exception as exc:
                        err_msg = str(exc)
                        if attempt < MAX_RETRIES:
                            wait = DELAY_MAX * attempt
                            log.warning(f"  ⚠  Error tier {tier} attempt {attempt}: "
                                        f"{err_msg}. Retrying in {wait:.0f}s…")
                            time.sleep(wait)
                        else:
                            log.error(f"  ✗  Tier {tier} failed after {MAX_RETRIES} attempts: {err_msg}")

                # Brief pause between tiers so we don't hammer DDG
                if tier < len(queries) and not domain:
                    time.sleep(random.uniform(1.0, 2.0))

            if not domain:
                log.warning(f"  –  No domain found after all tiers")

            df.at[idx, "website"] = domain

            # ---- Save checkpoint after every row ----
            # This means a crash won't lose work already done.
            df.to_csv(output_file, index=False, encoding="utf-8")

            # ---- Polite delay (skip after last row) ----
            if pos < len(rows_to_search):
                sleep_for = random.uniform(DELAY_MIN, DELAY_MAX)
                time.sleep(sleep_for)

    # ---- Final save & summary ----
    df.to_csv(output_file, index=False, encoding="utf-8")
    found   = df["website"].ne("").sum()
    total   = len(df)
    log.info("=" * 60)
    log.info(f"✓ Done.  Output → '{output_file}'")
    log.info(f"  Domains found : {found}/{total}  ({found/total*100:.0f}%)")
    log.info(f"  Missing       : {total - found}")
    log.info("=" * 60)


# ---------------------------------------------------------------------------
# ENTRY POINT
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Heavy Equipment Domain Bridge")
    parser.add_argument("--input",  default=DEFAULT_INPUT,
                        help=f"Path to input CSV (default: {DEFAULT_INPUT})")
    parser.add_argument("--output", default=DEFAULT_OUTPUT,
                        help=f"Path to output CSV (default: {DEFAULT_OUTPUT})")
    args = parser.parse_args()

    run(args.input, args.output)