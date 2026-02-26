# uses hunter.io to find owner emails from domains
# 25 free searches/month, use wisely

import argparse
import logging
import os
import time
import random
import requests
import pandas as pd

DEFAULT_INPUT  = "chasi_michigan_leads_with_domains.csv"
DEFAULT_OUTPUT = "chasi_michigan_leads_enriched.csv"

HUNTER_API_KEY = os.environ.get("HUNTER_API_KEY", "")

# owner > founder > president etc for picking best result
TARGET_TITLES = ["owner", "founder", "president", "ceo", "co-owner", "partner"]

DELAY_MIN = 1.5
DELAY_MAX = 3.0
MAX_RETRIES = 3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

HUNTER_URL = "https://api.hunter.io/v2/domain-search"

def hunter_search(domain: str) -> dict:
    # hit hunter api, get best contact (owner/founder/pres/etc)
    params = {
        "domain":    domain,
        "api_key":   HUNTER_API_KEY,
        "seniority": "executive",
        "type":      "personal",
        "limit":     10,
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(HUNTER_URL, params=params, timeout=15)

            if resp.status_code == 429:
                wait = DELAY_MAX * attempt
                log.warning(f"  Rate limited. Waiting {wait:.0f}s…")
                time.sleep(wait)
                continue

            if resp.status_code == 403:
                log.error("  Hunter 403 — monthly search limit reached. Stop the script.")
                return {}

            resp.raise_for_status()
            data = resp.json()

            emails = data.get("data", {}).get("emails", [])

            if not emails:
                log.info(f"  – Hunter: no emails found for {domain}")
                return {}

            # pick the best one - owner beats founder beats pres etc
            def rank(e):
                pos = (e.get("position") or "").lower()
                title_score = len(TARGET_TITLES)
                for i, t in enumerate(TARGET_TITLES):
                    if t in pos:
                        title_score = i
                        break
                confidence = e.get("confidence", 0)
                return (title_score, -confidence)

            best = sorted(emails, key=rank)[0]

            first = (best.get("first_name") or "").strip()
            last  = (best.get("last_name")  or "").strip()
            name  = f"{first} {last}".strip()
            title = (best.get("position")   or "").strip()
            email = (best.get("value")      or "").strip()
            conf  = best.get("confidence", 0)

            if not email:
                return {}

            return {
                "name":       name,
                "title":      title,
                "email":      email,
                "confidence": conf,
            }

        except requests.exceptions.HTTPError as e:
            log.warning(f"  Hunter HTTP {e.response.status_code} for {domain}: {e.response.text[:150]}")
            if attempt < MAX_RETRIES:
                time.sleep(DELAY_MAX * attempt)
                continue
            return {}

        except Exception as e:
            log.warning(f"  Hunter error for {domain} (attempt {attempt}): {e}")
            if attempt < MAX_RETRIES:
                time.sleep(DELAY_MAX * attempt)
                continue
            return {}

    return {}


# ─────────────────────────────────────────
# MAIN PIPELINE
# ─────────────────────────────────────────

def run(input_file: str, output_file: str):

    if not HUNTER_API_KEY:
        raise SystemExit(
            "\n  ✗  No Hunter API key found.\n"
            "  Run: export HUNTER_API_KEY='your_key'\n"
            "  Get your free key at: hunter.io → Dashboard → API\n"
        )

    log.info("Hunter API key loaded ✓")

    # ── Load CSV ──────────────────────────
    df = pd.read_csv(input_file, dtype=str).fillna("")
    if "website" not in df.columns:
        raise ValueError("CSV must have a 'website' column")

    for col in ["owner_name", "owner_title", "owner_email", "email_source"]:
        if col not in df.columns:
            df[col] = ""

    # Only rows with a domain that haven't been enriched yet
    needs_enrich = df[
        (df["website"] != "") &
        (df["owner_name"]  == "") &
        (df["owner_email"] == "")
    ].index.tolist()

    log.info(f"Loaded {len(df)} rows — {len(needs_enrich)} need enrichment")
    log.info(f"This will use {len(needs_enrich)} of your 25 free Hunter searches this month.")

    if not needs_enrich:
        log.info("Nothing to enrich.")
        df.to_csv(output_file, index=False, encoding="utf-8")
        return

    # ── Enrichment loop ───────────────────
    searches_used = 0

    for pos, idx in enumerate(needs_enrich, start=1):
        row     = df.loc[idx]
        company = row.get("company", "").strip()
        domain  = row["website"].strip()

        log.info(f"[{pos}/{len(needs_enrich)}] {company} ({domain})")

        result = hunter_search(domain)
        searches_used += 1

        if result:
            df.at[idx, "owner_name"]   = result["name"]
            df.at[idx, "owner_title"]  = result["title"]
            df.at[idx, "owner_email"]  = result["email"]
            df.at[idx, "email_source"] = "hunter"
            log.info(f"  ✓  {result['name']} ({result['title']}) — {result['email']}  [confidence: {result['confidence']}]")
        else:
            df.at[idx, "email_source"] = "not_found"
            log.warning(f"  –  No owner contact found for {domain}")

        # Checkpoint save after every single row
        df.to_csv(output_file, index=False, encoding="utf-8")

        # Polite delay between requests
        if pos < len(needs_enrich):
            time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

    # ── Final save + summary ──────────────
    df.to_csv(output_file, index=False, encoding="utf-8")

    total  = len(needs_enrich)
    named  = df.loc[needs_enrich, "owner_name"].ne("").sum()
    emails = df.loc[needs_enrich, "owner_email"].ne("").sum()

    log.info("=" * 60)
    log.info(f"✓ Done.  Output → '{output_file}'")
    log.info(f"  Processed      : {total}")
    log.info(f"  Names found    : {named}/{total}  ({named/total*100:.0f}%)")
    log.info(f"  Emails found   : {emails}/{total}  ({emails/total*100:.0f}%)")
    log.info(f"  Hunter searches used: {searches_used}/25 free this month")
    log.info("=" * 60)

    preview = df[df["owner_email"] != ""][
        ["company", "owner_name", "owner_title", "owner_email"]
    ]
    if not preview.empty:
        log.info("\nEnriched contacts:")
        for _, r in preview.iterrows():
            log.info(f"  {r['company']:<35} {r['owner_name']:<25} {r['owner_title']:<20} {r['owner_email']}")


# ─────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Chasi Lead Enrichment v4 — Hunter.io")
    parser.add_argument("--input",  default=DEFAULT_INPUT,
                        help=f"Cleaned CSV with website column (default: {DEFAULT_INPUT})")
    parser.add_argument("--output", default=DEFAULT_OUTPUT,
                        help=f"Output CSV path (default: {DEFAULT_OUTPUT})")
    args = parser.parse_args()
    run(args.input, args.output)