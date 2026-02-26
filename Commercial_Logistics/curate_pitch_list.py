"""
curate_pitch_list.py — Filter FMCSA leads to pitch-ready prospects
===================================================================
Loads the full trucking_fleet_leads.csv, removes rows with:
  - missing emails
  - generic role-based emails (info@, dispatch@, office@, etc.)
  - personal email providers (gmail, yahoo, hotmail, aol)

Then takes a random 30-row sample and exports to trucking_pitch_ready.csv.
"""

import pandas as pd
import re
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

INPUT_FILE  = "trucking_fleet_leads.csv"
OUTPUT_FILE = "trucking_pitch_ready.csv"
SAMPLE_SIZE = 30

ROLE_PREFIXES = [
    "info@", "dispatch@", "office@", "admin@",
    "sales@", "contact@", "safety@", "billing@",
]

PERSONAL_DOMAINS = [
    "@gmail.com", "@yahoo.com", "@hotmail.com", "@aol.com",
    "@outlook.com", "@icloud.com", "@msn.com", "@live.com",
]


def is_role_based(email: str) -> bool:
    el = email.lower()
    return any(el.startswith(prefix) for prefix in ROLE_PREFIXES)


def is_personal_domain(email: str) -> bool:
    el = email.lower()
    return any(el.endswith(domain) for domain in PERSONAL_DOMAINS)


def main():
    logging.info(f"Loading {INPUT_FILE}...")
    df = pd.read_csv(INPUT_FILE, dtype=str)
    df.fillna("", inplace=True)
    logging.info(f"  Total rows: {len(df)}")

    # 1. Drop rows with no email
    df = df[df["email"].str.strip().ne("")]
    logging.info(f"  After dropping empty emails: {len(df)}")

    # 2. Drop role-based emails
    df = df[~df["email"].apply(is_role_based)]
    logging.info(f"  After dropping role-based emails: {len(df)}")

    # 3. Drop personal email providers
    df = df[~df["email"].apply(is_personal_domain)]
    logging.info(f"  After dropping personal domains: {len(df)}")

    if len(df) == 0:
        logging.error("No qualifying rows remain. Check filters.")
        return

    # 4. Random sample of 30 (or all if fewer than 30 remain)
    sample_n = min(SAMPLE_SIZE, len(df))
    df_sample = df.sample(n=sample_n, random_state=42)
    logging.info(f"  Random sample: {sample_n} rows")

    # 5. Drop empty enrichment columns
    drop_cols = ["owner_name", "owner_title", "owner_email", "apollo_org_id"]
    df_sample = df_sample.drop(columns=[c for c in drop_cols if c in df_sample.columns])

    # 6. Export
    df_sample.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")
    logging.info(f"\nExported to {OUTPUT_FILE}")
    logging.info(f"  Rows: {len(df_sample)}")


if __name__ == "__main__":
    main()
