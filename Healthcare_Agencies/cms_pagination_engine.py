import asyncio
import aiohttp
import pandas as pd
import re
import time
import logging
import os
import sys
import argparse
from collections import Counter
from dataclasses import dataclass, asdict
from typing import Optional
from urllib.parse import urlparse

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(levelname)-7s │ %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)

HUNTER_API_KEY = os.environ.get("HUNTER_API_KEY", "")
GOOGLE_PLACES_API_KEY = os.environ.get("GOOGLE_PLACES_API_KEY", "")

OUTPUT_FILE = "healthcare_agencies_fl_tx_az_leads.csv"
STATE_FILTER: list[str] = ["FL", "TX", "AZ"]

CMS_DATASTORE_BASE = "https://data.cms.gov/provider-data/api/1/datastore/query/6jpm-sxkc/0"
CMS_PAGE_SIZE = 1500

MAX_ENRICHMENT_CONCURRENCY = 5

# MochaCare pitch: never exceed 100 Hunter Domain Search calls per run (strict cap).
MAX_HUNTER_VIP_ENRICHMENTS = 100
# Hunter.io account ceiling (for logging only).
HUNTER_ACCOUNT_CREDIT_LIMIT = 500

# Pre-Places: allow lower initial scores through (continuous scoring + post-enrich bump).
INITIAL_PRE_PLACES_MIN_SCORE = 75
# Post domain/franchise filter + Hunter eligibility.
POST_FILTER_MIN_SCORE = 80

AGGREGATOR_DOMAINS = [
    "npino.com", "senioradvice.com", "github.io"
]

# Pre-discovery blocklist: national franchises + hospital / health-system names (before Places spend).
ENTERPRISE_BLOCKLIST_KEYWORDS = [
    "AMEDISYS", "LHC GROUP", "BAYADA", "ENHABIT", "ENCOMPASS", "CENTERWELL",
    "INTERIM", "KINDRED", "MAXIM", "BROOKDALE", "ACCENTCARE", "COMPASSUS",
    "HOSPITAL", "CLINIC", "HCA", "ASCENSION", "CHRISTUS", "DIGNITY",
    "ADVENTHEALTH", "MEMORIAL", "REGIONAL", "MEDICAL CENTER", "HEALTH SYSTEM",
]

# Pre-discovery blocklist: penalize national franchise brands in scoring (before Places spend).
FRANCHISE_KEYWORDS = [
    "AMEDISYS", "LHC", "BAYADA", "ENHABIT", "ENCOMPASS", "CENTERWELL",
    "INTERIM", "KINDRED", "MAXIM", "BROOKDALE", "ACCENTCARE", "COMPASSUS",
]

ENTERPRISE_DOMAINS = [
    "lhcgroup.com", "centerwell.com", "bayada.com", "interimhealthcare.com",
    "compassus.com", "christushealth.org", "hcahoustonhealthcare.com",
    "brooksrehab.org", "vnahg.org", "locations.dignityhealth.org",
]

@dataclass
class HHARecord:
    agency_name: str = ""
    address: str = ""
    city: str = ""
    state: str = ""
    zip_code: str = ""
    phone: str = ""
    cms_ccn: str = ""
    website: str = ""
    owner_name: str = ""
    owner_title: str = ""
    owner_email: str = ""
    email_source: str = ""
    ownership_type: str = ""
    star_rating: str = ""
    lead_score: int = 0


async def fetch_cms_hha_data(session: aiohttp.ClientSession, limit: Optional[int] = None) -> list[HHARecord]:
    log.info("=" * 70)
    log.info("STAGE 1: CMS DATA EXTRACTION (Datastore API)")
    log.info("=" * 70)
    
    log.info(f"  Base endpoint: {CMS_DATASTORE_BASE}")
    log.info(f"  Page size: {CMS_PAGE_SIZE}")
    log.info(f"  Fetching all HHA records from CMS (paginated)...")
    
    all_records = []
    offset = 0
    page_num = 1
    
    while True:
        url = f"{CMS_DATASTORE_BASE}?limit={CMS_PAGE_SIZE}&offset={offset}"
        log.info(f"  Page {page_num}: offset={offset}")
        
        try:
            async with session.get(
                url,
                timeout=aiohttp.ClientTimeout(total=60)
            ) as resp:
                if resp.status != 200:
                    log.error(f"  CMS Datastore API returned HTTP {resp.status}")
                    text = await resp.text()
                    log.error(f"  Response: {text[:500]}")
                    break
                
                data = await resp.json()
                
        except Exception as e:
            log.error(f"  CMS Datastore API error on page {page_num}: {e}")
            break
        
        results = data.get("results", [])
        
        if not results:
            log.info(f"  Page {page_num}: Empty results, end of data reached")
            break
        
        log.info(f"  Page {page_num}: Received {len(results)} records")
        all_records.extend(results)
        
        if len(results) < CMS_PAGE_SIZE:
            log.info(f"  Page {page_num}: Partial page ({len(results)} < {CMS_PAGE_SIZE}), end of data")
            break
        
        offset += CMS_PAGE_SIZE
        page_num += 1
    
    if not all_records:
        log.error("  No records fetched from CMS API")
        return []
    
    log.info(f"  Total HHA records fetched: {len(all_records)}")
    
    if all_records:
        columns = list(all_records[0].keys())
        log.info(f"  Available columns: {columns}")
    
    df = pd.DataFrame(all_records)
    
    log.info(f"  Filtering for states: {STATE_FILTER}")
    df_states = df[df["state"].isin(STATE_FILTER)]
    for st in STATE_FILTER:
        n = int((df["state"] == st).sum())
        log.info(f"    {st}: {n} records (pre-limit)")
    log.info(f"  Combined target-state records: {len(df_states)}")
    
    if limit:
        df_states = df_states.head(limit)
        log.info(f"  TEST MODE: Limited to {limit} rows")
    
    COL_NAME = 'provider_name'
    COL_ADDRESS = 'address'
    COL_CITY = 'citytown'
    COL_ZIP = 'zip_code'
    COL_PHONE = 'telephone_number'
    COL_CCN = 'cms_certification_number_ccn'
    
    log.info(f"  Column mapping:")
    log.info(f"    Name:    {COL_NAME}")
    log.info(f"    Address: {COL_ADDRESS}")
    log.info(f"    City:    {COL_CITY}")
    log.info(f"    Zip:     {COL_ZIP}")
    log.info(f"    Phone:   {COL_PHONE}")
    log.info(f"    CCN:     {COL_CCN}")
    
    records = []
    for _, row in df_states.iterrows():
        name = str(row.get(COL_NAME, "")).strip() if pd.notna(row.get(COL_NAME)) else ""
        
        if not name:
            continue
        
        st_val = row.get("state", "")
        row_state = str(st_val).strip().upper() if pd.notna(st_val) else ""
        
        record = HHARecord(
            agency_name=name,
            address=str(row.get(COL_ADDRESS, "")).strip() if pd.notna(row.get(COL_ADDRESS)) else "",
            city=str(row.get(COL_CITY, "")).strip() if pd.notna(row.get(COL_CITY)) else "",
            state=row_state,
            zip_code=str(row.get(COL_ZIP, ""))[:5] if pd.notna(row.get(COL_ZIP)) else "",
            phone=_format_phone(row.get(COL_PHONE, "")),
            cms_ccn=str(row.get(COL_CCN, "")).strip() if pd.notna(row.get(COL_CCN)) else "",
            ownership_type=str(row.get("type_of_ownership", "")).strip() if pd.notna(row.get("type_of_ownership")) else "",
            star_rating=str(row.get("quality_of_patient_care_star_rating", "")).strip() if pd.notna(row.get("quality_of_patient_care_star_rating")) else "",
        )
        records.append(record)
    
    log.info(f"  ✓ Extracted {len(records)} HHAs from CMS ({', '.join(STATE_FILTER)})")
    return records


def _format_phone(phone) -> str:
    if pd.isna(phone) or not phone:
        return ""
    digits = re.sub(r'\D', '', str(phone))
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    return str(phone).strip()


def _extract_root_domain(url: str) -> str:
    try:
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        if domain.startswith("www."):
            domain = domain[4:]
        return domain
    except Exception:
        return ""


def _is_aggregator_domain(domain: str) -> bool:
    domain_lower = domain.lower()
    for blocked in AGGREGATOR_DOMAINS:
        if blocked in domain_lower:
            return True
    return False


async def discover_domains(records: list[HHARecord]) -> list[HHARecord]:
    log.info("")
    log.info("=" * 70)
    log.info("STAGE 3: DOMAIN DISCOVERY (Google Places API)")
    log.info("=" * 70)
    
    if not GOOGLE_PLACES_API_KEY:
        log.error("  ✗ GOOGLE_PLACES_API_KEY not set.")
        log.error("  Set GOOGLE_PLACES_API_KEY environment variable.")
        return records
    
    found = 0
    blocked = 0
    not_found = 0
    
    async with aiohttp.ClientSession() as session:
        for i, record in enumerate(records):
            log.info(f"  [{i+1}/{len(records)}] Searching: {record.agency_name[:50]}...")
            
            query = f"{record.agency_name} in {record.city}, {record.state} {record.zip_code}"
            
            url = "https://places.googleapis.com/v1/places:searchText"
            headers = {
                "Content-Type": "application/json",
                "X-Goog-Api-Key": GOOGLE_PLACES_API_KEY,
                "X-Goog-FieldMask": "places.websiteUri",
            }
            body = {
                "textQuery": query,
                "maxResultCount": 1
            }
            
            try:
                async with session.post(
                    url,
                    json=body,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=15)
                ) as resp:
                    if resp.status != 200:
                        log.warning(f"    ⚠ Google Places API HTTP {resp.status}")
                        not_found += 1
                        continue
                    
                    data = await resp.json()
                
                places = data.get("places", [])
                if not places:
                    log.info(f"    ⚠ No results found")
                    not_found += 1
                    continue
                
                place = places[0]
                website_uri = place.get("websiteUri", "")
                
                if not website_uri:
                    log.info(f"    ⚠ No website found in result")
                    not_found += 1
                    continue
                
                domain = _extract_root_domain(website_uri)
                
                if not domain:
                    log.info(f"    ⚠ Invalid domain extracted from {website_uri}")
                    not_found += 1
                    continue
                
                if _is_aggregator_domain(domain):
                    log.info(f"    ✗ BLOCKED: {domain} (aggregator)")
                    blocked += 1
                    continue
                
                record.website = domain
                log.info(f"    ✓ FOUND: {domain}")
                found += 1
                
            except Exception as e:
                log.warning(f"    ⚠ Search failed: {e}")
                not_found += 1
    
    log.info("")
    log.info(f"  Domain Discovery Summary:")
    log.info(f"    ✓ Found:      {found}")
    log.info(f"    ✗ Blocked:    {blocked} (aggregator domains rejected)")
    log.info(f"    ⚠ Not found:  {not_found}")
    
    return records


def apply_anti_franchise_filter(
    records: list[HHARecord], max_occurrences: int = 2
) -> list[HHARecord]:
    """
    Drop records whose website appears more than max_occurrences times (shared domain = likely chain).
    Empty/missing websites are kept (nothing to treat as a chain signature).
    """
    log.info("")
    log.info("=" * 70)
    log.info("STAGE 5: ANTI-FRANCHISE FILTER")
    log.info("=" * 70)

    def norm_site(w: str) -> str:
        return (w or "").strip().lower()

    non_empty_keys = [norm_site(r.website) for r in records if norm_site(r.website)]
    freq = Counter(non_empty_keys)

    kept: list[HHARecord] = []
    purged = 0
    for r in records:
        key = norm_site(r.website)
        if not key:
            kept.append(r)
            continue
        if freq[key] > max_occurrences:
            log.info(
                f"  Dropped (chain): {r.agency_name[:60]} — "
                f"website {key} appears {freq[key]}× (max {max_occurrences})"
            )
            purged += 1
            continue
        kept.append(r)

    log.info("")
    log.info(
        f"  Anti-franchise summary: kept {len(kept)} record(s), "
        f"purged {purged} chain lead(s) (shared domain > {max_occurrences} occurrence(s))"
    )
    return kept


class EnrichmentClient:
    TITLE_PRIORITY = [
        "owner", "director", "administrator", "ceo", 
        "president", "founder", "principal", "executive director",
        "chief executive", "managing director", "general manager"
    ]
    
    def __init__(self):
        self._request_count = 0
        self._window_start = time.monotonic()
        self._session: Optional[aiohttp.ClientSession] = None
        self.hunter_credits_consumed = 0

    async def __aenter__(self):
        self._session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, *args):
        await self._session.close()

    async def _throttle(self):
        now = time.monotonic()
        if now - self._window_start >= 60:
            self._request_count = 0
            self._window_start = now
        self._request_count += 1

    async def enrich_via_hunter(self, domain: str) -> dict:
        if not HUNTER_API_KEY or not domain:
            return {}
        
        await self._throttle()
        params = {
            "domain": domain,
            "api_key": HUNTER_API_KEY,
            "limit": 10,
            "type": "personal"
        }
        
        try:
            # One Domain Search request ≈ one Hunter credit (count at send time).
            self.hunter_credits_consumed += 1
            async with self._session.get(
                "https://api.hunter.io/v2/domain-search",
                params=params,
                timeout=aiohttp.ClientTimeout(total=15)
            ) as resp:
                if resp.status != 200:
                    return {}
                data = await resp.json()
            
            emails = data.get("data", {}).get("emails", [])
            if not emails:
                return {}
            
            def rank(e):
                pos = (e.get("position") or "").lower()
                for i, kw in enumerate(self.TITLE_PRIORITY):
                    if kw in pos:
                        return i
                return 99
            
            best = sorted(emails, key=rank)[0]
            return {
                "owner_name": f"{best.get('first_name', '')} {best.get('last_name', '')}".strip(),
                "owner_title": best.get("position", ""),
                "owner_email": best.get("value", ""),
                "email_source": "hunter",
            }
            
        except Exception as e:
            log.error(f"    Hunter error for '{domain}': {e}")
            return {}

    async def enrich(self, record: HHARecord) -> HHARecord:
        domain = record.website.strip()
        
        result = await self.enrich_via_hunter(domain)
        
        for k, v in result.items():
            if hasattr(record, k):
                setattr(record, k, v)
        
        if not record.email_source:
            record.email_source = "not_found"
        
        return record


async def enrich_all(records: list[HHARecord]) -> list[HHARecord]:
    log.info("")
    log.info("=" * 70)
    log.info("STAGE 6: HUNTER ENRICHMENT (VIP LEADS)")
    log.info("=" * 70)
    
    if not HUNTER_API_KEY:
        log.warning("  ⚠ HUNTER_API_KEY not set. Skipping enrichment.")
        log.warning("  Set HUNTER_API_KEY environment variable.")
        return records
    
    valid_for_enrichment = []
    blocked_count = 0
    no_domain_count = 0
    low_score_count = 0
    
    for r in records:
        if r.lead_score < POST_FILTER_MIN_SCORE:
            low_score_count += 1
            continue
        
        if not r.website:
            no_domain_count += 1
            continue
        
        if _is_aggregator_domain(r.website):
            log.info(f"  ✗ BLOCKED for enrichment: {r.agency_name[:30]} ({r.website})")
            blocked_count += 1
            continue
        
        valid_for_enrichment.append(r)
    
    log.info(f"  Records with Score >= {POST_FILTER_MIN_SCORE}: {len(valid_for_enrichment)}")
    log.info(f"  Records with Score < {POST_FILTER_MIN_SCORE}: {low_score_count}")
    log.info(f"  Records without domains: {no_domain_count}")
    log.info(f"  Records with blocked domains: {blocked_count}")
    
    if not valid_for_enrichment:
        log.info("  No VIP leads to enrich.")
        return records
    
    to_enrich = valid_for_enrichment[:MAX_HUNTER_VIP_ENRICHMENTS]
    skipped_due_to_limit = len(valid_for_enrichment) - len(to_enrich)
    
    if len(to_enrich) > MAX_HUNTER_VIP_ENRICHMENTS:
        log.error("  ✗ HARD STOP: to_enrich exceeds MAX_HUNTER_VIP_ENRICHMENTS (logic error).")
        to_enrich = to_enrich[:MAX_HUNTER_VIP_ENRICHMENTS]
    
    log.info(f"  MochaCare pitch — Hunter HARD CAP: exactly {MAX_HUNTER_VIP_ENRICHMENTS} VIP Domain Search calls max per run.")
    log.info(
        f"  Hunter.io budget: this run will consume up to {len(to_enrich)} credit(s) "
        f"(1 per domain); account ceiling {HUNTER_ACCOUNT_CREDIT_LIMIT} credits."
    )
    log.info(
        f"  VIP pool: {len(valid_for_enrichment)} eligible (score {POST_FILTER_MIN_SCORE}+, valid domain). "
        f"Enriching first {len(to_enrich)} only (slice [:MAX_HUNTER_VIP_ENRICHMENTS] enforced)."
    )
    
    if skipped_due_to_limit > 0:
        log.info(f"  ⚠ Skipping {skipped_due_to_limit} additional VIP(s) past the {MAX_HUNTER_VIP_ENRICHMENTS}-lead cap")
    
    log.info("")
    
    enriched_map = {}
    enrichment_count = 0
    credits_used = 0
    
    async with EnrichmentClient() as client:
        for i, rec in enumerate(to_enrich):
            if i >= MAX_HUNTER_VIP_ENRICHMENTS:
                log.error("  ✗ HARD STOP: loop exceeded MAX_HUNTER_VIP_ENRICHMENTS — breaking.")
                break
            log.info(f"  [{i+1}/{len(to_enrich)}] Enriching: {rec.agency_name[:40]}... (Score: {rec.lead_score}, {rec.website})")
            
            try:
                enriched_rec = await client.enrich(rec)
                enriched_map[rec.cms_ccn] = enriched_rec
                enrichment_count += 1
                
                if enriched_rec.owner_email:
                    log.info(f"    ✓ Found: {enriched_rec.owner_email}")
                else:
                    log.info(f"    ⚠ No email found")
                    
            except Exception as e:
                log.error(f"    ✗ Error: {e}")
        credits_used = client.hunter_credits_consumed
    
    final_records = []
    for r in records:
        if r.cms_ccn in enriched_map:
            final_records.append(enriched_map[r.cms_ccn])
        else:
            final_records.append(r)
    
    emails_found = sum(1 for r in enriched_map.values() if r.owner_email)
    
    remaining_budget = max(0, HUNTER_ACCOUNT_CREDIT_LIMIT - credits_used)
    
    log.info("")
    log.info(f"  Enrichment Summary:")
    log.info(f"    ✓ Hunter Domain Search calls: {credits_used} (hard-capped at {MAX_HUNTER_VIP_ENRICHMENTS} per run)")
    log.info(f"    ✓ Hunter.io credits consumed this run: {credits_used} (account limit {HUNTER_ACCOUNT_CREDIT_LIMIT}; ~{remaining_budget} left if starting at full quota)")
    log.info(f"    ✓ Hunter loop completions (no exception): {enrichment_count}")
    log.info(f"    ✓ Emails found: {emails_found}/{enrichment_count}")
    log.info(
        f"    ℹ Rows unchanged by Hunter this run: {len(records) - len(enriched_map)} "
        f"(capped past {MAX_HUNTER_VIP_ENRICHMENTS}, no domain, blocked domain, or errors)"
    )
    
    return final_records


def calculate_initial_scores(records: list[HHARecord]) -> list[HHARecord]:
    """
    Initial scoring from CMS + agency name only (no website / Places data).
    Continuous star contribution (rating * 5); score is uncapped until post-enrichment bump.
    """
    log.info("")
    log.info("=" * 70)
    log.info("STAGE 2: INITIAL LEAD SCORING (CMS + NAME BLOCKLISTS)")
    log.info("=" * 70)
    
    for record in records:
        score = 40.0
        
        if record.ownership_type:
            ownership_upper = record.ownership_type.upper()
            if "PROPRIETARY" in ownership_upper:
                score += 20
                log.debug(f"  {record.agency_name}: +20 (PROPRIETARY)")
            elif "NON-PROFIT" in ownership_upper or "GOVERNMENT" in ownership_upper:
                score -= 10
                log.debug(f"  {record.agency_name}: -10 (NON-PROFIT/GOVERNMENT)")
        
        if record.star_rating and record.star_rating.strip() and record.star_rating.strip() != "-":
            try:
                rating = float(record.star_rating)
                score += rating * 5
                log.debug(f"  {record.agency_name}: +{rating * 5:.1f} (star × 5, rating={rating})")
            except ValueError:
                pass
        
        name_upper = record.agency_name.upper()
        if any(kw in name_upper for kw in ENTERPRISE_BLOCKLIST_KEYWORDS):
            score -= 100
            log.debug(f"  [{record.agency_name}]: -100 (ENTERPRISE BLOCKLIST)")
        if any(kw in name_upper for kw in FRANCHISE_KEYWORDS):
            score -= 50
            log.debug(f"  [{record.agency_name}]: -50 (FRANCHISE BLOCKLIST)")
        
        record.lead_score = int(round(score))
    
    log.info(
        f"  ✓ Initial scores computed for {len(records)} agencies "
        f"(continuous stars, uncapped; pre-Places cutoff ≥{INITIAL_PRE_PLACES_MIN_SCORE})"
    )
    
    return records


def apply_actionability_bump(records: list[HHARecord]) -> list[HHARecord]:
    """
    Post-Hunter: +15 when a real email is present; cap all scores at 100.
    """
    log.info("")
    log.info("=" * 70)
    log.info("STAGE 7: ACTIONABILITY BUMP (POST-ENRICHMENT)")
    log.info("=" * 70)
    
    bumped = 0
    for record in records:
        email = (record.owner_email or "").strip()
        if (
            email
            and email.lower() != "not_found"
            and "@" in email
        ):
            record.lead_score += 15
            bumped += 1
            log.debug(f"  [{record.agency_name}]: +15 (actionability / email found)")
        record.lead_score = min(100, record.lead_score)
    
    log.info(f"  ✓ Actionability bump: +15 applied to {bumped} record(s); all scores capped at 100")
    return records


def apply_domain_penalties(records: list[HHARecord]) -> list[HHARecord]:
    """
    After Places discovery: penalize enterprise domains on record.website.
    """
    log.info("")
    log.info("=" * 70)
    log.info("STAGE 4: ENTERPRISE DOMAIN PENALTIES")
    log.info("=" * 70)
    
    penalized = 0
    for record in records:
        website_lower = (record.website or "").lower()
        if website_lower and any(d in website_lower for d in ENTERPRISE_DOMAINS):
            record.lead_score -= 100
            penalized += 1
            log.warning(
                f"  [{record.agency_name}]: -100 (ENTERPRISE DOMAIN BLOCKLIST)"
            )
    
    log.info(f"  ✓ Domain penalty applied to {penalized} record(s)")
    return records


def export_to_csv(records: list[HHARecord], output_file: str):
    log.info("")
    log.info("=" * 70)
    log.info("STAGE 8: CSV EXPORT")
    log.info("=" * 70)
    
    df = pd.DataFrame([asdict(r) for r in records])
    
    col_order = [
        "lead_score", "star_rating", "ownership_type",
        "agency_name", "owner_name", "owner_title", "owner_email", "email_source",
        "phone", "address", "city", "state", "zip_code",
        "website", "cms_ccn"
    ]
    df = df[[c for c in col_order if c in df.columns]]
    
    df.to_csv(output_file, index=False, encoding="utf-8")
    
    log.info(f"  ✓ Output saved → '{output_file}'")
    log.info(f"    Total agencies:    {len(df)}")
    log.info(f"    With websites:     {df['website'].ne('').sum()}")
    log.info(f"    With emails:       {df['owner_email'].ne('').sum()}")
    
    if len(df) > 0:
        email_rate = df['owner_email'].ne('').sum() / len(df) * 100
        log.info(f"    Email coverage:    {email_rate:.1f}%")


async def run_pipeline(test_mode: bool = False):
    log.info("")
    log.info("╔" + "═" * 68 + "╗")
    log.info("║  HEALTHCARE AGENCIES MULTI-STATE PIPELINE v2.4 (FL/TX/AZ)           ║")
    log.info("║  Continuous score → pre-75 cut → Places → penalties → bump → export   ║")
    log.info("╚" + "═" * 68 + "╝")
    log.info("")
    
    if test_mode:
        log.info("🧪 TEST MODE ENABLED — Processing only 15 records")
        log.info("")
    
    limit = 15 if test_mode else None
    
    async with aiohttp.ClientSession() as session:
        records = await fetch_cms_hha_data(session, limit=limit)
    
    if not records:
        log.error("No records extracted. Exiting.")
        return
    
    records = calculate_initial_scores(records)
    
    before_first_cut = len(records)
    records = [r for r in records if r.lead_score >= INITIAL_PRE_PLACES_MIN_SCORE]
    dropped_first = before_first_cut - len(records)
    log.info("")
    log.info(
        f"  FIRST CUT (lead_score >= {INITIAL_PRE_PLACES_MIN_SCORE}, pre-Places): "
        f"kept {len(records)} / {before_first_cut} (dropped {dropped_first})"
    )
    
    if not records:
        log.error("No survivors after initial VIP cut — skipping Google Places. Exiting.")
        return
    
    records = await discover_domains(records)
    
    records = apply_domain_penalties(records)
    
    records = apply_anti_franchise_filter(records)
    
    before_final_cut = len(records)
    records = [r for r in records if r.lead_score >= POST_FILTER_MIN_SCORE]
    dropped_final = before_final_cut - len(records)
    records = sorted(records, key=lambda r: r.lead_score, reverse=True)
    log.info("")
    log.info(
        f"  FINAL CUT & SORT (lead_score >= {POST_FILTER_MIN_SCORE} post-domain/franchise): "
        f"kept {len(records)} / {before_final_cut} (dropped {dropped_final}), sorted desc"
    )
    
    if not records:
        log.error(
            f"No records with lead_score >= {POST_FILTER_MIN_SCORE} after domain penalties / franchise filter. Exiting."
        )
        return
    
    records = await enrich_all(records)
    
    records = apply_actionability_bump(records)
    records = sorted(records, key=lambda r: r.lead_score, reverse=True)
    log.info("")
    log.info("  ✓ Final sort by lead_score (descending) after actionability bump")
    
    output_file = OUTPUT_FILE
    if test_mode:
        output_file = "healthcare_agencies_fl_tx_az_leads_TEST.csv"
    
    export_to_csv(records, output_file)
    
    log.info("")
    log.info("═" * 70)
    log.info("✓ PIPELINE COMPLETE")
    log.info("═" * 70)


def main():
    parser = argparse.ArgumentParser(
        description="Healthcare Agencies Pipeline — FL/TX/AZ (two-stage score, Places only on VIP pre-cut)"
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Run in test mode (first 15 rows only, saves API credits)"
    )
    args = parser.parse_args()
    
    if not GOOGLE_PLACES_API_KEY:
        log.warning("⚠ GOOGLE_PLACES_API_KEY not set. Domain discovery will be skipped.")
        log.warning("  export GOOGLE_PLACES_API_KEY='your_key'")
    
    if not HUNTER_API_KEY:
        log.warning("⚠ HUNTER_API_KEY not set. Enrichment will be disabled.")
        log.warning("  export HUNTER_API_KEY='your_key'")
    
    asyncio.run(run_pipeline(test_mode=args.test))


if __name__ == "__main__":
    main()
