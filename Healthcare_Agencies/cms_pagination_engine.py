import asyncio
import aiohttp
import pandas as pd
import re
import time
import logging
import os
import sys
import argparse
from dataclasses import dataclass, asdict
from typing import Optional
from urllib.parse import urlparse

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(levelname)-7s │ %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)

APOLLO_API_KEY = os.environ.get("APOLLO_API_KEY", "")
HUNTER_API_KEY = os.environ.get("HUNTER_API_KEY", "")

OUTPUT_FILE = "healthcare_agencies_fl_leads.csv"
STATE_FILTER = "FL"

CMS_DATASTORE_BASE = "https://data.cms.gov/provider-data/api/1/datastore/query/6jpm-sxkc/0"
CMS_PAGE_SIZE = 1500

MAX_ENRICHMENT_CONCURRENCY = 5
MAX_ENRICHMENT_ATTEMPTS = 5
APOLLO_RPM = 50
DDGS_DELAY_SECONDS = 3

AGGREGATOR_DOMAINS = [
    "yelp.com", "facebook.com", "linkedin.com", "twitter.com", "instagram.com",
    "nextdoor.com", "thumbtack.com", "alignable.com",
    "yellowpages.com", "whitepages.com", "superpages.com", "manta.com",
    "mapquest.com", "bbb.org", "chamberofcommerce.com", "dandb.com",
    "indeed.com", "glassdoor.com", "ziprecruiter.com", "careerbuilder.com",
    "caring.com", "medicare.gov", "healthgrades.com", "carelist.com",
    "agingcare.com", "senioradvisor.com", "aplaceformom.com", "care.com",
    "homecare.com", "homecareassistance.com", "visitingangels.com",
    "healthcare4ppl.com", "healthcarecomps.com",
    "cms.gov", "ahca.myflorida.com", "floridahealthfinder.gov",
    "google.com", "bing.com", "youtube.com", "wikipedia.org", "amazon.com",
    "apps.apple.com", "play.google.com",
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
    apollo_org_id: str = ""


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
    
    log.info(f"  Filtering for state = '{STATE_FILTER}'")
    df_fl = df[df['state'] == STATE_FILTER]
    log.info(f"  Florida records: {len(df_fl)}")
    
    if limit:
        df_fl = df_fl.head(limit)
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
    for _, row in df_fl.iterrows():
        name = str(row.get(COL_NAME, "")).strip() if pd.notna(row.get(COL_NAME)) else ""
        
        if not name:
            continue
        
        record = HHARecord(
            agency_name=name,
            address=str(row.get(COL_ADDRESS, "")).strip() if pd.notna(row.get(COL_ADDRESS)) else "",
            city=str(row.get(COL_CITY, "")).strip() if pd.notna(row.get(COL_CITY)) else "",
            state=STATE_FILTER,
            zip_code=str(row.get(COL_ZIP, ""))[:5] if pd.notna(row.get(COL_ZIP)) else "",
            phone=_format_phone(row.get(COL_PHONE, "")),
            cms_ccn=str(row.get(COL_CCN, "")).strip() if pd.notna(row.get(COL_CCN)) else "",
        )
        records.append(record)
    
    log.info(f"  ✓ Extracted {len(records)} Florida HHAs from CMS Datastore API")
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
    log.info("STAGE 2: DOMAIN DISCOVERY (DuckDuckGo)")
    log.info("=" * 70)
    
    try:
        from ddgs import DDGS
    except ImportError:
        log.error("  ✗ 'ddgs' library not installed. Run: pip install ddgs")
        return records
    
    ddgs = DDGS()
    found = 0
    blocked = 0
    not_found = 0
    
    for i, record in enumerate(records):
        log.info(f"  [{i+1}/{len(records)}] Searching: {record.agency_name[:50]}...")
        
        query = f'"{record.agency_name}" {record.city} FL home care agency website'
        
        try:
            results = ddgs.text(query, max_results=5, backend="auto")
        except Exception as e:
            log.warning(f"    ⚠ Search failed: {e}")
            not_found += 1
            await asyncio.sleep(DDGS_DELAY_SECONDS)
            continue
        
        domain_found = False
        for result in results:
            href = result.get("href", "")
            domain = _extract_root_domain(href)
            
            if not domain:
                continue
            
            if _is_aggregator_domain(domain):
                log.info(f"    ✗ BLOCKED: {domain} (aggregator)")
                blocked += 1
                continue
            
            record.website = domain
            log.info(f"    ✓ FOUND: {domain}")
            found += 1
            domain_found = True
            break
        
        if not domain_found and not record.website:
            log.info(f"    ⚠ No valid domain found")
            not_found += 1
        
        if i < len(records) - 1:
            await asyncio.sleep(DDGS_DELAY_SECONDS)
    
    log.info("")
    log.info(f"  Domain Discovery Summary:")
    log.info(f"    ✓ Found:      {found}")
    log.info(f"    ✗ Blocked:    {blocked} (aggregator domains rejected)")
    log.info(f"    ⚠ Not found:  {not_found}")
    
    return records


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
        if self._request_count >= APOLLO_RPM:
            wait = 60 - (now - self._window_start)
            log.info(f"    ⏳ Apollo rate limit — sleeping {wait:.1f}s")
            await asyncio.sleep(wait)
            self._request_count = 0
            self._window_start = time.monotonic()
        self._request_count += 1

    async def enrich_via_apollo(self, agency_name: str, domain: str = "") -> dict:
        if not APOLLO_API_KEY:
            return {}

        hdrs = {
            "Content-Type": "application/json",
            "X-Api-Key": APOLLO_API_KEY,
            "Cache-Control": "no-cache"
        }

        await self._throttle()
        org_body = {
            "page": 1,
            "per_page": 1,
        }
        
        if domain:
            org_body["q_organization_domains_list"] = [domain]
        else:
            org_body["q_organization_name"] = agency_name
            org_body["organization_locations"] = ["Florida, United States"]

        try:
            async with self._session.post(
                "https://api.apollo.io/api/v1/mixed_companies/search",
                json=org_body, headers=hdrs,
                timeout=aiohttp.ClientTimeout(total=15)
            ) as resp:
                if resp.status != 200:
                    log.warning(f"    Apollo org HTTP {resp.status} for '{agency_name[:30]}'")
                    return {}
                data = await resp.json()
            
            orgs = data.get("organizations", []) or data.get("accounts", [])
            if not orgs:
                log.info(f"    Apollo: No org match for '{agency_name[:30]}'")
                return {}
            
            org = orgs[0]
            org_id = org.get("id", "")
            org_domain = org.get("primary_domain") or org.get("domain") or domain
            log.info(f"    ✓ Apollo org: {org.get('name', 'Unknown')[:30]} [{org_domain}]")
            
        except Exception as e:
            log.error(f"    Apollo org error: {e}")
            return {}

        await self._throttle()
        people_body = {
            "organization_ids": [org_id],
            "person_titles": self.TITLE_PRIORITY,
            "page": 1,
            "per_page": 10,
        }
        
        try:
            async with self._session.post(
                "https://api.apollo.io/api/v1/people/search",
                json=people_body, headers=hdrs,
                timeout=aiohttp.ClientTimeout(total=15)
            ) as resp:
                if resp.status != 200:
                    return {"apollo_org_id": org_id, "email_source": "apollo_no_people"}
                data = await resp.json()
                
        except Exception as e:
            log.error(f"    Apollo people error: {e}")
            return {"apollo_org_id": org_id, "email_source": "apollo_error"}

        people = data.get("people", [])
        if not people:
            log.info(f"    Apollo: No decision-makers found")
            return {"apollo_org_id": org_id, "email_source": "apollo_no_people"}

        def rank(p):
            t = (p.get("title") or "").lower()
            for i, kw in enumerate(self.TITLE_PRIORITY):
                if kw in t:
                    return i
            return 99

        best = sorted(people, key=rank)[0]
        email = best.get("email", "")
        
        return {
            "owner_name": f"{best.get('first_name', '')} {best.get('last_name', '')}".strip(),
            "owner_title": best.get("title", ""),
            "owner_email": email,
            "apollo_org_id": org_id,
            "email_source": "apollo" if email else "apollo_no_email",
        }

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
        
        result = await self.enrich_via_apollo(record.agency_name, domain)
        
        if not result.get("owner_email") and domain:
            log.info(f"    ↳ Hunter fallback for: {domain}")
            hunter = await self.enrich_via_hunter(domain)
            for k, v in hunter.items():
                if v and not result.get(k):
                    result[k] = v
        
        for k, v in result.items():
            if hasattr(record, k):
                setattr(record, k, v)
        
        if not record.email_source:
            record.email_source = "not_found"
        
        return record


async def enrich_all(records: list[HHARecord]) -> list[HHARecord]:
    log.info("")
    log.info("=" * 70)
    log.info("STAGE 3: APOLLO/HUNTER ENRICHMENT")
    log.info("=" * 70)
    
    if not APOLLO_API_KEY and not HUNTER_API_KEY:
        log.warning("  ⚠ No API keys set. Skipping enrichment.")
        log.warning("  Set APOLLO_API_KEY and/or HUNTER_API_KEY environment variables.")
        return records
    
    log.info(f"  ⚠ HARD LIMIT: Will only enrich {MAX_ENRICHMENT_ATTEMPTS} valid domains (to save API credits)")
    
    valid_for_enrichment = []
    blocked_count = 0
    no_domain_count = 0
    
    for r in records:
        if not r.website:
            no_domain_count += 1
            continue
        
        if _is_aggregator_domain(r.website):
            log.info(f"  ✗ BLOCKED for enrichment: {r.agency_name[:30]} ({r.website})")
            blocked_count += 1
            continue
        
        valid_for_enrichment.append(r)
    
    log.info(f"  Records with valid domains: {len(valid_for_enrichment)}")
    log.info(f"  Records without domains: {no_domain_count}")
    log.info(f"  Records with blocked domains: {blocked_count}")
    
    if not valid_for_enrichment:
        log.info("  No valid records to enrich.")
        return records
    
    to_enrich = valid_for_enrichment[:MAX_ENRICHMENT_ATTEMPTS]
    skipped_due_to_limit = len(valid_for_enrichment) - len(to_enrich)
    
    if skipped_due_to_limit > 0:
        log.info(f"  ⚠ Skipping {skipped_due_to_limit} records due to hard limit")
    
    log.info(f"  Will enrich: {len(to_enrich)} records")
    log.info("")
    
    enriched_map = {}
    enrichment_count = 0
    
    async with EnrichmentClient() as client:
        for i, rec in enumerate(to_enrich):
            log.info(f"  [{i+1}/{len(to_enrich)}] Enriching: {rec.agency_name[:40]}... ({rec.website})")
            
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
    
    final_records = []
    for r in records:
        if r.cms_ccn in enriched_map:
            final_records.append(enriched_map[r.cms_ccn])
        else:
            final_records.append(r)
    
    emails_found = sum(1 for r in enriched_map.values() if r.owner_email)
    
    log.info("")
    log.info(f"  Enrichment Summary:")
    log.info(f"    ✓ Attempted: {enrichment_count}")
    log.info(f"    ✓ Emails found: {emails_found}/{enrichment_count}")
    log.info(f"    ⚠ Unenriched (kept in CSV): {len(records) - enrichment_count}")
    
    return final_records


def export_to_csv(records: list[HHARecord], output_file: str):
    log.info("")
    log.info("=" * 70)
    log.info("STAGE 4: CSV EXPORT")
    log.info("=" * 70)
    
    df = pd.DataFrame([asdict(r) for r in records])
    
    col_order = [
        "agency_name", "owner_name", "owner_title", "owner_email", "email_source",
        "phone", "address", "city", "state", "zip_code",
        "website", "cms_ccn", "apollo_org_id"
    ]
    df = df[[c for c in col_order if c in df.columns]]
    
    df.sort_values("agency_name", inplace=True)
    
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
    log.info("║  HEALTHCARE AGENCIES FL PIPELINE v1.0                               ║")
    log.info("║  Florida Home Health Agency Lead Generator                         ║")
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
    
    records = await discover_domains(records)
    
    records = await enrich_all(records)
    
    output_file = OUTPUT_FILE
    if test_mode:
        output_file = "healthcare_agencies_fl_leads_TEST.csv"
    
    export_to_csv(records, output_file)
    
    log.info("")
    log.info("═" * 70)
    log.info("✓ PIPELINE COMPLETE")
    log.info("═" * 70)


def main():
    parser = argparse.ArgumentParser(
        description="Healthcare Agencies FL Pipeline — Florida Home Health Agency Lead Generator"
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Run in test mode (first 15 rows only, saves API credits)"
    )
    args = parser.parse_args()
    
    if not APOLLO_API_KEY:
        log.warning("⚠ APOLLO_API_KEY not set. Enrichment will be limited.")
        log.warning("  export APOLLO_API_KEY='your_key'")
    if not HUNTER_API_KEY:
        log.warning("⚠ HUNTER_API_KEY not set. Hunter fallback disabled.")
        log.warning("  export HUNTER_API_KEY='your_key'")
    
    asyncio.run(run_pipeline(test_mode=args.test))


if __name__ == "__main__":
    main()
