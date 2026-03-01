import asyncio
import aiohttp
import pandas as pd
import logging
import os
import argparse
from dataclasses import dataclass, asdict
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(levelname)-7s │ %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)

HUNTER_API_KEY = os.environ.get("HUNTER_API_KEY", "")
OUTPUT_FILE = "dental_clinics_leads.csv"

DEFAULT_CLINIC_LIMIT = 20
TEST_MAX_CLINICS = 5
TEST_MAX_HUNTER_CALLS = 5

NPPES_BASE_URL = "https://npiregistry.cms.hhs.gov/api/"
CLEARBIT_URL = "https://autocomplete.clearbit.com/v1/companies/suggest"
HUNTER_URL = "https://api.hunter.io/v2/domain-search"

AGGREGATOR_DOMAINS = [
    "yelp.com", "healthgrades.com", "zocdoc.com",
    "facebook.com", "linkedin.com", "twitter.com", "instagram.com",
    "nextdoor.com", "thumbtack.com", "alignable.com",
    "yellowpages.com", "whitepages.com", "superpages.com", "manta.com",
    "mapquest.com", "bbb.org", "chamberofcommerce.com", "dandb.com",
    "indeed.com", "glassdoor.com", "ziprecruiter.com", "careerbuilder.com",
    "cms.gov", "google.com", "bing.com", "youtube.com", "wikipedia.org",
    "amazon.com", "apps.apple.com", "play.google.com",
]

PRACTICE_MANAGER_KEYWORDS = [
    "practice manager", "office manager", "owner", "dental director", "administrator"
]


@dataclass
class DentalClinicRecord:
    clinic_name: str = ""
    city: str = ""
    state: str = ""
    website: str = ""
    contact_name: str = ""
    contact_title: str = ""
    contact_email: str = ""
    email_source: str = ""


def _is_aggregator_domain(domain: str) -> bool:
    domain_lower = domain.lower()
    for blocked in AGGREGATOR_DOMAINS:
        if blocked in domain_lower:
            return True
    return False


def _extract_contact_from_position(position: str) -> bool:
    """Check if position contains any of the practice manager keywords (case-insensitive substring match)."""
    position_lower = (position or "").lower()
    for keyword in PRACTICE_MANAGER_KEYWORDS:
        if keyword in position_lower:
            return True
    return False


async def fetch_npi_clinics(session: aiohttp.ClientSession, target_limit: int = DEFAULT_CLINIC_LIMIT) -> list[DentalClinicRecord]:
    log.info("=" * 70)
    log.info("STAGE 1: NPPES NPI REGISTRY API EXTRACTION")
    log.info("=" * 70)
    
    log.info(f"  Base endpoint: {NPPES_BASE_URL}")
    log.info(f"  Target clinic limit: {target_limit}")
    log.info(f"  Fetching dental clinics (taxonomy: dentist, entity_type: 2=organization)...")
    
    all_clinics = []
    page_size = 20
    skip = 0
    page_num = 1
    
    while len(all_clinics) < target_limit:
        log.info(f"  Page {page_num}: skip={skip}, already extracted: {len(all_clinics)}/{target_limit}")
        
        params = {
            "version": "2.1",
            "taxonomy_description": "dentist",
            "entity_type": "2",
            "limit": page_size,
            "skip": skip
        }
        
        try:
            async with session.get(
                NPPES_BASE_URL,
                params=params,
                timeout=aiohttp.ClientTimeout(total=60)
            ) as resp:
                if resp.status != 200:
                    log.error(f"  NPPES API returned HTTP {resp.status}")
                    text = await resp.text()
                    log.error(f"  Response: {text[:500]}")
                    break
                
                data = await resp.json()
        
        except Exception as e:
            log.error(f"  NPPES API error on page {page_num}: {e}")
            break
        
        results = data.get("results", [])
        
        if not results:
            log.info(f"  Page {page_num}: Empty results, end of data reached")
            break
        
        log.info(f"  Page {page_num}: Received {len(results)} results")
        
        for result in results:
            if len(all_clinics) >= target_limit:
                log.info(f"  ✓ Reached target limit of {target_limit} clinics — stopping NPPES extraction")
                break
            
            try:
                org_name = result.get("basic", {}).get("organization_name", "").strip()
                if not org_name:
                    continue
                
                addresses = result.get("addresses", [])
                if not addresses:
                    continue
                
                city = addresses[0].get("city", "").strip()
                state = addresses[0].get("state", "").strip()
                
                clinic = DentalClinicRecord(
                    clinic_name=org_name,
                    city=city,
                    state=state
                )
                all_clinics.append(clinic)
            
            except Exception as e:
                log.warning(f"    ⚠ Error parsing clinic record: {e}")
                continue
        
        if len(all_clinics) >= target_limit:
            break
        
        if len(results) < page_size:
            log.info(f"  Page {page_num}: Partial page ({len(results)} < {page_size}), end of data")
            break
        
        skip += page_size
        page_num += 1
    
    if not all_clinics:
        log.error("  ✗ No clinics fetched from NPPES API")
        return []
    
    log.info(f"  ✓ Extracted {len(all_clinics)} dental clinics from NPPES API")
    return all_clinics


async def discover_domains(records: list[DentalClinicRecord]) -> list[DentalClinicRecord]:
    log.info("")
    log.info("=" * 70)
    log.info("STAGE 2: DOMAIN DISCOVERY (Clearbit Autocomplete API)")
    log.info("=" * 70)
    
    found = 0
    blocked = 0
    not_found = 0
    
    for i, record in enumerate(records):
        log.info(f"  [{i+1}/{len(records)}] Searching: {record.clinic_name[:50]}...")
        
        params = {"query": record.clinic_name}
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    CLEARBIT_URL,
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    if resp.status != 200:
                        log.warning(f"    ⚠ Clearbit HTTP {resp.status}")
                        not_found += 1
                        await asyncio.sleep(1)
                        continue
                    
                    data = await resp.json()
        
        except Exception as e:
            log.warning(f"    ⚠ Clearbit error: {e}")
            not_found += 1
            await asyncio.sleep(1)
            continue
        
        if not data or len(data) == 0:
            log.info(f"    ⚠ No results from Clearbit")
            not_found += 1
            await asyncio.sleep(1)
            continue
        
        try:
            domain = data[0].get("domain")
            if not domain:
                log.info(f"    ⚠ No domain in first result")
                not_found += 1
                await asyncio.sleep(1)
                continue
            
            if _is_aggregator_domain(domain):
                log.info(f"    ✗ BLOCKED: {domain} (aggregator)")
                blocked += 1
            else:
                record.website = domain
                log.info(f"    ✓ FOUND: {domain}")
                found += 1
        
        except Exception as e:
            log.warning(f"    ⚠ Parse error: {e}")
            not_found += 1
        
        await asyncio.sleep(1)
    
    log.info("")
    log.info(f"  Domain Discovery Summary:")
    log.info(f"    ✓ Found:      {found}")
    log.info(f"    ✗ Blocked:    {blocked} (aggregator domains rejected)")
    log.info(f"    ⚠ Not found:  {not_found}")
    
    return records


class HunterEnrichmentClient:
    def __init__(self):
        self._api_calls = 0
        self._session: Optional[aiohttp.ClientSession] = None
    
    async def __aenter__(self):
        self._session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, *args):
        await self._session.close()
    
    async def enrich_via_hunter(self, domain: str) -> dict:
        if not HUNTER_API_KEY:
            return {}
        
        if not domain:
            return {}
        
        self._api_calls += 1
        
        params = {
            "domain": domain,
            "api_key": HUNTER_API_KEY
        }
        
        try:
            async with self._session.get(
                HUNTER_URL,
                params=params,
                timeout=aiohttp.ClientTimeout(total=15)
            ) as resp:
                if resp.status != 200:
                    log.warning(f"    Hunter HTTP {resp.status} for '{domain}'")
                    return {"email_source": "hunter_error"}
                data = await resp.json()
        
        except Exception as e:
            log.error(f"    Hunter error: {e}")
            return {"email_source": "hunter_error"}
        
        emails = data.get("data", {}).get("emails", [])
        
        if not emails:
            log.info(f"    Hunter: No emails found for domain")
            return {"email_source": "hunter_no_emails"}
        
        log.info(f"    Hunter: Found {len(emails)} email(s)")
        
        for email_entry in emails:
            position = email_entry.get("position", "") or ""
            if _extract_contact_from_position(position):
                log.info(f"    ✓ Found matching contact: {position}")
                first_name = email_entry.get("first_name", "") or ""
                last_name = email_entry.get("last_name", "") or ""
                full_name = f"{first_name} {last_name}".strip()
                
                return {
                    "contact_name": full_name,
                    "contact_title": position,
                    "contact_email": email_entry.get("value", "") or "",
                    "email_source": "hunter",
                }
        
        log.info(f"    ⚠ No qualifying position found (searched for: {', '.join(PRACTICE_MANAGER_KEYWORDS)})")
        return {"email_source": "hunter_no_match"}
    
    async def enrich(self, record: DentalClinicRecord) -> DentalClinicRecord:
        domain = record.website.strip()
        
        result = await self.enrich_via_hunter(domain)
        
        for k, v in result.items():
            if hasattr(record, k):
                setattr(record, k, v)
        
        if not record.email_source:
            record.email_source = "not_found"
        
        return record
    
    @property
    def api_calls(self) -> int:
        return self._api_calls


async def enrich_all(records: list[DentalClinicRecord], max_api_calls: Optional[int] = None) -> list[DentalClinicRecord]:
    log.info("")
    log.info("=" * 70)
    log.info("STAGE 3: HUNTER.IO ENRICHMENT")
    log.info("=" * 70)
    
    if not HUNTER_API_KEY:
        log.warning("  ⚠ HUNTER_API_KEY not set. Skipping enrichment.")
        log.warning("  Set HUNTER_API_KEY environment variable.")
        return records
    
    if max_api_calls:
        log.info(f"  ⚠ Hunter API call limit: {max_api_calls} (to protect API credits)")
    else:
        log.info(f"  ✓ No Hunter API call limit — will process all valid domains")
    
    valid_for_enrichment = []
    blocked_count = 0
    no_domain_count = 0
    
    for r in records:
        if not r.website:
            no_domain_count += 1
            continue
        
        if _is_aggregator_domain(r.website):
            log.info(f"  ✗ BLOCKED for enrichment: {r.clinic_name[:30]} ({r.website})")
            blocked_count += 1
            continue
        
        valid_for_enrichment.append(r)
    
    log.info(f"  Records with valid domains: {len(valid_for_enrichment)}")
    log.info(f"  Records without domains: {no_domain_count}")
    log.info(f"  Records with blocked domains: {blocked_count}")
    
    if not valid_for_enrichment:
        log.info("  No valid records to enrich.")
        return records
    
    log.info(f"  Will attempt enrichment on: {len(valid_for_enrichment)} records")
    log.info("")
    
    enriched_map = {}
    emails_found = 0
    
    async with HunterEnrichmentClient() as client:
        for i, rec in enumerate(valid_for_enrichment):
            if max_api_calls and client.api_calls >= max_api_calls:
                log.info(f"  ⛔ HARD LIMIT REACHED: {max_api_calls} API calls — stopping enrichment")
                break
            
            log.info(f"  [{i+1}/{len(valid_for_enrichment)}] Enriching: {rec.clinic_name[:40]}... ({rec.website})")
            log.info(f"    [API calls used: {client.api_calls}/{max_api_calls or '∞'}]")
            
            try:
                enriched_rec = await client.enrich(rec)
                enriched_map[rec.clinic_name] = enriched_rec
                
                if enriched_rec.contact_email:
                    emails_found += 1
                    log.info(f"    ✓ {enriched_rec.contact_name} ({enriched_rec.contact_title})")
                    log.info(f"      Email: {enriched_rec.contact_email}")
                else:
                    log.info(f"    ⚠ No email found")
            
            except Exception as e:
                log.error(f"    ✗ Error: {e}")
        
        total_api_calls = client.api_calls
    
    final_records = []
    for r in records:
        if r.clinic_name in enriched_map:
            final_records.append(enriched_map[r.clinic_name])
        else:
            final_records.append(r)
    
    log.info("")
    log.info(f"  Enrichment Summary:")
    log.info(f"    ✓ Hunter API calls made: {total_api_calls}")
    log.info(f"    ✓ Emails found: {emails_found}/{total_api_calls}")
    log.info(f"    ⚠ Unenriched (kept in CSV): {len(records) - len(enriched_map)}")
    
    return final_records


def export_to_csv(records: list[DentalClinicRecord], output_file: str):
    log.info("")
    log.info("=" * 70)
    log.info("STAGE 4: CSV EXPORT")
    log.info("=" * 70)
    
    df = pd.DataFrame([asdict(r) for r in records])
    
    col_order = [
        "clinic_name", "city", "state", "website",
        "contact_name", "contact_title", "contact_email", "email_source"
    ]
    df = df[[c for c in col_order if c in df.columns]]
    
    df.sort_values("clinic_name", inplace=True)
    
    df.to_csv(output_file, index=False, encoding="utf-8")
    
    log.info(f"  ✓ Output saved → '{output_file}'")
    log.info(f"    Total clinics:     {len(df)}")
    log.info(f"    With websites:     {df['website'].ne('').sum()}")
    log.info(f"    With emails:       {df['contact_email'].ne('').sum()}")
    
    if len(df) > 0:
        website_rate = df['website'].ne('').sum() / len(df) * 100
        email_rate = df['contact_email'].ne('').sum() / len(df) * 100
        log.info(f"    Website coverage:  {website_rate:.1f}%")
        log.info(f"    Email coverage:    {email_rate:.1f}%")


async def run_pipeline(clinic_limit: int = DEFAULT_CLINIC_LIMIT, test_mode: bool = False):
    log.info("")
    log.info("╔" + "═" * 68 + "╗")
    log.info("║  NPI REGISTRY DENTAL CLINIC PIPELINE v1.0                       ║")
    log.info("║  Extract → Domain Discover → Practice Manager Enrichment        ║")
    log.info("╚" + "═" * 68 + "╝")
    log.info("")
    
    if test_mode:
        log.info("🧪 TEST MODE ENABLED")
        log.info(f"   - Max clinics: {clinic_limit}")
        log.info(f"   - Max Hunter API calls: {TEST_MAX_HUNTER_CALLS}")
        log.info("")
    
    async with aiohttp.ClientSession() as session:
        records = await fetch_npi_clinics(session, target_limit=clinic_limit)
    
    if not records:
        log.error("No clinics extracted. Exiting.")
        return
    
    records = await discover_domains(records)
    
    hunter_limit = TEST_MAX_HUNTER_CALLS if test_mode else None
    records = await enrich_all(records, max_api_calls=hunter_limit)
    
    output_file = OUTPUT_FILE
    if test_mode:
        output_file = "dental_clinics_leads_TEST.csv"
    
    export_to_csv(records, output_file)
    
    log.info("")
    log.info("═" * 70)
    log.info("✓ PIPELINE COMPLETE")
    log.info("═" * 70)


def main():
    parser = argparse.ArgumentParser(
        description="NPI Registry Dental Clinic Pipeline — Extract clinics, discover domains, enrich with practice manager contacts"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_CLINIC_LIMIT,
        help=f"Max number of clinics to extract from NPPES API (default: {DEFAULT_CLINIC_LIMIT})"
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help=f"Run in test mode ({TEST_MAX_CLINICS} clinics, {TEST_MAX_HUNTER_CALLS} Hunter API calls max)"
    )
    args = parser.parse_args()
    
    if not HUNTER_API_KEY:
        log.warning("⚠ HUNTER_API_KEY not set. Enrichment will be skipped.")
        log.warning("  export HUNTER_API_KEY='your_key'")
    
    clinic_limit = TEST_MAX_CLINICS if args.test else args.limit
    asyncio.run(run_pipeline(clinic_limit=clinic_limit, test_mode=args.test))


if __name__ == "__main__":
    main()
