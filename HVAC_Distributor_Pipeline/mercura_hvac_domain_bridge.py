"""
HVAC Domain Bridge Pipeline v1.0 — Independent HVAC Distributor Lead Generator
======================================================================================
Target:  Curated seed list of independent US HVAC/Plumbing distributors
Strategy: Seed distributor list → Clearbit domain discovery → Apollo.io enrichment → CSV

Usage:
  python hvac_domain_bridge.py          # Full run (15 distributors)
  python hvac_domain_bridge.py --test   # 5 distributors, 5 Apollo API calls max (API credit saver)
"""

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

APOLLO_API_KEY = os.environ.get("APOLLO_API_KEY", "")
OUTPUT_FILE = "hvac_leads.csv"

TARGET_DISTRIBUTORS = [
    "Johnstone Supply", "Munch's Supply", "Coburn Supply Company", "Gustave A. Larson",
    "Behler-Young", "Slakey Brothers", "Gensco", "Habegger Corporation",
    "US Air Conditioning Distributors", "Peirce-Phelps", "Homans Associates",
    "Mingledorff's", "Illco", "Century HVAC Distributing", "Ed's Supply"
]

TEST_MAX_BRANDS = 5
TEST_MAX_API_CALLS = 5

AGGREGATOR_DOMAINS = [
    "amazon.com", "ebay.com", "homedepot.com", "lowes.com", "grainger.com",
    "ferguson.com", "supplyhouse.com", "rexnord.com", "yelp.com", "google.com",
    "maps.google.com", "facebook.com", "linkedin.com", "twitter.com", "instagram.com",
    "tiktok.com", "youtube.com", "yellowpages.com", "whitepages.com", "superpages.com",
    "manta.com", "bbb.org", "chamberofcommerce.com", "dandb.com", "indeed.com",
    "glassdoor.com", "ziprecruiter.com", "careerbuilder.com", "crunchbase.com",
    "bloomberg.com", "forbes.com", "businessinsider.com", "cnbc.com",
    "reddit.com", "wikipedia.org", "apps.apple.com", "play.google.com",
    "thumbtack.com", "alignable.com", "nextdoor.com", "mapquest.com"
]

@dataclass
class HVACDistributor:
    distributor_name: str = ""
    website: str = ""
    contact_name: str = ""
    contact_title: str = ""
    contact_email: str = ""
    email_source: str = ""

def fetch_hvac_distributors(limit: Optional[int] = None) -> list[HVACDistributor]:
    log.info("=" * 70)
    log.info("STAGE 1: LOAD SEED DISTRIBUTOR LIST")
    log.info("=" * 70)
    
    log.info(f"  ✓ Loaded {len(TARGET_DISTRIBUTORS)} curated independent US HVAC/Plumbing distributors")
    
    distributor_list = TARGET_DISTRIBUTORS[:limit] if limit else TARGET_DISTRIBUTORS
    
    if limit and limit < len(TARGET_DISTRIBUTORS):
        log.info(f"  TEST MODE: Limited to {limit} distributors")
    
    records = [HVACDistributor(distributor_name=name) for name in distributor_list]
    
    if records:
        sample = records[:5]
        log.info(f"  Sample distributors: {[r.distributor_name for r in sample]}")
    
    log.info(f"  ✓ Ready to discover domains for {len(records)} distributors")
    
    return records

def _is_aggregator_domain(domain: str) -> bool:
    domain_lower = domain.lower()
    for blocked in AGGREGATOR_DOMAINS:
        if blocked in domain_lower:
            return True
    return False

async def discover_domains(records: list[HVACDistributor]) -> list[HVACDistributor]:
    log.info("")
    log.info("=" * 70)
    log.info("STAGE 2: DOMAIN DISCOVERY (Clearbit Autocomplete API)")
    log.info("=" * 70)
    
    found = 0
    blocked = 0
    not_found = 0
    
    for i, record in enumerate(records):
        log.info(f"  [{i+1}/{len(records)}] Searching: {record.distributor_name[:50]}...")
        
        url = "https://autocomplete.clearbit.com/v1/companies/suggest"
        params = {"query": record.distributor_name}
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    url,
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

class ApolloEnrichmentClient:
    SALES_OPS_KEYWORDS = [
        "branch manager", "inside sales", "vp of sales", "sales manager", "operations"
    ]
    
    def __init__(self):
        self._api_calls = 0
        self._session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self):
        self._session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, *args):
        await self._session.close()

    async def enrich_via_apollo(self, domain: str) -> dict:
        if not APOLLO_API_KEY:
            return {}

        if not domain:
            return {}

        headers = {
            "Cache-Control": "no-cache",
            "Content-Type": "application/json"
        }
        
        step1_person = await self._apollo_search_people(domain, headers)
        if not step1_person:
            return {"email_source": "apollo_no_contacts"}
        
        await asyncio.sleep(1)
        
        email = await self._apollo_match_email(step1_person.get("id"), headers)
        if not email:
            log.info(f"    Apollo: Person found but email match failed")
            return {"email_source": "apollo_no_email"}
        
        log.info(f"    ✓ Found Sales/Ops contact: {step1_person.get('title', 'Unknown')}")
        step1_person["email"] = email
        return self._extract_contact(step1_person, "apollo_sales_ops")

    async def _apollo_search_people(self, domain: str, headers: dict) -> dict:
        self._api_calls += 1
        
        url = "https://api.apollo.io/v1/mixed_people/search"
        
        payload = {
            "q_organization_domains": [domain],
            "person_titles": self.SALES_OPS_KEYWORDS,
            "per_page": 10
        }
        
        auth_headers = {
            "x-api-key": APOLLO_API_KEY,
            "Content-Type": "application/json",
            "Cache-Control": "no-cache"
        }
        
        try:
            async with self._session.post(
                url,
                json=payload,
                headers=auth_headers,
                timeout=aiohttp.ClientTimeout(total=15)
            ) as resp:
                if resp.status == 403:
                    log.warning(f"    Apollo Step 1 HTTP 403 (FORBIDDEN)")
                    log.warning(f"    Verify: 1) API key is valid, 2) Account has API access")
                    return {}
                elif resp.status != 200:
                    log.warning(f"    Apollo Step 1 HTTP {resp.status} for '{domain}'")
                    return {}
                data = await resp.json()
                
        except Exception as e:
            log.error(f"    Apollo Step 1 error: {e}")
            return {}

        people = data.get("people", [])
        
        if not people:
            log.info(f"    Apollo: No contacts found for domain")
            return {}

        log.info(f"    Apollo: Found {len(people)} contact(s)")
        
        person = people[0]
        if not person.get("id"):
            log.warning(f"    Apollo: Contact missing ID field")
            return {}
        
        return person

    async def _apollo_match_email(self, person_id: str, headers: dict) -> str:
        self._api_calls += 1
        
        url = "https://api.apollo.io/v1/people/match"
        
        payload = {
            "id": person_id,
            "reveal_personal_emails": True
        }
        
        auth_headers = {
            "x-api-key": APOLLO_API_KEY,
            "Content-Type": "application/json",
            "Cache-Control": "no-cache"
        }
        
        try:
            async with self._session.post(
                url,
                json=payload,
                headers=auth_headers,
                timeout=aiohttp.ClientTimeout(total=15)
            ) as resp:
                if resp.status == 403:
                    log.warning(f"    Apollo Step 2 HTTP 403 (FORBIDDEN)")
                    log.warning(f"    Verify: 1) API key is valid, 2) Account has enrichment access")
                    return ""
                elif resp.status != 200:
                    log.warning(f"    Apollo Step 2 HTTP {resp.status} for person ID '{person_id}'")
                    return ""
                data = await resp.json()
                
        except Exception as e:
            log.error(f"    Apollo Step 2 error: {e}")
            return ""

        person_data = data.get("person", {})
        email = person_data.get("email")
        
        if not email:
            return ""
        
        log.info(f"      Email: {email}")
        return email

    def _extract_contact(self, person: dict, source: str) -> dict:
        first_name = person.get("first_name", "") or ""
        last_name = person.get("last_name", "") or ""
        full_name = f"{first_name} {last_name}".strip()
        
        return {
            "contact_name": full_name,
            "contact_title": person.get("title", "") or "",
            "contact_email": person.get("email", "") or "",
            "email_source": source,
        }

    async def enrich(self, record: HVACDistributor) -> HVACDistributor:
        domain = record.website.strip()
        
        result = await self.enrich_via_apollo(domain)
        
        for k, v in result.items():
            if hasattr(record, k):
                setattr(record, k, v)
        
        if not record.email_source:
            record.email_source = "not_found"
        
        return record
    
    @property
    def api_calls(self) -> int:
        return self._api_calls

async def enrich_all(records: list[HVACDistributor], max_api_calls: Optional[int] = None) -> list[HVACDistributor]:
    log.info("")
    log.info("=" * 70)
    log.info("STAGE 3: APOLLO.IO ENRICHMENT")
    log.info("=" * 70)
    
    if not APOLLO_API_KEY:
        log.warning("  ⚠ APOLLO_API_KEY not set. Skipping enrichment.")
        log.warning("  Set APOLLO_API_KEY environment variable.")
        return records
    
    if max_api_calls:
        log.info(f"  ⚠ HARD LIMIT: Will stop after exactly {max_api_calls} Apollo API calls")
        log.info(f"     (to protect your remaining credits)")
    
    valid_for_enrichment = []
    blocked_count = 0
    no_domain_count = 0
    
    for r in records:
        if not r.website:
            no_domain_count += 1
            continue
        
        if _is_aggregator_domain(r.website):
            log.info(f"  ✗ BLOCKED for enrichment: {r.distributor_name[:30]} ({r.website})")
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
    
    async with ApolloEnrichmentClient() as client:
        for i, rec in enumerate(valid_for_enrichment):
            if max_api_calls and client.api_calls >= max_api_calls:
                log.info(f"  ⛔ HARD LIMIT REACHED: {max_api_calls} API calls — stopping enrichment")
                break
            
            log.info(f"  [{i+1}/{len(valid_for_enrichment)}] Enriching: {rec.distributor_name[:40]}... ({rec.website})")
            log.info(f"    [API calls used: {client.api_calls}/{max_api_calls or '∞'}]")
            
            try:
                enriched_rec = await client.enrich(rec)
                enriched_map[rec.distributor_name] = enriched_rec
                
                if enriched_rec.contact_email:
                    emails_found += 1
                    log.info(f"    ✓ {enriched_rec.contact_name} ({enriched_rec.contact_title})")
                    log.info(f"      Email: {enriched_rec.contact_email}")
                else:
                    log.info(f"    ⚠ No usable email found")
                    
            except Exception as e:
                log.error(f"    ✗ Error: {e}")
        
        total_api_calls = client.api_calls
    
    final_records = []
    for r in records:
        if r.distributor_name in enriched_map:
            final_records.append(enriched_map[r.distributor_name])
        else:
            final_records.append(r)
    
    log.info("")
    log.info(f"  Enrichment Summary:")
    log.info(f"    ✓ Apollo API calls made: {total_api_calls}")
    log.info(f"    ✓ Emails found: {emails_found}/{total_api_calls}")
    log.info(f"    ⚠ Unenriched (kept in CSV): {len(records) - len(enriched_map)}")
    
    return final_records

def export_to_csv(records: list[HVACDistributor], output_file: str):
    log.info("")
    log.info("=" * 70)
    log.info("STAGE 4: CSV EXPORT")
    log.info("=" * 70)
    
    df = pd.DataFrame([asdict(r) for r in records])
    
    col_order = [
        "distributor_name", "website", "contact_name", "contact_title", 
        "contact_email", "email_source"
    ]
    df = df[[c for c in col_order if c in df.columns]]
    
    df.sort_values("distributor_name", inplace=True)
    
    df.to_csv(output_file, index=False, encoding="utf-8")
    
    log.info(f"  ✓ Output saved → '{output_file}'")
    log.info(f"    Total distributors: {len(df)}")
    log.info(f"    With websites:      {df['website'].ne('').sum()}")
    log.info(f"    With emails:        {df['contact_email'].ne('').sum()}")
    
    if len(df) > 0:
        website_rate = df['website'].ne('').sum() / len(df) * 100
        email_rate = df['contact_email'].ne('').sum() / len(df) * 100
        log.info(f"    Website coverage:   {website_rate:.1f}%")
        log.info(f"    Email coverage:     {email_rate:.1f}%")

async def run_pipeline(test_mode: bool = False):
    log.info("")
    log.info("╔" + "═" * 68 + "╗")
    log.info("║         HVAC DOMAIN BRIDGE PIPELINE v1.0                         ║")
    log.info("║  Independent HVAC Distributor Lead Generator                     ║")
    log.info("╚" + "═" * 68 + "╝")
    log.info("")
    
    if test_mode:
        log.info("🧪 TEST MODE ENABLED")
        log.info(f"   - Max distributors: {TEST_MAX_BRANDS}")
        log.info(f"   - Max Apollo API calls: {TEST_MAX_API_CALLS}")
        log.info("")
    
    distributor_limit = TEST_MAX_BRANDS if test_mode else None
    api_call_limit = TEST_MAX_API_CALLS if test_mode else None
    
    records = fetch_hvac_distributors(limit=distributor_limit)
    
    if not records:
        log.error("No distributors to process. Exiting.")
        return
    
    records = await discover_domains(records)
    
    records = await enrich_all(records, max_api_calls=api_call_limit)
    
    output_file = OUTPUT_FILE
    if test_mode:
        output_file = "hvac_leads_TEST.csv"
    
    export_to_csv(records, output_file)
    
    log.info("")
    log.info("═" * 70)
    log.info("✓ PIPELINE COMPLETE")
    log.info("═" * 70)

def main():
    parser = argparse.ArgumentParser(
        description="HVAC Domain Bridge Pipeline — Independent HVAC Distributor Lead Generator"
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help=f"Run in test mode ({TEST_MAX_BRANDS} distributors, {TEST_MAX_API_CALLS} Apollo API calls max)"
    )
    args = parser.parse_args()
    
    if not APOLLO_API_KEY:
        log.warning("⚠ APOLLO_API_KEY not set. Enrichment will be skipped.")
        log.warning("  export APOLLO_API_KEY='your_key'")
    
    asyncio.run(run_pipeline(test_mode=args.test))

if __name__ == "__main__":
    main()
