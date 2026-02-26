"""
CPG Domain Bridge Pipeline v1.0 — Independent CPG Brand Lead Generator
=================================================================
Target:  Curated seed list of fast-growing independent US CPG brands
Strategy: Seed brand list → Clearbit domain discovery → Hunter.io enrichment → CSV

Usage:
  python cpg_domain_bridge.py          # Full run (20 brands)
  python cpg_domain_bridge.py --test   # 5 brands, 5 Hunter API calls max (API credit saver)
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

HUNTER_API_KEY = os.environ.get("HUNTER_API_KEY", "")
OUTPUT_FILE = "cpg_leads.csv"

TARGET_BRANDS = [
    "Olipop", "Magic Spoon", "Mid-Day Squares", "Liquid Death", "Siete Foods",
    "Truff", "Chamberlain Coffee", "Fly By Jing", "Poppi", "Feastables",
    "Oatly", "Ghia", "Kin Euphorics", "Partake Foods", "Goodles",
    "Banza", "MUD\\WTR", "LesserEvil", "Catalina Crunch", "Cuts Clothing"
]

TEST_MAX_BRANDS = 5
TEST_MAX_API_CALLS = 5

AGGREGATOR_DOMAINS = [
    "openfoodfacts.org", "amazon.com", "walmart.com", "target.com", "instacart.com",
    "yelp.com", "facebook.com", "linkedin.com", "twitter.com", "instagram.com",
    "nextdoor.com", "thumbtack.com", "alignable.com", "tiktok.com",
    "yellowpages.com", "whitepages.com", "superpages.com", "manta.com",
    "mapquest.com", "bbb.org", "chamberofcommerce.com", "dandb.com",
    "indeed.com", "glassdoor.com", "ziprecruiter.com", "careerbuilder.com",
    "kroger.com", "costco.com", "wholefoodsmarket.com", "safeway.com",
    "publix.com", "albertsons.com", "traderjoes.com", "sprouts.com",
    "freshdirect.com", "thrive.com", "vitacost.com", "iherb.com",
    "google.com", "bing.com", "youtube.com", "wikipedia.org", "reddit.com",
    "apps.apple.com", "play.google.com",
    "forbes.com", "businessinsider.com", "bloomberg.com", "cnbc.com",
]

@dataclass
class CPGBrand:
    brand_name: str = ""
    website: str = ""
    contact_name: str = ""
    contact_title: str = ""
    contact_email: str = ""
    email_source: str = ""

def fetch_cpg_brands(limit: Optional[int] = None) -> list[CPGBrand]:
    log.info("=" * 70)
    log.info("STAGE 1: LOAD SEED BRAND LIST")
    log.info("=" * 70)
    
    log.info(f"  ✓ Loaded {len(TARGET_BRANDS)} curated independent US CPG brands")
    
    brand_list = TARGET_BRANDS[:limit] if limit else TARGET_BRANDS
    
    if limit and limit < len(TARGET_BRANDS):
        log.info(f"  TEST MODE: Limited to {limit} brands")
    
    records = [CPGBrand(brand_name=name) for name in brand_list]
    
    if records:
        sample = records[:5]
        log.info(f"  Sample brands: {[r.brand_name for r in sample]}")
    
    log.info(f"  ✓ Ready to discover domains for {len(records)} brands")
    
    return records

def _is_aggregator_domain(domain: str) -> bool:
    domain_lower = domain.lower()
    for blocked in AGGREGATOR_DOMAINS:
        if blocked in domain_lower:
            return True
    return False

async def discover_domains(records: list[CPGBrand]) -> list[CPGBrand]:
    log.info("")
    log.info("=" * 70)
    log.info("STAGE 2: DOMAIN DISCOVERY (Clearbit Autocomplete API)")
    log.info("=" * 70)
    
    found = 0
    blocked = 0
    not_found = 0
    
    for i, record in enumerate(records):
        log.info(f"  [{i+1}/{len(records)}] Searching: {record.brand_name[:50]}...")
        
        url = "https://autocomplete.clearbit.com/v1/companies/suggest"
        params = {"query": record.brand_name}
        
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

class HunterEnrichmentClient:
    OPS_KEYWORDS = [
        "operations", "supply chain", "logistics", "coo", 
        "chief operating", "vp operations", "head of operations"
    ]
    
    EXEC_KEYWORDS = [
        "ceo", "founder", "president", "owner", 
        "chief executive", "managing director", "co-founder"
    ]
    
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
        
        url = "https://api.hunter.io/v2/domain-search"
        params = {
            "domain": domain,
            "api_key": HUNTER_API_KEY
        }
        
        try:
            async with self._session.get(
                url,
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
        
        ops_candidates = []
        for email_entry in emails:
            position = (email_entry.get("position") or "").lower()
            for kw in self.OPS_KEYWORDS:
                if kw in position:
                    ops_candidates.append(email_entry)
                    break
        
        if ops_candidates:
            best = ops_candidates[0]
            log.info(f"    ✓ Found Ops contact: {best.get('position', 'Unknown')}")
            return self._extract_contact(best, "hunter_ops")
        
        exec_candidates = []
        for email_entry in emails:
            position = (email_entry.get("position") or "").lower()
            for kw in self.EXEC_KEYWORDS:
                if kw in position:
                    exec_candidates.append(email_entry)
                    break
        
        if exec_candidates:
            best = exec_candidates[0]
            log.info(f"    ✓ Found Exec fallback: {best.get('position', 'Unknown')}")
            return self._extract_contact(best, "hunter_exec")
        
        for email_entry in emails:
            if email_entry.get("position"):
                log.info(f"    ✓ Found generic contact: {email_entry.get('position', 'Unknown')}")
                return self._extract_contact(email_entry, "hunter_generic")
        
        if emails:
            log.info(f"    ✓ Found email (no title)")
            return self._extract_contact(emails[0], "hunter_no_title")
        
        return {"email_source": "hunter_no_match"}

    def _extract_contact(self, email_entry: dict, source: str) -> dict:
        first_name = email_entry.get("first_name", "") or ""
        last_name = email_entry.get("last_name", "") or ""
        full_name = f"{first_name} {last_name}".strip()
        
        return {
            "contact_name": full_name,
            "contact_title": email_entry.get("position", "") or "",
            "contact_email": email_entry.get("value", "") or "",
            "email_source": source,
        }

    async def enrich(self, record: CPGBrand) -> CPGBrand:
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

async def enrich_all(records: list[CPGBrand], max_api_calls: Optional[int] = None) -> list[CPGBrand]:
    log.info("")
    log.info("=" * 70)
    log.info("STAGE 3: HUNTER.IO ENRICHMENT")
    log.info("=" * 70)
    
    if not HUNTER_API_KEY:
        log.warning("  ⚠ HUNTER_API_KEY not set. Skipping enrichment.")
        log.warning("  Set HUNTER_API_KEY environment variable.")
        return records
    
    if max_api_calls:
        log.info(f"  ⚠ HARD LIMIT: Will stop after exactly {max_api_calls} Hunter API calls")
        log.info(f"     (to protect your remaining credits)")
    
    valid_for_enrichment = []
    blocked_count = 0
    no_domain_count = 0
    
    for r in records:
        if not r.website:
            no_domain_count += 1
            continue
        
        if _is_aggregator_domain(r.website):
            log.info(f"  ✗ BLOCKED for enrichment: {r.brand_name[:30]} ({r.website})")
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
            
            log.info(f"  [{i+1}/{len(valid_for_enrichment)}] Enriching: {rec.brand_name[:40]}... ({rec.website})")
            log.info(f"    [API calls used: {client.api_calls}/{max_api_calls or '∞'}]")
            
            try:
                enriched_rec = await client.enrich(rec)
                enriched_map[rec.brand_name] = enriched_rec
                
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
        if r.brand_name in enriched_map:
            final_records.append(enriched_map[r.brand_name])
        else:
            final_records.append(r)
    
    log.info("")
    log.info(f"  Enrichment Summary:")
    log.info(f"    ✓ Hunter API calls made: {total_api_calls}")
    log.info(f"    ✓ Emails found: {emails_found}/{total_api_calls}")
    log.info(f"    ⚠ Unenriched (kept in CSV): {len(records) - len(enriched_map)}")
    
    return final_records

def export_to_csv(records: list[CPGBrand], output_file: str):
    log.info("")
    log.info("=" * 70)
    log.info("STAGE 4: CSV EXPORT")
    log.info("=" * 70)
    
    df = pd.DataFrame([asdict(r) for r in records])
    
    col_order = [
        "brand_name", "website", "contact_name", "contact_title", 
        "contact_email", "email_source"
    ]
    df = df[[c for c in col_order if c in df.columns]]
    
    df.sort_values("brand_name", inplace=True)
    
    df.to_csv(output_file, index=False, encoding="utf-8")
    
    log.info(f"  ✓ Output saved → '{output_file}'")
    log.info(f"    Total brands:      {len(df)}")
    log.info(f"    With websites:     {df['website'].ne('').sum()}")
    log.info(f"    With emails:       {df['contact_email'].ne('').sum()}")
    
    if len(df) > 0:
        website_rate = df['website'].ne('').sum() / len(df) * 100
        email_rate = df['contact_email'].ne('').sum() / len(df) * 100
        log.info(f"    Website coverage:  {website_rate:.1f}%")
        log.info(f"    Email coverage:    {email_rate:.1f}%")

async def run_pipeline(test_mode: bool = False):
    log.info("")
    log.info("╔" + "═" * 68 + "╗")
    log.info("║  CPG DOMAIN BRIDGE PIPELINE v1.0                                     ║")
    log.info("║  Independent CPG Brand Lead Generator                              ║")
    log.info("╚" + "═" * 68 + "╝")
    log.info("")
    
    if test_mode:
        log.info("🧪 TEST MODE ENABLED")
        log.info(f"   - Max brands: {TEST_MAX_BRANDS}")
        log.info(f"   - Max Hunter API calls: {TEST_MAX_API_CALLS}")
        log.info("")
    
    brand_limit = TEST_MAX_BRANDS if test_mode else None
    api_call_limit = TEST_MAX_API_CALLS if test_mode else None
    
    records = fetch_cpg_brands(limit=brand_limit)
    
    if not records:
        log.error("No brands to process. Exiting.")
        return
    
    records = await discover_domains(records)
    
    records = await enrich_all(records, max_api_calls=api_call_limit)
    
    output_file = OUTPUT_FILE
    if test_mode:
        output_file = "cpg_leads_TEST.csv"
    
    export_to_csv(records, output_file)
    
    log.info("")
    log.info("═" * 70)
    log.info("✓ PIPELINE COMPLETE")
    log.info("═" * 70)

def main():
    parser = argparse.ArgumentParser(
        description="CPG Domain Bridge Pipeline — Independent CPG Brand Lead Generator"
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help=f"Run in test mode ({TEST_MAX_BRANDS} brands, {TEST_MAX_API_CALLS} Hunter API calls max)"
    )
    args = parser.parse_args()
    
    if not HUNTER_API_KEY:
        log.warning("⚠ HUNTER_API_KEY not set. Enrichment will be skipped.")
        log.warning("  export HUNTER_API_KEY='your_key'")
    
    asyncio.run(run_pipeline(test_mode=args.test))

if __name__ == "__main__":
    main()
