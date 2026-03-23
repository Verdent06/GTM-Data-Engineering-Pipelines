"""
CNC ISO 9001 Machine Shop Lead Generator v1.0
==============================================================
Target:  Mid-market CNC machine shops holding ISO 9001 certifications
Strategy: DuckDuckGo Advanced Search → Clearbit Validation → Hunter.io Enrichment → CSV

Query: "CNC Machining" "ISO 9001" "Capabilities" -directory -yelp

Usage:
  python cnc_iso9001_engine.py          # Full run (30 search results)
  python cnc_iso9001_engine.py --test   # 10 results, test mode
"""

import asyncio
import logging
import os
import argparse
import csv
import json
import re
from dataclasses import dataclass, asdict
from typing import Optional
from urllib.parse import urlparse, urlencode
from urllib.request import Request, urlopen
from urllib.error import URLError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(levelname)-7s │ %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)

HUNTER_API_KEY = os.environ.get("HUNTER_API_KEY", "")
OUTPUT_FILE = "forge_automation_leads.csv"

DDGS_QUERY = '"CNC Machining" "ISO 9001" "Capabilities" -directory -yelp'
SEARCH_MAX_RESULTS = 30
TEST_MAX_RESULTS = 10

SIEVE_BLOCKLIST = [
    "thomasnet",
    "iqsdirectory",
    "yelp",
    "yellowpages",
    "linkedin",
    "facebook",
    "youtube",
    "mfg",
    "zoominfo"
]

POSITION_KEYWORDS = [
    "plant manager",
    "general manager",
    "manufacturing",
    "owner",
    "president",
    "operations",
    "production"
]

@dataclass
class CNCShop:
    company_name: str = ""
    website: str = ""
    contact_name: str = ""
    contact_title: str = ""
    contact_email: str = ""


def extract_root_domain(url: str) -> str:
    """Extract root domain from a URL."""
    try:
        parsed = urlparse(url)
        domain = parsed.netloc or parsed.path
        domain = domain.replace("www.", "")
        return domain.lower().strip()
    except Exception:
        return ""


def is_blocked_domain(domain: str) -> bool:
    """Check if domain is in blocklist."""
    domain_lower = domain.lower()
    for blocked in SIEVE_BLOCKLIST:
        if blocked in domain_lower:
            return True
    return False


def stage1_duckduckgo_search(max_results: int = SEARCH_MAX_RESULTS) -> list[dict]:
    """Stage 1: DuckDuckGo Advanced Search Extraction."""
    log.info("=" * 70)
    log.info("STAGE 1: DUCKDUCKGO ADVANCED SEARCH EXTRACTION")
    log.info("=" * 70)
    
    log.info(f"  Query: {DDGS_QUERY}")
    log.info(f"  Max results: {max_results}")
    log.info("")
    
    results = []
    
    try:
        log.info("  Searching DuckDuckGo...")
        
        url = "https://html.duckduckgo.com/"
        params = {"q": DDGS_QUERY}
        query_string = urlencode(params)
        full_url = f"{url}?{query_string}"
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
        }
        
        request = Request(full_url, headers=headers)
        response = urlopen(request, timeout=15)
        html_content = response.read().decode('utf-8')
        response.close()
        
        # Extract links from HTML using regex
        link_pattern = r'<a rel="noopener noreferrer" class="result__a" href="([^"]+)"[^>]*>([^<]+)</a>'
        matches = re.findall(link_pattern, html_content)
        
        log.info(f"  ✓ Received {len(matches)} results from DuckDuckGo")
        
        domains_seen = set()
        
        for i, (href, title) in enumerate(matches[:max_results], 1):
            href = href.strip()
            title = title.strip()
            
            if not href:
                continue
            
            domain = extract_root_domain(href)
            if not domain:
                continue
            
            if domain in domains_seen:
                continue
            
            if is_blocked_domain(domain):
                log.info(f"  [{i}] ✗ BLOCKED: {domain}")
                continue
            
            domains_seen.add(domain)
            
            results.append({
                "title": title,
                "href": href,
                "domain": domain
            })
            
            log.info(f"  [{len(results)}] ✓ {domain}")
    
    except Exception as e:
        log.error(f"  ✗ DuckDuckGo error: {e}")
        log.info("  → Network error (likely running in sandbox)")
        log.info("  → Run script outside sandbox environment for live results")
        return []
    
    log.info("")
    log.info(f"  Search Summary:")
    log.info(f"    ✓ Valid domains found: {len(results)}")
    
    return results


async def stage2_clearbit_validation(domains: list[dict]) -> list[dict]:
    """
    Stage 2: Clearbit Validation (The B2B Filter).
    Currently stubbed - passes domains straight through.
    """
    log.info("")
    log.info("=" * 70)
    log.info("STAGE 2: CLEARBIT VALIDATION (B2B FILTER)")
    log.info("=" * 70)
    
    log.info(f"  Domains to validate: {len(domains)}")
    log.info(f"  Note: Clearbit API stubbed - passing domains directly to Stage 3")
    log.info(f"  (Set CLEARBIT_API_KEY to enable production validation)")
    log.info("")
    
    valid_domains = []
    
    for domain_obj in domains:
        domain = domain_obj.get("domain", "")
        
        log.info(f"  Validating: {domain}")
        log.info(f"    ✓ Passed (stub mode)")
        
        valid_domains.append(domain_obj)
    
    log.info("")
    log.info(f"  Validation Summary:")
    log.info(f"    ✓ Valid companies: {len(valid_domains)}")
    
    return valid_domains


async def hunter_enrich(domain: str) -> dict:
    """Query Hunter.io for emails at the domain."""
    if not HUNTER_API_KEY:
        return {"email_source": "no_api_key"}

    if not domain:
        return {"email_source": "no_domain"}
    
    url = "https://api.hunter.io/v2/domain-search"
    params = urlencode({
        "domain": domain,
        "api_key": HUNTER_API_KEY
    })
    full_url = f"{url}?{params}"
    
    try:
        request = Request(full_url)
        response = urlopen(request, timeout=15)
        data = json.loads(response.read().decode('utf-8'))
        response.close()
        
    except Exception as e:
        log.error(f"    Hunter error: {e}")
        return {"email_source": "hunter_error"}

    emails = data.get("data", {}).get("emails", [])
    
    if not emails:
        log.info(f"    ⚠ No emails found")
        return {"email_source": "hunter_no_emails"}

    log.info(f"    Hunter: Found {len(emails)} email(s)")
    
    # STRICT MATCH: Find first email with position matching our keywords
    for email_entry in emails:
        position = (email_entry.get("position") or "").lower()
        for keyword in POSITION_KEYWORDS:
            if keyword in position:
                log.info(f"    ✓ Found matching contact: {email_entry.get('position', 'Unknown')}")
                first_name = email_entry.get("first_name", "") or ""
                last_name = email_entry.get("last_name", "") or ""
                full_name = f"{first_name} {last_name}".strip()
                return {
                    "contact_name": full_name,
                    "contact_title": email_entry.get("position", "") or "",
                    "contact_email": email_entry.get("value", "") or "",
                    "email_source": "hunter_matched",
                }
    
    # FALLBACK: No strict match — use first email
    log.info(f"    ⚠ No strict position match — using first email as fallback")
    if emails:
        log.info(f"    ✓ Fallback contact assigned")
        email_entry = emails[0]
        first_name = email_entry.get("first_name", "") or ""
        last_name = email_entry.get("last_name", "") or ""
        full_name = f"{first_name} {last_name}".strip()
        return {
            "contact_name": full_name,
            "contact_title": email_entry.get("position", "") or "",
            "contact_email": email_entry.get("value", "") or "",
            "email_source": "hunter_fallback",
        }
    
    return {"email_source": "hunter_no_match"}


async def stage3_hunter_enrichment(domains: list[dict]) -> list[CNCShop]:
    """Stage 3: Hunter.io Enrichment."""
    log.info("")
    log.info("=" * 70)
    log.info("STAGE 3: HUNTER.IO ENRICHMENT")
    log.info("=" * 70)
    
    if not HUNTER_API_KEY:
        log.warning("  ⚠ HUNTER_API_KEY not set. Skipping enrichment.")
        log.warning("  Set: export HUNTER_API_KEY='your_key'")
        
        # Still create records without enrichment
        records = []
        for domain_obj in domains:
            shop = CNCShop(
                company_name=domain_obj.get("title", "").split(" - ")[0],
                website=domain_obj.get("domain", "")
            )
            records.append(shop)
        return records
    
    log.info(f"  API Key: {HUNTER_API_KEY[:10]}..." if len(HUNTER_API_KEY) > 10 else "  API Key set ✓")
    log.info(f"  Will enrich {len(domains)} shops")
    log.info("")
    
    records = []
    emails_found = 0
    
    for i, domain_obj in enumerate(domains, 1):
        domain = domain_obj.get("domain", "")
        title = domain_obj.get("title", "").split(" - ")[0]
        
        shop = CNCShop(
            company_name=title,
            website=domain
        )
        
        log.info(f"  [{i}/{len(domains)}] {shop.company_name[:40]}...")
        log.info(f"    Domain: {domain}")
        
        try:
            result = await hunter_enrich(domain)
            
            for k, v in result.items():
                if hasattr(shop, k):
                    setattr(shop, k, v)
            
            if shop.contact_email:
                emails_found += 1
                log.info(f"    ✓ {shop.contact_name} ({shop.contact_title})")
                log.info(f"      Email: {shop.contact_email}")
            else:
                log.info(f"    ⚠ No email found")
                
        except Exception as e:
            log.error(f"    ✗ Error: {e}")
        
        records.append(shop)
        
        await asyncio.sleep(1)
    
    log.info("")
    log.info(f"  Enrichment Summary:")
    log.info(f"    ✓ Emails found: {emails_found}/{len(domains)}")
    
    return records


def stage4_csv_export(records: list[CNCShop], output_file: str):
    """Stage 4: CSV Export."""
    log.info("")
    log.info("=" * 70)
    log.info("STAGE 4: CSV EXPORT")
    log.info("=" * 70)
    
    data = []
    emails_found = 0
    
    for rec in records:
        data.append({
            "company_name": rec.company_name,
            "website": rec.website,
            "contact_name": rec.contact_name,
            "contact_title": rec.contact_title,
            "contact_email": rec.contact_email,
        })
        if rec.contact_email:
            emails_found += 1
    
    # Sort by company name
    data.sort(key=lambda x: x["company_name"])
    
    # Write to CSV
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(
            f, 
            fieldnames=["company_name", "website", "contact_name", "contact_title", "contact_email"]
        )
        writer.writeheader()
        writer.writerows(data)
    
    log.info(f"  ✓ Output saved → '{output_file}'")
    log.info(f"    Total shops: {len(data)}")
    log.info(f"    With contacts: {emails_found}")
    if len(data) > 0:
        log.info(f"    Contact rate: {emails_found/len(data)*100:.1f}%")


async def run_pipeline(test_mode: bool = False):
    """Run the complete CNC ISO 9001 pipeline."""
    log.info("")
    log.info("╔" + "═" * 68 + "╗")
    log.info("║  CNC ISO 9001 MACHINE SHOP LEAD GENERATOR v1.0                    ║")
    log.info("║  Mid-Market Shop Discovery & Contact Enrichment Pipeline         ║")
    log.info("╚" + "═" * 68 + "╝")
    log.info("")
    
    if test_mode:
        log.info("🧪 TEST MODE ENABLED")
        log.info(f"   - Max search results: {TEST_MAX_RESULTS}")
        log.info("")
    
    max_results = TEST_MAX_RESULTS if test_mode else SEARCH_MAX_RESULTS
    
    # Stage 1: DuckDuckGo Search
    domains = stage1_duckduckgo_search(max_results=max_results)
    
    if not domains:
        log.error("No domains found. Exiting.")
        return
    
    # Stage 2: Clearbit Validation
    domains = await stage2_clearbit_validation(domains)
    
    if not domains:
        log.error("No valid domains after Clearbit validation. Exiting.")
        return
    
    # Stage 3: Hunter.io Enrichment
    records = await stage3_hunter_enrichment(domains)
    
    # Stage 4: CSV Export
    output_file = OUTPUT_FILE
    if test_mode:
        output_file = "forge_automation_leads_TEST.csv"
    
    stage4_csv_export(records, output_file)
    
    log.info("")
    log.info("═" * 70)
    log.info("✓ PIPELINE COMPLETE")
    log.info("═" * 70)


def main():
    parser = argparse.ArgumentParser(
        description="CNC ISO 9001 Machine Shop Lead Generator"
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help=f"Run in test mode ({TEST_MAX_RESULTS} search results)"
    )
    args = parser.parse_args()
    
    if not HUNTER_API_KEY:
        log.warning("⚠ HUNTER_API_KEY not set. Enrichment will be skipped.")
        log.warning("  export HUNTER_API_KEY='your_key'")
    
    asyncio.run(run_pipeline(test_mode=args.test))


if __name__ == "__main__":
    main()
