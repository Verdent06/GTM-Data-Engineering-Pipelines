import asyncio
import aiohttp
import pandas as pd
import logging
import os
from dataclasses import dataclass, asdict
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(levelname)-7s │ %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)

HUNTER_API_KEY = os.environ.get("HUNTER_API_KEY", "")
INPUT_FILE = "clean_dental_seed.csv"
OUTPUT_FILE = "egress_health_leads.csv"
HUNTER_URL = "https://api.hunter.io/v2/domain-search"

POSITION_KEYWORDS = [
    "practice manager", "office manager", "owner", 
    "dental director", "administrator", "dentist"
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


def load_seed_data() -> list[DentalClinicRecord]:
    """Load clinics from clean_dental_seed.csv."""
    log.info("=" * 70)
    log.info("STAGE 1: LOAD SEED DATA")
    log.info("=" * 70)
    
    try:
        df = pd.read_csv(INPUT_FILE)
        log.info(f"  ✓ Loaded {INPUT_FILE}")
        log.info(f"  Total clinics: {len(df)}")
    except FileNotFoundError:
        log.error(f"  ✗ File not found: {INPUT_FILE}")
        return []
    except Exception as e:
        log.error(f"  ✗ Error reading file: {e}")
        return []
    
    records = []
    for _, row in df.iterrows():
        record = DentalClinicRecord(
            clinic_name=str(row.get("clinic_name", "")).strip(),
            city=str(row.get("city", "")).strip(),
            state=str(row.get("state", "")).strip(),
            website=str(row.get("website", "")).strip(),
        )
        if record.clinic_name and record.website:
            records.append(record)
    
    log.info(f"  ✓ Parsed {len(records)} valid clinics")
    return records


async def enrich_via_hunter(session: aiohttp.ClientSession, record: DentalClinicRecord) -> Optional[dict]:
    """Query Hunter.io for emails at the clinic domain."""
    if not record.website:
        return None
    
    params = {
        "domain": record.website,
        "api_key": HUNTER_API_KEY,
    }
    
    try:
        async with session.get(
            HUNTER_URL,
            params=params,
            timeout=aiohttp.ClientTimeout(total=15)
        ) as resp:
            if resp.status != 200:
                log.warning(f"    Hunter HTTP {resp.status} for '{record.website}'")
                return None
            data = await resp.json()
    
    except Exception as e:
        log.error(f"    Hunter error: {e}")
        return None
    
    emails = data.get("data", {}).get("emails", [])
    
    if not emails:
        log.info(f"    ⚠ No emails found")
        return None
    
    log.info(f"    Hunter: Found {len(emails)} email(s)")
    
    # STRICT MATCH: Find first email matching position keywords
    for email_entry in emails:
        position = email_entry.get("position", "") or ""
        position_lower = position.lower()
        
        # Check if position contains any of our keywords
        for keyword in POSITION_KEYWORDS:
            if keyword in position_lower:
                first_name = email_entry.get("first_name", "") or ""
                last_name = email_entry.get("last_name", "") or ""
                full_name = f"{first_name} {last_name}".strip()
                
                log.info(f"    ✓ Found matching contact: {position}")
                return {
                    "contact_name": full_name,
                    "contact_title": position,
                    "contact_email": email_entry.get("value", "") or "",
                }
    
    # FALLBACK: No strict position match — take first email
    log.info(f"    ⚠ No strict position match — using first email as fallback")
    first_email = emails[0]
    first_name = first_email.get("first_name", "") or ""
    last_name = first_email.get("last_name", "") or ""
    full_name = f"{first_name} {last_name}".strip()
    
    log.info(f"    ✓ Fallback contact: {full_name}")
    return {
        "contact_name": full_name,
        "contact_title": "General/Unknown",
        "contact_email": first_email.get("value", "") or "",
    }


async def enrich_all(records: list[DentalClinicRecord]) -> list[DentalClinicRecord]:
    """Enrich all records with Hunter.io data."""
    log.info("")
    log.info("=" * 70)
    log.info("STAGE 2: HUNTER.IO ENRICHMENT")
    log.info("=" * 70)
    
    if not HUNTER_API_KEY:
        log.error("  ✗ HUNTER_API_KEY not set. Cannot proceed.")
        log.error("  Set: export HUNTER_API_KEY='your_key'")
        return records
    
    log.info(f"  API Key: {HUNTER_API_KEY[:10]}..." if len(HUNTER_API_KEY) > 10 else "  API Key set ✓")
    log.info(f"  Will enrich {len(records)} clinics")
    log.info("")
    
    enriched_count = 0
    
    async with aiohttp.ClientSession() as session:
        for i, rec in enumerate(records):
            log.info(f"  [{i+1}/{len(records)}] {rec.clinic_name[:40]}...")
            log.info(f"    Domain: {rec.website}")
            
            result = await enrich_via_hunter(session, rec)
            
            if result:
                rec.contact_name = result.get("contact_name", "")
                rec.contact_title = result.get("contact_title", "")
                rec.contact_email = result.get("contact_email", "")
                enriched_count += 1
            
            await asyncio.sleep(1)
    
    log.info("")
    log.info(f"  Enrichment Summary:")
    log.info(f"    ✓ Successfully enriched: {enriched_count}/{len(records)}")
    log.info(f"    ⚠ No matches found: {len(records) - enriched_count}")
    
    return records


def export_to_csv(records: list[DentalClinicRecord]):
    """Save enriched data to CSV."""
    log.info("")
    log.info("=" * 70)
    log.info("STAGE 3: CSV EXPORT")
    log.info("=" * 70)
    
    # Prepare data for all records (enriched and unenriched)
    data = []
    enriched_count = 0
    
    for rec in records:
        data.append({
            "clinic_name": rec.clinic_name,
            "city": rec.city,
            "state": rec.state,
            "website": rec.website,
            "contact_name": rec.contact_name,
            "contact_title": rec.contact_title,
            "contact_email": rec.contact_email,
        })
        if rec.contact_email:
            enriched_count += 1
    
    # Initialize DataFrame with explicit columns
    df = pd.DataFrame(data, columns=["clinic_name", "city", "state", "website", "contact_name", "contact_title", "contact_email"])
    
    # Only sort if DataFrame is not empty
    if not df.empty:
        df.sort_values("clinic_name", inplace=True)
    
    df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")
    
    log.info(f"  ✓ Output saved → '{OUTPUT_FILE}'")
    log.info(f"    Total records exported: {len(df)}")
    log.info(f"    With enriched contacts: {enriched_count}")
    log.info(f"    Enrichment rate: {enriched_count/len(df)*100:.1f}%" if len(df) > 0 else "    No records to enrich")


async def main():
    log.info("")
    log.info("╔" + "═" * 68 + "╗")
    log.info("║  HUNTER.IO ENRICHMENT ONLY v1.0                                  ║")
    log.info("║  Clean Seed CSV → Hunter Domain Search → Enriched Leads CSV      ║")
    log.info("╚" + "═" * 68 + "╝")
    log.info("")
    
    records = load_seed_data()
    
    if not records:
        log.error("No records to enrich. Exiting.")
        return
    
    records = await enrich_all(records)
    
    export_to_csv(records)
    
    log.info("")
    log.info("═" * 70)
    log.info("✓ PIPELINE COMPLETE")
    log.info("═" * 70)


if __name__ == "__main__":
    asyncio.run(main())
