"""
EquipmentDealerScraper v2 — MachineryTrader.com Michigan Dealer Pipeline
=====================================================================
Target:  machinerytrader.com/dealer/directory/ (DOM-rendered, state-filtered)
Strategy: Multi-category URL sweep → dedup → Apollo/Hunter enrichment → CSV

Architecture notes:
  - No infinite scroll needed. MT renders all results on one page per URL.
  - We sweep N category/brand URLs all filtered to State=MICHIGAN, then
    deduplicate on normalized company name before enrichment.
  - Playwright navigates as a real browser, satisfying the Referer/cookie
    checks that cause 403s on direct HTTP clients like requests/aiohttp.
  - Apollo enrichment uses org search -> people search title-priority pattern.
  - Hunter.io is the fallback when Apollo returns no email.
"""

import asyncio
import aiohttp
import pandas as pd
import re
import time
import logging
import os
from dataclasses import dataclass, asdict
from typing import Optional
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - [%(module)s] - %(message)s"
)

APOLLO_API_KEY = os.environ.get("APOLLO_API_KEY", "")
HUNTER_API_KEY = os.environ.get("HUNTER_API_KEY", "")

OUTPUT_FILE  = "heavy_equipment_michigan_leads.csv"
STATE_FILTER = "MICHIGAN"

# MT paginates with a disabled attr, so we just keep clicking until we hit it
START_URL = "https://www.machinerytrader.com/dealer/directory/construction-equipment-dealers-in-michigan/?State=MICHIGAN"
SEL_NEXT_BUTTON = "button[aria-label='Next Page']" 

MAX_ENRICHMENT_CONCURRENCY = 5
APOLLO_RPM                 = 50  # free tier caps at 50/min
PAGE_LOAD_WAIT_SECONDS     = 3

@dataclass
class DealerRecord:
    company:       str = ""
    address:       str = ""
    city:          str = ""
    state:         str = ""
    zip_code:      str = ""
    phone:         str = ""
    website:       str = ""
    brands:        str = ""   # pipe-separated
    source_url:    str = ""
    # filled in by enrichment
    owner_name:    str = ""
    owner_title:   str = ""
    owner_email:   str = ""
    email_source:  str = ""   # apollo | hunter | not_found
    apollo_org_id: str = ""

# big companies and chains we don't care about
EXCLUDED_COMPANIES = [
    "united rentals", "sunbelt rentals", "herc rentals", "ahern rentals",
    "macallister rentals", "wolverine rental", "klochko equip rental",
    "casco rent all", "ryder vehicle",
    "maxim crane", "bigge crane", "barnhart", "neff corp",
    "ritchie bros", "iron planet", "purple wave", "miedema auctioneering",
    "bigiron", "proxibid",
    "michigan cat", "macallister", "hutson", "greenmark equipment",
    "tri county equipment", "burnips equipment", "alta equipment",
    "caterpillar financial", "cat financial", "deere financial",
    "cnh industrial", "komatsu america", "volvo financial",
    "roland machinery", "vermeer midwest", "southeastern equipment",
    "miller bradford",
    "truck sales", "truck center", "truck parts", "trucking",
    "trailer sales", "trailer connection", "trailer eq",
    "tank trailer", "peterbilt", "freightliner", "western star",
    "chevrolet", "auto sales", "vehicle sales",
    "custom truck one source", "m&k truck",
    "replacement parts", "aftermarket parts", "diesel parts",
    "reliable aftermarket", "aic replacement", "north american diesel",
    "forklift forks", "integrity lift", "magna forklift",
    "material handling",
    "city of", "county of", "state of ", "township of",
    "public works", "road commission", "transit authority", "army corps",
    "fruitport, michigan", "norton shores, michigan",
    "jim davis", "chad steendam", "john & leroy tomlinson", "m. nolan farms",
]

# regex to spot fake business names (just person names)
_INDIVIDUAL_NAME_RE = re.compile(
    r'^(?:[A-Z][a-z]+\.?\s+){1,2}[A-Z][a-z]+$'
)

# if it has these keywords, it's probably a real biz even if it looks like a name
_BUSINESS_KEYWORDS = re.compile(
    r'\b(equipment|sales|supply|service|machinery|tractor|rental|'
    r'parts|inc|llc|corp|co\b|bros|brothers|sons|enterprises|group|'
    r'solutions|systems|industries|international|midwest|national)\b',
    re.IGNORECASE
)

def _should_exclude(company: str) -> bool:
    # MT adds city/state suffix, strip it before checking
    base = company.split(' - ')[0].strip()
    cl = base.lower()
    if any(excl in cl for excl in EXCLUDED_COMPANIES):
        return True
    # catch "John Smith" type listings that have no biz keywords
    if _INDIVIDUAL_NAME_RE.match(base) and not _BUSINESS_KEYWORDS.search(base):
        return True
    return False

def _is_bad_address(address: str) -> bool:
    """Return True for addresses that are purely numeric, empty, or too short."""
    s = address.strip()
    if not s or s.isdigit() or len(s) < 6:
        return True
    return False

def _normalize_company(name: str) -> str:
    """Produce a dedup key: lowercase, strip legal suffixes and punctuation."""
    s = name.lower().strip()
    s = re.sub(r'\b(inc|llc|ltd|co|corp|company|equipment)\b\.?', '', s)
    s = re.sub(r'[^a-z0-9\s]', '', s)
    return re.sub(r'\s+', ' ', s).strip()

# selectors below are brittle as hell, needs manual tweaking if MT redesigns
SEL_DEALER_CARD = "div.dealer-directory-listing"
SEL_COMPANY     = "a.dealer-title-text"
SEL_ADDRESS     = "div.dealer-data-text"
SEL_PHONE       = "a.dealer-phone"
SEL_WEBSITE     = None
SEL_BRANDS      = None

class EnrichmentClient:
    """
    Apollo.io (primary) + Hunter.io (fallback) async enrichment.
    Sliding-window rate limiter enforces Apollo's 50 RPM limit.
    """
    def __init__(self):
        self._request_count = 0
        self._window_start  = time.monotonic()
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
            logging.info(f"    ⏳ Apollo rate limit — sleeping {wait:.1f}s")
            await asyncio.sleep(wait)
            self._request_count = 0
            self._window_start = time.monotonic()
        self._request_count += 1

    async def enrich_via_apollo(self, company: str, domain: str = "") -> dict:
        """
        Two-step Apollo enrichment:
          1. POST /organizations/search  → resolve org_id + domain
          2. POST /people/search         → filter by org_id + title keywords
        Returns the highest-priority contact (owner > president > GM > ...).
        """
        if not APOLLO_API_KEY:
            return {}

        hdrs = {"Content-Type": "application/json", "X-Api-Key": APOLLO_API_KEY}

        # first get the org ID, then search for people
        await self._throttle()
        org_body = {
            "q_organization_name":      company,
            "organization_locations":   ["United States"],
            "page": 1, "per_page": 1,
        }
        if domain:
            org_body["q_organization_domains"] = [domain]

        try:
            async with self._session.post(
                "https://api.apollo.io/v1/organizations/search",
                json=org_body, headers=hdrs,
                timeout=aiohttp.ClientTimeout(total=15)
            ) as resp:
                if resp.status != 200:
                    logging.warning(f"    Apollo org HTTP {resp.status} for '{company}'")
                    return {}
                data = await resp.json()
            orgs = data.get("organizations", [])
            if not orgs:
                return {}
            org    = orgs[0]
            org_id = org.get("id", "")
            logging.info(f"    ✓ Apollo org: {org.get('name')} [{org.get('primary_domain')}]")
        except Exception as e:
            logging.error(f"    Apollo org error for '{company}': {e}")
            return {}

        # now grab the decision makers (owner, pres, etc)
        await self._throttle()
        TITLE_PRIORITY = ["owner", "president", "principal", "general manager", "gm", "ceo", "founder", "partner"]
        people_body = {
            "organization_ids": [org_id],
            "person_titles":    TITLE_PRIORITY,
            "page": 1, "per_page": 5,
        }
        try:
            async with self._session.post(
                "https://api.apollo.io/v1/people/search",
                json=people_body, headers=hdrs,
                timeout=aiohttp.ClientTimeout(total=15)
            ) as resp:
                if resp.status != 200:
                    return {"apollo_org_id": org_id, "email_source": "not_found"}
                data = await resp.json()
        except Exception as e:
            logging.error(f"    Apollo people error for '{company}': {e}")
            return {"apollo_org_id": org_id, "email_source": "not_found"}

        people = data.get("people", [])
        if not people:
            return {"apollo_org_id": org_id, "email_source": "not_found"}

        def rank(p):
            t = (p.get("title") or "").lower()
            for i, kw in enumerate(TITLE_PRIORITY):
                if kw in t: return i
            return 99

        best = sorted(people, key=rank)[0]
        return {
            "owner_name":    f"{best.get('first_name','')} {best.get('last_name','')}".strip(),
            "owner_title":   best.get("title", ""),
            "owner_email":   best.get("email", ""),
            "apollo_org_id": org_id,
            "email_source":  "apollo" if best.get("email") else "apollo_no_email",
        }

    async def enrich_via_hunter(self, domain: str) -> dict:
        """Hunter.io domain email finder — used as Apollo fallback."""
        if not HUNTER_API_KEY or not domain:
            return {}
        await self._throttle()
        params = {"domain": domain, "api_key": HUNTER_API_KEY, "limit": 5, "type": "personal"}
        try:
            async with self._session.get(
                "https://api.hunter.io/v2/domain-search",
                params=params, timeout=aiohttp.ClientTimeout(total=15)
            ) as resp:
                if resp.status != 200: return {}
                data = await resp.json()
            emails = data.get("data", {}).get("emails", [])
            if not emails: return {}
            PRIORITY = ["owner", "president", "general manager", "director", "principal"]
            def rank(e):
                pos = (e.get("position") or "").lower()
                for i, kw in enumerate(PRIORITY):
                    if kw in pos: return i
                return 99
            best = sorted(emails, key=rank)[0]
            return {
                "owner_name":  f"{best.get('first_name','')} {best.get('last_name','')}".strip(),
                "owner_title": best.get("position", ""),
                "owner_email": best.get("value", ""),
                "email_source": "hunter",
            }
        except Exception as e:
            logging.error(f"    Hunter error for '{domain}': {e}")
            return {}

    async def enrich(self, record: DealerRecord) -> DealerRecord:
        """Orchestrate Apollo -> Hunter fallback. Mutates record in-place."""
        domain = (record.website
                  .replace("https://", "").replace("http://", "")
                  .split("/")[0].strip())

        result = await self.enrich_via_apollo(record.company, domain)

        # apollo came up empty or had no email, try hunter
        if not result.get("owner_email") and domain:
            logging.info(f"    ↳ Hunter fallback for: {domain}")
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

class DealerScraper:

    def __init__(self, headless: bool = True, enrich: bool = True):
        self.headless = headless
        self.enrich   = enrich

    async def _parse_page(self, page, source_url: str) -> list[DealerRecord]:
        """
        Parse all dealer cards on a single MachineryTrader directory page.
        Returns a list of DealerRecord objects for non-excluded dealers.
        """
        records = []
        kept = skipped_whale = skipped_empty = 0

        try:
            await page.wait_for_selector(SEL_DEALER_CARD, timeout=15000)
        except PlaywrightTimeout:
            logging.warning(f"  ⚠ Selector '{SEL_DEALER_CARD}' matched 0 elements.")
            logging.warning("  → Run with headless=False and validate selectors in DevTools.")
            return []

        cards = await page.locator(SEL_DEALER_CARD).all()
        logging.info(f"  Cards found: {len(cards)}")

        for idx, card in enumerate(cards):
            try:
                # grab company name (MT adds city/state suffix, we'll strip that)
                co_loc   = card.locator(SEL_COMPANY)
                raw_name = (await co_loc.first.inner_text()).strip() if await co_loc.count() else ""
                company  = raw_name.split(' - ')[0].strip()
                if not company:
                    skipped_empty += 1
                    continue

                if _should_exclude(raw_name):
                    logging.debug(f"  ✗ EXCL [{idx+1}]: {company}")
                    skipped_whale += 1
                    continue

                # split street and city/state (two separate elements on the page)
                all_addr  = card.locator("div.dealer-data-text")
                addr_count = await all_addr.count()
                address   = (await all_addr.nth(0).inner_text()).strip() if addr_count > 0 else ""
                city_st   = (await all_addr.nth(1).inner_text()).strip() if addr_count > 1 else ""
                city, state, zip_code = "", "MI", ""
                if "," in city_st:
                    city_part, state_zip = city_st.split(",", 1)
                    city = city_part.strip()
                    parts = state_zip.strip().split()
                    state    = parts[0] if parts else "MI"
                    zip_code = parts[1] if len(parts) > 1 else ""

                # trash data check
                if _is_bad_address(address) or _is_bad_address(city):
                    logging.debug(f"  ✗ BAD ADDR [{idx+1}]: {company} — '{address}'")
                    skipped_empty += 1
                    continue

                # grab phone from tel: href
                phone_loc = card.locator(SEL_PHONE)
                phone = ""
                if await phone_loc.count():
                    href = await phone_loc.first.get_attribute("href") or ""
                    phone = href.replace("tel:", "").strip() if "tel:" in href else (await phone_loc.first.inner_text()).strip()

                # MT doesn't list website/brands on the listing page, skip it
                website = ""
                brands  = ""

                logging.info(f"  ✓ [{idx+1}] {company} — {city}, {state} {zip_code}")
                records.append(DealerRecord(
                    company=company, address=address,
                    city=city, state=state, zip_code=zip_code, phone=phone,
                    website=website, brands=brands, source_url=source_url
                ))
                kept += 1

            except Exception as e:
                logging.error(f"  Card {idx} error: {e}")
                continue

        logging.info(f"  Summary: ✓ {kept} kept | ✗ {skipped_whale} excluded | ✗ {skipped_empty} empty")
        return records

    async def _run_pagination(self, page) -> list[DealerRecord]:
        """
        Navigate to START_URL, parse each page, then click Next until
        the button has disabled attribute (last page reached).
        """
        seen: dict[str, DealerRecord] = {}
        page_num = 1

        logging.info("=" * 70)
        logging.info(f"Navigating to start URL...")
        await page.goto(START_URL, timeout=60000, wait_until="domcontentloaded")
        await asyncio.sleep(PAGE_LOAD_WAIT_SECONDS)

        while True:
            logging.info("=" * 70)
            logging.info(f"Parsing page {page_num}...")

            page_records = await self._parse_page(page, page.url)
            added = 0
            for r in page_records:
                key = _normalize_company(r.company)
                if key not in seen:
                    seen[key] = r
                    added += 1
            logging.info(f"  +{added} new unique (running total: {len(seen)})")

            # if next button is gone or disabled, we're done
            next_btn = page.locator(SEL_NEXT_BUTTON)
            if await next_btn.count() == 0:
                logging.info("  No Next button found — done.")
                break
            is_disabled = await next_btn.get_attribute("disabled")
            if is_disabled is not None:
                logging.info(f"  Next button disabled — page {page_num} was the last page.")
                break

            logging.info(f"  Clicking Next → page {page_num + 1}")
            await next_btn.click()
            await page.wait_for_selector(SEL_DEALER_CARD, timeout=15000)
            await asyncio.sleep(PAGE_LOAD_WAIT_SECONDS)
            page_num += 1

        return list(seen.values())

    async def _enrich_all(self, records: list[DealerRecord]) -> list[DealerRecord]:
        """Fan-out enrichment with semaphore-capped concurrency."""
        sem = asyncio.Semaphore(MAX_ENRICHMENT_CONCURRENCY)

        async def _one(client, rec, i, n):
            async with sem:
                logging.info(f"Enriching [{i+1}/{n}]: {rec.company}")
                return await client.enrich(rec)

        async with EnrichmentClient() as client:
            results = await asyncio.gather(
                *[_one(client, r, i, len(records)) for i, r in enumerate(records)],
                return_exceptions=True
            )

        enriched = [r for r in results if not isinstance(r, Exception)]
        errors   = sum(1 for r in results if isinstance(r, Exception))
        hits     = sum(1 for r in enriched if r.owner_email)

        if errors:
            logging.warning(f"  {errors} enrichment tasks raised exceptions")
        logging.info(f"  Email hit rate: {hits}/{len(enriched)} ({hits/max(len(enriched),1)*100:.0f}%)")
        return enriched

    async def run_pipeline(self):
        logging.info("=" * 70)
        logging.info("DEALER SCRAPER v2 — Michigan Heavy Equipment Pipeline")
        logging.info(f"Target: MachineryTrader.com | Pagination: enabled | Enrich: {self.enrich}")
        logging.info("=" * 70)

        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=self.headless,
                args=["--disable-blink-features=AutomationControlled", "--disable-notifications"]
            )
            context = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/121.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1440, "height": 900},
                bypass_csp=True,
            )
            page = await context.new_page()
            records = await self._run_pagination(page)
            await context.close()
            await browser.close()

        logging.info(f"\n✓ Scrape complete: {len(records)} unique Michigan dealers extracted")

        if not records:
            logging.error("Zero records. Run with headless=False and fix selectors.")
            return

        # try to find owner email if we have API keys
        if self.enrich and (APOLLO_API_KEY or HUNTER_API_KEY):
            records = await self._enrich_all(records)
        elif self.enrich:
            logging.warning("Enrichment=True but no API keys set.")
            logging.warning("  export APOLLO_API_KEY=your_key  (and/or HUNTER_API_KEY)")

        # dump to CSV
        df = pd.DataFrame([asdict(r) for r in records])
        col_order = [
            "company", "owner_name", "owner_title", "owner_email", "email_source",
            "phone", "address", "city", "state", "zip_code",
            "source_url", "apollo_org_id"
        ]
        df = df[[c for c in col_order if c in df.columns]]
        df.sort_values("company", inplace=True)
        df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")

        logging.info(f"\n✓ Output saved → '{OUTPUT_FILE}'")
        logging.info(f"  Dealers:      {len(df)}")
        logging.info(f"  Emails found: {df['owner_email'].ne('').sum()}")
        logging.info(f"  Coverage:     {df['owner_email'].ne('').mean()*100:.1f}%")

if __name__ == "__main__":
    scraper = DealerScraper(
        headless=False,   # Set False during selector debugging
        enrich=False      # Set False for a free scrape-only dry run
    )
    asyncio.run(scraper.run_pipeline())