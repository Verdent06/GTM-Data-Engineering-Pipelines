import asyncio
import aiohttp
import pandas as pd
import time
import logging
import os
from dataclasses import dataclass, asdict
from typing import Optional
from urllib.parse import urlencode

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - [%(module)s] - %(message)s"
)

SOCRATA_APP_TOKEN = os.environ.get("SOCRATA_APP_TOKEN", "")
APOLLO_API_KEY    = os.environ.get("APOLLO_API_KEY", "")
HUNTER_API_KEY    = os.environ.get("HUNTER_API_KEY", "")

TARGET_STATES    = os.environ.get("TARGET_STATES", "MI").split(",")
MIN_POWER_UNITS  = 5
MAX_POWER_UNITS  = 50
OUTPUT_FILE      = "trucking_fleet_leads.csv"

SOCRATA_API_BASE = "https://data.transportation.gov/resource/kjg3-diqy.json"
PAGE_SIZE        = 1000

MAX_ENRICHMENT_CONCURRENCY = 5
APOLLO_RPM                 = 50


@dataclass
class CarrierRecord:
    company:       str = ""
    usdot_number:  str = ""
    dba_name:      str = ""
    power_units:   int = 0
    phone:         str = ""
    email:         str = ""
    address:       str = ""
    city:          str = ""
    state:         str = ""
    zip_code:      str = ""
    owner_name:    str = ""
    owner_title:   str = ""
    owner_email:   str = ""
    email_source:  str = ""
    apollo_org_id: str = ""


class EnrichmentClient:

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
            logging.info(f"    Apollo rate limit — sleeping {wait:.1f}s")
            await asyncio.sleep(wait)
            self._request_count = 0
            self._window_start = time.monotonic()
        self._request_count += 1

    async def enrich_via_apollo(self, company: str, domain: str = "") -> dict:
        if not APOLLO_API_KEY:
            return {}

        hdrs = {"Content-Type": "application/json", "X-Api-Key": APOLLO_API_KEY}

        await self._throttle()
        org_body = {
            "q_organization_name":    company,
            "organization_locations": ["United States"],
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
            logging.info(f"    Apollo org: {org.get('name')} [{org.get('primary_domain')}]")
        except Exception as e:
            logging.error(f"    Apollo org error for '{company}': {e}")
            return {}

        await self._throttle()
        TITLE_PRIORITY = [
            "owner", "president", "principal", "general manager",
            "gm", "ceo", "founder", "partner", "director of operations",
        ]
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
                if kw in t:
                    return i
            return 99

        best = sorted(people, key=rank)[0]
        return {
            "owner_name":    f"{best.get('first_name', '')} {best.get('last_name', '')}".strip(),
            "owner_title":   best.get("title", ""),
            "owner_email":   best.get("email", ""),
            "apollo_org_id": org_id,
            "email_source":  "apollo" if best.get("email") else "apollo_no_email",
        }

    async def enrich_via_hunter(self, domain: str) -> dict:
        if not HUNTER_API_KEY or not domain:
            return {}
        await self._throttle()
        params = {
            "domain":  domain,
            "api_key": HUNTER_API_KEY,
            "limit":   5,
            "type":    "personal",
        }
        try:
            async with self._session.get(
                "https://api.hunter.io/v2/domain-search",
                params=params, timeout=aiohttp.ClientTimeout(total=15)
            ) as resp:
                if resp.status != 200:
                    return {}
                data = await resp.json()
            emails = data.get("data", {}).get("emails", [])
            if not emails:
                return {}
            PRIORITY = ["owner", "president", "general manager", "director", "principal"]

            def rank(e):
                pos = (e.get("position") or "").lower()
                for i, kw in enumerate(PRIORITY):
                    if kw in pos:
                        return i
                return 99

            best = sorted(emails, key=rank)[0]
            return {
                "owner_name":   f"{best.get('first_name', '')} {best.get('last_name', '')}".strip(),
                "owner_title":  best.get("position", ""),
                "owner_email":  best.get("value", ""),
                "email_source": "hunter",
            }
        except Exception as e:
            logging.error(f"    Hunter error for '{domain}': {e}")
            return {}

    async def enrich(self, record: CarrierRecord) -> CarrierRecord:
        result = await self.enrich_via_apollo(record.company)

        if not result.get("owner_email"):
            logging.info(f"    Hunter fallback for: {record.company}")
            hunter = await self.enrich_via_hunter("")
            for k, v in hunter.items():
                if v and not result.get(k):
                    result[k] = v

        for k, v in result.items():
            if hasattr(record, k):
                setattr(record, k, v)

        if not record.email_source:
            record.email_source = "not_found"

        return record


class TruckingFleetScraper:

    def __init__(self, enrich: bool = True):
        self.enrich = enrich

    def _build_url(self, offset: int, state: str) -> str:
        where_clause = f"phy_state='{state}'"
        params = {
            "$where":  where_clause,
            "$limit":  PAGE_SIZE,
            "$offset": offset,
            "$order":  "dot_number ASC",
        }
        if SOCRATA_APP_TOKEN:
            params["$$app_token"] = SOCRATA_APP_TOKEN
        return f"{SOCRATA_API_BASE}?{urlencode(params)}"

    async def _fetch_page(
        self,
        session: aiohttp.ClientSession,
        offset: int,
        state: str,
    ) -> list[dict]:
        url = self._build_url(offset, state)
        try:
            async with session.get(
                url, timeout=aiohttp.ClientTimeout(total=30)
            ) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    logging.error(f"  Socrata HTTP {resp.status}: {text[:200]}")
                    return []
                return await resp.json()
        except Exception as e:
            logging.error(f"  Fetch error (offset={offset}, state={state}): {e}")
            return []

    async def _run_pagination(
        self, session: aiohttp.ClientSession
    ) -> list[dict]:
        all_rows: list[dict] = []

        for state in TARGET_STATES:
            state = state.strip().upper()
            offset = 0
            page_num = 1
            logging.info("=" * 70)
            logging.info(f"Pulling FMCSA data for state: {state}")

            while True:
                logging.info(f"  Page {page_num} (offset={offset})...")
                rows = await self._fetch_page(session, offset, state)

                if not rows:
                    logging.info(f"  Empty page — finished {state} after {page_num - 1} pages.")
                    break

                all_rows.extend(rows)
                logging.info(f"  +{len(rows)} rows (running total: {len(all_rows)})")

                if len(rows) < PAGE_SIZE:
                    logging.info(f"  Partial page — last page for {state}.")
                    break

                offset   += PAGE_SIZE
                page_num += 1
                await asyncio.sleep(0.25)

        return all_rows

    def _apply_filters(self, raw_rows: list[dict]) -> list[CarrierRecord]:
        if not raw_rows:
            logging.warning("No raw rows to filter.")
            return []

        df = pd.DataFrame(raw_rows)
        logging.info(f"\nRaw rows pulled: {len(df)}")

        for col in [
            "dot_number", "legal_name", "dba_name", "nbr_power_unit",
            "telephone", "email_address",
            "phy_street", "phy_city", "phy_state", "phy_zip",
        ]:
            if col not in df.columns:
                df[col] = ""

        df.fillna("", inplace=True)

        df = df[df["dot_number"].str.strip().ne("")]
        df = df[df["legal_name"].str.strip().ne("")]
        logging.info(f"After dropping null DOT/company: {len(df)}")

        df["nbr_power_unit"] = pd.to_numeric(df["nbr_power_unit"], errors="coerce")
        df.dropna(subset=["nbr_power_unit"], inplace=True)
        df["nbr_power_unit"] = df["nbr_power_unit"].astype(int)

        df = df[
            (df["nbr_power_unit"] >= MIN_POWER_UNITS) &
            (df["nbr_power_unit"] <= MAX_POWER_UNITS)
        ]
        logging.info(f"After power-unit filter ({MIN_POWER_UNITS}–{MAX_POWER_UNITS}): {len(df)}")

        df.drop_duplicates(subset=["dot_number"], inplace=True)
        logging.info(f"After dedup on dot_number: {len(df)}")

        records: list[CarrierRecord] = []
        for _, row in df.iterrows():
            fmcsa_email = row["email_address"].strip()
            rec = CarrierRecord(
                company      = row["legal_name"].strip(),
                usdot_number = row["dot_number"].strip(),
                dba_name     = row["dba_name"].strip(),
                power_units  = int(row["nbr_power_unit"]),
                phone        = row["telephone"].strip(),
                email        = fmcsa_email,
                address      = row["phy_street"].strip(),
                city         = row["phy_city"].strip(),
                state        = row["phy_state"].strip(),
                zip_code     = row["phy_zip"].strip(),
                email_source = "fmcsa" if fmcsa_email else "",
            )
            records.append(rec)

        logging.info(
            f"  FMCSA emails present: "
            f"{sum(1 for r in records if r.email)}/{len(records)}"
        )
        return records

    async def _enrich_all(self, records: list[CarrierRecord]) -> list[CarrierRecord]:
        needs_enrichment = [r for r in records if not r.email]
        already_have     = [r for r in records if r.email]

        logging.info(
            f"\nEnrichment: {len(needs_enrichment)} records need lookup "
            f"({len(already_have)} already have FMCSA email)."
        )

        if not needs_enrichment or not (APOLLO_API_KEY or HUNTER_API_KEY):
            if needs_enrichment:
                logging.warning(
                    "Enrichment=True but no API keys set. "
                    "Export APOLLO_API_KEY and/or HUNTER_API_KEY."
                )
                for r in needs_enrichment:
                    r.email_source = "not_found"
            return already_have + needs_enrichment

        sem = asyncio.Semaphore(MAX_ENRICHMENT_CONCURRENCY)
        n = len(needs_enrichment)

        async def _one(client: EnrichmentClient, rec: CarrierRecord, i: int) -> CarrierRecord:
            async with sem:
                logging.info(f"Enriching [{i + 1}/{n}]: {rec.company}")
                return await client.enrich(rec)

        async with EnrichmentClient() as client:
            results = await asyncio.gather(
                *[_one(client, r, i) for i, r in enumerate(needs_enrichment)],
                return_exceptions=True,
            )

        enriched = [r for r in results if not isinstance(r, Exception)]
        errors   = sum(1 for r in results if isinstance(r, Exception))
        hits     = sum(1 for r in enriched if r.owner_email)

        if errors:
            logging.warning(f"  {errors} enrichment tasks raised exceptions.")
        logging.info(
            f"  Enrichment email hit rate: {hits}/{len(enriched)} "
            f"({hits / max(len(enriched), 1) * 100:.0f}%)"
        )
        return already_have + enriched

    async def run_pipeline(self):
        logging.info("=" * 70)
        logging.info("TRUCKING FMSCA FLEET SCRAPER v1")
        logging.info(f"Target states:      {TARGET_STATES}")
        logging.info(f"Fleet-size filter:  {MIN_POWER_UNITS}–{MAX_POWER_UNITS} power units")
        logging.info(f"Enrichment:         {self.enrich}")
        logging.info(f"App token present:  {bool(SOCRATA_APP_TOKEN)}")
        logging.info("=" * 70)

        headers = {"Accept": "application/json"}
        if SOCRATA_APP_TOKEN:
            headers["X-App-Token"] = SOCRATA_APP_TOKEN

        async with aiohttp.ClientSession(headers=headers) as session:
            raw_rows = await self._run_pagination(session)

        if not raw_rows:
            logging.error("Zero rows returned from FMCSA API. Check network or app token.")
            return

        records = self._apply_filters(raw_rows)

        if not records:
            logging.error(
                "Zero records after filtering. "
                f"Check that TARGET_STATES {TARGET_STATES} contains active carriers "
                f"with {MIN_POWER_UNITS}–{MAX_POWER_UNITS} power units."
            )
            return

        if self.enrich:
            records = await self._enrich_all(records)

        df = pd.DataFrame([asdict(r) for r in records])
        col_order = [
            "company", "usdot_number", "dba_name", "power_units",
            "phone", "email",
            "owner_name", "owner_title", "owner_email", "email_source",
            "address", "city", "state", "zip_code",
            "apollo_org_id",
        ]
        df = df[[c for c in col_order if c in df.columns]]
        df.sort_values(["state", "power_units", "company"], inplace=True)
        df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")

        logging.info("\n" + "=" * 70)
        logging.info(f"Output saved → '{OUTPUT_FILE}'")
        logging.info(f"  Total leads:          {len(df)}")
        logging.info(
            f"  FMCSA email present:  "
            f"{df['email'].ne('').sum()}"
        )
        enriched_hits = df["owner_email"].ne("").sum() if "owner_email" in df.columns else 0
        logging.info(f"  Enrichment emails:    {enriched_hits}")
        total_contactable = df["email"].ne("").sum() + enriched_hits
        logging.info(
            f"  Total contactable:    {total_contactable} "
            f"({total_contactable / max(len(df), 1) * 100:.1f}%)"
        )
        logging.info("=" * 70)


if __name__ == "__main__":
    scraper = TruckingFleetScraper(enrich=False)
    asyncio.run(scraper.run_pipeline())
