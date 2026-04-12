"""
Fetch recent SEC Form D filings from EDGAR daily index, enrich with state/location
from the filing submission document, filter institutional names, export CSV.
"""

from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Iterable
from xml.etree import ElementTree as ET
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from requests import Response

# -----------------------------------------------------------------------------
# Demo: single index day (yesterday ET), max 20 XML/state fetches, fast throttle.
# Production: last 7 calendar days, enrich all rows after filters, slower throttle.
# -----------------------------------------------------------------------------
DEMO_MODE = True

SEC_USER_AGENT = "Vedant Desai vedantde@umich.edu"
SEC_BASE = "https://www.sec.gov"
REQUEST_TIMEOUT = 30
THROTTLE_FULL_SEC = 0.15
THROTTLE_DEMO_SEC = 0.05
DEMO_XML_CAP = 20
DEMO_INDEX_LOOKBACK_DAYS = 4  # yesterday + up to 3 prior days on 404

INSTITUTIONAL_SUBSTRINGS = ("fund ii", " lp", "trust", "portfolio")

# Lines in daily index before data rows (Description, dashes, blank, header, etc.)
INDEX_SKIP_PREFIXES = ("Description:", "Last Data", "Comments:", "Anonymous", "Form Type")

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s %(message)s",
)
log = logging.getLogger("form_d")


@dataclass(frozen=True)
class IndexRow:
    form_type: str
    company_name: str
    cik: str
    date_filed: str  # YYYYMMDD
    rel_path: str  # edgar/data/...


def quarter_slug(d: date) -> str:
    q = (d.month - 1) // 3 + 1
    return f"QTR{q}"


def daily_index_url(d: date) -> str:
    return (
        f"{SEC_BASE}/Archives/edgar/daily-index/"
        f"{d.year}/{quarter_slug(d)}/form.{d.strftime('%Y%m%d')}.idx"
    )


def accession_from_rel_path(rel_path: str) -> str:
    """Stable dedupe key from index path (folder + basename without extension)."""
    # edgar/data/CIK/0001234567-25-000001.txt -> 0001234567-25-000001
    m = re.search(
        r"edgar/data/\d+/([^/]+)\.(?:txt|htm|html)$",
        rel_path.strip(),
        re.IGNORECASE,
    )
    if m:
        return m.group(1)
    return rel_path.strip()


def parse_index_line(line: str) -> IndexRow | None:
    """
    Parse a fixed-width daily index data line for Form D or D/A.
    Format ends with: CIK YYYYMMDD edgar/data/...
    """
    line = line.rstrip()
    if not line or line.startswith("-") or line.startswith(INDEX_SKIP_PREFIXES):
        return None
    form_col = line[:12].strip()
    if form_col not in ("D", "D/A"):
        return None
    m = re.search(
        r"(\d{7,10})\s+(\d{8})\s+(edgar/data/\S+)\s*$",
        line,
    )
    if not m:
        return None
    cik, yyyymmdd, rel_path = m.group(1), m.group(2), m.group(3)
    company = line[12 : m.start()].strip()
    return IndexRow(
        form_type=form_col,
        company_name=company,
        cik=cik.lstrip("0") or "0",
        date_filed=yyyymmdd,
        rel_path=rel_path,
    )


def iter_index_rows(text: str) -> Iterable[IndexRow]:
    for line in text.splitlines():
        row = parse_index_line(line)
        if row is not None:
            yield row


def is_institutional_name(name: str) -> bool:
    lower = name.lower()
    return any(s in lower for s in INSTITUTIONAL_SUBSTRINGS)


def extract_state_from_submission(content: str) -> str:
    """
    State / location from Form D submission (.txt): prefer primaryIssuer issuerAddress,
    then SEC-HEADER business address, then incorporation.
    """
    try:
        start = content.find("<edgarSubmission")
        end = content.rfind("</edgarSubmission>")
        if start != -1 and end != -1 and end > start:
            frag = content[start : end + len("</edgarSubmission>")]
            root = ET.fromstring(frag)
            for el in root.iter():
                tag = el.tag.split("}")[-1]
                if tag == "primaryIssuer":
                    for child in el.iter():
                        ctag = child.tag.split("}")[-1]
                        if ctag == "issuerAddress":
                            for addr_el in child.iter():
                                at = addr_el.tag.split("}")[-1]
                                if at == "stateOrCountry" and (addr_el.text and addr_el.text.strip()):
                                    return addr_el.text.strip()
                                if at == "stateOrCountryDescription" and (
                                    addr_el.text and addr_el.text.strip()
                                ):
                                    return addr_el.text.strip()
    except ET.ParseError as e:
        log.warning("XML parse error in submission: %s", e)

    m = re.search(
        r"BUSINESS ADDRESS:\s*(?:.|\n)*?^\s*STATE:\s*(\S+)",
        content,
        re.MULTILINE | re.DOTALL,
    )
    if m:
        return m.group(1).strip()

    m = re.search(r"STATE OF INCORPORATION:\s*(\S+)", content)
    if m:
        return m.group(1).strip()

    return ""


class SecClient:
    def __init__(self, throttle_sec: float) -> None:
        self._throttle_sec = throttle_sec
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": SEC_USER_AGENT})
        self._last_request = 0.0

    def get(self, path_or_url: str) -> Response:
        url = path_or_url if path_or_url.startswith("http") else f"{SEC_BASE}/{path_or_url.lstrip('/')}"
        elapsed = time.monotonic() - self._last_request
        if elapsed < self._throttle_sec:
            time.sleep(self._throttle_sec - elapsed)
        try:
            resp = self._session.get(url, timeout=REQUEST_TIMEOUT)
        except requests.RequestException as e:
            log.error("Request failed %s: %s", url, e)
            raise
        finally:
            self._last_request = time.monotonic()
        return resp


def filing_archive_url(rel_path: str) -> str:
    return f"{SEC_BASE}/Archives/{rel_path}"


def dates_to_fetch(demo: bool) -> list[date]:
    tz = ZoneInfo("America/New_York")
    today = datetime.now(tz).date()
    if demo:
        return [today - timedelta(days=1)]
    return [today - timedelta(days=i) for i in range(1, 8)]


def fetch_demo_index(client: SecClient) -> tuple[date, str] | None:
    """Yesterday ET; walk back up to DEMO_INDEX_LOOKBACK_DAYS on 404. Returns (date, body)."""
    tz = ZoneInfo("America/New_York")
    start = datetime.now(tz).date() - timedelta(days=1)
    for i in range(DEMO_INDEX_LOOKBACK_DAYS):
        d = start - timedelta(days=i)
        text = fetch_index_text(client, d)
        if text:
            if i:
                log.info("Demo index: using %s (skipped %d missing day(s))", d, i)
            return d, text
        log.info("Demo index: no file for %s, trying prior day", d)
    return None


def fetch_index_text(client: SecClient, d: date) -> str | None:
    url = daily_index_url(d)
    try:
        resp = client.get(url)
    except requests.RequestException as e:
        log.warning("Index fetch failed for %s: %s", d, e)
        return None
    if resp.status_code == 404:
        log.info("No daily index for %s (404)", d)
        return None
    if resp.status_code != 200:
        log.warning("Index fetch bad status %s for %s", resp.status_code, d)
        return None
    return resp.text


def yyyymmdd_to_iso(s: str) -> str:
    return f"{s[:4]}-{s[4:6]}-{s[6:8]}"


def main() -> None:
    t0 = time.monotonic()
    demo = DEMO_MODE
    throttle = THROTTLE_DEMO_SEC if demo else THROTTLE_FULL_SEC
    client = SecClient(throttle_sec=throttle)

    log.info("DEMO_MODE=%s throttle=%.3fs", demo, throttle)

    all_rows: list[IndexRow] = []
    if demo:
        resolved = fetch_demo_index(client)
        if resolved is None:
            log.error("Could not load any daily index in demo lookback window.")
            return
        idx_date, text = resolved
        all_rows.extend(iter_index_rows(text))
        log.info("Demo: loaded index for %s, raw parsed rows=%d", idx_date, len(all_rows))
    else:
        for d in dates_to_fetch(demo=False):
            text = fetch_index_text(client, d)
            if text:
                rows = list(iter_index_rows(text))
                all_rows.extend(rows)
                log.info("Full: %s index rows parsed=%d", d, len(rows))
        log.info("Full: total parsed rows across days=%d", len(all_rows))

    log.info("Form D / D-A rows: %d", len(all_rows))

    seen: set[str] = set()
    deduped: list[IndexRow] = []
    for r in all_rows:
        key = accession_from_rel_path(r.rel_path)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(r)
    log.info("After accession dedupe: %d", len(deduped))

    startups: list[IndexRow] = []
    removed_inst = 0
    for r in deduped:
        if is_institutional_name(r.company_name):
            removed_inst += 1
            continue
        startups.append(r)
    log.info(
        "After institutional name filter: %d kept, %d removed",
        len(startups),
        removed_inst,
    )

    to_enrich = startups
    if demo:
        to_enrich = startups[:DEMO_XML_CAP]
        log.info("Demo: capping XML enrichment to %d companies", len(to_enrich))

    records: list[dict[str, str]] = []
    for i, r in enumerate(to_enrich, start=1):
        url = filing_archive_url(r.rel_path)
        try:
            resp = client.get(url)
        except requests.RequestException as e:
            log.warning("Skipping filing fetch %s: %s", url, e)
            records.append(
                {
                    "company_name": r.company_name,
                    "date_filed": yyyymmdd_to_iso(r.date_filed),
                    "state_or_location": "",
                }
            )
            continue
        if resp.status_code != 200:
            log.warning("Bad status %s for %s", resp.status_code, url)
            records.append(
                {
                    "company_name": r.company_name,
                    "date_filed": yyyymmdd_to_iso(r.date_filed),
                    "state_or_location": "",
                }
            )
            continue
        state = extract_state_from_submission(resp.text)
        if not state and demo is False and i % 25 == 0:
            log.info("Enriched %d filings...", i)
        if not state:
            log.warning("No state extracted for %s (%s)", r.company_name, url)
        records.append(
            {
                "company_name": r.company_name,
                "date_filed": yyyymmdd_to_iso(r.date_filed),
                "state_or_location": state,
            }
        )

    df = pd.DataFrame.from_records(
        records,
        columns=["company_name", "date_filed", "state_or_location"],
    )
    out_path = "new_funded_startups.csv"
    df.to_csv(out_path, index=False)

    elapsed = time.monotonic() - t0
    log.info("Total valid startups written to CSV: %d", len(df))
    log.info("Wrote %s", out_path)
    if demo:
        log.info("Demo elapsed wall time: %.2fs", elapsed)


if __name__ == "__main__":
    main()
