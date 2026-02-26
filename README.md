# B2B Go-To-Market Data Engineering Pipelines

A collection of asynchronous Python data pipelines designed to bypass legacy data brokers (like ZoomInfo) and programmatically map the Total Addressable Market (TAM) for highly fragmented, analog industries.

## The Architecture

Traditional B2B lead generation relies on scraping LinkedIn or Google Maps, which fails for offline industries (like heavy machinery, commercial trucking, or local healthcare). These pipelines use a 3-stage architecture:

1. **Extraction (Bypassing Scrapers):** Querying obscure endpoints like the DOT FMCSA Socrata API, the federal CMS Datastore, or manufacturer dealer locators.
2. **The Domain Bridge:** Using AI and search APIs (Clearbit Autocomplete, DDGS) to dynamically map company names to corporate root domains, filtering out aggregators (Yelp, Amazon, BBB).
3. **Targeted Enrichment:** Piping clean domains into Apollo.io and Hunter.io APIs, utilizing custom title-priority sorting to extract specific decision-makers (e.g., VP of Supply Chain, Agency Owner, Dealer Principal).

## Pipelines Included

- `CPG_Supply_Chain/`: Maps independent, high-growth consumer packaged goods brands.
- `Commercial_Logistics/`: Extracts fleet owners (5-50 power units) directly from federal DOT registries.
- `Healthcare_Agencies/`: Uses dynamic pagination to extract all 11,000+ US Home Health Agencies from the CMS Datastore.
- `Heavy_Equipment/`: Uses headless Playwright to navigate DOM-rendered directories and extract authorized machinery dealers.

## Tech Stack

- `asyncio` / `aiohttp` for concurrent API requests and rate-limit handling.
- `pandas` for dataframe manipulation and filtering.
- `playwright` for DOM-rendered target extraction.
- Custom semaphore logic for strict API credit protection (Apollo/Hunter limits).
