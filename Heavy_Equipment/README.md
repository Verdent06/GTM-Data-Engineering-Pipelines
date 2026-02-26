MiEnviro Lead Extractor (Async Playwright)
An asynchronous, headless web scraping pipeline designed to extract high-value SMB acquisition targets from the Michigan MiEnviro Portal.

This pipeline was built to handle dynamic DOM pagination (infinite scrolling) and bypass strict-mode locator violations in modern SPAs.

Architecture & Tech Stack
Language: Python 3.10+

Browser Automation: asyncio + playwright (Chromium)

Data Processing: pandas

Logging: Standard logging module for production monitoring

Engineering Hurdles Overcome

1. Dynamic Pagination (Auto-Scrolling)
   The MiEnviro portal does not expose a clean API endpoint for search results, nor does it use standard pagination. It uses an infinite-scroll JavaScript trigger.

Solution: Implemented an asynchronous while loop that injects window.scrollTo(0, document.body.scrollHeight) directly into the browser, tracking DOM row counts to detect network-load completion before extraction.

2. Strict Mode Violations & DOM Ambiguity
   During execution, Playwright encountered a strict mode violation: resolved to 2 elements when attempting to interact with the search input.

Solution: Rather than relying on fragile CSS classes, I implemented dynamic logic to traverse the DOM ancestry, isolating the visible search input based on its container context.

3. Signal-to-Noise Optimization (The "Whale" Filter)
   Raw extraction yielded a high volume of un-acquirable entities (Governments, Public Schools, Utility Giants like DTE Energy).

Solution: Built a configurable rules engine within the MiEnviroScraper class that applies exclusion heuristics on the fly, dropping municipal and utility targets to achieve a clean output of actionable SMB leads.

How to Run
Bash

# 1. Clone the repo

git clone https://github.com/yourusername/ghost-business-scraper.git

# 2. Install dependencies

pip install -r requirements.txt
playwright install chromium

# 3. Execute the pipeline

python ghost_scraper.py
