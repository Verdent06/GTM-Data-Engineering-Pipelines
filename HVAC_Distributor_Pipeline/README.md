# Mercura HVAC Domain Bridge Pipeline v1.0

> Independent HVAC Distributor Lead Generator with Clearbit Domain Discovery + Apollo.io Enrichment

---

## 📋 Overview

A production-grade async Python pipeline that discovers and enriches contact information for 15 independent US HVAC/Plumbing distributors. Uses Clearbit for company domain discovery and Apollo.io for contact extraction filtered by sales/operations leadership titles.

**Pipeline Architecture:**

```
Seed List (15 HVAC Distributors)
    ↓
Stage 1: Load seed list
    ↓
Stage 2: Clearbit domain discovery (GET requests)
    ↓
Stage 3: Apollo.io enrichment (POST requests, filtered titles)
    ↓
Stage 4: CSV export
    ↓
mercura_hvac_leads.csv
```

---

## 🎯 Target Distributors

15 independent US HVAC/Plumbing distributors:

- Johnstone Supply
- Munch's Supply
- Coburn Supply Company
- Gustave A. Larson
- Behler-Young
- Slakey Brothers
- Gensco
- Habegger Corporation
- US Air Conditioning Distributors
- Peirce-Phelps
- Homans Associates
- Mingledorff's
- Illco
- Century HVAC Distributing
- Ed's Supply

---

## 🔍 Enrichment Criteria

Apollo.io searches each domain for contacts matching **any** of these sales/operations titles (case-insensitive):

- `branch manager`
- `inside sales`
- `vp of sales`
- `sales manager`
- `operations`

Returns the first verified contact found with name, title, and email.

---

## ⚡ Quick Start

### Prerequisites

- Python 3.7+
- `pip install aiohttp pandas`
- Apollo API key (free tier available)

### 1. Get Your Apollo API Key

Log in to [Apollo.io](https://app.apollo.io) → Settings → Integrations → API → Copy Key

### 2. Export API Key

```bash
export APOLLO_API_KEY="your_api_key_here"
```

### 3. Run Pipeline

```bash
# Test mode: 5 distributors, 5 API calls
python mercura_hvac_domain_bridge.py --test

# Full run: 15 distributors
python mercura_hvac_domain_bridge.py
```

### 4. Check Output

```bash
cat mercura_hvac_leads.csv
```

---

## 📊 Output Format

### CSV Columns

| Column             | Description                           | Example                         |
| ------------------ | ------------------------------------- | ------------------------------- |
| `distributor_name` | HVAC distributor name                 | Johnstone Supply                |
| `website`          | Company domain discovered by Clearbit | johnstone-supply.com            |
| `contact_name`     | Full name (first + last)              | John Smith                      |
| `contact_title`    | Job title from Apollo                 | Branch Manager                  |
| `contact_email`    | Verified email address                | john.smith@johnstone-supply.com |
| `email_source`     | Data source indicator                 | apollo_sales_ops                |

### email_source Values

| Value                | Meaning                                         |
| -------------------- | ----------------------------------------------- |
| `apollo_sales_ops`   | ✓ Found verified contact matching target titles |
| `apollo_no_email`    | Found contact but email is unverified           |
| `apollo_no_contacts` | No matching contacts found for domain           |
| `apollo_error`       | API error during search                         |
| `not_found`          | No website domain discovered in Stage 2         |

---

## 🛠️ API Configuration

### Apollo.io Integration

**Endpoint:** `POST https://api.apollo.io/v1/mixed_people/search`

**Request Headers:**

```json
{
  "Cache-Control": "no-cache",
  "Content-Type": "application/json"
}
```

**Request Payload:**

```json
{
  "api_key": "your_api_key",
  "q_organization_domains": "johnstone-supply.com",
  "person_titles": [
    "branch manager",
    "inside sales",
    "vp of sales",
    "sales manager",
    "operations"
  ]
}
```

**Response Parsing:**

- Looks for `data["people"]` array
- Extracts first contact's: `first_name`, `last_name`, `title`, `email`
- Handles None/unverified emails gracefully

### Clearbit Integration

**Endpoint:** `GET https://autocomplete.clearbit.com/v1/companies/suggest`

**Query Parameter:** `query` (distributor name)

**Response Parsing:**

- Extracts `domain` from first result
- Filters against HVAC-focused aggregator blocklist
- Safe 1-second delays between requests to avoid rate limiting

---

## 📚 Documentation Files

| File                            | Purpose                                 |
| ------------------------------- | --------------------------------------- |
| `mercura_hvac_domain_bridge.py` | Main pipeline script (434 lines)        |
| `mercura_hvac_leads.csv`        | Output file (created after run)         |
| `QUICK_START.md`                | 2-minute setup guide                    |
| `APOLLO_PIVOT_GUIDE.md`         | Technical migration details             |
| `CHANGES.txt`                   | Complete list of changes from Hunter.io |
| `README.md`                     | This file                               |

---

## 🔧 Advanced Options

### Test Mode

```bash
python mercura_hvac_domain_bridge.py --test
```

- Processes only 5 distributors
- Makes maximum 5 Apollo API calls
- Output: `mercura_hvac_leads_TEST.csv`
- Useful for validating setup before full run

### Environment Variables

```bash
export APOLLO_API_KEY="your_key_here"  # Required
```

---

## 📈 Performance & Costs

### API Call Usage

- **Clearbit:** Free tier (1,000 calls/month) — No credits deducted
- **Apollo.io:** ~1 credit per successful contact found (depends on plan)
- **Test Mode:** ~5 Apollo credits maximum
- **Full Run:** ~7-15 Apollo credits (varies by match rate)

### Timing

- **Clearbit Stage:** ~15-20 seconds (1s delay per request)
- **Apollo Stage:** ~5-10 seconds (varies with API response time)
- **Total Runtime:** ~30-40 seconds full run

---

## 🛡️ Error Handling

The pipeline includes robust error handling:

- **Network errors:** Logged, record skipped, pipeline continues
- **HTTP errors:** Logged with status code, fallback to `*_error` source
- **Missing data:** Safely handles None values, uses defaults
- **Rate limiting:** 1-second delays between requests
- **Hard API limits:** Respects `max_api_calls` parameter in test mode

---

## 🏗️ Architecture Details

### Async Design

Full async/await pattern using `aiohttp`:

```python
async with ApolloEnrichmentClient() as client:
    enriched_rec = await client.enrich(record)
```

Benefits:

- Non-blocking I/O during API calls
- Can handle network delays efficiently
- Clean, scalable code pattern

### Logging

Standard format for easy parsing/filtering:

```
14:30:25 │ INFO    │ ✓ FOUND: johnstone-supply.com
14:30:26 │ INFO    │ ✓ Found Sales/Ops contact: Branch Manager
14:30:26 │ INFO    │ Email: john.smith@johnstone-supply.com
```

### Data Flow

1. **HVACDistributor** dataclass holds: name, website, contact fields, source
2. **discover_domains()** populates website field
3. **ApolloEnrichmentClient** populates contact fields
4. **export_to_csv()** writes clean CSV with coverage stats

---

## 🔄 Aggregator Blocklist

Domains blocked during domain discovery (marketplace/directory/job aggregators):

- Marketplaces: amazon.com, ebay.com, homedepot.com, lowes.com, grainger.com
- Directories: yelp.com, google.com, yellowpages.com, bbb.org
- Job boards: indeed.com, linkedin.com, glassdoor.com
- Social media: facebook.com, instagram.com, twitter.com, tiktok.com
- Media: forbes.com, bloomberg.com, cnbc.com

These domains would pollute your lead list, so they're automatically rejected.

---

## 📝 Logging Output Example

```
╔════════════════════════════════════════════════════════════════════╗
║  MERCURA HVAC DOMAIN BRIDGE PIPELINE v1.0                         ║
║  Independent HVAC Distributor Lead Generator                      ║
╚════════════════════════════════════════════════════════════════════╝

======================================================================
STAGE 1: LOAD SEED DISTRIBUTOR LIST
======================================================================
  ✓ Loaded 15 curated independent US HVAC/Plumbing distributors
  Sample distributors: ['Johnstone Supply', "Munch's Supply", ...]
  ✓ Ready to discover domains for 15 distributors

======================================================================
STAGE 2: DOMAIN DISCOVERY (Clearbit Autocomplete API)
======================================================================
  [1/15] Searching: Johnstone Supply...
    ✓ FOUND: johnstone-supply.com
  [2/15] Searching: Munch's Supply...
    ✓ FOUND: munchsupply.com
  ...
  Domain Discovery Summary:
    ✓ Found:      12
    ✗ Blocked:    0 (aggregator domains rejected)
    ⚠ Not found:  3

======================================================================
STAGE 3: APOLLO.IO ENRICHMENT
======================================================================
  Records with valid domains: 12
  Records without domains: 3
  Records with blocked domains: 0
  Will attempt enrichment on: 12 records

  [1/12] Enriching: Johnstone Supply... (johnstone-supply.com)
    [API calls used: 1/∞]
    Apollo: Found 2 contact(s)
    ✓ Found Sales/Ops contact: Branch Manager
    ✓ John Smith (Branch Manager)
      Email: john.smith@johnstone-supply.com
  ...
  Enrichment Summary:
    ✓ Apollo API calls made: 7
    ✓ Emails found: 7/12
    ⚠ Unenriched (kept in CSV): 5

======================================================================
STAGE 4: CSV EXPORT
======================================================================
  ✓ Output saved → 'mercura_hvac_leads.csv'
    Total distributors: 15
    With websites:      12 (80.0%)
    With emails:        7 (46.7%)
    Website coverage:   80.0%
    Email coverage:     46.7%

======================================================================
✓ PIPELINE COMPLETE
======================================================================
```

---

## 🐛 Troubleshooting

### "APOLLO_API_KEY not set"

```bash
export APOLLO_API_KEY="your_key_here"
echo $APOLLO_API_KEY  # Verify it's set
```

### No contacts found for any distributor?

1. Check Apollo dashboard for remaining credits
2. Verify API key is correct
3. Some domains may genuinely have no matching contacts
4. Run in test mode first: `--test`

### Pipeline seems stuck?

- Normal: Clearbit requests have 1-second delays to avoid rate limits
- Check network connectivity
- Check Apollo API status page

### ImportError for aiohttp or pandas?

```bash
pip install aiohttp pandas
```

---

## 📖 Further Reading

- [Apollo.io API Docs](https://docs.apollo.io)
- [Clearbit Company Search](https://clearbit.com/docs)
- Python [asyncio Documentation](https://docs.python.org/3/library/asyncio.html)

---

## 📄 License & Usage

This pipeline is for GTM/outreach purposes. Use responsibly:

- Respect rate limits on all APIs
- Follow Apollo.io and Clearbit terms of service
- Only contact verified email addresses
- Honor email list preferences and opt-outs

---

## ✅ Validation Checklist

Before running in production:

- [ ] Apollo API key obtained and tested
- [ ] Environment variable set: `export APOLLO_API_KEY="..."`
- [ ] Test run successful: `python mercura_hvac_domain_bridge.py --test`
- [ ] CSV generated with expected columns
- [ ] Sample contacts reviewed for accuracy
- [ ] Email addresses are valid and verified
- [ ] Ready for full 15-distributor run

---

**Questions?** Refer to `QUICK_START.md` or `APOLLO_PIVOT_GUIDE.md`.

---

**Last Updated:** February 26, 2026  
**Pipeline Version:** 1.0  
**Enrichment Provider:** Apollo.io (formerly Hunter.io)
