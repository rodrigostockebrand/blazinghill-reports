#!/usr/bin/env python3
"""
BlazingHill Report Engine v3 — Premium Data Source Integration

Architecture:
  Phase 1a: Premium data collection (PitchBook, CB Insights, Statista via Cashmere API)
  Phase 1b: Perplexity research (3 calls for digital/social/supplemental data)
  Phase 1c: GPT structuring (combines premium + Perplexity into unified JSON)
  Phase 1d: Gap-filling (targeted Perplexity calls for missing data only)
  Phase 2:  GPT report generation (3 batches, each gets explicit source map)
  Phase 3:  HTML assembly

Key fixes from v2:
  - Each source gets its OWN citation namespace (no cross-contamination)
  - Premium data (PitchBook/Statista) used as PRIMARY authoritative source
  - Perplexity used only for digital/social/reviews + gap-filling
  - Source attribution is explicit: every data point tagged with source_name + source_url
  - GPT report gen receives a flat "source registry" — no numbered citation list
"""

import argparse
import json
import os
import sys
import time
import traceback
import requests
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

PERPLEXITY_API_KEY = os.environ.get("PERPLEXITY_API_KEY", "")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
CASHMERE_API_KEY = os.environ.get("CASHMERE_API_KEY", "")

# Cashmere collection IDs
PITCHBOOK_COMPANY = 217
PITCHBOOK_INVESTOR = 221
CBINSIGHTS_RESEARCH = 211
STATISTA_PREMIUM = 367
STATISTA_FREE = 368


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


# ─── Cashmere API (PitchBook, CB Insights, Statista) ───

def _cashmere_search(query, collection_id, limit=5):
    """Search Cashmere API for premium data sources."""
    if not CASHMERE_API_KEY:
        log(f"  WARN: No CASHMERE_API_KEY — skipping premium search for: {query[:50]}")
        return []

    try:
        resp = requests.get(
            "https://cashmere.io/api/v2/search",
            headers={"Authorization": f"Bearer {CASHMERE_API_KEY}"},
            params={"q": query, "collection": str(collection_id), "limit": limit},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        # Normalize response — may be a list or dict with "results"
        if isinstance(data, list):
            return data
        elif isinstance(data, dict):
            return data.get("results", data.get("data", []))
        return []
    except Exception as e:
        log(f"  WARN: Cashmere search failed for collection {collection_id}: {e}")
        return []


def _extract_premium_data(results, source_name):
    """Extract structured data from Cashmere search results with source attribution."""
    extracted = []
    for r in results:
        if not isinstance(r, dict):
            continue
        extracted.append({
            "content": r.get("content", ""),
            "source_url": r.get("view_source_url", ""),
            "source_name": source_name,
            "title": r.get("omnipub_title", ""),
            "publisher": r.get("omnipub_publisher", source_name),
            "published_at": r.get("omnipub_published_at", ""),
            "score": r.get("score", 0),
        })
    return extracted


# ─── Perplexity API ───

def _perplexity_call(system_msg, user_msg, max_tokens=4000):
    """Make a Perplexity API call. Returns (content, citations_list)."""
    resp = requests.post(
        "https://api.perplexity.ai/chat/completions",
        headers={
            "Authorization": f"Bearer {PERPLEXITY_API_KEY}",
            "Content-Type": "application/json",
        },
        json={
            "model": "sonar-pro",
            "messages": [
                {"role": "system", "content": system_msg},
                {"role": "user", "content": user_msg},
            ],
            "max_tokens": max_tokens,
            "temperature": 0.1,
            "return_citations": True,
        },
        timeout=180,
    )
    resp.raise_for_status()
    data = resp.json()
    content = data["choices"][0]["message"]["content"]
    citations = data.get("citations", [])
    return content, citations


def _perplexity_call_with_sources(system_msg, user_msg, call_name, max_tokens=4000):
    """Perplexity call that returns content with INLINE source attribution.
    
    Instead of relying on numbered citations, we instruct Perplexity to include
    source URLs inline in the text, and we also return the raw citation list
    tagged with the call_name to prevent cross-contamination.
    """
    content, citations = _perplexity_call(system_msg, user_msg, max_tokens)
    
    # Tag each citation with its source call name
    tagged_citations = []
    for i, url in enumerate(citations):
        tagged_citations.append({
            "index": i + 1,
            "url": url,
            "call_name": call_name,
            "source_name": f"Perplexity ({call_name})",
        })
    
    return content, tagged_citations


# ─── GPT API ───

def _gpt_call(system_msg, user_msg, max_tokens=4000):
    """Make a GPT API call."""
    resp = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers={
            "Authorization": f"Bearer {OPENAI_API_KEY}",
            "Content-Type": "application/json",
        },
        json={
            "model": "gpt-4.1",
            "messages": [
                {"role": "system", "content": system_msg},
                {"role": "user", "content": user_msg},
            ],
            "max_tokens": max_tokens,
            "temperature": 0.0,
        },
        timeout=120,
    )
    resp.raise_for_status()
    data = resp.json()
    return data["choices"][0]["message"]["content"]


# ─── Phase 1: Multi-Source Research ───

def run_research(brand_name, domain, market):
    """Phase 1: Collect data from premium sources + Perplexity, then structure."""
    today = datetime.now().strftime("%B %d, %Y")

    if not PERPLEXITY_API_KEY:
        raise RuntimeError("PERPLEXITY_API_KEY not set")
    if not OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY not set")

    # ── Phase 1a: Premium Data Collection ──
    log("Phase 1a: Collecting premium data (PitchBook, CB Insights, Statista)...")
    
    premium_data = {
        "pitchbook": [],
        "cbinsights": [],
        "statista": [],
    }

    # Also check for pre-enriched data file (from agent-assisted runs)
    pre_enriched_path = os.environ.get("ENRICHMENT_FILE", "")
    if pre_enriched_path and os.path.exists(pre_enriched_path):
        log(f"  Found pre-enriched data: {pre_enriched_path}")
        try:
            with open(pre_enriched_path, "r") as f:
                pre_enriched = json.load(f)
            premium_data["pitchbook"] = pre_enriched.get("pitchbook", [])
            premium_data["cbinsights"] = pre_enriched.get("cbinsights", [])
            premium_data["statista"] = pre_enriched.get("statista", [])
            log(f"  Loaded: PB={len(premium_data['pitchbook'])}, CB={len(premium_data['cbinsights'])}, ST={len(premium_data['statista'])}")
        except Exception as e:
            log(f"  WARN: Failed to load pre-enriched data: {e}")

    # If no pre-enriched data, try Cashmere API
    if not any(premium_data.values()) and CASHMERE_API_KEY:
        premium_queries = {
            "pitchbook": [
                (PITCHBOOK_COMPANY, f"{brand_name}", "PitchBook"),
            ],
            "cbinsights": [
                (CBINSIGHTS_RESEARCH, f"{brand_name} DTC brand growth strategy", "CB Insights"),
                (CBINSIGHTS_RESEARCH, f"athletic apparel market trends acquisition valuation", "CB Insights"),
            ],
            "statista": [
                (STATISTA_PREMIUM, f"athletic apparel sportswear market size revenue global", "Statista"),
                (STATISTA_PREMIUM, f"Nike Adidas Lululemon Under Armour revenue sportswear", "Statista"),
                (STATISTA_FREE, f"{brand_name} fitness apparel ecommerce UK", "Statista"),
            ],
        }

        def do_cashmere_call(source_key, collection_id, query, source_name):
            results = _cashmere_search(query, collection_id)
            return source_key, _extract_premium_data(results, source_name)

        with ThreadPoolExecutor(max_workers=6) as executor:
            futures = []
            for source_key, queries in premium_queries.items():
                for coll_id, query, src_name in queries:
                    futures.append(executor.submit(do_cashmere_call, source_key, coll_id, query, src_name))

            for future in as_completed(futures):
                try:
                    source_key, data = future.result()
                    premium_data[source_key].extend(data)
                except Exception as e:
                    log(f"  Premium data error: {e}")

    pb_count = len(premium_data["pitchbook"])
    cb_count = len(premium_data["cbinsights"])
    st_count = len(premium_data["statista"])
    log(f"  Premium data: PitchBook={pb_count}, CB Insights={cb_count}, Statista={st_count}")

    # ── Phase 1b: Perplexity Research (focused on digital/social/reviews) ──
    log("Phase 1b: Perplexity research (digital, social, reviews, supplemental)...")

    pplx_system = f"""You are a senior research analyst conducting PE due diligence. Today's date is {today}.
Answer thoroughly with specific numbers, dates, and facts.
When citing sources, include the ACTUAL URL in parentheses after each claim, like: "Revenue was £646M (https://actual-source.com/article)".
Be precise — include exact figures, currencies, percentages.
Prioritize the MOST RECENT data available — prefer 2025-2026 data over older data.

SOURCE QUALITY RULES:
- For FINANCIAL data: prefer industry news sites (FashionUnited, TheIndustry.fashion, Business of Fashion, FashionNetwork, Bloomberg, Financial Times, Companies House filings) over blogs and marketing sites.
- NEVER cite: 1stformations.co.uk, company formation agents, generic business blogs, or marketing case study blogs as sources for financial figures.
- For SOCIAL MEDIA data: prefer official platform pages, HypeAuditor, SocialBlade, or SimilarWeb.
- For CUSTOMER REVIEWS: always check Trustpilot directly at trustpilot.com/review/[domain].
- For MARKET DATA: prefer Grand View Research, Mordor Intelligence, Statista, or Allied Market Research."""

    # Call 1: Company + Financials (only if no PitchBook data)
    has_pitchbook = pb_count > 0
    
    if has_pitchbook:
        # We have PitchBook — only need supplemental financials
        prompt_core = f"""Research {brand_name} ({domain}) for supplemental financial data not typically in PitchBook:

1. RECENT NEWS: Latest financial results announcements, press releases, or reports for {brand_name} in 2025-2026.
2. GROSS MARGIN & EBITDA: What is {brand_name}'s reported gross margin and EBITDA margin? Search for "{brand_name} gross margin", "{brand_name} EBITDA", "{brand_name} profitability".
3. OPERATIONS: Manufacturing model (own factory vs contract), manufacturing countries, logistics/fulfillment model, key supply chain risks.
4. BUSINESS MODEL DETAILS: Revenue channel split (DTC vs wholesale vs marketplace percentage), geographic revenue breakdown, AOV estimates, repeat purchase rate.

Important: For each fact, include the source URL in parentheses."""
    else:
        # No PitchBook — need full company + financials
        prompt_core = f"""Research {brand_name} ({domain}) thoroughly for PE due diligence. I need VERIFIED data from AUTHORITATIVE sources:

1. COMPANY BASICS: Legal name, founding year, founding CITY (where the company was literally started — this may differ from current HQ), current headquarters city, founders (names and backgrounds), employee count (most recent), business model, product categories.
   - For founding city: search "{brand_name} founded where" or "where was {brand_name} started" — the founding city is where the founders literally started the business, NOT the current HQ.

2. FINANCIALS (CRITICAL — search for the LATEST official results):
   - Search: "{brand_name} revenue FY25", "{brand_name} annual results 2025", "{brand_name} financial results"
   - Search: "{brand_name} Companies House filing" or "{brand_name} annual report"
   - I need: Most recent annual revenue in NATIVE CURRENCY, revenue history for 3+ years, gross margin percentage, EBITDA (amount and margin), profit before tax.
   - Also search: "{brand_name} funding round", "{brand_name} valuation", "{brand_name} investor"
   - IMPORTANT: Use industry news sources (FashionUnited, TheIndustry.fashion, Business of Fashion, Bloomberg) NOT marketing blogs.

3. OPERATIONS: Manufacturing model (own factory vs contract manufacturers), manufacturing countries, logistics/fulfillment model, key supply chain risks.

For each fact, include the source URL in parentheses. Prefer official filings and industry trade publications over blogs."""

    # Call 2: Digital + Social + Reviews (Perplexity is best for this)
    prompt_digital = f"""Research the digital presence and customer reviews of {brand_name} ({domain}). I need CURRENT data as of {today}:

1. WEBSITE TRAFFIC: Monthly visits, traffic trend, top channels (percentages), top countries, mobile percentage, domain authority.
   - Search: "SimilarWeb {brand_name}" or "{domain} traffic"

2. SOCIAL MEDIA (search for CURRENT follower counts from the platforms directly):
   - Instagram: Search "{brand_name} Instagram followers" or check HypeAuditor at hypeauditor.com/instagram/{brand_name}/
   - TikTok: Search "{brand_name} TikTok followers"
   - Facebook, YouTube, Twitter/X: Current followers

3. CUSTOMER REVIEWS (CRITICAL — get exact current numbers):
   - Trustpilot: Go to trustpilot.com/review/{domain} — what is the EXACT star rating (e.g., 3.5 out of 5) and EXACT total review count? This is essential.
   - If you cannot access Trustpilot directly, search: "Trustpilot {brand_name} rating reviews"
   - Google Reviews if available
   - Common praise themes (with example quotes) and complaint themes (with example quotes)

4. TECH STACK: E-commerce platform (Shopify/Magento/custom), analytics tools, email marketing provider.

IMPORTANT: For EACH data point, include the source URL in parentheses. I need TODAY'S numbers, not cached/old data."""

    # Call 3: Competitors + Market (supplemental to Statista/PitchBook)
    prompt_market = f"""Research the competitive landscape for {brand_name} ({domain}) in the {market} market:

1. DIRECT COMPETITORS: Identify 6-8 direct competitors to {brand_name} in the same product category and price segment. For each competitor provide: estimated annual revenue, price range, and key differentiator. Use industry reports and news, not blogs.

2. MARKET SIZE: Total addressable market (TAM), serviceable addressable market (SAM), and serviceable obtainable market (SOM) for the market category {brand_name} operates in. Search for recent market research reports from Grand View Research, Mordor Intelligence, Statista, or Allied Market Research. Include CAGR growth rate.

3. M&A COMPARABLES: Recent acquisitions or PE investments in comparable companies (2020-2026). Include deal values and EV/Revenue multiples. Search: "PE acquisition {market} DTC brand", "private equity investment apparel DTC".

4. INDUSTRY TRENDS: Key trends affecting {brand_name}'s market — DTC evolution, sustainability, AI/personalization, retail expansion, etc.

For each fact, include the source URL in parentheses. Prefer industry research reports and trade publications over blogs."""

    perplexity_findings = {}
    perplexity_citations = {}  # Separate citation namespaces per call

    def do_pplx_call(name, prompt, max_tok):
        log(f"  Perplexity: {name}...")
        content, tagged_cites = _perplexity_call_with_sources(pplx_system, prompt, name, max_tok)
        log(f"  {name}: {len(content)} chars, {len(tagged_cites)} citations")
        return name, content, tagged_cites

    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [
            executor.submit(do_pplx_call, "core", prompt_core, 4000),
            executor.submit(do_pplx_call, "digital", prompt_digital, 4000),
            executor.submit(do_pplx_call, "market", prompt_market, 4000),
        ]
        for future in as_completed(futures):
            try:
                name, content, tagged_cites = future.result()
                perplexity_findings[name] = content
                perplexity_citations[name] = tagged_cites
            except Exception as e:
                log(f"  ERROR in Perplexity call: {e}")

    log(f"  Perplexity done: {list(perplexity_findings.keys())}")

    # ── Phase 1c: GPT Structuring with Source Attribution ──
    log("Phase 1c: Structuring all data via GPT (with explicit source attribution)...")

    # Build premium data summary for GPT
    premium_summary = ""
    source_registry = []  # Master list of all sources

    if premium_data["pitchbook"]:
        premium_summary += "\n\n=== PITCHBOOK DATA (AUTHORITATIVE — use as primary source for company/financial data) ===\n"
        for i, item in enumerate(premium_data["pitchbook"]):
            premium_summary += f"\n[PB-{i+1}] {item['title']}\n{item['content']}\nSource URL: {item['source_url']}\n"
            source_registry.append({
                "id": f"PB-{i+1}",
                "name": f"PitchBook: {item['title']}",
                "url": item["source_url"],
                "publisher": "PitchBook",
                "type": "premium",
            })

    if premium_data["statista"]:
        premium_summary += "\n\n=== STATISTA DATA (AUTHORITATIVE — use for market sizing and industry data) ===\n"
        for i, item in enumerate(premium_data["statista"]):
            premium_summary += f"\n[ST-{i+1}] {item['title']}\n{item['content']}\nSource URL: {item['source_url']}\n"
            source_registry.append({
                "id": f"ST-{i+1}",
                "name": f"Statista: {item['title']}",
                "url": item["source_url"],
                "publisher": "Statista",
                "type": "premium",
            })

    if premium_data["cbinsights"]:
        premium_summary += "\n\n=== CB INSIGHTS DATA (use for market trends, DTC strategy, competitive analysis) ===\n"
        for i, item in enumerate(premium_data["cbinsights"]):
            premium_summary += f"\n[CB-{i+1}] {item['title']}\n{item['content']}\nSource URL: {item['source_url']}\n"
            source_registry.append({
                "id": f"CB-{i+1}",
                "name": f"CB Insights: {item['title']}",
                "url": item["source_url"],
                "publisher": "CB Insights",
                "type": "premium",
            })

    # Build Perplexity findings with SEPARATE citation lists per call
    pplx_summary = ""
    for call_name in ["core", "digital", "market"]:
        if call_name in perplexity_findings:
            pplx_summary += f"\n\n=== PERPLEXITY RESEARCH: {call_name.upper()} ===\n"
            pplx_summary += perplexity_findings[call_name]
            
            cites = perplexity_citations.get(call_name, [])
            if cites:
                pplx_summary += f"\n\nCITATIONS FOR {call_name.upper()} (numbered references in the text above refer ONLY to these URLs):\n"
                for c in cites:
                    ref_id = f"PPLX-{call_name[0].upper()}{c['index']}"
                    pplx_summary += f"  [{c['index']}] = [{ref_id}] {c['url']}\n"
                    source_registry.append({
                        "id": ref_id,
                        "name": f"Web: {c['url'][:60]}",
                        "url": c["url"],
                        "publisher": "Web Source",
                        "type": "web",
                    })

    structure_system = f"""You are a data extraction specialist for PE due diligence research.
Extract structured JSON from multi-source research findings.

CRITICAL RULES:
1. Extract EVERY specific number, date, and fact mentioned.
2. For EVERY data point, set source_id to the reference ID (e.g., "PB-1", "ST-2", "CB-1", "PPLX-D3") that contains that data.
3. For source_url: copy the EXACT URL from the source entry. NEVER fabricate URLs.
4. PRIORITY ORDER for conflicting data: PitchBook > Statista > CB Insights > Perplexity
5. If PitchBook says revenue is X but Perplexity says Y, USE PitchBook's figure.
6. If a data point has no source, set source_id and source_url to null.
7. Prefer the most recent data when multiple years are available.

Return ONLY valid JSON — no markdown, no code fences."""

    structure_prompt = f"""Extract structured data about {brand_name} ({domain}) from these research findings.

{premium_summary}

{pplx_summary}

SOURCE REGISTRY (use source_id and source_url from this list):
{json.dumps(source_registry, indent=2)}

Extract into this JSON structure. EVERY field with a data value MUST have source_id and source_url:
{{
  "company": {{
    "legal_name": "string or null",
    "brand_name": "{brand_name}",
    "domain": "{domain}",
    "founded_year": "year",
    "founded_city": "city",
    "current_headquarters": "city",
    "founders": [{{"name": "...", "title": "...", "background": "..."}}],
    "employee_count": {{"value": number, "date": "YYYY-MM", "source_id": "PB-1", "source_url": "url"}},
    "business_model": "DTC / B2B / etc",
    "product_categories": ["cat1", "cat2"],
    "price_range": "$X-$Y",
    "key_markets": ["market1"],
    "unique_selling_points": ["usp1"],
    "brand_positioning": "description",
    "source_id": "PB-1",
    "source_url": "url"
  }},
  "financials": {{
    "revenue_history": [{{"year": 2024, "revenue": "amount with currency", "growth_yoy": "X%", "source_id": "id", "source_url": "url"}}],
    "latest_revenue": {{"year": 2024, "amount": "amount with currency", "source_id": "id", "source_url": "url"}},
    "gross_margin": {{"value": "X%", "basis": "reported/estimated", "source_id": "id", "source_url": "url"}},
    "ebitda": {{"amount": "£XM", "margin": "X%", "source_id": "id", "source_url": "url"}},
    "funding_rounds": [{{"round": "type", "amount": "$XM", "date": "YYYY-MM", "investors": ["name"], "post_money_valuation": "$XM", "revenue_at_deal": "$XM", "source_id": "id", "source_url": "url"}}],
    "aov_estimate": {{"value": "$X", "source_id": "id", "source_url": "url"}},
    "repeat_purchase_rate": {{"value": "X%", "source_id": "id", "source_url": "url"}},
    "revenue_channels": {{"dtc_pct": "X%", "wholesale_pct": "X%", "source_id": "id", "source_url": "url"}},
    "geographic_revenue": [{{"region": "name", "pct": "X%", "source_id": "id", "source_url": "url"}}]
  }},
  "competitors": {{
    "direct": [
      {{
        "name": "Real Company Name",
        "revenue_est": "amount",
        "price_range": "$X-$Y",
        "differentiator": "description",
        "market_position": "leader/challenger/niche",
        "source_id": "id",
        "source_url": "url"
      }}
    ],
    "market_size": {{"tam": "$XB", "sam": "$XB", "som": "$XM", "growth_rate": "X% CAGR", "source_id": "id", "source_url": "url"}},
    "industry_trends": ["trend1"],
    "ma_comparables": [
      {{"target": "company", "acquirer": "buyer", "year": 2024, "value": "$XM", "ev_revenue": "X.Xx", "source_id": "id", "source_url": "url"}}
    ]
  }},
  "digital_marketing": {{
    "monthly_traffic": {{"value": "XM visits", "source_id": "id", "source_url": "url"}},
    "traffic_trend": "growing/stable/declining",
    "top_channels": [{{"channel": "Organic", "pct": "X%"}}],
    "top_countries": [{{"country": "US", "pct": "X%"}}],
    "social_media": {{
      "instagram": {{"followers": "XM", "source_id": "id", "source_url": "url"}},
      "tiktok": {{"followers": "XM", "source_id": "id", "source_url": "url"}},
      "facebook": {{"followers": "XK"}},
      "youtube": {{"subscribers": "XK"}},
      "twitter": {{"followers": "XK"}}
    }},
    "tech_stack": ["platform1"]
  }},
  "customer_sentiment": {{
    "trustpilot": {{"rating": X.X, "reviews": XXXXX, "source_id": "id", "source_url": "url"}},
    "praise_themes": [{{"theme": "description", "quote": "actual quote"}}],
    "complaint_themes": [{{"theme": "description", "quote": "actual quote"}}]
  }},
  "operations": {{
    "manufacturing": "own factory / contract / mixed",
    "manufacturing_locations": ["country1"],
    "logistics": "3PL / in-house",
    "supply_chain_risks": ["risk1"]
  }}
}}

CRITICAL: 
- Use PitchBook data as the PRIMARY source for revenue, employees, funding, valuation.
- Use Statista for market size and industry data.
- Use Perplexity for digital/social/reviews data.
- Every numeric value MUST have a source_id and source_url. If no source, use null."""

    gpt_response = _gpt_call(structure_system, structure_prompt, 8000)
    research = _parse_json(gpt_response)

    if not research:
        import re
        json_match = re.search(r'\{[\s\S]*\}', gpt_response)
        if json_match:
            try:
                research = json.loads(json_match.group())
            except:
                log("WARN: Could not parse GPT structuring response")
                research = {}
        else:
            log("WARN: No JSON in GPT structuring response")
            research = {}

    # ── Phase 1d: Gap-filling ──
    log("Phase 1d: Checking for critical data gaps...")

    financials = research.get("financials", {})
    latest_rev = financials.get("latest_revenue", {})
    sentiment = research.get("customer_sentiment", {})
    tp = sentiment.get("trustpilot", {})
    digital = research.get("digital_marketing", {})
    social = digital.get("social_media", {})
    ig = social.get("instagram", {})

    gap_queries = []
    if not latest_rev or not latest_rev.get("amount"):
        gap_queries.append(("revenue", f"What is {brand_name} most recent official annual revenue? Search for '{brand_name} revenue 2025', '{brand_name} financial results FY25'. Give the exact figure in native currency."))

    if not tp or not tp.get("rating"):
        gap_queries.append(("trustpilot", f"What is the current Trustpilot rating for {brand_name}? Search for 'Trustpilot {brand_name}'. What star rating (out of 5) and total review count?"))

    if not ig or not ig.get("followers"):
        gap_queries.append(("instagram", f"How many Instagram followers does {brand_name} have currently? Search for '{brand_name} Instagram followers' or 'HypeAuditor {brand_name}'."))

    if gap_queries:
        log(f"  Found {len(gap_queries)} gaps: {[q[0] for q in gap_queries]}. Running targeted searches...")
        
        def do_gap_call(name, prompt):
            log(f"    Gap search: {name}...")
            content, cites = _perplexity_call(pplx_system, prompt, 2000)
            log(f"    {name}: {len(content)} chars, {len(cites)} citations")
            # Tag citations
            tagged = [{"url": url, "call_name": f"gap_{name}"} for url in cites]
            return name, content, tagged

        with ThreadPoolExecutor(max_workers=3) as executor:
            gap_futures = [executor.submit(do_gap_call, n, p) for n, p in gap_queries]
            gap_findings = {}
            gap_cites_all = []
            for future in as_completed(gap_futures):
                try:
                    name, content, tagged = future.result()
                    gap_findings[name] = content
                    gap_cites_all.extend(tagged)
                except Exception as e:
                    log(f"    Gap search error: {e}")

        if gap_findings:
            log("  Merging gap findings...")
            gap_text = "\n\n".join(f"=== {name.upper()} ===\n{text}" for name, text in gap_findings.items())
            gap_cite_urls = "\n".join(f"  GAP-{i+1}: {c['url']}" for i, c in enumerate(gap_cites_all))

            merge_system = """You are a data extraction specialist. Extract specific data from these follow-up research findings.
Return ONLY valid JSON. For source_url: use the URLs listed below. Never fabricate URLs."""

            merge_prompt = f"""Fill data gaps for {brand_name}:

{gap_text}

AVAILABLE SOURCE URLs:
{gap_cite_urls}

Return JSON with ONLY the fields where you found data:
{{
  "latest_revenue": {{"year": 2025, "amount": "£XXM", "source_url": "url_or_null"}},
  "trustpilot": {{"rating": X.X, "reviews": XXXXX, "source_url": "url_or_null"}},
  "instagram": {{"followers": "X.XM", "source_url": "url_or_null"}}
}}"""

            merge_response = _gpt_call(merge_system, merge_prompt, 2000)
            gap_data = _parse_json(merge_response)

            if gap_data:
                log(f"  Gap data found: {list(gap_data.keys())}")
                if gap_data.get("latest_revenue") and gap_data["latest_revenue"].get("amount"):
                    research.setdefault("financials", {})["latest_revenue"] = gap_data["latest_revenue"]
                    log(f"    Updated revenue: {gap_data['latest_revenue']}")
                if gap_data.get("trustpilot") and gap_data["trustpilot"].get("rating"):
                    research.setdefault("customer_sentiment", {})["trustpilot"] = gap_data["trustpilot"]
                    log(f"    Updated Trustpilot: {gap_data['trustpilot']}")
                if gap_data.get("instagram") and gap_data["instagram"].get("followers"):
                    research.setdefault("digital_marketing", {}).setdefault("social_media", {})["instagram"] = gap_data["instagram"]
                    log(f"    Updated Instagram: {gap_data['instagram']}")
    else:
        log("  No critical gaps found.")

    # ── Post-Processing: Source Validation & Staleness Checks ──
    log("Post-processing: Source validation and staleness checks...")
    research = _validate_and_clean_sources(research, brand_name, domain, pplx_system)

    # Attach metadata
    research["_source_registry"] = source_registry
    research["_premium_data"] = {k: len(v) for k, v in premium_data.items()}
    research["_perplexity_calls"] = list(perplexity_findings.keys())

    # Log key metrics
    financials = research.get("financials", {})
    latest_rev = financials.get("latest_revenue", {})
    sentiment = research.get("customer_sentiment", {})
    tp = sentiment.get("trustpilot", {})
    log(f"  Revenue: {latest_rev.get('amount', 'N/A')} ({latest_rev.get('year', 'N/A')})")
    log(f"  Trustpilot: {tp.get('rating', 'N/A')} ({tp.get('reviews', 'N/A')} reviews)")

    return research


# ─── Source Validation & Staleness Checks ───

SOURCE_BLOCKLIST = [
    "1stformations.co.uk",
    "companieshouse.service.gov.uk/company",  # Keep this for actual filings, block only generic pages
    "businessofapps.com",
    "comparably.com",
    "craft.co",
    "dnb.com",
    "growjo.com",
    "incfile.com",
    "indeed.com",
    "owler.com",
    "rocketreach.co",
    "similarweb.com/website",  # SimilarWeb overview pages are OK, but /website/ subpages can be stale
    "startuptalky.com",
    "theceomagazine.com",
    "tracxn.com",
    "wisesheets.io",
    "zoominfo.com",
]


def _is_blocklisted(url):
    """Check if a URL is from a blocklisted source."""
    if not url:
        return False
    url_lower = url.lower()
    return any(blocked in url_lower for blocked in SOURCE_BLOCKLIST)


def _check_revenue_staleness(financials, brand_name, pplx_system):
    """Check if revenue data is stale (>18 months old) and re-search if needed."""
    latest_rev = financials.get("latest_revenue", {})
    if not latest_rev or not latest_rev.get("year"):
        return financials  # No revenue data to check

    rev_year = latest_rev.get("year")
    current_year = datetime.now().year
    current_month = datetime.now().month

    # Consider stale if the revenue year is more than 18 months ago
    # e.g., if it's March 2026 and we have 2023 revenue, that's stale
    months_since_rev = (current_year - rev_year) * 12 + current_month
    
    if months_since_rev > 18:
        log(f"  WARN: Revenue data for year {rev_year} is {months_since_rev} months old (>18) — re-searching for latest...")
        
        try:
            staleness_prompt = f"""URGENT: The revenue data I have for this company is from {rev_year}, which is now {months_since_rev} months old.

Search specifically for the MOST RECENT financial results for {brand_name}:
- Search: "{brand_name} revenue 2025", "{brand_name} annual results 2025", "{brand_name} FY25"
- Search: "{brand_name} revenue 2024", "{brand_name} financial results 2024"
- Search: "{brand_name} Companies House 2024", "{brand_name} Companies House 2025"
- Check: companies.house.gov.uk for {brand_name}

I need:
1. The MOST RECENT annual revenue figure with the year and source URL
2. Has the company had any major financial events (fundraise, acquisition, IPO) since {rev_year}?

For each fact, include the exact source URL in parentheses."""

            content, cites = _perplexity_call(pplx_system, staleness_prompt, 2000)
            
            # Parse the response for updated revenue
            if content and any(year_str in content for year_str in ["2024", "2025", "2026"]):
                log(f"  Staleness check found newer data. Extracting...")
                
                cite_urls = "\n".join(f"  [{i+1}]: {url}" for i, url in enumerate(cites))
                
                extract_prompt = f"""Extract the most recent annual revenue from this text:

{content}

SOURCE URLs:
{cite_urls}

Return JSON only:
{{"year": 2025, "amount": "£XXM", "source_url": "url_or_null"}}"""

                response = _gpt_call("Extract revenue data. Return ONLY valid JSON.", extract_prompt, 500)
                new_rev = _parse_json(response)
                
                if new_rev and new_rev.get("amount") and new_rev.get("year", 0) > rev_year:
                    log(f"  Updated revenue: {new_rev['amount']} ({new_rev['year']}) vs old: {latest_rev.get('amount')} ({rev_year})")
                    financials["latest_revenue"] = new_rev
                    # Also add to history
                    history = financials.get("revenue_history", [])
                    if not any(r.get("year") == new_rev["year"] for r in history):
                        history.append({"year": new_rev["year"], "revenue": new_rev["amount"], "source_url": new_rev.get("source_url")})
                        financials["revenue_history"] = history
                else:
                    log(f"  Staleness re-search did not find newer data.")
        except Exception as e:
            log(f"  WARN: Staleness check failed: {e}")
    
    return financials


def _re_search_trustpilot(brand_name, domain, pplx_system):
    """Dedicated Trustpilot re-search to get current rating."""
    log(f"  Re-searching Trustpilot for {brand_name}...")
    
    try:
        tp_prompt = f"""I need the CURRENT Trustpilot rating for {brand_name} ({domain}).

Please:
1. Go directly to: trustpilot.com/review/{domain}
2. Search: "Trustpilot {brand_name} review"
3. Search: "site:trustpilot.com {brand_name}"

I need:
- Exact star rating out of 5 (e.g., 3.8)
- Exact total number of reviews (e.g., 12,847)
- The direct Trustpilot URL for {brand_name}
- Breakdown of ratings if available (% 5-star, 4-star, etc.)
- Most common praise and complaint themes

Return the source URL for the Trustpilot page."""

        content, cites = _perplexity_call(pplx_system, tp_prompt, 1500)
        
        if content:
            cite_urls = "\n".join(f"  [{i+1}]: {url}" for i, url in enumerate(cites))
            
            extract_prompt = f"""Extract Trustpilot data from this text:

{content}

SOURCE URLs:
{cite_urls}

Return JSON only (null if not found):
{{"rating": X.X, "reviews": XXXXX, "source_url": "trustpilot_url_or_null"}}"""

            response = _gpt_call("Extract Trustpilot rating. Return ONLY valid JSON.", extract_prompt, 300)
            tp_data = _parse_json(response)
            
            if tp_data and tp_data.get("rating") and tp_data.get("reviews"):
                log(f"  Trustpilot: {tp_data['rating']}/5 ({tp_data['reviews']} reviews)")
                return tp_data
    except Exception as e:
        log(f"  WARN: Trustpilot re-search failed: {e}")
    
    return None


def _validate_and_clean_sources(research, brand_name, domain, pplx_system):
    """Validate sources, remove blocklisted ones, check for staleness."""
    
    # 1. Check and clean blocklisted sources from revenue data
    financials = research.get("financials", {})
    latest_rev = financials.get("latest_revenue", {})
    if latest_rev and _is_blocklisted(latest_rev.get("source_url", "")):
        log(f"  WARN: Revenue source is blocklisted: {latest_rev.get('source_url', '')}")
        log(f"  Clearing revenue data and will re-search...")
        financials["latest_revenue"] = {}
        # Trigger a gap-fill by clearing
        research["financials"] = financials

    # 2. Check revenue staleness
    research["financials"] = _check_revenue_staleness(financials, brand_name, pplx_system)

    # 3. Re-search Trustpilot if rating is missing or suspiciously round (might be cached)
    sentiment = research.get("customer_sentiment", {})
    tp = sentiment.get("trustpilot", {})
    needs_tp_research = (
        not tp or 
        not tp.get("rating") or 
        not tp.get("reviews") or
        tp.get("reviews", 0) < 10  # suspiciously low review count
    )
    
    if needs_tp_research:
        log(f"  Trustpilot data missing or suspicious — re-searching...")
        new_tp = _re_search_trustpilot(brand_name, domain, pplx_system)
        if new_tp:
            research.setdefault("customer_sentiment", {})["trustpilot"] = new_tp

    # 4. Clean blocklisted URLs from source registry
    source_registry = research.get("_source_registry", [])
    cleaned_registry = [s for s in source_registry if not _is_blocklisted(s.get("url", ""))]
    if len(cleaned_registry) < len(source_registry):
        removed = len(source_registry) - len(cleaned_registry)
        log(f"  Removed {removed} blocklisted sources from registry")
        research["_source_registry"] = cleaned_registry

    return research


def _parse_json(text):
    """Try to parse JSON from GPT response."""
    if not text:
        return None
    text = text.strip()
    # Remove markdown code blocks if present
    if text.startswith("```"):
        lines = text.split("\n")
        text = "\n".join(lines[1:-1] if lines[-1] == "```" else lines[1:])
    try:
        return json.loads(text)
    except:
        return None


# ─── Phase 2: Report Generation ───

def run_report(research, brand_name, domain):
    """Phase 2: Generate structured report sections via GPT."""
    log("Phase 2: Generating report sections...")

    company = research.get("company", {})
    financials = research.get("financials", {})
    competitors = research.get("competitors", {})
    digital = research.get("digital_marketing", {})
    sentiment = research.get("customer_sentiment", {})
    operations = research.get("operations", {})
    source_registry = research.get("_source_registry", [])

    # Build a flat source map for report generation (id -> url)
    source_map = {s["id"]: s["url"] for s in source_registry if s.get("id") and s.get("url")}
    source_map_str = json.dumps(source_map, indent=2)

    report_system = f"""You are a senior investment analyst at a top-tier PE firm writing a due diligence report.
Today's date: {datetime.now().strftime('%B %d, %Y')}.

WRITING STYLE:
- Write in professional investment banking language
- Be specific: include exact numbers, percentages, currencies, and dates
- Each claim must reference a source using [SOURCE_ID] notation (e.g., [PB-1], [ST-2], [PPLX-D3])
- Source IDs must match the SOURCE MAP provided
- Do not fabricate sources — if no source, write "(source not available)"
- Never use placeholder text like "[X]" or "[Company]"

FORMAT:
- Use HTML with these exact class names for styling
- Wrap sections in <div class="section">
- Use <h2 class="section-title"> for section headers
- Use <p> for paragraphs, <ul>/<li> for bullet points
- For citations, use: <a href="SOURCE_URL" class="citation" target="_blank">[SOURCE_ID]</a>
- SOURCE_URL must come from the SOURCE MAP — look up the id to find the url
- Never output raw [SOURCE_ID] without converting to an <a> tag with the URL"""

    # BATCH 1: Company Overview + Financials
    batch1_prompt = f"""Write the first two sections of a PE due diligence report for {brand_name} ({domain}).

SOURCE MAP (source_id -> url):
{source_map_str}

DATA:
{json.dumps({'company': company, 'financials': financials}, indent=2)}

Write these sections:

1. COMPANY OVERVIEW
Include: founding story (city where founded), current HQ, founders and backgrounds, business model, product categories, price positioning, key markets, brand positioning, employee count, unique selling points.

2. FINANCIAL PERFORMANCE
Include: revenue history table (3+ years), latest revenue with YoY growth, gross margin, EBITDA, funding history with investors and valuations, revenue channel breakdown (DTC vs wholesale), geographic revenue split.

For each specific data point, add a citation link: <a href="SOURCE_URL" class="citation" target="_blank">[SOURCE_ID]</a>
Look up SOURCE_URL from the SOURCE MAP using the source_id."""

    # BATCH 2: Digital + Competitive
    batch2_prompt = f"""Write two more sections of a PE due diligence report for {brand_name} ({domain}).

SOURCE MAP (source_id -> url):
{source_map_str}

DATA:
{json.dumps({'digital_marketing': digital, 'customer_sentiment': sentiment, 'competitors': competitors}, indent=2)}

Write these sections:

3. DIGITAL PRESENCE & MARKETING
Include: website traffic (monthly visits, trend, top channels with percentages), top countries, mobile percentage, social media followers for each platform (Instagram, TikTok, Facebook, YouTube, Twitter), tech stack.

4. CUSTOMER SENTIMENT & REPUTATION
Include: Trustpilot rating and review count (with direct link), praise themes (with example quotes), complaint themes (with example quotes), Google reviews if available.

5. COMPETITIVE LANDSCAPE
Include: 6-8 direct competitors table with revenue, price range, differentiator, market position. Market size (TAM/SAM/SOM) with CAGR. Recent M&A comparables with deal values and EV/Revenue multiples.

For each specific data point, add a citation link using the SOURCE MAP."""

    # BATCH 3: Operations + Investment Thesis
    batch3_prompt = f"""Write the final sections of a PE due diligence report for {brand_name} ({domain}).

SOURCE MAP (source_id -> url):
{source_map_str}

DATA:
{json.dumps({'operations': operations, 'financials': financials, 'competitors': competitors, 'company': company}, indent=2)}

Write these sections:

6. OPERATIONS & SUPPLY CHAIN
Include: manufacturing model (own factory vs contract), manufacturing countries, logistics and fulfillment model, key supply chain risks.

7. INVESTMENT THESIS & KEY RISKS
Include: 
- Bull case (3-4 points): Why this is an attractive acquisition target
- Bear case / key risks (3-4 points): Key risks and challenges
- EV/Revenue valuation range based on M&A comparables and growth profile
- Suggested due diligence priorities

For each specific data point, add a citation link using the SOURCE MAP."""

    report_sections = {}
    
    def do_batch(name, prompt):
        log(f"  Batch {name}...")
        html = _gpt_call(report_system, prompt, 6000)
        log(f"  Batch {name}: {len(html)} chars")
        return name, html

    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [
            executor.submit(do_batch, "1", batch1_prompt),
            executor.submit(do_batch, "2", batch2_prompt),
            executor.submit(do_batch, "3", batch3_prompt),
        ]
        for future in as_completed(futures):
            try:
                name, html = future.result()
                report_sections[name] = html
            except Exception as e:
                log(f"  ERROR in batch: {e}")
                traceback.print_exc()

    return report_sections


# ─── Phase 3: HTML Assembly ───

def assemble_html(report_sections, brand_name, research):
    """Phase 3: Assemble final HTML report."""
    log("Phase 3: Assembling HTML report...")

    company = research.get("company", {})
    financials = research.get("financials", {})
    latest_rev = financials.get("latest_revenue", {})

    # Format header info
    hq = company.get("current_headquarters") or company.get("founded_city") or "Unknown"
    founded = company.get("founded_year", "N/A")
    employees_obj = company.get("employee_count", {})
    employees = employees_obj.get("value", "N/A") if isinstance(employees_obj, dict) else employees_obj
    revenue_str = latest_rev.get("amount", "N/A")
    revenue_year = latest_rev.get("year", "")
    if revenue_year:
        revenue_str = f"{revenue_str} (FY{str(revenue_year)[-2:]})"

    today = datetime.now().strftime("%B %d, %Y")

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>BlazingHill Due Diligence Report: {brand_name}</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{
            font-family: 'Georgia', 'Times New Roman', serif;
            font-size: 14px;
            line-height: 1.7;
            color: #1a1a1a;
            background: #f8f7f4;
        }}
        .cover {{
            background: linear-gradient(135deg, #0a0a0a 0%, #1a1a2e 50%, #16213e 100%);
            color: white;
            padding: 80px 60px;
            min-height: 300px;
            position: relative;
            overflow: hidden;
        }}
        .cover::before {{
            content: '';
            position: absolute;
            top: -50%;
            right: -20%;
            width: 600px;
            height: 600px;
            background: radial-gradient(circle, rgba(255,165,0,0.08) 0%, transparent 70%);
            pointer-events: none;
        }}
        .cover-brand {{
            font-size: 42px;
            font-weight: 700;
            letter-spacing: -1px;
            margin-bottom: 8px;
            font-family: 'Helvetica Neue', Arial, sans-serif;
        }}
        .cover-subtitle {{
            font-size: 16px;
            color: rgba(255,255,255,0.6);
            text-transform: uppercase;
            letter-spacing: 3px;
            margin-bottom: 40px;
        }}
        .cover-meta {{
            display: flex;
            gap: 60px;
            margin-top: 40px;
            border-top: 1px solid rgba(255,255,255,0.1);
            padding-top: 30px;
        }}
        .cover-meta-item {{
            display: flex;
            flex-direction: column;
            gap: 4px;
        }}
        .cover-meta-label {{
            font-size: 10px;
            text-transform: uppercase;
            letter-spacing: 2px;
            color: rgba(255,165,0,0.8);
            font-family: 'Helvetica Neue', Arial, sans-serif;
        }}
        .cover-meta-value {{
            font-size: 16px;
            font-weight: 600;
            font-family: 'Helvetica Neue', Arial, sans-serif;
        }}
        .blazinghill-logo {{
            position: absolute;
            top: 40px;
            right: 60px;
            font-family: 'Helvetica Neue', Arial, sans-serif;
            font-size: 13px;
            font-weight: 700;
            letter-spacing: 2px;
            text-transform: uppercase;
            color: rgba(255,165,0,0.9);
        }}
        .container {{
            max-width: 1100px;
            margin: 0 auto;
            padding: 0 40px 60px;
        }}
        .section {{
            background: white;
            border-radius: 2px;
            margin: 24px 0;
            padding: 40px 48px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.06);
            border-left: 3px solid transparent;
        }}
        .section:hover {{
            border-left-color: #e8a020;
        }}
        .section-title {{
            font-family: 'Helvetica Neue', Arial, sans-serif;
            font-size: 20px;
            font-weight: 700;
            color: #0a0a0a;
            margin-bottom: 24px;
            padding-bottom: 12px;
            border-bottom: 2px solid #f0ede8;
            text-transform: uppercase;
            letter-spacing: 1px;
        }}
        h3 {{
            font-family: 'Helvetica Neue', Arial, sans-serif;
            font-size: 14px;
            font-weight: 700;
            color: #333;
            margin: 20px 0 8px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}
        p {{
            margin: 12px 0;
            color: #2a2a2a;
        }}
        ul, ol {{
            margin: 12px 0 12px 24px;
        }}
        li {{
            margin: 6px 0;
            color: #2a2a2a;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
            font-family: 'Helvetica Neue', Arial, sans-serif;
            font-size: 13px;
        }}
        th {{
            background: #0a0a0a;
            color: white;
            padding: 10px 14px;
            text-align: left;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            font-size: 11px;
        }}
        td {{
            padding: 9px 14px;
            border-bottom: 1px solid #f0ede8;
            vertical-align: top;
        }}
        tr:nth-child(even) td {{
            background: #fafaf8;
        }}
        tr:hover td {{
            background: #fff8ee;
        }}
        .citation {{
            display: inline-block;
            background: #fff8ee;
            border: 1px solid #e8c87a;
            color: #8b6914;
            padding: 0px 5px;
            border-radius: 3px;
            font-size: 10px;
            font-family: 'Helvetica Neue', Arial, sans-serif;
            font-weight: 600;
            text-decoration: none;
            margin: 0 1px;
            vertical-align: middle;
        }}
        .citation:hover {{
            background: #e8c87a;
            color: #000;
        }}
        .metric-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
            gap: 16px;
            margin: 20px 0;
        }}
        .metric-card {{
            background: #fafaf8;
            border: 1px solid #f0ede8;
            border-radius: 4px;
            padding: 16px 20px;
        }}
        .metric-label {{
            font-size: 10px;
            text-transform: uppercase;
            letter-spacing: 1px;
            color: #888;
            font-family: 'Helvetica Neue', Arial, sans-serif;
            margin-bottom: 4px;
        }}
        .metric-value {{
            font-size: 22px;
            font-weight: 700;
            color: #0a0a0a;
            font-family: 'Helvetica Neue', Arial, sans-serif;
        }}
        .tag {{
            display: inline-block;
            background: #f0ede8;
            color: #555;
            padding: 3px 10px;
            border-radius: 20px;
            font-size: 11px;
            font-family: 'Helvetica Neue', Arial, sans-serif;
            margin: 2px;
        }}
        .highlight {{
            background: linear-gradient(135deg, #fff8ee, #fff3d6);
            border: 1px solid #e8c87a;
            border-radius: 4px;
            padding: 16px 20px;
            margin: 16px 0;
        }}
        .footer {{
            text-align: center;
            padding: 40px;
            color: #999;
            font-size: 11px;
            font-family: 'Helvetica Neue', Arial, sans-serif;
            border-top: 1px solid #f0ede8;
            margin-top: 40px;
        }}
        @media print {{
            body {{ background: white; }}
            .section {{
                box-shadow: none;
                border: 1px solid #eee;
                break-inside: avoid;
            }}
        }}
    </style>
</head>
<body>
<div class="cover">
    <div class="blazinghill-logo">BlazingHill Capital</div>
    <div class="cover-brand">{brand_name}</div>
    <div class="cover-subtitle">Private Equity Due Diligence Report</div>
    <div class="cover-meta">
        <div class="cover-meta-item">
            <span class="cover-meta-label">Report Date</span>
            <span class="cover-meta-value">{today}</span>
        </div>
        <div class="cover-meta-item">
            <span class="cover-meta-label">Headquarters</span>
            <span class="cover-meta-value">{hq}</span>
        </div>
        <div class="cover-meta-item">
            <span class="cover-meta-label">Founded</span>
            <span class="cover-meta-value">{founded}</span>
        </div>
        <div class="cover-meta-item">
            <span class="cover-meta-label">Employees</span>
            <span class="cover-meta-value">{employees:,} if isinstance(employees, int) else {employees}</span>
        </div>
        <div class="cover-meta-item">
            <span class="cover-meta-label">Revenue</span>
            <span class="cover-meta-value">{revenue_str}</span>
        </div>
    </div>
</div>
<div class="container">
"""

    # Add sections in order
    for i in ["1", "2", "3"]:
        if i in report_sections:
            html += report_sections[i]

    html += f"""
</div>
<div class="footer">
    <strong>BlazingHill Capital</strong> — Confidential Due Diligence Report — {today}<br>
    This report was prepared using proprietary data sources including PitchBook, CB Insights, Statista, and AI-powered research.
    All figures should be independently verified prior to making investment decisions.
</div>
</body>
</html>"""

    return html


# ─── Main Entry Point ───

def main(brand_name, domain, market, report_id=None, output_dir="."):
    """Full pipeline: research → report → HTML."""
    log(f"BlazingHill Report Engine v3 — Starting for: {brand_name} ({domain})")
    log(f"Market: {market}")

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    report_id = report_id or f"{brand_name.lower().replace(' ', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    # Phase 1: Research
    t0 = time.time()
    research = run_research(brand_name, domain, market)
    t1 = time.time()
    log(f"Phase 1 complete in {t1-t0:.1f}s")

    # Save research JSON
    research_path = output_dir / f"{report_id}_research.json"
    with open(research_path, "w") as f:
        json.dump(research, f, indent=2)
    log(f"Research saved: {research_path}")

    # Phase 2: Report generation
    report_sections = run_report(research, brand_name, domain)
    t2 = time.time()
    log(f"Phase 2 complete in {t2-t1:.1f}s")

    # Phase 3: HTML assembly
    html = assemble_html(report_sections, brand_name, research)
    t3 = time.time()
    log(f"Phase 3 complete in {t3-t2:.1f}s")

    # Save HTML
    html_path = output_dir / f"{report_id}_report.html"
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)
    log(f"Report saved: {html_path}")
    log(f"Total time: {t3-t0:.1f}s")

    return str(html_path), str(research_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="BlazingHill Report Engine v3")
    parser.add_argument("brand_name", help="Brand name (e.g., 'Gym Shark')")
    parser.add_argument("domain", help="Domain (e.g., 'gymshark.com')")
    parser.add_argument("market", help="Market category (e.g., 'athletic apparel DTC')")
    parser.add_argument("--report-id", help="Optional report ID")
    parser.add_argument("--output-dir", default=".", help="Output directory")
    args = parser.parse_args()
    main(args.brand_name, args.domain, args.market, args.report_id, args.output_dir)
