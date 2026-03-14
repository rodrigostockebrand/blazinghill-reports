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
    log(f"  Structured research complete. Revenue: {latest_rev.get('amount', 'N/A')}")

    return research


def _parse_json(text):
    """Parse JSON from GPT response, handling common issues."""
    if not text:
        return None
    text = text.strip()
    # Remove markdown code fences
    if text.startswith("```"):
        lines = text.split("\n")
        # Find the actual JSON start
        start = 1
        if lines[0].startswith("```json") or lines[0].startswith("```JSON"):
            start = 1
        # Find end
        end = len(lines)
        for i, line in enumerate(lines):
            if i > 0 and line.strip() == "```":
                end = i
                break
        text = "\n".join(lines[start:end])
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        # Try to find JSON object in the text
        import re
        match = re.search(r'\{[\s\S]*\}', text)
        if match:
            try:
                return json.loads(match.group())
            except:
                return None
        return None


# ─── Source Validation & Trustpilot ───

# ─── Trustpilot Gap-Fill Helper ──────────────────────────────────────────────

def _fetch_trustpilot_data(brand_name, domain, pplx_system):
    """
    Dedicated Trustpilot lookup. Always runs; returns a dict with keys:
      rating (float), reviews (int), praise_themes (list), complaint_themes (list),
      source_url (str)
    Returns None if nothing useful found.
    """
    log(f"  [Trustpilot] Dedicated lookup for {brand_name} ({domain})...")

    tp_query_1 = (
        f'site:trustpilot.com/review/{domain} rating reviews "{brand_name}"'
    )
    tp_query_2 = (
        f'Trustpilot {brand_name} star rating total reviews customer feedback 2025'
    )
    tp_query_3 = (
        f'"{brand_name}" trustpilot.com review score out of 5'
    )

    trustpilot_url = f"https://www.trustpilot.com/review/{domain}"

    tp_system = (
        f"You are a research assistant. Today's date is {__import__('datetime').datetime.now().strftime('%B %d, %Y')}. "
        "Find the Trustpilot rating for the requested brand. "
        "Return the EXACT star rating (e.g. 4.2 out of 5) and EXACT total review count. "
        "Also list the top 3 praise themes and top 3 complaint themes from reviews. "
        "Include the direct URL to the Trustpilot review page."
    )

    tp_user = f"""Search Trustpilot for {brand_name}.

Primary URL to check: {trustpilot_url}

Search queries to use:
1. {tp_query_1}
2. {tp_query_2}
3. {tp_query_3}

Report:
- Exact star rating (X.X / 5.0)
- Total review count (exact integer)
- Top 3 praise themes (with example customer quote if available)
- Top 3 complaint themes (with example customer quote if available)
- Source URL (should be trustpilot.com/review/{domain})

If you cannot find a Trustpilot page for {brand_name}, say so explicitly."""

    try:
        content, cites = _perplexity_call(tp_system, tp_user, max_tokens=2000)
    except Exception as e:
        log(f"  [Trustpilot] Perplexity call failed: {e}")
        return None

    # Ask GPT to extract structured data from the Perplexity response
    extract_system = (
        "You are a data extraction specialist. "
        "Extract Trustpilot data from the research text below. "
        "Return ONLY valid JSON — no markdown, no code fences. "
        "If a value is not found, set it to null."
    )

    # Build citation reference
    cite_text = ""
    tp_source_url = trustpilot_url
    for i, url in enumerate(cites):
        cite_text += f"  [{i+1}] {url}\n"
        if "trustpilot.com" in url:
            tp_source_url = url

    extract_user = f"""Extract Trustpilot data for {brand_name} from this research:

{content}

Available source URLs:
{cite_text if cite_text else '  (none)'}

Return JSON:
{{
  "rating": 4.2,
  "reviews": 12500,
  "praise_themes": [
    {{"theme": "Fast delivery", "quote": "example quote or null"}},
    {{"theme": "Great quality", "quote": "example quote or null"}},
    {{"theme": "Easy returns", "quote": "example quote or null"}}
  ],
  "complaint_themes": [
    {{"theme": "Sizing issues", "quote": "example quote or null"}},
    {{"theme": "Slow customer service", "quote": "example quote or null"}},
    {{"theme": "Delivery delays", "quote": "example quote or null"}}
  ],
  "source_url": "{tp_source_url}"
}}

Rules:
- rating must be a float (e.g. 4.2), not a string
- reviews must be an integer, not a string
- If Trustpilot page not found, set rating and reviews to null"""

    try:
        gpt_resp = _gpt_call(extract_system, extract_user, max_tokens=800)
        result = _parse_json(gpt_resp)
    except Exception as e:
        log(f"  [Trustpilot] GPT extraction failed: {e}")
        return None

    if not result:
        log("  [Trustpilot] Could not parse extraction result.")
        return None

    if result.get("rating") is None and result.get("reviews") is None:
        log("  [Trustpilot] No data found on Trustpilot.")
        return None

    log(
        f"  [Trustpilot] Found: {result.get('rating')} stars, "
        f"{result.get('reviews')} reviews"
    )
    return result


# ─── Revenue Staleness Check ───────────────────────────────────────────────

def _check_revenue_staleness(research, brand_name, domain, pplx_system):
    """
    Checks whether the latest_revenue year in research is stale (> 1 year old).
    If stale, runs a Perplexity search for the current year's revenue and overrides.
    Returns updated research dict.
    """
    financials = research.get("financials", {})
    latest_rev = financials.get("latest_revenue", {})

    if not latest_rev or not latest_rev.get("year"):
        log("  [Staleness] No revenue year to check — skipping staleness fix.")
        return research

    current_year = datetime.now().year
    rev_year = latest_rev.get("year")

    # Normalize: could be int or string like "FY2024" or "2024"
    try:
        if isinstance(rev_year, str):
            import re as _re
            match = _re.search(r"(\d{4})", rev_year)
            rev_year_int = int(match.group(1)) if match else None
        else:
            rev_year_int = int(rev_year)
    except (TypeError, ValueError):
        rev_year_int = None

    if rev_year_int is None:
        log(f"  [Staleness] Could not parse revenue year: {rev_year!r} — skipping.")
        return research

    age_years = current_year - rev_year_int
    if age_years <= 1:
        log(
            f"  [Staleness] Revenue year {rev_year_int} is fresh enough "
            f"(age: {age_years}y) — no override needed."
        )
        return research

    # Revenue is stale — search for latest
    log(
        f"  [Staleness] Revenue year {rev_year_int} is stale (age: {age_years}y). "
        f"Searching for FY{current_year} / FY{current_year - 1} data..."
    )

    staleness_user = (
        f'"{brand_name}" revenue {current_year} OR "{brand_name}" annual results {current_year} '
        f'OR "{brand_name}" FY{current_year} revenue '
        f'OR "{brand_name}" financial results {current_year - 1} {current_year}'
    )

    try:
        content, cites = _perplexity_call(pplx_system, staleness_user, max_tokens=2000)
    except Exception as e:
        log(f"  [Staleness] Perplexity search failed: {e}")
        return research

    # Build source map from citations
    cite_lines = ""
    best_url = None
    for i, url in enumerate(cites):
        cite_lines += f"  [{i+1}] {url}\n"
        if best_url is None and any(
            domain_hint in url
            for domain_hint in [
                "fashionunited", "theindustry", "businessoffashion", "bloomberg",
                "ft.com", "reuters", "companieshouse", "fashionnetwork",
            ]
        ):
            best_url = url

    extract_sys = (
        "You are a data extraction specialist for PE due diligence. "
        "Extract the most recent annual revenue figure for the brand from the research text. "
        "Return ONLY valid JSON — no markdown, no code fences."
    )

    extract_user = f"""From this research about {brand_name}, extract the most recent annual revenue:

{content}

Source URLs:
{cite_lines if cite_lines else '  (none returned)'}

Return JSON:
{{
  "year": 2025,
  "amount": "£646M",
  "currency": "GBP",
  "source_url": "https://actual-url.com/article",
  "source_name": "FashionUnited",
  "confidence": "high/medium/low"
}}

Rules:
- year must be the fiscal year integer (e.g. 2025)
- amount must include the currency symbol and unit (M, B, K) — e.g. "£646M" or "$1.2B"
- source_url must be a real URL from the list above; if none available, use null
- If you cannot find clear recent revenue data, return {{"year": null, "amount": null}}"""

    try:
        gpt_resp = _gpt_call(extract_sys, extract_user, max_tokens=600)
        new_rev = _parse_json(gpt_resp)
    except Exception as e:
        log(f"  [Staleness] GPT extraction failed: {e}")
        return research

    if not new_rev or not new_rev.get("amount"):
        log("  [Staleness] Could not extract updated revenue from search results.")
        return research

    new_year = new_rev.get("year")
    new_amount = new_rev.get("amount")
    new_url = new_rev.get("source_url") or best_url

    # Only override if the new data is actually newer
    if new_year and new_year > rev_year_int:
        log(
            f"  [Staleness] Overriding revenue: {latest_rev.get('amount')} ({rev_year_int}) "
            f"→ {new_amount} ({new_year})"
        )
        research["financials"]["latest_revenue"] = {
            "year": new_year,
            "amount": new_amount,
            "currency": new_rev.get("currency"),
            "source_url": new_url,
            "source_id": "PPLX-staleness",
            "source_name": new_rev.get("source_name", "Web Research"),
            "source_note": (
                f"Updated from web research — PitchBook data was stale "
                f"(was FY{rev_year_int}: {latest_rev.get('amount')})"
            ),
        }

        # Also append to revenue_history so the chart shows both data points
        rev_history = research["financials"].get("revenue_history", [])
        # Avoid duplicating
        existing_years = {r.get("year") for r in rev_history}
        if new_year not in existing_years:
            rev_history.append({
                "year": new_year,
                "revenue": new_amount,
                "growth_yoy": None,
                "source_id": "PPLX-staleness",
                "source_url": new_url,
                "source_note": "Updated from web research",
            })
            research["financials"]["revenue_history"] = sorted(
                rev_history, key=lambda r: r.get("year") or 0
            )
    else:
        log(
            f"  [Staleness] New data (year={new_year}) is not newer than "
            f"existing (year={rev_year_int}) — keeping original."
        )

    return research


# ─── Main Validate + Clean Function ─────────────────────────────────────────────

def _validate_and_clean_sources(research, brand_name, domain, pplx_system):
    """
    Validate and clean sources in the research data.

    Steps:
      1. Flag/clean low-quality source URLs (bad-source list).
      2. Revenue staleness check — if latest_revenue.year > 1 year old, override.
      3. Dedicated Trustpilot lookup — always run regardless of existing data.
    """

    # ── Step 1: Flag low-quality sources ─────────────────────────────────────
    BAD_SOURCE_PATTERNS = [
        "1stformations.co.uk",
        "companieshouse.gov.uk/search",   # search pages, not actual filings
        "linkedin.com/company",           # often has wrong financials
        "crunchbase.com",                 # often stale or user-submitted
        "growjo.com",
        "zoominfo.com",
        "dnb.com",
        "owler.com",
        "craft.co",
        "macrotrends.net",
        "wisesheets.io",
        "simplywall.st",
    ]

    def is_bad_source(url):
        if not url:
            return False
        for pattern in BAD_SOURCE_PATTERNS:
            if pattern in url:
                return True
        return False

    def clean_financial_sources(data_dict):
        """Recursively flag financial data with bad sources."""
        if not isinstance(data_dict, dict):
            return data_dict
        source_url = data_dict.get("source_url", "")
        if is_bad_source(source_url):
            data_dict["source_quality"] = "low"
            data_dict["source_warning"] = f"Low-quality source: {source_url}"
        for key, value in data_dict.items():
            if isinstance(value, dict):
                data_dict[key] = clean_financial_sources(value)
            elif isinstance(value, list):
                data_dict[key] = [
                    clean_financial_sources(item) if isinstance(item, dict) else item
                    for item in value
                ]
        return data_dict

    if "financials" in research:
        research["financials"] = clean_financial_sources(research["financials"])

    # ── Step 2: Revenue staleness check ────────────────────────────────────
    research = _check_revenue_staleness(research, brand_name, domain, pplx_system)

    # ── Step 3: Dedicated Trustpilot lookup ───────────────────────────────────
    # Always run — Trustpilot is never in PitchBook/Statista/CB Insights.
    tp_data = _fetch_trustpilot_data(brand_name, domain, pplx_system)
    if tp_data:
        existing_tp = research.get("customer_sentiment", {}).get("trustpilot", {})
        # Always override with the dedicated lookup (it's more authoritative)
        research.setdefault("customer_sentiment", {})["trustpilot"] = tp_data
        if existing_tp and existing_tp.get("rating") and existing_tp.get("rating") != tp_data.get("rating"):
            log(
                f"  [Trustpilot] Overrode existing data "
                f"(was {existing_tp.get('rating')} → now {tp_data.get('rating')})"
            )
        # Also merge praise/complaint themes if GPT-structured data had them
        if existing_tp.get("praise_themes") and not tp_data.get("praise_themes"):
            research["customer_sentiment"]["trustpilot"]["praise_themes"] = (
                existing_tp["praise_themes"]
            )
        if existing_tp.get("complaint_themes") and not tp_data.get("complaint_themes"):
            research["customer_sentiment"]["trustpilot"]["complaint_themes"] = (
                existing_tp["complaint_themes"]
            )
    else:
        log("  [Trustpilot] No Trustpilot data found — leaving existing data intact.")

    return research


# ─── Phase 2: Report Generation (51 Sections) ───

# ─── Section Definitions ─────────────────────────────────────────────────────

SECTIONS = [
    ("s01", "01", "Executive Summary"),
    ("s02", "02", "Company Profile"),
    ("s03", "03", "PE Economics"),
    ("s04", "04", "Digital Marketing"),
    ("s05", "05", "Competitive Intelligence"),
    ("s06", "06", "AI & Innovation"),
    ("s07", "07", "Risk Assessment"),
    ("s08", "08", "Channel Economics"),
    ("s09", "09", "Cohort Analysis"),
    ("s10", "10", "TAM/SAM/SOM"),
    ("s11", "11", "Customer Sentiment"),
    ("s12", "12", "Content Strategy Gap"),
    ("s13", "13", "Value Creation"),
    ("s14", "14", "Pricing Strategy"),
    ("s15", "15", "Revenue Quality"),
    ("s16", "16", "Management & Org"),
    ("s17", "17", "Technology Stack"),
    ("s18", "18", "Brand Equity"),
    ("s19", "19", "Supply Chain"),
    ("s20", "20", "Regulatory"),
    ("s21", "21", "Working Capital"),
    ("s22", "22", "Exit Analysis"),
    ("s23", "23", "Geo Expansion"),
    ("s24", "24", "LTV Model"),
    ("s25", "25", "CAC Payback"),
    ("s26", "26", "Contribution Margin"),
    ("s27", "27", "Marketing P&L"),
    ("s28", "28", "RFM Segmentation"),
    ("s29", "29", "Retention Curves"),
    ("s30", "30", "AOV Dynamics"),
    ("s31", "31", "NPS & VoC"),
    ("s32", "32", "Journey Mapping"),
    ("s33", "33", "SEO Authority"),
    ("s34", "34", "Paid Media"),
    ("s35", "35", "Email & CRM"),
    ("s36", "36", "CRO Analysis"),
    ("s37", "37", "Social Commerce"),
    ("s38", "38", "Share of Voice"),
    ("s39", "39", "Price Elasticity"),
    ("s40", "40", "Disruption Threats"),
    ("s41", "41", "Cross-Border"),
    ("s42", "42", "IP & Trademark"),
    ("s43", "43", "Data Assets"),
    ("s44", "44", "Content Library"),
    ("s45", "45", "MarTech Stack"),
    ("s46", "46", "100-Day Plan"),
    ("s47", "47", "EBITDA Bridge"),
    ("s48", "48", "Scenario Analysis"),
    ("s49", "49", "IC Summary"),
    ("s50", "50", "AI Readiness"),
    ("s51", "51", "Appendix"),
]

# Batch definitions: list of section ids per batch
BATCHES = [
    ["s01", "s02", "s03", "s04", "s05"],   # Batch 1
    ["s06", "s07", "s08", "s09", "s10"],   # Batch 2
    ["s11", "s12", "s13", "s14", "s15"],   # Batch 3
    ["s16", "s17", "s18", "s19", "s20"],   # Batch 4
    ["s21", "s22", "s23", "s24", "s25"],   # Batch 5
    ["s26", "s27", "s28", "s29", "s30"],   # Batch 6
    ["s31", "s32", "s33", "s34", "s35"],   # Batch 7
    ["s36", "s37", "s38", "s39", "s40"],   # Batch 8
    ["s41", "s42", "s43", "s44", "s45"],   # Batch 9
    ["s46", "s47", "s48", "s49", "s50", "s51"],  # Batch 10
]
