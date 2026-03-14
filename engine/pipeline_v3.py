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


def _validate_and_clean_sources(research, brand_name, domain, pplx_system):
    """Validate and clean sources in the research data."""
    
    # List of known bad/low-quality source domains to flag
    BAD_SOURCE_PATTERNS = [
        "1stformations.co.uk",
        "companieshouse.gov.uk/search",  # search pages, not actual filings
        "linkedin.com/company",  # LinkedIn company pages often have wrong financials
        "crunchbase.com",  # Often stale or user-submitted data
        "growjo.com",
        "zoominfo.com",
        "dnb.com",
        "owler.com",
        "craft.co",
    ]
    
    def is_bad_source(url):
        if not url:
            return False
        for pattern in BAD_SOURCE_PATTERNS:
            if pattern in url:
                return True
        return False
    
    def clean_financial_sources(data_dict):
        """Recursively clean financial data with bad sources."""
        if not isinstance(data_dict, dict):
            return data_dict
        
        # If this dict has a source_url that's bad, clear the value
        source_url = data_dict.get("source_url", "")
        if is_bad_source(source_url):
            # Flag it but don't delete — let report generation handle it
            data_dict["source_quality"] = "low"
            data_dict["source_warning"] = f"Low-quality source: {source_url}"
        
        # Recurse into nested dicts
        for key, value in data_dict.items():
            if isinstance(value, dict):
                data_dict[key] = clean_financial_sources(value)
            elif isinstance(value, list):
                data_dict[key] = [clean_financial_sources(item) if isinstance(item, dict) else item for item in value]
        
        return data_dict
    
    # Clean financial data
    if "financials" in research:
        research["financials"] = clean_financial_sources(research["financials"])
    
    return research


# ─── Phase 2: Report Generation ───

def run_report_generation(research, brand_name, domain, report_id):
    """Phase 2: Generate report sections via GPT using structured research data."""
    log("Phase 2: Generating report sections...")

    # Build a flat source registry string for GPT
    source_registry = research.get("_source_registry", [])
    
    source_map = "AVAILABLE SOURCES (use these exact IDs and URLs when citing):\n"
    for src in source_registry:
        source_map += f"  [{src['id']}] {src['name']} — {src.get('url', 'no url')}\n"

    company = research.get("company", {})
    financials = research.get("financials", {})
    competitors = research.get("competitors", {})
    digital = research.get("digital_marketing", {})
    sentiment = research.get("customer_sentiment", {})
    operations = research.get("operations", {})

    # Revenue history for GPT context
    rev_history = financials.get("revenue_history", [])
    rev_summary = ""
    for r in rev_history:
        rev_summary += f"  {r.get('year', '?')}: {r.get('revenue', '?')} (growth: {r.get('growth_yoy', '?')}) [Source: {r.get('source_id', '?')}]\n"

    # ── Batch 1: Company Overview + Financials ──
    log("  Batch 1: Company overview + financials...")

    batch1_system = """You are a senior PE analyst writing a structured due diligence report.
Write in professional, factual prose. Be specific with numbers, dates, and sources.

CITATION FORMAT: After every factual claim, add a source tag: [Source: SOURCE_ID]
Example: "Revenue grew 23% to £45M in FY2024 [Source: PB-1]"

IMPORTANT RULES:
- Only cite sources from the PROVIDED SOURCE REGISTRY.
- If a claim comes from PitchBook (PB-X), cite it as [Source: PB-X].
- If from Statista (ST-X), cite as [Source: ST-X].
- If from Perplexity web research (PPLX-XX), cite as [Source: PPLX-XX].
- If you don't have a source for a claim, write "[Source: estimated]" or omit the claim.
- NEVER fabricate source IDs or URLs."""

    batch1_prompt = f"""Write Section 1 (Company Overview) and Section 2 (Financial Analysis) for {brand_name} ({domain}).

{source_map}

COMPANY DATA:
{json.dumps(company, indent=2)}

FINANCIAL DATA:
{json.dumps(financials, indent=2)}

REVENUE HISTORY:
{rev_summary}

Write these sections:

## 1. Company Overview
Include: Founded (year, city), headquarters, founders + backgrounds, business model, product categories, price positioning, key markets, brand positioning statement, employee count.

## 2. Financial Analysis  
Include: Revenue trend (3+ years with % growth), latest revenue figure, gross margin, EBITDA, funding history with investors + valuations, revenue channel breakdown (DTC vs wholesale), geographic breakdown, AOV, repeat purchase rate.

Format as professional HTML sections with <h2>, <p>, <ul> tags. Use <span class="source-tag">[Source: ID]</span> after each factual claim."""

    batch1_html = _gpt_call(batch1_system, batch1_prompt, 4000)

    # ── Batch 2: Digital + Customer Sentiment ──
    log("  Batch 2: Digital presence + customer sentiment...")

    batch2_prompt = f"""Write Section 3 (Digital & Marketing) and Section 4 (Customer Sentiment) for {brand_name} ({domain}).

{source_map}

DIGITAL MARKETING DATA:
{json.dumps(digital, indent=2)}

CUSTOMER SENTIMENT DATA:
{json.dumps(sentiment, indent=2)}

Write these sections:

## 3. Digital & Marketing Presence
Include: Monthly website traffic (with trend), traffic channels breakdown, top geographic markets, social media following (all platforms with exact numbers), tech stack, marketing strategy observations.

## 4. Customer Sentiment Analysis
Include: Trustpilot rating (exact score + review count), key praise themes (2-3 with example quotes), key complaint themes (2-3 with example quotes), overall sentiment assessment.

Format as professional HTML with <h2>, <p>, <ul> tags. Use <span class="source-tag">[Source: ID]</span> after each factual claim."""

    batch2_html = _gpt_call(batch1_system, batch2_prompt, 4000)

    # ── Batch 3: Competitive + Operations + Investment Thesis ──
    log("  Batch 3: Competitive landscape + operations + investment thesis...")

    batch3_prompt = f"""Write Section 5 (Competitive Landscape), Section 6 (Operations), and Section 7 (Investment Thesis & Risks) for {brand_name} ({domain}).

{source_map}

COMPETITOR DATA:
{json.dumps(competitors, indent=2)}

OPERATIONS DATA:
{json.dumps(operations, indent=2)}

COMPANY + FINANCIAL SUMMARY:
- Brand: {brand_name}
- Latest Revenue: {financials.get('latest_revenue', {}).get('amount', 'N/A')}
- Growth Rate: {financials.get('revenue_history', [{}])[-1].get('growth_yoy', 'N/A') if financials.get('revenue_history') else 'N/A'}
- Gross Margin: {financials.get('gross_margin', {}).get('value', 'N/A')}
- Business Model: {company.get('business_model', 'N/A')}
- Key Markets: {company.get('key_markets', [])}

Write these sections:

## 5. Competitive Landscape
Include: Market size (TAM/SAM/SOM with CAGR), direct competitors table (name, revenue, price range, differentiator), M&A comparables (recent deals with EV/Revenue multiples), {brand_name}'s competitive position.

## 6. Operations & Supply Chain
Include: Manufacturing model, manufacturing countries, logistics/fulfillment, supply chain risks, operational scalability assessment.

## 7. Investment Thesis & Risk Assessment
Include:
- Bull case (3-4 key investment positives with supporting data)
- Bear case / key risks (3-4 risks with specific data)
- Valuation framework (EV/Revenue range based on comps)
- Key value creation levers
- Recommended next steps for DD

Format as professional HTML with <h2>, <p>, <ul>, <table> tags where appropriate. Use <span class="source-tag">[Source: ID]</span> after each factual claim."""

    batch3_html = _gpt_call(batch1_system, batch3_prompt, 6000)

    return batch1_html, batch2_html, batch3_html


# ─── Phase 3: HTML Assembly ───

def assemble_html(brand_name, domain, batch1, batch2, batch3, research, report_id):
    """Phase 3: Assemble the final HTML report."""
    log("Phase 3: Assembling HTML report...")

    company = research.get("company", {})
    financials = research.get("financials", {})
    source_registry = research.get("_source_registry", [])
    premium_data = research.get("_premium_data", {})

    latest_rev = financials.get("latest_revenue", {})
    rev_amount = latest_rev.get("amount", "N/A")
    rev_year = latest_rev.get("year", "")
    rev_source = latest_rev.get("source_url", "")
    gross_margin = financials.get("gross_margin", {}).get("value", "N/A")
    ebitda_margin = financials.get("ebitda", {}).get("margin", "N/A")
    
    employee_count = company.get("employee_count", {})
    if isinstance(employee_count, dict):
        emp_val = employee_count.get("value", "N/A")
    else:
        emp_val = employee_count or "N/A"

    # Build source references section
    source_refs_html = ""
    if source_registry:
        source_refs_html = "<div class='source-references'>\n<h3>Sources & References</h3>\n<ol>\n"
        for src in source_registry:
            url = src.get("url", "")
            name = src.get("name", src.get("id", "Unknown"))
            publisher = src.get("publisher", "")
            if url:
                source_refs_html += f'  <li id="ref-{src["id"]}"><strong>[{src["id"]}]</strong> {name} ({publisher}). <a href="{url}" target="_blank">{url}</a></li>\n'
            else:
                source_refs_html += f'  <li id="ref-{src["id"]}"><strong>[{src["id"]}]</strong> {name} ({publisher}).</li>\n'
        source_refs_html += "</ol>\n</div>"

    # Premium data badge
    premium_badges = []
    if premium_data.get("pitchbook", 0) > 0:
        premium_badges.append(f'<span class="badge badge-pitchbook">PitchBook ({premium_data["pitchbook"]} results)</span>')
    if premium_data.get("statista", 0) > 0:
        premium_badges.append(f'<span class="badge badge-statista">Statista ({premium_data["statista"]} results)</span>')
    if premium_data.get("cbinsights", 0) > 0:
        premium_badges.append(f'<span class="badge badge-cb">CB Insights ({premium_data["cbinsights"]} results)</span>')
    premium_badge_html = " ".join(premium_badges) if premium_badges else '<span class="badge badge-web">Web Research</span>'

    # Replace source tags in batch HTML
    def linkify_sources(html_text, registry):
        """Convert [Source: ID] tags to hyperlinks."""
        import re
        source_map = {src["id"]: src for src in registry}
        
        def replace_tag(match):
            src_id = match.group(1).strip()
            if src_id in source_map:
                src = source_map[src_id]
                url = src.get("url", "")
                if url:
                    return f'<a href="{url}" target="_blank" class="source-link" title="{src["name"]}">[{src_id}]</a>'
                else:
                    return f'<span class="source-tag">[{src_id}]</span>'
            elif src_id == "estimated":
                return '<span class="source-tag estimated">[estimated]</span>'
            else:
                return f'<span class="source-tag">[{src_id}]</span>'
        
        # Match patterns like [Source: PB-1] or <span class="source-tag">[Source: PB-1]</span>
        html_text = re.sub(r'<span class="source-tag">\[Source:\s*([^\]]+)\]</span>', replace_tag, html_text)
        html_text = re.sub(r'\[Source:\s*([^\]]+)\]', replace_tag, html_text)
        return html_text
    
    batch1 = linkify_sources(batch1, source_registry)
    batch2 = linkify_sources(batch2, source_registry)
    batch3 = linkify_sources(batch3, source_registry)

    today = datetime.now().strftime("%B %d, %Y")
    founded_year = company.get("founded_year", "")
    hq = company.get("current_headquarters", "")
    business_model = company.get("business_model", "DTC")

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{brand_name} — PE Due Diligence Report</title>
  <style>
    :root {{
      --primary: #1a2540;
      --accent: #2d6af6;
      --accent-light: #e8efff;
      --success: #16a34a;
      --warning: #d97706;
      --danger: #dc2626;
      --text: #1e293b;
      --text-muted: #64748b;
      --border: #e2e8f0;
      --bg: #f8fafc;
      --card-bg: #ffffff;
      --pitchbook: #e84b23;
      --statista: #2d7df6;
      --cbinsights: #7c3aed;
    }}
    
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    
    body {{
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
      background: var(--bg);
      color: var(--text);
      font-size: 14px;
      line-height: 1.6;
    }}
    
    /* ── Header ── */
    .report-header {{
      background: var(--primary);
      color: white;
      padding: 32px 48px;
      display: flex;
      justify-content: space-between;
      align-items: flex-start;
    }}
    
    .header-left h1 {{ font-size: 28px; font-weight: 700; margin-bottom: 4px; }}
    .header-left .subtitle {{ color: #94a3b8; font-size: 14px; }}
    .header-right {{ text-align: right; }}
    .header-right .date {{ color: #94a3b8; font-size: 13px; }}
    .header-right .report-id {{ color: #64748b; font-size: 11px; margin-top: 4px; }}
    
    /* ── KPI Strip ── */
    .kpi-strip {{
      background: white;
      border-bottom: 1px solid var(--border);
      padding: 16px 48px;
      display: flex;
      gap: 32px;
      overflow-x: auto;
    }}
    
    .kpi-item {{ min-width: 120px; }}
    .kpi-label {{ font-size: 11px; color: var(--text-muted); text-transform: uppercase; letter-spacing: 0.5px; }}
    .kpi-value {{ font-size: 20px; font-weight: 700; color: var(--primary); margin-top: 2px; }}
    .kpi-sub {{ font-size: 11px; color: var(--text-muted); }}
    
    /* ── Layout ── */
    .report-body {{
      max-width: 1200px;
      margin: 0 auto;
      padding: 32px 48px;
    }}
    
    /* ── Sections ── */
    .section {{
      background: var(--card-bg);
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 28px 32px;
      margin-bottom: 24px;
    }}
    
    .section h2 {{
      font-size: 18px;
      font-weight: 700;
      color: var(--primary);
      margin-bottom: 16px;
      padding-bottom: 12px;
      border-bottom: 2px solid var(--accent-light);
    }}
    
    .section h3 {{
      font-size: 15px;
      font-weight: 600;
      color: var(--primary);
      margin: 16px 0 8px;
    }}
    
    .section p {{ margin-bottom: 12px; }}
    
    .section ul, .section ol {{
      padding-left: 20px;
      margin-bottom: 12px;
    }}
    
    .section li {{ margin-bottom: 4px; }}
    
    /* ── Tables ── */
    table {{
      width: 100%;
      border-collapse: collapse;
      margin: 16px 0;
      font-size: 13px;
    }}
    
    th {{
      background: var(--accent-light);
      color: var(--primary);
      font-weight: 600;
      text-align: left;
      padding: 10px 12px;
      border: 1px solid var(--border);
    }}
    
    td {{
      padding: 8px 12px;
      border: 1px solid var(--border);
      vertical-align: top;
    }}
    
    tr:nth-child(even) td {{ background: #f8fafc; }}
    
    /* ── Source Tags ── */
    .source-tag, .source-link {{
      font-size: 10px;
      color: var(--accent);
      background: var(--accent-light);
      padding: 1px 4px;
      border-radius: 3px;
      text-decoration: none;
      white-space: nowrap;
    }}
    
    .source-tag.estimated {{
      color: var(--text-muted);
      background: #f1f5f9;
    }}
    
    a.source-link:hover {{ background: #c7d9ff; }}
    
    /* ── Badges ── */
    .badge {{
      display: inline-block;
      padding: 3px 8px;
      border-radius: 4px;
      font-size: 11px;
      font-weight: 600;
      margin-right: 6px;
    }}
    
    .badge-pitchbook {{ background: #fde8e0; color: var(--pitchbook); }}
    .badge-statista {{ background: #e0edff; color: var(--statista); }}
    .badge-cb {{ background: #ede9ff; color: var(--cbinsights); }}
    .badge-web {{ background: #f1f5f9; color: var(--text-muted); }}
    
    /* ── Premium data banner ── */
    .premium-banner {{
      background: linear-gradient(135deg, #1a2540, #2d4080);
      color: white;
      padding: 10px 48px;
      font-size: 12px;
      display: flex;
      align-items: center;
      gap: 12px;
    }}
    
    .premium-banner .label {{ color: #94a3b8; }}
    
    /* ── Source References ── */
    .source-references {{
      margin-top: 8px;
      font-size: 12px;
    }}
    
    .source-references h3 {{
      font-size: 14px;
      color: var(--text-muted);
      margin-bottom: 12px;
    }}
    
    .source-references ol {{
      padding-left: 20px;
    }}
    
    .source-references li {{
      margin-bottom: 6px;
      color: var(--text-muted);
      word-break: break-all;
    }}
    
    .source-references a {{
      color: var(--accent);
      text-decoration: none;
    }}
    
    /* ── Footer ── */
    .report-footer {{
      background: var(--primary);
      color: #64748b;
      padding: 16px 48px;
      font-size: 12px;
      text-align: center;
    }}
    
    /* ── Print styles ── */
    @media print {{
      body {{ background: white; }}
      .report-header, .kpi-strip, .premium-banner {{ print-color-adjust: exact; }}
      .section {{ break-inside: avoid; border: 1px solid #ddd; }}
    }}
  </style>
</head>
<body>

  <!-- Header -->
  <div class="report-header">
    <div class="header-left">
      <h1>{brand_name}</h1>
      <div class="subtitle">PE Due Diligence Report · {business_model} · {hq}</div>
    </div>
    <div class="header-right">
      <div class="date">{today}</div>
      <div class="report-id">Report ID: {report_id}</div>
    </div>
  </div>

  <!-- Premium Data Banner -->
  <div class="premium-banner">
    <span class="label">Data Sources:</span>
    {premium_badge_html}
    <span class="label">+ Perplexity AI Web Research</span>
  </div>

  <!-- KPI Strip -->
  <div class="kpi-strip">
    <div class="kpi-item">
      <div class="kpi-label">Latest Revenue</div>
      <div class="kpi-value">{rev_amount}</div>
      <div class="kpi-sub">{rev_year}</div>
    </div>
    <div class="kpi-item">
      <div class="kpi-label">Gross Margin</div>
      <div class="kpi-value">{gross_margin}</div>
      <div class="kpi-sub">reported</div>
    </div>
    <div class="kpi-item">
      <div class="kpi-label">EBITDA Margin</div>
      <div class="kpi-value">{ebitda_margin}</div>
      <div class="kpi-sub">reported</div>
    </div>
    <div class="kpi-item">
      <div class="kpi-label">Employees</div>
      <div class="kpi-value">{emp_val}</div>
      <div class="kpi-sub">est.</div>
    </div>
    <div class="kpi-item">
      <div class="kpi-label">Founded</div>
      <div class="kpi-value">{founded_year}</div>
      <div class="kpi-sub">{hq}</div>
    </div>
  </div>

  <!-- Report Body -->
  <div class="report-body">

    <!-- Batch 1: Company + Financials -->
    <div class="section">
      {batch1}
    </div>

    <!-- Batch 2: Digital + Sentiment -->
    <div class="section">
      {batch2}
    </div>

    <!-- Batch 3: Competitive + Operations + Thesis -->
    <div class="section">
      {batch3}
    </div>

    <!-- Source References -->
    <div class="section">
      {source_refs_html}
    </div>

  </div>

  <!-- Footer -->
  <div class="report-footer">
    BlazingHill Research · Confidential · Generated {today} · {report_id}
  </div>

</body>
</html>"""

    return html


# ─── Main Entry Point ───

def main(brand_name, domain, market, report_id, output_dir):
    """Main pipeline: research → report generation → HTML assembly."""
    log(f"Starting BlazingHill Report Engine v3")
    log(f"Brand: {brand_name} | Domain: {domain} | Market: {market}")
    log(f"Report ID: {report_id} | Output: {output_dir}")

    start_time = time.time()

    Path(output_dir).mkdir(parents=True, exist_ok=True)

    # ── Phase 1: Research ──
    try:
        research = run_research(brand_name, domain, market)
    except Exception as e:
        log(f"ERROR in research phase: {e}")
        traceback.print_exc()
        sys.exit(1)

    # Save research JSON
    research_path = Path(output_dir) / "research.json"
    with open(research_path, "w") as f:
        json.dump(research, f, indent=2)
    log(f"Research saved to {research_path}")

    # ── Phase 2: Report Generation ──
    try:
        batch1, batch2, batch3 = run_report_generation(research, brand_name, domain, report_id)
    except Exception as e:
        log(f"ERROR in report generation: {e}")
        traceback.print_exc()
        sys.exit(1)

    # ── Phase 3: HTML Assembly ──
    try:
        html = assemble_html(brand_name, domain, batch1, batch2, batch3, research, report_id)
    except Exception as e:
        log(f"ERROR in HTML assembly: {e}")
        traceback.print_exc()
        sys.exit(1)

    # Save the HTML report
    html_path = Path(output_dir) / "index.html"
    with open(html_path, "w") as f:
        f.write(html)
    log(f"HTML report saved to {html_path}")

    elapsed = time.time() - start_time
    log(f"Done! Report generated in {elapsed:.1f}s")
    log(f"Output: {html_path}")

    return str(html_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="BlazingHill Report Engine v3")
    parser.add_argument("--brand", required=True, help="Brand name")
    parser.add_argument("--domain", required=True, help="Brand domain")
    parser.add_argument("--market", required=True, help="Market category")
    parser.add_argument("--report-id", required=True, help="Report ID")
    parser.add_argument("--output-dir", required=True, help="Output directory")
    args = parser.parse_args()
    main(args.brand, args.domain, args.market, args.report_id, args.output_dir)
