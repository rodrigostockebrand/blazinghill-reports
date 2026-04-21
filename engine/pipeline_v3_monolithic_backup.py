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

# ─── Trustpilot Gap-Fill Helper ────────────────────────────────────────────────

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


# ─── Revenue Staleness Check ───────────────────────────────────────────────────

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


# ─── Main Validate + Clean Function ───────────────────────────────────────────

def _validate_and_clean_sources(research, brand_name, domain, pplx_system):
    """
    Validate and clean sources in the research data.

    Steps:
      1. Flag/clean low-quality source URLs (bad-source list).
      2. Revenue staleness check — if latest_revenue.year > 1 year old, override.
      3. Dedicated Trustpilot lookup — always run regardless of existing data.
    """

    # ── Step 1: Flag low-quality sources ──────────────────────────────────────
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

    # ── Step 2: Revenue staleness check ───────────────────────────────────────
    research = _check_revenue_staleness(research, brand_name, domain, pplx_system)

    # ── Step 3: Dedicated Trustpilot lookup ────────────────────────────────────
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

# ─── Section Definitions ──────────────────────────────────────────────────────

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


def _build_section_prompts(batch_section_ids):
    """Build per-section instructions for a batch."""
    section_map = {sid: (num, title) for sid, num, title in SECTIONS}
    prompts = []
    for sid in batch_section_ids:
        num, title = section_map[sid]
        prompts.append(_get_section_instructions(sid, num, title))
    return "\n\n".join(prompts)


def _get_section_instructions(sid, num, title):
    """Return detailed writing instructions for a single section."""
    instructions = {
        "s01": """Write a 500-700 word executive summary. Include:
- 4-6 KPI cards showing revenue, growth rate, gross margin, EBITDA, employees, founded year
- Investment thesis in 3 bullet points (bull case)
- 3 key risks in callout.warn boxes
- Recommended entry valuation range with EV/Revenue multiple
- A Chart.js bar chart: revenue history (last 4 years)""",

        "s02": """Write Company Profile section. Include:
- Legal name, founding year, founding city, HQ, founders with backgrounds
- Business model description, product categories table, price range
- stat-rows for: employees, founded year, HQ, domain, business model
- Brand positioning statement in a thesis-box
- A Chart.js doughnut chart: revenue channel split (DTC vs wholesale vs marketplace)""",

        "s03": """Write PE Economics section. Include:
- Ownership structure (founders/PE/management split) in a stat-row table
- Capital efficiency metrics: revenue per employee, revenue CAGR, LTV/CAC ratio
- Funding history table (round, date, amount, investors, post-money valuation)
- EV/Revenue comp set table (4-6 comparable transactions)
- Entry valuation analysis: base case, bear, bull scenarios with MOIC projections
- A Chart.js bar chart: EV/Revenue multiples comparison vs comps
- A scenarios section with .scenario-card.bear/.base/.bull showing MOIC and IRR""",

        "s04": """Write Digital Marketing section. Include:
- Monthly traffic with trend in a stat-row
- Traffic channel breakdown table (organic, paid, social, direct, referral percentages)
- Top 5 countries by traffic
- Social media following table (platform, followers, engagement rate)
- Tech stack tags using .tag elements
- A Chart.js bar chart: traffic channel breakdown
- A Chart.js doughnut: social media follower distribution""",

        "s05": """Write Competitive Intelligence section. Include:
- Market position narrative
- Competitors table (name, revenue, price range, differentiator, Trustpilot rating, social followers)
- Competitive moat analysis: 4-6 moat items rated strong/medium/weak
- M&A comparables table (target, acquirer, year, deal value, EV/Revenue)
- A Chart.js radar chart: competitive positioning (5 dimensions: Price, Quality, Brand, Digital, Growth)""",

        "s06": """Write AI & Innovation section. Include:
- Current AI/tech investments (personalization, recommendation engine, etc.)
- Innovation pipeline opportunities
- AI readiness score with metric bars
- Comparison vs competitors on AI adoption
- A Chart.js bar chart: AI adoption maturity vs top 5 competitors""",

        "s07": """Write Risk Assessment section. Include:
- Risk matrix table: each risk with category, severity (High/Med/Low), likelihood, mitigation
- Top 3 risks in .callout.danger boxes
- Key watch items in .callout.warn boxes
- A Chart.js bubble chart: risk severity vs likelihood (bubble size = impact)""",

        "s08": """Write Channel Economics section. Include:
- Revenue by channel: DTC, wholesale, marketplace with margins per channel
- stat-rows: gross margin per channel, contribution margin, CAC per channel
- Channel mix trend over 3 years
- A Chart.js grouped bar chart: revenue and margin by channel""",

        "s09": """Write Cohort Analysis section. Include:
- Customer cohort retention table (12, 24, 36 month retention rates)
- Payback period analysis
- LTV cohort curves discussion
- Repeat purchase rate stat-row
- A Chart.js line chart: cohort retention curves (month 1-24)""",

        "s10": """Write TAM/SAM/SOM section. Include:
- TAM/SAM/SOM with sources and growth rates
- Market sizing methodology note
- Key market trends driving growth (3-5 trends)
- Geographic opportunity map table
- A Chart.js doughnut chart: TAM/SAM/SOM nested visualization
- A Chart.js line chart: market size projection (current year + 5 years)""",

        "s11": """Write Customer Sentiment section. Include:
- Trustpilot score prominently (large KPI card), total reviews, year-over-year trend
- Top praise themes (3) with example customer quotes in blockquotes
- Top complaint themes (3) with example customer quotes in blockquotes
- Net Promoter Score estimate
- Sentiment vs competitors comparison
- A Chart.js bar chart: star rating distribution (1-5 stars)
- A Chart.js doughnut: sentiment split (positive/neutral/negative)""",

        "s12": """Write Content Strategy Gap section. Include:
- Current content output assessment (blog, video, UGC, influencer)
- SEO content gap analysis: top missing keyword clusters
- Competitor content benchmarking
- Recommendations in callout.success boxes
- A Chart.js radar chart: content maturity across 6 dimensions""",

        "s13": """Write Value Creation section. Include:
- 100-day quick wins table (initiative, owner, impact, effort)
- 12-month priorities with revenue impact estimates
- 3-year transformation roadmap milestones
- EBITDA bridge from entry to exit
- A Chart.js bar chart: value creation waterfall (base EBITDA → levers → exit EBITDA)""",

        "s14": """Write Pricing Strategy section. Include:
- Current price architecture table (product category, price range, positioning)
- Price vs competitor comparison
- Price elasticity discussion
- Pricing power indicators
- Recommended pricing actions in callout.success
- A Chart.js scatter chart: price vs quality positioning vs competitors""",

        "s15": """Write Revenue Quality section. Include:
- Revenue concentration analysis (top 10 customers / geographic concentration)
- Recurring vs one-time revenue split
- Revenue predictability score
- Seasonality patterns (monthly/quarterly distribution)
- A Chart.js line chart: monthly revenue seasonality index (12 months)""",

        "s16": """Write Management & Org section. Include:
- Leadership team table (name, role, tenure, background, equity)
- Org structure description
- Key person risk assessment
- Management depth and succession planning
- Hiring gaps and recommendations
- A Chart.js doughnut: org split by department headcount""",

        "s17": """Write Technology Stack section. Include:
- Current tech stack diagram (list with categories: ecommerce, analytics, CRM, logistics, etc.)
- Technical debt assessment
- Scalability rating per system
- Integration complexity
- Build vs buy recommendations
- A Chart.js radar chart: tech stack maturity (6 dimensions: ecommerce, analytics, CRM, logistics, personalization, security)""",

        "s18": """Write Brand Equity section. Include:
- Brand awareness metrics (aided/unaided if available)
- Brand search volume trend
- Brand sentiment score
- Share of voice in social media
- Brand vs private label premium
- A Chart.js line chart: brand search volume trend (last 24 months, indexed to 100)""",

        "s19": """Write Supply Chain section. Include:
- Manufacturing model (own/contract/mixed)
- Manufacturing locations map (table: country, % of production, lead time, risk level)
- Supplier concentration risk
- Logistics/3PL model and costs
- Supply chain resilience score
- A Chart.js doughnut: manufacturing geography split""",

        "s20": """Write Regulatory section. Include:
- Key regulatory frameworks applicable (GDPR, CCPA, product safety, sustainability)
- Compliance status per framework (stat-row with green/amber/red tags)
- Upcoming regulatory risks (2025-2027)
- ESG commitments and progress
- Regulatory tail risks in callout.warn
- A Chart.js bar chart: compliance maturity by regulatory area""",

        "s21": """Write Working Capital section. Include:
- Key working capital metrics: DSO, DIO, DPO, CCC
- Working capital as % of revenue
- Inventory analysis (turnover, days, risk)
- Cash conversion cycle discussion
- Improvement opportunities in callout.success
- A Chart.js bar chart: working capital components and CCC trend""",

        "s22": """Write Exit Analysis section. Include:
- Exit route options: strategic trade sale, sponsor-to-sponsor, IPO
- Comparable exit valuations (EV/EBITDA and EV/Revenue)
- Buyer universe: strategic acquirers, PE sponsors, potential IPO candidates
- Timeline to exit (3-5 year horizon)
- Valuation upside scenarios
- A Chart.js bar chart: exit valuation bridge (entry → exit by scenario)""",

        "s23": """Write Geo Expansion section. Include:
- Current geographic revenue split (table with countries/regions)
- Expansion priority ranking (market size, competition, logistics feasibility)
- US market opportunity deep-dive
- EU/APAC opportunities
- Recommended expansion sequence
- A Chart.js bar chart: revenue by geography with expansion potential overlay""",

        "s24": """Write LTV Model section. Include:
- LTV calculation methodology
- Average order value, purchase frequency, gross margin, churn
- LTV by customer segment
- LTV trend over time
- LTV benchmarking vs competitors
- A Chart.js line chart: cumulative LTV curve over 36 months""",

        "s25": """Write CAC Payback section. Include:
- Blended CAC by channel (table: channel, CAC, LTV, LTV/CAC ratio, payback months)
- CAC trend over time
- Efficiency improvements available
- Payback period by channel
- A Chart.js grouped bar chart: CAC vs 12-month revenue per customer by channel""",

        "s26": """Write Contribution Margin section. Include:
- Contribution margin P&L: revenue, COGS, gross margin, variable marketing, net contribution
- Contribution margin by channel
- Contribution margin by product category
- Threshold analysis (break-even new customer CAC)
- A Chart.js waterfall bar chart: from gross revenue to net contribution""",

        "s27": """Write Marketing P&L section. Include:
- Full marketing P&L (spend by channel, revenue attributed, ROAS, net contribution)
- Marketing efficiency ratio trend
- Budget allocation vs recommended allocation
- Media mix modeling notes
- A Chart.js grouped bar chart: marketing spend vs revenue attributed by channel""",

        "s28": """Write RFM Segmentation section. Include:
- RFM model description (Recency, Frequency, Monetary)
- Customer segments table (Champions, Loyal, At-Risk, Lost, New — with size and revenue %)
- Segment-specific action recommendations
- Revenue concentration by RFM tier
- A Chart.js doughnut: customer revenue by RFM segment""",

        "s29": """Write Retention Curves section. Include:
- 30/60/90/180/365 day retention rates
- Cohort retention heatmap description
- Churn analysis by channel, product, geography
- Win-back campaign effectiveness
- Retention benchmark vs industry
- A Chart.js line chart: retention rate at 1,3,6,12,18,24 months""",

        "s30": """Write AOV Dynamics section. Include:
- AOV trend (monthly or quarterly for last 8 quarters)
- AOV by channel (DTC vs wholesale vs marketplace)
- AOV by product category
- Cross-sell and upsell attachment rates
- Price increase vs unit volume analysis
- A Chart.js line chart: AOV trend over last 8 quarters""",

        "s31": """Write NPS & VoC section. Include:
- NPS score (if available) with benchmarks
- Voice of Customer themes from reviews (praise and complaints)
- CSAT score if available
- Customer feedback loop description
- Improvement recommendations
- A Chart.js bar chart: top customer feedback themes (positive and negative)""",

        "s32": """Write Journey Mapping section. Include:
- Customer journey stages (Awareness → Consideration → Purchase → Retention → Advocacy)
- Key touchpoints per stage
- Friction points identified (with callout.warn)
- Optimization opportunities per stage
- A Chart.js line chart: conversion funnel (awareness → purchase, drop-off %)""",

        "s33": """Write SEO Authority section. Include:
- Domain authority / domain rating
- Organic traffic estimate and trend
- Top organic keyword categories
- Backlink profile (total backlinks, referring domains, top backlinks)
- SEO vs top 3 competitors comparison
- Keyword ranking distribution
- A Chart.js bar chart: organic vs paid traffic split comparison vs competitors""",

        "s34": """Write Paid Media section. Include:
- Estimated paid media spend (Google, Meta, TikTok, etc.)
- ROAS by channel
- Paid vs organic traffic ratio
- Ad creative strategy assessment
- Paid media efficiency benchmarks
- A Chart.js doughnut: paid media budget allocation by channel""",

        "s35": """Write Email & CRM section. Include:
- Email list size estimate
- Key email metrics: open rate, click rate, unsubscribe rate vs benchmarks
- CRM platform and capabilities
- Automation and segmentation maturity
- Email revenue contribution estimate
- A Chart.js bar chart: email performance vs industry benchmarks""",

        "s36": """Write CRO Analysis section. Include:
- Site conversion rate estimate vs benchmarks
- Checkout funnel drop-off analysis
- Mobile vs desktop conversion comparison
- A/B testing cadence assessment
- Top 5 CRO opportunities ranked by impact
- A Chart.js bar chart: conversion funnel steps with drop-off rates""",

        "s37": """Write Social Commerce section. Include:
- Instagram Shop / TikTok Shop presence
- Social commerce revenue estimate
- Influencer marketing program assessment
- UGC (user-generated content) volume and quality
- Social commerce growth opportunity
- A Chart.js doughnut: social media revenue split by platform""",

        "s38": """Write Share of Voice section. Include:
- Share of voice in organic search vs top 5 competitors
- Social media share of voice
- Branded vs non-branded search share
- PR / earned media volume
- A Chart.js bar chart: share of voice comparison (brand vs top 4 competitors)""",

        "s39": """Write Price Elasticity section. Include:
- Price elasticity estimate discussion
- Historical price change impacts (if data available)
- Elasticity by product category
- Pricing power indicators (brand loyalty, switching costs)
- Recommended pricing strategy
- A Chart.js line chart: price index vs volume index (last 8 quarters)""",

        "s40": """Write Disruption Threats section. Include:
- Top 3 disruption scenarios with probability and impact
- New entrants and challenger brands to watch
- Technology disruption risks (AI, social commerce, D2C platforms)
- Business model disruption risks
- Mitigation strategies in callout.success
- A Chart.js bubble chart: disruption scenarios (probability vs impact, bubble = urgency)""",

        "s41": """Write Cross-Border section. Include:
- Current international revenue % and countries
- Cross-border ecommerce opportunities (US, EU, APAC)
- Localization requirements (language, payment, logistics)
- Regulatory barriers per market
- Cross-border expansion investment estimate
- A Chart.js bar chart: cross-border market opportunity sizing by region""",

        "s42": """Write IP & Trademark section. Include:
- Trademark registrations (jurisdictions covered, key marks)
- Patent portfolio (if applicable)
- Brand protection risks (counterfeiting, grey market)
- IP valuation estimate
- Recommended IP protections
- A Chart.js doughnut: trademark coverage by key market""",

        "s43": """Write Data Assets section. Include:
- Customer data asset description (size, depth, consent status)
- First-party data strategy
- Data monetization potential
- Data infrastructure and quality
- GDPR/CCPA compliance status
- A Chart.js bar chart: data asset maturity scores by dimension""",

        "s44": """Write Content Library section. Include:
- Content asset inventory (photos, videos, blog posts, lookbooks)
- Content production cadence and cost
- Content performance metrics
- Content gaps vs competitors
- Content repurposing opportunities
- A Chart.js doughnut: content mix by type""",

        "s45": """Write MarTech Stack section. Include:
- Full MarTech stack inventory (email, analytics, CRM, CDP, attribution, etc.)
- Stack cost estimate
- Integration quality assessment
- Stack gaps vs best-in-class
- Consolidation / migration opportunities
- A Chart.js radar chart: MarTech capability maturity (6 dimensions)""",

        "s46": """Write 100-Day Plan section. Include:
- Days 1-30: Quick wins table (action, owner, KPI, cost)
- Days 31-60: Foundation builds table
- Days 61-100: Growth initiatives table
- KPI dashboard targets at Day 100
- A Chart.js gantt-style bar chart: 100-day initiative timeline""",

        "s47": """Write EBITDA Bridge section. Include:
- Entry EBITDA (current)
- Revenue growth contribution
- Gross margin expansion contribution
- OpEx efficiency contribution
- New channel contributions
- Exit EBITDA
- Detailed waterfall table for each bridge item
- A Chart.js waterfall bar chart: EBITDA bridge from entry to exit""",

        "s48": """Write Scenario Analysis section. Include:
- Bear / Base / Bull case assumptions table
- Revenue projections for each scenario (3-5 years)
- EBITDA margin progression
- Exit valuation range
- Key scenario drivers and sensitivities
- Three .scenario-card.bear/.base/.bull with MOIC and IRR
- A Chart.js line chart: revenue projection by scenario (5 years)""",

        "s49": """Write IC Summary section. Include:
- Investment thesis in 3 bullet points
- Deal structure recommendation (equity, debt, governance)
- Key conditions precedent
- Recommended advisors for next-phase DD
- Go/No-Go recommendation with rationale
- Key open questions for management
- A final KPI grid with 8 key metrics""",

        "s50": """Write AI Readiness section. Include:
- AI readiness score (overall and by dimension)
- Current AI/ML capabilities
- AI opportunity map (personalization, forecasting, content, customer service)
- Build vs buy vs partner recommendation per AI use case
- Investment required to reach AI-ready state
- A Chart.js radar chart: AI readiness across 8 dimensions""",

        "s51": """Write Appendix section. Include:
- Data sources summary table (source name, type, date accessed, URL)
- Methodology notes
- Glossary of key terms
- Key assumptions
- Disclaimer text
- A sources table with all referenced URLs""",
    }

    default = f"Write the {title} section with relevant analysis, stat-rows, callouts, and one Chart.js chart."
    return f"""--- SECTION {sid.upper()} ({num}): {title} ---
{instructions.get(sid, default)}

HTML structure required:
<section class="section" id="{sid}">
  <div class="section-label">Section {num}</div>
  <h2>{title}</h2>
  <p class="section-intro">Brief intro paragraph...</p>
  <!-- prose content using .stat-row, .callout, .kpi-grid, .table-wrap, .two-col, .scenarios -->
  <div class="chart-container" data-chart='VALID_CHART_JS_4X_JSON'></div>
  <figcaption>Exhibit {num}: [chart title] | Sources: <a href="URL" target="_blank">Source Name</a></figcaption>
</section>"""


# ─── Main Function ─────────────────────────────────────────────────────────────

def run_report_generation(research, brand_name, domain, report_id):
    """
    Phase 2: Generate all 51 report sections via GPT in 10 parallel batches.
    Returns a list of HTML strings (one per batch) in order.
    """
    log("Phase 2: Generating 51-section report (10 GPT batches, parallel)...")

    # ── Build source registry string ──────────────────────────────────────────
    source_registry = research.get("_source_registry", [])
    source_map_lines = ["AVAILABLE SOURCES (cite using these exact IDs and URLs):"]
    for src in source_registry:
        source_map_lines.append(
            f"  [{src['id']}] {src['name']} — {src.get('url', 'no url')}"
        )
    source_map = "\n".join(source_map_lines)

    # ── Build research context JSON (compact) ─────────────────────────────────
    company = research.get("company", {})
    financials = research.get("financials", {})
    competitors = research.get("competitors", {})
    digital = research.get("digital_marketing", {})
    sentiment = research.get("customer_sentiment", {})
    operations = research.get("operations", {})

    research_context = json.dumps(
        {
            "company": company,
            "financials": financials,
            "competitors": competitors,
            "digital_marketing": digital,
            "customer_sentiment": sentiment,
            "operations": operations,
        },
        indent=2,
    )

    # ── GPT system prompt ─────────────────────────────────────────────────────
    gpt_system = f"""You are a McKinsey-grade PE due diligence analyst writing an institutional-quality report.
The target company is {brand_name} ({domain}).

CITATION FORMAT: Every factual claim MUST include a source link. Use this format:
<a href="ACTUAL_URL" target="_blank" class="cite">Source Name</a>

If you only have a source ID (e.g. PB-1, ST-2, PPLX-D3), look up the URL in the AVAILABLE SOURCES list below and use that URL.
Never write [Source: ID] — always use a real <a href> tag.
If you have no URL for a claim, write: <span class="no-source">(source not available)</span>

AVAILABLE SOURCES:
{source_map}

CHART FORMAT: For every chart, output a div with the Chart.js 4.x config as a JSON attribute:
<div class="chart-container" data-chart='VALID_JSON_CONFIG'></div>

Rules for chart JSON:
- Must be valid JSON (double-quote all keys and string values, use only numeric values for data)
- Chart types allowed: bar, line, doughnut, radar, scatter, bubble, polarArea
- Always include: type, data (labels + datasets), options (responsive:true, plugins.legend)
- Color palette: ["#2563eb","#16a34a","#d97706","#dc2626","#7c3aed","#06b6d4","#ec4899"]
- Background colors should be hex with alpha: e.g. "#2563eb40" for 25% opacity
- For waterfall/bridge charts use bar type with stacked:true
- NEVER use JavaScript expressions inside the JSON — only literal values

HTML FORMAT: Use these CSS classes:
- .stat-row with children: .stat-label, .stat-value, .stat-note
- .callout.success or .callout.warn or .callout.info or .callout.danger with .callout-icon span
- .two-col for side-by-side layouts
- .scenario-card.bear, .scenario-card.base, .scenario-card.bull (inside .scenarios div)
- .table-wrap > table with thead/tbody
- .tag.tag-opp (green), .tag.tag-watch (amber), .tag.tag-risk (red)
- .kpi-grid with .kpi-card.kpi-blue/.kpi-green/.kpi-amber/.kpi-red/.kpi-navy
- .metric-bar with .mb-label, .mb-track, .mb-fill
- .key-insight for highlighted analysis
- .thesis-box for investment thesis statements

CRITICAL RULES:
1. Every numeric claim MUST have a real source <a> tag.
2. If you don't have a specific data point, write: <em>Data not available from current sources.</em>
3. Never fabricate revenue, employee, or valuation figures.
4. Never fabricate URLs — if the URL is unknown, omit the href and note "(source not available)".
5. Use the EXACT section IDs provided (s01-s51).
6. Always include at least one <div class="chart-container" data-chart='...'> per section.
7. Chart JSON must be single-line-valid inside the data-chart attribute (escape any single quotes as &#39;).
8. Write substantive analysis — minimum 400 words per section.
9. Include actual numbers from the research context where available.
10. Produce clean HTML only — no markdown, no code fences, no explanations outside the HTML."""

    # ── Per-batch generation function ─────────────────────────────────────────
    def generate_batch(batch_num, section_ids):
        batch_label = f"Batch {batch_num} ({', '.join(section_ids)})"
        log(f"  [Gen] Starting {batch_label}...")

        section_prompts = _build_section_prompts(section_ids)

        batch_user = f"""Write the following sections of the {brand_name} PE Due Diligence Report.

RESEARCH DATA (use this as your primary data source):
{research_context}

SECTIONS TO WRITE:
{section_prompts}

IMPORTANT:
- Write ALL sections listed above, in order.
- Each section must start with <section class="section" id="sXX"> and end with </section>.
- Include the required HTML components for each section as specified.
- Every chart must use real data from the research context above.
- If data is not available, use reasonable industry benchmarks and mark them as estimated.
- Output ONLY the HTML — no preamble, no explanation, no markdown."""

        try:
            html = _gpt_call(gpt_system, batch_user, max_tokens=32000)
            log(f"  [Gen] {batch_label} done: {len(html)} chars")
            return batch_num, html
        except Exception as e:
            log(f"  [Gen] ERROR in {batch_label}: {e}")
            # Return placeholder sections so the report doesn't have gaps
            fallback = ""
            section_map = {sid: (num, title) for sid, num, title in SECTIONS}
            for sid in section_ids:
                num, title = section_map[sid]
                fallback += f"""<section class="section" id="{sid}">
  <div class="section-label">Section {num}</div>
  <h2>{title}</h2>
  <div class="callout danger">
    <span class="callout-icon">⚠</span>
    <div>Section generation failed: {str(e)[:200]}</div>
  </div>
</section>\n"""
            return batch_num, fallback

    # ── Wave 1: Batches 1-5 in parallel ──────────────────────────────────────
    log("Phase 2 — Wave 1: Batches 1-5...")
    wave1_results = {}

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {
            executor.submit(generate_batch, i + 1, BATCHES[i]): i + 1
            for i in range(5)
        }
        for future in as_completed(futures):
            batch_num, html = future.result()
            wave1_results[batch_num] = html

    # ── Wave 2: Batches 6-10 in parallel ─────────────────────────────────────
    log("Phase 2 — Wave 2: Batches 6-10...")
    wave2_results = {}

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {
            executor.submit(generate_batch, i + 6, BATCHES[i + 5]): i + 6
            for i in range(5)
        }
        for future in as_completed(futures):
            batch_num, html = future.result()
            wave2_results[batch_num] = html

    # ── Reassemble in order ───────────────────────────────────────────────────
    all_results = {**wave1_results, **wave2_results}
    ordered_batches = [all_results[i + 1] for i in range(10)]

    log(
        f"Phase 2 complete: {sum(len(b) for b in ordered_batches):,} total chars "
        f"across {len(ordered_batches)} batches"
    )

    return ordered_batches


# ─── Phase 3: HTML Assembly ───

# The 51-section navigation list (matches SECTIONS in new_run_report_generation.py)
_NAV_SECTIONS = [
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


def _build_sidebar_nav():
    """Build sidebar nav HTML for all 51 sections."""
    links = []
    for sid, num, title in _NAV_SECTIONS:
        links.append(
            f'<a href="#{sid}"><span class="nav-num">{num}</span>{title}</a>'
        )
    return "\n        ".join(links)


def _linkify_sources(html_text, registry):
    """
    Convert [Source: ID] tags and <span class="source-tag">[Source: ID]</span>
    to real hyperlinks using the source registry.
    """
    import re as _re

    source_map = {src["id"]: src for src in registry}

    def replace_tag(match):
        src_id = match.group(1).strip()
        if src_id in source_map:
            src = source_map[src_id]
            url = src.get("url", "")
            name = src.get("name", src_id)
            if url:
                return (
                    f'<a href="{url}" target="_blank" class="cite" '
                    f'title="{name}">[{src_id}]</a>'
                )
            else:
                return f'<span class="no-source">[{src_id}]</span>'
        elif src_id.lower() == "estimated":
            return '<span class="no-source">[estimated]</span>'
        else:
            return f'<span class="no-source">[{src_id}]</span>'

    # Match <span class="source-tag">[Source: ID]</span>
    html_text = _re.sub(
        r'<span[^>]*class="source-tag"[^>]*>\[Source:\s*([^\]]+)\]</span>',
        replace_tag,
        html_text,
    )
    # Match bare [Source: ID]
    html_text = _re.sub(r'\[Source:\s*([^\]]+)\]', replace_tag, html_text)
    return html_text


def assemble_html(brand_name, domain, batches, research, report_id):
    """
    Phase 3: Assemble the final HTML report.

    Parameters
    ----------
    brand_name : str
    domain     : str
    batches    : list[str]  — 10 HTML strings from run_report_generation()
    research   : dict       — structured research JSON
    report_id  : str
    """
    log("Phase 3: Assembling HTML report (51 sections, McKinsey CSS, Chart.js)...")

    # ── Extract key data ──────────────────────────────────────────────────────
    company     = research.get("company", {})
    financials  = research.get("financials", {})
    sentiment   = research.get("customer_sentiment", {})
    source_registry = research.get("_source_registry", [])
    premium_data    = research.get("_premium_data", {})

    latest_rev   = financials.get("latest_revenue", {})
    rev_amount   = latest_rev.get("amount", "N/A")
    rev_year     = latest_rev.get("year", "")
    rev_source   = latest_rev.get("source_url", "")
    rev_note     = latest_rev.get("source_note", "")
    gross_margin = financials.get("gross_margin", {}).get("value", "N/A")
    ebitda_margin = financials.get("ebitda", {}).get("margin", "N/A")
    ebitda_amount = financials.get("ebitda", {}).get("amount", "N/A")

    employee_count = company.get("employee_count", {})
    if isinstance(employee_count, dict):
        emp_val = employee_count.get("value", "N/A")
    else:
        emp_val = employee_count or "N/A"

    founded_year = company.get("founded_year", "")
    hq = company.get("current_headquarters", "")
    business_model = company.get("business_model", "DTC")
    brand_positioning = company.get("brand_positioning", "")

    # Trustpilot
    tp = sentiment.get("trustpilot", {})
    tp_rating = tp.get("rating", "N/A")
    tp_reviews = tp.get("reviews", "N/A")
    if isinstance(tp_reviews, int) and tp_reviews >= 1000:
        tp_reviews_fmt = f"{tp_reviews:,}"
    else:
        tp_reviews_fmt = str(tp_reviews) if tp_reviews != "N/A" else "N/A"

    today = datetime.now().strftime("%B %d, %Y")

    # ── Linkify source tags in all batches ────────────────────────────────────
    processed_batches = [_linkify_sources(b, source_registry) for b in batches]
    sections_html = "\n\n".join(processed_batches)

    # ── Source references section ─────────────────────────────────────────────
    source_refs_html = ""
    if source_registry:
        rows = []
        for src in source_registry:
            url = src.get("url", "")
            name = src.get("name", src.get("id", "Unknown"))
            publisher = src.get("publisher", "")
            src_type = src.get("type", "web")
            tag_class = "opp" if src_type == "premium" else "watch"
            if url:
                url_display = url[:70] + ("\u2026" if len(url) > 70 else "")
                rows.append(
                    f'<tr><td><code>{src["id"]}</code></td>'
                    f'<td>{name}</td>'
                    f'<td>{publisher}</td>'
                    f'<td><span class="tag tag-{tag_class}">{src_type}</span></td>'
                    f'<td><a href="{url}" target="_blank" class="cite">{url_display}</a></td></tr>'
                )
            else:
                rows.append(
                    f'<tr><td><code>{src["id"]}</code></td>'
                    f'<td>{name}</td>'
                    f'<td>{publisher}</td>'
                    f'<td><span class="tag tag-watch">{src_type}</span></td>'
                    f'<td><em>No URL</em></td></tr>'
                )
        source_refs_html = f"""<section class="section" id="s51-sources">
  <div class="section-label">Appendix</div>
  <h2>Source Registry</h2>
  <div class="table-wrap">
    <table>
      <thead>
        <tr><th>ID</th><th>Name</th><th>Publisher</th><th>Type</th><th>URL</th></tr>
      </thead>
      <tbody>
        {"".join(rows)}
      </tbody>
    </table>
  </div>
</section>"""

    # ── Premium data badges ───────────────────────────────────────────────────
    badge_parts = []
    if premium_data.get("pitchbook", 0) > 0:
        badge_parts.append(
            f'<span class="premium-badge pb-badge">PitchBook '
            f'<strong>{premium_data["pitchbook"]}</strong></span>'
        )
    if premium_data.get("statista", 0) > 0:
        badge_parts.append(
            f'<span class="premium-badge st-badge">Statista '
            f'<strong>{premium_data["statista"]}</strong></span>'
        )
    if premium_data.get("cbinsights", 0) > 0:
        badge_parts.append(
            f'<span class="premium-badge cb-badge">CB Insights '
            f'<strong>{premium_data["cbinsights"]}</strong></span>'
        )
    badge_parts.append('<span class="premium-badge pplx-badge">Perplexity AI</span>')
    premium_badges_html = "\n      ".join(badge_parts)

    # ── Rev source link ───────────────────────────────────────────────────────
    if rev_source:
        rev_source_html = f' <a href="{rev_source}" target="_blank" class="cite" style="font-size:11px">source</a>'
    else:
        rev_source_html = ""
    if rev_note:
        rev_source_html += f' <span class="no-source" title="{rev_note}" style="font-size:10px">ℹ updated</span>'

    # ── Sidebar nav ──────────────────────────────────────────────────────────
    sidebar_nav_html = _build_sidebar_nav()

    # ── Full HTML document ─────────────────────────────────────────────────────
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{brand_name} — PE Due Diligence Report | BlazingHill</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap" rel="stylesheet">
  <!-- Chart.js 4.x -->
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.3/dist/chart.umd.min.js"></script>
  <style>
/* ═══════════════════════════════════════════════════════
   Meller Brand PE DD Report — McKinsey-Grade Stylesheet
   White background, professional consulting style
   ═══════════════════════════════════════════════════════ */

:root {{
  --navy: #1a2332;
  --blue: #2563eb;
  --green: #16a34a;
  --amber: #d97706;
  --red: #dc2626;
  --gray-50: #f8fafc;
  --gray-100: #f1f5f9;
  --gray-200: #e2e8f0;
  --gray-300: #cbd5e1;
  --gray-400: #94a3b8;
  --gray-500: #64748b;
  --gray-600: #475569;
  --gray-700: #334155;
  --gray-800: #1e293b;
  --sidebar-w: 260px;
  --header-h: 0px;
}}

/* ── RESET ── */
*, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
html {{ scroll-behavior: smooth; font-size: 16px; }}
body {{
  font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
  color: var(--navy);
  background: white;
  line-height: 1.7;
  -webkit-font-smoothing: antialiased;
}}
a {{ color: var(--blue); text-decoration: none; }}
a:hover {{ text-decoration: underline; }}
img {{ max-width: 100%; height: auto; display: block; }}

/* ── SIDEBAR ── */
#sidebar {{
  position: fixed;
  top: 0; left: 0;
  width: var(--sidebar-w);
  height: 100vh;
  background: var(--navy);
  color: #e2e8f0;
  display: flex;
  flex-direction: column;
  z-index: 100;
  overflow-y: auto;
  border-right: 1px solid rgba(255,255,255,0.08);
}}
.sidebar-header {{
  padding: 24px 20px 16px;
  border-bottom: 1px solid rgba(255,255,255,0.1);
}}
.sidebar-logo {{
  font-size: 11px;
  text-transform: uppercase;
  letter-spacing: 0.15em;
  color: var(--gray-400);
  margin-bottom: 6px;
}}
.sidebar-title {{
  font-size: 15px;
  font-weight: 700;
  line-height: 1.3;
  color: white;
}}
.confidential-badge {{
  display: inline-block;
  margin-top: 8px;
  padding: 2px 8px;
  font-size: 10px;
  text-transform: uppercase;
  letter-spacing: 0.1em;
  background: rgba(220,38,38,0.2);
  color: #fca5a5;
  border-radius: 3px;
  font-weight: 600;
}}
.sidebar-nav {{
  flex: 1;
  padding: 12px 0;
  overflow-y: auto;
}}
.sidebar-nav a {{
  display: flex;
  align-items: center;
  padding: 7px 20px;
  font-size: 12.5px;
  color: var(--gray-400);
  text-decoration: none;
  transition: all 0.15s;
  border-left: 3px solid transparent;
  line-height: 1.3;
}}
.sidebar-nav a:hover {{
  color: white;
  background: rgba(255,255,255,0.05);
  text-decoration: none;
}}
.sidebar-nav a.active {{
  color: white;
  background: rgba(37,99,235,0.15);
  border-left-color: var(--blue);
  font-weight: 600;
}}
.nav-num {{
  display: inline-block;
  width: 26px;
  font-size: 10px;
  font-weight: 700;
  color: var(--gray-500);
  flex-shrink: 0;
}}
.sidebar-nav a.active .nav-num {{ color: var(--blue); }}
.sidebar-footer {{
  padding: 12px 20px;
  font-size: 11px;
  color: var(--gray-500);
  border-top: 1px solid rgba(255,255,255,0.08);
}}

/* ── HAMBURGER (mobile) ── */
#hamburger {{
  display: none;
  position: fixed;
  top: 12px; left: 12px;
  z-index: 200;
  background: var(--navy);
  color: white;
  border: none;
  padding: 8px 12px;
  font-size: 20px;
  border-radius: 6px;
  cursor: pointer;
}}
#overlay {{
  display: none;
  position: fixed;
  inset: 0;
  background: rgba(0,0,0,0.5);
  z-index: 90;
}}

/* ── MAIN CONTENT ── */
#main {{
  margin-left: var(--sidebar-w);
  padding: 48px 56px 80px;
  max-width: 1120px;
}}

/* ── REPORT HEADER ── */
.report-header {{
  margin-bottom: 48px;
  padding-bottom: 32px;
  border-bottom: 3px solid var(--navy);
}}
.firm-label {{
  font-size: 11px;
  text-transform: uppercase;
  letter-spacing: 0.15em;
  color: var(--gray-500);
  margin-bottom: 8px;
}}
.report-header h1 {{
  font-size: 28px;
  font-weight: 800;
  line-height: 1.2;
  color: var(--navy);
  margin-bottom: 8px;
}}
.subtitle {{
  font-size: 15px;
  color: var(--gray-600);
  margin-bottom: 16px;
}}
.report-meta {{
  display: flex;
  flex-wrap: wrap;
  gap: 8px 24px;
  font-size: 12px;
  color: var(--gray-500);
}}
.report-meta span {{
  padding: 4px 0;
}}

/* ── PREMIUM DATA BADGES ── */
.premium-badges {{
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  margin: 16px 0 0;
}}
.premium-badge {{
  display: inline-flex;
  align-items: center;
  padding: 4px 10px;
  border-radius: 4px;
  font-size: 11px;
  font-weight: 500;
  letter-spacing: 0.02em;
  gap: 4px;
}}
.pb-badge  {{ background: #fde8e0; color: #c13b1a; border: 1px solid #f5c4b4; }}
.st-badge  {{ background: #e0edff; color: #1a5fd6; border: 1px solid #b3d0ff; }}
.cb-badge  {{ background: #ede9ff; color: #5b21b6; border: 1px solid #d0c8ff; }}
.pplx-badge {{ background: #f0fdf4; color: #15803d; border: 1px solid #bbf7d0; }}
.premium-badge strong {{ font-weight: 700; }}

/* ── KPI STRIP ── */
.kpi-strip {{
  display: flex;
  flex-wrap: wrap;
  gap: 0;
  margin: 24px 0 40px;
  border: 1px solid var(--gray-200);
  border-radius: 8px;
  overflow: hidden;
}}
.kpi-strip-item {{
  flex: 1 1 160px;
  padding: 16px 20px;
  border-right: 1px solid var(--gray-200);
  background: var(--gray-50);
}}
.kpi-strip-item:last-child {{ border-right: none; }}
.kpi-strip-label {{
  font-size: 11px;
  text-transform: uppercase;
  letter-spacing: 0.08em;
  color: var(--gray-500);
  margin-bottom: 4px;
}}
.kpi-strip-value {{
  font-size: 22px;
  font-weight: 800;
  color: var(--navy);
  line-height: 1.1;
  margin-bottom: 2px;
}}
.kpi-strip-sub {{
  font-size: 11px;
  color: var(--gray-400);
}}

/* ── SECTIONS ── */
.section {{
  margin-bottom: 56px;
  padding-top: 24px;
}}
.section-label {{
  font-size: 11px;
  text-transform: uppercase;
  letter-spacing: 0.15em;
  color: var(--blue);
  font-weight: 700;
  margin-bottom: 4px;
}}
.section h2 {{
  font-size: 22px;
  font-weight: 800;
  color: var(--navy);
  margin-bottom: 16px;
  padding-bottom: 12px;
  border-bottom: 2px solid var(--gray-200);
}}
.section-intro {{
  font-size: 15px;
  color: var(--gray-700);
  line-height: 1.8;
  margin-bottom: 24px;
}}
.section-rule {{ border-top: 1px solid var(--gray-200); margin: 32px 0; }}

h3.subsection {{
  font-size: 16px;
  font-weight: 700;
  color: var(--navy);
  margin: 28px 0 12px;
}}
.mt-sm {{ margin-top: 8px; }}
.mt-md {{ margin-top: 16px; }}
.mt-lg {{ margin-top: 32px; }}

/* ── TWO-COLUMN LAYOUT ── */
.two-col {{
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 32px;
  margin-bottom: 24px;
}}

/* ── KPI CARDS ── */
.kpi-grid {{
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
  gap: 16px;
  margin: 24px 0;
}}
.kpi-card {{
  padding: 20px;
  border-radius: 8px;
  border: 1px solid var(--gray-200);
  background: var(--gray-50);
}}
.kpi-card.kpi-navy {{ border-left: 4px solid var(--navy); }}
.kpi-card.kpi-blue {{ border-left: 4px solid var(--blue); }}
.kpi-card.kpi-green {{ border-left: 4px solid var(--green); }}
.kpi-card.kpi-amber {{ border-left: 4px solid var(--amber); }}
.kpi-card.kpi-red {{ border-left: 4px solid var(--red); }}
.kpi-label {{
  font-size: 11px;
  text-transform: uppercase;
  letter-spacing: 0.08em;
  color: var(--gray-500);
  margin-bottom: 4px;
}}
.kpi-value {{
  font-size: 28px;
  font-weight: 800;
  color: var(--navy);
  line-height: 1.1;
  margin-bottom: 4px;
}}
.kpi-sub {{
  font-size: 12px;
  color: var(--gray-600);
  line-height: 1.4;
}}
.kpi-source {{
  margin-top: 8px;
  font-size: 11px;
}}
.kpi-source a {{ color: var(--blue); }}

/* ── STAT ROWS ── */
.stat-row {{
  display: flex;
  align-items: baseline;
  padding: 8px 0;
  border-bottom: 1px solid var(--gray-100);
  gap: 12px;
}}
.stat-label {{
  font-size: 13px;
  color: var(--gray-500);
  min-width: 140px;
  flex-shrink: 0;
}}
.stat-value {{
  font-size: 14px;
  font-weight: 700;
  color: var(--navy);
}}
.stat-note {{
  font-size: 12px;
  color: var(--gray-400);
  margin-left: auto;
}}

/* ── METRIC BARS ── */
.metric-bar {{ margin-bottom: 10px; }}
.mb-label {{
  display: flex;
  justify-content: space-between;
  font-size: 13px;
  margin-bottom: 4px;
}}
.mb-track {{
  height: 8px;
  background: var(--gray-100);
  border-radius: 4px;
  overflow: hidden;
}}
.mb-fill {{
  height: 100%;
  background: var(--blue);
  border-radius: 4px;
  transition: width 0.5s;
}}
.mb-fill.green {{ background: var(--green); }}
.mb-fill.amber {{ background: var(--amber); }}
.mb-fill.red {{ background: var(--red); }}

/* ── TABLES ── */
.table-wrap {{
  overflow-x: auto;
  margin: 16px 0 24px;
}}
table {{
  width: 100%;
  border-collapse: collapse;
  font-size: 13px;
}}
thead {{
  background: var(--gray-50);
}}
th {{
  padding: 10px 14px;
  text-align: left;
  font-weight: 700;
  font-size: 11px;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  color: var(--gray-600);
  border-bottom: 2px solid var(--gray-200);
}}
td {{
  padding: 10px 14px;
  border-bottom: 1px solid var(--gray-100);
  vertical-align: top;
  line-height: 1.5;
}}
tr:hover td {{ background: var(--gray-50); }}
.data-table td {{ font-variant-numeric: tabular-nums; }}
.sources-table td {{ font-size: 12px; }}

/* ── TAGS ── */
.tag {{
  display: inline-block;
  padding: 2px 8px;
  border-radius: 3px;
  font-size: 11px;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.04em;
  white-space: nowrap;
}}
.tag-risk {{ background: #fef2f2; color: #b91c1c; }}
.tag-opp {{ background: #f0fdf4; color: #15803d; }}
.tag-watch {{ background: #fffbeb; color: #b45309; }}
.tag-high {{ background: #fef2f2; color: #b91c1c; }}
.tag-med {{ background: #fffbeb; color: #b45309; }}
.tag-low {{ background: #f0fdf4; color: #15803d; }}

/* ── CALLOUTS ── */
.callout {{
  display: flex;
  gap: 12px;
  padding: 16px 20px;
  border-radius: 8px;
  margin: 16px 0;
  font-size: 13px;
  line-height: 1.6;
}}
.callout.info {{ background: #eff6ff; border-left: 4px solid var(--blue); }}
.callout.success {{ background: #f0fdf4; border-left: 4px solid var(--green); }}
.callout.warn {{ background: #fffbeb; border-left: 4px solid var(--amber); }}
.callout.danger {{ background: #fef2f2; border-left: 4px solid var(--red); }}
.callout-icon {{ font-size: 18px; flex-shrink: 0; line-height: 1.4; }}

/* ── THESIS BOX ── */
.thesis-box {{
  padding: 24px;
  background: var(--gray-50);
  border: 1px solid var(--gray-200);
  border-radius: 8px;
  font-size: 14px;
  line-height: 1.8;
  margin: 16px 0 24px;
}}

/* ── KEY INSIGHT ── */
.key-insight {{
  padding: 20px 24px;
  background: linear-gradient(135deg, #eff6ff 0%, #f0fdf4 100%);
  border-left: 4px solid var(--blue);
  border-radius: 0 8px 8px 0;
  margin: 20px 0;
  font-size: 14px;
  line-height: 1.7;
}}

/* ── HIGHLIGHT BOX ── */
.highlight-box {{
  padding: 20px 24px;
  background: var(--gray-50);
  border: 1px solid var(--gray-200);
  border-radius: 8px;
  margin: 16px 0;
}}

/* ── SCENARIOS ── */
.scenarios {{
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 16px;
  margin: 16px 0 24px;
}}
.scenario-card {{
  padding: 24px;
  border-radius: 8px;
  text-align: center;
  border: 1px solid var(--gray-200);
}}
.scenario-card.bear {{ background: #fef2f2; border-color: #fecaca; }}
.scenario-card.base {{ background: #eff6ff; border-color: #bfdbfe; }}
.scenario-card.bull {{ background: #f0fdf4; border-color: #bbf7d0; }}
.s-label {{
  font-size: 12px;
  text-transform: uppercase;
  letter-spacing: 0.08em;
  font-weight: 700;
  margin-bottom: 8px;
}}
.bear .s-label {{ color: var(--red); }}
.base .s-label {{ color: var(--blue); }}
.bull .s-label {{ color: var(--green); }}
.s-moic {{
  font-size: 36px;
  font-weight: 800;
  line-height: 1;
  margin-bottom: 8px;
}}
.bear .s-moic {{ color: var(--red); }}
.base .s-moic {{ color: var(--blue); }}
.bull .s-moic {{ color: var(--green); }}
.s-sub {{
  font-size: 12px;
  color: var(--gray-600);
  line-height: 1.5;
}}

/* ── CHART CONTAINERS (Chart.js) ── */
.chart-container {{
  position: relative;
  margin: 24px 0;
  background: var(--gray-50);
  border: 1px solid var(--gray-200);
  border-radius: 8px;
  padding: 20px 20px 12px;
  overflow: hidden;
}}
.chart-container canvas {{
  max-height: 320px;
  width: 100% !important;
}}
.chart-container + figcaption {{
  font-size: 12px;
  color: var(--gray-500);
  line-height: 1.5;
  padding: 8px 4px 16px;
  border-top: none;
}}
.chart-container + figcaption a {{ color: var(--blue); }}
figcaption {{
  font-size: 12px;
  color: var(--gray-500);
  line-height: 1.5;
  padding: 8px 4px 16px;
}}

/* ── EXHIBIT (legacy chart support) ── */
.exhibit {{
  margin: 24px 0;
  background: var(--gray-50);
  border: 1px solid var(--gray-200);
  border-radius: 8px;
  overflow: hidden;
}}
.exhibit figcaption {{
  padding: 12px 16px;
  font-size: 12px;
  color: var(--gray-500);
  line-height: 1.5;
  border-top: 1px solid var(--gray-200);
}}
.exhibit figcaption a {{ color: var(--blue); }}

/* ── LISTS ── */
.report-list {{
  list-style: none;
  padding: 0;
  margin: 12px 0;
}}
.report-list li {{
  padding: 8px 0 8px 20px;
  position: relative;
  font-size: 14px;
  line-height: 1.6;
  border-bottom: 1px solid var(--gray-100);
}}
.report-list li::before {{
  content: '▸';
  position: absolute;
  left: 0;
  color: var(--blue);
  font-weight: bold;
}}

/* ── RISK INDICATORS ── */
.risk-high {{ color: var(--red); font-weight: 700; }}
.risk-medium {{ color: var(--amber); font-weight: 700; }}
.risk-low {{ color: var(--green); font-weight: 700; }}

/* ── TEXT UTILITIES ── */
.text-muted {{ color: var(--gray-500); }}
.text-blue {{ color: var(--blue); }}
.text-green {{ color: var(--green); }}
.text-amber {{ color: var(--amber); }}
.text-red {{ color: var(--red); }}
.tiny {{ font-size: 12px; }}
.small {{ font-size: 13px; }}

/* ── SOURCE / CITATION TAGS ── */
a.cite {{
  font-size: 10px;
  color: var(--blue);
  background: #eff6ff;
  padding: 1px 5px;
  border-radius: 3px;
  text-decoration: none;
  white-space: nowrap;
  border: 1px solid #bfdbfe;
  vertical-align: middle;
}}
a.cite:hover {{ background: #dbeafe; text-decoration: none; }}
.no-source {{
  font-size: 10px;
  color: var(--gray-400);
  background: var(--gray-100);
  padding: 1px 5px;
  border-radius: 3px;
  white-space: nowrap;
  vertical-align: middle;
}}

/* ── ACCESS GATE OVERLAY ── */
.report-gate-overlay {{
  position: fixed;
  inset: 0;
  background: var(--navy);
  z-index: 500;
  display: flex;
  align-items: center;
  justify-content: center;
}}
.report-gate-card {{
  background: white;
  border-radius: 12px;
  padding: 48px;
  max-width: 440px;
  width: 90%;
  text-align: center;
  box-shadow: 0 24px 48px rgba(0,0,0,0.3);
}}
.report-gate-card .gate-brand {{
  font-size: 11px;
  text-transform: uppercase;
  letter-spacing: 0.15em;
  color: var(--gray-400);
  margin-bottom: 16px;
  font-weight: 700;
}}
.report-gate-card h2 {{
  font-size: 24px;
  font-weight: 800;
  color: var(--navy);
  margin-bottom: 8px;
  border: none;
  padding: 0;
}}
.report-gate-card p {{
  font-size: 14px;
  color: var(--gray-500);
  margin-bottom: 24px;
}}
.report-gate-form {{
  display: flex;
  flex-direction: column;
  gap: 12px;
}}
.report-gate-form input {{
  padding: 14px 16px;
  border: 2px solid var(--gray-200);
  border-radius: 8px;
  font-size: 16px;
  text-align: center;
  letter-spacing: 0.05em;
  outline: none;
  transition: border-color 0.2s;
}}
.report-gate-form input:focus {{ border-color: var(--blue); }}
.report-gate-form button {{
  padding: 14px;
  background: var(--navy);
  color: white;
  border: none;
  border-radius: 8px;
  font-size: 15px;
  font-weight: 700;
  cursor: pointer;
  transition: background 0.2s;
}}
.report-gate-form button:hover {{ background: #243347; }}
.report-gate-status {{
  font-size: 13px;
  margin-top: 8px;
  min-height: 20px;
}}
.report-gate-status.error {{ color: var(--red); }}
.gate-hint {{
  margin-top: 20px;
  padding-top: 16px;
  border-top: 1px solid var(--gray-200);
  font-size: 12px;
  color: var(--gray-400);
}}
.gate-hint strong {{
  color: var(--navy);
  font-size: 14px;
  letter-spacing: 0.05em;
}}
.gate-back {{
  display: inline-block;
  margin-top: 12px;
  font-size: 13px;
  color: var(--gray-400);
}}

/* ── LIGHTBOX ── */
#lightbox-overlay {{
  display: none;
  position: fixed;
  inset: 0;
  background: rgba(0,0,0,0.85);
  z-index: 1000;
  justify-content: center;
  align-items: center;
  cursor: zoom-out;
}}
#lightbox-overlay.active {{ display: flex; flex-direction: column; }}
#lightbox-close {{
  position: fixed;
  top: 16px; right: 16px;
  background: none;
  border: none;
  color: white;
  font-size: 36px;
  cursor: pointer;
  z-index: 1001;
  line-height: 1;
}}
#lightbox-img {{
  max-width: 90vw;
  max-height: 85vh;
  object-fit: contain;
  border-radius: 4px;
}}
#lightbox-caption {{
  color: #cbd5e1;
  font-size: 13px;
  text-align: center;
  max-width: 80vw;
  margin-top: 12px;
  line-height: 1.5;
}}

/* ── REPORT FOOTER ── */
.report-footer {{
  margin-left: var(--sidebar-w);
  padding: 24px 56px;
  border-top: 1px solid var(--gray-200);
  font-size: 12px;
  color: var(--gray-400);
  display: flex;
  justify-content: space-between;
  align-items: center;
  flex-wrap: wrap;
  gap: 8px;
}}
.report-footer .footer-brand {{
  font-weight: 700;
  color: var(--navy);
}}

/* ── RESPONSIVE ── */
@media (max-width: 900px) {{
  #sidebar {{ transform: translateX(-100%); transition: transform 0.3s; }}
  #sidebar.open {{ transform: translateX(0); }}
  #hamburger {{ display: block; }}
  #overlay.active {{ display: block; }}
  #main {{ margin-left: 0; padding: 60px 24px 80px; }}
  .report-footer {{ margin-left: 0; padding: 24px; }}
  .two-col {{ grid-template-columns: 1fr; }}
  .kpi-grid {{ grid-template-columns: 1fr 1fr; }}
  .scenarios {{ grid-template-columns: 1fr; }}
  .report-header h1 {{ font-size: 22px; }}
  .kpi-strip {{ flex-direction: column; }}
}}
@media (max-width: 500px) {{
  .kpi-grid {{ grid-template-columns: 1fr; }}
  .report-meta {{ flex-direction: column; }}
}}
  </style>
</head>
<body>

<!-- ── ACCESS GATE ── -->
<div class="report-gate-overlay" id="reportGate">
  <div class="report-gate-card">
    <div class="gate-brand">BlazingHill Research</div>
    <h2>Confidential Report</h2>
    <p>This PE due diligence report is confidential. Enter your access code to continue.</p>
    <div class="report-gate-form">
      <input type="password" id="gateInput" placeholder="Access code" autocomplete="off">
      <button onclick="checkGate()">Access Report</button>
      <div class="report-gate-status" id="gateStatus"></div>
    </div>
    <div class="gate-hint">
      <div>Report: <strong>{brand_name.upper()}</strong></div>
      <a href="#" class="gate-back" onclick="document.getElementById('reportGate').style.display='none';return false;">
        Skip (demo mode)
      </a>
    </div>
  </div>
</div>

<!-- ── HAMBURGER BUTTON (mobile) ── -->
<button id="hamburger" onclick="toggleSidebar()" aria-label="Open navigation">&#9776;</button>
<div id="overlay" onclick="toggleSidebar()"></div>

<!-- ── SIDEBAR ── -->
<nav id="sidebar" role="navigation" aria-label="Report sections">
  <div class="sidebar-header">
    <div class="sidebar-logo">BlazingHill Research</div>
    <div class="sidebar-title">{brand_name}<br>PE Due Diligence</div>
    <span class="confidential-badge">Confidential</span>
  </div>
  <div class="sidebar-nav">
    {sidebar_nav_html}
  </div>
  <div class="sidebar-footer">
    Generated {today}<br>
    Report ID: {report_id}
  </div>
</nav>

<!-- ── LIGHTBOX ── -->
<div id="lightbox-overlay" onclick="closeLightbox()">
  <button id="lightbox-close" onclick="closeLightbox()">&#215;</button>
  <img id="lightbox-img" src="" alt="">
  <div id="lightbox-caption"></div>
</div>

<!-- ── MAIN CONTENT ── -->
<div id="main">

  <!-- Report Header -->
  <div class="report-header">
    <div class="firm-label">BlazingHill Research &middot; PE Due Diligence</div>
    <h1>{brand_name} &mdash; Commercial & Digital DD</h1>
    <p class="subtitle">{business_model} &middot; {hq}{(" &middot; " + brand_positioning[:80]) if brand_positioning else ""}</p>
    <div class="report-meta">
      <span>Generated: {today}</span>
      <span>Domain: <a href="https://{domain}" target="_blank">{domain}</a></span>
      <span>Report ID: {report_id}</span>
      <span>Sections: 51</span>
    </div>
    <div class="premium-badges">
      {premium_badges_html}
    </div>
  </div>

  <!-- KPI Strip -->
  <div class="kpi-strip">
    <div class="kpi-strip-item">
      <div class="kpi-strip-label">Latest Revenue</div>
      <div class="kpi-strip-value">{rev_amount}</div>
      <div class="kpi-strip-sub">FY{rev_year}{rev_source_html}</div>
    </div>
    <div class="kpi-strip-item">
      <div class="kpi-strip-label">Gross Margin</div>
      <div class="kpi-strip-value">{gross_margin}</div>
      <div class="kpi-strip-sub">reported</div>
    </div>
    <div class="kpi-strip-item">
      <div class="kpi-strip-label">EBITDA</div>
      <div class="kpi-strip-value">{ebitda_amount}</div>
      <div class="kpi-strip-sub">{ebitda_margin} margin</div>
    </div>
    <div class="kpi-strip-item">
      <div class="kpi-strip-label">Employees</div>
      <div class="kpi-strip-value">{emp_val}</div>
      <div class="kpi-strip-sub">est.</div>
    </div>
    <div class="kpi-strip-item">
      <div class="kpi-strip-label">Founded</div>
      <div class="kpi-strip-value">{founded_year}</div>
      <div class="kpi-strip-sub">{hq}</div>
    </div>
    <div class="kpi-strip-item">
      <div class="kpi-strip-label">Trustpilot</div>
      <div class="kpi-strip-value">{tp_rating}<span style="font-size:14px;font-weight:400;color:var(--gray-400)">/5</span></div>
      <div class="kpi-strip-sub">{tp_reviews_fmt} reviews</div>
    </div>
  </div>

  <!-- Generated Sections (51 sections across 10 batches) -->
  {sections_html}

  <!-- Source Registry -->
  {source_refs_html}

</div><!-- /#main -->

<!-- ── FOOTER ── -->
<footer class="report-footer">
  <span><span class="footer-brand">BlazingHill Research</span> &mdash; Commercial &amp; Digital Due Diligence</span>
  <span>Confidential &middot; {today} &middot; {report_id}</span>
</footer>

<script>
/* ═══════════════════════════════════════════════════════
   BlazingHill Report v3.2 — Interactive JS
   ═══════════════════════════════════════════════════════ */

// ── Access Gate ────────────────────────────────────────
var VALID_CODES = ['BLAZINGHILL', 'BH2025', 'DD2025'];

function checkGate() {{
  var val = document.getElementById('gateInput').value.trim().toUpperCase();
  var status = document.getElementById('gateStatus');
  if (VALID_CODES.indexOf(val) >= 0) {{
    document.getElementById('reportGate').style.display = 'none';
    status.textContent = '';
  }} else {{
    status.textContent = 'Incorrect code. Please try again.';
    status.className = 'report-gate-status error';
    document.getElementById('gateInput').value = '';
  }}
}}

document.getElementById('gateInput').addEventListener('keydown', function(e) {{
  if (e.key === 'Enter') checkGate();
}});

// ── Sidebar Toggle (mobile) ────────────────────────────
function toggleSidebar() {{
  document.getElementById('sidebar').classList.toggle('open');
  document.getElementById('overlay').classList.toggle('active');
}}

// ── Active Sidebar Link on Scroll ─────────────────────
(function() {{
  var navLinks = document.querySelectorAll('.sidebar-nav a[href^="#"]');
  var sections = [];
  navLinks.forEach(function(link) {{
    var id = link.getAttribute('href').slice(1);
    var el = document.getElementById(id);
    if (el) sections.push({{ id: id, el: el, link: link }});
  }});

  if (!sections.length) return;

  function onScroll() {{
    var scrollY = window.scrollY + 100;
    var active = sections[0];
    for (var i = 0; i < sections.length; i++) {{
      if (sections[i].el.getBoundingClientRect().top + window.scrollY <= scrollY) {{
        active = sections[i];
      }}
    }}
    navLinks.forEach(function(l) {{ l.classList.remove('active'); }});
    if (active) active.link.classList.add('active');
  }}

  window.addEventListener('scroll', onScroll, {{ passive: true }});
  onScroll();
}})();

// ── Lightbox for images ────────────────────────────────
function openLightbox(src, caption) {{
  document.getElementById('lightbox-img').src = src;
  document.getElementById('lightbox-caption').textContent = caption || '';
  document.getElementById('lightbox-overlay').classList.add('active');
}}
function closeLightbox() {{
  document.getElementById('lightbox-overlay').classList.remove('active');
  document.getElementById('lightbox-img').src = '';
}}
document.querySelectorAll('.exhibit img').forEach(function(img) {{
  img.addEventListener('click', function() {{
    var cap = img.closest('.exhibit') && img.closest('.exhibit').querySelector('figcaption');
    openLightbox(img.src, cap ? cap.textContent : '');
  }});
}});

// ── Chart.js Auto-initialization ──────────────────────
(function() {{
  var chartContainers = document.querySelectorAll('.chart-container[data-chart]');
  var chartIndex = 0;

  chartContainers.forEach(function(container) {{
    chartIndex++;
    var rawConfig = container.getAttribute('data-chart');
    if (!rawConfig) return;

    // Parse JSON config
    var config;
    try {{
      config = JSON.parse(rawConfig);
    }} catch(e) {{
      console.warn('Chart ' + chartIndex + ': invalid JSON —', e.message, rawConfig.substring(0, 100));
      container.innerHTML = '<div style="padding:16px;color:var(--gray-400);font-size:13px;">'
        + 'Chart data unavailable (JSON parse error)</div>';
      return;
    }}

    // Create canvas
    var canvas = document.createElement('canvas');
    canvas.setAttribute('id', 'chart-' + chartIndex);
    canvas.setAttribute('role', 'img');
    canvas.setAttribute('aria-label', 'Chart ' + chartIndex);
    container.innerHTML = '';
    container.appendChild(canvas);

    // Enforce responsive defaults
    if (!config.options) config.options = {{}};
    config.options.responsive = true;
    config.options.maintainAspectRatio = true;
    if (!config.options.aspectRatio) config.options.aspectRatio = 2;

    // Enforce legend display
    if (!config.options.plugins) config.options.plugins = {{}};
    if (!config.options.plugins.legend) {{
      config.options.plugins.legend = {{ position: 'bottom', labels: {{ font: {{ size: 11 }} }} }};
    }}

    // Default color palette if datasets have no colors
    var palette = ['#2563eb','#16a34a','#d97706','#dc2626','#7c3aed','#06b6d4','#ec4899'];
    var paletteAlpha = ['#2563eb33','#16a34a33','#d9770633','#dc262633','#7c3aed33','#06b6d433','#ec489933'];

    if (config.data && config.data.datasets) {{
      config.data.datasets.forEach(function(ds, idx) {{
        var c = palette[idx % palette.length];
        var ca = paletteAlpha[idx % paletteAlpha.length];
        var chartType = config.type;
        if (!ds.backgroundColor) {{
          if (chartType === 'line') {{
            ds.backgroundColor = ca;
          }} else if (chartType === 'doughnut' || chartType === 'pie' || chartType === 'polarArea') {{
            ds.backgroundColor = palette.slice(0, (ds.data || []).length);
          }} else {{
            ds.backgroundColor = c;
          }}
        }}
        if (!ds.borderColor && chartType === 'line') {{
          ds.borderColor = c;
          ds.borderWidth = ds.borderWidth || 2;
          ds.tension = ds.tension !== undefined ? ds.tension : 0.35;
          ds.fill = ds.fill !== undefined ? ds.fill : false;
          ds.pointRadius = ds.pointRadius !== undefined ? ds.pointRadius : 3;
        }}
      }});
    }}

    try {{
      new Chart(canvas.getContext('2d'), config);
    }} catch(e) {{
      console.warn('Chart ' + chartIndex + ': render error —', e.message);
      container.innerHTML = '<div style="padding:16px;color:var(--gray-400);font-size:13px;">'
        + 'Chart render error: ' + e.message + '</div>';
    }}
  }});

  console.log('[BlazingHill] Initialized ' + chartIndex + ' Chart.js charts.');
}})();

// ── Smooth scroll for sidebar links ───────────────────
document.querySelectorAll('.sidebar-nav a[href^="#"]').forEach(function(link) {{
  link.addEventListener('click', function(e) {{
    var target = document.getElementById(link.getAttribute('href').slice(1));
    if (target) {{
      e.preventDefault();
      target.scrollIntoView({{ behavior: 'smooth', block: 'start' }});
      // Close mobile sidebar
      document.getElementById('sidebar').classList.remove('open');
      document.getElementById('overlay').classList.remove('active');
    }}
  }});
}});

</script>
</body>
</html>"""

    log(f"Phase 3 complete: {len(html):,} chars HTML")
    return html



# ─── Main Entry Point ───

def main(brand_name, domain, market, report_id, output_dir):
    """Main pipeline v3.2: research → report generation → HTML assembly."""
    log(f"Starting BlazingHill Report Engine v3.2")
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

    # ── Phase 2: Report Generation (10 batches, 51 sections) ──
    try:
        batches = run_report_generation(research, brand_name, domain, report_id)
    except Exception as e:
        log(f"ERROR in report generation: {e}")
        traceback.print_exc()
        sys.exit(1)

    # ── Phase 3: HTML Assembly ──
    try:
        html = assemble_html(brand_name, domain, batches, research, report_id)
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
    parser = argparse.ArgumentParser(description="BlazingHill Report Engine v3.2")
    parser.add_argument("--brand", required=True, help="Brand name")
    parser.add_argument("--domain", required=True, help="Brand domain")
    parser.add_argument("--market", required=True, help="Market category")
    parser.add_argument("--lens", default="Commercial diligence", help="Analysis lens")
    parser.add_argument("--report-id", required=True, help="Report ID")
    parser.add_argument("--output-dir", required=True, help="Output directory")
    args = parser.parse_args()
    main(args.brand, args.domain, args.market, args.report_id, args.output_dir)
