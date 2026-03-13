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


# --- Cashmere API (PitchBook, CB Insights, Statista) ---

def _cashmere_search(query, collection_id, limit=5):
    """Search Cashmere API for premium data sources."""
    if not CASHMERE_API_KEY:
        log(f"  WARN: No CASHMERE_API_KEY -- skipping premium search for: {query[:50]}")
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


# --- Perplexity API ---

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
    """Perplexity call that returns content with INLINE source attribution."""
    content, citations = _perplexity_call(system_msg, user_msg, max_tokens)
    tagged_citations = []
    for i, url in enumerate(citations):
        tagged_citations.append({
            "index": i + 1,
            "url": url,
            "call_name": call_name,
            "source_name": f"Perplexity ({call_name})",
        })
    return content, tagged_citations


# --- GPT API ---

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


# --- Phase 1: Multi-Source Research ---

def run_research(brand_name, domain, market):
    """Phase 1: Collect data from premium sources + Perplexity, then structure."""
    today = datetime.now().strftime("%B %d, %Y")
    if not PERPLEXITY_API_KEY:
        raise RuntimeError("PERPLEXITY_API_KEY not set")
    if not OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY not set")

    # Phase 1a: Premium Data Collection
    log("Phase 1a: Collecting premium data (PitchBook, CB Insights, Statista)...")
    premium_data = {"pitchbook": [], "cbinsights": [], "statista": []}

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

    if not any(premium_data.values()) and CASHMERE_API_KEY:
        premium_queries = {
            "pitchbook": [(PITCHBOOK_COMPANY, f"{brand_name}", "PitchBook")],
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

    # Phase 1b: Perplexity Research
    log("Phase 1b: Perplexity research (digital, social, reviews, supplemental)...")
    pplx_system = f"""You are a senior research analyst. Today's date is {today}.
Answer thoroughly with specific numbers, dates, and facts.
When citing sources, include the ACTUAL URL in parentheses after each claim.
Be precise with exact figures, currencies, percentages.
Prioritize the MOST RECENT data available."""

    has_pitchbook = pb_count > 0
    if has_pitchbook:
        prompt_core = f"""Research {brand_name} ({domain}) for supplemental financial data not typically in PitchBook:
1. RECENT NEWS: Latest financial results for {brand_name} in 2025-2026.
2. GROSS MARGIN & EBITDA: Search for "{brand_name} gross margin", "{brand_name} EBITDA".
3. OPERATIONS: Manufacturing model, countries, logistics, supply chain risks.
4. BUSINESS MODEL: Revenue channel split, geographic breakdown, AOV, repeat rate.
For each fact, include the source URL in parentheses."""
    else:
        prompt_core = f"""Research {brand_name} ({domain}) thoroughly. I need:
1. COMPANY BASICS: Legal name, founding year, founding city, HQ, founders, employees, business model.
2. FINANCIALS: Most recent revenue, revenue history 3+ years, gross margin, EBITDA, funding.
3. OPERATIONS: Manufacturing, locations, logistics, supply chain risks.
For each fact, include the source URL in parentheses."""

    prompt_digital = f"""Research the digital presence and customer reviews of {brand_name} ({domain}). Current data as of {today}:
1. WEBSITE TRAFFIC: Monthly visits, trend, top channels, top countries, mobile pct, DA.
2. SOCIAL MEDIA: Instagram, TikTok, Facebook, YouTube, Twitter/X follower counts.
3. CUSTOMER REVIEWS: Trustpilot rating and review count, themes.
4. TECH STACK: E-commerce platform, analytics, email marketing.
For EACH data point, include the source URL in parentheses."""

    prompt_market = f"""Research the competitive landscape for {brand_name} ({domain}) in the {market} market:
1. DIRECT COMPETITORS: Revenue for Lululemon, Nike activewear, Under Armour, Alo Yoga, Vuori, Alphalete, YoungLA, Fabletics.
2. MARKET SIZE: TAM/SAM/SOM for global activewear/athleisure.
3. M&A COMPARABLES: Recent acquisitions/PE investments in athletic/DTC apparel 2020-2026.
4. INDUSTRY TRENDS: Key trends in DTC fitness apparel.
For each fact, include the source URL in parentheses."""

    perplexity_findings = {}
    perplexity_citations = {}
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

    # Phase 1c: GPT Structuring with Source Attribution
    log("Phase 1c: Structuring all data via GPT (with explicit source attribution)...")
    premium_summary = ""
    source_registry = []

    if premium_data["pitchbook"]:
        premium_summary += "\n\n=== PITCHBOOK DATA (AUTHORITATIVE -- use as primary source for company/financial data) ===\n"
        for i, item in enumerate(premium_data["pitchbook"]):
            premium_summary += f"\n[PB-{i+1}] {item['title']}\n{item['content']}\nSource URL: {item['source_url']}\n"
            source_registry.append({"id": f"PB-{i+1}", "name": f"PitchBook: {item['title']}", "url": item["source_url"], "publisher": "PitchBook", "type": "premium"})

    if premium_data["statista"]:
        premium_summary += "\n\n=== STATISTA DATA (AUTHORITATIVE -- use for market sizing and industry data) ===\n"
        for i, item in enumerate(premium_data["statista"]):
            premium_summary += f"\n[ST-{i+1}] {item['title']}\n{item['content']}\nSource URL: {item['source_url']}\n"
            source_registry.append({"id": f"ST-{i+1}", "name": f"Statista: {item['title']}", "url": item["source_url"], "publisher": "Statista", "type": "premium"})

    if premium_data["cbinsights"]:
        premium_summary += "\n\n=== CB INSIGHTS DATA (use for market trends, DTC strategy, competitive analysis) ===\n"
        for i, item in enumerate(premium_data["cbinsights"]):
            premium_summary += f"\n[CB-{i+1}] {item['title']}\n{item['content']}\nSource URL: {item['source_url']}\n"
            source_registry.append({"id": f"CB-{i+1}", "name": f"CB Insights: {item['title']}", "url": item["source_url"], "publisher": "CB Insights", "type": "premium"})

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
                    source_registry.append({"id": ref_id, "name": f"Web: {c['url'][:60]}", "url": c["url"], "publisher": "Web Source", "type": "web"})

    structure_system = f"""You are a data extraction specialist for PE due diligence research.
Extract structured JSON from multi-source research findings.
CRITICAL RULES:
1. Extract EVERY specific number, date, and fact mentioned.
2. For EVERY data point, set source_id to the reference ID that contains that data.
3. For source_url: copy the EXACT URL from the source entry. NEVER fabricate URLs.
4. PRIORITY ORDER for conflicting data: PitchBook > Statista > CB Insights > Perplexity
5. If PitchBook says revenue is X but Perplexity says Y, USE PitchBook's figure.
6. If a data point has no source, set source_id and source_url to null.
7. Prefer the most recent data when multiple years are available.
Return ONLY valid JSON -- no markdown, no code fences."""

    structure_prompt = f"""Extract structured data about {brand_name} ({domain}) from these research findings.

{premium_summary}

{pplx_summary}

SOURCE REGISTRY (use source_id and source_url from this list):
{json.dumps(source_registry, indent=2)}

Extract into this JSON structure. EVERY field with a data value MUST have source_id and source_url:
{{
  "company": {{"legal_name": "string or null", "brand_name": "{brand_name}", "domain": "{domain}", "founded_year": "year", "founded_city": "city", "current_headquarters": "city", "founders": [{{"name": "...", "title": "...", "background": "..."}}], "employee_count": {{"value": 0, "date": "YYYY-MM", "source_id": "PB-1", "source_url": "url"}}, "business_model": "DTC/B2B", "product_categories": [], "price_range": "$X-$Y", "key_markets": [], "unique_selling_points": [], "brand_positioning": "desc", "source_id": "PB-1", "source_url": "url"}},
  "financials": {{"revenue_history": [{{"year": 2024, "revenue": "amount", "growth_yoy": "X%", "source_id": "id", "source_url": "url"}}], "latest_revenue": {{"year": 2024, "amount": "amount", "source_id": "id", "source_url": "url"}}, "gross_margin": {{"value": "X%", "basis": "reported/estimated", "source_id": "id", "source_url": "url"}}, "ebitda": {{"amount": "amount", "margin": "X%", "source_id": "id", "source_url": "url"}}, "funding_rounds": [{{"round": "type", "amount": "$XM", "date": "YYYY-MM", "investors": [], "post_money_valuation": "$XM", "source_id": "id", "source_url": "url"}}], "aov_estimate": {{"value": "$X", "source_id": "id", "source_url": "url"}}, "repeat_purchase_rate": {{"value": "X%", "source_id": "id", "source_url": "url"}}, "revenue_channels": {{"dtc_pct": "X%", "wholesale_pct": "X%", "source_id": "id", "source_url": "url"}}, "geographic_revenue": [{{"region": "name", "pct": "X%", "source_id": "id", "source_url": "url"}}]}},
  "competitors": {{"direct": [{{"name": "Real Company", "revenue_est": "amount", "price_range": "$X-$Y", "differentiator": "desc", "market_position": "leader", "source_id": "id", "source_url": "url"}}], "market_size": {{"tam": "$XB", "sam": "$XB", "som": "$XM", "growth_rate": "X% CAGR", "source_id": "id", "source_url": "url"}}, "industry_trends": [], "ma_comparables": [{{"target": "company", "acquirer": "buyer", "year": 2024, "value": "$XM", "ev_revenue": "X.Xx", "source_id": "id", "source_url": "url"}}]}},
  "digital_marketing": {{"monthly_traffic": {{"value": "XM visits", "source_id": "id", "source_url": "url"}}, "traffic_trend": "growing/stable/declining", "top_channels": [{{"channel": "Organic", "pct": "X%"}}], "top_countries": [{{"country": "US", "pct": "X%"}}], "social_media": {{"instagram": {{"followers": "XM", "source_id": "id", "source_url": "url"}}, "tiktok": {{"followers": "XM", "source_id": "id", "source_url": "url"}}, "facebook": {{"followers": "XK"}}, "youtube": {{"subscribers": "XK"}}, "twitter": {{"followers": "XK"}}}}, "tech_stack": []}},
  "customer_sentiment": {{"trustpilot": {{"rating": 0.0, "reviews": 0, "source_id": "id", "source_url": "url"}}, "praise_themes": [{{"theme": "desc", "quote": "quote"}}], "complaint_themes": [{{"theme": "desc", "quote": "quote"}}]}},
  "operations": {{"manufacturing": "type", "manufacturing_locations": [], "logistics": "type", "supply_chain_risks": []}}
}}

CRITICAL: PitchBook=PRIMARY for revenue/employees/funding. Statista=market size. Perplexity=digital/social. Every numeric value MUST have source_id and source_url or null."""

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

    # Phase 1d: Gap-filling
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
        gap_queries.append(("revenue", f"What is {brand_name} most recent official annual revenue? Give exact figure in native currency."))
    if not tp or not tp.get("rating"):
        gap_queries.append(("trustpilot", f"What is the current Trustpilot rating for {brand_name}? Star rating and total review count?"))
    if not ig or not ig.get("followers"):
        gap_queries.append(("instagram", f"How many Instagram followers does {brand_name} have currently?"))

    if gap_queries:
        log(f"  Found {len(gap_queries)} gaps: {[q[0] for q in gap_queries]}. Running targeted searches...")
        def do_gap_call(name, prompt):
            log(f"    Gap search: {name}...")
            content, cites = _perplexity_call(pplx_system, prompt, 2000)
            log(f"    {name}: {len(content)} chars, {len(cites)} citations")
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
{{"latest_revenue": {{"year": 2025, "amount": "amount", "source_url": "url_or_null"}}, "trustpilot": {{"rating": 0.0, "reviews": 0, "source_url": "url_or_null"}}, "instagram": {{"followers": "X.XM", "source_url": "url_or_null"}}}}"""
            merge_response = _gpt_call(merge_system, merge_prompt, 2000)
            gap_data = _parse_json(merge_response)
            if gap_data:
                log(f"  Gap data found: {list(gap_data.keys())}")
                if gap_data.get("latest_revenue") and gap_data["latest_revenue"].get("amount"):
                    research.setdefault("financials", {})["latest_revenue"] = gap_data["latest_revenue"]
                if gap_data.get("trustpilot") and gap_data["trustpilot"].get("rating"):
                    research.setdefault("customer_sentiment", {})["trustpilot"] = gap_data["trustpilot"]
                if gap_data.get("instagram") and gap_data["instagram"].get("followers"):
                    research.setdefault("digital_marketing", {}).setdefault("social_media", {})["instagram"] = gap_data["instagram"]
    else:
        log("  No critical gaps found.")

    research["_source_registry"] = source_registry
    research["_premium_data"] = {k: len(v) for k, v in premium_data.items()}
    research["_perplexity_calls"] = list(perplexity_findings.keys())

    financials = research.get("financials", {})
    latest_rev = financials.get("latest_revenue", {})
    sentiment = research.get("customer_sentiment", {})
    tp = sentiment.get("trustpilot", {})
    digital = research.get("digital_marketing", {})
    social = digital.get("social_media", {})
    ig = social.get("instagram", {})
    company = research.get("company", {})
    log(f"Research complete:")
    log(f"  Revenue: {latest_rev}")
    log(f"  Trustpilot: {tp}")
    log(f"  Instagram: {ig}")
    log(f"  Founded: {company.get('founded_city')} | HQ: {company.get('current_headquarters')}")
    log(f"  Sources: {len(source_registry)} total")
    return research


# --- Phase 2: Report Generation ---

REPORT_SYSTEM = """You are a McKinsey-grade PE due diligence report writer.
You produce investment-quality HTML report content for senior private equity partners.
This report is being used to make million-dollar investment decisions. Any misinformation could jeopardize the deal.

CRITICAL RULES:
1. Write substantive analysis with minimum 2-3 paragraphs per section with specific data points
2. Every numeric claim must have a source citation as a clickable link
3. Use PE metrics throughout: EBITDA, EV/Revenue, LTV/CAC, payback periods, ROIC, IRR, MOIC
4. Include Chart.js configurations where specified using REAL data from the research
5. Tables must have real data, never empty rows or N/A for everything
6. If exact data unavailable, provide industry-benchmark estimates labeled (Est.)
7. Competitor names must be REAL companies
8. Return ONLY the HTML content, no markdown, no code fences

SOURCE CITATION RULES (CRITICAL):
9. ONLY use source URLs from the SOURCE REGISTRY provided. NEVER invent URLs.
10. Format citations as: <a href="EXACT_URL_FROM_REGISTRY" target="_blank">Publisher Name</a>
11. If no URL available, cite as plain text (Source: Company annual report) with NO hyperlink.
12. NEVER create URLs with future dates or URLs not in the registry.
13. When citing PitchBook data, link to the PitchBook source URL and label it PitchBook.
14. When citing Statista data, link to the Statista source URL and label it Statista.

DATA PRIORITY (when sources conflict):
15. PitchBook > Statista > CB Insights > Perplexity web sources
16. Use the MOST RECENT revenue figure. For UK companies, use native currency.
17. Founding city and current HQ city may differ -- check both fields."""

SECTION_DEFS = [
    (1, "Executive Summary", "KPI cards (6-8), investment thesis box, key risks & opportunities table"),
    (2, "Company Profile", "Corporate fundamentals stat-rows, product portfolio, revenue timeline, transaction summary"),
    (3, "PE Economics", "EBITDA analysis, unit economics (AOV/CAC/LTV/payback/margin), M&A comps, return scenarios, Chart: EBITDA waterfall"),
    (4, "Digital Marketing Performance", "Traffic overview, channel mix, geo distribution, funnel metrics, Chart: Traffic channel bar"),
    (5, "Competitive Intelligence", "Competitor comparison table (5+), Chart: Radar chart vs top competitors on 6 dimensions"),
    (6, "AI & Innovation Assessment", "Overall score, capability assessment, AI transfer plan, Chart: AI readiness heatmap"),
    (7, "Risk Assessment", "Risk matrix (risk/likelihood/impact/severity/mitigation), Chart: Risk severity horizontal bars"),
    (8, "Channel Economics", "ROAS by channel, Meta CPM trend, Chart: Channel ROI bar"),
    (9, "Cohort Analysis", "DTC retention benchmarks, LTV build, Chart: Retention decay curve"),
    (10, "TAM / SAM / SOM", "Market sizing stat-rows, growth dynamics, Chart: TAM/SAM/SOM nested visualization"),
    (11, "Customer Sentiment", "Aggregate ratings, praise themes, complaint themes, Chart: Sentiment distribution"),
    (12, "Content Strategy Gap", "SEO opportunity analysis, keyword table, content roadmap"),
    (13, "Value Creation Roadmap", "Value lever table, DTC case studies"),
    (14, "Pricing Strategy & Architecture", "Pricing tiers, competitive pricing map, maturity score, Chart: Pricing comparison"),
    (15, "Revenue Quality & Concentration", "Revenue growth, channel mix, geographic concentration, Chart: Revenue doughnut"),
    (16, "Management & Organization", "Founding team, structure, key person risk"),
    (17, "Technology Stack Assessment", "Core platform, payment infra, tech gap analysis"),
    (18, "Brand Equity Deep Dive", "Review breakdown, themes, brand dimensions, Chart: Brand equity radar"),
    (19, "Supply Chain & Fulfillment", "Manufacturing model, post-acquisition synergies"),
    (20, "Regulatory & Compliance", "GDPR assessment, local regulations, product safety, timeline"),
    (21, "Working Capital & Cash Dynamics", "Cash conversion cycle, FCF build, seasonal dynamics"),
    (22, "Exit Analysis & M&A Comparables", "M&A comps, exit paths, Chart: M&A scatter"),
    (23, "Geographic Expansion Roadmap", "Priority markets, expansion phasing"),
    (24, "Marketing-Adjusted LTV Model", "LTV scenarios, impact waterfall, Chart: LTV waterfall"),
    (25, "CAC Payback & Efficiency", "CAC by channel, organic vs paid, Chart: CAC payback bar"),
    (26, "Contribution Margin Bridge", "Margin bridge steps, optimization, Chart: Contribution margin waterfall"),
    (27, "Marketing P&L & Budget Allocation", "Budget allocation, full-funnel architecture, Chart: Marketing spend pie"),
    (28, "Customer Segmentation & RFM", "RFM segment table, LTV amplification"),
    (29, "Repeat Purchase & Retention", "Retention analysis, structural constraints, Chart: Retention curve"),
    (30, "AOV Dynamics & Uplift Levers", "AOV by geography, uplift roadmap"),
    (31, "NPS & Voice of Customer", "VOC theme decomposition, NPS estimate"),
    (32, "Customer Journey & Funnel", "Full-funnel stage analysis, Chart: Conversion funnel"),
    (33, "SEO Authority & Organic Position", "Domain authority comparison, keyword gaps, Chart: SEO comparison"),
    (34, "Paid Media Performance", "Paid media efficiency, ROAS benchmarks"),
    (35, "Email & CRM Maturity", "CRM maturity audit, email revenue upside"),
    (36, "CRO Analysis", "Conversion audit, mobile-first priorities"),
    (37, "Social Commerce & Influencer ROI", "UGC program, influencer scale"),
    (38, "Share of Voice Analysis", "Competitive social footprint, SOV analysis"),
    (39, "Price Elasticity & Discounting", "Discount dependency, exit roadmap"),
    (40, "Category Disruption Threats", "Threat matrix with probability and impact"),
    (41, "Cross-Border E-Commerce", "Localization scorecard, market entry analysis"),
    (42, "Brand Trademark & IP Valuation", "IP inventory, licensing potential"),
    (43, "First-Party Data Asset", "Data valuation, GDPR compliance"),
    (44, "Content & Creative Library", "Content inventory, production model, reusability"),
    (45, "MarTech Stack ROI", "Tech stack table, optimization recommendations"),
    (46, "100-Day Post-Close Plan", "Phased action plan (Day 1-30, 31-60, 61-100), budget reallocation"),
    (47, "EBITDA Bridge", "Marketing-driven EBITDA levers, Chart: EBITDA bridge waterfall"),
    (48, "Scenario Analysis", "Bull/Base/Bear assumptions, sensitivity drivers, Chart: Scenario comparison"),
    (49, "Investment Committee Summary", "Deal scorecard (10 dimensions), red flags, thesis, conditions, returns, Chart: Deal scorecard radar"),
    (50, "Appendix", "Data sources table, methodology notes"),
]


def _build_batch_prompt(brand_name, domain, market, research_json, source_registry_json, start_section, end_section):
    """Build prompt for a batch of sections with explicit source registry."""
    batch_sections = [s for s in SECTION_DEFS if start_section <= s[0] <= end_section]
    section_list = "\n".join(f"{num:02d}. {title} -- {desc}" for num, title, desc in batch_sections)
    return f"""Using the research data below, generate sections {start_section:02d}-{end_section:02d} of a PE due diligence report for {brand_name} ({domain}) in the {market} market.

RESEARCH DATA (all data points include source_id and source_url for attribution):
{research_json}

SOURCE REGISTRY (use ONLY these URLs for citations -- NEVER invent URLs):
{source_registry_json}

OUTPUT FORMAT:
Return a sequence of HTML section blocks. Each section must follow this structure:

<section class="section" id="sXX">
  <div class="section-label">Section XX</div>
  <h2>Section Title</h2>
  <p class="section-intro">Substantive intro paragraph with <a href="SOURCE_URL" target="_blank">Source Name</a>.</p>
  <h3 class="subsection">Subsection Title</h3>
</section>

COMPONENT TEMPLATES:

KPI Cards:
<div class="kpi-grid">
  <div class="kpi-card kpi-navy">
    <div class="kpi-label">METRIC NAME</div>
    <div class="kpi-value">VALUE</div>
    <div class="kpi-sub">Time period</div>
    <div class="kpi-source"><a href="URL_FROM_REGISTRY" target="_blank">PitchBook</a></div>
  </div>
</div>

Tables (MUST include Source column):
<div class="table-wrap">
  <table>
    <thead><tr><th>Metric</th><th>Value</th><th>Source</th></tr></thead>
    <tbody><tr><td>Revenue</td><td>amount</td><td><a href="URL" target="_blank">PitchBook</a></td></tr></tbody>
  </table>
</div>

Stat Rows:
<div class="stat-row"><span class="stat-label">Label</span><span class="stat-value">Value</span><span class="stat-note"><a href="URL" target="_blank">Source</a></span></div>

Chart.js:
<div class="chart-container" style="position:relative;height:350px;margin:24px 0;">
  <canvas id="chartUniqueId"></canvas>
</div>
<script>
new Chart(document.getElementById('chartUniqueId'), {{
  type: 'bar',
  data: {{ labels: [...], datasets: [{{ ... }}] }},
  options: {{ responsive: true, maintainAspectRatio: false }}
}});
</script>
<p class="tiny text-muted">Sources: <a href="URL" target="_blank">PitchBook</a></p>

CHART COLORS: Navy #1a2332, Blue #2563eb, Green #16a34a, Amber #d97706, Red #dc2626

GENERATE THESE SECTIONS:
{section_list}

CRITICAL:
- Use ONLY data from the research. Every number MUST come from the research data.
- Every data point MUST cite a source from the SOURCE REGISTRY using the exact URL.
- If data unavailable, mark (Est.) and cite as plain text (Source: Industry benchmark)
- Charts use REAL data from research. Canvas IDs must be unique.
- Write for senior PE partners. Minimum 2-3 paragraphs + tables/charts per section.
- Return ONLY HTML."""


def run_report_generation(brand_name, domain, market, research_data, output_dir):
    """Phase 2: Generate full HTML report body via GPT-4.1 in 3 batches."""
    log("Phase 2: Generating report via GPT-4.1 (3 batches)...")
    if not OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY not set")

    clean_research = {k: v for k, v in research_data.items() if not k.startswith("_")}
    research_json = json.dumps(clean_research, indent=2, default=str)
    source_registry = research_data.get("_source_registry", [])
    source_registry_json = json.dumps(source_registry, indent=2, default=str) if source_registry else "[]"

    batches = [
        {"start": 1, "end": 17, "label": "Batch 1/3 (Sections 1-17)"},
        {"start": 18, "end": 34, "label": "Batch 2/3 (Sections 18-34)"},
        {"start": 35, "end": 50, "label": "Batch 3/3 (Sections 35-50)"},
    ]

    def generate_batch(batch):
        prompt = _build_batch_prompt(brand_name, domain, market, research_json, source_registry_json, batch["start"], batch["end"])
        resp = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
            json={"model": "gpt-4.1", "messages": [{"role": "system", "content": REPORT_SYSTEM}, {"role": "user", "content": prompt}], "max_tokens": 32000, "temperature": 0.15},
            timeout=300,
        )
        if resp.status_code != 200:
            log(f"OpenAI API error for {batch['label']}: {resp.status_code}: {resp.text[:300]}")
            resp.raise_for_status()
        data = resp.json()
        body = data["choices"][0]["message"]["content"].strip()
        if body.startswith("```"):
            lines = body.split("\n")
            lines = lines[1:]
            if lines and lines[-1].strip() == "```":
                lines = lines[:-1]
            body = "\n".join(lines)
        finish = data["choices"][0].get("finish_reason", "stop")
        log(f"{batch['label']}: {len(body)} chars, finish_reason={finish}")
        return (batch["start"], body)

    results = {}
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = {executor.submit(generate_batch, b): b for b in batches}
        for future in as_completed(futures):
            batch = futures[future]
            try:
                start, body = future.result()
                results[start] = body
            except Exception as e:
                log(f"ERROR generating {batch['label']}: {e}")
                results[batch["start"]] = f'<section class="section" id="s{batch["start"]:02d}"><h2>Generation failed</h2><p>{str(e)[:200]}</p></section>'

    report_body = "\n\n".join(results[k] for k in sorted(results.keys()))
    log(f"Full report: {len(report_body)} characters from {len(results)} batches")
    return report_body


# --- Phase 3: HTML Assembly ---

def assemble_full_report(brand_name, domain, market, report_body, output_dir):
    """Phase 3: Wrap report body in full HTML with CSS, sidebar, and Chart.js CDN."""
    import re
    log("Phase 3: Assembling final HTML report...")
    now = datetime.now()

    css_path = os.path.join(os.path.dirname(__file__), '..', 'sample-report', 'style.css')
    if os.path.exists(css_path):
        with open(css_path, 'r', encoding='utf-8') as f:
            css = f.read()
    else:
        css = _get_fallback_css()

    sections = re.findall(r'<section[^>]*id="s(\d+)"[^>]*>.*?<h2>(.*?)</h2>', report_body, re.DOTALL)
    if not sections:
        sections = [(f"{i+1:02d}", f"Section {i+1:02d}") for i in range(50)]

    sidebar_links = ""
    for num, title in sections:
        clean_title = re.sub(r'<[^>]+>', '', title).strip()
        short_title = clean_title[:22] + "..." if len(clean_title) > 22 else clean_title
        active = ' class="active"' if num == "01" else ""
        sidebar_links += f'    <a href="#s{num}"{active}><span class="nav-num">{num}</span>{short_title}</a>\n'

    html = f'''<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <meta name="robots" content="noindex, nofollow">
  <title>{_esc(brand_name)} -- PE Marketing Due Diligence Report</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.4/dist/chart.umd.min.js"></script>
  <style>
{css}
.chart-container {{ position: relative; margin: 24px 0; background: #fff; border: 1px solid var(--gray-200, #e2e8f0); border-radius: 8px; padding: 16px; }}
.chart-container canvas {{ max-width: 100%; }}
  </style>
</head>
<body>
<div id="reportContent">
<nav id="sidebar" role="navigation" aria-label="Report sections">
  <div class="sidebar-header">
    <div class="sidebar-logo">Private Equity</div>
    <div class="sidebar-title">{_esc(brand_name)} DD<br>Marketing Due Diligence</div>
    <span class="confidential-badge">Confidential</span>
  </div>
  <div class="sidebar-nav" id="sidebar-nav">
{sidebar_links}  </div>
  <div class="sidebar-footer">
    {now.strftime("%B %Y")} &nbsp;·&nbsp; Confidential
  </div>
</nav>
<button id="hamburger" aria-label="Toggle navigation" aria-expanded="false">&#9776;</button>
<div id="overlay"></div>
<main id="main">
  <header class="report-header">
    <div class="firm-label">Commercial Diligence · Private &amp; Confidential</div>
    <h1>{_esc(brand_name)} -- PE Marketing Due Diligence</h1>
    <div class="subtitle">Commercial Diligence · {_esc(domain)}</div>
    <div class="report-meta">
      <span>Subject: {_esc(brand_name)} ({_esc(domain)})</span>
      <span>Date: {now.strftime("%B %Y")}</span>
      <span>Market: {_esc(market)}</span>
      <span>Status: Confidential Draft</span>
    </div>
  </header>
{report_body}
</main>
</div>
<script>
(function() {{
  const links = document.querySelectorAll('.sidebar-nav a');
  const sections = [];
  links.forEach(a => {{
    const id = a.getAttribute('href')?.replace('#','');
    const el = id ? document.getElementById(id) : null;
    if (el) sections.push({{ el, link: a }});
  }});
  function updateActive() {{
    let current = sections[0];
    const scrollY = window.scrollY + 120;
    for (const s of sections) {{
      if (s.el.offsetTop <= scrollY) current = s;
    }}
    links.forEach(a => a.classList.remove('active'));
    if (current) current.link.classList.add('active');
  }}
  window.addEventListener('scroll', updateActive, {{ passive: true }});
  updateActive();
  const hamburger = document.getElementById('hamburger');
  const sidebar = document.getElementById('sidebar');
  const overlay = document.getElementById('overlay');
  if (hamburger) {{
    hamburger.addEventListener('click', () => {{
      sidebar.classList.toggle('open');
      overlay.classList.toggle('show');
    }});
    overlay?.addEventListener('click', () => {{
      sidebar.classList.remove('open');
      overlay.classList.remove('show');
    }});
    links.forEach(a => a.addEventListener('click', () => {{
      sidebar.classList.remove('open');
      overlay?.classList.remove('show');
    }}));
  }}
}})();
</script>
</body>
</html>'''

    output_path = os.path.join(output_dir, "index.html")
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)
    log(f"Report assembled: {output_path} ({len(html)} chars)")
    return output_path


def _esc(text):
    if not text:
        return ""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")


def _get_fallback_css():
    """Fallback CSS if sample-report/style.css not found."""
    return ''':root {
  --navy: #1a2332; --blue: #2563eb; --green: #16a34a;
  --amber: #d97706; --red: #dc2626;
  --gray-50: #f8fafc; --gray-100: #f1f5f9; --gray-200: #e2e8f0;
  --gray-300: #cbd5e1; --gray-400: #94a3b8; --gray-500: #64748b;
  --gray-600: #475569; --gray-700: #334155; --gray-800: #1e293b;
  --sidebar-w: 260px;
}
*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
html { scroll-behavior: smooth; font-size: 16px; }
body { font-family: 'Inter', sans-serif; color: var(--navy); background: white; line-height: 1.7; }
a { color: var(--blue); text-decoration: none; }
img { max-width: 100%; height: auto; display: block; }
#sidebar { position: fixed; top: 0; left: 0; width: var(--sidebar-w); height: 100vh; background: var(--navy); color: #e2e8f0; display: flex; flex-direction: column; z-index: 100; overflow-y: auto; }
.sidebar-header { padding: 24px 20px 16px; border-bottom: 1px solid rgba(255,255,255,0.1); }
.sidebar-logo { font-size: 11px; text-transform: uppercase; letter-spacing: 0.15em; color: var(--gray-400); }
.sidebar-title { font-size: 15px; font-weight: 700; color: white; }
.confidential-badge { display: inline-block; margin-top: 8px; padding: 2px 8px; font-size: 10px; text-transform: uppercase; background: rgba(220,38,38,0.2); color: #fca5a5; border-radius: 3px; }
.sidebar-nav { flex: 1; padding: 12px 0; overflow-y: auto; }
.sidebar-nav a { display: flex; align-items: center; padding: 7px 20px; font-size: 12.5px; color: var(--gray-400); text-decoration: none; border-left: 3px solid transparent; }
.sidebar-nav a:hover { color: white; background: rgba(255,255,255,0.05); }
.sidebar-nav a.active { color: white; background: rgba(37,99,235,0.15); border-left-color: var(--blue); font-weight: 600; }
.nav-num { display: inline-block; width: 26px; font-size: 10px; font-weight: 700; color: var(--gray-500); }
.sidebar-footer { padding: 12px 20px; font-size: 11px; color: var(--gray-500); border-top: 1px solid rgba(255,255,255,0.08); }
#main { margin-left: var(--sidebar-w); padding: 48px 56px 80px; max-width: 1120px; }
.report-header { margin-bottom: 48px; padding-bottom: 32px; border-bottom: 3px solid var(--navy); }
.firm-label { font-size: 11px; text-transform: uppercase; letter-spacing: 0.15em; color: var(--gray-500); }
.report-header h1 { font-size: 28px; font-weight: 800; color: var(--navy); }
.subtitle { font-size: 15px; color: var(--gray-600); }
.report-meta { display: flex; flex-wrap: wrap; gap: 8px 24px; font-size: 12px; color: var(--gray-500); }
.section { margin-bottom: 56px; padding-top: 24px; }
.section-label { font-size: 11px; text-transform: uppercase; letter-spacing: 0.15em; color: var(--blue); font-weight: 700; }
.section h2 { font-size: 22px; font-weight: 800; color: var(--navy); padding-bottom: 12px; border-bottom: 2px solid var(--gray-200); }
.section-intro { font-size: 15px; color: var(--gray-700); line-height: 1.8; margin-bottom: 24px; }
h3.subsection { font-size: 16px; font-weight: 700; color: var(--navy); margin: 28px 0 12px; }
.kpi-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(200px, 1fr)); gap: 16px; margin: 24px 0; }
.kpi-card { padding: 20px; border-radius: 8px; border: 1px solid var(--gray-200); background: var(--gray-50); }
.kpi-card.kpi-navy { border-left: 4px solid var(--navy); }
.kpi-card.kpi-blue { border-left: 4px solid var(--blue); }
.kpi-card.kpi-green { border-left: 4px solid var(--green); }
.kpi-card.kpi-amber { border-left: 4px solid var(--amber); }
.kpi-card.kpi-red { border-left: 4px solid var(--red); }
.kpi-label { font-size: 11px; text-transform: uppercase; letter-spacing: 0.08em; color: var(--gray-500); }
.kpi-value { font-size: 28px; font-weight: 800; color: var(--navy); }
.kpi-sub { font-size: 12px; color: var(--gray-600); }
.kpi-source { margin-top: 8px; font-size: 11px; }
.kpi-source a { color: var(--blue); }
.stat-row { display: flex; align-items: baseline; padding: 8px 0; border-bottom: 1px solid var(--gray-100); gap: 12px; }
.stat-label { font-size: 13px; color: var(--gray-500); min-width: 140px; }
.stat-value { font-size: 14px; font-weight: 700; color: var(--navy); }
.stat-note { font-size: 12px; color: var(--gray-400); }
.table-wrap { overflow-x: auto; margin: 16px 0; }
table { width: 100%; border-collapse: collapse; font-size: 13px; }
th { background: var(--gray-50); font-weight: 700; text-align: left; padding: 10px 12px; border-bottom: 2px solid var(--gray-200); }
td { padding: 10px 12px; border-bottom: 1px solid var(--gray-100); }
.tag { display: inline-block; padding: 2px 8px; border-radius: 3px; font-size: 11px; font-weight: 600; }
.tag-risk { background: rgba(220,38,38,0.1); color: var(--red); }
.tag-opp { background: rgba(22,163,74,0.1); color: var(--green); }
.tag-watch { background: rgba(217,119,6,0.1); color: var(--amber); }
.tag-high { background: rgba(220,38,38,0.1); color: var(--red); }
.tag-med { background: rgba(217,119,6,0.1); color: var(--amber); }
.tag-low { background: rgba(22,163,74,0.1); color: var(--green); }
.thesis-box { background: var(--gray-50); border-left: 4px solid var(--blue); padding: 20px 24px; border-radius: 0 8px 8px 0; font-size: 15px; line-height: 1.8; }
.report-list { padding-left: 20px; }
.report-list li { margin-bottom: 8px; font-size: 14px; }
.callout { padding: 16px 20px; border-radius: 8px; display: flex; gap: 12px; margin: 16px 0; }
.callout.info { background: rgba(37,99,235,0.05); border: 1px solid rgba(37,99,235,0.15); }
.two-col { display: grid; grid-template-columns: 1fr 1fr; gap: 32px; }
.tiny { font-size: 11px; }
.text-muted { color: var(--gray-400); }
#hamburger { display: none; position: fixed; top: 16px; left: 16px; z-index: 200; background: var(--navy); color: white; border: none; padding: 8px 12px; border-radius: 4px; font-size: 20px; cursor: pointer; }
#overlay { display: none; position: fixed; top: 0; left: 0; width: 100%; height: 100%; background: rgba(0,0,0,0.5); z-index: 90; }
@media (max-width: 768px) {
  #sidebar { transform: translateX(-100%); transition: transform 0.3s; }
  #sidebar.open { transform: translateX(0); }
  #hamburger { display: block; }
  #overlay.show { display: block; }
  #main { margin-left: 0; padding: 24px 16px 60px; }
  .two-col { grid-template-columns: 1fr; }
  .kpi-grid { grid-template-columns: 1fr 1fr; }
}'''


def _parse_json(text):
    text = text.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        text = "\n".join(lines).strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}") + 1
        if start >= 0 and end > start:
            try:
                return json.loads(text[start:end])
            except json.JSONDecodeError:
                pass
    return None


# --- Main Pipeline ---

def run_pipeline(brand_name, domain, market, analysis_lens, report_id, output_dir):
    """Main pipeline v3 entry point."""
    start_time = time.time()
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(os.path.join(output_dir, "assets"), exist_ok=True)
    try:
        research = run_research(brand_name, domain, market)
        research_save = {k: v for k, v in research.items() if k != "_raw_findings"}
        research_path = os.path.join(output_dir, "research.json")
        with open(research_path, "w") as f:
            json.dump(research_save, f, indent=2, default=str)
        log(f"Research saved to {research_path}")
        report_body = run_report_generation(brand_name, domain, market, research, output_dir)
        report_path = assemble_full_report(brand_name, domain, market, report_body, output_dir)
        elapsed = time.time() - start_time
        log(f"Pipeline complete in {elapsed:.0f}s: {report_path}")
        return report_path
    except Exception as e:
        log(f"Pipeline failed: {e}")
        traceback.print_exc()
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="BlazingHill Report Engine v3")
    parser.add_argument("--brand", required=True)
    parser.add_argument("--domain", required=True)
    parser.add_argument("--market", default="United States")
    parser.add_argument("--lens", default="Commercial diligence")
    parser.add_argument("--report-id", required=True)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()
    run_pipeline(args.brand, args.domain, args.market, args.lens, args.report_id, args.output_dir)
