#!/usr/bin/env python3
"""
BlazingHill Report Engine v3.2 — Research Module
Phase 1: Data collection from premium + Perplexity sources.
"""
from pipeline_utils import *
from pipeline_validation import _parse_json, _validate_and_clean_sources

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


