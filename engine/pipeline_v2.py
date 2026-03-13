#!/usr/bin/env python3
"""
BlazingHill Report Engine v2 — Simplified Architecture
Single Perplexity research call → Single GPT-4o report generation call → HTML assembly

Replaces the old 4-phase pipeline (12 batch LLM calls, Python matplotlib charts, etc.)
with a streamlined 3-step process that produces consistent, data-rich reports.
"""

import argparse
import json
import os
import sys
import time
import traceback
import requests
from pathlib import Path

PERPLEXITY_API_KEY = os.environ.get("PERPLEXITY_API_KEY", "")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


# ─── Phase 1: Comprehensive Research via Perplexity ───

RESEARCH_SYSTEM = """You are a senior private equity analyst conducting commercial due diligence.
You must return ONLY valid JSON. No markdown, no code fences, no explanations.

CRITICAL DATA ACCURACY RULES:
1. Every data point must include its source URL where a human analyst can verify it.
2. Be precise with numbers. Include currency symbols. Prefer exact figures over ranges.
3. TODAY'S DATE is {today}. You MUST find the MOST RECENT data available.
   - Revenue: find the latest fiscal year reported. Prioritize 2024/2025 data. Search for recent press releases, annual report announcements, and financial news.
   - Social media followers: search for the CURRENT count. Look at platform analytics sites (e.g., SocialBlade, HypeAuditor) or recent news articles mentioning follower counts.
   - Review ratings (Trustpilot, Google): search for the CURRENT live rating. Check recent reviews and current score.
4. Distinguish between FOUNDING CITY (where the company originally started) and CURRENT HEADQUARTERS (where the main office is today). These are often different.
5. For source URLs: ONLY include URLs you found in your search results. NEVER fabricate or guess URLs. If you have data but no specific URL, set source_url to null but STILL include the data point with the number.
6. If you cannot find a specific data point at all, use null for the value. But TRY HARD to find it — search multiple queries.
7. When you have data from an older year (e.g., 2023 revenue) but cannot find a newer figure, include it with the correct year — do NOT omit it entirely. An older data point is better than no data point."""

RESEARCH_PROMPT = """Conduct exhaustive commercial due diligence research on {brand_name} ({domain}) in the {market} market.

Research ALL of the following areas thoroughly. For each area, find SPECIFIC numbers, dates, and facts with source URLs.

Return a single JSON object with these top-level keys:

{{
  "company": {{
    "legal_name": "...",
    "brand_name": "{brand_name}",
    "domain": "{domain}",
    "founded_year": "year",
    "founded_city": "city where the company was originally founded/started",
    "current_headquarters": "city where the HQ is located today (may differ from founding city)",
    "founders": [{{"name": "...", "title": "...", "background": "..."}}],
    "employee_count": number_or_null,
    "business_model": "DTC / B2B / Marketplace / etc",
    "product_categories": ["cat1", "cat2"],
    "price_range": "$X-$Y",
    "key_markets": ["market1"],
    "unique_selling_points": ["usp1"],
    "brand_positioning": "description",
    "source_urls": ["url1", "url2"]
  }},
  "financials": {{
    "revenue_history": [{{"year": 2025, "revenue": "$XM", "growth_yoy": "X%", "source_url": "url"}}],
    "latest_revenue": {{"year": "MUST be most recent fiscal year available (2024 or 2025 preferred)", "amount": "$XM", "source_url": "url"}},
    "gross_margin": {{"value": "X%", "basis": "reported/estimated", "source_url": "url"}},
    "ebitda": {{"amount": "$XM", "margin": "X%", "source_url": "url_or_null"}},
    "funding_rounds": [{{"round": "Series A", "amount": "$XM", "date": "YYYY", "investors": ["name"], "source_url": "url"}}],
    "acquisitions": [{{"acquirer": "name", "date": "YYYY", "value": "$XM", "stake": "X%", "source_url": "url"}}],
    "aov_estimate": "$X",
    "repeat_purchase_rate": "X%",
    "revenue_channels": {{"dtc_pct": "X%", "wholesale_pct": "X%", "marketplace_pct": "X%"}},
    "geographic_revenue": [{{"region": "name", "pct": "X%"}}],
    "source_urls": ["url1"]
  }},
  "competitors": {{
    "direct": [
      {{
        "name": "Competitor Name",
        "domain": "domain.com",
        "revenue_est": "$XM",
        "price_range": "$X-$Y",
        "differentiator": "description",
        "market_position": "leader/challenger/niche",
        "social_followers": "XK",
        "source_url": "url"
      }}
    ],
    "market_size": {{"tam": "$XB", "sam": "$XB", "som": "$XM", "growth_rate": "X% CAGR", "source_url": "url"}},
    "industry_trends": ["trend1", "trend2"],
    "ma_comparables": [
      {{"target": "company", "acquirer": "buyer", "year": 2024, "value": "$XM", "ev_revenue": "X.Xx", "source_url": "url"}}
    ],
    "source_urls": ["url1"]
  }},
  "digital_marketing": {{
    "monthly_traffic": "XK visits",
    "traffic_trend": "growing/stable/declining",
    "top_channels": [{{"channel": "Organic Search", "pct": "X%"}}],
    "top_countries": [{{"country": "US", "pct": "X%"}}],
    "mobile_pct": "X%",
    "domain_authority": number_or_null,
    "backlinks": "XK",
    "top_keywords": [{{"keyword": "term", "position": number, "volume": "XK/mo"}}],
    "social_media": {{
      "instagram": {{"followers": "XK", "engagement_rate": "X.X%", "handle": "@handle"}},
      "tiktok": {{"followers": "XK", "handle": "@handle"}},
      "facebook": {{"followers": "XK"}},
      "youtube": {{"subscribers": "XK"}},
      "twitter": {{"followers": "XK"}}
    }},
    "tech_stack": ["Shopify", "Google Analytics", "Klaviyo"],
    "source_urls": ["url1"]
  }},
  "customer_sentiment": {{
    "trustpilot": {{"rating": 4.2, "reviews": 5000, "source_url": "url"}},
    "google_reviews": {{"rating": null, "reviews": null}},
    "praise_themes": [{{"theme": "description", "frequency": "very common", "quote": "actual quote"}}],
    "complaint_themes": [{{"theme": "description", "frequency": "common", "quote": "actual quote"}}],
    "nps_estimate": number_or_null,
    "source_urls": ["url1"]
  }},
  "operations": {{
    "manufacturing": "own factory / contract / mixed",
    "manufacturing_locations": ["country1"],
    "logistics": "3PL / in-house",
    "fulfillment_centers": ["location1"],
    "supply_chain_risks": ["risk1"],
    "regulatory": {{
      "gdpr_applicable": true,
      "product_safety": ["regulation1"],
      "key_risks": ["risk1"]
    }},
    "source_urls": ["url1"]
  }}
}}

CRITICAL REQUIREMENTS:
1. Revenue: Find the MOST RECENT fiscal year results (2024 or 2025). Revenue from 2022 or earlier is NOT acceptable for "latest_revenue" — search harder.
2. Include at least 3 years of revenue data. If only older data exists publicly, include it BUT also search for the most recent annual reports, press releases, or news articles for current figures.
3. Social media: Report CURRENT follower counts as of today — not figures from old articles. Check the actual platform pages.
4. Trustpilot/review ratings: Report the CURRENT live rating — not what an article from years ago stated.
5. Founding: Clearly distinguish where the company was FOUNDED vs. where its headquarters are TODAY.
6. Include at least 5 direct competitors with revenue estimates and at least 3 M&A comparables.
7. Every source_url must be a real URL from your search results. NEVER fabricate or guess URLs. If you found a data point but don't have a direct URL, set source_url to null but STILL include the data value.
8. Use the company's native currency for revenue (e.g., £ for UK companies, € for EU companies) and also provide USD equivalent.
9. SEARCH THOROUGHLY. If your first search didn't find revenue, try searching for "[brand] annual revenue 2025", "[brand] financial results", "[brand] annual report". Search multiple ways before giving up."""


def _perplexity_call(system_msg, user_msg, max_tokens=4000):
    """Make a single Perplexity API call and return (content, citations)."""
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


def run_research(brand_name, domain, market):
    """Phase 1: Run comprehensive research via 3 parallel focused Perplexity calls."""
    from datetime import datetime
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import re as _re

    log("Phase 1: Researching via Perplexity (3 parallel focused calls)...")
    today = datetime.now().strftime("%B %d, %Y")

    if not PERPLEXITY_API_KEY:
        raise RuntimeError("PERPLEXITY_API_KEY not set")

    system = RESEARCH_SYSTEM.format(today=today)

    # --- Call 1: Company + Financials (the core DD facts) ---
    prompt_core = f"""Research {brand_name} ({domain}). Today's date is {today}.

Find and return ONLY valid JSON with these fields:
{{
  "company": {{
    "legal_name": "...",
    "brand_name": "{brand_name}",
    "domain": "{domain}",
    "founded_year": "year",
    "founded_city": "city where originally founded/started",
    "current_headquarters": "city where main office is today",
    "founders": [{{"name": "...", "title": "...", "background": "..."}}],
    "employee_count": number_or_null,
    "business_model": "DTC / B2B / Marketplace / etc",
    "product_categories": ["cat1", "cat2"],
    "price_range": "$X-$Y",
    "key_markets": ["market1"],
    "unique_selling_points": ["usp1"],
    "brand_positioning": "description",
    "source_urls": ["url1", "url2"]
  }},
  "financials": {{
    "revenue_history": [{{"year": 2025, "revenue": "amount with currency", "growth_yoy": "X%", "source_url": "url_or_null"}}],
    "latest_revenue": {{"year": 2025, "amount": "amount with currency", "source_url": "url_or_null"}},
    "gross_margin": {{"value": "X%", "basis": "reported/estimated", "source_url": "url_or_null"}},
    "ebitda": {{"amount": "$XM", "margin": "X%", "source_url": "url_or_null"}},
    "funding_rounds": [{{"round": "Series A", "amount": "$XM", "date": "YYYY", "investors": ["name"], "source_url": "url_or_null"}}],
    "aov_estimate": "$X",
    "repeat_purchase_rate": "X%",
    "revenue_channels": {{"dtc_pct": "X%", "wholesale_pct": "X%", "marketplace_pct": "X%"}},
    "geographic_revenue": [{{"region": "name", "pct": "X%"}}],
    "source_urls": ["url1"]
  }},
  "operations": {{
    "manufacturing": "own factory / contract / mixed",
    "manufacturing_locations": ["country1"],
    "logistics": "3PL / in-house",
    "fulfillment_centers": ["location1"],
    "supply_chain_risks": ["risk1"],
    "regulatory": {{"gdpr_applicable": true, "product_safety": ["reg1"], "key_risks": ["risk1"]}},
    "source_urls": ["url1"]
  }}
}}

CRITICAL: Find the MOST RECENT revenue figure. Search for "{brand_name} revenue 2025" and "{brand_name} financial results 2025" and "{brand_name} annual report". Use the company's native currency (e.g., \u00a3 for UK companies). Include at least 3 years of revenue history.
Founding city and HQ city may differ — verify both separately."""

    # --- Call 2: Digital, Social, Reviews (live metrics) ---
    prompt_digital = f"""Research the digital presence and customer sentiment of {brand_name} ({domain}). Today's date is {today}.

Find CURRENT live data — not figures from old articles. Check platform analytics sites.

Return ONLY valid JSON:
{{
  "digital_marketing": {{
    "monthly_traffic": "XM visits",
    "traffic_trend": "growing/stable/declining",
    "top_channels": [{{"channel": "Organic Search", "pct": "X%"}}],
    "top_countries": [{{"country": "US", "pct": "X%"}}],
    "mobile_pct": "X%",
    "domain_authority": number_or_null,
    "backlinks": "XK",
    "top_keywords": [{{"keyword": "term", "position": number, "volume": "XK/mo"}}],
    "social_media": {{
      "instagram": {{"followers": "XM or XK", "engagement_rate": "X.X%", "handle": "@handle"}},
      "tiktok": {{"followers": "XM or XK", "handle": "@handle"}},
      "facebook": {{"followers": "XK"}},
      "youtube": {{"subscribers": "XK"}},
      "twitter": {{"followers": "XK"}}
    }},
    "tech_stack": ["platform1", "platform2"],
    "source_urls": ["url1"]
  }},
  "customer_sentiment": {{
    "trustpilot": {{"rating": number, "reviews": number, "source_url": "url_or_null"}},
    "google_reviews": {{"rating": null, "reviews": null}},
    "praise_themes": [{{"theme": "description", "frequency": "very common", "quote": "actual customer quote"}}],
    "complaint_themes": [{{"theme": "description", "frequency": "common", "quote": "actual customer quote"}}],
    "nps_estimate": number_or_null,
    "source_urls": ["url1"]
  }}
}}

CRITICAL: For Instagram, search "{brand_name} Instagram followers 2026" or check social analytics sites.
For Trustpilot, search "Trustpilot {brand_name}" to find the CURRENT rating — NOT what an old article says.
Report the CURRENT numbers as of today, not historical figures from years ago."""

    # --- Call 3: Competitors + Market (industry context) ---
    prompt_market = f"""Research the competitive landscape and market context for {brand_name} ({domain}) in the {market} market. Today's date is {today}.

Return ONLY valid JSON:
{{
  "competitors": {{
    "direct": [
      {{
        "name": "Competitor Name",
        "domain": "domain.com",
        "revenue_est": "$XM or native currency",
        "price_range": "$X-$Y",
        "differentiator": "description",
        "market_position": "leader/challenger/niche",
        "social_followers": "XK",
        "source_url": "url_or_null"
      }}
    ],
    "market_size": {{"tam": "$XB", "sam": "$XB", "som": "$XM", "growth_rate": "X% CAGR", "source_url": "url_or_null"}},
    "industry_trends": ["trend1", "trend2"],
    "ma_comparables": [
      {{"target": "company", "acquirer": "buyer", "year": 2024, "value": "$XM", "ev_revenue": "X.Xx", "source_url": "url_or_null"}}
    ],
    "source_urls": ["url1"]
  }}
}}

Include at least 5 direct competitors with real revenue estimates and at least 3 recent M&A comparables in the {market} sector."""

    # Run all 3 calls in parallel
    all_citations = []
    results = {}

    def do_call(name, prompt, max_tok):
        log(f"  Research call: {name}...")
        content, cites = _perplexity_call(system, prompt, max_tok)
        log(f"  {name}: {len(content)} chars, {len(cites)} citations")
        return name, content, cites

    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [
            executor.submit(do_call, "core", prompt_core, 4000),
            executor.submit(do_call, "digital", prompt_digital, 4000),
            executor.submit(do_call, "market", prompt_market, 4000),
        ]
        for future in as_completed(futures):
            try:
                name, content, cites = future.result()
                parsed = _parse_json(content)
                if not parsed:
                    json_match = _re.search(r'\{[\s\S]*\}', content)
                    if json_match:
                        try:
                            parsed = json.loads(json_match.group())
                        except:
                            log(f"  WARN: Could not parse {name} response")
                            parsed = {}
                    else:
                        log(f"  WARN: No JSON in {name} response")
                        parsed = {}
                results[name] = parsed
                all_citations.extend(cites)
            except Exception as e:
                log(f"  ERROR in research call: {e}")
                results[name] = {}

    # Merge all results into one research object
    research = {}
    for name, data in results.items():
        for key, value in data.items():
            if key not in research or research[key] is None:
                research[key] = value
            elif isinstance(value, dict) and isinstance(research.get(key), dict):
                # Deep merge: fill in nulls from the new data
                for k, v in value.items():
                    if k not in research[key] or research[key][k] is None:
                        research[key][k] = v

    # Deduplicate citations
    seen = set()
    unique_citations = []
    for c in all_citations:
        if c not in seen:
            seen.add(c)
            unique_citations.append(c)

    research["_citations"] = unique_citations
    log(f"Research complete. Keys: {list(research.keys())}, {len(unique_citations)} unique citations")
    return research


# ─── Phase 2: Generate full report HTML via GPT-4o ───

REPORT_SYSTEM = """You are a McKinsey-grade PE due diligence report writer.
You produce investment-quality HTML report content for senior private equity partners.
This report is being used to make million-dollar investment decisions. Any misinformation could jeopardize the deal.

CRITICAL RULES:
1. Write substantive analysis — minimum 2-3 paragraphs per section with specific data points
2. Every numeric claim must have a source citation as a clickable link
3. Use PE metrics throughout: EBITDA, EV/Revenue, LTV/CAC, payback periods, ROIC, IRR, MOIC
4. Include Chart.js configurations where specified — they must use REAL data from the research
5. Tables must have real data — never empty rows or "N/A" for everything
6. If exact data unavailable, provide industry-benchmark estimates labeled "(Est.)"
7. Competitor names must be REAL companies, never "Comp 1" or "Competitor A"
8. Return ONLY the HTML content — no markdown, no code fences, no ```html wrappers

SOURCE CITATION RULES (CRITICAL — an analyst WILL click every link):
9. ONLY use source URLs provided in the CITATIONS and RESEARCH DATA. NEVER invent or fabricate URLs.
10. If you need to cite something but don't have a URL from the research, write the source name as plain text (not a hyperlink). Example: "(Source: Company annual report)" instead of a made-up link.
11. NEVER create URLs with future dates or URLs you are not 100% certain exist.
12. If the research data has a source_url of null, do NOT make up a URL — cite it as plain text or omit the link.

DATA RECENCY RULES (CRITICAL):
13. Use the MOST RECENT revenue figure from the research. If multiple years are available, highlight the latest one prominently.
14. Founding city and current HQ city may differ — check the research data for both fields and use them correctly.
15. Social media followers and review ratings should reflect current figures from the research, not historical ones."""


def _build_report_prompt(brand_name, domain, market, research_data):
    """Build the mega-prompt for report generation."""

    # Clean research data for prompt (remove internal keys)
    clean_research = {k: v for k, v in research_data.items() if not k.startswith("_")}
    research_json = json.dumps(clean_research, indent=2, default=str)

    # Get citations list
    citations = research_data.get("_citations", [])
    citations_text = "\n".join(f"[{i+1}] {url}" for i, url in enumerate(citations)) if citations else "No citations available"

    return f"""Using the research data below, generate a complete PE due diligence report for {brand_name} ({domain}) in the {market} market.

RESEARCH DATA:
{research_json}

CITATIONS FROM RESEARCH:
{citations_text}

OUTPUT FORMAT:
Return a sequence of HTML section blocks. Each section must follow this exact structure:

<section class="section" id="sXX">
  <div class="section-label">Section XX</div>
  <h2>Section Title</h2>
  <p class="section-intro">Substantive intro paragraph with data points and <a href="SOURCE_URL" target="_blank">source links</a>.</p>

  <h3 class="subsection">Subsection Title</h3>
  <!-- Content: tables, stat-rows, charts, paragraphs, lists -->
</section>

COMPONENT TEMPLATES:

KPI Cards (use in Executive Summary):
<div class="kpi-grid">
  <div class="kpi-card kpi-navy">
    <div class="kpi-label">METRIC NAME</div>
    <div class="kpi-value">$28.3M</div>
    <div class="kpi-sub">+40% YoY growth</div>
    <div class="kpi-source"><a href="URL" target="_blank">Source Name</a></div>
  </div>
</div>

Tables:
<div class="table-wrap">
  <table>
    <thead><tr><th>Column 1</th><th>Column 2</th><th>Source</th></tr></thead>
    <tbody>
      <tr><td>Data</td><td>$10M</td><td><a href="URL" target="_blank">source.com</a></td></tr>
    </tbody>
  </table>
</div>

Stat Rows:
<div class="stat-row"><span class="stat-label">Revenue FY2024</span><span class="stat-value">$28.3M</span><span class="stat-note"><a href="URL" target="_blank">Source</a></span></div>

Risk Tags:
<span class="tag tag-risk">High Risk</span>
<span class="tag tag-opp">Opportunity</span>
<span class="tag tag-watch">Watch</span>

Chart.js (wrap in a <div> with a <canvas>):
<div class="chart-container" style="position:relative;height:350px;margin:24px 0;">
  <canvas id="chartUniqueId"></canvas>
</div>
<script>
new Chart(document.getElementById('chartUniqueId'), {{
  type: 'bar',
  data: {{
    labels: ['2021', '2022', '2023', '2024'],
    datasets: [{{
      label: 'Revenue ($M)',
      data: [15.2, 18.7, 22.1, 28.3],
      backgroundColor: ['#e2e8f0', '#e2e8f0', '#e2e8f0', '#2563eb']
    }}]
  }},
  options: {{
    responsive: true,
    maintainAspectRatio: false,
    plugins: {{ legend: {{ display: false }}, title: {{ display: true, text: 'Revenue Growth Trajectory' }} }}
  }}
}});
</script>
<p class="tiny text-muted">Sources: <a href="URL" target="_blank">source.com</a></p>

Thesis Box (for Investment Thesis):
<div class="thesis-box">Investment thesis paragraph here...</div>

Lists:
<ul class="report-list"><li><strong>Key Point</strong> — Description with data</li></ul>

CHART TYPES TO USE (with real data from research):
- Bar charts: Revenue history, competitor comparison, pricing tiers, budget allocation
- Horizontal bar: Market share, channel mix, geographic distribution
- Line charts: Traffic trends, retention curves, CPM trends
- Doughnut/Pie: Revenue channel mix, geographic concentration, traffic sources
- Radar: Competitive positioning (5-7 dimensions), AI readiness, brand equity dimensions
- Stacked bar: Scenario analysis (bear/base/bull), EBITDA bridge, contribution margin
- Scatter: M&A comparables (EV/Revenue vs Revenue)
- Mixed (bar+line): Revenue with growth rate overlay, AOV trends

IMPORTANT CHART RULES:
- Every chart must use REAL data from the research — never random or placeholder numbers
- Use real competitor names from the research data — never "Comp 1" or "Competitor A"
- Every chart canvas ID must be unique (e.g., chart_revenue, chart_competitors, chart_radar)
- Include source attribution below every chart
- Use these colors: Navy #1a2332, Blue #2563eb, Green #16a34a, Amber #d97706, Red #dc2626, Gray tones for secondary data

GENERATE THESE 50 SECTIONS (in order, numbered 01-50):

01. Executive Summary — KPI cards (6-8), investment thesis box, key risks & opportunities table
02. Company Profile — Corporate fundamentals stat-rows, product portfolio list, revenue timeline table, transaction summary table
03. PE Economics — EBITDA analysis table, unit economics stat-rows (AOV, CAC, LTV, LTV/CAC, payback, gross margin), M&A comparables table, return scenarios (bear/base/bull) table, Chart: EBITDA waterfall
04. Digital Marketing Performance — Traffic overview stat-rows, channel mix table, geo distribution table, marketing funnel metrics, Chart: Traffic channel bar chart
05. Competitive Intelligence — Competitor comparison table (5+ competitors with revenue, traffic, positioning), Chart: Radar chart comparing brand vs top 2-3 competitors on 6 dimensions
06. AI & Innovation Assessment — Overall score stat-row, capability assessment table, AI transfer plan phases, Chart: AI readiness heatmap
07. Risk Assessment — Risk matrix table (risk, likelihood, impact, severity, mitigation), channel dependency analysis, Chart: Risk severity horizontal bars
08. Channel Economics — ROAS by channel table, Meta CPM trend table, Chart: Channel ROI bar chart
09. Cohort Analysis — DTC retention benchmark table, LTV build components, Chart: Retention decay curve (line)
10. TAM / SAM / SOM — Market sizing stat-rows with sources, growth dynamics, Chart: TAM/SAM/SOM nested visualization
11. Customer Sentiment — Aggregate ratings table, praise themes table with quotes, complaint themes with quotes, Chart: Sentiment distribution (stacked bar)
12. Content Strategy Gap — SEO opportunity analysis, high-value keyword table, content roadmap phases
13. Value Creation Roadmap — Value lever table (lever, year 1 impact, year 3 impact, confidence), DTC acquisition case studies table
14. Pricing Strategy & Architecture — Pricing tier table, competitive pricing map table, pricing maturity score, Chart: Pricing architecture comparison (horizontal bar)
15. Revenue Quality & Concentration — Revenue growth table, channel mix, geographic concentration, Chart: Revenue concentration doughnut
16. Management & Organization — Founding team profiles, company structure, key person risk assessment
17. Technology Stack Assessment — Core platform table, payment infrastructure, tech gap analysis table
18. Brand Equity Deep Dive — Review breakdown table, positive/negative themes, brand dimensions, Chart: Brand equity radar
19. Supply Chain & Fulfillment — Manufacturing model, post-acquisition synergies table
20. Regulatory & Compliance — GDPR assessment, local regulations table, product safety, regulatory timeline
21. Working Capital & Cash Dynamics — Cash conversion cycle stat-rows, FCF build analysis, seasonal dynamics
22. Exit Analysis & M&A Comparables — M&A comps table, exit path analysis, Chart: M&A scatter (EV/Revenue vs Revenue)
23. Geographic Expansion Roadmap — Priority markets table, expansion phasing with timeline
24. Marketing-Adjusted LTV Model — LTV scenario table, impact waterfall breakdown, Chart: LTV waterfall
25. CAC Payback & Efficiency — CAC by channel table, organic vs paid comparison, Chart: CAC payback bar chart
26. Contribution Margin Bridge — Margin bridge steps, optimization opportunities, Chart: Contribution margin bridge (waterfall)
27. Marketing P&L & Budget Allocation — Budget allocation table, full-funnel architecture, Chart: Marketing spend pie chart
28. Customer Segmentation & RFM — RFM segment table, LTV amplification strategies
29. Repeat Purchase & Retention — Retention analysis, structural constraints, Chart: Retention curve (line)
30. AOV Dynamics & Uplift Levers — AOV by geography table, uplift roadmap
31. NPS & Voice of Customer — VOC theme decomposition table, NPS estimate analysis
32. Customer Journey & Funnel — Full-funnel stage analysis table, Chart: Conversion funnel (horizontal bars)
33. SEO Authority & Organic Position — Domain authority comparison, keyword gap analysis table, Chart: SEO comparison (bar)
34. Paid Media Performance — Paid media efficiency metrics, ROAS benchmarks
35. Email & CRM Maturity — CRM maturity audit table, email revenue upside analysis
36. CRO Analysis — Conversion audit findings, mobile-first priorities
37. Social Commerce & Influencer ROI — UGC program assessment, influencer scale analysis
38. Share of Voice Analysis — Competitive social footprint table, SOV analysis
39. Price Elasticity & Discounting — Discount dependency assessment, exit roadmap from promotions
40. Category Disruption Threats — Threat matrix table with probability and impact
41. Cross-Border E-Commerce — Localization scorecard table, market entry analysis
42. Brand Trademark & IP Valuation — IP asset inventory, licensing potential
43. First-Party Data Asset — Data valuation analysis, GDPR compliance checklist
44. Content & Creative Library — Content asset inventory, production model, reusability assessment
45. MarTech Stack ROI — Confirmed tech stack table, optimization recommendations
46. 100-Day Post-Close Plan — Phased action plan table (Day 1-30, 31-60, 61-100), budget reallocation
47. EBITDA Bridge — Marketing-driven EBITDA levers table, Chart: EBITDA bridge waterfall
48. Scenario Analysis — Bull/Base/Bear assumptions table, key sensitivity drivers, Chart: Scenario comparison (grouped bar with Revenue, EBITDA, MOIC, IRR for each case)
49. Investment Committee Summary — Deal scorecard (10 dimensions scored 1-10), red flags list, final investment thesis, conditions precedent, return summary, Chart: Deal scorecard radar
50. Appendix — Data sources table, methodology notes

CRITICAL REQUIREMENTS:
- Use ONLY real data from the research. If a data point is unavailable, estimate from industry benchmarks and mark with "(Est.)"
- Include 15-20 Chart.js charts distributed throughout the report where specified above
- Every table must have real, populated rows — never empty tables
- Source URLs must be real and clickable — use citations from the research
- Write for senior PE partners — assume financial sophistication
- Each section must have substantial content (not just one sentence)
- Competitor names must be real companies from the research, never generic labels
"""


# Section definitions for batch splitting
SECTION_DEFS = [
    (1, "Executive Summary", "KPI cards (6-8), investment thesis box, key risks & opportunities table"),
    (2, "Company Profile", "Corporate fundamentals stat-rows, product portfolio list, revenue timeline table, transaction summary table"),
    (3, "PE Economics", "EBITDA analysis table, unit economics stat-rows (AOV, CAC, LTV, LTV/CAC, payback, gross margin), M&A comparables table, return scenarios (bear/base/bull) table, Chart: EBITDA waterfall"),
    (4, "Digital Marketing Performance", "Traffic overview stat-rows, channel mix table, geo distribution table, marketing funnel metrics, Chart: Traffic channel bar chart"),
    (5, "Competitive Intelligence", "Competitor comparison table (5+ competitors with revenue, traffic, positioning), Chart: Radar chart comparing brand vs top 2-3 competitors on 6 dimensions"),
    (6, "AI & Innovation Assessment", "Overall score stat-row, capability assessment table, AI transfer plan phases, Chart: AI readiness heatmap"),
    (7, "Risk Assessment", "Risk matrix table (risk, likelihood, impact, severity, mitigation), channel dependency analysis, Chart: Risk severity horizontal bars"),
    (8, "Channel Economics", "ROAS by channel table, Meta CPM trend table, Chart: Channel ROI bar chart"),
    (9, "Cohort Analysis", "DTC retention benchmark table, LTV build components, Chart: Retention decay curve (line)"),
    (10, "TAM / SAM / SOM", "Market sizing stat-rows with sources, growth dynamics, Chart: TAM/SAM/SOM nested visualization"),
    (11, "Customer Sentiment", "Aggregate ratings table, praise themes table with quotes, complaint themes with quotes, Chart: Sentiment distribution (stacked bar)"),
    (12, "Content Strategy Gap", "SEO opportunity analysis, high-value keyword table, content roadmap phases"),
    (13, "Value Creation Roadmap", "Value lever table (lever, year 1 impact, year 3 impact, confidence), DTC acquisition case studies table"),
    (14, "Pricing Strategy & Architecture", "Pricing tier table, competitive pricing map table, pricing maturity score, Chart: Pricing architecture comparison (horizontal bar)"),
    (15, "Revenue Quality & Concentration", "Revenue growth table, channel mix, geographic concentration, Chart: Revenue concentration doughnut"),
    (16, "Management & Organization", "Founding team profiles, company structure, key person risk assessment"),
    (17, "Technology Stack Assessment", "Core platform table, payment infrastructure, tech gap analysis table"),
    (18, "Brand Equity Deep Dive", "Review breakdown table, positive/negative themes, brand dimensions, Chart: Brand equity radar"),
    (19, "Supply Chain & Fulfillment", "Manufacturing model, post-acquisition synergies table"),
    (20, "Regulatory & Compliance", "GDPR assessment, local regulations table, product safety, regulatory timeline"),
    (21, "Working Capital & Cash Dynamics", "Cash conversion cycle stat-rows, FCF build analysis, seasonal dynamics"),
    (22, "Exit Analysis & M&A Comparables", "M&A comps table, exit path analysis, Chart: M&A scatter (EV/Revenue vs Revenue)"),
    (23, "Geographic Expansion Roadmap", "Priority markets table, expansion phasing with timeline"),
    (24, "Marketing-Adjusted LTV Model", "LTV scenario table, impact waterfall breakdown, Chart: LTV waterfall"),
    (25, "CAC Payback & Efficiency", "CAC by channel table, organic vs paid comparison, Chart: CAC payback bar chart"),
    (26, "Contribution Margin Bridge", "Margin bridge steps, optimization opportunities, Chart: Contribution margin bridge (waterfall)"),
    (27, "Marketing P&L & Budget Allocation", "Budget allocation table, full-funnel architecture, Chart: Marketing spend pie chart"),
    (28, "Customer Segmentation & RFM", "RFM segment table, LTV amplification strategies"),
    (29, "Repeat Purchase & Retention", "Retention analysis, structural constraints, Chart: Retention curve (line)"),
    (30, "AOV Dynamics & Uplift Levers", "AOV by geography table, uplift roadmap"),
    (31, "NPS & Voice of Customer", "VOC theme decomposition table, NPS estimate analysis"),
    (32, "Customer Journey & Funnel", "Full-funnel stage analysis table, Chart: Conversion funnel (horizontal bars)"),
    (33, "SEO Authority & Organic Position", "Domain authority comparison, keyword gap analysis table, Chart: SEO comparison (bar)"),
    (34, "Paid Media Performance", "Paid media efficiency metrics, ROAS benchmarks"),
    (35, "Email & CRM Maturity", "CRM maturity audit table, email revenue upside analysis"),
    (36, "CRO Analysis", "Conversion audit findings, mobile-first priorities"),
    (37, "Social Commerce & Influencer ROI", "UGC program assessment, influencer scale analysis"),
    (38, "Share of Voice Analysis", "Competitive social footprint table, SOV analysis"),
    (39, "Price Elasticity & Discounting", "Discount dependency assessment, exit roadmap from promotions"),
    (40, "Category Disruption Threats", "Threat matrix table with probability and impact"),
    (41, "Cross-Border E-Commerce", "Localization scorecard table, market entry analysis"),
    (42, "Brand Trademark & IP Valuation", "IP asset inventory, licensing potential"),
    (43, "First-Party Data Asset", "Data valuation analysis, GDPR compliance checklist"),
    (44, "Content & Creative Library", "Content asset inventory, production model, reusability assessment"),
    (45, "MarTech Stack ROI", "Confirmed tech stack table, optimization recommendations"),
    (46, "100-Day Post-Close Plan", "Phased action plan table (Day 1-30, 31-60, 61-100), budget reallocation"),
    (47, "EBITDA Bridge", "Marketing-driven EBITDA levers table, Chart: EBITDA bridge waterfall"),
    (48, "Scenario Analysis", "Bull/Base/Bear assumptions table, key sensitivity drivers, Chart: Scenario comparison (grouped bar with Revenue, EBITDA, MOIC, IRR for each case)"),
    (49, "Investment Committee Summary", "Deal scorecard (10 dimensions scored 1-10), red flags list, final investment thesis, conditions precedent, return summary, Chart: Deal scorecard radar"),
    (50, "Appendix", "Data sources table, methodology notes"),
]


def _build_batch_prompt(brand_name, domain, market, research_json, citations_text, start_section, end_section):
    """Build a prompt for a specific batch of sections (e.g., 1-17, 18-34, 35-50)."""
    # Filter section definitions for this batch
    batch_sections = [s for s in SECTION_DEFS if start_section <= s[0] <= end_section]
    section_list = "\n".join(
        f"{num:02d}. {title} — {desc}" for num, title, desc in batch_sections
    )

    return f"""Using the research data below, generate sections {start_section:02d}-{end_section:02d} of a PE due diligence report for {brand_name} ({domain}) in the {market} market.

This is part of a 50-section report. You are generating ONLY sections {start_section:02d} through {end_section:02d}.

RESEARCH DATA:
{research_json}

CITATIONS FROM RESEARCH:
{citations_text}

OUTPUT FORMAT:
Return a sequence of HTML section blocks. Each section must follow this exact structure:

<section class="section" id="sXX">
  <div class="section-label">Section XX</div>
  <h2>Section Title</h2>
  <p class="section-intro">Substantive intro paragraph with data points and <a href="SOURCE_URL" target="_blank">source links</a>.</p>

  <h3 class="subsection">Subsection Title</h3>
  <!-- Content: tables, stat-rows, charts, paragraphs, lists -->
</section>

COMPONENT TEMPLATES:

KPI Cards (use in Executive Summary):
<div class="kpi-grid">
  <div class="kpi-card kpi-navy">
    <div class="kpi-label">METRIC NAME</div>
    <div class="kpi-value">$28.3M</div>
    <div class="kpi-sub">+40% YoY growth</div>
    <div class="kpi-source"><a href="URL" target="_blank">Source Name</a></div>
  </div>
</div>

Tables:
<div class="table-wrap">
  <table>
    <thead><tr><th>Column 1</th><th>Column 2</th><th>Source</th></tr></thead>
    <tbody>
      <tr><td>Data</td><td>$10M</td><td><a href="URL" target="_blank">source.com</a></td></tr>
    </tbody>
  </table>
</div>

Stat Rows:
<div class="stat-row"><span class="stat-label">Revenue FY2024</span><span class="stat-value">$28.3M</span><span class="stat-note"><a href="URL" target="_blank">Source</a></span></div>

Risk Tags:
<span class="tag tag-risk">High Risk</span>
<span class="tag tag-opp">Opportunity</span>
<span class="tag tag-watch">Watch</span>

Chart.js (wrap in a <div> with a <canvas>):
<div class="chart-container" style="position:relative;height:350px;margin:24px 0;">
  <canvas id="chartUniqueId"></canvas>
</div>
<script>
new Chart(document.getElementById('chartUniqueId'), {{
  type: 'bar',
  data: {{
    labels: ['2021', '2022', '2023', '2024'],
    datasets: [{{
      label: 'Revenue ($M)',
      data: [15.2, 18.7, 22.1, 28.3],
      backgroundColor: ['#e2e8f0', '#e2e8f0', '#e2e8f0', '#2563eb']
    }}]
  }},
  options: {{
    responsive: true,
    maintainAspectRatio: false,
    plugins: {{ legend: {{ display: false }}, title: {{ display: true, text: 'Revenue Growth Trajectory' }} }}
  }}
}});
</script>
<p class="tiny text-muted">Sources: <a href="URL" target="_blank">source.com</a></p>

Thesis Box (for Investment Thesis):
<div class="thesis-box">Investment thesis paragraph here...</div>

Lists:
<ul class="report-list"><li><strong>Key Point</strong> — Description with data</li></ul>

CHART TYPES TO USE (with real data from research):
- Bar charts: Revenue history, competitor comparison, pricing tiers, budget allocation
- Horizontal bar: Market share, channel mix, geographic distribution
- Line charts: Traffic trends, retention curves, CPM trends
- Doughnut/Pie: Revenue channel mix, geographic concentration, traffic sources
- Radar: Competitive positioning (5-7 dimensions), AI readiness, brand equity dimensions
- Stacked bar: Scenario analysis (bear/base/bull), EBITDA bridge, contribution margin
- Scatter: M&A comparables (EV/Revenue vs Revenue)
- Mixed (bar+line): Revenue with growth rate overlay, AOV trends

IMPORTANT CHART RULES:
- Every chart must use REAL data from the research — never random or placeholder numbers
- Use real competitor names from the research data — never "Comp 1" or "Competitor A"
- Every chart canvas ID must be unique (e.g., chart_s{start_section:02d}_revenue, chart_s{start_section:02d}_competitors)
- Include source attribution below every chart
- Use these colors: Navy #1a2332, Blue #2563eb, Green #16a34a, Amber #d97706, Red #dc2626, Gray tones for secondary data

GENERATE THESE SECTIONS (in order):

{section_list}

CRITICAL REQUIREMENTS:
- Use ONLY real data from the research. If a data point is unavailable, estimate from industry benchmarks and mark with "(Est.)"
- Include Chart.js charts where specified in the section descriptions above
- Every table must have real, populated rows — never empty tables
- Source URLs must be real and clickable — use citations from the research
- Write for senior PE partners — assume financial sophistication
- Each section must have substantial content (minimum 2-3 paragraphs plus tables/charts)
- Competitor names must be real companies from the research, never generic labels
- Return ONLY the HTML — no markdown, no code fences, no explanations"""


def run_report_generation(brand_name, domain, market, research_data, output_dir):
    """Phase 2: Generate full HTML report body via GPT-4.1 in 3 batches."""
    log("Phase 2: Generating full report via GPT-4.1 (3 batches)...")

    if not OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY not set")

    clean_research = {k: v for k, v in research_data.items() if not k.startswith("_")}
    research_json = json.dumps(clean_research, indent=2, default=str)
    citations = research_data.get("_citations", [])
    citations_text = "\n".join(f"[{i+1}] {url}" for i, url in enumerate(citations)) if citations else ""

    # Split 50 sections into 3 batches
    batches = [
        {"start": 1, "end": 17, "label": "Batch 1/3 (Sections 1-17)"},
        {"start": 18, "end": 34, "label": "Batch 2/3 (Sections 18-34)"},
        {"start": 35, "end": 50, "label": "Batch 3/3 (Sections 35-50)"},
    ]

    all_parts = []
    from concurrent.futures import ThreadPoolExecutor, as_completed

    def generate_batch(batch):
        prompt = _build_batch_prompt(brand_name, domain, market, research_json, citations_text, batch["start"], batch["end"])
        resp = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {OPENAI_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": "gpt-4.1",
                "messages": [
                    {"role": "system", "content": REPORT_SYSTEM},
                    {"role": "user", "content": prompt},
                ],
                "max_tokens": 32000,
                "temperature": 0.15,
            },
            timeout=300,
        )
        if resp.status_code != 200:
            log(f"OpenAI API error for {batch['label']}: {resp.status_code}: {resp.text[:300]}")
            resp.raise_for_status()
        data = resp.json()
        body = data["choices"][0]["message"]["content"].strip()
        # Strip markdown fences
        if body.startswith("```"):
            lines = body.split("\n")
            lines = lines[1:]
            if lines and lines[-1].strip() == "```":
                lines = lines[:-1]
            body = "\n".join(lines)
        finish = data["choices"][0].get("finish_reason", "stop")
        log(f"{batch['label']}: {len(body)} chars, finish_reason={finish}")
        return (batch["start"], body)

    # Run batches in parallel (2 at a time to respect rate limits)
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
                results[batch["start"]] = f'<section class="section" id="s{batch["start"]:02d}"><h2>Section generation failed</h2><p>{str(e)[:200]}</p></section>'

    # Merge in order
    report_body = "\n\n".join(results[k] for k in sorted(results.keys()))
    log(f"Full report: {len(report_body)} characters from {len(results)} batches")
    return report_body


# ─── Phase 3: HTML Assembly ───

def assemble_full_report(brand_name, domain, market, report_body, output_dir):
    """Phase 3: Wrap report body in full HTML with CSS, sidebar, and Chart.js CDN."""
    import re
    from datetime import datetime

    log("Phase 3: Assembling final HTML report...")

    now = datetime.now()

    # Load CSS from sample report
    css_path = os.path.join(os.path.dirname(__file__), '..', 'sample-report', 'style.css')
    if os.path.exists(css_path):
        with open(css_path, 'r', encoding='utf-8') as f:
            css = f.read()
    else:
        css = _get_fallback_css()

    # Extract section titles from the generated body for sidebar
    sections = re.findall(r'<section[^>]*id="s(\d+)"[^>]*>.*?<h2>(.*?)</h2>', report_body, re.DOTALL)
    if not sections:
        # Fallback: generate default sidebar
        sections = [(f"{i+1:02d}", f"Section {i+1:02d}") for i in range(50)]

    # Build sidebar
    sidebar_links = ""
    for num, title in sections:
        # Clean title of any HTML tags
        clean_title = re.sub(r'<[^>]+>', '', title).strip()
        short_title = clean_title[:22] + "…" if len(clean_title) > 22 else clean_title
        active = ' class="active"' if num == "01" else ""
        sidebar_links += f'    <a href="#s{num}"{active}><span class="nav-num">{num}</span>{short_title}</a>\n'

    html = f'''<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <meta name="robots" content="noindex, nofollow">
  <title>{_esc(brand_name)} — PE Marketing Due Diligence Report</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.4/dist/chart.umd.min.js"></script>
  <style>
{css}

/* Chart container styles */
.chart-container {{
  position: relative;
  margin: 24px 0;
  background: #fff;
  border: 1px solid var(--gray-200, #e2e8f0);
  border-radius: 8px;
  padding: 16px;
}}
.chart-container canvas {{
  max-width: 100%;
}}
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
    <h1>{_esc(brand_name)} — PE Marketing Due Diligence</h1>
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
/* Sidebar scroll-spy + active state */
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

  /* Hamburger */
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
    """Simple HTML escape."""
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
.exhibit { margin: 24px 0; }
.exhibit img { border: 1px solid var(--gray-200); border-radius: 8px; }
.exhibit figcaption { font-size: 12px; color: var(--gray-500); margin-top: 8px; }
.thesis-box { background: var(--gray-50); border-left: 4px solid var(--blue); padding: 20px 24px; border-radius: 0 8px 8px 0; font-size: 15px; line-height: 1.8; }
.report-list { padding-left: 20px; }
.report-list li { margin-bottom: 8px; font-size: 14px; }
.callout { padding: 16px 20px; border-radius: 8px; display: flex; gap: 12px; margin: 16px 0; }
.callout.info { background: rgba(37,99,235,0.05); border: 1px solid rgba(37,99,235,0.15); }
.callout-icon { font-size: 18px; }
.two-col { display: grid; grid-template-columns: 1fr 1fr; gap: 32px; }
.tiny { font-size: 11px; }
.text-muted { color: var(--gray-400); }
.mt-sm { margin-top: 8px; }
.mt-md { margin-top: 16px; }
.mt-lg { margin-top: 32px; }
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
    """Parse JSON from LLM response."""
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


# ─── Main Pipeline ───

def run_pipeline(brand_name, domain, market, analysis_lens, report_id, output_dir):
    """Main pipeline v2 entry point."""
    start_time = time.time()
    os.makedirs(output_dir, exist_ok=True)
    assets_dir = os.path.join(output_dir, "assets")
    os.makedirs(assets_dir, exist_ok=True)

    try:
        # Phase 1: Research
        research = run_research(brand_name, domain, market)

        # Save research data
        research_path = os.path.join(output_dir, "research.json")
        with open(research_path, "w") as f:
            json.dump(research, f, indent=2, default=str)
        log(f"Research saved to {research_path}")

        # Phase 2: Generate report HTML body
        report_body = run_report_generation(brand_name, domain, market, research, output_dir)

        # Phase 3: Assemble final HTML
        report_path = assemble_full_report(brand_name, domain, market, report_body, output_dir)

        elapsed = time.time() - start_time
        log(f"Pipeline complete in {elapsed:.0f}s: {report_path}")
        return report_path

    except Exception as e:
        log(f"Pipeline failed: {e}")
        traceback.print_exc()
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="BlazingHill Report Engine v2")
    parser.add_argument("--brand", required=True)
    parser.add_argument("--domain", required=True)
    parser.add_argument("--market", default="United States")
    parser.add_argument("--lens", default="Commercial diligence")
    parser.add_argument("--report-id", required=True)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()

    run_pipeline(args.brand, args.domain, args.market, args.lens, args.report_id, args.output_dir)
