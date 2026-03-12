#!/usr/bin/env python3
"""
Section Generator
Uses LLM to generate all 51 report sections from collected data.
Each section gets its own targeted prompt with relevant data context.
"""

import os
import json
import time
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

PERPLEXITY_API_KEY = os.environ.get("PERPLEXITY_API_KEY", "")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")

# Use a non-search model for analysis (cheaper, faster)
ANALYSIS_MODEL = "gpt-4o"

# Section generation is batched: groups of ~5 sections share one LLM call
# to reduce total API calls from 51 to ~10


def _call_analysis_llm(system_prompt, user_prompt, max_tokens=8000):
    """Call LLM for analysis (no web search needed — data already collected)."""
    # Prefer OpenAI for analysis (faster, cheaper for non-search tasks)
    if OPENAI_API_KEY:
        try:
            resp = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {OPENAI_API_KEY}",
                    "Content-Type": "application/json"
                },
                json={
                    "model": ANALYSIS_MODEL,
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt}
                    ],
                    "max_tokens": max_tokens,
                    "temperature": 0.2,
                    "response_format": {"type": "json_object"}
                },
                timeout=180
            )
            resp.raise_for_status()
            data = resp.json()
            return json.loads(data["choices"][0]["message"]["content"])
        except Exception as e:
            print(f"  [section_gen] OpenAI error: {e}")

    # Fallback to Perplexity
    if PERPLEXITY_API_KEY:
        try:
            resp = requests.post(
                "https://api.perplexity.ai/chat/completions",
                headers={
                    "Authorization": f"Bearer {PERPLEXITY_API_KEY}",
                    "Content-Type": "application/json"
                },
                json={
                    "model": "sonar-pro",
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt}
                    ],
                    "max_tokens": max_tokens,
                    "temperature": 0.2
                },
                timeout=180
            )
            resp.raise_for_status()
            data = resp.json()
            content = data["choices"][0]["message"]["content"]
            # Parse JSON
            content = content.strip()
            if content.startswith("```"):
                lines = content.split("\n")
                lines = lines[1:]
                if lines and lines[-1].strip() == "```":
                    lines = lines[:-1]
                content = "\n".join(lines)
            return json.loads(content)
        except Exception as e:
            print(f"  [section_gen] Perplexity error: {e}")

    return {}


SYSTEM_PROMPT = """You are a senior PE due diligence analyst writing a McKinsey-grade report.
Write in a formal, analytical style with specific data points, sourced claims, and PE-relevant metrics.
Return ONLY valid JSON as specified in the user prompt.

CRITICAL RULES:
1. Every numeric claim MUST include a source_url pointing to a real, publicly accessible web page
2. Use exact numbers from the provided data — do NOT round or estimate unless explicitly stated
3. Include PE-specific metrics: EBITDA margins, EV/Revenue, LTV/CAC, payback periods, ROIC
4. Write for senior PE partners — assume financial sophistication
5. Use HTML formatting in text fields: <strong>, <em>, <a href>, <ul><li>, <br>
6. Every section must have substantial analysis (minimum 3 paragraphs or equivalent structured content)
7. Flag data gaps explicitly rather than filling with generic text"""


def _prepare_data_context(collected_data, relevant_keys):
    """Extract relevant subset of collected data for a section batch."""
    context = {}
    web = collected_data.get("web_research", {})
    dforseo = collected_data.get("dataforseo", {})
    ahrefs = collected_data.get("ahrefs", {})

    for key in relevant_keys:
        if key in web:
            context[f"web_{key}"] = _truncate_data(web[key], 3000)
        if key in dforseo:
            context[f"seo_{key}"] = _truncate_data(dforseo[key], 3000)
        if key in ahrefs:
            context[f"ahrefs_{key}"] = _truncate_data(ahrefs[key], 3000)

    # Always include company basics
    for k in ["company_profile", "financials"]:
        if k in web and f"web_{k}" not in context:
            context[f"web_{k}"] = _truncate_data(web[k], 2000)

    return context


def _truncate_data(data, max_chars):
    """Truncate data to fit in prompt context."""
    text = json.dumps(data, default=str)
    if len(text) > max_chars:
        return json.loads(text[:max_chars - 100] + '..."}}')  # try graceful truncation
    return data


def _safe_truncate(data, max_chars):
    """Safely truncate data dict to JSON string within char limit."""
    text = json.dumps(data, default=str, indent=None)
    if len(text) <= max_chars:
        return text
    return text[:max_chars] + "...[truncated]"


# ─── Section batch definitions ───
# Each batch generates multiple related sections in one LLM call

SECTION_BATCHES = [
    {
        "batch_id": "exec_company",
        "sections": ["executive_summary", "company_profile"],
        "data_keys": ["company_profile", "financials", "traffic_estimation", "competitors", "sentiment"],
        "prompt_template": """Using the collected data below, generate content for these report sections.
Brand: {brand_name} | Domain: {domain} | Market: {market}

DATA CONTEXT:
{data_context}

Return JSON with this exact structure:
{{
  "executive_summary": {{
    "section_intro": "2-3 sentence overview paragraph (HTML allowed)",
    "kpi_cards": [
      {{"label": "Deal Value", "value": "€41.5M", "sub": "details", "color": "navy", "source_name": "Source Name", "source_url": "https://..."}}
    ],
    "investment_thesis": "Full investment thesis paragraph (HTML allowed)",
    "risks_opportunities": [
      {{"category": "Meta Dependency", "finding": "description", "type": "risk|opportunity|watch", "priority": "high|medium|low"}}
    ]
  }},
  "company_profile": {{
    "section_intro": "overview paragraph",
    "corporate_fundamentals": [
      {{"label": "Founded", "value": "2014", "note": "Barcelona, Spain"}}
    ],
    "product_portfolio": ["<li><strong>Category</strong> — description</li>"],
    "revenue_timeline": [
      {{"year": "2024", "revenue": "€28.3M", "note": "+40% YoY"}}
    ],
    "transaction_summary": [
      {{"parameter": "Acquirer", "value": "Company Name", "notes": "details"}}
    ],
    "sources": [{{"name": "Source Name", "url": "https://..."}}]
  }}
}}
Generate 6-8 KPI cards for exec summary. Include ALL available financial metrics."""
    },
    {
        "batch_id": "pe_economics",
        "sections": ["pe_economics"],
        "data_keys": ["financials", "competitors", "company_profile"],
        "prompt_template": """Generate PE Economics section content.
Brand: {brand_name} | Domain: {domain} | Market: {market}

DATA CONTEXT:
{data_context}

Return JSON:
{{
  "pe_economics": {{
    "section_intro": "overview paragraph",
    "ebitda_analysis": [
      {{"label": "FY2024 Revenue", "value": "€28.3M", "note": ""}}
    ],
    "unit_economics": {{
      "aov": {{"value": "€XX", "source": "url"}},
      "cac_paid": {{"value": "€XX", "source": "url"}},
      "cac_blended": {{"value": "€XX", "source": "url"}},
      "ltv_3yr": {{"value": "€XX", "source": "url"}},
      "ltv_cac_ratio": {{"value": "X.Xx", "source": "url"}},
      "payback_months": {{"value": "X", "source": "url"}},
      "gross_margin": {{"value": "XX%", "source": "url"}}
    }},
    "ma_comps": [
      {{"target": "Company", "acquirer": "Buyer", "year": 2024, "ev": "$XM", "ev_revenue": "X.Xx", "ev_ebitda": "X.Xx", "source_url": "url"}}
    ],
    "return_scenarios": {{
      "bear": {{"exit_revenue": "€XM", "exit_multiple": "X.Xx", "moic": "X.Xx", "irr": "XX%"}},
      "base": {{"exit_revenue": "€XM", "exit_multiple": "X.Xx", "moic": "X.Xx", "irr": "XX%"}},
      "bull": {{"exit_revenue": "€XM", "exit_multiple": "X.Xx", "moic": "X.Xx", "irr": "XX%"}}
    }},
    "sources": [{{"name": "Source Name", "url": "https://..."}}]
  }}
}}"""
    },
    {
        "batch_id": "digital_competitive",
        "sections": ["digital_marketing", "competitive_intel"],
        "data_keys": ["traffic_estimation", "domain_rank", "ranked_keywords", "competitors", "social_media", "competitors_seo"],
        "prompt_template": """Generate Digital Marketing Performance and Competitive Intelligence sections.
Brand: {brand_name} | Domain: {domain} | Market: {market}

DATA CONTEXT:
{data_context}

Return JSON:
{{
  "digital_marketing": {{
    "section_intro": "overview paragraph",
    "traffic_overview": [{{"label": "Monthly Visits", "value": "XXK", "note": "", "source_url": "url"}}],
    "channel_mix": [{{"channel": "Organic Search", "pct": "XX%", "trend": "growing"}}],
    "geo_distribution": [{{"country": "Spain", "pct": "XX%", "visits": "XXK"}}],
    "funnel_metrics": {{"impressions": "XM", "clicks": "XXK", "ctr": "X.X%", "conversion_rate": "X.X%"}},
    "instagram_metrics": {{"followers": "XXXK", "engagement_rate": "X.X%", "growth": "+XX% YoY"}},
    "mobile_pct": "XX%",
    "sources": [{{"name": "Source Name", "url": "https://..."}}]
  }},
  "competitive_intel": {{
    "section_intro": "overview paragraph",
    "competitor_comparison": [
      {{"name": "Competitor", "domain": "domain.com", "traffic": "XXK/mo", "revenue_est": "$XM", "positioning": "description"}}
    ],
    "radar_dimensions": [
      {{"dimension": "Brand Awareness", "brand_score": 7, "comp1_score": 8, "comp2_score": 6}}
    ],
    "competitive_advantages": ["advantage1"],
    "competitive_gaps": ["gap1"],
    "sources": [{{"name": "Source Name", "url": "https://..."}}]
  }}
}}"""
    },
    {
        "batch_id": "ai_risk",
        "sections": ["ai_innovation", "risk_assessment"],
        "data_keys": ["tech_stack", "traffic_estimation", "competitors", "financials"],
        "prompt_template": """Generate AI & Innovation Assessment and Risk Assessment sections.
Brand: {brand_name} | Domain: {domain} | Market: {market}

DATA CONTEXT:
{data_context}

Return JSON:
{{
  "ai_innovation": {{
    "section_intro": "overview paragraph",
    "overall_score": {{"score": 5, "max": 10, "assessment": "description"}},
    "capabilities": [
      {{"capability": "AR Try-On", "status": "not_implemented", "priority": "high", "impact": "description"}}
    ],
    "transfer_plan": [
      {{"phase": "Phase 1 (0-3 mo)", "actions": ["action1"], "expected_impact": "description"}}
    ],
    "sources": [{{"name": "Source Name", "url": "https://..."}}]
  }},
  "risk_assessment": {{
    "section_intro": "overview paragraph",
    "risk_matrix": [
      {{"risk": "Meta Dependency", "likelihood": "high", "impact": "high", "severity": "critical", "mitigation": "description"}}
    ],
    "channel_dependency": [
      {{"channel": "Meta/Instagram", "revenue_pct": "XX%", "risk_level": "high"}}
    ],
    "seo_risks": [
      {{"risk": "Low organic content", "current_state": "description", "recommendation": "description"}}
    ],
    "sources": [{{"name": "Source Name", "url": "https://..."}}]
  }}
}}"""
    },
    {
        "batch_id": "channel_cohort_tam",
        "sections": ["channel_economics", "cohort_analysis", "tam_sam_som"],
        "data_keys": ["traffic_estimation", "financials", "competitors", "company_profile"],
        "prompt_template": """Generate Channel Economics, Cohort Analysis, and TAM/SAM/SOM sections.
Brand: {brand_name} | Domain: {domain} | Market: {market}

DATA CONTEXT:
{data_context}

Return JSON:
{{
  "channel_economics": {{
    "section_intro": "overview paragraph",
    "roas_by_channel": [{{"channel": "Meta", "roas": "X.Xx", "cpm": "$XX", "trend": "description"}}],
    "meta_cpm_trend": [{{"period": "Q1 2024", "cpm": "$XX", "change": "+X%"}}],
    "sources": [{{"name": "Source Name", "url": "https://..."}}]
  }},
  "cohort_analysis": {{
    "section_intro": "overview paragraph",
    "retention_benchmarks": [{{"period": "Month 1", "retention_pct": "XX%", "industry_avg": "XX%"}}],
    "ltv_components": [{{"component": "First Purchase", "value": "€XX", "pct_of_ltv": "XX%"}}],
    "seasonality": [{{"quarter": "Q1", "index": 0.85, "notes": "Post-holiday dip"}}],
    "sources": [{{"name": "Source Name", "url": "https://..."}}]
  }},
  "tam_sam_som": {{
    "section_intro": "overview paragraph",
    "tam": {{"value": "$XXB", "description": "Global eyewear market", "source_url": "url"}},
    "sam": {{"value": "$XXB", "description": "DTC fashion eyewear, EU + US", "source_url": "url"}},
    "som": {{"value": "$XXM", "description": "Current addressable share", "source_url": "url"}},
    "growth_rate": "XX% CAGR",
    "penetration": "X.X%",
    "sources": [{{"name": "Source Name", "url": "https://..."}}]
  }}
}}"""
    },
    {
        "batch_id": "sentiment_content_value",
        "sections": ["customer_sentiment", "content_strategy", "value_creation"],
        "data_keys": ["sentiment", "trustpilot_reviews", "ranked_keywords", "keyword_suggestions", "financials", "competitors"],
        "prompt_template": """Generate Customer Sentiment, Content Strategy Gap, and Value Creation Roadmap sections.
Brand: {brand_name} | Domain: {domain} | Market: {market}

DATA CONTEXT:
{data_context}

Return JSON:
{{
  "customer_sentiment": {{
    "section_intro": "overview paragraph",
    "aggregate_ratings": [{{"platform": "Trustpilot", "rating": 4.2, "reviews": 5000, "source_url": "url"}}],
    "praise_themes": [{{"theme": "Design", "frequency": "very common", "quote": "actual quote", "source_url": "url"}}],
    "complaint_themes": [{{"theme": "Sizing", "frequency": "common", "quote": "actual quote", "source_url": "url"}}],
    "sources": [{{"name": "Source Name", "url": "https://..."}}]
  }},
  "content_strategy": {{
    "section_intro": "overview paragraph",
    "seo_opportunity": "paragraph describing the gap",
    "high_value_keywords": [{{"keyword": "best sunglasses under $50", "volume": "XXK/mo", "difficulty": "XX", "current_rank": null}}],
    "content_roadmap": [{{"phase": "Month 1-3", "actions": ["action1"], "expected_traffic": "+XXK/mo"}}],
    "sources": [{{"name": "Source Name", "url": "https://..."}}]
  }},
  "value_creation": {{
    "section_intro": "overview paragraph",
    "value_levers": [{{"lever": "India Expansion", "year1_impact": "€XM", "year3_impact": "€XM", "confidence": "high"}}],
    "case_studies": [{{"brand": "MVMT", "acquirer": "Movado", "outcome": "description", "relevance": "description", "source_url": "url"}}],
    "sources": [{{"name": "Source Name", "url": "https://..."}}]
  }}
}}"""
    },
    {
        "batch_id": "pricing_revenue_mgmt",
        "sections": ["pricing_strategy", "revenue_quality", "management"],
        "data_keys": ["company_profile", "financials", "competitors", "traffic_estimation"],
        "prompt_template": """Generate Pricing Strategy, Revenue Quality, and Management sections.
Brand: {brand_name} | Domain: {domain} | Market: {market}

DATA CONTEXT:
{data_context}

Return JSON:
{{
  "pricing_strategy": {{
    "section_intro": "overview paragraph",
    "pricing_tiers": [{{"tier": "Entry", "price_range": "€29-39", "products": "Basic sunglasses"}}],
    "business_model": "DTC-first with BOGO mechanics description",
    "competitive_pricing_map": [{{"brand": "Competitor", "price_range": "€XX-XX", "positioning": "premium/value"}}],
    "maturity_score": {{"score": "X/5", "assessment": "description"}},
    "sources": [{{"name": "Source Name", "url": "https://..."}}]
  }},
  "revenue_quality": {{
    "section_intro": "overview paragraph",
    "revenue_growth": [{{"year": 2024, "revenue": "€28.3M", "growth": "+40%"}}],
    "channel_mix": [{{"channel": "DTC Website", "pct": "XX%", "trend": "stable"}}],
    "geographic_concentration": [{{"region": "Spain", "pct": "XX%"}}],
    "product_mix": [{{"category": "Sunglasses", "pct": "XX%", "margin": "XX%"}}],
    "sources": [{{"name": "Source Name", "url": "https://..."}}]
  }},
  "management": {{
    "section_intro": "overview paragraph",
    "founding_team": [{{"name": "Name", "title": "CEO", "background": "description"}}],
    "company_size": {{"employees": "XX", "structure": "description"}},
    "key_person_risk": "assessment paragraph",
    "sources": [{{"name": "Source Name", "url": "https://..."}}]
  }}
}}"""
    },
    {
        "batch_id": "tech_brand_supply",
        "sections": ["tech_stack", "brand_equity", "supply_chain", "regulatory"],
        "data_keys": ["tech_stack", "sentiment", "social_media", "company_profile", "competitors"],
        "prompt_template": """Generate Technology Stack, Brand Equity, Supply Chain, and Regulatory sections.
Brand: {brand_name} | Domain: {domain} | Market: {market}

DATA CONTEXT:
{data_context}

Return JSON:
{{
  "tech_stack": {{
    "section_intro": "overview paragraph",
    "core_platform": [{{"category": "E-Commerce", "technology": "Shopify", "assessment": "description"}}],
    "payment_infra": [{{"provider": "Stripe", "assessment": "description"}}],
    "tech_gap_analysis": [{{"gap": "No AR try-on", "priority": "high", "recommendation": "description"}}],
    "sources": [{{"name": "Source Name", "url": "https://..."}}]
  }},
  "brand_equity": {{
    "section_intro": "overview paragraph",
    "review_breakdown": [{{"platform": "Trustpilot", "rating": 4.2, "count": 5000}}],
    "positive_themes": [{{"theme": "description", "strength": "high"}}],
    "negative_themes": [{{"theme": "description", "severity": "medium"}}],
    "brand_dimensions": [{{"dimension": "Awareness", "score": 7, "max": 10}}],
    "share_of_voice": {{"brand_pct": "XX%", "top_competitor_pct": "XX%"}},
    "sources": [{{"name": "Source Name", "url": "https://..."}}]
  }},
  "supply_chain": {{
    "section_intro": "overview paragraph",
    "manufacturing_model": "description",
    "synergies": [{{"synergy": "COGS Reduction", "est_impact": "30-40%", "timeline": "12-18 months"}}],
    "sources": [{{"name": "Source Name", "url": "https://..."}}]
  }},
  "regulatory": {{
    "section_intro": "overview paragraph",
    "gdpr": "compliance assessment paragraph",
    "local_regs": [{{"regulation": "description", "risk": "low/medium/high"}}],
    "product_safety": "assessment paragraph",
    "regulatory_timeline": [{{"date": "2025 Q3", "event": "description"}}],
    "sources": [{{"name": "Source Name", "url": "https://..."}}]
  }}
}}"""
    },
    {
        "batch_id": "financial_deep",
        "sections": ["working_capital", "exit_analysis", "geo_expansion", "ltv_model", "cac_payback", "contribution_margin"],
        "data_keys": ["financials", "competitors", "traffic_estimation", "company_profile"],
        "prompt_template": """Generate Working Capital, Exit Analysis, Geo Expansion, LTV Model, CAC Payback, and Contribution Margin sections.
Brand: {brand_name} | Domain: {domain} | Market: {market}

DATA CONTEXT:
{data_context}

Return JSON with these section keys: working_capital, exit_analysis, geo_expansion, ltv_model, cac_payback, contribution_margin.
Each section should have: section_intro (paragraph), relevant structured data arrays/objects, and sources array.
Include PE metrics: cash conversion cycle, FCF build, exit multiples, IRR scenarios, LTV waterfall, CAC by channel, contribution margin bridge.
Make sure every numeric claim has a source_url."""
    },
    {
        "batch_id": "marketing_deep",
        "sections": ["marketing_pl", "rfm_segmentation", "retention", "aov_dynamics", "nps_voc", "customer_journey"],
        "data_keys": ["financials", "traffic_estimation", "sentiment", "social_media"],
        "prompt_template": """Generate Marketing P&L, RFM Segmentation, Retention, AOV Dynamics, NPS/VoC, and Customer Journey sections.
Brand: {brand_name} | Domain: {domain} | Market: {market}

DATA CONTEXT:
{data_context}

Return JSON with these section keys: marketing_pl, rfm_segmentation, retention, aov_dynamics, nps_voc, customer_journey.
Each section should have: section_intro (paragraph), relevant structured data, and sources array.
Include DTC benchmarks, retention curves, AOV uplift levers, NPS waterfall, full-funnel analysis."""
    },
    {
        "batch_id": "seo_paid_email",
        "sections": ["seo_authority", "paid_media", "email_crm", "cro_analysis"],
        "data_keys": ["ranked_keywords", "domain_rank", "backlinks_summary", "referring_domains", "tech_stack", "traffic_estimation"],
        "prompt_template": """Generate SEO Authority, Paid Media, Email/CRM, and CRO Analysis sections.
Brand: {brand_name} | Domain: {domain} | Market: {market}

DATA CONTEXT:
{data_context}

Return JSON with these section keys: seo_authority, paid_media, email_crm, cro_analysis.
Each should have: section_intro, relevant metrics/tables/analysis, and sources array.
Include domain authority, backlink analysis, keyword rankings, paid media ROAS, email maturity assessment, CRO audit."""
    },
    {
        "batch_id": "social_price_disruption",
        "sections": ["social_commerce", "share_of_voice", "price_elasticity", "category_disruption", "cross_border"],
        "data_keys": ["social_media", "competitors", "company_profile", "traffic_estimation", "financials"],
        "prompt_template": """Generate Social Commerce, Share of Voice, Price Elasticity, Category Disruption, and Cross-Border sections.
Brand: {brand_name} | Domain: {domain} | Market: {market}

DATA CONTEXT:
{data_context}

Return JSON with these section keys: social_commerce, share_of_voice, price_elasticity, category_disruption, cross_border.
Each should have: section_intro, relevant analysis/metrics, and sources array."""
    },
    {
        "batch_id": "ip_data_content_martech",
        "sections": ["ip_valuation", "data_asset", "content_library", "martech"],
        "data_keys": ["company_profile", "tech_stack", "social_media", "traffic_estimation"],
        "prompt_template": """Generate IP Valuation, Data Asset, Content Library, and MarTech Stack sections.
Brand: {brand_name} | Domain: {domain} | Market: {market}

DATA CONTEXT:
{data_context}

Return JSON with these section keys: ip_valuation, data_asset, content_library, martech.
Each should have: section_intro, relevant analysis, and sources array."""
    },
    {
        "batch_id": "final_sections",
        "sections": ["hundred_day_plan", "ebitda_bridge", "scenario_analysis", "ic_summary", "ai_readiness", "appendix"],
        "data_keys": ["financials", "traffic_estimation", "tech_stack", "competitors", "sentiment", "company_profile", "ranked_keywords"],
        "prompt_template": """Generate 100-Day Plan, EBITDA Bridge, Scenario Analysis, IC Summary, AI Readiness, and Appendix sections.
Brand: {brand_name} | Domain: {domain} | Market: {market}

DATA CONTEXT:
{data_context}

Return JSON with these section keys: hundred_day_plan, ebitda_bridge, scenario_analysis, ic_summary, ai_readiness, appendix.
For ic_summary include: deal_scorecard (10 dimensions scored 1-10), red_flags, investment_thesis, conditions_precedent, return_summary.
For scenario_analysis include: bull/base/bear with revenue, EBITDA, exit multiple, MOIC, IRR.
For ai_readiness include: schema_structured_data, core_web_vitals, ai_search_optimization, marketing_stack_ai, competitor_ai_benchmarking, critical_gaps.
Each should have: section_intro, relevant structured data, and sources array."""
    },
]


def generate_all_sections(report_context, collected_data):
    """
    Generate all 51 report sections using batched LLM calls.
    Returns a dict mapping section_id → section content dict.
    """
    all_sections = {}
    brand_name = report_context["brand_name"]
    domain = report_context["domain"]
    market = report_context["market"]

    # Flatten collected data for easier access
    flat_data = {}
    for source_key, source_data in collected_data.items():
        if isinstance(source_data, dict):
            flat_data.update(source_data)

    print(f"  [section_gen] Generating sections for {brand_name} in {len(SECTION_BATCHES)} batches...")

    # Process batches with limited parallelism (2 at a time to respect rate limits)
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = {}
        for batch in SECTION_BATCHES:
            # Build data context for this batch
            context_parts = {}
            for key in batch["data_keys"]:
                if key in flat_data:
                    context_parts[key] = flat_data[key]

            data_context_str = _safe_truncate(context_parts, 12000)

            prompt = batch["prompt_template"].format(
                brand_name=brand_name,
                domain=domain,
                market=market,
                data_context=data_context_str
            )

            futures[executor.submit(
                _call_analysis_llm, SYSTEM_PROMPT, prompt, 8000
            )] = batch

        for future in as_completed(futures):
            batch = futures[future]
            try:
                result = future.result()
                if result:
                    for section_id in batch["sections"]:
                        if section_id in result:
                            all_sections[section_id] = result[section_id]
                            print(f"  [section_gen] ✓ {section_id}")
                        else:
                            print(f"  [section_gen] ⚠ {section_id} missing from batch {batch['batch_id']}")
                            all_sections[section_id] = {"section_intro": f"Section data for {section_id} could not be generated.", "sources": []}
                else:
                    print(f"  [section_gen] ✗ Batch {batch['batch_id']} returned empty")
                    for section_id in batch["sections"]:
                        all_sections[section_id] = {"section_intro": "Section generation failed.", "sources": []}
            except Exception as e:
                print(f"  [section_gen] ✗ Batch {batch['batch_id']} error: {e}")
                for section_id in batch["sections"]:
                    all_sections[section_id] = {"section_intro": f"Error generating section: {e}", "sources": []}

    print(f"  [section_gen] Complete: {len(all_sections)} sections generated")
    return all_sections
