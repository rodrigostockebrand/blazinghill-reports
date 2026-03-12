#!/usr/bin/env python3
"""
Web Research Collector
Uses Perplexity API (or OpenAI-compatible) to gather comprehensive company intelligence.
Runs multiple targeted research queries in parallel for speed.
"""

import os
import json
import time
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

# Perplexity API (OpenAI-compatible endpoint)
PERPLEXITY_API_KEY = os.environ.get("PERPLEXITY_API_KEY", "")
PERPLEXITY_BASE = "https://api.perplexity.ai"

# Fallback: OpenAI API
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
OPENAI_BASE = "https://api.openai.com/v1"


def _call_llm(system_prompt, user_prompt, max_tokens=4000):
    """Call Perplexity API (with web search) or fall back to OpenAI."""
    # Try Perplexity first (has built-in web search)
    if PERPLEXITY_API_KEY:
        try:
            resp = requests.post(
                f"{PERPLEXITY_BASE}/chat/completions",
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
                    "temperature": 0.1,
                    "return_citations": True
                },
                timeout=120
            )
            resp.raise_for_status()
            data = resp.json()
            content = data["choices"][0]["message"]["content"]
            citations = data.get("citations", [])
            return {"content": content, "citations": citations}
        except Exception as e:
            print(f"  [web_research] Perplexity API error: {e}")

    # Fallback to OpenAI (no web search, but can analyze provided context)
    if OPENAI_API_KEY:
        try:
            resp = requests.post(
                f"{OPENAI_BASE}/chat/completions",
                headers={
                    "Authorization": f"Bearer {OPENAI_API_KEY}",
                    "Content-Type": "application/json"
                },
                json={
                    "model": "gpt-4o",
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt}
                    ],
                    "max_tokens": max_tokens,
                    "temperature": 0.1
                },
                timeout=120
            )
            resp.raise_for_status()
            data = resp.json()
            content = data["choices"][0]["message"]["content"]
            return {"content": content, "citations": []}
        except Exception as e:
            print(f"  [web_research] OpenAI API error: {e}")

    return {"content": "", "citations": []}


SYSTEM_PROMPT = """You are a senior private equity analyst conducting commercial due diligence.
Return ONLY valid JSON. No markdown, no code fences, no explanations outside the JSON.
Every data point must include a source URL where the data can be verified.
If you cannot find a verified data point, use null and explain in a "notes" field.
Be precise with numbers — no rounding unless stated. Include currency symbols."""


def _research_company_profile(brand, domain, market):
    """Gather company fundamentals."""
    prompt = f"""Research {brand} ({domain}) and return JSON with these fields:
{{
  "company_name": "legal entity name",
  "brand_name": "{brand}",
  "domain": "{domain}",
  "founded_year": number or null,
  "headquarters": "city, country",
  "founders": ["name1", "name2"],
  "business_model": "DTC / B2B / Marketplace / etc",
  "product_categories": ["category1", "category2"],
  "price_range": "e.g. $29-$149",
  "employee_count": number or null,
  "employee_count_range": "e.g. 50-100",
  "key_markets": ["market1", "market2"],
  "customer_base_size": "e.g. 2M+",
  "physical_stores": number or null,
  "notable_investors": ["investor1"],
  "funding_history": [
    {{"round": "Series A", "amount": "$XM", "date": "YYYY", "lead": "investor", "source_url": "url"}}
  ],
  "acquisitions": [
    {{"acquirer": "name", "date": "YYYY-MM", "stake": "80%", "value": "$XM", "source_url": "url"}}
  ],
  "key_executives": [
    {{"name": "name", "title": "CEO", "background": "brief"}}
  ],
  "unique_selling_propositions": ["usp1", "usp2"],
  "brand_positioning": "description",
  "primary_market": "{market}",
  "sources": [{{"fact": "description", "url": "source_url"}}]
}}"""
    return _call_llm(SYSTEM_PROMPT, prompt, max_tokens=3000)


def _research_financials(brand, domain, market):
    """Gather financial data."""
    prompt = f"""Research the financials of {brand} ({domain}) and return JSON:
{{
  "revenue_history": [
    {{"year": 2024, "revenue": "€28.3M", "currency": "EUR", "growth_yoy": "40%", "source_url": "url"}}
  ],
  "latest_revenue": {{"year": 2024, "amount": "€28.3M", "source_url": "url"}},
  "ebitda": {{"amount": null, "margin_pct": null, "year": null, "source_url": null}},
  "gross_margin_est": "60-65%",
  "net_income": null,
  "cac_estimate": {{"paid": null, "blended": null, "source_url": null}},
  "ltv_estimate": {{"value": null, "timeframe": "3yr", "source_url": null}},
  "aov_estimate": {{"value": null, "currency": null, "source_url": null}},
  "repeat_purchase_rate": null,
  "marketing_spend_pct_revenue": null,
  "revenue_channels": {{"dtc_pct": null, "wholesale_pct": null, "marketplace_pct": null}},
  "geographic_revenue_split": [{{"region": "Europe", "pct": null}}],
  "seasonality_notes": "description of seasonal patterns",
  "valuation_multiples": {{"ev_revenue": null, "ev_ebitda": null, "source_url": null}},
  "deal_value": null,
  "sources": [{{"fact": "description", "url": "source_url"}}]
}}
Search broadly. Include estimates from industry reports if exact numbers are unavailable. Market: {market}."""
    return _call_llm(SYSTEM_PROMPT, prompt, max_tokens=3000)


def _research_competitors(brand, domain, market):
    """Identify and analyze competitors."""
    prompt = f"""Research the competitive landscape for {brand} ({domain}) in the {market} market.
Return JSON:
{{
  "direct_competitors": [
    {{
      "name": "competitor name",
      "domain": "competitor.com",
      "estimated_revenue": "$XM",
      "price_range": "$X-$Y",
      "key_differentiator": "description",
      "market_position": "leader/challenger/niche",
      "funding_total": "$XM",
      "source_url": "url"
    }}
  ],
  "indirect_competitors": [
    {{"name": "name", "domain": "domain.com", "overlap": "description"}}
  ],
  "market_size": {{"tam": "$XB", "sam": "$XB", "som": "$XM", "source_url": "url"}},
  "market_growth_rate": "X% CAGR",
  "industry_trends": ["trend1", "trend2"],
  "competitive_advantages": ["advantage1"],
  "competitive_weaknesses": ["weakness1"],
  "ma_comparables": [
    {{
      "target": "company name",
      "acquirer": "buyer name",
      "year": 2024,
      "ev": "$XM",
      "ev_revenue": "X.Xx",
      "ev_ebitda": "X.Xx",
      "source_url": "url"
    }}
  ],
  "sources": [{{"fact": "description", "url": "source_url"}}]
}}
Include at least 5 direct competitors and 3 M&A comparables in the same industry."""
    return _call_llm(SYSTEM_PROMPT, prompt, max_tokens=4000)


def _research_social_media(brand, domain, market):
    """Gather social media presence data."""
    prompt = f"""Research the social media presence of {brand} ({domain}).
Return JSON:
{{
  "instagram": {{
    "handle": "@handle",
    "followers": number,
    "engagement_rate": "X.X%",
    "avg_likes_per_post": number,
    "avg_comments_per_post": number,
    "posting_frequency": "X posts/week",
    "top_content_types": ["reels", "carousel"],
    "growth_trend": "growing/stable/declining",
    "source_url": "url"
  }},
  "tiktok": {{
    "handle": "@handle",
    "followers": number,
    "avg_views": number,
    "source_url": "url"
  }},
  "facebook": {{
    "page_name": "name",
    "followers": number,
    "source_url": "url"
  }},
  "youtube": {{
    "channel": "name",
    "subscribers": number,
    "source_url": "url"
  }},
  "twitter": {{
    "handle": "@handle",
    "followers": number,
    "source_url": "url"
  }},
  "pinterest": {{
    "handle": "name",
    "followers": number,
    "source_url": "url"
  }},
  "ugc_program": {{"exists": true, "description": "details"}},
  "influencer_partnerships": ["partner1", "partner2"],
  "social_commerce_enabled": true,
  "total_social_reach": number,
  "sources": [{{"fact": "description", "url": "source_url"}}]
}}
Use actual verified numbers. If a platform presence doesn't exist, set to null."""
    return _call_llm(SYSTEM_PROMPT, prompt, max_tokens=3000)


def _research_sentiment(brand, domain, market):
    """Gather customer sentiment and reviews."""
    prompt = f"""Research customer reviews and sentiment for {brand} ({domain}).
Check Trustpilot, Google Reviews, Reddit, and other review platforms.
Return JSON:
{{
  "trustpilot": {{
    "rating": 4.2,
    "total_reviews": 5000,
    "score_distribution": {{"5star": 60, "4star": 20, "3star": 10, "2star": 5, "1star": 5}},
    "source_url": "url"
  }},
  "google_reviews": {{
    "rating": null,
    "total_reviews": null,
    "source_url": null
  }},
  "praise_themes": [
    {{"theme": "Design quality", "frequency": "very common", "example_quote": "actual review quote", "source_url": "url"}}
  ],
  "complaint_themes": [
    {{"theme": "Sizing issues", "frequency": "common", "example_quote": "actual review quote", "source_url": "url"}}
  ],
  "nps_estimate": {{"score": null, "basis": "explanation"}},
  "brand_perception": "description of overall brand sentiment",
  "return_rate_indicators": "any mentions of returns/refunds",
  "customer_service_rating": null,
  "sources": [{{"fact": "description", "url": "source_url"}}]
}}"""
    return _call_llm(SYSTEM_PROMPT, prompt, max_tokens=3000)


def _research_industry_context(brand, domain, market):
    """Gather broader industry and regulatory context."""
    prompt = f"""Research the industry context for {brand} ({domain}) operating in {market}.
Return JSON:
{{
  "industry": "industry name",
  "sub_industry": "sub-category",
  "regulatory_environment": {{
    "gdpr_applicable": true,
    "product_safety_regs": ["regulation1"],
    "import_duties": "notes on tariffs/duties",
    "key_regulatory_risks": ["risk1"]
  }},
  "supply_chain": {{
    "manufacturing_model": "own factory / contract / mixed",
    "manufacturing_locations": ["country1"],
    "logistics_model": "3PL / in-house",
    "fulfillment_centers": ["location1"]
  }},
  "industry_trends": [
    {{"trend": "description", "impact": "positive/negative/neutral", "source_url": "url"}}
  ],
  "disruption_threats": [
    {{"threat": "description", "probability": "high/medium/low", "source_url": "url"}}
  ],
  "ip_and_trademarks": {{
    "registered_trademarks": null,
    "patents": null,
    "domain_portfolio": null
  }},
  "sources": [{{"fact": "description", "url": "source_url"}}]
}}"""
    return _call_llm(SYSTEM_PROMPT, prompt, max_tokens=3000)


def collect_web_research(brand_name, domain, market):
    """
    Run all web research queries in parallel and merge results.
    Returns a dict with keys: company_profile, financials, competitors, social_media, sentiment, industry_context
    """
    results = {}
    research_tasks = {
        "company_profile": (_research_company_profile, (brand_name, domain, market)),
        "financials": (_research_financials, (brand_name, domain, market)),
        "competitors": (_research_competitors, (brand_name, domain, market)),
        "social_media": (_research_social_media, (brand_name, domain, market)),
        "sentiment": (_research_sentiment, (brand_name, domain, market)),
        "industry_context": (_research_industry_context, (brand_name, domain, market)),
    }

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {}
        for key, (func, args) in research_tasks.items():
            futures[executor.submit(func, *args)] = key

        for future in as_completed(futures):
            key = futures[future]
            try:
                raw = future.result()
                content = raw.get("content", "")
                citations = raw.get("citations", [])

                # Try to parse JSON from the content
                parsed = _parse_json_from_text(content)
                if parsed:
                    parsed["_citations"] = citations
                    results[key] = parsed
                else:
                    results[key] = {
                        "_raw": content,
                        "_citations": citations,
                        "_parse_error": True
                    }
                print(f"  [web_research] ✓ {key} collected ({len(content)} chars)")
            except Exception as e:
                print(f"  [web_research] ✗ {key} failed: {e}")
                results[key] = {"error": str(e)}

    return results


def _parse_json_from_text(text):
    """Extract JSON from LLM response text, handling markdown code fences."""
    text = text.strip()

    # Remove markdown code fences
    if text.startswith("```"):
        lines = text.split("\n")
        # Remove first line (```json) and last line (```)
        lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        text = "\n".join(lines).strip()

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        # Try to find JSON object in the text
        start = text.find("{")
        end = text.rfind("}") + 1
        if start >= 0 and end > start:
            try:
                return json.loads(text[start:end])
            except json.JSONDecodeError:
                pass
    return None
