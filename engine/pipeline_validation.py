#!/usr/bin/env python3
"""
BlazingHill Report Engine v3.2 — Validation Module
Trustpilot lookup, revenue staleness checking, source validation.
"""
from pipeline_utils import *
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
        "Only include praise/complaint themes if you can cite actual quoted reviews from Trustpilot for this specific company. "
        "If the company does not have a Trustpilot page, or has fewer than 10 reviews, explicitly say so — do not invent themes. "
        "Never fabricate quotes. Never assume the business is e-commerce (shipping, stock, payment) unless you have direct evidence."
    )

    tp_user = f"""Search Trustpilot for {brand_name}.

Primary URL to check: {trustpilot_url}

Search queries to use:
1. {tp_query_1}
2. {tp_query_2}
3. {tp_query_3}

Report ONLY what you can verify:
- Exact star rating (X.X / 5.0) — only if Trustpilot page exists
- Total review count (exact integer) — only if Trustpilot page exists
- Top praise themes: ONLY include if you can quote real reviews from this company's Trustpilot page. Each theme MUST have a real quote from that page.
- Top complaint themes: same rule. Only include if real quotes exist.
- Source URL (should be trustpilot.com/review/{domain})

IMPORTANT:
- If {brand_name} has no Trustpilot page, or has < 10 reviews, return rating=null, reviews=null, and empty theme arrays.
- Do NOT output generic themes like "fast shipping", "out of stock", "payment options" unless those are literal quotes from real reviews of THIS company.
- Many B2B, SaaS, and lead-generation companies (e.g. enterprise software that generates demos via sales reps) have no Trustpilot presence — in that case, return empty results rather than guessing."""

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

Return JSON with this exact structure:
{{
  "rating": 4.2,
  "reviews": 12500,
  "praise_themes": [
    {{"theme": "short theme name", "quote": "verbatim quote from a real review"}}
  ],
  "complaint_themes": [
    {{"theme": "short theme name", "quote": "verbatim quote from a real review"}}
  ],
  "source_url": "{tp_source_url}"
}}

STRICT RULES — follow exactly:
1. rating must be a float (e.g. 4.2), or null if not found.
2. reviews must be an integer, or null if not found.
3. praise_themes and complaint_themes: include ONLY items that have a real verbatim quote from an actual Trustpilot review of THIS company.
4. If the research text does NOT contain verbatim customer quotes for this company, return empty arrays [].
5. DO NOT copy or paraphrase the example themes from the original prompt ("Fast delivery", "Sizing issues", "Delivery delays", "Slow customer service", "Easy returns", "Great quality", etc.). These were illustrative placeholders, not real findings.
6. If the company is B2B / SaaS / lead-gen and shipping/stock/payment themes are clearly not applicable, return empty arrays.
7. Never invent quotes. A theme without a real quote must be dropped.
8. If Trustpilot page not found at all, set rating=null, reviews=null, praise_themes=[], complaint_themes=[]."""

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

