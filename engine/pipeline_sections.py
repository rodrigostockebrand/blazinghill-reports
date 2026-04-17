#!/usr/bin/env python3
"""
BlazingHill Report Engine v3.2 — Section Definitions & Report Generation Module
Section list, batch definitions, GPT report generation.
"""
from pipeline_utils import *
from pipeline_validation import _parse_json, _fetch_trustpilot_data, _check_revenue_staleness, _validate_and_clean_sources
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
    ("s52", "52", "Data Integrity"),
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

    default = f"Write section {num}: {title} with relevant analysis, data, and charts. Only use data from the RESEARCH DATA provided. If specific metrics are unavailable, provide qualitative strategic analysis instead of fabricated numbers."
    base = instructions.get(sid, default)
    preamble = "REMINDER: Use ALL available data from the RESEARCH DATA — it contains real sourced numbers. Create charts from this data. Only use disclosure callouts for metrics genuinely absent from the research JSON. NEVER fabricate numbers not in the data."
    return f"{preamble}\n\n{base}"


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
This report will be reviewed by senior private equity partners evaluating a potential acquisition.

═══════════════════════════════════════════════════════════════════════════════
            WHITELIST-ONLY DATA INTEGRITY PROTOCOL
═══════════════════════════════════════════════════════════════════════════════

This report uses a WHITELIST-ONLY approach. You may ONLY present numerical data
that appears in the RESEARCH DATA JSON below. Think of the RESEARCH DATA as a
closed universe — if a number is not explicitly stated there, it does not exist
for the purposes of this report.

RULE 1 — WHITELIST NUMBERS ONLY:
  Before writing ANY number, mentally verify: "Is this exact figure in the
  RESEARCH DATA JSON?" If YES, use it with a source link. If NO, do not write it.
  There are ZERO exceptions. No estimates, no ranges, no industry averages
  presented as company-specific data.

RULE 2 — FORBIDDEN PATTERNS (automatic disqualification):
  - "(est.)" or "(estimated)" next to any number
  - "approximately" + a specific number not in the data
  - AOV, CAC, LTV, retention rate, NPS, or churn numbers that are NOT in the JSON
  - Tables with sequential/incremental data (e.g. Q1=54, Q2=56, Q3=57) — this is
    the hallmark of fabricated data
  - Revenue channel splits (DTC/Wholesale %) unless explicitly in the JSON
  - Any "projected" or "forecast" company-specific figure not from a source

RULE 3 — EVERY NUMBER GETS A SOURCE LINK:
  Format: <a href="ACTUAL_URL" target="_blank" class="cite">Source Name</a>
  Look up the source ID (PB-1, ST-2, VF-1, VS-1, PPLX-D3) in AVAILABLE SOURCES.
  A number without a source link should not appear in the report.

RULE 4 — WHEN DATA IS MISSING, WRITE STRATEGY NOT NUMBERS:
  For metrics not in the JSON (AOV, CAC, LTV, retention, NPS, channel mix, etc.):
  - Do NOT create tables with fabricated rows
  - Do NOT create charts with fabricated data points
  - Instead write 400+ words of substantive strategic analysis:
    * What this metric means for the investment thesis
    * Industry benchmarks from public sources (cited)
    * Specific management questions to ask in DD
    * Red flags if the metric is worse than expected
    * Upside scenarios if the metric is strong
  - Use a disclosure callout:
    <div class="callout info"><span class="callout-icon">ℹ</span>
    <div><strong>Data not publicly available.</strong> Recommend requesting
    [specific metric] directly from management during DD.</div></div>

RULE 5 — CHARTS FROM SOURCED DATA ONLY:
  These data points ARE in the research and SHOULD be charted:
  - Revenue history (multi-year)
  - Funding rounds and valuations
  - Market size data (global sports apparel)
  - Competitor revenue comparisons
  - Social media follower counts
  - Traffic data (if available)
  Do NOT chart: AOV trends, retention curves, CAC trends, LTV cohorts,
  or any other metric not explicitly in the RESEARCH DATA.

RULE 6 — CHART QUALITY:
  Every chart MUST have:
  - Axis titles (e.g. "Revenue (£M)", "Year", "EV/Revenue Multiple (x)")
  - Data labels on every bar/point/slice showing the actual value
  - A figcaption with "Exhibit NN: [title] | Sources: <a href>Source</a>"
  - Properly closing JSON braces (count them before outputting)

═══════════════════════════════════════════════════════════════════════════════

AVAILABLE SOURCES (use these IDs and URLs for citations):
{source_map}

CHART FORMAT:
<div class="chart-container" data-chart='VALID_JSON_CONFIG'></div>
<figcaption>Exhibit NN: [title] | Sources: <a href="URL" target="_blank" class="cite">Source</a></figcaption>

Chart JSON rules:
- Valid JSON: double-quote all keys and strings, numeric data values only
- Types: bar, line, doughnut, radar, scatter, bubble, polarArea
- Always include: type, data (labels + datasets), options
- options MUST include:
  * responsive: true, maintainAspectRatio: true
  * plugins.legend with display:true, position:"bottom"
  * plugins.datalabels with display:true, anchor:"end", align:"top"
  * For doughnut/pie: datalabels formatter showing percentage
  * scales (bar/line): x and y axis titles via title.display:true, title.text:"Label"
- Colors: ["#2563eb","#16a34a","#d97706","#dc2626","#7c3aed","#06b6d4","#ec4899"]
- Backgrounds: hex with alpha "#2563eb80"
- NEVER use JS expressions — only literal values
- VERIFY brace count: every {{ must have a matching }}

HTML FORMAT classes:
- .stat-row > .stat-label, .stat-value, .stat-note
- .callout.success/.warn/.info/.danger with .callout-icon span
- .two-col for side-by-side layouts
- .scenario-card.bear/.base/.bull inside .scenarios div
- .table-wrap > table with thead/tbody
- .tag.tag-opp (green), .tag.tag-watch (amber), .tag.tag-risk (red)
- .kpi-grid > .kpi-card.kpi-blue/.kpi-green/.kpi-amber/.kpi-red/.kpi-navy
- .metric-bar > .mb-label, .mb-track, .mb-fill
- .key-insight for highlighted analysis
- .thesis-box for investment thesis

OUTPUT RULES:
1. Every number MUST have a <a href> source from AVAILABLE SOURCES — no exceptions.
2. No data = strategic qualitative analysis (400+ words), not fabricated tables.
3. Section IDs: use s01-s51 as provided.
4. Charts: only from RESEARCH DATA. Close all JSON braces correctly.
5. Output clean HTML only — no markdown, no code fences, no explanations."""

    # ── Per-batch generation function ─────────────────────────────────────────
    def generate_batch(batch_num, section_ids):
        batch_label = f"Batch {batch_num} ({', '.join(section_ids)})"
        log(f"  [Gen] Starting {batch_label}...")

        section_prompts = _build_section_prompts(section_ids)

        batch_user = f"""Write the following sections of the {brand_name} PE Due Diligence Report.

RESEARCH DATA (this is your ONLY source of facts — do NOT invent data beyond this):
{research_context}

SECTIONS TO WRITE:
{section_prompts}

ABSOLUTE REQUIREMENTS:
- Write ALL sections listed above, in order.
- Each section: <section class="section" id="sXX"> ... </section>
- If the RESEARCH DATA above does not contain a specific metric, DO NOT fabricate it.
  Instead provide qualitative strategic analysis and a disclosure callout.
- Only create charts when you have REAL data points from the RESEARCH DATA.
- NEVER create tables with fabricated rows — only include rows backed by real data.
- Output ONLY HTML — no preamble, no explanation, no markdown."""

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

    # ── Sequential batch generation (serialized to avoid rate limits) ────────
    log("Phase 2 — Generating 10 batches sequentially (rate limit safe)...")
    ordered_batches = []

    for batch_idx in range(10):
        batch_num = batch_idx + 1
        section_ids = BATCHES[batch_idx]
        log(f"Phase 2 — Batch {batch_num}/10: {', '.join(section_ids)}")
        
        _, html = generate_batch(batch_num, section_ids)
        ordered_batches.append(html)
        
        # Brief pause between batches to stay under rate limits
        if batch_idx < 9:
            time.sleep(5)

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
    ("s52", "52", "Data Integrity"),
]


