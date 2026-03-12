"""
BlazingHill Report Engine — Configuration
Maps report sections to data requirements and generation order.
"""

# The 51 section groups in the report, in order
# Each section has: id, title, data_deps (what data it needs), chart_ids (exhibits to generate)
REPORT_SECTIONS = [
    {
        "id": "executive_summary",
        "title": "Executive Summary",
        "subsections": ["Investment Thesis", "Key Risks & Opportunities"],
        "data_deps": ["company_profile", "financials", "traffic", "competitors", "sentiment"],
        "charts": []
    },
    {
        "id": "company_profile",
        "title": "Company Profile",
        "subsections": ["Corporate Fundamentals", "Product Portfolio", "Revenue Growth Timeline", "Transaction Summary"],
        "data_deps": ["company_profile", "financials"],
        "charts": ["ex1_revenue"]
    },
    {
        "id": "pe_economics",
        "title": "PE Economics",
        "subsections": ["EBITDA Analysis", "Unit Economics", "M&A Comparables", "Return Scenarios"],
        "data_deps": ["financials", "competitors", "ma_comps"],
        "charts": ["ex2_ebitda", "ex3_unit_economics", "ex4_pe_returns"]
    },
    {
        "id": "digital_marketing",
        "title": "Digital Marketing Performance",
        "subsections": ["Traffic Overview", "Traffic Channel Mix", "Geographic Distribution", "Marketing Funnel", "Instagram Performance"],
        "data_deps": ["traffic", "social_media", "seo_data"],
        "charts": ["ex6_traffic_heatmap", "ex10_geo", "ex11_funnel", "ex12_instagram", "ex13_mobile"]
    },
    {
        "id": "competitive_intel",
        "title": "Competitive Intelligence",
        "subsections": ["Competitor Revenue Comparison", "Competitive Position Assessment"],
        "data_deps": ["competitors", "traffic"],
        "charts": ["ex5_radar", "ex9_social"]
    },
    {
        "id": "ai_innovation",
        "title": "AI & Innovation Assessment",
        "subsections": ["Overall Score", "Capability Transfer Plan"],
        "data_deps": ["tech_stack", "competitors"],
        "charts": ["ex7_ai_heatmap"]
    },
    {
        "id": "risk_assessment",
        "title": "Risk Assessment",
        "subsections": ["Channel Dependency Gauges", "Risk Register"],
        "data_deps": ["traffic", "financials", "competitors"],
        "charts": ["ex8_risk_matrix", "ex15_gauges", "ex14_seo_risk"]
    },
    {
        "id": "channel_economics",
        "title": "Channel Economics",
        "subsections": ["ROAS by Channel", "Meta CPM Trend Analysis"],
        "data_deps": ["traffic", "seo_data"],
        "charts": ["ex19_meta_cpm", "ex20_channel_roi"]
    },
    {
        "id": "cohort_analysis",
        "title": "Cohort Analysis",
        "subsections": ["DTC Retention Benchmarks", "LTV Build Components"],
        "data_deps": ["financials", "traffic"],
        "charts": ["ex17_cohort_decay", "ex18_seasonality"]
    },
    {
        "id": "tam_sam_som",
        "title": "TAM / SAM / SOM",
        "subsections": ["Market Growth Dynamics", "Current Penetration"],
        "data_deps": ["company_profile", "financials", "competitors"],
        "charts": ["ex21_tam_sam_som"]
    },
    {
        "id": "customer_sentiment",
        "title": "Customer Sentiment",
        "subsections": ["Aggregate Ratings", "Top Praise Themes", "Top Complaint Themes"],
        "data_deps": ["sentiment"],
        "charts": ["ex22_sentiment"]
    },
    {
        "id": "content_strategy",
        "title": "Content Strategy Gap",
        "subsections": ["The SEO Opportunity", "High-Value Uncontested Keywords", "Content Build Roadmap"],
        "data_deps": ["seo_data", "competitors"],
        "charts": ["ex24_cac_trajectory"]
    },
    {
        "id": "value_creation",
        "title": "Value Creation Roadmap",
        "subsections": ["DTC Acquisition Case Studies"],
        "data_deps": ["financials", "competitors", "ma_comps"],
        "charts": ["ex23_value_creation", "ex25_acquisition_outcomes"]
    },
    {
        "id": "pricing_strategy",
        "title": "Pricing Strategy & Architecture",
        "subsections": ["Pricing Tiers", "Core Business Model", "Competitive Pricing Map", "Pricing Maturity Framework"],
        "data_deps": ["company_profile", "competitors"],
        "charts": ["ex28_pricing_architecture"]
    },
    {
        "id": "revenue_quality",
        "title": "Revenue Quality & Concentration",
        "subsections": ["Revenue Growth", "Channel Mix", "Geographic Concentration", "Product Mix & Seasonality", "E-Commerce Benchmarks"],
        "data_deps": ["financials", "traffic"],
        "charts": ["ex29_revenue_quality"]
    },
    {
        "id": "management",
        "title": "Management & Organization",
        "subsections": ["Founding Team", "Company Size & Structure", "Key Person Risk"],
        "data_deps": ["company_profile"],
        "charts": []
    },
    {
        "id": "tech_stack",
        "title": "Technology Stack Assessment",
        "subsections": ["Core Platform", "Payment Infrastructure", "Technology Gap Analysis"],
        "data_deps": ["tech_stack"],
        "charts": ["ex31_tech_stack"]
    },
    {
        "id": "brand_equity",
        "title": "Brand Equity Deep Dive",
        "subsections": ["Review Platform Breakdown", "Positive Themes", "Negative Themes", "Brand Dimensions", "Share of Voice"],
        "data_deps": ["sentiment", "social_media", "competitors"],
        "charts": ["ex32_brand_equity"]
    },
    {
        "id": "supply_chain",
        "title": "Supply Chain & Fulfillment",
        "subsections": ["Manufacturing Model", "Post-Acquisition Synergies"],
        "data_deps": ["company_profile"],
        "charts": ["ex33_supply_chain"]
    },
    {
        "id": "regulatory",
        "title": "Regulatory & Compliance",
        "subsections": ["GDPR", "Local Regulations", "Product Safety", "Regulatory Timeline"],
        "data_deps": ["company_profile"],
        "charts": ["ex34_regulatory"]
    },
    {
        "id": "working_capital",
        "title": "Working Capital & Cash Dynamics",
        "subsections": ["Cash Conversion Cycle", "Free Cash Flow Build", "Seasonal Dynamics"],
        "data_deps": ["financials"],
        "charts": []
    },
    {
        "id": "exit_analysis",
        "title": "Exit Analysis & M&A Comparables",
        "subsections": ["M&A Comps Table", "IPO Trajectories", "Exit Path Analysis"],
        "data_deps": ["financials", "ma_comps", "competitors"],
        "charts": ["ex30_exit_comps"]
    },
    {
        "id": "geo_expansion",
        "title": "Geographic Expansion Roadmap",
        "subsections": ["Priority Markets", "Expansion Phasing"],
        "data_deps": ["company_profile", "traffic"],
        "charts": ["ex35_market_expansion"]
    },
    {
        "id": "ltv_model",
        "title": "Marketing-Adjusted LTV Model",
        "subsections": ["LTV Scenario Analysis", "Impact Waterfall"],
        "data_deps": ["financials", "traffic"],
        "charts": ["ex36_ltv_waterfall"]
    },
    {
        "id": "cac_payback",
        "title": "CAC Payback & Efficiency Matrix",
        "subsections": ["CAC by Channel", "Organic vs Paid CAC"],
        "data_deps": ["traffic", "seo_data"],
        "charts": ["ex37_cac_payback"]
    },
    {
        "id": "contribution_margin",
        "title": "Contribution Margin Bridge",
        "subsections": ["Estimated Contribution Margin Bridge"],
        "data_deps": ["financials"],
        "charts": ["ex38_contribution_margin"]
    },
    {
        "id": "marketing_pl",
        "title": "Marketing P&L & Budget Allocation",
        "subsections": ["Budget Allocation", "Full-Funnel Architecture"],
        "data_deps": ["financials", "traffic"],
        "charts": ["ex39_marketing_pl"]
    },
    {
        "id": "rfm_segmentation",
        "title": "Customer Segmentation & RFM Analysis",
        "subsections": ["RFM Segment Distribution", "LTV Amplification"],
        "data_deps": ["financials", "traffic"],
        "charts": ["ex40_rfm_scatter"]
    },
    {
        "id": "retention",
        "title": "Repeat Purchase & Retention Analysis",
        "subsections": ["Retention Survival Curve", "Structural Constraints"],
        "data_deps": ["financials"],
        "charts": ["ex41_retention_curves"]
    },
    {
        "id": "aov_dynamics",
        "title": "AOV Dynamics & Uplift Levers",
        "subsections": ["AOV by Geography", "AOV Uplift Roadmap"],
        "data_deps": ["financials", "traffic"],
        "charts": ["ex42_aov_breakdown"]
    },
    {
        "id": "nps_voc",
        "title": "NPS & Voice of Customer Analysis",
        "subsections": ["VOC Theme Decomposition"],
        "data_deps": ["sentiment"],
        "charts": ["ex43_nps_waterfall"]
    },
    {
        "id": "customer_journey",
        "title": "Customer Journey Mapping & Funnel Analysis",
        "subsections": ["Full-Funnel Stage Analysis"],
        "data_deps": ["traffic", "seo_data"],
        "charts": ["ex44_journey_funnel"]
    },
    {
        "id": "seo_authority",
        "title": "SEO Authority & Organic Search Position",
        "subsections": ["Competitor SEO Benchmarking", "SEO Gap Analysis"],
        "data_deps": ["seo_data", "competitors"],
        "charts": ["ex45_seo_comparison"]
    },
    {
        "id": "paid_media",
        "title": "Paid Media Performance Assessment",
        "subsections": ["Paid Media Efficiency"],
        "data_deps": ["traffic"],
        "charts": ["ex46_paid_media_radar"]
    },
    {
        "id": "email_crm",
        "title": "Email & CRM Maturity Assessment",
        "subsections": ["CRM Maturity Audit", "Email Revenue Upside"],
        "data_deps": ["tech_stack", "traffic"],
        "charts": ["ex47_email_maturity"]
    },
    {
        "id": "cro_analysis",
        "title": "Conversion Rate Optimisation Analysis",
        "subsections": ["CRO Audit", "Mobile-First Priority"],
        "data_deps": ["traffic", "tech_stack"],
        "charts": []
    },
    {
        "id": "social_commerce",
        "title": "Social Commerce & Influencer ROI",
        "subsections": ["UGC Program", "Influencer Scale", "Viral Mechanics"],
        "data_deps": ["social_media"],
        "charts": ["ex48_influencer_roi"]
    },
    {
        "id": "share_of_voice",
        "title": "Share of Voice Analysis",
        "subsections": ["Competitive Social Footprint", "SOV Velocity"],
        "data_deps": ["social_media", "competitors"],
        "charts": ["ex49_sov_stacked"]
    },
    {
        "id": "price_elasticity",
        "title": "Price Elasticity & Discounting Risk",
        "subsections": ["Dependency Risk Framework", "Exit Roadmap"],
        "data_deps": ["company_profile", "financials"],
        "charts": ["ex50_price_elasticity"]
    },
    {
        "id": "category_disruption",
        "title": "Category Disruption Threats",
        "subsections": ["Competitive Threat Matrix"],
        "data_deps": ["competitors"],
        "charts": ["ex51_disruption_heatmap"]
    },
    {
        "id": "cross_border",
        "title": "Cross-Border E-Commerce Readiness",
        "subsections": ["Localisation Scorecard", "Market Entry Analysis"],
        "data_deps": ["company_profile", "traffic"],
        "charts": []
    },
    {
        "id": "ip_valuation",
        "title": "Brand Trademark & IP Valuation",
        "subsections": ["IP Asset Inventory", "Licensing Potential"],
        "data_deps": ["company_profile"],
        "charts": []
    },
    {
        "id": "data_asset",
        "title": "First-Party Data Asset Valuation",
        "subsections": ["Data Valuation", "GDPR Compliance Checklist"],
        "data_deps": ["traffic", "tech_stack"],
        "charts": ["ex52_data_asset"]
    },
    {
        "id": "content_library",
        "title": "Content & Creative Library Audit",
        "subsections": ["Content Asset Inventory", "Production Model", "Content Reusability"],
        "data_deps": ["social_media", "traffic"],
        "charts": []
    },
    {
        "id": "martech",
        "title": "MarTech Stack ROI",
        "subsections": ["Confirmed MarTech Stack"],
        "data_deps": ["tech_stack"],
        "charts": ["ex53_martech_landscape"]
    },
    {
        "id": "hundred_day_plan",
        "title": "100-Day Post-Close Marketing Plan",
        "subsections": ["Action Plan", "Budget Reallocation Framework"],
        "data_deps": ["financials", "traffic", "seo_data"],
        "charts": ["ex54_100day_timeline"]
    },
    {
        "id": "ebitda_bridge",
        "title": "Marketing-Driven EBITDA Bridge",
        "subsections": ["EBITDA Bridge — Marketing Levers"],
        "data_deps": ["financials"],
        "charts": ["ex55_ebitda_bridge"]
    },
    {
        "id": "scenario_analysis",
        "title": "Scenario Analysis: Bull / Base / Bear",
        "subsections": ["Scenario Assumptions", "Tornado Analysis"],
        "data_deps": ["financials", "traffic"],
        "charts": ["ex56_scenario_tornado"]
    },
    {
        "id": "ic_summary",
        "title": "Investment Committee Summary",
        "subsections": ["Deal Scorecard", "Red Flags", "Investment Thesis", "Conditions Precedent", "Return Summary"],
        "data_deps": ["company_profile", "financials", "traffic", "competitors", "sentiment"],
        "charts": ["ex57_deal_scorecard"]
    },
    {
        "id": "ai_readiness",
        "title": "AI Readiness & Optimization Maturity",
        "subsections": ["Schema.org & Structured Data", "Core Web Vitals", "AI Search Optimization", "Marketing Stack AI", "Competitor AI Benchmarking", "Critical Gaps"],
        "data_deps": ["tech_stack", "seo_data", "competitors"],
        "charts": ["ex58_ai_readiness_radar", "ex60_cwv_gauges", "ex59_ai_maturity_heatmap"]
    },
    {
        "id": "appendix",
        "title": "Appendix",
        "subsections": ["M&A Comparables Scatter", "Data Sources", "Methodology Notes"],
        "data_deps": ["ma_comps"],
        "charts": ["ex16_ma_scatter"]
    }
]

# Data collection tasks — each runs as a parallel job
DATA_COLLECTORS = {
    "company_profile": {
        "description": "Company background, founding team, product portfolio, corporate structure",
        "sources": ["web_research"]
    },
    "financials": {
        "description": "Revenue, EBITDA, margins, funding rounds, valuations, cash flow",
        "sources": ["web_research"]
    },
    "traffic": {
        "description": "Website traffic, channels, geo, mobile split, page metrics",
        "sources": ["dataforseo_traffic", "dataforseo_keywords"]
    },
    "seo_data": {
        "description": "Keyword rankings, backlink profile, domain authority, content gaps",
        "sources": ["dataforseo_keywords", "dataforseo_backlinks", "ahrefs_backlinks"]
    },
    "tech_stack": {
        "description": "CMS, analytics, payment, marketing tools, Core Web Vitals",
        "sources": ["dataforseo_tech"]
    },
    "social_media": {
        "description": "Instagram, TikTok, YouTube, Facebook followers and engagement",
        "sources": ["web_research"]
    },
    "sentiment": {
        "description": "Customer reviews, ratings, NPS estimates, praise/complaint themes",
        "sources": ["dataforseo_reviews", "web_research"]
    },
    "competitors": {
        "description": "Key competitors, their traffic, revenue, positioning",
        "sources": ["web_research", "dataforseo_traffic"]
    },
    "ma_comps": {
        "description": "M&A transactions in the industry, valuations, multiples",
        "sources": ["web_research"]
    }
}
