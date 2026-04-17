# CHANGELOG — v5.0

## v5.0: V2 Template with Source Icons, Fact-Check Section, Enhanced API Charts

### Source Citation Icons (`.src-icon` system)
- **pipeline_assembly.py**: Added teal (`#0d9488`) clickable citation badges (`.src-icon`) that replace `[Source: ID]` tags inline
- Each badge shows an "i" icon; hover reveals a tooltip with the source name
- Updated `_linkify_sources()` to produce `<a class="src-icon">` tags instead of plain `[S1]` text links
- Added `--teal` CSS variable to `:root`
- Added `.src-icon`, `.src-icon:hover`, `.src-icon .src-tip` CSS rules

### Data Integrity & Fact-Check Section (Section 52)
- **pipeline_assembly.py**: Added `_build_factcheck_section()` — generates Section 52 from source registry
  - Visual gauge showing Data Integrity Score (0–100)
  - Three tier badges: Confirmed (green), Partially Verified (amber), Estimated (red)
  - Score formula: `(confirmed×1.0 + partial×0.7 + estimated×0.4) / total × 100`
  - Full verification table auditing every source
  - Score methodology section
- **pipeline_sections.py**: Added `("s52", "52", "Data Integrity")` to both `SECTIONS` and `_NAV_SECTIONS`
- Added CSS for `.integrity-gauge`, `.tier-badge`, `.tier-confirmed/.tier-partial/.tier-estimated`, `.discrepancy-box`

### Enhanced API Data Charts (DataForSEO)
- **pipeline_assembly.py**: Added `_build_dataforseo_charts()` — generates up to 3 chart sections from DataForSEO data:
  - Search Volume — top keywords horizontal bar chart
  - Website Traffic Estimation — ETV/organic/paid bar chart
  - Brand Sentiment Analysis — doughnut chart from content analysis
- **pipeline_research.py**: Added Phase 1e DataForSEO integration
  - Imports `collect_dataforseo_data` from `collectors/dataforseo_collector.py`
  - Runs all 10+ DataForSEO API calls in parallel (traffic, keywords, backlinks, tech, reviews, content)
  - Stores results in `research["_dataforseo"]` dict
  - Graceful fallback if credentials not set or API errors
- **run_report.js**: Already had `DATAFORSEO_LOGIN` and `DATAFORSEO_PASSWORD` in ENV_VARS (verified)

### CSS Updates
- Added `letter-spacing: 0.03em` to section headers (`.section h2`)
- Updated stylesheet comment to v5.0
- White background McKinsey professional styling preserved
- Teal as primary accent color via `--teal` variable
- Dark navy sidebar (`--navy: #1a2332`) unchanged

### GPT Prompt Integrity Fixes (from prior session)
- **pipeline_sections.py**: Replaced GPT system prompt with ABSOLUTE DATA INTEGRITY RULES (Rules 1–4)
- **pipeline_sections.py**: Replaced batch user prompt with stricter "ONLY source of facts" directive
- **pipeline_sections.py**: Added universal preamble to `_get_section_instructions()` reminding GPT not to fabricate data

### Deployment
- Added `DEPLOY.md` with Railway deployment instructions
- Documented all required and optional environment variables
- Volume mount instructions for persistent SQLite DB

### Files Modified
- `engine/pipeline_assembly.py` — v2 CSS, src-icon, fact-check section, DataForSEO charts
- `engine/pipeline_sections.py` — Section 52, anti-fabrication prompts
- `engine/pipeline_research.py` — DataForSEO integration
- `DEPLOY.md` — new
- `CHANGELOG-v5.md` — new
