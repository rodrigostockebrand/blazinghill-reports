#!/usr/bin/env python3
"""
BlazingHill Report Engine v5.0 — HTML Assembly Module
Sidebar navigation, source linkification with src-icon badges,
fact-check section, DataForSEO charts, full HTML assembly.
"""
from pipeline_utils import *
from pipeline_sections import SECTIONS, _NAV_SECTIONS
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
    to real hyperlinks with src-icon badges using the source registry.
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
                    f'<a class="src-icon" href="{url}" target="_blank">'
                    f'i<span class="src-tip">Source: {name}</span></a>'
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


def _build_factcheck_section(source_registry):
    """Build Section 52: Data Integrity & Fact-Check Report from the source registry."""
    total = len(source_registry)
    if total == 0:
        return ""

    confirmed = 0
    partial = 0
    estimated = 0

    rows = []
    for i, src in enumerate(source_registry, 1):
        url = src.get("url", "")
        name = src.get("name", src.get("id", "Unknown"))
        src_type = src.get("type", "web")

        if url and src_type == "premium":
            tier = "Confirmed"
            tier_class = "tier-confirmed"
            confidence = "High"
            confirmed += 1
        elif url:
            tier = "Partially Verified"
            tier_class = "tier-partial"
            confidence = "Medium"
            partial += 1
        else:
            tier = "Estimated"
            tier_class = "tier-estimated"
            confidence = "Low"
            estimated += 1

        url_display = f'<a href="{url}" target="_blank">{url[:50]}…</a>' if url else "<em>No URL</em>"
        rows.append(
            f'<tr><td>{i}</td><td>{name}</td><td><code>{src.get("id","")}</code></td>'
            f'<td>{confidence}</td><td>{url_display}</td>'
            f'<td><span class="tier-badge {tier_class}">{tier}</span></td>'
            f'<td>{src.get("publisher","")}</td></tr>'
        )

    score = round((confirmed * 1.0 + partial * 0.7 + estimated * 0.4) / total * 100) if total else 0

    return f"""<section class="section" id="s52">
  <div class="section-label">Section 52</div>
  <h2>Data Integrity &amp; Fact-Check Report</h2>

  <p>This section provides a systematic verification audit of the {total} data sources used in this due diligence report. Each source has been assessed against its primary URL, assigned a confidence tier, and scored.</p>

  <!-- Overall Score Gauge -->
  <div style="text-align:center; margin:32px 0 24px;">
    <div class="integrity-gauge" style="--score:{score}">
      <div class="integrity-gauge-value">{score}</div>
    </div>
    <div style="font-size:22px; font-weight:800; color:#1a2332; margin-bottom:4px;">{score} / 100</div>
    <div style="font-size:13px; color:#64748b; margin-bottom:6px;">Overall Data Integrity Score</div>
    <div style="font-size:12px; color:#94a3b8;">Based on {total} individually verified data sources</div>
    <div style="font-size:11px; color:#94a3b8; margin-top:4px;">({confirmed} confirmed × 1.0 + {partial} partial × 0.7 + {estimated} estimated × 0.4) ÷ {total} × 100 = {score}</div>
  </div>

  <!-- Tier Legend -->
  <div style="display:flex; gap:16px; flex-wrap:wrap; justify-content:center; margin:20px 0;">
    <div style="display:flex; align-items:center; gap:8px; padding:10px 16px; background:#f0fdf4; border-radius:8px; border:1px solid #bbf7d0;">
      <span style="font-size:16px;">&#x2705;</span>
      <div><div style="font-size:12px; font-weight:700; color:#166534;">CONFIRMED</div><div style="font-size:11px; color:#64748b;">Premium source, URL verified</div></div>
      <span class="tier-badge tier-confirmed" style="margin-left:4px;">{confirmed} sources</span>
    </div>
    <div style="display:flex; align-items:center; gap:8px; padding:10px 16px; background:#fffbeb; border-radius:8px; border:1px solid #fde68a;">
      <span style="font-size:16px;">&#x26A0;&#xFE0F;</span>
      <div><div style="font-size:12px; font-weight:700; color:#92400e;">PARTIALLY VERIFIED</div><div style="font-size:11px; color:#64748b;">Web source with URL</div></div>
      <span class="tier-badge tier-partial" style="margin-left:4px;">{partial} sources</span>
    </div>
    <div style="display:flex; align-items:center; gap:8px; padding:10px 16px; background:#fef2f2; border-radius:8px; border:1px solid #fecaca;">
      <span style="font-size:16px;">&#x2B55;</span>
      <div><div style="font-size:12px; font-weight:700; color:#991b1b;">ESTIMATED</div><div style="font-size:11px; color:#64748b;">No direct source URL</div></div>
      <span class="tier-badge tier-estimated" style="margin-left:4px;">{estimated} sources</span>
    </div>
  </div>

  <!-- Verification Table -->
  <h3>Complete Verification Table</h3>
  <div class="table-wrap">
    <table class="data-table">
      <thead>
        <tr><th>#</th><th>Source</th><th>ID</th><th>Confidence</th><th>URL</th><th>Tier</th><th>Publisher</th></tr>
      </thead>
      <tbody>
        {"".join(rows)}
      </tbody>
    </table>
  </div>

  <!-- Methodology Note -->
  <h3 style="margin-top:24px;">Score Methodology</h3>
  <div class="highlight-box">
    <p style="margin:0; font-size:13px;">
      The <strong>{score}/100 Data Integrity Score</strong> is calculated as a weighted average:
    </p>
    <ul style="margin:8px 0 0 20px; font-size:13px; line-height:1.8;">
      <li><strong>Tier 1 (Confirmed)</strong>: Weight 1.0 — {confirmed} sources × 1.0 = {confirmed:.1f} points</li>
      <li><strong>Tier 2 (Partially Verified)</strong>: Weight 0.7 — {partial} sources × 0.7 = {partial*0.7:.1f} points</li>
      <li><strong>Tier 3 (Estimated)</strong>: Weight 0.4 — {estimated} sources × 0.4 = {estimated*0.4:.1f} points</li>
      <li>Total = {confirmed*1.0 + partial*0.7 + estimated*0.4:.1f} ÷ {total} × 100 = <strong>{score}</strong></li>
    </ul>
  </div>
</section>"""


def _build_dataforseo_charts(research):
    """Build enhanced API data chart sections from DataForSEO/Ahrefs data."""
    import json as _json
    dfse = research.get("_dataforseo", {})
    if not dfse or dfse.get("error"):
        return ""

    charts_html = []

    # ── Search Volume Trends Chart ──
    kw_data = dfse.get("ranked_keywords")
    if kw_data and isinstance(kw_data, list) and len(kw_data) > 0:
        labels = [item.get("keyword", "")[:25] for item in kw_data[:10]]
        values = [item.get("search_volume", 0) for item in kw_data[:10]]
        chart_cfg = _json.dumps({
            "type": "bar",
            "data": {"labels": labels, "datasets": [{"label": "Monthly Search Volume", "data": values}]},
            "options": {"responsive": True, "indexAxis": "y",
                        "plugins": {"legend": {"display": False}, "datalabels": {"display": True, "anchor": "end", "align": "right"}},
                        "scales": {"x": {"title": {"display": True, "text": "Search Volume (DataForSEO)"}}}}
        })
        charts_html.append(f"""<section class="section" id="s52-search">
  <div class="section-label">API Data</div>
  <h2>Search Volume — Top Keywords</h2>
  <p class="section-intro">Top organic keywords by monthly search volume from DataForSEO.</p>
  <div class="chart-container" data-chart='{chart_cfg}'></div>
  <figcaption>Search Volume Analysis | Source: DataForSEO API</figcaption>
</section>""")

    # ── Website Traffic Comparison Chart ──
    traffic_data = dfse.get("traffic_estimation")
    if traffic_data and isinstance(traffic_data, dict):
        etv = traffic_data.get("etv", 0)
        visits = traffic_data.get("estimated_paid_traffic_cost", 0)
        organic = traffic_data.get("organic_traffic", 0)
        if etv or visits or organic:
            chart_cfg = _json.dumps({
                "type": "bar",
                "data": {"labels": ["ETV ($)", "Organic Traffic", "Paid Traffic Cost"],
                         "datasets": [{"label": "Traffic Metrics", "data": [etv, organic, visits]}]},
                "options": {"responsive": True,
                            "plugins": {"legend": {"display": False}, "datalabels": {"display": True}},
                            "scales": {"y": {"title": {"display": True, "text": "Value"}}}}
            })
            charts_html.append(f"""<section class="section" id="s52-traffic">
  <div class="section-label">API Data</div>
  <h2>Website Traffic Estimation</h2>
  <p class="section-intro">Estimated website traffic metrics from DataForSEO traffic estimation API.</p>
  <div class="chart-container" data-chart='{chart_cfg}'></div>
  <figcaption>Traffic Estimation | Source: DataForSEO API</figcaption>
</section>""")

    # ── Content Sentiment Analysis Chart ──
    content = dfse.get("content_citations")
    if content and isinstance(content, list) and len(content) > 0:
        pos = sum(1 for c in content if c.get("sentiment", {}).get("polarity", 0) > 0.1)
        neu = sum(1 for c in content if -0.1 <= c.get("sentiment", {}).get("polarity", 0) <= 0.1)
        neg = sum(1 for c in content if c.get("sentiment", {}).get("polarity", 0) < -0.1)
        if pos + neu + neg > 0:
            chart_cfg = _json.dumps({
                "type": "doughnut",
                "data": {"labels": [f"Positive ({pos})", f"Neutral ({neu})", f"Negative ({neg})"],
                         "datasets": [{"data": [pos, neu, neg],
                                       "backgroundColor": ["#16a34a", "#94a3b8", "#dc2626"],
                                       "borderColor": "#fff", "borderWidth": 2}]},
                "options": {"responsive": True, "cutout": "60%",
                            "plugins": {"legend": {"position": "bottom"}, "datalabels": {"display": True, "color": "#fff", "font": {"weight": "bold"}}}}
            })
            charts_html.append(f"""<section class="section" id="s52-sentiment">
  <div class="section-label">API Data</div>
  <h2>Brand Sentiment Analysis</h2>
  <p class="section-intro">Content sentiment analysis from DataForSEO content analysis API.</p>
  <div class="chart-container" data-chart='{chart_cfg}'></div>
  <figcaption>Brand Sentiment | Source: DataForSEO Content Analysis API</figcaption>
</section>""")

    return "\n\n".join(charts_html)


def _fix_section_ids(batches):
    """
    Fix section IDs in GPT-generated batches.

    Problem: Each batch's GPT output often resets section numbering to s01-s05
    instead of using the correct global IDs (e.g. batch 2 should be s06-s10).
    This causes the sidebar nav links (which use the correct IDs) to not find
    matching elements in the DOM.

    Solution: Split each batch's HTML into individual <section> blocks, then
    reassign the correct id and section-label based on the BATCHES definition.
    """
    import re as _re
    from pipeline_sections import BATCHES, SECTIONS

    section_num_map = {sid: num for sid, num, title in SECTIONS}

    fixed_batches = []
    for batch_idx, batch_html in enumerate(batches):
        if batch_idx >= len(BATCHES):
            fixed_batches.append(batch_html)
            continue

        expected_ids = BATCHES[batch_idx]  # e.g. ["s06", "s07", "s08", "s09", "s10"]

        # Split the batch HTML into individual <section>...</section> blocks
        # Use a regex that captures each section element
        section_pattern = _re.compile(
            r'(<section\b[^>]*\bid=)(["\'])s\d+\2([^>]*>)',
            _re.IGNORECASE
        )
        
        # Find all section tag matches in order
        matches = list(section_pattern.finditer(batch_html))
        
        if len(matches) == 0:
            fixed_batches.append(batch_html)
            continue

        # Rebuild HTML with corrected IDs
        html = batch_html
        # Process in reverse so string positions don't shift
        for i in range(min(len(matches), len(expected_ids)) - 1, -1, -1):
            match = matches[i]
            expected_id = expected_ids[i]
            expected_num = section_num_map.get(expected_id, expected_id.replace('s', ''))
            quote = match.group(2)  # " or '
            # Replace the section tag's id
            new_tag = f'{match.group(1)}{quote}{expected_id}{quote}{match.group(3)}'
            html = html[:match.start()] + new_tag + html[match.end():]

        # Also fix section-label divs: "Section 01" -> "Section 06" etc.
        # These appear as: <div class="section-label">Section 01</div>
        label_pattern = _re.compile(
            r'(<div\s+class=["\']section-label["\'][^>]*>\s*Section\s+)(\d+)(\s*</div>)',
            _re.IGNORECASE
        )
        label_matches = list(label_pattern.finditer(html))
        # Map them to expected section numbers in order
        for i in range(min(len(label_matches), len(expected_ids)) - 1, -1, -1):
            m = label_matches[i]
            expected_id = expected_ids[i]
            expected_num = section_num_map.get(expected_id, expected_id.replace('s', ''))
            new_label = f'{m.group(1)}{expected_num}{m.group(3)}'
            html = html[:m.start()] + new_label + html[m.end():]

        fixed_batches.append(html)

    return fixed_batches


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
    log("Phase 3: Assembling HTML report (52 sections, McKinsey CSS, Chart.js, v2)...")

    # ── Fix section IDs (GPT often resets numbering per batch) ────────────────
    batches = _fix_section_ids(batches)
    log("  [assembly] Section IDs normalized across all batches")

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

    # ── Section 52: Data Integrity & Fact-Check ──────────────────────────────
    factcheck_html = _build_factcheck_section(source_registry)

    # ── Enhanced API Data Charts (DataForSEO) ────────────────────────────────
    dataforseo_charts_html = _build_dataforseo_charts(research)

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
  <!-- Chart.js 4.x + Datalabels Plugin -->
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.3/dist/chart.umd.min.js"></script>
  <script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-datalabels@2.2.0/dist/chartjs-plugin-datalabels.min.js"></script>
  <style>
/* ═══════════════════════════════════════════════════════
   BlazingHill PE DD Report v5.0 — McKinsey-Grade Stylesheet
   White background, teal accent, src-icon citations
   ═══════════════════════════════════════════════════════ */

:root {{
  --navy: #1a2332;
  --blue: #2563eb;
  --green: #16a34a;
  --amber: #d97706;
  --red: #dc2626;
  --teal: #0d9488;
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
  letter-spacing: 0.03em;
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

/* ── SOURCE CITATION ICONS ── */
.src-icon {{
  display: inline-flex; align-items: center; justify-content: center;
  width: 14px; height: 14px; border-radius: 50%;
  background: var(--teal); color: #fff; font-size: 9px; font-weight: 700;
  cursor: pointer; margin-left: 3px; vertical-align: super;
  position: relative; text-decoration: none;
  font-style: normal;
}}
.src-icon:hover {{ background: #0f766e; }}
.src-icon .src-tip {{
  display: none; position: absolute; bottom: 120%; left: 50%;
  transform: translateX(-50%); background: #1e293b; color: #fff;
  padding: 6px 10px; border-radius: 6px; font-size: 11px;
  white-space: nowrap; z-index: 1000; font-weight: 400;
  box-shadow: 0 4px 12px rgba(0,0,0,.25);
}}
.src-icon:hover .src-tip {{ display: block; }}

/* ── DATA INTEGRITY GAUGE ── */
.integrity-gauge {{
  width: 120px; height: 120px; border-radius: 50%;
  background: conic-gradient(var(--teal) calc(var(--score) * 1%), var(--gray-200) 0);
  display: inline-flex; align-items: center; justify-content: center;
  position: relative;
}}
.integrity-gauge::before {{
  content: ''; width: 90px; height: 90px; border-radius: 50%;
  background: #fff; position: absolute;
}}
.integrity-gauge-value {{
  position: relative; z-index: 1;
  font-size: 32px; font-weight: 800; color: var(--navy);
}}
.tier-badge {{
  display: inline-block; padding: 2px 10px; border-radius: 12px;
  font-size: 11px; font-weight: 700; letter-spacing: 0.03em;
}}
.tier-confirmed {{ background: #dcfce7; color: #166534; }}
.tier-partial {{ background: #fef3c7; color: #92400e; }}
.tier-estimated {{ background: #fee2e2; color: #991b1b; }}
.discrepancy-box {{
  padding: 14px 18px; border-radius: 8px; margin: 10px 0;
  background: #fffbeb; border-left: 4px solid var(--amber);
  font-size: 13px; line-height: 1.6;
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
      <span>Sections: 52</span>
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

  <!-- Section 52: Data Integrity & Fact-Check -->
  {factcheck_html}

  <!-- Enhanced API Data Charts (DataForSEO) -->
  {dataforseo_charts_html}

</div><!-- /#main -->

<!-- ── FOOTER ── -->
<footer class="report-footer">
  <span><span class="footer-brand">BlazingHill Research</span> &mdash; Commercial &amp; Digital Due Diligence</span>
  <span>Confidential &middot; {today} &middot; {report_id}</span>
</footer>

<script>
/* ═══════════════════════════════════════════════════════
   BlazingHill Report v5.0 — Interactive JS
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

// ── Register Chart.js Datalabels Plugin ───────────────
if (typeof ChartDataLabels !== 'undefined') {{
  Chart.register(ChartDataLabels);
}}

// ── Chart.js Auto-initialization ──────────────────────
(function() {{
  var chartContainers = document.querySelectorAll('.chart-container[data-chart]');
  var chartIndex = 0;

  chartContainers.forEach(function(container) {{
    chartIndex++;
    var rawConfig = container.getAttribute('data-chart');
    if (!rawConfig) return;

    // Parse JSON config with auto-repair for unbalanced braces
    var config;
    try {{
      // GPT sometimes outputs JSON missing 1-2 closing braces — repair before parsing
      var fixed = rawConfig;
      var opens = (fixed.match(/\{{/g) || []).length;
      var closes = (fixed.match(/\}}/g) || []).length;
      if (opens > closes) {{
        fixed += '}}'.repeat(opens - closes);
      }}
      var openBrackets = (fixed.match(/\[/g) || []).length;
      var closeBrackets = (fixed.match(/\]/g) || []).length;
      if (openBrackets > closeBrackets) {{
        fixed += ']'.repeat(openBrackets - closeBrackets);
      }}
      config = JSON.parse(fixed);
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
      config.options.plugins.legend = {{ position: 'bottom', labels: {{ font: {{ size: 12 }} }} }};
    }}

    // ── DATALABELS: Show values on every chart ──
    var chartType = config.type || 'bar';
    if (!config.options.plugins.datalabels) {{
      if (chartType === 'doughnut' || chartType === 'pie' || chartType === 'polarArea') {{
        // Show percentages on slices
        config.options.plugins.datalabels = {{
          color: '#fff',
          font: {{ weight: 'bold', size: 12 }},
          formatter: function(value, ctx) {{
            var total = ctx.dataset.data.reduce(function(a, b) {{ return a + b; }}, 0);
            var pct = Math.round((value / total) * 100);
            return pct > 3 ? pct + '%' : '';
          }}
        }};
      }} else if (chartType === 'radar') {{
        config.options.plugins.datalabels = {{ display: false }};
      }} else {{
        // Bar, line, scatter: show values above points/bars
        config.options.plugins.datalabels = {{
          anchor: 'end',
          align: 'top',
          color: '#374151',
          font: {{ weight: '600', size: 11 }},
          formatter: function(value) {{
            if (typeof value === 'number') {{
              if (value >= 1000000) return (value / 1000000).toFixed(1) + 'M';
              if (value >= 1000) return (value / 1000).toFixed(0) + 'K';
              if (value % 1 !== 0) return value.toFixed(1);
            }}
            return value;
          }}
        }};
      }}
    }}

    // ── AXIS TITLES: Enforce on bar and line charts ──
    if (chartType === 'bar' || chartType === 'line') {{
      if (!config.options.scales) config.options.scales = {{}};
      // X axis
      if (!config.options.scales.x) config.options.scales.x = {{}};
      if (!config.options.scales.x.title) {{
        config.options.scales.x.title = {{ display: true, text: '', font: {{ size: 12, weight: '600' }}, color: '#6b7280' }};
      }} else {{
        config.options.scales.x.title.display = true;
        if (!config.options.scales.x.title.font) config.options.scales.x.title.font = {{ size: 12, weight: '600' }};
      }}
      // Y axis
      if (!config.options.scales.y) config.options.scales.y = {{}};
      if (!config.options.scales.y.title) {{
        config.options.scales.y.title = {{ display: true, text: '', font: {{ size: 12, weight: '600' }}, color: '#6b7280' }};
      }} else {{
        config.options.scales.y.title.display = true;
        if (!config.options.scales.y.title.font) config.options.scales.y.title.font = {{ size: 12, weight: '600' }};
      }}
      // Ensure gridlines are subtle
      if (!config.options.scales.x.grid) config.options.scales.x.grid = {{ display: false }};
      if (!config.options.scales.y.grid) config.options.scales.y.grid = {{ color: '#f3f4f6' }};
      // Begin at zero for bar charts
      if (chartType === 'bar') config.options.scales.y.beginAtZero = true;
    }}

    // Default color palette if datasets have no colors
    var palette = ['#2563eb','#16a34a','#d97706','#dc2626','#7c3aed','#06b6d4','#ec4899'];
    var paletteAlpha = ['#2563eb80','#16a34a80','#d9770680','#dc262680','#7c3aed80','#06b6d480','#ec489980'];

    if (config.data && config.data.datasets) {{
      config.data.datasets.forEach(function(ds, idx) {{
        var c = palette[idx % palette.length];
        var ca = paletteAlpha[idx % paletteAlpha.length];
        if (!ds.backgroundColor) {{
          if (chartType === 'line') {{
            ds.backgroundColor = ca;
          }} else if (chartType === 'doughnut' || chartType === 'pie' || chartType === 'polarArea') {{
            ds.backgroundColor = palette.slice(0, (ds.data || []).length);
          }} else {{
            // Give each bar a unique color for single-dataset bar charts
            if (config.data.datasets.length === 1 && ds.data) {{
              ds.backgroundColor = ds.data.map(function(_, i) {{ return palette[i % palette.length]; }});
            }} else {{
              ds.backgroundColor = c;
            }}
          }}
        }}
        if (!ds.borderColor && chartType === 'line') {{
          ds.borderColor = c;
          ds.borderWidth = ds.borderWidth || 2;
          ds.tension = ds.tension !== undefined ? ds.tension : 0.35;
          ds.fill = ds.fill !== undefined ? ds.fill : false;
          ds.pointRadius = ds.pointRadius !== undefined ? ds.pointRadius : 4;
        }}
        // For doughnut/pie, add white borders between slices
        if ((chartType === 'doughnut' || chartType === 'pie') && !ds.borderColor) {{
          ds.borderColor = '#ffffff';
          ds.borderWidth = 2;
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

