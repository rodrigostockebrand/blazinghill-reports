#!/usr/bin/env python3
"""
HTML Assembler
Assembles the final report HTML from generated sections and charts.
Matches the exact format and styling of the Meller sample report.
"""

import os
import json
import base64
import shutil
from datetime import datetime
from pathlib import Path

from engine.config import REPORT_SECTIONS


def _get_section_num(index):
    """Format section number with zero-padding."""
    return f"{index + 1:02d}"


def _html_escape(text):
    """Escape HTML entities."""
    if not text:
        return ""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")


def _safe_html(text):
    """Return text as-is if it contains HTML tags, otherwise escape."""
    if not text:
        return ""
    text = str(text)
    if "<" in text and ">" in text:
        return text  # Already contains HTML
    return _html_escape(text)


def _render_kpi_cards(cards):
    """Render KPI card grid."""
    if not cards:
        return ""
    html = '<div class="kpi-grid">\n'
    for card in cards:
        color = card.get("color", "blue")
        label = _html_escape(card.get("label", ""))
        value = _html_escape(card.get("value", ""))
        sub = _html_escape(card.get("sub", ""))
        source_name = card.get("source_name", "")
        source_url = card.get("source_url", "")
        source_html = ""
        if source_url:
            source_html = f'<div class="kpi-source"><a href="{source_url}" target="_blank" rel="noopener noreferrer">{_html_escape(source_name or "Source")}</a></div>'

        html += f'''      <div class="kpi-card kpi-{color}">
        <div class="kpi-label">{label}</div>
        <div class="kpi-value">{value}</div>
        <div class="kpi-sub">{sub}</div>
        {source_html}
      </div>\n'''
    html += '    </div>\n'
    return html


def _render_table(headers, rows, sources=None):
    """Render a data table."""
    html = '<div class="table-wrap">\n      <table>\n        <thead>\n          <tr>'
    for h in headers:
        html += f'<th>{_html_escape(h)}</th>'
    html += '</tr>\n        </thead>\n        <tbody>\n'
    for row in rows:
        html += '          <tr>'
        for cell in row:
            html += f'<td>{_safe_html(cell)}</td>'
        html += '</tr>\n'
    html += '        </tbody>\n      </table>\n    </div>\n'
    if sources:
        html += _render_source_line(sources)
    return html


def _render_stat_rows(stats):
    """Render stat-row items."""
    html = ""
    for stat in stats:
        label = _html_escape(stat.get("label", ""))
        value = _html_escape(stat.get("value", ""))
        note = stat.get("note", "")
        note_html = f'<span class="stat-note">{_html_escape(note)}</span>' if note else ""
        html += f'        <div class="stat-row"><span class="stat-label">{label}</span><span class="stat-value">{value}</span>{note_html}</div>\n'
    return html


def _normalize_source(s):
    """Normalize a source entry to {name, url} dict. Handles str or dict."""
    if isinstance(s, dict):
        return {"name": s.get("name", "Source"), "url": s.get("url", "#")}
    if isinstance(s, str):
        # Plain URL string
        if s.startswith("http"):
            # Extract domain as display name
            try:
                from urllib.parse import urlparse
                domain = urlparse(s).netloc.replace("www.", "")
                return {"name": domain or "Source", "url": s}
            except Exception:
                return {"name": "Source", "url": s}
        return {"name": s, "url": "#"}
    return {"name": "Source", "url": "#"}


def _render_exhibit(chart_id, chart_paths, brand_name, title, sources=None):
    """Render a chart exhibit."""
    path = chart_paths.get(chart_id, "")
    if not path:
        return ""
    filename = os.path.basename(path)
    source_html = ""
    if sources:
        source_parts = []
        for s in sources[:3]:
            ns = _normalize_source(s)
            name = ns["name"]
            url = ns["url"]
            source_parts.append(f'<a href="{url}" target="_blank" rel="noopener noreferrer">{_html_escape(name)}</a>')
        source_html = " | Sources: " + ", ".join(source_parts)

    return f'''    <figure class="exhibit">
      <img src="./assets/{filename}" alt="{_html_escape(title)}" loading="lazy">
      <figcaption>{_html_escape(title)}{source_html}</figcaption>
    </figure>\n'''


def _render_source_line(sources):
    """Render a tiny source attribution line."""
    if not sources:
        return ""
    parts = []
    for s in sources[:5]:
        ns = _normalize_source(s)
        name = ns["name"]
        url = ns["url"]
        parts.append(f'<a href="{url}" target="_blank" rel="noopener noreferrer">{_html_escape(name)}</a>')
    return f'    <p class="tiny text-muted mt-sm">Sources: {" · ".join(parts)}</p>\n'


def _render_risk_table(risks):
    """Render risk/opportunity table."""
    if not risks:
        return ""
    rows = []
    for r in risks:
        cat = f'<strong>{_html_escape(r.get("category", ""))}</strong>'
        finding = _html_escape(r.get("finding", ""))
        rtype = r.get("type", "risk").lower()
        priority = r.get("priority", "medium").lower()

        type_class = {"risk": "tag-risk", "opportunity": "tag-opp", "watch": "tag-watch"}.get(rtype, "tag-risk")
        type_label = rtype.capitalize()
        priority_class = {"high": "tag-high", "medium": "tag-med", "low": "tag-low"}.get(priority, "tag-med")

        rows.append([
            cat, finding,
            f'<span class="tag {type_class}">{type_label}</span>',
            f'<span class="tag {priority_class}">{priority.capitalize()}</span>'
        ])
    return _render_table(["Category", "Finding", "Severity / Type", "Priority"], rows)


def _render_list(items):
    """Render a bullet list."""
    if not items:
        return ""
    html = '<ul class="report-list">\n'
    for item in items:
        html += f'          <li>{_safe_html(item)}</li>\n'
    html += '        </ul>\n'
    return html


# ─── Section renderers ───

def _render_executive_summary(data, chart_paths, brand, idx):
    """Render exec summary section."""
    html = f'''  <section class="section" id="s{_get_section_num(idx)}">
    <div class="section-label">Section {_get_section_num(idx)}</div>
    <h2>Executive Summary</h2>

    <p class="section-intro">{_safe_html(data.get("section_intro", ""))}</p>

{_render_kpi_cards(data.get("kpi_cards", []))}

    <h3 class="subsection">Investment Thesis</h3>
    <div class="thesis-box">
      {_safe_html(data.get("investment_thesis", ""))}
    </div>

    <h3 class="subsection">Key Risks &amp; Opportunities</h3>
{_render_risk_table(data.get("risks_opportunities", []))}
  </section>\n\n'''
    return html


def _render_generic_section(section_config, data, chart_paths, brand, idx):
    """Render any section using its config and generated data."""
    section_id = section_config["id"]
    title = section_config["title"]
    num = _get_section_num(idx)

    html = f'''  <section class="section" id="s{num}">
    <div class="section-label">Section {num}</div>
    <h2>{_html_escape(title)}</h2>

    <p class="section-intro">{_safe_html(data.get("section_intro", f"Analysis of {title} for {brand}."))}</p>
'''

    # Render subsections based on available data
    for sub_title in section_config.get("subsections", []):
        sub_key = sub_title.lower().replace(" ", "_").replace("&", "and").replace("/", "_")
        sub_data = data.get(sub_key)

        html += f'\n    <h3 class="subsection">{_html_escape(sub_title)}</h3>\n'

        if isinstance(sub_data, list):
            if sub_data and isinstance(sub_data[0], dict):
                # Render as stat rows or table depending on structure
                if "label" in sub_data[0] and "value" in sub_data[0]:
                    html += _render_stat_rows(sub_data)
                elif len(sub_data[0].keys()) <= 5:
                    # Render as table
                    headers = list(sub_data[0].keys())
                    rows = [[str(item.get(h, "")) for h in headers] for item in sub_data]
                    html += _render_table(headers, rows)
                else:
                    html += _render_table(list(sub_data[0].keys()), [[str(v) for v in item.values()] for item in sub_data])
            elif sub_data and isinstance(sub_data[0], str):
                html += _render_list(sub_data)
        elif isinstance(sub_data, dict):
            # Render dict as stat rows
            stats = [{"label": k.replace("_", " ").title(), "value": str(v)} for k, v in sub_data.items() if not k.startswith("_")]
            html += _render_stat_rows(stats)
        elif isinstance(sub_data, str):
            html += f'    <p>{_safe_html(sub_data)}</p>\n'
        else:
            # Check for data under various key patterns
            for alt_key in [sub_key, sub_title.lower().replace(" ", "_"), sub_title.lower()]:
                if alt_key in data:
                    alt_data = data[alt_key]
                    if isinstance(alt_data, str):
                        html += f'    <p>{_safe_html(alt_data)}</p>\n'
                    break

    # Render any exhibits for this section
    for chart_id in section_config.get("charts", []):
        exhibit_num = chart_id.replace("ex", "Exhibit ").replace("_", " — ").title()
        sources = data.get("sources", [])
        html += _render_exhibit(chart_id, chart_paths, brand, f'{exhibit_num}', sources)

    # Source attribution
    sources = data.get("sources", [])
    if sources:
        html += _render_source_line(sources)

    html += '  </section>\n\n'
    return html


def _build_sidebar(report_context, sections_content):
    """Build the left sidebar navigation."""
    brand = report_context["brand_name"]
    now = datetime.now()

    html = f'''<nav id="sidebar" role="navigation" aria-label="Report sections">
  <div class="sidebar-header">
    <div class="sidebar-logo">Private Equity</div>
    <div class="sidebar-title">{_html_escape(brand)} DD<br>Marketing Due Diligence</div>
    <span class="confidential-badge">Confidential</span>
  </div>

  <div class="sidebar-nav" id="sidebar-nav">
'''
    for i, section in enumerate(REPORT_SECTIONS):
        num = _get_section_num(i)
        title = section["title"]
        # Shorten long titles for nav
        short_title = title
        if len(title) > 22:
            short_title = title[:20] + "…"
        active = ' class="active"' if i == 0 else ''
        html += f'    <a href="#s{num}"{active}><span class="nav-num">{num}</span>{_html_escape(short_title)}</a>\n'

    html += f'''  </div>

  <div class="sidebar-footer">
    {now.strftime("%B %Y")} &nbsp;·&nbsp; Confidential
  </div>
</nav>

<!-- Mobile hamburger -->
<button id="hamburger" aria-label="Toggle navigation" aria-expanded="false">&#9776;</button>
<div id="overlay"></div>
'''
    return html


def _build_header(report_context):
    """Build the report header."""
    brand = report_context["brand_name"]
    domain = report_context["domain"]
    market = report_context["market"]
    lens = report_context.get("analysis_lens", "Commercial diligence")
    now = datetime.now()

    return f'''  <header class="report-header">
    <div class="firm-label">{_html_escape(lens)} · Private &amp; Confidential</div>
    <h1>{_html_escape(brand)} — PE Marketing Due Diligence</h1>
    <div class="subtitle">{_html_escape(lens)} · {_html_escape(domain)}</div>
    <div class="report-meta">
      <span>Subject: {_html_escape(brand)} ({_html_escape(domain)})</span>
      <span>Date: {now.strftime("%B %Y")}</span>
      <span>Market: {_html_escape(market)}</span>
      <span>Status: Confidential Draft</span>
    </div>
  </header>
'''


def _build_javascript():
    """JavaScript for sidebar navigation, hamburger, and scroll spy."""
    return '''<script>
/* Sidebar scroll-spy + active state */
(function() {
  const links = document.querySelectorAll('.sidebar-nav a');
  const sections = [];
  links.forEach(a => {
    const id = a.getAttribute('href')?.replace('#','');
    const el = id ? document.getElementById(id) : null;
    if (el) sections.push({ el, link: a });
  });

  function updateActive() {
    let current = sections[0];
    const scrollY = window.scrollY + 120;
    for (const s of sections) {
      if (s.el.offsetTop <= scrollY) current = s;
    }
    links.forEach(a => a.classList.remove('active'));
    if (current) current.link.classList.add('active');
  }
  window.addEventListener('scroll', updateActive, { passive: true });
  updateActive();

  /* Hamburger */
  const hamburger = document.getElementById('hamburger');
  const sidebar = document.getElementById('sidebar');
  const overlay = document.getElementById('overlay');
  if (hamburger) {
    hamburger.addEventListener('click', () => {
      sidebar.classList.toggle('open');
      overlay.classList.toggle('show');
    });
    overlay?.addEventListener('click', () => {
      sidebar.classList.remove('open');
      overlay.classList.remove('show');
    });
    links.forEach(a => a.addEventListener('click', () => {
      sidebar.classList.remove('open');
      overlay?.classList.remove('show');
    }));
  }
})();
</script>'''


def assemble_report(report_context, sections_content, chart_paths, output_dir):
    """
    Assemble the final HTML report.
    Returns the path to the generated index.html.
    """
    brand = report_context["brand_name"]

    # Copy the CSS from sample report
    sample_css = os.path.join(os.path.dirname(__file__), '..', '..', 'sample-report', 'style.css')
    output_css = os.path.join(output_dir, 'style.css')
    if os.path.exists(sample_css):
        shutil.copy2(sample_css, output_css)
    else:
        # Write a minimal CSS if sample not available
        _write_fallback_css(output_css)

    # Build HTML
    html = f'''<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <meta name="robots" content="noindex, nofollow">
  <title>{_html_escape(brand)} — PE Marketing Due Diligence Report</title>
  <link rel="stylesheet" href="./style.css">
</head>
<body>

<div id="reportContent">
{_build_sidebar(report_context, sections_content)}

<main id="main">

{_build_header(report_context)}

'''

    # Render each section
    for i, section_config in enumerate(REPORT_SECTIONS):
        section_id = section_config["id"]
        section_data = sections_content.get(section_id, {})

        if section_id == "executive_summary":
            html += _render_executive_summary(section_data, chart_paths, brand, i)
        else:
            html += _render_generic_section(section_config, section_data, chart_paths, brand, i)

    html += f'''</main>
</div>

{_build_javascript()}
</body>
</html>'''

    # Write output
    output_path = os.path.join(output_dir, "index.html")
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"  [assembler] Report assembled: {output_path}")
    return output_path


def _write_fallback_css(path):
    """Write minimal CSS if sample stylesheet isn't available."""
    css = ''':root {
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
@media (max-width: 768px) {
  #sidebar { transform: translateX(-100%); transition: transform 0.3s; }
  #sidebar.open { transform: translateX(0); }
  #hamburger { display: block; }
  #overlay.show { display: block; }
  #main { margin-left: 0; padding: 24px 16px 60px; }
  .two-col { grid-template-columns: 1fr; }
  .kpi-grid { grid-template-columns: 1fr 1fr; }
}
'''
    with open(path, "w") as f:
        f.write(css)
