#!/usr/bin/env python3
"""
BlazingHill Report Generation Pipeline
Main orchestrator that coordinates data collection, analysis, chart generation, and HTML assembly.

Usage: python engine/pipeline.py --brand "Acme Corp" --domain "acme.com" --market "United States" --report-id "abc123" --output-dir "./reports/abc123"
"""

import argparse
import json
import os
import sys
import time
import traceback
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add parent dir to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from engine.config import REPORT_SECTIONS, DATA_COLLECTORS
from engine.collectors.web_research import collect_web_research
from engine.collectors.dataforseo_collector import collect_dataforseo_data
from engine.collectors.ahrefs_collector import collect_ahrefs_data
from engine.analyzers.section_generator import generate_all_sections
from engine.charts.chart_generator import generate_all_charts
from engine.templates.html_assembler import assemble_report


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def run_pipeline(brand_name, domain, market, analysis_lens, report_id, output_dir):
    """
    Main pipeline entry point.
    Returns the path to the generated report HTML.
    """
    start_time = time.time()
    os.makedirs(output_dir, exist_ok=True)
    assets_dir = os.path.join(output_dir, "assets")
    os.makedirs(assets_dir, exist_ok=True)

    report_context = {
        "brand_name": brand_name,
        "domain": domain,
        "market": market,
        "analysis_lens": analysis_lens,
        "report_id": report_id
    }

    # ─── Phase 1: Data Collection (parallel) ───
    log(f"Phase 1: Collecting data for {brand_name} ({domain})")
    collected_data = collect_all_data(report_context)

    data_path = os.path.join(output_dir, "collected_data.json")
    with open(data_path, "w") as f:
        json.dump(collected_data, f, indent=2, default=str)
    log(f"Data collection complete. Saved to {data_path}")

    # ─── Phase 2: LLM Section Generation ───
    log("Phase 2: Generating report sections with LLM")
    sections_content = generate_all_sections(report_context, collected_data)

    sections_path = os.path.join(output_dir, "sections.json")
    with open(sections_path, "w") as f:
        json.dump(sections_content, f, indent=2, default=str)
    log(f"Section generation complete. {len(sections_content)} sections generated.")

    # ─── Phase 3: Chart Generation ───
    log("Phase 3: Generating charts and exhibits")
    chart_paths = generate_all_charts(report_context, collected_data, sections_content, assets_dir)
    log(f"Chart generation complete. {len(chart_paths)} exhibits created.")

    # ─── Phase 4: HTML Assembly ───
    log("Phase 4: Assembling final HTML report")
    report_html_path = assemble_report(
        report_context, sections_content, chart_paths, output_dir
    )
    log(f"Report assembled at {report_html_path}")

    elapsed = time.time() - start_time
    log(f"Pipeline complete in {elapsed:.1f}s")

    return report_html_path


def collect_all_data(context):
    """Run all data collectors in parallel and merge results."""
    data = {}

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {}

        # Web research (company profile, financials, competitors, etc.)
        futures[executor.submit(
            collect_web_research,
            context["brand_name"],
            context["domain"],
            context["market"]
        )] = "web_research"

        # DataForSEO (traffic, keywords, tech stack, backlinks)
        futures[executor.submit(
            collect_dataforseo_data,
            context["domain"],
            context["market"]
        )] = "dataforseo"

        # Ahrefs (backlinks, referring domains)
        futures[executor.submit(
            collect_ahrefs_data,
            context["domain"]
        )] = "ahrefs"

        for future in as_completed(futures):
            source = futures[future]
            try:
                result = future.result()
                data[source] = result
                log(f"  ✓ {source} collection complete")
            except Exception as e:
                log(f"  ✗ {source} collection failed: {e}")
                traceback.print_exc()
                data[source] = {"error": str(e)}

    return data


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="BlazingHill Report Generator")
    parser.add_argument("--brand", required=True, help="Brand name")
    parser.add_argument("--domain", required=True, help="Domain (e.g. acme.com)")
    parser.add_argument("--market", required=True, help="Market (e.g. United States)")
    parser.add_argument("--lens", default="Commercial diligence", help="Analysis lens")
    parser.add_argument("--report-id", required=True, help="Unique report ID")
    parser.add_argument("--output-dir", required=True, help="Output directory for report")

    args = parser.parse_args()

    run_pipeline(
        brand_name=args.brand,
        domain=args.domain,
        market=args.market,
        analysis_lens=args.lens,
        report_id=args.report_id,
        output_dir=args.output_dir
    )
