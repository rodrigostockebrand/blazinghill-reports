#!/usr/bin/env python3
"""
BlazingHill Report Engine v3.2 — Main Entry Point

Modular architecture:
  pipeline_utils.py      — Shared imports, constants, API call wrappers
  pipeline_research.py   — Phase 1: Data collection (premium + Perplexity)
  pipeline_validation.py — Trustpilot, revenue staleness, source validation
  pipeline_sections.py   — Section definitions, batch config, GPT report generation
  pipeline_assembly.py   — HTML assembly with sidebar nav + Chart.js
"""

import argparse
import json
import sys
import time
import traceback
from pathlib import Path

from pipeline_utils import log
from pipeline_research import run_research
from pipeline_sections import run_report_generation
from pipeline_assembly import assemble_html


def main(brand_name, domain, market, report_id, output_dir):
    """Main pipeline v3.2: research -> report generation -> HTML assembly."""
    log(f"Starting BlazingHill Report Engine v3.2")
    log(f"Brand: {brand_name} | Domain: {domain} | Market: {market}")
    log(f"Report ID: {report_id} | Output: {output_dir}")

    start_time = time.time()

    Path(output_dir).mkdir(parents=True, exist_ok=True)

    # -- Phase 1: Research --
    try:
        research = run_research(brand_name, domain, market)
    except Exception as e:
        log(f"ERROR in research phase: {e}")
        traceback.print_exc()
        sys.exit(1)

    # Save research JSON
    research_path = Path(output_dir) / "research.json"
    with open(research_path, "w") as f:
        json.dump(research, f, indent=2)
    log(f"Research saved to {research_path}")

    # -- Phase 2: Report Generation (10 batches, 51 sections) --
    try:
        batches = run_report_generation(research, brand_name, domain, report_id)
    except Exception as e:
        log(f"ERROR in report generation: {e}")
        traceback.print_exc()
        sys.exit(1)

    # -- Phase 3: HTML Assembly --
    try:
        html = assemble_html(brand_name, domain, batches, research, report_id)
    except Exception as e:
        log(f"ERROR in HTML assembly: {e}")
        traceback.print_exc()
        sys.exit(1)

    # Save the HTML report
    html_path = Path(output_dir) / "index.html"
    with open(html_path, "w") as f:
        f.write(html)
    log(f"HTML report saved to {html_path}")

    elapsed = time.time() - start_time
    log(f"Done! Report generated in {elapsed:.1f}s")
    log(f"Output: {html_path}")

    return str(html_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="BlazingHill Report Engine v3.2")
    parser.add_argument("--brand", required=True, help="Brand name")
    parser.add_argument("--domain", required=True, help="Brand domain")
    parser.add_argument("--market", required=True, help="Market category")
    parser.add_argument("--lens", default="Commercial diligence", help="Analysis lens")
    parser.add_argument("--report-id", required=True, help="Report ID")
    parser.add_argument("--output-dir", required=True, help="Output directory")
    args = parser.parse_args()
    main(args.brand, args.domain, args.market, args.report_id, args.output_dir)
