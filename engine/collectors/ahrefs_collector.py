#!/usr/bin/env python3
"""
Ahrefs Collector
Calls Ahrefs API v3 for backlink and referring domain data.
"""

import os
import json
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

AHREFS_API_KEY = os.environ.get("AHREFS_API_KEY", "")
AHREFS_BASE = "https://api.ahrefs.com/v3"


def _get_headers():
    """Build auth header for Ahrefs API."""
    if not AHREFS_API_KEY:
        return None
    return {
        "Authorization": f"Bearer {AHREFS_API_KEY}",
        "Accept": "application/json"
    }


def _get(endpoint, params, timeout=60):
    """Make a GET request to Ahrefs API."""
    headers = _get_headers()
    if not headers:
        return {"error": "Ahrefs API key not configured"}
    try:
        resp = requests.get(
            f"{AHREFS_BASE}{endpoint}",
            headers=headers,
            params=params,
            timeout=timeout
        )
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.HTTPError as e:
        return {"error": f"HTTP {e.response.status_code}: {e.response.text[:500]}"}
    except Exception as e:
        return {"error": str(e)}


def _collect_referring_domains(domain):
    """Get referring domains pointing to the target."""
    print(f"  [ahrefs] Fetching referring domains for {domain}...")
    result = _get("/site-explorer/refdomains", {
        "target": domain,
        "mode": "domain",
        "select": "domain_rating,domain,backlinks,first_seen,last_visited",
        "limit": 50,
        "order_by": "domain_rating:desc"
    })
    return {"referring_domains": result}


def _collect_backlinks_best(domain):
    """Get best backlinks (one per domain, highest ahrefs_rank)."""
    print(f"  [ahrefs] Fetching best backlinks for {domain}...")
    result = _get("/site-explorer/all-backlinks", {
        "target": domain,
        "mode": "domain",
        "select": "ahrefs_rank,url_from,url_to,anchor,page_title,domain_rating,traffic_domain",
        "limit": 50,
        "order_by": "ahrefs_rank:desc"
    })
    return {"best_backlinks": result}


def _collect_domain_overview(domain):
    """Get domain-level overview metrics."""
    print(f"  [ahrefs] Fetching domain overview for {domain}...")
    result = _get("/site-explorer/overview", {
        "target": domain,
        "mode": "domain",
        "select": "ahrefs_rank,domain_rating,url_rating,backlinks,refdomains,organic_keywords,organic_traffic,organic_cost"
    })
    return {"domain_overview": result}


def collect_ahrefs_data(domain):
    """
    Run all Ahrefs collectors in parallel.
    Returns a merged dict.
    """
    if not AHREFS_API_KEY:
        print("  [ahrefs] WARNING: No API key configured. Returning empty data.")
        return {"error": "Ahrefs API key not configured"}

    results = {}

    tasks = {
        "referring_domains": (_collect_referring_domains, (domain,)),
        "best_backlinks": (_collect_backlinks_best, (domain,)),
        "domain_overview": (_collect_domain_overview, (domain,)),
    }

    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {}
        for key, (func, args) in tasks.items():
            futures[executor.submit(func, *args)] = key

        for future in as_completed(futures):
            key = futures[future]
            try:
                result = future.result()
                results.update(result)
                print(f"  [ahrefs] ✓ {key} complete")
            except Exception as e:
                print(f"  [ahrefs] ✗ {key} failed: {e}")
                results[key] = {"error": str(e)}

    return results
