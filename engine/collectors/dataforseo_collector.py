#!/usr/bin/env python3
"""
DataForSEO Collector
Calls DataForSEO API directly for traffic, keywords, tech stack, backlinks, and reviews.
Uses REST API v3 with login/password authentication.
"""

import os
import json
import time
import base64
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

DATAFORSEO_LOGIN = os.environ.get("DATAFORSEO_LOGIN", "")
DATAFORSEO_PASSWORD = os.environ.get("DATAFORSEO_PASSWORD", "")
DATAFORSEO_BASE = "https://api.dataforseo.com/v3"

# Location codes: US=2840, UK=2826, Spain=2724, Germany=2276, France=2250
LOCATION_CODES = {
    "United States": 2840, "United Kingdom": 2826, "Spain": 2724,
    "Germany": 2276, "France": 2250, "Italy": 2380, "Netherlands": 2528,
    "Australia": 2036, "Canada": 2124, "India": 2356, "Brazil": 2076,
    "Mexico": 2484, "Japan": 2392,
}


def _get_auth_header():
    """Build Basic auth header for DataForSEO."""
    if not DATAFORSEO_LOGIN or not DATAFORSEO_PASSWORD:
        return None
    creds = base64.b64encode(f"{DATAFORSEO_LOGIN}:{DATAFORSEO_PASSWORD}".encode()).decode()
    return {"Authorization": f"Basic {creds}", "Content-Type": "application/json"}


def _post(endpoint, payload, timeout=60):
    """Make a POST request to DataForSEO API."""
    headers = _get_auth_header()
    if not headers:
        return {"error": "DataForSEO credentials not configured"}
    try:
        resp = requests.post(
            f"{DATAFORSEO_BASE}{endpoint}",
            headers=headers,
            json=[payload],
            timeout=timeout
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("status_code") == 20000 and data.get("tasks"):
            task = data["tasks"][0]
            if task.get("status_code") == 20000 and task.get("result"):
                return task["result"]
        return {"error": f"API error: {data.get('status_message', 'unknown')}", "raw": data}
    except Exception as e:
        return {"error": str(e)}


def _get_location_code(market):
    """Resolve market string to DataForSEO location code."""
    for name, code in LOCATION_CODES.items():
        if name.lower() in market.lower() or market.lower() in name.lower():
            return code
    return 2840  # Default US


def _collect_traffic(domain, location_code):
    """Get traffic estimates for the domain and competitors."""
    print(f"  [dataforseo] Fetching traffic for {domain}...")
    result = _post("/dataforseo_labs/google/bulk_traffic_estimation/live", {
        "targets": [domain],
        "location_code": location_code,
        "language_code": "en"
    })
    return {"traffic_estimation": result}


def _collect_domain_rank(domain, location_code):
    """Get domain authority and rank metrics."""
    print(f"  [dataforseo] Fetching domain rank for {domain}...")
    result = _post("/dataforseo_labs/google/domain_rank_overview/live", {
        "target": domain,
        "location_code": location_code,
        "language_code": "en"
    })
    return {"domain_rank": result}


def _collect_ranked_keywords(domain, location_code):
    """Get keywords the domain ranks for."""
    print(f"  [dataforseo] Fetching ranked keywords for {domain}...")
    result = _post("/dataforseo_labs/google/ranked_keywords/live", {
        "target": domain,
        "location_code": location_code,
        "language_code": "en",
        "limit": 100
    })
    return {"ranked_keywords": result}


def _collect_competitors(domain, location_code):
    """Find competing domains."""
    print(f"  [dataforseo] Fetching competitors for {domain}...")
    result = _post("/dataforseo_labs/google/competitors_domain/live", {
        "target": domain,
        "location_code": location_code,
        "language_code": "en",
        "limit": 20
    })
    return {"competitors": result}


def _collect_backlinks_summary(domain):
    """Get backlinks overview."""
    print(f"  [dataforseo] Fetching backlinks summary for {domain}...")
    result = _post("/backlinks/summary/live", {
        "target": domain,
        "include_subdomains": True,
        "backlinks_status_type": "live"
    })
    return {"backlinks_summary": result}


def _collect_backlinks_history(domain):
    """Get historical backlinks data."""
    print(f"  [dataforseo] Fetching backlinks history for {domain}...")
    result = _post("/backlinks/history/live", {
        "target": domain,
        "date_from": "2023-01-01"
    })
    return {"backlinks_history": result}


def _collect_tech_stack(domain):
    """Get technologies used by the domain."""
    print(f"  [dataforseo] Fetching tech stack for {domain}...")
    result = _post("/domain_analytics/technologies/domain_technologies/live", {
        "target": domain
    })
    return {"tech_stack": result}


def _collect_trustpilot_reviews(domain):
    """Get Trustpilot reviews."""
    print(f"  [dataforseo] Fetching Trustpilot reviews for {domain}...")
    # Trustpilot uses task_post → task_get pattern
    post_result = _post("/business_data/trustpilot/reviews/task_post", {
        "domain": domain,
        "sort_by": "recency",
        "depth": 20
    })
    if isinstance(post_result, list) and post_result:
        task_id = post_result[0].get("id")
        if task_id:
            # Wait for task to complete
            time.sleep(10)
            headers = _get_auth_header()
            try:
                resp = requests.get(
                    f"{DATAFORSEO_BASE}/business_data/trustpilot/reviews/task_get/{task_id}",
                    headers=headers,
                    timeout=30
                )
                resp.raise_for_status()
                data = resp.json()
                if data.get("tasks") and data["tasks"][0].get("result"):
                    return {"trustpilot_reviews": data["tasks"][0]["result"]}
            except Exception as e:
                print(f"  [dataforseo] Trustpilot task_get error: {e}")
    return {"trustpilot_reviews": post_result}


def _collect_google_reviews(brand_name):
    """Get Google business reviews."""
    print(f"  [dataforseo] Fetching Google reviews for {brand_name}...")
    result = _post("/business_data/google/reviews/task_post", {
        "keyword": brand_name,
        "depth": 20
    })
    if isinstance(result, list) and result:
        task_id = result[0].get("id")
        if task_id:
            time.sleep(10)
            headers = _get_auth_header()
            try:
                resp = requests.get(
                    f"{DATAFORSEO_BASE}/business_data/google/reviews/task_get/{task_id}",
                    headers=headers,
                    timeout=30
                )
                resp.raise_for_status()
                data = resp.json()
                if data.get("tasks") and data["tasks"][0].get("result"):
                    return {"google_reviews": data["tasks"][0]["result"]}
            except Exception as e:
                print(f"  [dataforseo] Google reviews task_get error: {e}")
    return {"google_reviews": result}


def _collect_content_citations(brand_name, domain):
    """Get brand mentions and citations online."""
    print(f"  [dataforseo] Fetching content citations for {brand_name}...")
    result = _post("/content_analysis/search/live", {
        "keyword": brand_name,
        "search_mode": "as_is",
        "limit": 20
    })
    return {"content_citations": result}


def _collect_keyword_suggestions(domain, location_code):
    """Get keyword suggestions related to the domain."""
    print(f"  [dataforseo] Fetching keyword suggestions for {domain}...")
    result = _post("/dataforseo_labs/google/keyword_suggestions/live", {
        "target": domain,
        "location_code": location_code,
        "language_code": "en",
        "limit": 50
    })
    return {"keyword_suggestions": result}


def collect_dataforseo_data(domain, market):
    """
    Run all DataForSEO collectors in parallel.
    Returns a merged dict of all data.
    """
    if not DATAFORSEO_LOGIN:
        print("  [dataforseo] WARNING: No credentials configured. Returning empty data.")
        return {"error": "DataForSEO credentials not configured"}

    location_code = _get_location_code(market)
    results = {}

    tasks = {
        "traffic": (_collect_traffic, (domain, location_code)),
        "domain_rank": (_collect_domain_rank, (domain, location_code)),
        "ranked_keywords": (_collect_ranked_keywords, (domain, location_code)),
        "competitors_seo": (_collect_competitors, (domain, location_code)),
        "backlinks": (_collect_backlinks_summary, (domain,)),
        "backlinks_history": (_collect_backlinks_history, (domain,)),
        "tech": (_collect_tech_stack, (domain,)),
        "trustpilot": (_collect_trustpilot_reviews, (domain,)),
        "content": (_collect_content_citations, (domain.replace(".com", "").replace(".co", ""), domain)),
        "keyword_suggestions": (_collect_keyword_suggestions, (domain, location_code)),
    }

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {}
        for key, (func, args) in tasks.items():
            futures[executor.submit(func, *args)] = key

        for future in as_completed(futures):
            key = futures[future]
            try:
                result = future.result()
                results.update(result)
                print(f"  [dataforseo] ✓ {key} complete")
            except Exception as e:
                print(f"  [dataforseo] ✗ {key} failed: {e}")
                results[key] = {"error": str(e)}

    return results
