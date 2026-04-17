#!/usr/bin/env python3
"""
BlazingHill Report Engine v3.2 — Utility Module
Shared imports, constants, and API call wrappers.
"""
import argparse
import json
import os
import sys
import time
import traceback
import requests
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# Explicit exports for 'from pipeline_utils import *'
__all__ = [
    # Standard library re-exports
    'argparse', 'json', 'os', 'sys', 'time', 'traceback', 'requests',
    'Path', 'ThreadPoolExecutor', 'as_completed', 'datetime',
    # Constants
    'PERPLEXITY_API_KEY', 'OPENAI_API_KEY', 'CASHMERE_API_KEY',
    'PITCHBOOK_COMPANY', 'PITCHBOOK_INVESTOR', 'CBINSIGHTS_RESEARCH',
    'STATISTA_PREMIUM', 'STATISTA_FREE',
    # Functions
    'log', '_cashmere_search', '_extract_premium_data',
    '_perplexity_call', '_perplexity_call_with_sources', '_gpt_call',
]

PERPLEXITY_API_KEY = os.environ.get("PERPLEXITY_API_KEY", "")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
CASHMERE_API_KEY = os.environ.get("CASHMERE_API_KEY", "")

# Cashmere collection IDs
PITCHBOOK_COMPANY = 217
PITCHBOOK_INVESTOR = 221
CBINSIGHTS_RESEARCH = 211
STATISTA_PREMIUM = 367
STATISTA_FREE = 368


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


# ─── Cashmere API (PitchBook, CB Insights, Statista) ───

def _cashmere_search(query, collection_id, limit=5):
    """Search Cashmere API for premium data sources."""
    if not CASHMERE_API_KEY:
        log(f"  WARN: No CASHMERE_API_KEY — skipping premium search for: {query[:50]}")
        return []

    try:
        resp = requests.get(
            "https://cashmere.io/api/v2/search",
            headers={"Authorization": f"Bearer {CASHMERE_API_KEY}"},
            params={"q": query, "collection": str(collection_id), "limit": limit},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        # Normalize response — may be a list or dict with "results"
        if isinstance(data, list):
            return data
        elif isinstance(data, dict):
            return data.get("results", data.get("data", []))
        return []
    except Exception as e:
        log(f"  WARN: Cashmere search failed for collection {collection_id}: {e}")
        return []


def _extract_premium_data(results, source_name):
    """Extract structured data from Cashmere search results with source attribution."""
    extracted = []
    for r in results:
        if not isinstance(r, dict):
            continue
        extracted.append({
            "content": r.get("content", ""),
            "source_url": r.get("view_source_url", ""),
            "source_name": source_name,
            "title": r.get("omnipub_title", ""),
            "publisher": r.get("omnipub_publisher", source_name),
            "published_at": r.get("omnipub_published_at", ""),
            "score": r.get("score", 0),
        })
    return extracted


# ─── Perplexity API ───

def _perplexity_call(system_msg, user_msg, max_tokens=4000, max_retries=4):
    """Make a Perplexity API call with retry logic. Returns (content, citations_list)."""
    import time as _time
    for attempt in range(max_retries):
        resp = requests.post(
            "https://api.perplexity.ai/chat/completions",
            headers={
                "Authorization": f"Bearer {PERPLEXITY_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": "sonar-pro",
                "messages": [
                    {"role": "system", "content": system_msg},
                    {"role": "user", "content": user_msg},
                ],
                "max_tokens": max_tokens,
                "temperature": 0.1,
                "return_citations": True,
            },
            timeout=180,
        )
        if resp.status_code == 429:
            wait = max(int(resp.headers.get("Retry-After", 0)), (2 ** attempt) * 10)
            log(f"  [Perplexity] Rate limited (429). Retry {attempt+1}/{max_retries} in {wait}s...")
            _time.sleep(wait)
            continue
        resp.raise_for_status()
        data = resp.json()
        content = data["choices"][0]["message"]["content"]
        citations = data.get("citations", [])
        return content, citations
    resp.raise_for_status()
    return "", []


def _perplexity_call_with_sources(system_msg, user_msg, call_name, max_tokens=4000):
    """Perplexity call that returns content with INLINE source attribution.
    
    Instead of relying on numbered citations, we instruct Perplexity to include
    source URLs inline in the text, and we also return the raw citation list
    tagged with the call_name to prevent cross-contamination.
    """
    content, citations = _perplexity_call(system_msg, user_msg, max_tokens)
    
    # Tag each citation with its source call name
    tagged_citations = []
    for i, url in enumerate(citations):
        tagged_citations.append({
            "index": i + 1,
            "url": url,
            "call_name": call_name,
            "source_name": f"Perplexity ({call_name})",
        })
    
    return content, tagged_citations


# ─── GPT API ───

# Model hierarchy: try best first, fall back to cheaper models if rate limited
# Note: gpt-4o-mini has much higher rate limits than gpt-4.1/gpt-5.4
_GPT_MODELS = ["gpt-4o-mini", "gpt-4.1"]

def _gpt_call(system_msg, user_msg, max_tokens=4000, max_retries=5):
    """Make a GPT API call with model fallback and exponential backoff for rate limits.
    
    Tries GPT-5.4 first (with reasoning). If rate-limited after retries,
    falls back to gpt-4.1 (which has higher rate limits).
    """
    import time as _time
    
    for model_idx, model in enumerate(_GPT_MODELS):
        is_reasoning_model = model.startswith("gpt-5")
        retries_for_model = 3 if model_idx == 0 else 2
        
        for attempt in range(retries_for_model):
            # Build request body based on model capabilities
            body = {
                "model": model,
                "messages": [
                    {"role": "developer" if is_reasoning_model else "system", "content": system_msg},
                    {"role": "user", "content": user_msg},
                ],
            }
            
            if is_reasoning_model:
                body["max_completion_tokens"] = max_tokens
                body["reasoning_effort"] = "medium"
            else:
                body["max_tokens"] = max_tokens
                body["temperature"] = 0.1
            
            try:
                resp = requests.post(
                    "https://api.openai.com/v1/chat/completions",
                    headers={
                        "Authorization": f"Bearer {OPENAI_API_KEY}",
                        "Content-Type": "application/json",
                    },
                    json=body,
                    timeout=300,
                )
            except requests.exceptions.Timeout:
                log(f"  [GPT] Timeout on {model}. Retry {attempt+1}/{retries_for_model}...")
                continue
            
            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", 0))
                wait = max(retry_after, min((2 ** attempt) * 10, 60))  # 10s, 20s, 40s, 60s cap
                log(f"  [GPT] Rate limited (429) on {model}. Retry {attempt+1}/{retries_for_model} in {wait}s...")
                _time.sleep(wait)
                continue
            
            if resp.status_code == 404 and model_idx == 0:
                # Model not available — skip to fallback
                log(f"  [GPT] Model {model} not available (404). Falling back...")
                break
            
            resp.raise_for_status()
            data = resp.json()
            if model_idx > 0:
                log(f"  [GPT] Using fallback model: {model}")
            return data["choices"][0]["message"]["content"]
        
        if model_idx < len(_GPT_MODELS) - 1:
            log(f"  [GPT] Exhausted retries on {model}. Trying fallback model...")
    
    # All models exhausted
    raise RuntimeError(f"GPT call failed: all models rate-limited after retries")


# ─── Phase 1: Multi-Source Research ───

