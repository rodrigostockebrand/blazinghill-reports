# BlazingHill Report Engine — CHANGELOG v5.1

**Release: v5.1 — GPT-4o-mini Primary + Parallel Batching**

Addresses persistent 429 rate-limiting failures (~60% of reports) and 40+ minute generation times.

---

## Changes

### 1. `engine/pipeline_utils.py` — GPT Model Strategy

**Problem:** `gpt-4.1` was the primary model with very low rate limits. Retries waited up to 120s, burning time before falling back.

**Changes:**
- **Model order flipped:** `["gpt-4o-mini", "gpt-4.1"]` — gpt-4o-mini is now primary (orders-of-magnitude higher rate limits, excellent quality for HTML report generation, far lower cost). gpt-4.1 is the fallback.
- **Reduced retries:** Primary model now gets 3 retries (was `max_retries=5`), fallback gets 2 (was 3). Fail fast to reduce wasted time.
- **Reduced backoff:** Caps at 60s with 10s base (was 120s cap, 30s base). Sequence: 10s → 20s → 40s → 60s (was 30s → 60s → 120s).

### 2. `engine/pipeline_sections.py` — Parallel Batch Generation

**Problem:** 10 batches ran sequentially with a 5-second sleep between each. Even under ideal conditions this was slow; with rate-limit retries it could take 40+ minutes.

**Changes:**
- Replaced sequential loop with `ThreadPoolExecutor(max_workers=3)` parallel execution.
- All 10 batch futures are submitted at once; up to 3 run concurrently.
- Results are collected via `as_completed()` and reassembled in order.
- Per-batch error handling: failed batches produce a graceful HTML fallback section (warning callout) instead of crashing the whole pipeline.
- Removed the 5-second `time.sleep()` between batches.

**Expected impact:** With gpt-4o-mini's higher limits and 3× parallelism, generation time should drop from 40+ minutes to ~5 minutes.

### 3. `engine/pipeline_research.py` — Dynamic Cashmere Queries

**Problem:** Cashmere API queries were hardcoded for Gymshark/athletic apparel (e.g., "athletic apparel sportswear market size revenue global"), making them useless for other brands/markets.

**Changes:**
- Replaced hardcoded "athletic apparel" / "Nike Adidas Lululemon" strings with `{market}` and `{brand_name}` template variables.
- Queries now adapt to whatever brand and market are passed into the pipeline.

### 4. `engine/pipeline_v3.py` — Version Logging

**Changes:**
- Module docstring updated: `v4.0` → `v5.1`
- Startup log line updated: `"Starting BlazingHill Report Engine v4.0"` → `"Starting BlazingHill Report Engine v5.1 — gpt-4o-mini primary, parallel batching"`

### 5. `engine/run_report.js` — Reduced Timeout

**Problem:** 45-minute timeout was calibrated for the old sequential + slow-retry approach and provided false tolerance for broken pipelines.

**Changes:**
- Timeout reduced: 45 minutes → 25 minutes.
- Updated all three references: comment, `console.error` message, and the DB `notes` string.

---

## Expected Outcomes

| Metric | Before | After |
|--------|--------|-------|
| Report failure rate | ~60% (429 errors) | <5% (gpt-4o-mini high limits) |
| Generation time (success) | 3–5 min | 2–4 min |
| Generation time (with retries) | 40+ min | 5–8 min |
| Timeout window | 45 min | 25 min |
| Cost per report | High (gpt-4.1 primary) | Low (gpt-4o-mini primary) |
