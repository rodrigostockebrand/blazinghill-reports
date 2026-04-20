const express = require('express');
const path = require('path');
const { v4: uuidv4 } = require('uuid');
const db = require('../db');
const { runReport } = require('../../engine/run_report');

const router = express.Router();

// GET /api/reports — List all reports for the current user
router.get('/', (req, res) => {
  const reports = db.prepare(`
    SELECT id, brand_name, domain, market, analysis_lens, priority, notes, status, report_url, created_at, completed_at
    FROM reports
    WHERE user_id = ?
    ORDER BY created_at DESC
  `).all(req.user.id);

  res.json({ reports });
});

// GET /api/reports/:id — Get a single report
router.get('/:id', (req, res) => {
  const report = db.prepare(`
    SELECT id, brand_name, domain, market, analysis_lens, priority, notes, status, report_url, created_at, completed_at
    FROM reports
    WHERE id = ? AND user_id = ?
  `).get(req.params.id, req.user.id);

  if (!report) {
    return res.status(404).json({ error: 'Report not found.' });
  }

  res.json({ report });
});

// POST /api/reports — Create a new report (consumes 1 credit)
router.post('/', (req, res) => {
  const { brandName, domain, market, analysisLens, priority, notes, enrichmentData } = req.body;

  if (!brandName || !domain || !market) {
    return res.status(400).json({ error: 'Brand name, domain, and market are required.' });
  }

  // Check credits
  const user = db.prepare('SELECT plan, credits FROM users WHERE id = ?').get(req.user.id);

  if (user.plan !== 'Lifetime' && user.credits <= 0) {
    return res.status(403).json({ error: 'No credits remaining. Please upgrade your plan.' });
  }

  // Consume credit (skip for Lifetime)
  if (user.plan !== 'Lifetime') {
    db.prepare("UPDATE users SET credits = credits - 1, updated_at = datetime('now') WHERE id = ?").run(req.user.id);
  }

  const reportId = uuidv4();
  const now = new Date().toISOString();

  db.prepare(`
    INSERT INTO reports (id, user_id, brand_name, domain, market, analysis_lens, priority, notes, status, created_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'generating', ?)
  `).run(
    reportId,
    req.user.id,
    brandName.trim(),
    domain.trim(),
    market,
    analysisLens || 'Commercial diligence',
    priority || 'Standard',
    notes || null,
    now
  );

  // Fire off the report generation pipeline asynchronously
  runReport({
    reportId,
    brandName: brandName.trim(),
    domain: domain.trim(),
    market,
    analysisLens: analysisLens || 'Commercial diligence',
    enrichmentData: enrichmentData || null,
  }, db).then((reportUrl) => {
    console.log(`[reports] Report ${reportId} completed: ${reportUrl}`);
  }).catch((err) => {
    console.error(`[reports] Report ${reportId} failed:`, err.message);
  });

  const updated = db.prepare('SELECT credits FROM users WHERE id = ?').get(req.user.id);

  res.status(201).json({
    report: {
      id: reportId,
      brandName: brandName.trim(),
      domain: domain.trim(),
      market,
      analysisLens: analysisLens || 'Commercial diligence',
      priority: priority || 'Standard',
      status: 'generating'
    },
    creditsRemaining: user.plan === 'Lifetime' ? 'unlimited' : updated.credits
  });
});

// DELETE /api/reports/:id — Remove a failed report from the user's list
router.delete('/:id', (req, res) => {
  const report = db.prepare(`
    SELECT id, status FROM reports WHERE id = ? AND user_id = ?
  `).get(req.params.id, req.user.id);

  if (!report) {
    return res.status(404).json({ error: 'Report not found.' });
  }

  if (report.status !== 'failed') {
    return res.status(400).json({ error: 'Only failed reports can be removed.' });
  }

  db.prepare('DELETE FROM reports WHERE id = ? AND user_id = ?').run(req.params.id, req.user.id);
  res.json({ success: true });
});

module.exports = router;
