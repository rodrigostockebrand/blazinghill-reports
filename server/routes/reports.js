const express = require('express');
const { v4: uuidv4 } = require('uuid');
const db = require('../db');

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
  const { brandName, domain, market, analysisLens, priority, notes } = req.body;

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

  // Simulate report generation — in production this would trigger an async job
  // For now, mark as completed after a brief delay with a link to the sample report format
  setTimeout(() => {
    try {
      const reportUrl = `/reports/${reportId}`;
      db.prepare(`
        UPDATE reports SET status = 'completed', report_url = ?, completed_at = ?
        WHERE id = ?
      `).run(reportUrl, new Date().toISOString(), reportId);
    } catch (e) {
      console.error('Report generation error:', e);
    }
  }, 5000); // 5 second simulated generation time

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

module.exports = router;
