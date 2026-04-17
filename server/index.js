const express = require('express');
const path = require('path');
const fs = require('fs');
const cors = require('cors');
const db = require('./db');
const authRoutes = require('./routes/auth');
const creditRoutes = require('./routes/credits');
const reportRoutes = require('./routes/reports');
const webhookRoutes = require('./routes/webhook');
const { authenticateToken } = require('./middleware/auth');

const app = express();
const PORT = process.env.PORT || 8000;
const NODE_ENV = process.env.NODE_ENV || 'development';

// Stripe webhook needs raw body — must be before express.json()
app.use('/api/webhook', express.raw({ type: 'application/json' }));

// Regular middleware
app.use(cors());
app.use(express.json());

// ─── Serve generated reports at /reports/:reportId ───
// Each report is a folder with index.html + assets/
app.use('/reports', (req, res, next) => {
  // Extract the report ID (first path segment after /reports/)
  const parts = req.path.split('/').filter(Boolean);
  if (parts.length === 0) {
    return next();
  }

  const reportId = parts[0];

  // Redirect /reports/<id> to /reports/<id>/ so relative paths resolve correctly
  if (parts.length === 1 && !req.path.endsWith('/')) {
    return res.redirect(301, `/reports/${reportId}/`);
  }

  const subPath = parts.slice(1).join('/') || 'index.html';
  // Serve reports from persistent volume if available, otherwise fallback to app dir
  const volumeMount = process.env.RAILWAY_VOLUME_MOUNT_PATH || '';
  const reportsBase = volumeMount ? path.join(volumeMount, 'reports') : path.join(__dirname, '..', 'reports');
  const reportDir = path.join(reportsBase, reportId);
  const filePath = path.join(reportDir, subPath);

  // Security: prevent directory traversal
  if (!filePath.startsWith(reportDir)) {
    return res.status(403).json({ error: 'Forbidden' });
  }

  if (fs.existsSync(filePath)) {
    return res.sendFile(filePath);
  }

  // If requesting the report root (with trailing slash), serve index.html
  if (parts.length === 1) {
    const indexPath = path.join(reportDir, 'index.html');
    if (fs.existsSync(indexPath)) {
      return res.sendFile(indexPath);
    }
  }

  next();
});

// Serve static files from root (homepage, assets, etc.)
app.use(express.static(path.join(__dirname, '..'), {
  index: 'index.html',
  extensions: ['html']
}));

// API Routes
app.use('/api/auth', authRoutes);
app.use('/api/credits', authenticateToken, creditRoutes);
app.use('/api/reports', authenticateToken, reportRoutes);
app.use('/api/webhook', webhookRoutes);

// Health check
app.get('/api/health', (req, res) => {
  res.json({
    status: 'ok',
    version: 'v5.3',
    engine: 'perplexity-primary',
    timestamp: new Date().toISOString(),
    keys: {
      perplexity: !!process.env.PERPLEXITY_API_KEY,
      openai: !!process.env.OPENAI_API_KEY,
      dataforseo: !!process.env.DATAFORSEO_LOGIN,
      cashmere: !!process.env.CASHMERE_API_KEY,
    }
  });
});

// ─── Admin: Add credits to a user (protected by admin key) ───
app.post('/api/admin/add-credits', (req, res) => {
  const adminKey = req.headers['x-admin-key'];
  if (!adminKey || adminKey !== 'BH-ADMIN-2026-TEMP') {
    return res.status(403).json({ error: 'Forbidden' });
  }
  const { email, credits } = req.body;
  if (!email || !credits) {
    return res.status(400).json({ error: 'email and credits required' });
  }
  const user = db.prepare('SELECT id, email, credits, plan FROM users WHERE email = ?').get(email.toLowerCase().trim());
  if (!user) {
    return res.status(404).json({ error: 'User not found' });
  }
  db.prepare("UPDATE users SET credits = credits + ?, updated_at = datetime('now') WHERE id = ?").run(credits, user.id);
  const updated = db.prepare('SELECT id, email, credits, plan FROM users WHERE id = ?').get(user.id);
  res.json({ success: true, user: updated });
});

// Report status endpoint (public, for polling)
app.get('/api/report-status/:id', (req, res) => {
  const report = db.prepare(`
    SELECT id, status, report_url, brand_name, completed_at
    FROM reports WHERE id = ?
  `).get(req.params.id);

  if (!report) {
    return res.status(404).json({ error: 'Report not found' });
  }

  res.json({ report });
});

// SPA fallback — serve index.html for unmatched routes
app.get('*', (req, res) => {
  // Don't interfere with API routes or report routes
  if (req.path.startsWith('/api/') || req.path.startsWith('/reports/')) {
    return res.status(404).json({ error: 'Not found' });
  }
  res.sendFile(path.join(__dirname, '..', 'index.html'));
});

app.listen(PORT, '0.0.0.0', () => {
  console.log(`BlazingHill server running on port ${PORT}`);
  console.log(`Environment: ${NODE_ENV}`);

  // Log API key availability
  const keys = {
    PERPLEXITY_API_KEY: !!process.env.PERPLEXITY_API_KEY,
    OPENAI_API_KEY: !!process.env.OPENAI_API_KEY,
    DATAFORSEO_LOGIN: !!process.env.DATAFORSEO_LOGIN,
    AHREFS_API_KEY: !!process.env.AHREFS_API_KEY,
  };
  console.log('API keys configured:', keys);
});
