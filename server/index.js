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

// ─── Admin: Update user plan (protected by admin key) ───
app.post('/api/admin/update-plan', (req, res) => {
  const adminKey = req.headers['x-admin-key'];
  if (!adminKey || adminKey !== 'BH-ADMIN-2026-TEMP') {
    return res.status(403).json({ error: 'Forbidden' });
  }
  const { email, plan } = req.body;
  if (!email || !plan) {
    return res.status(400).json({ error: 'email and plan required' });
  }
  const validPlans = ['demo', 'Starter', 'Professional', 'Lifetime'];
  if (!validPlans.includes(plan)) {
    return res.status(400).json({ error: `Invalid plan. Must be one of: ${validPlans.join(', ')}` });
  }
  const user = db.prepare('SELECT id, email, credits, plan FROM users WHERE email = ?').get(email.toLowerCase().trim());
  if (!user) {
    return res.status(404).json({ error: 'User not found' });
  }
  db.prepare("UPDATE users SET plan = ?, updated_at = datetime('now') WHERE id = ?").run(plan, user.id);
  const updated = db.prepare('SELECT id, email, credits, plan FROM users WHERE id = ?').get(user.id);
  res.json({ success: true, user: updated });
});

// ─── Admin: List recent reports with error details ───
app.get('/api/admin/reports', (req, res) => {
  const adminKey = req.headers['x-admin-key'];
  if (!adminKey || adminKey !== 'BH-ADMIN-2026-TEMP') {
    return res.status(403).json({ error: 'Forbidden' });
  }
  const limit = parseInt(req.query.limit) || 20;
  const reports = db.prepare(`
    SELECT r.id, r.brand_name, r.domain, r.market, r.status, r.notes, r.report_url,
           r.created_at, r.completed_at, u.email as user_email
    FROM reports r
    LEFT JOIN users u ON r.user_id = u.id
    ORDER BY r.created_at DESC
    LIMIT ?
  `).all(limit);
  res.json({ reports });
});

// ─── Admin: Patch access codes in an existing report ───
app.post('/api/admin/patch-report-codes', (req, res) => {
  const adminKey = req.headers['x-admin-key'];
  if (!adminKey || adminKey !== 'BH-ADMIN-2026-TEMP') {
    return res.status(403).json({ error: 'Forbidden' });
  }
  const { reportId } = req.body;
  if (!reportId) return res.status(400).json({ error: 'reportId required' });

  const fs = require('fs');
  const path = require('path');
  const volumeMount = process.env.RAILWAY_VOLUME_MOUNT_PATH || '';
  const reportsBase = volumeMount ? path.join(volumeMount, 'reports') : path.join(__dirname, '..', 'reports');
  const reportDir = path.join(reportsBase, reportId);
  const indexPath = path.join(reportDir, 'index.html');

  if (!fs.existsSync(indexPath)) {
    return res.status(404).json({ error: 'Report HTML not found' });
  }

  let html = fs.readFileSync(indexPath, 'utf8');
  // Extract brand name from the report HTML
  const brandMatch = html.match(/var CHAT_BRAND = '([^']+)';/) || html.match(/Report:\s*<strong>([^<]+)<\/strong>/);
  const brandName = brandMatch ? brandMatch[1].trim().toUpperCase() : '';

  const oldPattern = /var VALID_CODES\s*=\s*\[.*?\];/;
  // Also handle the older REPORT_BRAND pattern
  const oldBrandPattern = /var REPORT_BRAND\s*=\s*'[^']*';\nvar VALID_CODES\s*=\s*\[.*?\];/;

  let newCodes;
  if (brandName) {
    newCodes = `var REPORT_BRAND = '${brandName}';\nvar VALID_CODES = [REPORT_BRAND, REPORT_BRAND.replace(/\\s+/g, ''), 'BLAZINGHILL'];`;
  } else {
    newCodes = "var VALID_CODES = ['BLAZINGHILL'];";
  }

  if (oldBrandPattern.test(html)) {
    html = html.replace(oldBrandPattern, newCodes);
  } else if (oldPattern.test(html)) {
    html = html.replace(oldPattern, newCodes);
  } else {
    return res.json({ success: false, message: 'VALID_CODES pattern not found in report HTML' });
  }
  fs.writeFileSync(indexPath, html, 'utf8');
  res.json({ success: true, message: `Access codes updated for brand: ${brandName || 'unknown'}` });
});

// ─── Admin: Trigger report generation ───
app.post('/api/admin/generate-report', (req, res) => {
  const adminKey = req.headers['x-admin-key'];
  if (!adminKey || adminKey !== 'BH-ADMIN-2026-TEMP') {
    return res.status(403).json({ error: 'Forbidden' });
  }
  const { email, brandName, domain, market } = req.body;
  if (!email || !brandName || !domain) {
    return res.status(400).json({ error: 'email, brandName, domain required' });
  }

  const user = db.prepare('SELECT id FROM users WHERE email = ?').get(email);
  if (!user) return res.status(404).json({ error: 'User not found' });

  const { v4: uuidv4 } = require('uuid');
  const { runReport } = require('../engine/run_report');
  const reportId = uuidv4();
  const now = new Date().toISOString();

  db.prepare(`
    INSERT INTO reports (id, user_id, brand_name, domain, market, analysis_lens, priority, status, created_at)
    VALUES (?, ?, ?, ?, ?, 'Commercial diligence', 'Standard', 'generating', ?)
  `).run(reportId, user.id, brandName.trim(), domain.trim(), market || 'United States', now);

  runReport({
    reportId,
    brandName: brandName.trim(),
    domain: domain.trim(),
    market: market || 'United States',
    analysisLens: 'Commercial diligence',
    enrichmentData: null,
  }, db).then((reportUrl) => {
    console.log(`[admin] Report ${reportId} completed: ${reportUrl}`);
  }).catch((err) => {
    console.error(`[admin] Report ${reportId} failed:`, err.message);
  });

  res.status(201).json({ reportId, status: 'generating', brandName: brandName.trim(), domain: domain.trim() });
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

// ─── Chat API: Proxy to Perplexity for report AI assistant ───
app.post('/api/chat', async (req, res) => {
  const PPLX_KEY = process.env.PERPLEXITY_API_KEY;
  if (!PPLX_KEY) {
    return res.status(503).json({ error: 'AI chat unavailable — no API key configured' });
  }

  const { messages, brandName, domain } = req.body;
  if (!messages || !Array.isArray(messages) || messages.length === 0) {
    return res.status(400).json({ error: 'messages array required' });
  }

  // Build system prompt with report context
  const systemPrompt = `You are BlazingHill Research AI, an expert PE due diligence analyst assistant. You are embedded inside a confidential PE due diligence report for ${brandName || 'the target company'} (${domain || 'unknown domain'}).

Your role:
- Answer questions about the company, its financials, market position, risks, and opportunities
- Use real-time web search to provide current, accurate data
- Cite sources with URLs when providing data points
- Speak with the authority and precision expected by senior PE partners
- If asked about something in the report, reference specific sections
- Format responses with markdown: **bold** for emphasis, bullet points for lists, and clear structure
- Keep responses concise but thorough — PE partners value density over fluff
- When providing financial figures, always note the source and date
- Flag any data that may be outdated or unverified`;

  try {
    const response = await fetch('https://api.perplexity.ai/chat/completions', {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${PPLX_KEY}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        model: 'sonar-pro',
        messages: [
          { role: 'system', content: systemPrompt },
          ...messages.slice(-10) // Keep last 10 messages for context window
        ],
        max_tokens: 2000,
        temperature: 0.3,
        search_recency_filter: 'month'
      })
    });

    if (!response.ok) {
      const errText = await response.text();
      console.error('[chat] Perplexity API error:', response.status, errText);
      return res.status(502).json({ error: 'AI service error', detail: errText.slice(0, 200) });
    }

    const data = await response.json();
    const reply = data.choices?.[0]?.message?.content || 'No response generated.';
    const citations = data.citations || [];
    res.json({ reply, citations });
  } catch (err) {
    console.error('[chat] Error:', err.message);
    res.status(500).json({ error: 'Chat request failed' });
  }
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
