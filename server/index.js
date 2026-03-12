const express = require('express');
const path = require('path');
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
  res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

// SPA fallback — serve index.html for unmatched routes
app.get('*', (req, res) => {
  // Don't interfere with API routes
  if (req.path.startsWith('/api/')) {
    return res.status(404).json({ error: 'Not found' });
  }
  res.sendFile(path.join(__dirname, '..', 'index.html'));
});

app.listen(PORT, '0.0.0.0', () => {
  console.log(`BlazingHill server running on port ${PORT}`);
});
