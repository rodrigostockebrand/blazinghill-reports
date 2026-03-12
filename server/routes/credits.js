const express = require('express');
const db = require('../db');

const router = express.Router();

// Stripe Payment Links — these redirect to Stripe's hosted checkout
const PAYMENT_LINKS = {
  Starter: 'https://buy.stripe.com/4gM00l1GHaXK3oreky53O00',
  Professional: 'https://buy.stripe.com/aFadRbadd8PCbUXfoC53O01',
  Lifetime: 'https://buy.stripe.com/8x25kFgBB4zm4svb8m53O02'
};

// GET /api/credits — Get current user's credit balance
router.get('/', (req, res) => {
  const user = db.prepare('SELECT plan, credits, credit_capacity FROM users WHERE id = ?').get(req.user.id);
  if (!user) {
    return res.status(404).json({ error: 'User not found.' });
  }
  res.json({
    plan: user.plan,
    credits: user.plan === 'Lifetime' ? 'unlimited' : user.credits,
    creditCapacity: user.credit_capacity
  });
});

// GET /api/credits/checkout-link/:plan — Get Stripe Payment Link for a plan
router.get('/checkout-link/:plan', (req, res) => {
  const planName = req.params.plan;
  const link = PAYMENT_LINKS[planName];
  if (!link) {
    return res.status(400).json({ error: 'Invalid plan name. Use Starter, Professional, or Lifetime.' });
  }

  // Append client_reference_id so we can match the payment to the user
  const url = `${link}?client_reference_id=${req.user.id}`;
  res.json({ url, plan: planName });
});

// POST /api/credits/consume — Consume 1 credit (called when generating a report)
router.post('/consume', (req, res) => {
  const user = db.prepare('SELECT plan, credits FROM users WHERE id = ?').get(req.user.id);

  if (!user) {
    return res.status(404).json({ error: 'User not found.' });
  }

  // Lifetime users have unlimited credits
  if (user.plan === 'Lifetime') {
    return res.json({ success: true, creditsRemaining: 'unlimited' });
  }

  if (user.credits <= 0) {
    return res.status(403).json({ error: 'No credits remaining. Please upgrade your plan.' });
  }

  db.prepare("UPDATE users SET credits = credits - 1, updated_at = datetime('now') WHERE id = ?").run(req.user.id);

  const updated = db.prepare('SELECT credits FROM users WHERE id = ?').get(req.user.id);
  res.json({ success: true, creditsRemaining: updated.credits });
});

module.exports = router;
