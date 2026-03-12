const express = require('express');
const { v4: uuidv4 } = require('uuid');
const db = require('../db');

const router = express.Router();

// Plan config: maps Stripe product IDs to plan details
const PLAN_CONFIG = {
  'prod_U8S0vTcPewzIJf': { plan: 'Starter', credits: 10, capacity: 10 },
  'prod_U8S029nTBq71ZI': { plan: 'Professional', credits: 20, capacity: 20 },
  'prod_U8S01XpLfXnN7m': { plan: 'Lifetime', credits: 999999, capacity: 999999 }
};

// POST /api/webhook — Stripe webhook handler
// In production, you'd verify the webhook signature with your Stripe webhook secret
router.post('/', (req, res) => {
  let event;
  try {
    // req.body is raw Buffer from express.raw() middleware
    event = JSON.parse(req.body.toString());
  } catch (err) {
    console.error('Webhook parse error:', err);
    return res.status(400).json({ error: 'Invalid payload' });
  }

  console.log('Stripe webhook received:', event.type);

  switch (event.type) {
    case 'checkout.session.completed': {
      const session = event.data.object;
      handleCheckoutComplete(session);
      break;
    }
    case 'invoice.paid': {
      const invoice = event.data.object;
      handleSubscriptionRenewal(invoice);
      break;
    }
    case 'customer.subscription.deleted': {
      const subscription = event.data.object;
      handleSubscriptionCancelled(subscription);
      break;
    }
    default:
      console.log('Unhandled event type:', event.type);
  }

  res.json({ received: true });
});

function handleCheckoutComplete(session) {
  const userId = session.client_reference_id;
  if (!userId) {
    console.warn('No client_reference_id in checkout session');
    return;
  }

  // Find which plan was purchased from line items metadata
  const lineItems = session.line_items?.data || [];
  let planConfig = null;

  // Try to match by product ID from session metadata or line items
  for (const [productId, config] of Object.entries(PLAN_CONFIG)) {
    // Check if any line item matches
    if (session.metadata?.product_id === productId) {
      planConfig = config;
      break;
    }
  }

  // Fallback: determine plan from amount
  if (!planConfig) {
    const amount = session.amount_total;
    if (amount === 14900) planConfig = PLAN_CONFIG['prod_U8S0vTcPewzIJf'];
    else if (amount === 24900) planConfig = PLAN_CONFIG['prod_U8S029nTBq71ZI'];
    else if (amount === 500000) planConfig = PLAN_CONFIG['prod_U8S01XpLfXnN7m'];
  }

  if (!planConfig) {
    console.warn('Could not determine plan from checkout session:', session.id);
    return;
  }

  // Update user's plan and credits
  db.prepare(`
    UPDATE users 
    SET plan = ?, credits = ?, credit_capacity = ?,
        stripe_customer_id = ?, stripe_subscription_id = ?,
        updated_at = datetime('now')
    WHERE id = ?
  `).run(
    planConfig.plan,
    planConfig.credits,
    planConfig.capacity,
    session.customer || null,
    session.subscription || null,
    userId
  );

  // Log the payment event
  db.prepare(`
    INSERT INTO payment_events (id, user_id, stripe_session_id, stripe_payment_intent, plan, amount, status)
    VALUES (?, ?, ?, ?, ?, ?, 'completed')
  `).run(
    uuidv4(),
    userId,
    session.id,
    session.payment_intent || null,
    planConfig.plan,
    session.amount_total
  );

  console.log(`User ${userId} upgraded to ${planConfig.plan}`);
}

function handleSubscriptionRenewal(invoice) {
  const customerId = invoice.customer;
  if (!customerId) return;

  const user = db.prepare('SELECT * FROM users WHERE stripe_customer_id = ?').get(customerId);
  if (!user) return;

  // Reset credits on subscription renewal
  const planConfig = Object.values(PLAN_CONFIG).find(p => p.plan === user.plan);
  if (planConfig) {
    db.prepare(`
      UPDATE users SET credits = ?, updated_at = datetime('now') WHERE id = ?
    `).run(planConfig.credits, user.id);

    console.log(`Credits reset for user ${user.id} (${user.plan})`);
  }
}

function handleSubscriptionCancelled(subscription) {
  const customerId = subscription.customer;
  if (!customerId) return;

  // Downgrade to demo plan
  db.prepare(`
    UPDATE users 
    SET plan = 'demo', credits = 0, credit_capacity = 2, 
        stripe_subscription_id = NULL, updated_at = datetime('now')
    WHERE stripe_customer_id = ?
  `).run(customerId);

  console.log(`Subscription cancelled for customer ${customerId}`);
}

module.exports = router;
