const Database = require('better-sqlite3');
const path = require('path');

// Use RAILWAY_VOLUME_MOUNT_PATH if available (persistent), otherwise fall back to local data/
const DATA_DIR = process.env.RAILWAY_VOLUME_MOUNT_PATH || path.join(__dirname, '..', 'data');
const DB_PATH = path.join(DATA_DIR, 'blazinghill.db');

// Ensure data directory exists
const fs = require('fs');
fs.mkdirSync(path.dirname(DB_PATH), { recursive: true });

const db = new Database(DB_PATH);

// Enable WAL mode for better concurrent performance
db.pragma('journal_mode = WAL');

// Create tables
db.exec(`
  CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    plan TEXT NOT NULL DEFAULT 'demo',
    credits INTEGER NOT NULL DEFAULT 2,
    credit_capacity INTEGER NOT NULL DEFAULT 2,
    stripe_customer_id TEXT,
    stripe_subscription_id TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
  );

  CREATE TABLE IF NOT EXISTS reports (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    brand_name TEXT NOT NULL,
    domain TEXT NOT NULL,
    market TEXT NOT NULL,
    analysis_lens TEXT NOT NULL DEFAULT 'Commercial diligence',
    priority TEXT NOT NULL DEFAULT 'Standard',
    notes TEXT,
    status TEXT NOT NULL DEFAULT 'generating',
    report_url TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    completed_at TEXT,
    FOREIGN KEY (user_id) REFERENCES users(id)
  );

  CREATE TABLE IF NOT EXISTS payment_events (
    id TEXT PRIMARY KEY,
    user_id TEXT,
    stripe_session_id TEXT,
    stripe_payment_intent TEXT,
    plan TEXT,
    amount INTEGER,
    currency TEXT DEFAULT 'usd',
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
  );
`);

module.exports = db;
