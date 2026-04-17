'use strict';
require('dotenv').config();

const express = require('express');
const cors    = require('cors');
const mysql   = require('mysql2/promise');
const amqp    = require('amqplib');

const { logger }              = require('../shared/logger');
const { requestIdMiddleware }  = require('../shared/requestId');
const { errorMiddleware, createError } = require('../shared/errorMiddleware');

const PORT     = process.env.PORT    || 3006;
const MQ_URL   = process.env.MQ_URL  || 'amqp://rabbitmq';
const EXCHANGE = 'banking_events';
const TTL_DAYS = 7; // processed_events TTL

let pool;
let mqChannel;
let isStarted = false;

// ──────────────────────────────────────────────
// Backoff connector
// ──────────────────────────────────────────────
async function connectWithRetry(connectFn, name, maxRetries = 10) {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      const r = await connectFn();
      logger.info({ service: 'notification-service' }, `${name} connected`);
      return r;
    } catch (err) {
      const delay = Math.min(1000 * Math.pow(2, attempt - 1), 30000);
      logger.warn({ attempt, delay }, `${name} not ready, retrying...`);
      if (attempt === maxRetries) throw err;
      await new Promise(r => setTimeout(r, delay));
    }
  }
}

// ──────────────────────────────────────────────
// Dedup helper
// ──────────────────────────────────────────────
async function isAlreadyProcessed(eventId) {
  const [rows] = await pool.execute('SELECT id FROM processed_events WHERE event_id=?', [eventId]);
  return rows.length > 0;
}

async function markProcessed(eventId) {
  await pool.execute('INSERT IGNORE INTO processed_events (event_id) VALUES (?)', [eventId]);
}

// ──────────────────────────────────────────────
// MQ Consumer
// ──────────────────────────────────────────────
async function startConsumer(channel) {
  await channel.assertExchange(EXCHANGE, 'topic', { durable: true });
  const q = await channel.assertQueue('notifications', { durable: true });

  const events = ['TransactionCompleted','TransactionFlagged','FraudRejected','FraudApproved','LoanApplicationReceived','LoanApproved','LoanRejected','PaymentCompleted'];
  for (const ev of events) {
    await channel.bindQueue(q.queue, EXCHANGE, ev);
  }

  channel.consume(q.queue, async (msg) => {
    if (!msg) return;
    try {
      const raw   = JSON.parse(msg.content.toString());
      const eventType = msg.fields.routingKey;
      const eventId   = raw.eventId || `${eventType}:${raw.transactionId || raw.loanId || raw.paymentId}:${Date.now()}`;

      // ── Deduplication ──
      if (await isAlreadyProcessed(eventId)) {
        logger.info({ eventId }, 'Duplicate event — skipping');
        channel.ack(msg);
        return;
      }

      const fmt = (n) => new Intl.NumberFormat('en-IN', { style: 'currency', currency: 'INR' }).format(n);
      const amt = raw.amount ? fmt(raw.amount) : '';
      const notifs = [];

      switch (eventType) {
        case 'TransactionCompleted': {
          let senderUserId = raw.userId;
          if (!senderUserId && raw.fromAccountId) {
            const [acc] = await pool.execute('SELECT user_id FROM accounts WHERE id=?', [raw.fromAccountId]);
            if (acc.length > 0) senderUserId = acc[0].user_id;
          }
          if (senderUserId) notifs.push({ userId: senderUserId, msg: `Transfer of ${amt} completed successfully. Transaction #${raw.transactionId}` });
          if (raw.toAccountId) {
            const [rows] = await pool.execute('SELECT user_id FROM accounts WHERE id=?', [raw.toAccountId]);
            if (rows.length > 0) notifs.push({ userId: rows[0].user_id, msg: `You received ${amt} in your account. Transaction #${raw.transactionId}` });
          }
          break;
        }
        case 'TransactionFlagged': {
          let senderUserId = raw.userId;
          if (!senderUserId && raw.fromAccountId) {
            const [acc] = await pool.execute('SELECT user_id FROM accounts WHERE id=?', [raw.fromAccountId]);
            if (acc.length > 0) senderUserId = acc[0].user_id;
          }
          if (senderUserId) notifs.push({ userId: senderUserId, msg: `Your transfer of ${amt} is under fraud review. Transaction #${raw.transactionId}` });
          break;
        }
        case 'FraudRejected': {
          let senderUserId = raw.userId;
          if (!senderUserId && raw.fromAccountId) {
            const [acc] = await pool.execute('SELECT user_id FROM accounts WHERE id=?', [raw.fromAccountId]);
            if (acc.length > 0) senderUserId = acc[0].user_id;
          }
          if (senderUserId) notifs.push({ userId: senderUserId, msg: `Your transfer of ${amt} was blocked by fraud detection and reversed. Transaction #${raw.transactionId}` });
          break;
        }
        case 'FraudApproved': {
          let senderUserId = raw.userId;
          if (!senderUserId && raw.fromAccountId) {
            const [acc] = await pool.execute('SELECT user_id FROM accounts WHERE id=?', [raw.fromAccountId]);
            if (acc.length > 0) senderUserId = acc[0].user_id;
          }
          if (senderUserId) notifs.push({ userId: senderUserId, msg: `Your transfer of ${amt} passed fraud review and is complete. Transaction #${raw.transactionId}` });
          if (raw.toAccountId) {
            const [rows] = await pool.execute('SELECT user_id FROM accounts WHERE id=?', [raw.toAccountId]);
            if (rows.length > 0) notifs.push({ userId: rows[0].user_id, msg: `You received ${amt} in your account. Transaction #${raw.transactionId}` });
          }
          break;
        }
        case 'LoanApplicationReceived':
          if (raw.userId) notifs.push({ userId: raw.userId, msg: `📝 Your loan application for ${amt} has been received and is under review.` });
          break;
        case 'LoanApproved':
          notifs.push({ userId: raw.userId, msg: `🎉 Your loan of ${amt} has been approved and credited to your savings account!` });
          break;
        case 'LoanRejected':
          notifs.push({ userId: raw.userId, msg: `❌ Your loan application for ${amt} was rejected.` });
          break;
        case 'PaymentCompleted':
          notifs.push({ userId: raw.userId, msg: `Bill payment of ${amt} to ${raw.billerName || raw.billerCode} completed.` });
          break;
      }

      for (const notif of notifs) {
        if (notif.userId) {
          await pool.execute(
            'INSERT INTO notifications (user_id, event_type, message) VALUES (?,?,?)',
            [notif.userId, eventType, notif.msg]
          );
          logger.info({ eventType, userId: notif.userId }, 'Notification stored');
        }
      }

      await markProcessed(eventId);
      channel.ack(msg);
    } catch (err) {
      logger.error({ err: err.message }, 'Error processing notification event');
      channel.nack(msg, false, true); // requeue
    }
  });
}

// ──────────────────────────────────────────────
// TTL cleanup — delete processed_events older than 7 days
// ──────────────────────────────────────────────
function startTtlCleanup() {
  setInterval(async () => {
    try {
      const [result] = await pool.execute(
        `DELETE FROM processed_events WHERE created_at < DATE_SUB(NOW(), INTERVAL ${TTL_DAYS} DAY)`
      );
      if (result.affectedRows > 0) {
        logger.info({ deleted: result.affectedRows }, 'TTL cleanup: removed old processed_events');
      }
    } catch (err) {
      logger.error({ err: err.message }, 'TTL cleanup error');
    }
  }, 6 * 60 * 60 * 1000); // every 6 hours
}

async function init() {
  pool = await connectWithRetry(async () => {
    const p = mysql.createPool({
      host: process.env.DB_HOST || 'mysql', user: process.env.DB_USER || 'root',
      password: process.env.DB_PASS || 'rootpassword', database: process.env.DB_NAME || 'banking_db',
      waitForConnections: true, connectionLimit: 10, queueLimit: 0
    });
    await p.execute('SELECT 1');
    return p;
  }, 'MySQL');

  await connectWithRetry(async () => {
    const conn  = await amqp.connect(MQ_URL);
    mqChannel    = await conn.createChannel();
    await mqChannel.assertExchange(EXCHANGE, 'topic', { durable: true });
    return conn;
  }, 'RabbitMQ');

  await startConsumer(mqChannel);
  startTtlCleanup();
  isStarted = true;
}

// ──────────────────────────────────────────────
// Express App
// ──────────────────────────────────────────────
const app = express();
app.use(cors());
app.use(express.json());
app.use(requestIdMiddleware);

app.get('/health/startup',   (req, res) => res.json({ status: isStarted ? 'UP' : 'STARTING', service: 'notification-service' }));
app.get('/health/liveness',  async (req, res, next) => {
  try { await pool.execute('SELECT 1'); res.json({ status: 'UP', service: 'notification-service' }); }
  catch (err) { next(createError(503, 'HEALTH_CHECK_FAILED', 'DB ping failed')); }
});
app.get('/health/readiness', (req, res) => res.json({ status: isStarted ? 'READY' : 'NOT_READY', service: 'notification-service' }));
app.get('/health',           (req, res) => res.json({ status: 'UP', service: 'notification-service' }));

// ── Get Notifications for a User ──────────────
app.get('/notifications/:userId', async (req, res, next) => {
  try {
    const [rows] = await pool.execute(
      'SELECT * FROM notifications WHERE user_id=? ORDER BY created_at DESC LIMIT 50',
      [req.params.userId]
    );
    res.json({ success: true, notifications: rows });
  } catch (err) { next(err); }
});

// ── Mark Notification as Read ──────────────────
app.patch('/notifications/:id/read', async (req, res, next) => {
  try {
    await pool.execute('UPDATE notifications SET is_read=1 WHERE id=?', [req.params.id]);
    res.json({ success: true });
  } catch (err) { next(err); }
});

// ── Mark All Read ──────────────────────────────
app.patch('/notifications/user/:userId/read-all', async (req, res, next) => {
  try {
    await pool.execute('UPDATE notifications SET is_read=1 WHERE user_id=?', [req.params.userId]);
    res.json({ success: true });
  } catch (err) { next(err); }
});

app.use(errorMiddleware);

app.listen(PORT, () => logger.info({ port: PORT }, 'notification-service listening'));

init().catch(err => {
  logger.fatal({ err }, 'notification-service failed to initialise');
  process.exit(1);
});
