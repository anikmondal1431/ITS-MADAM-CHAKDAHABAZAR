'use strict';
require('dotenv').config();

const express = require('express');
const crypto  = require('crypto');
const fs      = require('fs');
const path    = require('path');
const http    = require('http');
const Razorpay = require('razorpay');

// ── Config ──────────────────────────────────────────────────────────────────
const PORT            = process.env.PORT || 3000;
const KEY_ID          = process.env.RAZORPAY_KEY_ID        || '';
const KEY_SECRET      = process.env.RAZORPAY_KEY_SECRET    || '';
const ADMIN_PASS      = process.env.ADMIN_PASSWORD         || 'admin123';
const GOOGLE_MAPS_KEY = process.env.GOOGLE_MAPS_API_KEY    || '';

// ── Data helpers ─────────────────────────────────────────────────────────────
const DATA_DIR = path.join(__dirname, 'data');
if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });

function readData(file) {
  const p = path.join(DATA_DIR, file);
  try { return JSON.parse(fs.readFileSync(p, 'utf8')); } catch { return []; }
}
function readDataObj(file, def) {
  const p = path.join(DATA_DIR, file);
  try { return JSON.parse(fs.readFileSync(p, 'utf8')); } catch { return def || {}; }
}
function writeData(file, data) {
  fs.writeFileSync(path.join(DATA_DIR, file), JSON.stringify(data, null, 2), 'utf8');
}

// ── SSE broadcast ────────────────────────────────────────────────────────────
const sseClients = new Set();
function addSSEClient(res) {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.flushHeaders();
  res.write(': connected\n\n');
  sseClients.add(res);
  const hb = setInterval(() => { try { res.write(': heartbeat\n\n'); } catch { clearInterval(hb); sseClients.delete(res); } }, 25000);
  res.on('close', () => { clearInterval(hb); sseClients.delete(res); });
}
function broadcast(event) {
  const msg = `data: ${JSON.stringify({ event })}\n\n`;
  for (const res of sseClients) { try { res.write(msg); } catch { sseClients.delete(res); } }
}

// ── Razorpay helpers ─────────────────────────────────────────────────────────
function getRazorpay() {
  return new Razorpay({ key_id: KEY_ID, key_secret: KEY_SECRET });
}

// ── Order helpers ────────────────────────────────────────────────────────────
function isPrepaidOrder(order) {
  return !!(order.paymentMethod && order.paymentMethod !== 'Cash on Delivery');
}
function buildCancelRefundFields(order) {
  const fields = {};
  const already = ((order.paymentStatus || '')).toLowerCase().startsWith('refunded');
  if (isPrepaidOrder(order) && !already) {
    const amt = Number(order.total || order.totalAmount || 0);
    fields.paymentStatus = `Refunded (₹${amt})`;
    fields.refundAmount  = amt;
    fields.refundedAt    = new Date().toISOString();
    fields.refundStatus  = 'Processing';
  }
  return fields;
}
function createNotification(phone, notif) {
  if (!phone) return;
  const notifs = readDataObj('notifications.json', {});
  if (!notifs[phone]) notifs[phone] = [];
  notifs[phone].unshift({ ...notif, id: 'notif-' + Date.now() });
  if (notifs[phone].length > 50) notifs[phone] = notifs[phone].slice(0, 50);
  writeData('notifications.json', notifs);
}
function updateCustomerOrderCount(phone) {
  if (!phone) return;
  const customers = readData('customers.json');
  const idx = customers.findIndex(c => c.phone === phone);
  if (idx >= 0) {
    const orders = readData('orders.json');
    customers[idx].totalOrders  = orders.filter(o => o.customerPhone === phone || o.phone === phone).length;
    customers[idx].lastOrderDate = new Date().toISOString();
    writeData('customers.json', customers);
  }
}

// ── Simple rate-limiter ───────────────────────────────────────────────────────
const hits = {};
function rateLimiter(max, windowMs) {
  return (req, res, next) => {
    const key = req.ip + req.path;
    const now = Date.now();
    hits[key] = (hits[key] || []).filter(t => now - t < windowMs);
    if (hits[key].length >= max) return res.status(429).json({ error: 'Too many requests' });
    hits[key].push(now);
    next();
  };
}

// ── HTML injection ────────────────────────────────────────────────────────────
const HTML_PATH = path.join(__dirname, 'index.html');
function serveHTML(res) {
  try {
    let html = fs.readFileSync(HTML_PATH, 'utf8');
    html = html.replace(
      /window\.RAZORPAY_KEY_ID\s*=\s*['"][^'"]*['"]/,
      `window.RAZORPAY_KEY_ID = '${KEY_ID}'`
    );
    html = html.replace(
      /window\.GOOGLE_MAPS_API_KEY\s*=\s*['"][^'"]*['"]/,
      `window.GOOGLE_MAPS_API_KEY = '${GOOGLE_MAPS_KEY}'`
    );
    if (ADMIN_PASS) {
      html = html.replace(
        /const ADMIN_PASS\s*=\s*["'][^"']*["']/,
        `const ADMIN_PASS = "${ADMIN_PASS}"`
      );
    }
    res.setHeader('Content-Type', 'text/html; charset=utf-8');
    res.setHeader('Cache-Control', 'no-store');
    res.send(html);
  } catch (e) {
    res.status(500).send('Server error: ' + e.message);
  }
}

// ── App setup ────────────────────────────────────────────────────────────────
const app = express();
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true, limit: '10mb' }));
app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,PUT,DELETE,OPTIONS,PATCH');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type,Authorization');
  if (req.method === 'OPTIONS') { res.sendStatus(200); return; }
  next();
});

// ── Static ────────────────────────────────────────────────────────────────────
app.use(express.static(__dirname, { index: false, maxAge: '1d', etag: true }));

// ════════════════════════════════════════════════════════════════════════════
// PAYMENT ROUTES — /store-api/payment/*
// ════════════════════════════════════════════════════════════════════════════

// Create Razorpay order
app.post('/store-api/payment/create-order', rateLimiter(20, 60000), async (req, res) => {
  try {
    const { amount } = req.body;
    if (!amount || amount <= 0) return res.status(400).json({ error: 'Invalid amount' });
    if (!KEY_ID || !KEY_SECRET) return res.status(500).json({ error: 'Razorpay not configured on server.' });
    const rzp = getRazorpay();
    const order = await rzp.orders.create({
      amount: Math.round(amount * 100),
      currency: 'INR',
      receipt: `order_${Date.now()}`,
    });
    return res.json({ order_id: order.id, key_id: KEY_ID, amount: order.amount });
  } catch (err) {
    console.error('Razorpay create-order error:', err.message || err);
    return res.status(500).json({ error: 'Payment gateway error. Please try again.' });
  }
});

// Verify payment signature
app.post('/store-api/payment/verify', (req, res) => {
  try {
    const { razorpay_payment_id, razorpay_order_id, razorpay_signature } = req.body;
    const body     = razorpay_order_id + '|' + razorpay_payment_id;
    const expected = crypto.createHmac('sha256', KEY_SECRET).update(body).digest('hex');
    if (expected === razorpay_signature)
      return res.json({ success: true, payment_id: razorpay_payment_id });
    return res.status(400).json({ success: false, error: 'Signature mismatch' });
  } catch (err) {
    return res.status(500).json({ success: false, error: 'Verification failed' });
  }
});

// Manual refund by payment_id
app.post('/store-api/payment/refund/:paymentId', async (req, res) => {
  try {
    const { amount } = req.body;
    const rzp    = getRazorpay();
    const refund = await rzp.payments.refund(req.params.paymentId, { amount: Math.round(amount * 100) });
    return res.json({ success: true, refund });
  } catch (err) {
    console.error('Razorpay refund error:', err.message || err);
    return res.status(500).json({ error: 'Refund failed. Please try via Razorpay dashboard.' });
  }
});

// ════════════════════════════════════════════════════════════════════════════
// STORE ROUTES — /store-api/*
// ════════════════════════════════════════════════════════════════════════════

// ── Products ──
app.get('/store-api/products', (_req, res) => res.json(readData('products.json')));

app.post('/store-api/products', (req, res) => {
  const products = readData('products.json');
  const prod = req.body;
  if (!prod?.id) return res.status(400).json({ error: 'Invalid product data' });
  const idx = products.findIndex(p => p.id === prod.id);
  if (idx >= 0) products[idx] = prod; else products.push(prod);
  writeData('products.json', products);
  broadcast('products_updated');
  return res.json({ success: true, product: prod });
});

app.put('/store-api/products/:id', (req, res) => {
  const products = readData('products.json');
  const idx = products.findIndex(p => p.id === req.params.id);
  if (idx < 0) return res.status(404).json({ error: 'Product not found' });
  products[idx] = { ...products[idx], ...req.body };
  writeData('products.json', products);
  broadcast('products_updated');
  return res.json({ success: true, product: products[idx] });
});

app.delete('/store-api/products/:id', (req, res) => {
  writeData('products.json', readData('products.json').filter(p => p.id !== req.params.id));
  broadcast('products_updated');
  res.json({ success: true });
});

app.post('/store-api/products/bulk-stock', (req, res) => {
  const products = readData('products.json');
  Object.entries(req.body).forEach(([id, stock]) => {
    const idx = products.findIndex(p => p.id === id);
    if (idx >= 0) products[idx].stock = Number(stock);
  });
  writeData('products.json', products);
  broadcast('products_updated');
  res.json({ success: true });
});

// ── Orders ──
app.get('/store-api/orders', (req, res) => {
  const orders = readData('orders.json');
  const { phone, status, limit, skip } = req.query;
  let filtered = orders;
  if (phone)  filtered = filtered.filter(o => o.customerPhone === phone || o.phone === phone);
  if (status && status !== 'All') filtered = filtered.filter(o => o.deliveryStatus === status);
  const total = filtered.length;
  if (skip)  filtered = filtered.slice(parseInt(skip));
  if (limit) filtered = filtered.slice(0, parseInt(limit));
  res.json({ orders: filtered, total });
});

app.post('/store-api/orders', (req, res) => {
  const orders = readData('orders.json');
  const order  = req.body;
  if (!order?.orderId) return res.status(400).json({ error: 'Invalid order data' });
  order.deliveryStatus  = order.deliveryStatus || 'Pending';
  order.paymentStatus   = order.paymentStatus  || (order.paymentMethod === 'Cash on Delivery' ? 'Pending' : 'Paid');
  order.timestamp       = order.timestamp      || new Date().toISOString();
  order.statusTimeline  = order.statusTimeline || [{ status: 'Order Placed', time: order.timestamp }];
  order.deliveryBoy     = order.deliveryBoy    || { name: 'Raju Das', phone: '9883518860', vehicle: 'Bicycle' };
  const idx = orders.findIndex(o => o.orderId === order.orderId);
  if (idx >= 0) orders[idx] = order; else orders.unshift(order);
  writeData('orders.json', orders);
  const phone = order.customerPhone || order.phone;
  updateCustomerOrderCount(phone);
  createNotification(phone, { type: 'order_placed', title: 'Order Placed! 🎉', message: `Your order ${order.orderId} has been placed successfully.`, orderId: order.orderId, time: new Date().toISOString(), read: false });
  broadcast('orders_updated');
  return res.json({ success: true, order });
});

app.put('/store-api/orders/:orderId', (req, res) => {
  const orders = readData('orders.json');
  const idx = orders.findIndex(o => o.orderId === req.params.orderId);
  if (idx < 0) return res.status(404).json({ error: 'Order not found' });
  const prevStatus = orders[idx].deliveryStatus;
  if (req.body?.deliveryStatus === 'Cancelled' && prevStatus !== 'Cancelled' && !('refundAmount' in req.body))
    Object.assign(req.body, buildCancelRefundFields(orders[idx]));
  Object.assign(orders[idx], req.body);
  const newStatus = orders[idx].deliveryStatus;
  if (prevStatus !== newStatus && newStatus) {
    if (!orders[idx].statusTimeline) orders[idx].statusTimeline = [];
    orders[idx].statusTimeline.push({ status: newStatus, time: new Date().toISOString() });
  }
  writeData('orders.json', orders);
  broadcast('orders_updated');
  const phone = orders[idx].customerPhone;
  if (prevStatus !== newStatus && phone) {
    const msgs = {
      Processing:        { title: 'Order Confirmed ✅',    msg: `Your order ${req.params.orderId} is confirmed.` },
      Packed:            { title: 'Order Packed 📦',       msg: `Your order ${req.params.orderId} is packed.` },
      'Out for Delivery':{ title: 'Out for Delivery 🚚',   msg: `Your order ${req.params.orderId} is on its way!` },
      Delivered:         { title: 'Delivered! 🎉',         msg: `Your order ${req.params.orderId} has been delivered.` },
      Cancelled:         { title: 'Order Cancelled ❌',     msg: `Your order ${req.params.orderId} has been cancelled.` + (orders[idx].refundAmount ? ` ₹${orders[idx].refundAmount} refund has been initiated.` : '') },
    };
    const m = msgs[newStatus];
    if (m) createNotification(phone, { type: 'order_status', title: m.title, message: m.msg, orderId: req.params.orderId, time: new Date().toISOString(), read: false });
  }
  return res.json({ success: true, order: orders[idx] });
});

// Cancel order — real Razorpay refund when payment_id exists
app.post('/store-api/orders/:orderId/cancel', async (req, res) => {
  const orders = readData('orders.json');
  const idx = orders.findIndex(o => o.orderId === req.params.orderId);
  if (idx < 0) return res.status(404).json({ error: 'Order not found' });
  const order = orders[idx];
  if (['Out for Delivery','Delivered','Cancelled'].includes(order.deliveryStatus))
    return res.status(400).json({ error: 'Order cannot be cancelled at this stage.' });

  order.deliveryStatus = 'Cancelled';
  order.cancelledAt    = new Date().toISOString();
  order.cancelReason   = (req.body || {}).reason || 'Cancelled by customer';
  if (!order.statusTimeline) order.statusTimeline = [];
  order.statusTimeline.push({ status: 'Cancelled', time: order.cancelledAt });

  const refundFields = buildCancelRefundFields(order);
  Object.assign(order, refundFields);
  const isRefunding = !!(refundFields.refundAmount);

  // Attempt real Razorpay refund
  const razorpayPaymentId = order.razorpay_payment_id;
  if (isRefunding && razorpayPaymentId && KEY_ID && KEY_SECRET) {
    try {
      const rzp = getRazorpay();
      const amtPaise = Math.round(Number(refundFields.refundAmount) * 100);
      const refund   = await rzp.payments.refund(razorpayPaymentId, { amount: amtPaise });
      order.razorpay_refund_id = refund.id;
      order.refundStatus       = 'Refunded';
      order.paymentStatus      = `Refunded (₹${refundFields.refundAmount})`;
    } catch (err) {
      console.error('Razorpay refund error:', err.message || err);
      order.refundStatus = 'RefundFailed';
    }
  }

  writeData('orders.json', orders);
  broadcast('orders_updated');
  const phone = order.customerPhone || order.phone;
  createNotification(phone, {
    type: 'order_cancelled', title: 'Order Cancelled ❌',
    message: `Your order ${req.params.orderId} has been cancelled.` + (isRefunding ? ` ₹${refundFields.refundAmount} refund has been initiated and will reflect in 5–7 business days.` : ''),
    orderId: req.params.orderId, time: new Date().toISOString(), read: false,
  });
  return res.json({ success: true, order });
});

// ── Reviews ──
app.get('/store-api/reviews', (req, res) => {
  const reviews = readData('reviews.json');
  const { productId } = req.query;
  return res.json(productId ? reviews.filter(r => r.productId === productId) : reviews);
});
app.post('/store-api/reviews', (req, res) => {
  const reviews = readData('reviews.json');
  const review  = req.body;
  if (!review?.productId) return res.status(400).json({ error: 'Invalid review' });
  review.id   = review.id   || 'rev-' + Date.now();
  review.time = review.time || new Date().toISOString();
  reviews.unshift(review);
  writeData('reviews.json', reviews);
  broadcast('reviews_updated');
  return res.json({ success: true, review });
});
app.delete('/store-api/reviews/:id', (req, res) => {
  writeData('reviews.json', readData('reviews.json').filter(r => r.id !== req.params.id));
  res.json({ success: true });
});

// ── Wishlist ──
app.get('/store-api/wishlist/:phone', (req, res) => {
  const wishlists = readDataObj('wishlists.json', {});
  res.json({ phone: req.params.phone, items: wishlists[req.params.phone] || [] });
});
app.post('/store-api/wishlist/:phone', (req, res) => {
  const wishlists = readDataObj('wishlists.json', {});
  const { phone } = req.params;
  const { productId, action, items } = req.body;
  if (!wishlists[phone]) wishlists[phone] = [];
  if (action === 'add'    && productId) { if (!wishlists[phone].includes(productId)) wishlists[phone].push(productId); }
  else if (action === 'remove' && productId) { wishlists[phone] = wishlists[phone].filter(id => id !== productId); }
  else if (action === 'set') { wishlists[phone] = items || []; }
  wishlists[phone] = [...new Set(wishlists[phone])];
  writeData('wishlists.json', wishlists);
  res.json({ success: true, items: wishlists[phone] });
});

// ── Notifications ──
app.get('/store-api/notifications/:phone', (req, res) => {
  const notifs = readDataObj('notifications.json', {});
  const items  = (notifs[req.params.phone] || []).sort((a, b) => new Date(b.time).getTime() - new Date(a.time).getTime());
  res.json({ phone: req.params.phone, notifications: items });
});
app.post('/store-api/notifications', (req, res) => {
  const notif = req.body;
  if (!notif?.phone) return res.status(400).json({ error: 'Phone required' });
  createNotification(notif.phone, notif);
  return res.json({ success: true });
});
app.put('/store-api/notifications/:phone/read', (req, res) => {
  const notifs = readDataObj('notifications.json', {});
  const { phone } = req.params;
  if (notifs[phone]) notifs[phone] = notifs[phone].map(n => ({ ...n, read: true }));
  writeData('notifications.json', notifs);
  res.json({ success: true });
});

// ── Dashboard ──
app.get('/store-api/dashboard', (_req, res) => {
  const orders    = readData('orders.json');
  const customers = readData('customers.json');
  const products  = readData('products.json');
  const now       = new Date();
  const todayStr  = now.toISOString().split('T')[0];
  const weekAgo   = new Date(now.getTime() - 7  * 86400000);
  const monthAgo  = new Date(now.getTime() - 30 * 86400000);

  const todayOrders  = orders.filter(o => (o.timestamp || '').startsWith(todayStr));
  const weekOrders   = orders.filter(o => new Date(o.timestamp || 0).getTime() >= weekAgo.getTime());
  const monthOrders  = orders.filter(o => new Date(o.timestamp || 0).getTime() >= monthAgo.getTime());
  const completed    = orders.filter(o => o.deliveryStatus === 'Delivered');
  const pending      = orders.filter(o => o.deliveryStatus === 'Pending');
  const cancelled    = orders.filter(o => o.deliveryStatus === 'Cancelled');
  const sum = arr => arr.reduce((s, o) => s + parseFloat(o.totalAmount || o.total || 0), 0);

  const lowStock  = products.filter(p => Number(p.stock || 0) <= 10 && Number(p.stock || 0) > 0);
  const outOfStock = products.filter(p => Number(p.stock || 0) === 0);

  const productSales = {};
  completed.forEach(o => {
    (o.items || []).forEach(item => {
      const key = item.productId || item.name;
      productSales[key] = (productSales[key] || 0) + Number(item.qty || item.quantity || 1);
    });
  });
  const topProducts = Object.entries(productSales).sort((a,b)=>b[1]-a[1]).slice(0,5)
    .map(([id, qty]) => { const prod = products.find(p => p.id === id || p.name === id); return { id, name: prod ? prod.name : id, qty }; });

  const weeklyChart = [];
  for (let i = 6; i >= 0; i--) {
    const d = new Date(now.getTime() - i * 86400000);
    const dStr = d.toISOString().split('T')[0];
    const dayOrders = orders.filter(o => (o.timestamp||'').startsWith(dStr) && o.deliveryStatus === 'Delivered');
    weeklyChart.push({ date: dStr, day: d.toLocaleDateString('en-IN', { weekday: 'short' }), orders: dayOrders.length, revenue: sum(dayOrders) });
  }

  res.json({
    totalOrders: orders.length, todayOrders: todayOrders.length,
    todayRevenue: sum(todayOrders.filter(o=>o.deliveryStatus==='Delivered')),
    weekRevenue: sum(weekOrders.filter(o=>o.deliveryStatus==='Delivered')),
    monthRevenue: sum(monthOrders.filter(o=>o.deliveryStatus==='Delivered')),
    totalRevenue: sum(completed),
    pendingOrders: pending.length, completedOrders: completed.length, cancelledOrders: cancelled.length,
    totalCustomers: customers.length, totalProducts: products.length,
    lowStockCount: lowStock.length, outOfStockCount: outOfStock.length,
    lowStockItems: lowStock, topProducts, weeklyChart,
    recentOrders: orders.slice(0, 10),
  });
});

// ── Partners ──
app.get('/store-api/partners', (_req, res) => res.json(readData('partners.json')));
app.post('/store-api/partners', (req, res) => {
  const partners = readData('partners.json');
  const partner  = req.body;
  if (!partner) return res.status(400).json({ error: 'Invalid data' });
  partner.id = partner.id || 'part-' + Date.now();
  const idx = partners.findIndex(p => p.id === partner.id);
  if (idx >= 0) partners[idx] = partner; else partners.push(partner);
  writeData('partners.json', partners);
  return res.json({ success: true, partner });
});
app.delete('/store-api/partners/:id', (req, res) => {
  writeData('partners.json', readData('partners.json').filter(p => p.id !== req.params.id));
  res.json({ success: true });
});

// ── Settings ──
const DEFAULT_SETTINGS = {
  pincodes: '743127, 741222, 741223, 741248',
  coupons: '[{"code":"FRESH10","discount":10,"type":"percent"}]',
  whatsappNumber: '917478926834',
  storeUrl: 'https://chakdahabazar.in',
  deliveryBoyName: 'Raju Das',
  deliveryBoyPhone: '9883518860',
  deliveryBoyVehicle: 'Bicycle',
  freeDeliveryMin: 120,
  deliveryFee: 30,
};
app.get('/store-api/settings', (_req, res) => res.json({ ...DEFAULT_SETTINGS, ...readDataObj('settings.json', {}) }));
app.post('/store-api/settings', (req, res) => {
  const current = readDataObj('settings.json', DEFAULT_SETTINGS);
  const updated = Object.assign(current, req.body);
  writeData('settings.json', updated);
  broadcast('settings_updated');
  res.json({ success: true, settings: updated });
});

// ── Customers ──
app.get('/store-api/customers', (_req, res) => {
  const customers  = readData('customers.json');
  const orders     = readData('orders.json');
  const wishlists  = readDataObj('wishlists.json', {});
  const enriched   = customers.map(c => {
    const co = orders.filter(o => o.customerPhone === c.phone || o.phone === c.phone);
    return { ...c, totalOrders: co.length, wishlistItems: (wishlists[c.phone] || []).length, lastOrderDate: co[0] ? co[0].timestamp : null };
  });
  res.json(enriched);
});
app.post('/store-api/customers', (req, res) => {
  const customers = readData('customers.json');
  const customer  = req.body;
  if (!customer?.phone) return res.status(400).json({ error: 'Invalid customer data' });
  customer.registeredAt = customer.registeredAt || new Date().toISOString();
  customer.lastLogin    = new Date().toISOString();
  customer.status       = customer.status || 'Active';
  const idx = customers.findIndex(c => c.phone === customer.phone);
  if (idx >= 0) customers[idx] = { ...customers[idx], ...customer, lastLogin: new Date().toISOString() };
  else customers.push(customer);
  writeData('customers.json', customers);
  return res.json({ success: true });
});
app.put('/store-api/customers/:phone', (req, res) => {
  const customers = readData('customers.json');
  const idx = customers.findIndex(c => c.phone === req.params.phone);
  if (idx < 0) return res.status(404).json({ error: 'Customer not found' });
  customers[idx] = { ...customers[idx], ...req.body };
  writeData('customers.json', customers);
  return res.json({ success: true, customer: customers[idx] });
});
app.get('/store-api/customers/:phone', (req, res) => {
  const customers = readData('customers.json');
  const customer  = customers.find(c => c.phone === req.params.phone || c.phone === '+91' + req.params.phone);
  if (!customer) return res.status(404).json({ error: 'Not found' });
  return res.json(customer);
});

// ── Categories ──
app.get('/store-api/categories', (_req, res) => res.json(readData('categories.json')));
app.post('/store-api/categories', (req, res) => {
  const cats = readData('categories.json');
  const cat  = req.body;
  if (!cat?.id) return res.status(400).json({ error: 'Invalid category' });
  const idx = cats.findIndex(c => c.id === cat.id);
  if (idx >= 0) cats[idx] = cat; else cats.push(cat);
  writeData('categories.json', cats);
  broadcast('categories_updated');
  return res.json({ success: true, category: cat });
});
app.delete('/store-api/categories/:id', (req, res) => {
  writeData('categories.json', readData('categories.json').filter(c => c.id !== req.params.id));
  broadcast('categories_updated');
  res.json({ success: true });
});

// ── CSV Export ──
app.get('/store-api/export/orders', (_req, res) => {
  const orders = readData('orders.json');
  let csv = 'Order ID,Customer,Phone,Address,Products,Total,Payment,Status,Date\n';
  orders.forEach(o => {
    const items = (o.items || []).map(i => `${i.name || i.productName} x${i.qty || i.quantity || 1}`).join(' | ');
    const row   = [o.orderId, o.customerName||o.name, o.customerPhone||o.phone,
      (o.address||'').replace(/,/g,';'), items, o.totalAmount||o.total,
      o.paymentMethod, o.deliveryStatus,
      o.timestamp ? new Date(o.timestamp).toLocaleDateString('en-IN') : '']
      .map(v => `"${(v||'').toString().replace(/"/g,'""')}"`).join(',');
    csv += row + '\n';
  });
  res.setHeader('Content-Type', 'text/csv');
  res.setHeader('Content-Disposition', 'attachment; filename="orders.csv"');
  res.send(csv);
});

app.get('/store-api/export/customers', (_req, res) => {
  const customers = readData('customers.json');
  const orders    = readData('orders.json');
  let csv = 'Name,Phone,Email,Address,Pincode,Orders,Registered,Status\n';
  customers.forEach(c => {
    const orderCount = orders.filter(o => o.customerPhone === c.phone || o.phone === c.phone).length;
    const row = [c.name, c.phone, c.email, (c.address||'').replace(/,/g,';'),
      c.pincode, orderCount,
      c.registeredAt ? new Date(c.registeredAt).toLocaleDateString('en-IN') : '',
      c.status||'Active']
      .map(v => `"${(v||'').toString().replace(/"/g,'""')}"`).join(',');
    csv += row + '\n';
  });
  res.setHeader('Content-Type', 'text/csv');
  res.setHeader('Content-Disposition', 'attachment; filename="customers.csv"');
  res.send(csv);
});

// ── SSE ──
app.get('/store-api/events', (_req, res) => addSSEClient(res));

// ── Health check ──
app.get('/api/health', (_req, res) => res.json({ status: 'ok', timestamp: new Date().toISOString() }));

// ── Serve index.html (SPA catch-all) ──
app.get('/', (_req, res) => serveHTML(res));
app.get(/.*/, (req, res) => {
  if (req.path.includes('.') && !req.path.endsWith('.html')) return res.status(404).send('Not found');
  serveHTML(res);
});

// ── Start ──
http.createServer(app).listen(PORT, () => {
  console.log(`✅ Chakdaha Bazar server running on port ${PORT}`);
  console.log(`   Razorpay key: ${KEY_ID ? KEY_ID.substring(0,12) + '***' : '⚠️  NOT SET'}`);
});
