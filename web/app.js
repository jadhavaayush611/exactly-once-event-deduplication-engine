/**
 * Pulse — Exactly-Once Event Deduplication Engine
 * Live Event Stream Simulator & Telemetry Dashboard
 */

// Helper utilities
const $ = id => document.getElementById(id);
const fmt = n => Number(n || 0).toLocaleString();
const stamp = t => t == null ? '—' : new Date(Number(t) * 1000).toLocaleTimeString([], { hour12: false });
function esc(s) {
  return String(s ?? '').replace(/[&<>"']/g, c => ({
    '&': '&amp;',
    '<': '&lt;',
    '>': '&gt;',
    '"': '&quot;',
    "'": '&#39;'
  }[c]));
}

let toastTimer;
function toast(s) {
  const e = $('toast');
  if (!e) return;
  e.textContent = s;
  e.classList.add('show');
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => e.classList.remove('show'), 2800);
}

async function api(url, opts = {}) {
  const r = await fetch(url, {
    headers: { 'Content-Type': 'application/json' },
    ...opts
  });
  const b = await r.json().catch(() => ({}));
  if (!r.ok) {
    throw new Error(b.message || b.error || b.detail || `Request failed (${r.status})`);
  }
  return b;
}

/**
 * Requirement 3: Automatic UUID Generation using crypto.randomUUID()
 * Robust fallback for environments where crypto.randomUUID might not be present.
 */
function generateEventId() {
  if (typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function') {
    return crypto.randomUUID();
  }
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, c => {
    const r = (Math.random() * 16) | 0;
    const v = c === 'x' ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}

/**
 * Requirement 5: Realistic Event Payloads across multiple real-world domains
 */
function generateRealisticPayload() {
  const archetypes = [
    () => {
      const currencies = ['USD', 'EUR', 'GBP', 'CAD'];
      const methods = ['apple_pay', 'card', 'google_pay', 'wire'];
      const amount = Number((12 + Math.random() * 450).toFixed(2));
      return {
        domain: 'fintech',
        type: 'payment_settled',
        order_ref: `ORD-${Math.floor(10000 + Math.random() * 90000)}`,
        amount,
        currency: currencies[Math.floor(Math.random() * currencies.length)],
        payment_method: methods[Math.floor(Math.random() * methods.length)],
        risk_score: Number((Math.random() * 0.12).toFixed(3))
      };
    },
    () => {
      const carriers = ['FedEx', 'UPS', 'DHL', 'USPS'];
      const hubs = ['Austin Hub', 'Frankfurt Cargo', 'Tokyo Air', 'London Gateway'];
      return {
        domain: 'logistics',
        type: 'package_routed',
        carrier: carriers[Math.floor(Math.random() * carriers.length)],
        tracking: `TRK-${Math.floor(1000000 + Math.random() * 9000000)}`,
        origin_hub: hubs[Math.floor(Math.random() * hubs.length)],
        priority: Math.random() > 0.4 ? 'express' : 'standard'
      };
    },
    () => {
      const skus = ['SKU-SRV-980', 'SKU-NVME-4TB', 'SKU-MEM-128G', 'SKU-PWR-850'];
      const warehouses = ['WH-EAST-1', 'WH-WEST-2', 'WH-EU-CENTRAL', 'WH-APAC-NORTH'];
      return {
        domain: 'inventory',
        type: 'stock_reserved',
        sku: skus[Math.floor(Math.random() * skus.length)],
        quantity: Math.floor(1 + Math.random() * 6),
        warehouse: warehouses[Math.floor(Math.random() * warehouses.length)],
        hold_ttl_sec: 900
      };
    },
    () => {
      const sensors = ['pressure_sensor_4', 'inlet_temp_degc', 'turbine_rpm_1', 'flow_lpm_a'];
      const sensor = sensors[Math.floor(Math.random() * sensors.length)];
      return {
        domain: 'telemetry',
        type: 'metric_reading',
        sensor_id: sensor,
        value: Number((25 + Math.random() * 75).toFixed(2)),
        unit: sensor.includes('temp') ? 'celsius' : sensor.includes('pressure') ? 'psi' : 'units',
        healthy: true
      };
    },
    () => {
      const actions = ['checkout_start', 'tier_upgrade', 'api_key_rotated', 'mfa_enabled'];
      return {
        domain: 'security',
        type: 'audit_record',
        action: actions[Math.floor(Math.random() * actions.length)],
        actor_id: `usr_${Math.floor(100 + Math.random() * 900)}`,
        tenant_id: 'enterprise_org_10',
        authorized: true
      };
    }
  ];
  const factory = archetypes[Math.floor(Math.random() * archetypes.length)];
  return factory();
}

/**
 * Requirement 4 & 5: Continuous Event Producer
 * Features:
 *  - Start, pause, resume, stop.
 *  - Configurable rate and duplicate probability.
 *  - Generate ID once per logical event.
 *  - Reuse identical ID and payload for intentional duplicate replays and retries.
 *  - Safe handling of in-flight requests and clean termination.
 *  - Submits directly to the real backend ingestion endpoint.
 */
class StreamProducer {
  constructor(options = {}) {
    this.endpoint = options.endpoint || '/api/events';
    this.rate = Math.max(0.5, Math.min(50, Number(options.rate) || 2)); // events per second
    this.duplicateProbability = Math.max(0, Math.min(1, Number(options.duplicateProbability) ?? 0.3));
    this.state = 'STOPPED'; // 'STOPPED' | 'RUNNING' | 'PAUSED'

    // Logical event history pool for replaying duplicates
    this.recentLogicalEvents = [];
    this.maxPoolSize = 60;

    // In-flight and retry handling
    this.inFlightRequests = new Set();
    this.maxConcurrency = 20;
    this.maxRetries = 2;
    this.timer = null;
    this.fetchFn = options.fetchFn || api;

    // Callbacks
    this.onEventScheduled = options.onEventScheduled || (() => {});
    this.onEventCompleted = options.onEventCompleted || (() => {});
    this.onStatusChange = options.onStatusChange || (() => {});
    this.onInFlightChange = options.onInFlightChange || (() => {});

    // Client counters
    this.metrics = {
      submitted: 0,
      accepted: 0,
      duplicates: 0,
      failed: 0,
      retries: 0
    };
  }

  setRate(eps) {
    this.rate = Math.max(0.5, Math.min(50, Number(eps) || 2));
    if (this.state === 'RUNNING') {
      this._scheduleNext(true);
    }
  }

  setDuplicateProbability(prob) {
    this.duplicateProbability = Math.max(0, Math.min(1, Number(prob) ?? 0.3));
  }

  start() {
    if (this.state === 'RUNNING') return;
    this.state = 'RUNNING';
    this._aborted = false;
    this.onStatusChange(this.state);
    this._scheduleNext();
  }

  pause() {
    if (this.state !== 'RUNNING') return;
    this.state = 'PAUSED';
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    this.onStatusChange(this.state);
  }

  resume() {
    if (this.state !== 'PAUSED') return;
    this.state = 'RUNNING';
    this.onStatusChange(this.state);
    this._scheduleNext();
  }

  async stop() {
    if (this.state === 'STOPPED') return;
    this.state = 'STOPPED';
    this._aborted = true;
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    this.onStatusChange(this.state);
    // Let current in-flight requests finish or settle
    if (this.inFlightRequests.size > 0) {
      await Promise.allSettled(Array.from(this.inFlightRequests));
    }
  }

  _scheduleNext(replaceExisting = false) {
    if (this.state !== 'RUNNING') return;
    if (replaceExisting && this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    if (this.timer) return;

    const delayMs = Math.max(20, Math.floor(1000 / this.rate));
    this.timer = setTimeout(async () => {
      this.timer = null;
      if (this.state === 'RUNNING') {
        this._tick();
        this._scheduleNext();
      }
    }, delayMs);
  }

  /**
   * Generates a new logical event OR intentionally replays an existing logical event.
   * Reuses the exact same ID and payload!
   */
  createEvent() {
    const shouldReplay = this.recentLogicalEvents.length > 0 && Math.random() < this.duplicateProbability;

    if (shouldReplay) {
      // Intentionally replay an existing logical event
      const existing = this.recentLogicalEvents[Math.floor(Math.random() * this.recentLogicalEvents.length)];
      return {
        id: existing.id,
        ts: Date.now() / 1000,
        data: existing.data, // exact same payload
        isReplay: true,
        logicalBirth: existing.firstTs
      };
    } else {
      // Create a fresh logical event with automatic UUID
      const id = generateEventId();
      const payload = generateRealisticPayload();
      const ts = Date.now() / 1000;
      const logicalEvent = { id, data: payload, firstTs: ts };

      this.recentLogicalEvents.push(logicalEvent);
      if (this.recentLogicalEvents.length > this.maxPoolSize) {
        this.recentLogicalEvents.shift();
      }

      return {
        id,
        ts,
        data: payload,
        isReplay: false,
        logicalBirth: ts
      };
    }
  }

  async _tick() {
    if (this.state !== 'RUNNING') return;
    if (this.inFlightRequests.size >= this.maxConcurrency) {
      // Backpressure: skip this tick to avoid unbounded concurrency
      return;
    }

    const eventObj = this.createEvent();
    this.metrics.submitted++;

    // Track feed item with pending state
    const feedItem = {
      id: eventObj.id,
      ts: eventObj.ts,
      data: eventObj.data,
      isReplay: eventObj.isReplay,
      status: 'pending', // AMBER initial visual state
      attempt: 1,
      createdAt: Date.now()
    };

    this.onEventScheduled(feedItem);

    // Send event through ingestion pipeline with retry support
    const taskPromise = this._sendWithRetry(feedItem);
    this.inFlightRequests.add(taskPromise);
    this.onInFlightChange(this.inFlightRequests.size);

    try {
      await taskPromise;
    } finally {
      this.inFlightRequests.delete(taskPromise);
      this.onInFlightChange(this.inFlightRequests.size);
    }
  }

  async _sendWithRetry(feedItem) {
    let lastError = null;

    for (let attempt = 1; attempt <= this.maxRetries + 1; attempt++) {
      feedItem.attempt = attempt;
      if (attempt > 1) {
        if (this._aborted) break;
        feedItem.status = 'retrying'; // AMBER visual state
        this.metrics.retries++;
        this.onEventCompleted(feedItem);
        // Exponential backoff
        await new Promise(r => setTimeout(r, attempt * 150));
      }

      try {
        // Requirement 6: Submit directly to the actual backend ingestion endpoint
        const result = await this.fetchFn(this.endpoint, {
          method: 'POST',
          body: JSON.stringify({
            id: feedItem.id,
            ts: Number(feedItem.ts),
            data: feedItem.data
          })
        });

        // Backend returns status: "accepted", "duplicate", "too_old", etc.
        const status = result.status || 'accepted';
        feedItem.status = status;
        feedItem.backend = result;

        if (status === 'accepted') {
          this.metrics.accepted++;
        } else if (status === 'duplicate') {
          this.metrics.duplicates++;
        } else {
          this.metrics.failed++;
        }

        this.onEventCompleted(feedItem);
        return result;
      } catch (err) {
        lastError = err;
        if (attempt <= this.maxRetries && !this._aborted) {
          continue; // retry
        }
      }
    }

    // Exhausted retries or non-retryable failure
    feedItem.status = 'failed';
    feedItem.error = lastError ? lastError.message : 'Request failed';
    this.metrics.failed++;
    this.onEventCompleted(feedItem);
    return null;
  }
}

// ---------------------------------------------------------------------------
// Dashboard Controller
// ---------------------------------------------------------------------------

let localFeed = []; // local ring buffer for live feed: { id, ts, data, status, isReplay }
const MAX_FEED_ITEMS = 80;
let producer = null;

function updateMetricsDisplay(stats) {
  if (!stats) return;

  // Requirement 7:
  // - Total events received
  // - Unique events processed
  // - Duplicates detected
  // - Failed events
  const totalReceived = stats.received ?? 0;
  const uniqueProcessed = (stats.accepted != null ? stats.accepted : stats.delivered) ?? 0;
  const duplicatesBlocked = stats.duplicates ?? 0;
  const failedEvents = stats.failed ?? (
    (stats.too_old || 0) +
    (stats.invalid_timestamp || 0) +
    (stats.too_far_future || 0) +
    (stats.buffer_overflow || 0) +
    (stats.flush_failures || 0)
  );

  $('received').textContent = fmt(totalReceived);
  $('duplicates').textContent = fmt(duplicatesBlocked);
  $('delivered').textContent = fmt(uniqueProcessed);
  $('failedCount').textContent = fmt(failedEvents);

  // Secondary stats
  if ($('keys')) $('keys').textContent = fmt(stats.kv_keys ?? 0);
  if ($('dupRate')) {
    const rate = totalReceived ? (duplicatesBlocked / totalReceived * 100).toFixed(1) : '0.0';
    $('dupRate').textContent = `${rate}% duplicate rate`;
  }
  if ($('sinkCount')) $('sinkCount').textContent = `${fmt(stats.sink_effects ?? uniqueProcessed)} effects`;
  if ($('bufferCount')) $('bufferCount').textContent = `${fmt(stats.buffered ?? 0)} buffered`;
  if ($('watermark')) $('watermark').textContent = stats.watermark == null ? 'Not set' : stamp(stats.watermark);
  if ($('windowLabel')) $('windowLabel').textContent = `${stats.window ?? 60}s`;
  if ($('latenessLabel')) $('latenessLabel').textContent = `${stats.lateness ?? 2}s`;
  if ($('retries')) $('retries').textContent = fmt((stats.kv_retries || 0) + (stats.sink_retries || 0));
}

/**
 * Requirement 8: Visual states
 * - Green for processed (accepted)
 * - Red for duplicates (duplicate)
 * - Amber for pending / retrying
 * - Crimson / Red for failed
 */
function renderLiveFeed() {
  const tbody = $('eventRows');
  if (!tbody) return;

  if (!localFeed.length) {
    tbody.innerHTML = '<tr><td colspan="5" class="empty">No events yet. Start the stream simulator or publish an event.</td></tr>';
    if ($('eventCount')) $('eventCount').textContent = '0 events in view';
    return;
  }

  const rows = localFeed.slice(0, 18).map(e => {
    const statusClass = (e.status || 'pending').toLowerCase();
    let displayStatus = (e.status || 'PENDING').toUpperCase();

    if (e.status === 'retrying') {
      displayStatus = `RETRYING (${e.attempt || 2})`;
    } else if (e.isReplay && e.status === 'duplicate') {
      displayStatus = 'DUPLICATE (REPLAY)';
    }

    // Domain / type extraction
    let domainType = '—';
    if (e.data && typeof e.data === 'object') {
      if (e.data.type) {
        domainType = e.data.domain ? `${e.data.domain}:${e.data.type}` : String(e.data.type);
      } else if (e.data.action) {
        domainType = String(e.data.action);
      }
    }

    const payloadJson = JSON.stringify(e.data ?? {});

    return `
      <tr data-id="${esc(e.id)}">
        <td><span class="event-id-cell" title="Click to copy: ${esc(e.id)}" onclick="navigator.clipboard?.writeText('${esc(e.id)}');toast('Copied event ID')">${esc(e.id)}</span></td>
        <td>${stamp(e.ts)}</td>
        <td><span class="tag ${e.isReplay ? 'purple' : 'cyan'}">${esc(domainType)}</span></td>
        <td><span class="payload-cell" title="${esc(payloadJson)}">${esc(payloadJson)}</span></td>
        <td><span class="decision ${esc(statusClass)}">${esc(displayStatus)}</span></td>
      </tr>
    `;
  });

  tbody.innerHTML = rows.join('');
  if ($('eventCount')) $('eventCount').textContent = `${localFeed.length} events in view`;
}

function addOrUpdateFeedItem(item) {
  const existingIdx = localFeed.findIndex(x => x.id === item.id && x.createdAt === item.createdAt);
  if (existingIdx >= 0) {
    localFeed[existingIdx] = { ...localFeed[existingIdx], ...item };
  } else {
    localFeed.unshift(item);
    if (localFeed.length > MAX_FEED_ITEMS) {
      localFeed.pop();
    }
  }
  renderLiveFeed();
}

async function refresh() {
  try {
    const [s, serverEvents] = await Promise.all([
      api('/api/stats'),
      api('/api/events?limit=60')
    ]);

    updateMetricsDisplay(s);

    // Merge server events with pending/local in-flight events if stream is running
    if (serverEvents && Array.isArray(serverEvents)) {
      if (!producer || producer.state === 'STOPPED') {
        localFeed = serverEvents.map(e => ({
          id: e.id,
          ts: e.ts,
          data: e.data,
          status: e.status,
          createdAt: e.at ? e.at * 1000 : Date.now()
        }));
        renderLiveFeed();
      }
    }
  } catch (e) {
    console.error('Refresh error:', e);
  }
}

async function action(btn, fn, msg) {
  if (btn) btn.disabled = true;
  try {
    const r = await fn();
    if (msg) toast(msg);
    await refresh();
    return r;
  } catch (e) {
    toast(e.message);
    throw e;
  } finally {
    if (btn) btn.disabled = false;
  }
}

// ---------------------------------------------------------------------------
// Initialization
// ---------------------------------------------------------------------------

function initDashboard() {
  // Initialize stream producer
  producer = new StreamProducer({
    rate: Number($('streamRate')?.value) || 2,
    duplicateProbability: (Number($('streamDupRatio')?.value) || 30) / 100,
    onEventScheduled: item => {
      addOrUpdateFeedItem(item);
    },
    onEventCompleted: item => {
      addOrUpdateFeedItem(item);
    },
    onStatusChange: state => {
      updateStreamStatusUI(state);
    },
    onInFlightChange: count => {
      const el = $('inflightTag');
      if (el) {
        el.textContent = `In-Flight: ${count}`;
        el.className = 'inflight-tag ' + (count > 0 ? 'active' : 'idle');
      }
    }
  });

  function updateStreamStatusUI(state) {
    const badge = $('streamStatusBadge');
    const dot = $('streamStatusDot');
    const text = $('streamStatusText');
    const startBtn = $('streamStartBtn');
    const pauseBtn = $('streamPauseBtn');
    const stopBtn = $('streamStopBtn');

    if (!badge) return;

    badge.className = `status-badge ${state.toLowerCase()}`;
    text.textContent = state;

    if (state === 'RUNNING') {
      startBtn.disabled = true;
      pauseBtn.disabled = false;
      pauseBtn.textContent = '⏸ Pause';
      stopBtn.disabled = false;
    } else if (state === 'PAUSED') {
      startBtn.disabled = true;
      pauseBtn.disabled = false;
      pauseBtn.textContent = '▶ Resume';
      stopBtn.disabled = false;
    } else {
      // STOPPED
      startBtn.disabled = false;
      pauseBtn.disabled = true;
      pauseBtn.textContent = '⏸ Pause';
      stopBtn.disabled = true;
    }
  }

  // Stream controls
  $('streamStartBtn')?.addEventListener('click', () => {
    producer.start();
    toast('Live event stream started.');
  });

  $('streamPauseBtn')?.addEventListener('click', () => {
    if (producer.state === 'RUNNING') {
      producer.pause();
      toast('Live stream paused.');
    } else if (producer.state === 'PAUSED') {
      producer.resume();
      toast('Live stream resumed.');
    }
  });

  $('streamStopBtn')?.addEventListener('click', async () => {
    await producer.stop();
    toast('Live stream stopped.');
    await refresh();
  });

  // Sliders
  $('streamRate')?.addEventListener('input', e => {
    const val = Number(e.target.value);
    $('rateDisplay').textContent = `${val.toFixed(1)} eps`;
    producer.setRate(val);
  });

  $('streamDupRatio')?.addEventListener('input', e => {
    const val = Number(e.target.value);
    $('dupRatioDisplay').textContent = `${val}%`;
    producer.setDuplicateProbability(val / 100);
  });

  // Manual event submission form
  // Requirement: Keep existing manual event submission support
  if ($('genIdBtn')) {
    $('genIdBtn').onclick = () => {
      $('eventId').value = generateEventId();
      toast('Generated new UUIDv4');
    };
  }

  if ($('nowBtn')) {
    $('nowBtn').onclick = () => {
      $('eventTs').value = (Date.now() / 1000).toFixed(3);
    };
  }

  if ($('eventTs')) {
    $('eventTs').value = (Date.now() / 1000).toFixed(3);
  }

  $('eventForm')?.addEventListener('submit', async e => {
    e.preventDefault();
    let data;
    try {
      data = JSON.parse($('eventData').value || 'null');
    } catch {
      toast('Payload must be valid JSON.');
      return;
    }

    const id = $('eventId').value.trim() || generateEventId();
    const ts = Number($('eventTs').value);

    // Immediate optimistic feed row in pending state
    const manualItem = {
      id,
      ts,
      data,
      isReplay: false,
      status: 'pending',
      attempt: 1,
      createdAt: Date.now()
    };
    addOrUpdateFeedItem(manualItem);

    try {
      await action(e.submitter, async () => {
        const res = await api('/api/events', {
          method: 'POST',
          body: JSON.stringify({ id, ts, data })
        });
        manualItem.status = res.status || 'accepted';
        addOrUpdateFeedItem(manualItem);
        return res;
      }, 'Event submitted to engine.');
    } catch (err) {
      manualItem.status = 'failed';
      addOrUpdateFeedItem(manualItem);
    }
  });

  // Quick actions: Demo & Drain
  $('demoBtn')?.addEventListener('click', async () => {
    try {
      await action($('demoBtn'), () => api('/api/demo', { method: 'POST', body: '{}' }), 'Demo batch executed — duplicate IDs blocked.');
    } catch {}
  });

  $('drainBtn')?.addEventListener('click', async () => {
    try {
      await action($('drainBtn'), () => api('/api/drain', { method: 'POST', body: '{}' }), 'Reorder buffer flush requested.');
    } catch {}
  });

  $('refreshBtn')?.addEventListener('click', refresh);

  // Navigation
  document.querySelectorAll('.nav').forEach(a => {
    a.onclick = () => {
      document.querySelectorAll('.nav').forEach(n => n.classList.remove('active'));
      a.classList.add('active');
    };
  });

  // Chaos lab
  $('faultBtn')?.addEventListener('click', async () => {
    const out = $('faultResult');
    if (!out) return;
    out.classList.remove('success');
    out.textContent = '◌ Injecting failures and retrying the stream…';
    try {
      const r = await action($('faultBtn'), () => api('/api/fault-lab', {
        method: 'POST',
        body: JSON.stringify({ count: 500, duplicate_ratio: 0.4 })
      }), 'Fault injection completed.');
      out.classList.toggle('success', r.exactly_once);
      out.textContent = `${r.exactly_once ? 'PASS' : 'CHECK FAILED'} — ${fmt(r.sink_effects)} effects / ${fmt(r.unique_expected)} unique expected. KV retries ${r.kv_retries}, sink retries ${r.sink_retries}, client retries ${r.client_retries}.`;
    } catch (e) {
      out.textContent = `Experiment error: ${e.message}`;
    }
  });

  // Benchmark
  $('benchBtn')?.addEventListener('click', async () => {
    try {
      const r = await action($('benchBtn'), () => api('/api/benchmark', {
        method: 'POST',
        body: JSON.stringify({
          count: Number($('benchCount').value),
          duplicate_ratio: Number($('benchRatio').value)
        })
      }), 'Benchmark finished.');
      $('benchResults').innerHTML = `
        <div><label>THROUGHPUT</label><b>${fmt(r.throughput_eps)} <small>events/sec</small></b></div>
        <div><label>P50 LATENCY</label><b>${r.p50_ms} <small>ms</small></b></div>
        <div><label>P95 LATENCY</label><b>${r.p95_ms} <small>ms</small></b></div>
        <div><label>EXACTLY-ONCE CHECK</label><b class="${r.exactly_once ? 'green' : 'red'}">${r.exactly_once ? 'PASS' : 'FAIL'}</b></div>
      `;
      toast(`Benchmark: ${fmt(r.throughput_eps)} events/sec.`);
    } catch {}
  });

  // Initial load and periodic refresh
  refresh();
  setInterval(refresh, 2500);
}

if (typeof window !== 'undefined') {
  window.addEventListener('DOMContentLoaded', initDashboard);
}

// Export for Node.js unit testing
if (typeof module !== 'undefined' && module.exports) {
  module.exports = {
    generateEventId,
    generateRealisticPayload,
    StreamProducer
  };
}
