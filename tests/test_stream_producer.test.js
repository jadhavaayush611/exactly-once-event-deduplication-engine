/**
 * Unit and integration tests for the Live Stream Producer and Automatic UUID Idempotency.
 * Run with: node --test tests/test_stream_producer.test.js
 */

const test = require('node:test');
const assert = require('node:assert');
const { generateEventId, generateRealisticPayload, StreamProducer } = require('../web/app.js');

test('Automatic UUID generation conforms to RFC 4122 v4', () => {
  const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
  for (let i = 0; i < 50; i++) {
    const id = generateEventId();
    assert.match(id, uuidRegex, `Expected valid UUIDv4, got ${id}`);
  }
});

test('Successive UUID generations produce unique IDs', () => {
  const set = new Set();
  const count = 1000;
  for (let i = 0; i < count; i++) {
    const id = generateEventId();
    assert.strictEqual(set.has(id), false, `Collision detected on UUID ${id}`);
    set.add(id);
  }
  assert.strictEqual(set.size, count);
});

test('Realistic payload generator produces varied domains and structured objects', () => {
  const seenDomains = new Set();
  for (let i = 0; i < 50; i++) {
    const p = generateRealisticPayload();
    assert.ok(p && typeof p === 'object', 'Payload should be a valid object');
    assert.ok(p.domain, 'Payload should have a domain');
    assert.ok(p.type, 'Payload should have an event type');
    seenDomains.add(p.domain);
  }
  // Should see multiple domain types (e.g. fintech, logistics, inventory, telemetry, security)
  assert.ok(seenDomains.size >= 3, `Expected at least 3 distinct domains, saw: ${Array.from(seenDomains).join(', ')}`);
});

test('StreamProducer state transitions: start -> pause -> resume -> stop', async () => {
  const statusHistory = [];
  const producer = new StreamProducer({
    rate: 10,
    onStatusChange: s => statusHistory.push(s)
  });

  assert.strictEqual(producer.state, 'STOPPED');

  producer.start();
  assert.strictEqual(producer.state, 'RUNNING');

  producer.pause();
  assert.strictEqual(producer.state, 'PAUSED');

  producer.resume();
  assert.strictEqual(producer.state, 'RUNNING');

  await producer.stop();
  assert.strictEqual(producer.state, 'STOPPED');

  assert.deepStrictEqual(statusHistory, ['RUNNING', 'PAUSED', 'RUNNING', 'STOPPED']);
});

test('StreamProducer with 0% duplicate probability produces unique logical events', () => {
  const producer = new StreamProducer({ duplicateProbability: 0 });
  const ids = new Set();

  for (let i = 0; i < 30; i++) {
    const ev = producer.createEvent();
    assert.strictEqual(ev.isReplay, false);
    assert.strictEqual(ids.has(ev.id), false, `Event ID ${ev.id} should be unique`);
    ids.add(ev.id);
  }
  assert.strictEqual(ids.size, 30);
});

test('StreamProducer with 100% duplicate probability reuses identical ID and payload', () => {
  const producer = new StreamProducer({ duplicateProbability: 0 });
  // Seed first event
  const first = producer.createEvent();
  assert.strictEqual(producer.recentLogicalEvents.length, 1);

  // Set duplicate probability to 1.0 (100%)
  producer.setDuplicateProbability(1.0);

  for (let i = 0; i < 10; i++) {
    const dup = producer.createEvent();
    assert.strictEqual(dup.isReplay, true);
    assert.strictEqual(dup.id, first.id, 'Duplicate must reuse the original ID');
    assert.deepStrictEqual(dup.data, first.data, 'Duplicate must reuse the original payload');
  }
});

test('In-flight requests tracking and clean stop', async () => {
  let inflightObservedMax = 0;
  let inflightCurrent = 0;

  // Mock fetch function with controlled delay
  const mockFetch = async () => {
    inflightCurrent++;
    if (inflightCurrent > inflightObservedMax) {
      inflightObservedMax = inflightCurrent;
    }
    await new Promise(r => setTimeout(r, 40));
    inflightCurrent--;
    return { status: 'accepted' };
  };

  const producer = new StreamProducer({
    rate: 25,
    fetchFn: mockFetch,
    duplicateProbability: 0.2
  });

  producer.start();
  // Let it emit several events
  await new Promise(r => setTimeout(r, 120));

  assert.ok(producer.metrics.submitted > 0, 'Should have submitted events');

  // Stop the producer cleanly
  await producer.stop();
  assert.strictEqual(producer.state, 'STOPPED');
  assert.strictEqual(producer.inFlightRequests.size, 0, 'In-flight set must be empty after stop');
});

test('Retry behavior reuses identical ID and payload on failure with backoff', async () => {
  let attemptsSeen = 0;
  const idsSeen = [];
  const payloadsSeen = [];

  // Mock fetch that fails on attempt 1 & 2, then succeeds on attempt 3
  const mockFetch = async (url, opts) => {
    attemptsSeen++;
    const body = JSON.parse(opts.body);
    idsSeen.push(body.id);
    payloadsSeen.push(body.data);

    if (attemptsSeen < 3) {
      throw new Error(`Transient network failure attempt ${attemptsSeen}`);
    }
    return { status: 'accepted' };
  };

  const completedStatuses = [];
  const producer = new StreamProducer({
    fetchFn: mockFetch,
    onEventCompleted: item => completedStatuses.push(item.status)
  });

  const feedItem = {
    id: generateEventId(),
    ts: Date.now() / 1000,
    data: { domain: 'fintech', amount: 99.5 },
    status: 'pending'
  };

  const result = await producer._sendWithRetry(feedItem);
  assert.strictEqual(result.status, 'accepted');
  assert.strictEqual(attemptsSeen, 3, 'Should have retried 2 times (total 3 attempts)');
  assert.strictEqual(idsSeen[0], idsSeen[1]);
  assert.strictEqual(idsSeen[1], idsSeen[2]);
  assert.deepStrictEqual(payloadsSeen[0], payloadsSeen[1]);
  assert.deepStrictEqual(payloadsSeen[1], payloadsSeen[2]);

  // Should have transitioned through retrying to accepted
  assert.ok(completedStatuses.includes('retrying'), 'Should have signaled retrying status');
  assert.strictEqual(feedItem.status, 'accepted');
});

test('Retry failure when all attempts are exhausted sets status to failed', async () => {
  const mockFailingFetch = async () => {
    throw new Error('Permanent downstream outage');
  };

  const producer = new StreamProducer({
    fetchFn: mockFailingFetch
  });

  const feedItem = {
    id: generateEventId(),
    ts: Date.now() / 1000,
    data: { test: true },
    status: 'pending'
  };

  const result = await producer._sendWithRetry(feedItem);
  assert.strictEqual(result, null);
  assert.strictEqual(feedItem.status, 'failed');
  assert.strictEqual(producer.metrics.failed, 1);
});
