'use strict';

const test = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const os = require('os');
const path = require('path');

const { Queue, Worker, MemoryStore, FileStore } = require('../src');
const { sleep } = require('./helpers/sleep');

const BACKENDS = [
  ['memoryStore', () => new MemoryStore()],
  [
    'fileStore',
    () => new FileStore(path.join(fs.mkdtempSync(path.join(os.tmpdir(), 'jobq-')), 'jobs.json')),
  ],
];

/**
 * Fencing: once a lease expires and the job is handed to a new owner, the
 * previous owner's lease token must be useless. A stale worker must not be
 * able to ack (delete) the job out from under the current owner.
 *
 * Fully deterministic: every method takes an explicit clock value.
 */
for (const [label, factory] of BACKENDS) {
  test(`[${label}] a stale lease token cannot ack a re-reserved job`, () => {
    const store = factory();
    store.enqueue({ id: 'j1', name: 'charge' }, 0);

    // Worker A reserves the job with a 100ms lease.
    const first = store.reserve(0, 100);
    assert.ok(first);

    // The lease expires and worker B recovers and re-reserves the job.
    assert.strictEqual(store.recoverExpired(150), 1);
    const second = store.reserve(150, 100);
    assert.ok(second);
    assert.notStrictEqual(second.leaseToken, first.leaseToken);

    // Worker A finally finishes and tries to ack with its stale token.
    assert.strictEqual(store.ack('j1', first.leaseToken, 160), false);
    // The job must still be there, owned by worker B.
    assert.strictEqual(store.stats().leased, 1);

    // Worker B (the legitimate owner) can ack.
    assert.strictEqual(store.ack('j1', second.leaseToken, 170), true);
    assert.strictEqual(store.stats().total, 0);
  });

  test(`[${label}] a stale lease token cannot nack or renew a re-reserved job`, () => {
    const store = factory();
    store.enqueue({ id: 'j1', name: 'charge' }, 0);

    const first = store.reserve(0, 100);
    store.recoverExpired(150);
    store.reserve(150, 100);

    assert.strictEqual(store.nack('j1', first.leaseToken, 160, 0), false);
    assert.strictEqual(store.renew('j1', first.leaseToken, 160, 100), false);
    assert.strictEqual(store.stats().leased, 1);
  });
}

/**
 * The production race, reproduced deterministically: the first handler
 * invocation blocks the event loop with a synchronous busy-wait far longer
 * than the lease, so the lease is guaranteed to expire and a second worker
 * is guaranteed to pick the job up. The worker that lost the lease must
 * abort its handler instead of completing it, so the job's side effect
 * (here: writing a receipt) happens exactly once.
 *
 * Fails on every run without the fencing fix; passes on every run with it.
 */
test('a worker that loses its lease aborts instead of duplicating the job', async () => {
  const leaseMs = 60;
  const store = new MemoryStore();
  const queue = new Queue({
    store,
    config: { leaseMs, pollIntervalMs: 5, backoffMs: 1, maxAttempts: 5 },
  });

  const receipts = [];
  let firstCall = true;
  const handler = async (job, ctx) => {
    if (firstCall) {
      firstCall = false;
      // Synchronous blocking: no timer (heartbeat, poll) can fire while this
      // runs, so the lease is guaranteed to expire underneath us.
      const end = Date.now() + leaseMs * 5;
      while (Date.now() < end);
    }
    if (ctx.signal.aborted) return;
    await sleep(leaseMs, ctx.signal);
    if (ctx.signal.aborted) return;
    receipts.push(job.id);
  };

  const workers = [0, 1].map((i) =>
    new Worker(queue, { handler, pollIntervalMs: 5, id: 'w-' + i }).start()
  );

  queue.enqueue('charge', { amount: 10 }, { id: 'job-1' });

  await sleep(leaseMs * 15);
  await Promise.all(workers.map((w) => w.stop()));

  assert.deepStrictEqual(
    receipts,
    ['job-1'],
    `expected exactly one receipt, got ${JSON.stringify(receipts)}`
  );
  const aborted = workers.reduce((sum, w) => sum + w.stats.aborted, 0);
  assert.ok(aborted >= 1, 'expected the stale worker to abort its handler');
  assert.strictEqual(queue.stats().total, 0, 'job must be acked exactly once');
});
