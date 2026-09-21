'use strict';

const test = require('node:test');
const assert = require('node:assert');

const { Queue, Worker, MemoryStore } = require('../src');
const { sleep } = require('./helpers/sleep');

// Most batches are small. A few percent are big enough that parsing them
// synchronously occupies the event loop for longer than a lease window.
const SMALL_BATCH = '[' + '1,'.repeat(9999) + '1]';
const BIG_BATCH = '[' + '1,'.repeat(2999999) + '1]';
const BIG_BATCH_RATE = 0.04;

/**
 * A job must be executed exactly once, even when a handler briefly overruns the
 * lease window and another worker picks the job up.
 */
test('handler runs exactly once under lease contention', async () => {
  const leaseMs = 40;
  const store = new MemoryStore();
  const queue = new Queue({
    store,
    config: { leaseMs, pollIntervalMs: 5, backoffMs: 1, maxAttempts: 1 },
  });

  const runs = new Map();
  const handler = async (job, ctx) => {
    const payload = Math.random() < BIG_BATCH_RATE ? BIG_BATCH : SMALL_BATCH;
    JSON.parse(payload); // synchronous: no timer can fire while this runs

    if (ctx.signal.aborted) return;
    await sleep(leaseMs, ctx.signal);
    if (ctx.signal.aborted) return;

    runs.set(job.id, (runs.get(job.id) || 0) + 1);
  };

  const workers = [0, 1, 2].map((i) =>
    new Worker(queue, { handler, pollIntervalMs: 5, id: 'w-' + i }).start()
  );

  queue.enqueue('charge', { amount: 10 }, { id: 'job-1' });

  await sleep(leaseMs * 12);
  await Promise.all(workers.map((w) => w.stop()));

  assert.strictEqual(
    runs.get('job-1'),
    1,
    `expected handler to run once, but ran ${runs.get('job-1')} times`
  );
});
