'use strict';

const test = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const os = require('os');
const path = require('path');

const { Queue, Worker, MemoryStore, FileStore } = require('../src');
const { sleep } = require('./helpers/sleep');

/**
 * Synchronously freeze the event loop for `ms`, exactly like a long parse or a
 * blocking native call does in production. Neither heartbeats nor other
 * workers' poll timers can fire while this runs.
 */
function blockEventLoop(ms) {
  const deadline = Date.now() + ms;
  while (Date.now() < deadline) {
    // busy wait
  }
}

const BACKENDS = [
  ['memoryStore', () => new MemoryStore()],
  [
    'fileStore',
    () => new FileStore(path.join(fs.mkdtempSync(path.join(os.tmpdir(), 'jobq-')), 'jobs.json')),
  ],
];

for (const [label, makeStore] of BACKENDS) {
  test(`[${label}] deterministic lease-expiry race executes the job exactly once`, async () => {
    const leaseMs = 40;
    const store = makeStore();
    const queue = new Queue({
      store,
      config: { leaseMs, pollIntervalMs: 5, backoffMs: 1, maxAttempts: 3 },
    });

    // Every commit of the business side effect (the "charge") is recorded.
    const charges = [];

    // The first worker wins the initial reserve because it is started first
    // (reserve happens synchronously inside start()). Its handler models a
    // billing job: a synchronous chunk of work that overruns the ENTIRE lease,
    // then an async checkpoint where a cooperative handler expects the abort
    // signal to fire if the lease was lost.
    const blockingWorker = new Worker(
      queue,
      {
        id: 'blocker',
        handler: async (job, ctx) => {
          blockEventLoop(leaseMs * 3);

          // Yield to the event loop before committing the side effect, like a
          // real handler waiting on its payment gateway. This is where the
          // lost-lease heartbeat must abort us.
          await sleep(10, ctx.signal);
          if (ctx.signal.aborted) return;

          charges.push(job.id + '@' + ctx.workerId);
        },
      }
    );

    const fastHandler = async (job, ctx) => {
      if (ctx.signal.aborted) return;
      await sleep(2, ctx.signal);
      if (ctx.signal.aborted) return;
      charges.push(job.id + '@' + ctx.workerId);
    };

    blockingWorker.start();
    const backupA = new Worker(queue, { id: 'backup-a', handler: fastHandler }).start();
    const backupB = new Worker(queue, { id: 'backup-b', handler: fastHandler }).start();

    queue.enqueue('charge', { amount: 10 }, { id: 'job-1' });

    await sleep(leaseMs * 8);
    await Promise.all([blockingWorker.stop(), backupA.stop(), backupB.stop()]);

    assert.strictEqual(
      charges.length,
      1,
      `expected exactly one charge, got ${charges.length}: ${charges.join(', ')}`
    );
    assert.strictEqual(queue.stats().total, 0);
  });
}
