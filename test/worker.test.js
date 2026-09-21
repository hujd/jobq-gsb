'use strict';

const test = require('node:test');
const assert = require('node:assert');

const { Queue, Worker, MemoryStore } = require('../src');
const { sleep } = require('./helpers/sleep');

function makeQueue(overrides) {
  return new Queue({
    store: new MemoryStore(),
    config: Object.assign({ leaseMs: 500, pollIntervalMs: 5, backoffMs: 1, maxAttempts: 3 }, overrides),
  });
}

test('worker runs a job and removes it from the queue', async () => {
  const queue = makeQueue();
  const seen = [];

  const worker = new Worker(queue, {
    pollIntervalMs: 5,
    handler: async (job) => {
      seen.push(job.id);
    },
  }).start();

  queue.enqueue('work', {}, { id: 'j1' });
  await sleep(60);
  await worker.stop();

  assert.deepStrictEqual(seen, ['j1']);
  assert.strictEqual(queue.stats().total, 0);
  assert.strictEqual(worker.stats.done, 1);
});

test('worker passes a context with workerId and an AbortSignal', async () => {
  const queue = makeQueue();
  let ctx = null;

  const worker = new Worker(queue, {
    pollIntervalMs: 5,
    id: 'w-test',
    handler: async (job, c) => {
      ctx = c;
    },
  }).start();

  queue.enqueue('work', {}, { id: 'j1' });
  await sleep(50);
  await worker.stop();

  assert.ok(ctx, 'handler was never called');
  assert.strictEqual(ctx.workerId, 'w-test');
  assert.ok(ctx.signal, 'context is missing an AbortSignal');
  assert.strictEqual(ctx.signal.aborted, false);
});

test('worker retries a failing job and eventually drops it', async () => {
  const queue = makeQueue({ maxAttempts: 2 });
  let calls = 0;

  const worker = new Worker(queue, {
    pollIntervalMs: 5,
    handler: async () => {
      calls += 1;
      throw new Error('boom');
    },
  }).start();

  queue.enqueue('work', {}, { id: 'j1' });
  await sleep(120);
  await worker.stop();

  assert.strictEqual(calls, 2);
  assert.strictEqual(queue.stats().total, 0);
  assert.strictEqual(worker.stats.failed, 2);
});

test('idle workers do not spin the store into handing out extra jobs', async () => {
  const queue = makeQueue();

  const workers = [0, 1, 2].map(
    (i) =>
      new Worker(queue, {
        pollIntervalMs: 5,
        id: 'idle-' + i,
        handler: async () => {
          await sleep(10);
        },
      }).start()
  );

  await sleep(40);
  await Promise.all(workers.map((w) => w.stop()));

  assert.strictEqual(queue.stats().total, 0);
  assert.strictEqual(
    workers.reduce((sum, w) => sum + w.stats.reserved, 0),
    0
  );
});
