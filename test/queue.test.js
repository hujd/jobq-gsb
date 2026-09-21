'use strict';

const test = require('node:test');
const assert = require('node:assert');

const { Queue, MemoryStore } = require('../src');

// Fixed clock base so these tests are deterministic: every method that looks at
// time is passed an explicit value derived from T0.
const T0 = 1700000000000;

function makeQueue(overrides) {
  return new Queue({
    store: new MemoryStore(),
    config: Object.assign({ leaseMs: 1000, pollIntervalMs: 10, backoffMs: 50, maxAttempts: 3 }, overrides),
  });
}

test('enqueue returns an id and the job is reservable', () => {
  const queue = makeQueue();
  const id = queue.enqueue('send-email', { to: 'a@b.c' }, { id: 'j1', now: T0 });
  assert.strictEqual(id, 'j1');

  const reserved = queue.reserve(T0);
  assert.ok(reserved, 'expected the job to be reservable');
  assert.strictEqual(reserved.job.id, 'j1');
  assert.strictEqual(reserved.job.name, 'send-email');
  assert.deepStrictEqual(reserved.job.payload, { to: 'a@b.c' });
});

test('reserve returns null when the queue is empty', () => {
  const queue = makeQueue();
  assert.strictEqual(queue.reserve(T0), null);
});

test('a reserved job is not handed out twice', () => {
  const queue = makeQueue();
  queue.enqueue('work', {}, { id: 'j1', now: T0 });

  const first = queue.reserve(T0);
  assert.ok(first, 'expected the first reserve to succeed');
  assert.strictEqual(queue.reserve(T0), null);
});

test('ack removes the job from the queue', () => {
  const queue = makeQueue();
  queue.enqueue('work', {}, { id: 'j1', now: T0 });
  const { leaseToken } = queue.reserve(T0);

  assert.strictEqual(queue.ack('j1', leaseToken, T0 + 1), true);
  assert.strictEqual(queue.stats().total, 0);
});

test('nack schedules a retry after the backoff delay', () => {
  const queue = makeQueue({ backoffMs: 50 });
  queue.enqueue('work', {}, { id: 'j1', now: T0 });

  const { job, leaseToken } = queue.reserve(T0);
  queue.nack('j1', leaseToken, job, T0);

  // Not ready yet: the backoff has not elapsed.
  assert.strictEqual(queue.reserve(T0 + 10), null);
  // Ready once the backoff has elapsed.
  const retried = queue.reserve(T0 + 60);
  assert.ok(retried, 'expected the job back after the backoff');
  assert.strictEqual(retried.job.attempts, 2);
});

test('a job is dropped once maxAttempts is exhausted', () => {
  const queue = makeQueue({ maxAttempts: 2, backoffMs: 0 });

  queue.enqueue('work', {}, { id: 'j1', now: T0 });

  let reserved = queue.reserve(T0);
  assert.strictEqual(reserved.job.attempts, 1);
  queue.nack('j1', reserved.leaseToken, reserved.job, T0);

  reserved = queue.reserve(T0 + 1);
  assert.strictEqual(reserved.job.attempts, 2);
  queue.nack('j1', reserved.leaseToken, reserved.job, T0 + 1);

  assert.strictEqual(queue.stats().total, 0);
});

test('a job is not reservable before its notBefore time', () => {
  const queue = makeQueue();
  queue.enqueue('delayed', {}, { id: 'j1', now: T0, notBefore: T0 + 500 });

  assert.strictEqual(queue.reserve(T0), null);
  assert.strictEqual(queue.reserve(T0 + 499), null);
  assert.ok(queue.reserve(T0 + 500), 'expected the job to become reservable at notBefore');
});
