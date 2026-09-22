'use strict';

const test = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const os = require('os');
const path = require('path');

const { MemoryStore, FileStore } = require('../src');

const BACKENDS = [
  ['memoryStore', () => new MemoryStore()],
  [
    'fileStore',
    () => new FileStore(path.join(fs.mkdtempSync(path.join(os.tmpdir(), 'jobq-')), 'jobs.json')),
  ],
];

for (const [label, factory] of BACKENDS) {
  test(`[${label}] reserve hands out a pending job and marks it leased`, () => {
    const store = factory();
    store.enqueue({ id: 'j1', name: 'work' }, 0);

    const reserved = store.reserve(0, 1000);
    assert.ok(reserved);
    assert.strictEqual(reserved.job.id, 'j1');
    assert.ok(reserved.leaseToken);

    assert.strictEqual(store.stats().leased, 1);
    assert.strictEqual(store.reserve(0, 1000), null);
  });

  test(`[${label}] renew works while we still hold the lease`, () => {
    const store = factory();
    store.enqueue({ id: 'j1', name: 'work' }, 0);

    const { leaseToken } = store.reserve(0, 100);
    assert.strictEqual(store.renew('j1', leaseToken, 50, 100), true);
    // Lease was extended to 150, so it is still alive at 120.
    assert.strictEqual(store.reserve(120, 100), null);
  });

  test(`[${label}] renew fails for a foreign token`, () => {
    const store = factory();
    store.enqueue({ id: 'j1', name: 'work' }, 0);
    store.reserve(0, 100);

    assert.strictEqual(store.renew('j1', 'not-my-token', 10, 100), false);
  });

  test(`[${label}] ack and nack reject a stale lease token (fencing)`, () => {
    const store = factory();
    store.enqueue({ id: 'j1', name: 'work' }, 0);

    // Worker A takes the lease, then loses it after expiry.
    const first = store.reserve(0, 100);
    store.recoverExpired(200);

    // Worker B now legitimately owns the job.
    const second = store.reserve(200, 100);
    assert.ok(second);

    // A's stale token must not be able to nack or ack B's lease away.
    assert.strictEqual(store.nack('j1', first.leaseToken, 200, 0), false);
    assert.strictEqual(store.stats().leased, 1);
    assert.strictEqual(store.ack('j1', first.leaseToken), false);
    assert.strictEqual(store.stats().total, 1);

    // B can still finish the job.
    assert.strictEqual(store.ack('j1', second.leaseToken), true);
    assert.strictEqual(store.stats().total, 0);
  });

  test(`[${label}] nack returns the job to the pool`, () => {
    const store = factory();
    store.enqueue({ id: 'j1', name: 'work' }, 0);

    const { leaseToken } = store.reserve(0, 100);
    assert.strictEqual(store.nack('j1', leaseToken, 10, 0), true);

    const again = store.reserve(10, 100);
    assert.ok(again);
    assert.strictEqual(again.job.attempts, 2);
  });

  test(`[${label}] recoverExpired returns orphaned jobs to the pool`, () => {
    const store = factory();
    store.enqueue({ id: 'j1', name: 'work' }, 0);
    store.reserve(0, 100);

    assert.strictEqual(store.recoverExpired(50), 0);
    assert.strictEqual(store.recoverExpired(200), 1);
    assert.ok(store.reserve(200, 100));
  });

  test(`[${label}] a job scheduled in the future is not reserved early`, () => {
    const store = factory();
    store.enqueue({ id: 'j1', name: 'work', notBefore: 500 }, 0);
    assert.strictEqual(store.reserve(100, 100), null);
    assert.ok(store.reserve(500, 100));
  });
}
