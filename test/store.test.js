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
