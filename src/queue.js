'use strict';

const crypto = require('crypto');
const { resolveConfig } = require('./config');

/**
 * Job queue facade: owns the store and the queue-level policy
 * (attempt counting, backoff, lease duration).
 */
class Queue {
  constructor(options = {}) {
    if (!options.store) throw new Error('Queue requires a store');
    this.store = options.store;
    this.config = resolveConfig(options.config);
    this.handlers = new Map();
  }

  /** Register a handler for a job name. */
  handle(name, fn) {
    this.handlers.set(name, fn);
    return this;
  }

  getHandler(name) {
    return this.handlers.get(name);
  }

  /**
   * Add a job. Returns the job id.
   * `opts.now` lets callers inject a deterministic timestamp.
   */
  enqueue(name, payload, opts = {}) {
    const now = opts.now !== undefined ? opts.now : Date.now();
    return this.store.enqueue(
      {
        id: opts.id || crypto.randomUUID(),
        name,
        payload,
        attempts: opts.attempts || 0,
        notBefore: opts.notBefore || now,
      },
      now
    );
  }

  /** Reserve the next ready job for this worker. */
  reserve(now = Date.now()) {
    return this.store.reserve(now, this.config.leaseMs);
  }

  renew(jobId, leaseToken, now = Date.now()) {
    return this.store.renew(jobId, leaseToken, now, this.config.leaseMs);
  }

  ack(jobId, leaseToken, now = Date.now()) {
    return this.store.ack(jobId, leaseToken, now);
  }

  /**
   * Return a failed job to the pool. Once maxAttempts is exhausted the job is
   * dropped (acknowledged) so it does not loop forever.
   */
  nack(jobId, leaseToken, job, now = Date.now()) {
    if (job && job.attempts >= this.config.maxAttempts) {
      return this.store.ack(jobId, leaseToken, now);
    }
    return this.store.nack(jobId, leaseToken, now, this.config.backoffMs);
  }

  recoverExpired(now = Date.now()) {
    return this.store.recoverExpired(now);
  }

  stats() {
    return this.store.stats();
  }
}

module.exports = { Queue };
