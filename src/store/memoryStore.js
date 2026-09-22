'use strict';

const crypto = require('crypto');

const STATES = {
  PENDING: 'pending',
  LEASED: 'leased',
};

function newToken() {
  return crypto.randomBytes(8).toString('hex');
}

/**
 * In-memory job store.
 *
 * A job is either `pending` (ready to be reserved, possibly not before
 * `notBefore`) or `leased` (held by exactly one worker until `leaseExpiresAt`).
 *
 * All methods take `now` explicitly so callers control the clock.
 */
class MemoryStore {
  constructor() {
    this.jobs = new Map();
  }

  enqueue(job, now = Date.now()) {
    const id = job.id || crypto.randomUUID();
    const record = {
      id,
      name: job.name,
      payload: job.payload !== undefined ? job.payload : null,
      attempts: job.attempts || 0,
      state: STATES.PENDING,
      notBefore: job.notBefore || now,
      leaseToken: null,
      leaseExpiresAt: 0,
      createdAt: now,
    };
    this.jobs.set(id, record);
    return id;
  }

  /**
   * Claim the oldest ready job for `leaseMs`.
   * Returns { job, leaseToken } or null when nothing is available.
   */
  reserve(now, leaseMs) {
    let picked = null;
    for (const record of this.jobs.values()) {
      if (record.state !== STATES.PENDING) continue;
      if (record.notBefore > now) continue;
      if (!picked || record.createdAt < picked.createdAt) picked = record;
    }
    if (!picked) return null;

    picked.state = STATES.LEASED;
    picked.leaseToken = newToken();
    picked.leaseExpiresAt = now + leaseMs;
    picked.attempts += 1;

    return {
      job: {
        id: picked.id,
        name: picked.name,
        payload: picked.payload,
        attempts: picked.attempts,
      },
      leaseToken: picked.leaseToken,
    };
  }

  /**
   * Extend the lease of a job we still hold.
   * Returns true when the lease was extended, false when we lost it.
   */
  renew(jobId, leaseToken, now, leaseMs) {
    const record = this.jobs.get(jobId);
    if (!record || record.state !== STATES.LEASED) return false;
    if (record.leaseToken !== leaseToken) return false;
    if (record.leaseExpiresAt <= now) return false;

    record.leaseExpiresAt = now + leaseMs;
    return true;
  }

  /**
   * Finish a job.
   *
   * The lease token is a fencing token: a worker whose lease expired and was
   * handed to somebody else must not be able to remove the job. This check is
   * what makes a lost lease harmless even if the stale worker keeps running.
   */
  ack(jobId, leaseToken) {
    const record = this.jobs.get(jobId);
    if (!record) return false;
    if (record.state !== STATES.LEASED) return false;
    if (record.leaseToken !== leaseToken) return false;

    this.jobs.delete(jobId);
    return true;
  }

  /**
   * Give the job back for a later attempt.
   */
  nack(jobId, leaseToken, now, delayMs = 0) {
    const record = this.jobs.get(jobId);
    if (!record || record.state !== STATES.LEASED) return false;
    if (record.leaseToken !== leaseToken) return false;

    record.state = STATES.PENDING;
    record.leaseToken = null;
    record.leaseExpiresAt = 0;
    record.notBefore = now + delayMs;
    return true;
  }

  /**
   * Return jobs whose lease expired to the pending pool.
   * Used after a crash / restart.
   */
  recoverExpired(now) {
    let recovered = 0;
    for (const record of this.jobs.values()) {
      if (record.state !== STATES.LEASED) continue;
      if (record.leaseExpiresAt > now) continue;
      record.state = STATES.PENDING;
      record.leaseToken = null;
      record.leaseExpiresAt = 0;
      record.notBefore = now;
      recovered += 1;
    }
    return recovered;
  }

  stats() {
    let pending = 0;
    let leased = 0;
    for (const record of this.jobs.values()) {
      if (record.state === STATES.LEASED) leased += 1;
      else pending += 1;
    }
    return { total: this.jobs.size, pending, leased };
  }
}

module.exports = { MemoryStore, STATES, newToken };
