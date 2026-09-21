'use strict';

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const { STATES, newToken } = require('./memoryStore');

/**
 * File-backed job store.
 *
 * Same contract as MemoryStore, but the job table lives in a JSON file so the
 * queue survives a process restart. Writes are serialised through an internal
 * promise chain (a single-process mutex) and performed atomically via
 * write-to-temp + rename.
 */
class FileStore {
  constructor(filePath) {
    this.filePath = filePath;
    this.jobs = new Map();
    this._chain = Promise.resolve();
    this._load();
  }

  _load() {
    if (!fs.existsSync(this.filePath)) return;
    const raw = fs.readFileSync(this.filePath, 'utf8');
    if (!raw.trim()) return;
    const data = JSON.parse(raw);
    for (const record of data.jobs || []) {
      this.jobs.set(record.id, record);
    }
  }

  /** Serialise a mutation and persist the table afterwards. */
  _mutate(fn) {
    const run = this._chain.then(() => {
      const result = fn();
      this._persist();
      return result;
    });
    // Keep the chain alive even if a mutation throws.
    this._chain = run.then(
      () => undefined,
      () => undefined
    );
    return run;
  }

  _persist() {
    const dir = path.dirname(this.filePath);
    fs.mkdirSync(dir, { recursive: true });
    const tmp = this.filePath + '.tmp' + process.pid;
    const payload = JSON.stringify({ version: 1, jobs: Array.from(this.jobs.values()) });
    fs.writeFileSync(tmp, payload, 'utf8');
    fs.renameSync(tmp, this.filePath);
  }

  enqueue(job, now = Date.now()) {
    const id = job.id || crypto.randomUUID();
    // Synchronous so callers can rely on the id being usable immediately.
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
    this._persist();
    return id;
  }

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
    this._persist();

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

  renew(jobId, leaseToken, now, leaseMs) {
    const record = this.jobs.get(jobId);
    if (!record || record.state !== STATES.LEASED) return false;
    if (record.leaseToken !== leaseToken) return false;
    if (record.leaseExpiresAt <= now) return false;

    record.leaseExpiresAt = now + leaseMs;
    this._persist();
    return true;
  }

  /**
   * Finish a job. The lease token is verified so that a worker which lost its
   * lease (and therefore no longer owns the job) cannot remove it.
   */
  ack(jobId, leaseToken) {
    const record = this.jobs.get(jobId);
    if (!record) return false;
    if (record.state !== STATES.LEASED) return false;
    if (record.leaseToken !== leaseToken) return false;

    this.jobs.delete(jobId);
    this._persist();
    return true;
  }

  nack(jobId, leaseToken, now, delayMs = 0) {
    const record = this.jobs.get(jobId);
    if (!record || record.state !== STATES.LEASED) return false;
    if (record.leaseToken !== leaseToken) return false;

    record.state = STATES.PENDING;
    record.leaseToken = null;
    record.leaseExpiresAt = 0;
    record.notBefore = now + delayMs;
    this._persist();
    return true;
  }

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
    if (recovered > 0) this._persist();
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

module.exports = { FileStore };
