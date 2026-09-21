'use strict';

const crypto = require('crypto');

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Worker: polls the queue, runs handlers, keeps its leases alive.
 *
 * The worker hands the handler a context object `{ workerId, signal }`.
 * `signal` is an AbortSignal that is meant to fire when the worker loses the
 * lease for a job, so a long-running handler can bail out early instead of
 * duplicating work that another worker has already picked up.
 */
class Worker {
  constructor(queue, options = {}) {
    this.queue = queue;
    this.id = options.id || 'w-' + crypto.randomBytes(4).toString('hex');
    this.handler = options.handler || null;
    this.pollIntervalMs = options.pollIntervalMs || queue.config.pollIntervalMs;
    this.running = false;
    this.inFlight = 0;
    this.stats = { reserved: 0, done: 0, failed: 0, aborted: 0 };
  }

  start() {
    if (this.running) return this;
    this.running = true;
    this._loop();
    return this;
  }

  async stop() {
    this.running = false;
    // Give in-flight handlers a moment to settle.
    const deadline = Date.now() + 1000;
    while (this.inFlight > 0 && Date.now() < deadline) {
      await sleep(5);
    }
  }

  async _loop() {
    while (this.running) {
      let reserved = null;
      try {
        // Reclaim leases left behind by workers that died without acking.
        this.queue.recoverExpired();
        reserved = this.queue.reserve();
      } catch (err) {
        reserved = null;
      }
      if (!reserved) {
        await sleep(this.pollIntervalMs);
        continue;
      }
      this.stats.reserved += 1;
      await this._run(reserved);
    }
  }

  async _run(reserved) {
    const { job, leaseToken } = reserved;
    const handler = this.handler || this.queue.getHandler(job.name);
    if (!handler) {
      this.queue.ack(job.id, leaseToken);
      return;
    }

    const leaseMs = this.queue.config.leaseMs;
    const heartbeatMs = Math.max(1, Math.floor(leaseMs * this.queue.config.heartbeatRatio));

    const controller = new AbortController();
    const ctx = { workerId: this.id, signal: controller.signal };

    // Keep the lease alive for as long as the handler runs.
    const heartbeat = setInterval(() => {
      this.queue.renew(job.id, leaseToken);
    }, heartbeatMs);

    this.inFlight += 1;
    let failed = false;
    try {
      await handler(job, ctx);
    } catch (err) {
      failed = true;
    } finally {
      clearInterval(heartbeat);
      this.inFlight -= 1;
    }

    if (failed) {
      this.stats.failed += 1;
      this.queue.nack(job.id, leaseToken, job);
      return;
    }

    this.stats.done += 1;
    this.queue.ack(job.id, leaseToken);
  }
}

module.exports = { Worker };
