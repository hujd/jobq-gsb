'use strict';

/**
 * Queue-wide defaults. Values can be overridden per instance.
 *
 * leaseMs is intentionally short by default so that tests (and local demos)
 * exercise the lease-expiry path without waiting for seconds.
 */
const DEFAULTS = {
  leaseMs: 5000,
  // Renew the lease at half its lifetime so a single missed renewal does not
  // immediately expose the job to another worker.
  heartbeatRatio: 0.5,
  maxAttempts: 3,
  backoffMs: 200,
  // How often an idle worker polls the store when there is no work.
  pollIntervalMs: 200,
};

function resolveConfig(overrides) {
  return Object.assign({}, DEFAULTS, overrides || {});
}

module.exports = { DEFAULTS, resolveConfig };
