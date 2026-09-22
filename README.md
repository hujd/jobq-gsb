# jobq

A small in-process job queue with **lease-based reservation**, used by our billing
and notification pipelines. Zero third-party dependencies — everything runs on the
Node.js standard library (Node 18+).

## Concepts

| Concept | Meaning |
|---|---|
| `enqueue(name, payload)` | Add a job. Returns the job id. |
| `reserve(now)` | Claim the next ready job for `config.leaseMs`. Only one worker can hold a job at a time. |
| `renew(jobId, leaseToken, now)` | Extend the lease of a job you still hold. Returns `false` if the lease was lost. |
| `ack(jobId, leaseToken, now)` | Mark a job as finished and remove it. |
| `nack(jobId, leaseToken, job, now)` | Give the job back for a retry (with `config.backoffMs`). Once `maxAttempts` is reached the job is dropped. |
| `recoverExpired(now)` | Return leases that expired (crashed worker) to the pending pool. |

Every job is handed to a handler as `handler(job, ctx)` where
`ctx = { workerId, signal }`. `signal` is an `AbortSignal` that is supposed to fire
when the worker loses the lease for that job, so long-running handlers can stop
instead of duplicating work.

## Stores

Two interchangeable backends:

- `MemoryStore` — in-process `Map`, no persistence.
- `FileStore` — JSON file on disk, survives restarts, serialises writes through an
  internal mutex and commits atomically (temp file + rename).

Both are expected to implement the exact same semantics.

## Usage

```js
const { Queue, Worker, MemoryStore } = require('./src');

const queue = new Queue({ store: new MemoryStore(), config: { leaseMs: 5000 } });

queue.handle('charge', async (job, ctx) => {
  await chargeOnce(job.payload.amount, { signal: ctx.signal });
});

new Worker(queue, { pollIntervalMs: 200 }).start();

queue.enqueue('charge', { amount: 10 });
```

## Tests

```bash
node --test            # whole suite
node --test test/exactlyOnce.test.js
```

## Exactly-once under lease expiry

The lease token is a **fencing token**. When a handler blocks the event loop
(a large synchronous parse, a blocking native call, …) past `leaseMs`, the
heartbeat cannot fire and the lease expires; another worker may then reserve
the job. This is safe by construction:

1. As soon as a heartbeat renewal fails, the worker aborts `ctx.signal` and
   marks the run as lease-lost. A cooperative handler checks the signal and
   stops before committing any side effect (e.g. before the charge call).
2. A lease-lost run never calls `ack`/`nack`. Both stores additionally verify
   the lease token on every `ack` and `nack`, so even a stray call with a stale
   token cannot remove or release a job that another worker now owns.

Handlers still own their side effects: respect `ctx.signal` around awaits and
immediately before external calls, exactly as the usage example shows.

`test/exactlyOnce.test.js` covers the timing-dependent case;
`test/exactlyOnceRegression.test.js` deterministically blocks the event loop
past the lease and must reproduce a double execution on the pre-fix code on
every run.

## Layout

```
src/
  index.js              public exports
  config.js             defaults (leaseMs, heartbeatRatio, maxAttempts, backoffMs)
  queue.js              queue facade: attempts, backoff, lease policy
  worker.js             polling worker pool + lease heartbeats
  store/memoryStore.js  in-memory backend
  store/fileStore.js    file-backed backend
bin/jobq.js             demo CLI
test/                   node:test suite
```
