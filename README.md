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

## Known issue (open)

`test/exactlyOnce.test.js` is **flaky** — roughly one run in fifty fails with:

```
AssertionError: expected handler to run once, but ran 2 times
```

We have also seen the same symptom in production (4 workers, `leaseMs: 5000`,
handlers taking 2–8s): a job occasionally gets processed twice, which double-charges
the customer. Nothing in the suite currently reproduces it on demand, so the failure
is timing dependent.

Related note in the code: `MemoryStore#ack` deliberately skips the lease-token check
because "the worker aborts as soon as a renewal fails" — see the comment there.

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
