'use strict';

/**
 * Sleep for `ms`.
 *
 * When an AbortSignal is passed the sleep rejects with an AbortError as soon as
 * the signal fires, so a handler can stop early instead of finishing work that
 * the queue has already handed to somebody else.
 */
function sleep(ms, signal) {
  return new Promise((resolve, reject) => {
    if (signal && signal.aborted) {
      return reject(makeAbortError());
    }

    const timer = setTimeout(() => {
      if (signal) signal.removeEventListener('abort', onAbort);
      resolve();
    }, ms);

    function onAbort() {
      clearTimeout(timer);
      if (signal) signal.removeEventListener('abort', onAbort);
      reject(makeAbortError());
    }

    if (signal) signal.addEventListener('abort', onAbort, { once: true });
  });
}

function makeAbortError() {
  const err = new Error('handler aborted: lease lost');
  err.name = 'AbortError';
  return err;
}

module.exports = { sleep, makeAbortError };
