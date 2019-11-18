import crypto from "crypto";
import {CancelablePromise} from 'global-apps-common';

export const RETRY_TIMEOUT = 1000;

export function getRoomName(participants, names, myPublicKey) {
  if (participants.length === 1) {
    return names.get(myPublicKey) || 'Me';
  }

  const resolvedNames = [];

  for (const participantPublicKey of participants) {
    if (participantPublicKey === myPublicKey) {
      continue;
    }

    if (!names.has(participantPublicKey)) {
      return '';
    }

    resolvedNames.push(names.get(participantPublicKey));
  }

  return resolvedNames.join(', ');
}

export function createDialogRoomId(firstPublicKey, secondPublicKey) {
  return crypto
    .createHash('sha256')
    .update([...new Set([firstPublicKey, secondPublicKey])].sort().join(';'))
    .digest('hex');
}

export class TimeoutError extends Error {
  constructor(message = 'Timeout error') {
    super(message);

    // Maintains proper stack trace for where our error was thrown (only available on V8)
    if (Error.captureStackTrace) {
      Error.captureStackTrace(this, TimeoutError);
    }

    this.name = 'TimeoutError';
  }
}

export function timeout(promise, timeout) {
  let resolveTimeout;
  let timeoutId;

  return CancelablePromise.all([
    promise.finally(() => {
      clearTimeout(timeoutId);
      resolveTimeout();
    }),
    new CancelablePromise((resolve, reject) => {
      resolveTimeout = resolve;
      timeoutId = setTimeout(() => {
        reject(new TimeoutError());
      }, timeout);
    })
  ]).then(([result]) => result);
}
