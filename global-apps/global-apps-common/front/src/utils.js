import {CancelablePromise} from './CancelablePromise';

export const ROOT_COMMIT_ID = 'AQAAAAAAAAA=';

const randomStringChars = '0123456789qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM';

export function delay(timeout) {
  let timeoutId;

  return new CancelablePromise(resolve => {
    timeoutId = setTimeout(resolve, timeout);
  }, () => {
    clearTimeout(timeoutId);
  });
}

export function randomString(length) {
  let result = '';
  for (let i = length; i > 0; --i) {
    result += randomStringChars[Math.floor(Math.random() * randomStringChars.length)];
  }
  return result;
}

export function wait(delay) {
  return new Promise(resolve => {
    setTimeout(resolve, delay);
  });
}

const emojiGroups = [
  [0x1F601, 0x1F64F],
  [0x1F680, 0x1F6C0]
];

export function toEmoji(str, length) {
  const count = emojiGroups.reduce((count, [from, to]) => {
    return count + to - from + 1;
  }, 0);

  let emoji = '';
  for (let i = 0; i < length; i++) {
    let emojiIndex = (str.charCodeAt(i * 3) + str.charCodeAt(i * 3 + 1) + str.charCodeAt(i * 3 + 2)) % count;

    for (const [startCode, endCode] of emojiGroups) {
      const countInGroup = endCode - startCode + 1;

      if (emojiIndex < countInGroup) {
        emoji += String.fromCodePoint(startCode + emojiIndex);
        break;
      }

      emojiIndex -= countInGroup;
    }
  }
  return emoji;
}

export class RejectionError extends Error {
  constructor(message = 'Promise has been cancelled') {
    super(message);

    // Maintains proper stack trace for where our error was thrown (only available on V8)
    if (Error.captureStackTrace) {
      Error.captureStackTrace(this, RejectionError);
    }

    this.name = 'RejectionError';
  }
}

export function retry(fn, timeout) {
  return new CancelablePromise((resolve, reject) => {
    fn().then(resolve, reject);
  }).catch(error => {
    if (!(error instanceof RejectionError)) {
      console.error(error);
      return delay(timeout).then(() => retry(fn, timeout));
    }
  });
}

export function getAppStoreContactName(contact) {
  if (contact === undefined || Object.keys(contact).length === 0) {
    return;
  }
  return contact.firstName !== '' && contact.lastName !== '' ?
    contact.firstName + ' ' + contact.lastName :
    contact.username;
}

export function getAvatarLetters(name) {
  if (!name) {
    return '';
  }
  const nameString = [...name];
  if (name.includes(" ")) {
    if (nameString[0].length === 2) {
      return (nameString[0][0] + nameString[0][1] + nameString[name.indexOf(" ") - 2]).toUpperCase()
    }
    return (nameString[0][0] + nameString[name.indexOf(" ") + 1]).toUpperCase()
  } else {
    return (name.length > 1 ?
      nameString[0].length === 2 ?
        nameString[0][0] + nameString[0][1] :
        nameString[0][0] + nameString[1] :
      nameString[0][0]).toUpperCase();
  }
}

export function isValidURL(urlString) {
  try {
    new URL(urlString);
    return true;
  } catch (err) {
    return false;
  }
}
