import crypto from "crypto";
import {createMapOTSystem} from 'global-apps-common';

export const RETRY_TIMEOUT = 1000;

export const profileOTSystem = createMapOTSystem((left, right) => left.localeCompare(right));

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
      return null;
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
