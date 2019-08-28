import crypto from "crypto";

export function getRoomName(participants, names, myPublicKey) {
  if (participants.length === 1) {
    return (names.get(myPublicKey) === '' || names.get(myPublicKey) === undefined) ? 'Me' : names.get(myPublicKey);
  }
  return participants
    .filter(participantPublicKey => participantPublicKey !== myPublicKey)
    .map(publicKey => {
      return names.get(publicKey)
    })
    .join(', ');
}

export function createDialogRoomId(firstPublicKey, secondPublicKey) {
  if (firstPublicKey === secondPublicKey) {
    return crypto.createHash('sha256').update([firstPublicKey]).digest('hex');
  }
  return crypto
    .createHash('sha256')
    .update([firstPublicKey, secondPublicKey].sort().join(';'))
    .digest('hex');
}