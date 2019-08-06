export const ROOT_COMMIT_ID = 'AQAAAAAAAAA=';

const randomStringChars = '0123456789qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM';
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

export function getAvatarLetters(name) {
  const nameString = [...name];
  if (name === '') {
    return 'Me'
  }
  if (name.includes(" ")) {
    if (nameString[0].length === 2) {
      return nameString[0][0] + nameString[0][1] + nameString[name.indexOf(" ") - 2]
    }
    return nameString[0][0] + nameString[name.indexOf(" ") + 1]
  } else {
    return name.length > 1 ?
      nameString[0].length === 2 ?
        nameString[0][0] + nameString[0][1] :
        nameString[0][0] + nameString[1] :
      nameString[0][0];
  }
}
