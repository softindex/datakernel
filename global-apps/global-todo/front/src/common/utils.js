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
