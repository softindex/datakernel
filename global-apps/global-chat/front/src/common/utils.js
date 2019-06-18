const eq = (a1, a2) => a1 === a2;

export function isArraysEqual(arr1, arr2, eqFn) {
  if (arr1 === arr2) return true;
  if (arr1 == null || arr2 == null) return false;
  if (arr1.length !== arr2.length) return false;

  eqFn = eqFn || eq;

  for (let i = 0; i < arr1.length; i++) {
    if (!eqFn(arr1[i], arr2[i])) return false;
  }

  return true;
}

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
