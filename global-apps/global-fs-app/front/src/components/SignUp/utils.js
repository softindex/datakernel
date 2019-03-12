export function getRandomNumberInRange(min, max) {
  return min + Math.random() * (max - min);
}

export function getRandomArrayElement(a) {
  return a[Math.floor(Math.random() * a.length)];
}
