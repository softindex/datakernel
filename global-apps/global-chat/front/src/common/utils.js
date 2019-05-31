const eq = (a1, a2) => a1 === a2;

function arraysEq(arr1, arr2, eqFn) {
  if (arr1 === arr2) return true;
  if (arr1 == null || arr2 == null) return false;
  if (arr1.length !== arr2.length) return false;

  eqFn = eqFn || eq;

  for (let i = 0; i < arr1.length; i++) {
    if (!eqFn(arr1[i], arr2[i])) return false;
  }

  return true;
}

export {arraysEq};
