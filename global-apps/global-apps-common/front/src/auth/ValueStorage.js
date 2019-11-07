import cookies from 'js-cookie';

export class ValueStorage {
  constructor(storage, fieldName) {
    this._storage = storage;
    this._fieldName = fieldName;
  }

  static createCookie(fieldName) {
    return new ValueStorage(cookies, fieldName);
  }

  static createLocalStorage(fieldName) {
    return new ValueStorage({
      get: key => localStorage.getItem(key),
      remove: key => localStorage.removeItem(key),
      set: (key, value) => localStorage.setItem(key, value)
    }, fieldName);
  }

  get() {
    return this._storage.get(this._fieldName);
  }

  set(value) {
    return this._storage.set(this._fieldName, value);
  }

  remove() {
    this._storage.remove(this._fieldName);
  }
}
