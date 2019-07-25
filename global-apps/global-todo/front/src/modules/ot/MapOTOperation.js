import isEqual from 'lodash/isEqual';
import values from 'lodash/values';
import mapValues from 'lodash/mapValues';

class MapOTOperation {
  static EMPTY = new MapOTOperation({});

  constructor(values) {
    this._values = mapValues(values, (value) => ({
      prev: value.prev === undefined ? null : value.prev,
      next: value.next === undefined ? null : value.next
    }));
  }

  static createFromJson(json) {
    return new MapOTOperation(json);
  }

  apply(state) {
    const nextState = {...state};
    for (const [fieldName, {next}] of Object.entries(this._values)) {
      if (next !== undefined && next !== null) { // next can be boolean
        nextState[fieldName] = next;
      } else {
        delete nextState[fieldName];
      }
    }

    return nextState;
  }

  values() {
    return this._values;
  }

  isEmpty() {
    return values(this._values).every(({next, prev}) => next === prev);
  }

  invert() {
    const nextValues = {};
    for (const [fieldName, {prev, next}] of Object.entries(this._values)) {
      nextValues[fieldName] = {
        prev: next,
        next: prev
      };
    }

    return new MapOTOperation(nextValues);
  }

  isEqual(mapOTOperation) {
    return isEqual(mapOTOperation.toJSON(), this.toJSON());
  }

  toJSON() {
    return this._values;
  }
}

export default MapOTOperation;
