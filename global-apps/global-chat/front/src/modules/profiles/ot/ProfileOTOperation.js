import isEmpty from 'lodash/isEmpty';
import isEqual from 'lodash/isEqual';
import mapValues from 'lodash/mapValues';

class ProfileOTOperation {
  static EMPTY = new ProfileOTOperation({});

  constructor(values) {
    this._values = mapValues(values, (value) => ({
      prev: value.prev === undefined ? null : value.prev,
      next: value.next === undefined ? null : value.next
    }));
  }

  static createFromJson(json) {
    return new ProfileOTOperation(json);
  }

  apply(state) {
    const nextState = {...state};
    for (const [fieldName, {next}] of Object.entries(this._values)) {
      nextState[fieldName] = next;
    }
    return nextState;
  }

  isEmpty() {
    return isEmpty(this._values);
  }

  invert() {
    const nextValues = {};
    for (const [fieldName, {prev, next}] of Object.entries(this._values)) {
      nextValues[fieldName] = {
        prev: next,
        next: prev
      };
    }

    return new ProfileOTOperation(nextValues);
  }

  isEqual(profilesOTOperation) {
    return isEqual(profilesOTOperation.toJSON(), this.toJSON());
  }

  toJSON() {
    return this._values;
  }
}

export default ProfileOTOperation;
