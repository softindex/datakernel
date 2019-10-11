import mapValues from 'lodash/mapValues';
import omitBy from 'lodash/omitBy';
import isEmpty from 'lodash/isEmpty';

class MapOTOperation {
  constructor(values) {
    this._values = omitBy(values, ({prev, next}) => prev === next);
  }

  static createFromJson(json) {
    return new MapOTOperation(json);
  }

  apply(state) {
    const nextState = {...state};
    for (const [fieldName, {next}] of Object.entries(this._values)) {
      if (next === undefined || next === null) {
        delete nextState[fieldName];
      } else {
        nextState[fieldName] = next;
      }
    }

    return nextState;
  }

  getValues() {
    return this._values;
  }

  isEmpty() {
    return isEmpty(this._values);
  }

  invert() {
    return new MapOTOperation(
      mapValues(this._values, ({prev, next}) => ({
        prev: next,
        next: prev
      }))
    );
  }

  toJSON() {
    return this._values;
  }
}

export default MapOTOperation;
