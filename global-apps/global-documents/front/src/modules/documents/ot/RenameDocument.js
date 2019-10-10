import omitBy from "lodash/omitBy";
import isEmpty from "lodash/isEmpty";
import mapValues from "lodash/mapValues";
import isEqual from "lodash/isEqual";

class RenameDocument {
  constructor(renames) {
    this._renames = omitBy(renames, ({prev, next}) => prev === next);
  }

  static createFromJson(json) {
    return new RenameDocument(json);
  }

  apply(state) {
    for (const [id, {next}] of Object.entries(this._renames)) {
      state.get(id).name = next;
    }

    return state;
  }

  get renames() {
    return this._renames;
  }

  isEmpty() {
    return isEmpty(this._renames);
  }

  invert() {
    return new RenameDocument(
      mapValues(this._renames, ({prev, next}) => ({
        prev: next,
        next: prev
      }))
    );
  }

  isEqual(renameDocument) {
    return isEqual(renameDocument._renames, this._renames);
  }

  toJSON() {
    return this._renames;
  }
}

export default RenameDocument;
