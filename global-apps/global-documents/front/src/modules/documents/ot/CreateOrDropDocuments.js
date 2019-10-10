import mapValues from 'lodash/mapValues';
import values from 'lodash/values';

class CreateOrDropDocuments {
  constructor(documents) {
    this._documents = documents;
  }

  static createFromJson(json) {
    return new CreateOrDropDocuments(json);
  }

  apply(state) {
    Object.entries(this._documents).forEach(([id, document]) => {
      if (document.remove) {
        state.delete(id);
      } else {
        state.set(id, {
          name: document.name,
          participants: document.participants
        })
      }
    });

    return state;
  }

  get documents() {
    return this._documents;
  }

  isEmpty() {
    return values(this._documents).every(el => el.participants.length === 0);
  }

  invert() {
    return new CreateOrDropDocuments(mapValues(this._documents, room => ({
      name: room.name,
      participants: room.participants,
      remove: !room.remove
    })));
  }

  toJSON() {
    return this._documents;
  }
}

export default CreateOrDropDocuments;
