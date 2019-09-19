import isEmpty from 'lodash/isEmpty';
import isEqual from 'lodash/isEqual';

class CreateOrDropDocument {
  constructor(documentId, documentName, participants, remove) {
    this._documentId = documentId;
    this._documentName = documentName;
    this._participants = participants;
    this._removed = remove;
  }

  static EMPTY = new CreateOrDropDocument(null, null, [], false);

  static createFromJson(json) {
    const document = json['shared repo'];
    return new CreateOrDropDocument(document.id, document.name, document.participants, json.remove);
  }

  apply(state) {
    if (this._removed) {
      state.delete(this._documentId);
    } else {
      state.set(this._documentId, {
        name: this._documentName,
        participants: this._participants
      });
    }

    return state;
  }

  isEmpty() {
    return isEmpty(this._participants);
  }

  invert() {
    return new CreateOrDropDocument(this._documentId, this._documentName, this._participants, !this._removed);
  }

  isEqual(documentOTOperation) {
    return (
      documentOTOperation._documentId === this._documentId
      && documentOTOperation._documentName === this._documentName
      && isEqual(documentOTOperation._participants, this._participants)
      && documentOTOperation._removed === this._removed
    );
  }

  toJSON() {
    return {
      'shared repo': {
        id: this._documentId,
        name: this._documentName,
        participants: this._participants
      },
      remove: this._removed
    };
  }
}

export default CreateOrDropDocument;
