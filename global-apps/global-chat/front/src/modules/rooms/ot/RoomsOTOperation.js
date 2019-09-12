import isEmpty from 'lodash/isEmpty';
import isEqual from 'lodash/isEqual';

class RoomsOTOperation {
  constructor(roomId, participants, remove) {
    this._roomId = roomId;
    this._participants = participants;
    this._removed = remove;
  }

  static EMPTY = new RoomsOTOperation(null, [], false);

  static createFromJson(json) {
    const room = json.value['shared repo'];
    return new RoomsOTOperation(room.id, room.participants, json.value.remove);
  }

  apply(state) {
    if (this._removed) {
      state.delete(this._roomId);
    } else {
      state.set(this._roomId, {
        participants: this._participants
      });
    }

    return state;
  }

  isEmpty() {
    return isEmpty(this._participants);
  }

  invert() {
    return new RoomsOTOperation(this._roomId, this._participants, !this._removed);
  }

  isEqual(roomOTOperation) {
    return (
      roomOTOperation._roomId === this._roomId
      && isEqual(roomOTOperation._participants, this._participants)
      && roomOTOperation._removed === this._removed
    );
  }

  toJSON() {
    return {
      type: 'CreateOrDropRepo',
      value: {
        'shared repo': {
          id: this._roomId,
          name: '', // TODO Add names to operation
          participants: this._participants
        },
        remove: this._removed
      }
    };
  }
}

export default RoomsOTOperation;
